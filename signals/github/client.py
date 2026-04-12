"""
GitHub REST API v3 wrapper.

Generic — knows nothing about Skills. Handles:
  - Authentication via Bearer token
  - Rate limiting: sleeps until X-RateLimit-Reset + 1s, then retries
  - Transient failures: exponential backoff, up to max_retries retries
  - Pagination: follows Link rel="next" headers
  - Per-request timeout (default 30s)
"""

import base64
import logging
import time
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


class GitHubClient:
    BASE_URL = "https://api.github.com"

    def __init__(self, token: str, timeout: int = 30, max_retries: int = 3) -> None:
        self.timeout = timeout
        self.max_retries = max_retries  # retries after the initial attempt
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _sleep_until_reset(self, resp: requests.Response) -> None:
        """Sleep until the rate-limit window resets (X-RateLimit-Reset + 1s)."""
        reset_ts = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait = max(1.0, reset_ts - time.time() + 1)
        logger.info("Rate limited — sleeping %.0fs until reset.", wait)
        time.sleep(wait)

    def _request(
        self, method: str, url: str, params: Optional[dict] = None
    ) -> Optional[requests.Response]:
        """
        Make one HTTP request with retry logic.

        Returns a Response on HTTP 200, None on 404 or permanent failure.
        Sleeps on rate limits (403/429) and retries.
        Applies exponential backoff on 5xx and network errors.
        """
        for attempt in range(self.max_retries + 1):
            try:
                resp = self.session.request(
                    method, url, params=params, timeout=self.timeout
                )
            except requests.Timeout:
                logger.warning(
                    "Timeout on %s (attempt %d/%d)", url, attempt + 1, self.max_retries + 1
                )
                if attempt < self.max_retries:
                    time.sleep(2**attempt)
                continue
            except requests.RequestException as exc:
                logger.warning(
                    "Request error on %s: %s (attempt %d/%d)",
                    url, exc, attempt + 1, self.max_retries + 1,
                )
                if attempt < self.max_retries:
                    time.sleep(2**attempt)
                continue

            if resp.status_code == 200:
                # Proactively sleep if the rate limit is exhausted
                if int(resp.headers.get("X-RateLimit-Remaining", 1)) == 0:
                    self._sleep_until_reset(resp)
                return resp

            if resp.status_code == 404:
                return None  # not found — not a retryable error

            if resp.status_code in (403, 429):
                logger.warning("Rate limited on %s.", url)
                self._sleep_until_reset(resp)
                continue  # retry after sleeping

            if resp.status_code >= 500:
                logger.warning(
                    "Server error %d on %s (attempt %d/%d)",
                    resp.status_code, url, attempt + 1, self.max_retries + 1,
                )
                if attempt < self.max_retries:
                    time.sleep(2**attempt)
                continue

            logger.warning("Unexpected status %d on %s.", resp.status_code, url)
            return None

        logger.error("All %d attempts failed for %s.", self.max_retries + 1, url)
        return None

    def _paginate(
        self,
        url: str,
        params: dict,
        items_key: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Collect all pages from a paginated endpoint.

        If `items_key` is given (e.g. "items" for search endpoints), extract
        that key from each page's JSON. Otherwise the JSON is expected to be
        a list directly (commits, contributors, etc.).
        """
        results: list[dict[str, Any]] = []
        page_params = {**params, "page": 1}

        while True:
            resp = self._request("GET", url, params=page_params)
            if resp is None:
                break

            data = resp.json()
            items = data.get(items_key, []) if items_key else (data if isinstance(data, list) else [])

            if not items:
                break

            results.extend(items)

            if 'rel="next"' not in resp.headers.get("Link", ""):
                break

            page_params = {**page_params, "page": page_params["page"] + 1}

        return results

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def get_repo(self, owner: str, repo: str) -> Optional[dict[str, Any]]:
        """GET /repos/{owner}/{repo} — full repository metadata."""
        resp = self._request("GET", f"{self.BASE_URL}/repos/{owner}/{repo}")
        return resp.json() if resp else None

    def get_contents(
        self, owner: str, repo: str, path: str = ""
    ) -> Optional[Any]:
        """
        GET /repos/{owner}/{repo}/contents/{path}

        Returns a list of entry dicts for directories, a single dict for files,
        or None if the path does not exist.
        """
        resp = self._request(
            "GET", f"{self.BASE_URL}/repos/{owner}/{repo}/contents/{path}"
        )
        return resp.json() if resp else None

    def get_file_content(
        self, owner: str, repo: str, path: str
    ) -> Optional[str]:
        """
        Fetch and base64-decode the text content of a file.
        Returns None if the file is not found or is not base64-encoded.
        """
        data = self.get_contents(owner, repo, path)
        if not isinstance(data, dict) or data.get("encoding") != "base64":
            return None
        try:
            return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
        except Exception as exc:
            logger.warning("Failed to decode %s/%s/%s: %s", owner, repo, path, exc)
            return None

    def get_commits(
        self,
        owner: str,
        repo: str,
        since: Optional[str] = None,
        until: Optional[str] = None,
        per_page: int = 100,
    ) -> list[dict[str, Any]]:
        """GET /repos/{owner}/{repo}/commits — returns all pages."""
        params: dict[str, Any] = {"per_page": per_page}
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        return self._paginate(
            f"{self.BASE_URL}/repos/{owner}/{repo}/commits", params
        )

    def get_contributors(
        self, owner: str, repo: str, per_page: int = 100
    ) -> list[dict[str, Any]]:
        """GET /repos/{owner}/{repo}/contributors — returns all pages."""
        return self._paginate(
            f"{self.BASE_URL}/repos/{owner}/{repo}/contributors",
            {"per_page": per_page, "anon": "false"},
        )

    def search_repos(
        self, query: str, sort: str = "stars", per_page: int = 100
    ) -> list[dict[str, Any]]:
        """GET /search/repositories — returns all repo items."""
        return self._paginate(
            f"{self.BASE_URL}/search/repositories",
            {"q": query, "sort": sort, "order": "desc", "per_page": per_page},
            items_key="items",
        )

    def search_code(
        self, query: str, per_page: int = 100
    ) -> list[dict[str, Any]]:
        """GET /search/code — returns all code match items."""
        return self._paginate(
            f"{self.BASE_URL}/search/code",
            {"q": query, "per_page": per_page},
            items_key="items",
        )
