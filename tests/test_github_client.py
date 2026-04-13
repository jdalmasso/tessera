"""
Tests for signals/github/client.py

All network calls are intercepted via unittest.mock — no real HTTP traffic.
"""

import base64
import time
from unittest.mock import MagicMock, patch

import requests

from signals.github.client import GitHubClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_response(
    status_code: int = 200,
    json_body=None,
    headers: dict = None,
) -> MagicMock:
    """Build a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_body if json_body is not None else {}
    base_headers = {"X-RateLimit-Remaining": "100", "X-RateLimit-Reset": "9999999999"}
    if headers:
        base_headers.update(headers)
    resp.headers = base_headers
    return resp


def make_client() -> GitHubClient:
    return GitHubClient(token="test-token", timeout=5, max_retries=3)


# ---------------------------------------------------------------------------
# _request — success paths
# ---------------------------------------------------------------------------

class TestRequestSuccess:
    def test_200_returns_response(self):
        client = make_client()
        resp = make_response(200, {"full_name": "a/b"})
        with patch.object(client.session, "request", return_value=resp):
            result = client._request("GET", "https://api.github.com/repos/a/b")
        assert result is resp

    def test_404_returns_none(self):
        client = make_client()
        resp = make_response(404)
        with patch.object(client.session, "request", return_value=resp):
            result = client._request("GET", "https://api.github.com/repos/a/b")
        assert result is None

    def test_unexpected_status_returns_none(self):
        client = make_client()
        resp = make_response(422)
        with patch.object(client.session, "request", return_value=resp):
            result = client._request("GET", "https://api.github.com/foo")
        assert result is None


# ---------------------------------------------------------------------------
# _request — rate limiting
# ---------------------------------------------------------------------------

class TestRateLimiting:
    def test_429_sleeps_until_reset_and_retries(self):
        client = make_client()
        reset_ts = int(time.time()) + 2
        rate_limited = make_response(429, headers={"X-RateLimit-Reset": str(reset_ts)})
        success = make_response(200, {"ok": True})

        with patch.object(client.session, "request", side_effect=[rate_limited, success]):
            with patch("signals.github.client.time.sleep") as mock_sleep:
                result = client._request("GET", "https://api.github.com/foo")

        assert result is success
        mock_sleep.assert_called_once()
        assert mock_sleep.call_args[0][0] >= 1  # slept at least 1 second

    def test_403_also_triggers_rate_limit_sleep(self):
        client = make_client()
        reset_ts = int(time.time()) + 5
        rate_limited = make_response(403, headers={"X-RateLimit-Reset": str(reset_ts)})
        success = make_response(200, {})

        with patch.object(client.session, "request", side_effect=[rate_limited, success]):
            with patch("signals.github.client.time.sleep"):
                result = client._request("GET", "https://api.github.com/foo")

        assert result is success

    def test_remaining_zero_sleeps_after_success(self):
        client = make_client()
        reset_ts = int(time.time()) + 10
        resp = make_response(200, {}, headers={
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(reset_ts),
        })

        with patch.object(client.session, "request", return_value=resp):
            with patch("signals.github.client.time.sleep") as mock_sleep:
                result = client._request("GET", "https://api.github.com/foo")

        assert result is resp
        mock_sleep.assert_called_once()


# ---------------------------------------------------------------------------
# _request — retries and backoff
# ---------------------------------------------------------------------------

class TestRetryAndBackoff:
    def test_5xx_retries_with_backoff(self):
        client = make_client()
        server_err = make_response(500)
        success = make_response(200, {"ok": True})

        with patch.object(client.session, "request", side_effect=[server_err, success]):
            with patch("signals.github.client.time.sleep") as mock_sleep:
                result = client._request("GET", "https://api.github.com/foo")

        assert result is success
        mock_sleep.assert_called_once_with(1)  # 2**0 = 1

    def test_exponential_backoff_sequence(self):
        client = make_client()
        errors = [make_response(503)] * 3
        success = make_response(200, {})

        with patch.object(client.session, "request", side_effect=[*errors, success]):
            with patch("signals.github.client.time.sleep") as mock_sleep:
                result = client._request("GET", "https://api.github.com/foo")

        assert result is success
        sleep_calls = [c[0][0] for c in mock_sleep.call_args_list]
        assert sleep_calls == [1, 2, 4]  # 2**0, 2**1, 2**2

    def test_all_retries_exhausted_returns_none(self):
        client = make_client()  # max_retries=3 → 4 total attempts
        server_err = make_response(500)

        with patch.object(client.session, "request", return_value=server_err):
            with patch("signals.github.client.time.sleep"):
                result = client._request("GET", "https://api.github.com/foo")

        assert result is None

    def test_timeout_triggers_retry(self):
        client = make_client()
        success = make_response(200, {"ok": True})

        with patch.object(
            client.session, "request",
            side_effect=[requests.Timeout(), success],
        ):
            with patch("signals.github.client.time.sleep"):
                result = client._request("GET", "https://api.github.com/foo")

        assert result is success

    def test_connection_error_triggers_retry(self):
        client = make_client()
        success = make_response(200, {})

        with patch.object(
            client.session, "request",
            side_effect=[requests.ConnectionError("refused"), success],
        ):
            with patch("signals.github.client.time.sleep"):
                result = client._request("GET", "https://api.github.com/foo")

        assert result is success


# ---------------------------------------------------------------------------
# _paginate
# ---------------------------------------------------------------------------

class TestPaginate:
    def test_single_page_no_link_header(self):
        client = make_client()
        resp = make_response(200, [{"id": 1}, {"id": 2}])
        resp.headers["Link"] = ""

        with patch.object(client, "_request", return_value=resp):
            results = client._paginate("https://api.github.com/foo", {})

        assert results == [{"id": 1}, {"id": 2}]

    def test_follows_next_link(self):
        client = make_client()
        page1 = make_response(200, [{"id": 1}])
        page1.headers["Link"] = '<https://api.github.com/foo?page=2>; rel="next"'
        page2 = make_response(200, [{"id": 2}])
        page2.headers["Link"] = ""

        with patch.object(client, "_request", side_effect=[page1, page2]):
            results = client._paginate("https://api.github.com/foo", {})

        assert results == [{"id": 1}, {"id": 2}]

    def test_extracts_items_key(self):
        client = make_client()
        resp = make_response(200, {"total_count": 2, "items": [{"id": 1}, {"id": 2}]})
        resp.headers["Link"] = ""

        with patch.object(client, "_request", return_value=resp):
            results = client._paginate("https://api.github.com/search/repos", {}, items_key="items")

        assert results == [{"id": 1}, {"id": 2}]

    def test_empty_items_stops_pagination(self):
        client = make_client()
        resp = make_response(200, {"items": []})
        resp.headers["Link"] = '<https://api.github.com/foo?page=2>; rel="next"'

        with patch.object(client, "_request", return_value=resp):
            results = client._paginate("https://api.github.com/foo", {}, items_key="items")

        assert results == []

    def test_failed_request_stops_pagination(self):
        client = make_client()
        with patch.object(client, "_request", return_value=None):
            results = client._paginate("https://api.github.com/foo", {})
        assert results == []


# ---------------------------------------------------------------------------
# High-level methods
# ---------------------------------------------------------------------------

class TestGetRepo:
    def test_returns_parsed_json(self):
        client = make_client()
        payload = {"full_name": "owner/repo", "stargazers_count": 42}
        with patch.object(client, "_request", return_value=make_response(200, payload)):
            result = client.get_repo("owner", "repo")
        assert result == payload

    def test_returns_none_on_404(self):
        client = make_client()
        with patch.object(client, "_request", return_value=None):
            result = client.get_repo("owner", "missing")
        assert result is None


class TestGetFileContent:
    def test_decodes_base64_content(self):
        client = make_client()
        raw = "---\nname: my-skill\n---\n\nContent here."
        encoded = base64.b64encode(raw.encode()).decode()
        payload = {"encoding": "base64", "content": encoded}
        with patch.object(client, "get_contents", return_value=payload):
            result = client.get_file_content("owner", "repo", "SKILL.md")
        assert result == raw

    def test_returns_none_for_missing_file(self):
        client = make_client()
        with patch.object(client, "get_contents", return_value=None):
            result = client.get_file_content("owner", "repo", "SKILL.md")
        assert result is None

    def test_returns_none_for_non_base64_encoding(self):
        client = make_client()
        with patch.object(client, "get_contents", return_value={"encoding": "utf-8", "content": "x"}):
            result = client.get_file_content("owner", "repo", "SKILL.md")
        assert result is None

    def test_returns_none_for_directory(self):
        client = make_client()
        with patch.object(client, "get_contents", return_value=[{"name": "SKILL.md"}]):
            result = client.get_file_content("owner", "repo", "")
        assert result is None


class TestSearchMethods:
    def test_search_repos_passes_items_key(self):
        client = make_client()
        items = [{"full_name": "a/b"}]
        with patch.object(client, "_paginate", return_value=items) as mock_pag:
            result = client.search_repos("topic:claude-skill")
        assert result == items
        assert mock_pag.call_args[1].get("items_key") == "items" or \
               mock_pag.call_args[0][2] == "items"

    def test_search_code_passes_items_key(self):
        client = make_client()
        items = [{"path": "SKILL.md", "repository": {"full_name": "a/b"}}]
        with patch.object(client, "_paginate", return_value=items):
            result = client.search_code("filename:SKILL.md")
        assert result == items

    def test_get_commits_passes_since_until(self):
        client = make_client()
        with patch.object(client, "_paginate", return_value=[]) as mock_pag:
            client.get_commits("owner", "repo", since="2026-01-01T00:00:00Z", until="2026-04-01T00:00:00Z")
        params = mock_pag.call_args[0][1]
        assert params["since"] == "2026-01-01T00:00:00Z"
        assert params["until"] == "2026-04-01T00:00:00Z"

    def test_get_commits_omits_none_params(self):
        client = make_client()
        with patch.object(client, "_paginate", return_value=[]) as mock_pag:
            client.get_commits("owner", "repo")
        params = mock_pag.call_args[0][1]
        assert "since" not in params
        assert "until" not in params

    def test_auth_header_set(self):
        client = make_client()
        assert client.session.headers["Authorization"] == "Bearer test-token"
        assert "application/vnd.github" in client.session.headers["Accept"]


# ---------------------------------------------------------------------------
# get_contents
# ---------------------------------------------------------------------------

class TestGetContents:
    def test_returns_list_for_directory(self):
        client = make_client()
        payload = [{"name": "SKILL.md", "type": "file", "path": "SKILL.md"}]
        resp = make_response(200, payload)
        with patch.object(client.session, "request", return_value=resp):
            result = client.get_contents("owner", "repo", "")
        assert result == payload

    def test_returns_dict_for_file(self):
        client = make_client()
        payload = {"name": "SKILL.md", "encoding": "base64", "content": "dGVzdA==\n"}
        resp = make_response(200, payload)
        with patch.object(client.session, "request", return_value=resp):
            result = client.get_contents("owner", "repo", "SKILL.md")
        assert result == payload

    def test_returns_none_on_404(self):
        client = make_client()
        resp = make_response(404)
        with patch.object(client.session, "request", return_value=resp):
            result = client.get_contents("owner", "repo", "missing.md")
        assert result is None

    def test_correct_url_constructed(self):
        client = make_client()
        with patch.object(client, "_request", return_value=None) as mock_req:
            client.get_contents("alice", "myrepo", "subdir/SKILL.md")
        url = mock_req.call_args[0][1]
        assert "alice/myrepo/contents/subdir/SKILL.md" in url


# ---------------------------------------------------------------------------
# get_contributors
# ---------------------------------------------------------------------------

class TestGetContributors:
    def test_returns_list(self):
        client = make_client()
        payload = [{"login": "alice"}, {"login": "bob"}]
        with patch.object(client, "_paginate", return_value=payload):
            result = client.get_contributors("owner", "repo")
        assert result == payload

    def test_empty_repo_returns_empty_list(self):
        client = make_client()
        with patch.object(client, "_paginate", return_value=[]):
            result = client.get_contributors("owner", "repo")
        assert result == []

    def test_correct_url_and_params(self):
        client = make_client()
        with patch.object(client, "_paginate", return_value=[]) as mock_pag:
            client.get_contributors("alice", "repo", per_page=50)
        url = mock_pag.call_args[0][0]
        params = mock_pag.call_args[0][1]
        assert "alice/repo/contributors" in url
        assert params["per_page"] == 50
        assert params["anon"] == "false"
