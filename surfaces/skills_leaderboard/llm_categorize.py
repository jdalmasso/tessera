"""
LLM-based skill categoriser using Claude Haiku.

Replaces levels 2–5 of the keyword cascade with a single Claude API call.
Level 1 (frontmatter override) and level 6 (default "other") are unchanged.

The system prompt lists all valid category ids (excluding "other") with their
descriptions. The LLM is instructed to return ONLY the category id, which
means max_tokens=16 is more than sufficient.

Prompt caching: the system prompt uses cache_control: ephemeral so it is
reused across calls within the same 5-minute billing window, reducing the
per-call cost to the ~70-token uncached user message.
"""

from __future__ import annotations

import logging
import os
import time

logger = logging.getLogger(__name__)


class LLMCategorizer:
    """
    Single-label classifier that uses the Anthropic API to assign a skill
    to one of the predefined categories (excluding "other").

    Raises ``RuntimeError`` on construction if ``ANTHROPIC_API_KEY`` is not
    set or if the ``anthropic`` package is not installed.

    ``classify()`` returns ``None`` on any API or parsing failure so the
    caller can transparently fall through to the keyword cascade.
    """

    def __init__(
        self,
        categories_config: dict,
        model: str = "claude-3-5-haiku-20241022",
        max_retries: int = 3,
    ) -> None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY environment variable is not set")

        try:
            import anthropic as _anthropic
        except ImportError as exc:
            raise RuntimeError(
                "The 'anthropic' package is required for LLM categorisation. "
                "Install it with: pip install 'anthropic>=0.40,<1.0'"
            ) from exc

        self._client = _anthropic.Anthropic(api_key=api_key)
        self._model = model
        self._max_retries = max_retries

        # Build valid id set and system prompt, EXCLUDING "other" so the LLM
        # is never prompted to return it (forces a real classification).
        categories = [
            cat for cat in categories_config.get("categories", [])
            if cat.get("id") != "other"
        ]
        self._valid_ids: frozenset[str] = frozenset(
            cat["id"] for cat in categories
        )
        self._system_prompt = self._build_system_prompt(categories)

    @staticmethod
    def _build_system_prompt(categories: list[dict]) -> str:
        lines = [
            "You are a skill categoriser. Given a Claude Code skill's metadata, "
            "return EXACTLY ONE category id from the list below — nothing else, "
            "no punctuation, no explanation.\n",
            "Valid categories:",
        ]
        for cat in categories:
            lines.append(f"  {cat['id']} — {cat.get('description', '')}")
        lines.append(
            "\nRespond with only the category id, lowercase, no spaces around it."
        )
        return "\n".join(lines)

    def classify(
        self,
        name: str,
        description: str,
        repo_topics: list[str],
        skill_path: str,
    ) -> str | None:
        """
        Return the category id for a skill, or ``None`` on failure.

        ``None`` signals the caller to fall through to the keyword cascade.
        """
        user_msg = (
            f"Name: {name[:80]}\n"
            f"Description: {description[:300]}\n"
            f"Topics: {', '.join(repo_topics[:10])}\n"
            f"Path: {skill_path[:60]}"
        )

        for attempt in range(self._max_retries):
            try:
                response = self._client.messages.create(
                    model=self._model,
                    max_tokens=16,
                    system=[
                        {
                            "type": "text",
                            "text": self._system_prompt,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    messages=[{"role": "user", "content": user_msg}],
                )
                raw = response.content[0].text.strip().lower()
                if raw in self._valid_ids:
                    return raw
                logger.debug(
                    "LLM returned unexpected category %r for %r; falling back to keyword cascade.",
                    raw, name,
                )
                return None  # bad-but-successful response — don't retry

            except Exception as exc:
                if attempt < self._max_retries - 1:
                    wait = 2 ** (attempt + 1)  # 2s, 4s
                    logger.warning(
                        "LLM classify attempt %d/%d failed for %r: %s; retrying in %ds",
                        attempt + 1, self._max_retries, name, exc, wait,
                    )
                    time.sleep(wait)
                else:
                    logger.warning(
                        "LLM classify failed after %d attempts for %r: %s",
                        self._max_retries, name, exc,
                    )

        return None
