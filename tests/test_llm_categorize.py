"""
Tests for surfaces/skills_leaderboard/llm_categorize.py

All tests use mocked Anthropic API calls — no network traffic required.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from surfaces.skills_leaderboard.llm_categorize import LLMCategorizer


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

# Minimal categories config matching the real categories.yaml structure.
CATEGORIES_CONFIG = {
    "categories": [
        {"id": "backend",      "description": "Server frameworks, APIs, databases"},
        {"id": "frontend_design", "description": "UI components, CSS, design systems"},
        {"id": "data_ai",      "description": "ML pipelines, model training, analytics"},
        {"id": "devops_infra", "description": "CI/CD, Docker, Kubernetes, cloud"},
        {"id": "other",        "description": "Fallback", "keywords": []},
    ]
}


def _make_response(text: str) -> SimpleNamespace:
    """Build a fake anthropic.messages.create() response."""
    content_block = SimpleNamespace(text=text)
    return SimpleNamespace(content=[content_block])


def _make_categorizer(monkeypatch) -> LLMCategorizer:
    """
    Return a LLMCategorizer with ANTHROPIC_API_KEY set and the Anthropic
    client mocked so no real HTTP call is made.
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")
    with patch("anthropic.Anthropic") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        cat = LLMCategorizer(CATEGORIES_CONFIG)
        cat._client = mock_client  # keep a reference for test assertions
    return cat


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestLLMCategorizerInit:

    def test_raises_without_api_key(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
            LLMCategorizer(CATEGORIES_CONFIG)

    def test_other_excluded_from_valid_ids(self, monkeypatch):
        cat = _make_categorizer(monkeypatch)
        assert "other" not in cat._valid_ids

    def test_valid_ids_populated(self, monkeypatch):
        cat = _make_categorizer(monkeypatch)
        assert "backend" in cat._valid_ids
        assert "data_ai" in cat._valid_ids

    def test_system_prompt_contains_category_ids(self, monkeypatch):
        cat = _make_categorizer(monkeypatch)
        assert "backend" in cat._system_prompt
        assert "data_ai" in cat._system_prompt
        # "other" should NOT be in the system prompt (excluded)
        assert "other" not in cat._system_prompt


# ---------------------------------------------------------------------------
# classify() — happy path
# ---------------------------------------------------------------------------

class TestClassifyHappyPath:

    def test_valid_response_returned(self, monkeypatch):
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("backend")
        result = cat.classify("My API", "REST backend service", [], "SKILL.md")
        assert result == "backend"

    def test_response_stripped_and_lowercased(self, monkeypatch):
        """Leading/trailing whitespace and uppercase are normalised."""
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("  Backend  ")
        result = cat.classify("My API", "REST backend service", [], "SKILL.md")
        assert result == "backend"

    def test_user_message_respects_truncation(self, monkeypatch):
        """Long name/description are truncated before being sent."""
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("data_ai")
        # Use a unique sentinel at position 81 / 301 so we can test presence/absence
        overflow_name_marker = "NAME_OVERFLOW_MARKER"
        overflow_desc_marker = "DESC_OVERFLOW_MARKER"
        long_name = "a" * 80 + overflow_name_marker
        long_desc = "b" * 300 + overflow_desc_marker
        cat.classify(long_name, long_desc, [], "SKILL.md")

        call_kwargs = cat._client.messages.create.call_args
        user_content = call_kwargs.kwargs["messages"][0]["content"]
        assert "a" * 80 in user_content
        assert overflow_name_marker not in user_content
        assert "b" * 300 in user_content
        assert overflow_desc_marker not in user_content

    def test_system_prompt_has_cache_control(self, monkeypatch):
        """The system block must include cache_control so prompt caching fires."""
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("backend")
        cat.classify("name", "desc", [], "SKILL.md")

        call_kwargs = cat._client.messages.create.call_args
        system_blocks = call_kwargs.kwargs["system"]
        assert any(
            block.get("cache_control") == {"type": "ephemeral"}
            for block in system_blocks
        )

    def test_max_tokens_is_16(self, monkeypatch):
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("backend")
        cat.classify("name", "desc", [], "SKILL.md")
        call_kwargs = cat._client.messages.create.call_args
        assert call_kwargs.kwargs["max_tokens"] == 16


# ---------------------------------------------------------------------------
# classify() — invalid / failed responses
# ---------------------------------------------------------------------------

class TestClassifyInvalidResponse:

    def test_unknown_category_returns_none(self, monkeypatch):
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("unknown_cat")
        assert cat.classify("name", "desc", [], "SKILL.md") is None

    def test_other_returned_by_llm_is_invalid(self, monkeypatch):
        """LLM returning 'other' is treated as invalid — never a real answer."""
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("other")
        assert cat.classify("name", "desc", [], "SKILL.md") is None

    def test_empty_response_returns_none(self, monkeypatch):
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("")
        assert cat.classify("name", "desc", [], "SKILL.md") is None

    def test_api_exception_returns_none_after_retries(self, monkeypatch):
        """All retries exhaust → None returned (not re-raised)."""
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.side_effect = Exception("API error")
        with patch("time.sleep"):  # don't actually sleep in tests
            result = cat.classify("name", "desc", [], "SKILL.md")
        assert result is None

    def test_retries_attempted_on_exception(self, monkeypatch):
        """Verify that max_retries attempts are made before giving up."""
        cat = _make_categorizer(monkeypatch)
        cat._max_retries = 3
        cat._client.messages.create.side_effect = Exception("flaky")
        with patch("time.sleep"):
            cat.classify("name", "desc", [], "SKILL.md")
        assert cat._client.messages.create.call_count == 3

    def test_success_on_second_attempt(self, monkeypatch):
        """Transient failure on attempt 1 followed by success on attempt 2."""
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.side_effect = [
            Exception("transient"),
            _make_response("data_ai"),
        ]
        with patch("time.sleep"):
            result = cat.classify("name", "desc", [], "SKILL.md")
        assert result == "data_ai"
        assert cat._client.messages.create.call_count == 2

    def test_bad_response_does_not_retry(self, monkeypatch):
        """An unexpected-but-valid HTTP response (wrong category) → no retry."""
        cat = _make_categorizer(monkeypatch)
        cat._client.messages.create.return_value = _make_response("garbage")
        cat.classify("name", "desc", [], "SKILL.md")
        # Only one call — wrong response shouldn't trigger retry loop
        assert cat._client.messages.create.call_count == 1


# ---------------------------------------------------------------------------
# Integration with categorize()
# ---------------------------------------------------------------------------

class TestCategorizationIntegration:
    """
    Verify the interaction between categorize() and LLMCategorizer without
    making real API calls.
    """

    MINI_CATS_CONFIG = {
        "categories": [
            {
                "id": "backend", "name": "Backend",
                "description": "Server, API",
                "keywords": ["rest api", "graphql", "backend"],
            },
            {
                "id": "data_ai", "name": "Data & AI",
                "description": "ML, AI",
                "keywords": ["machine learning", "ml model", "llm"],
            },
            {"id": "other", "name": "Other", "description": "Fallback", "keywords": []},
        ]
    }

    def test_frontmatter_override_bypasses_llm(self, monkeypatch):
        """When frontmatter has a valid category, LLM is never called."""
        from surfaces.skills_leaderboard.categorization import categorize
        mock_llm = MagicMock()
        result = categorize(
            frontmatter_category="backend",
            frontmatter_tags=[],
            name="my-skill",
            description="some desc",
            repo_topics=[],
            skill_path="SKILL.md",
            readme_excerpt="",
            config=self.MINI_CATS_CONFIG,
            llm_categorizer=mock_llm,
        )
        assert result == "backend"
        mock_llm.classify.assert_not_called()

    def test_llm_result_used_when_frontmatter_absent(self, monkeypatch):
        """Without frontmatter, the LLM result is used."""
        from surfaces.skills_leaderboard.categorization import categorize
        mock_llm = MagicMock()
        mock_llm.classify.return_value = "data_ai"
        result = categorize(
            frontmatter_category=None,
            frontmatter_tags=[],
            name="mystery skill",
            description="something vague",
            repo_topics=[],
            skill_path="SKILL.md",
            readme_excerpt="",
            config=self.MINI_CATS_CONFIG,
            llm_categorizer=mock_llm,
        )
        assert result == "data_ai"
        mock_llm.classify.assert_called_once()

    def test_llm_failure_falls_back_to_keyword_cascade(self, monkeypatch):
        """LLM returning None triggers the keyword cascade."""
        from surfaces.skills_leaderboard.categorization import categorize
        mock_llm = MagicMock()
        mock_llm.classify.return_value = None  # LLM failed
        result = categorize(
            frontmatter_category=None,
            frontmatter_tags=[],
            name="rest api helper",
            description="rest api backend service",  # ≥2 keywords for backend
            repo_topics=[],
            skill_path="SKILL.md",
            readme_excerpt="",
            config=self.MINI_CATS_CONFIG,
            llm_categorizer=mock_llm,
        )
        assert result == "backend"

    def test_no_llm_keyword_cascade_only(self):
        """llm_categorizer=None → keyword cascade runs without touching anthropic."""
        from surfaces.skills_leaderboard.categorization import categorize
        result = categorize(
            frontmatter_category=None,
            frontmatter_tags=[],
            name="ml model trainer",
            description="machine learning ml model pipeline",  # ≥2 hits for data_ai
            repo_topics=[],
            skill_path="SKILL.md",
            readme_excerpt="",
            config=self.MINI_CATS_CONFIG,
            llm_categorizer=None,
        )
        assert result == "data_ai"

    def test_llm_none_and_no_match_returns_other(self):
        """No LLM and no keyword match → falls back to 'other'."""
        from surfaces.skills_leaderboard.categorization import categorize
        result = categorize(
            frontmatter_category=None,
            frontmatter_tags=[],
            name="helper",
            description="a helper",
            repo_topics=[],
            skill_path="SKILL.md",
            readme_excerpt="",
            config=self.MINI_CATS_CONFIG,
            llm_categorizer=None,
        )
        assert result == "other"
