"""
Tests for surfaces/skills_leaderboard/categorization.py

Covers:
  - Each of the 6 cascade levels independently
  - Case-insensitive matching
  - Category name (not just id) resolution at level 1
  - Multi-match tiebreaker (most keyword hits wins)
  - Config-order tiebreaker when hit counts are equal
  - Fallback to "other" when nothing matches
"""


from surfaces.skills_leaderboard.categorization import categorize, _count_matches, _best_match

# ---------------------------------------------------------------------------
# Minimal test config  — a representative subset of categories.yaml
# ---------------------------------------------------------------------------

_CONFIG = {
    "categories": [
        {
            "id": "backend",
            "name": "Backend",
            "keywords": ["api", "rest", "graphql", "server", "backend",
                         "database", "sql", "fastapi", "django", "flask"],
        },
        {
            "id": "data_ai",
            "name": "Data & AI",
            "keywords": ["machine learning", "ml", "ai", "model", "training",
                         "nlp", "deep learning", "pytorch", "embedding", "rag"],
        },
        {
            "id": "testing_qa",
            "name": "Testing & QA",
            "keywords": ["testing", "test", "qa", "unit test", "integration test",
                         "e2e", "pytest", "debugging", "mock", "coverage"],
        },
        {
            "id": "devops_infra",
            "name": "DevOps & Infra",
            "keywords": ["devops", "docker", "kubernetes", "k8s", "terraform",
                         "ci/cd", "deployment", "cloud", "aws", "helm"],
        },
        {
            "id": "frontend_design",
            "name": "Frontend & Design",
            "keywords": ["frontend", "ui", "react", "vue", "css", "tailwind",
                         "html", "component", "responsive", "accessibility"],
        },
        {
            "id": "security",
            "name": "Security",
            "keywords": ["security", "audit", "vulnerability", "owasp",
                         "encryption", "threat", "hardening", "sast"],
        },
        {
            "id": "other",
            "name": "Other",
            "keywords": [],
        },
    ]
}


# Convenience: call categorize with all-empty inputs except the ones under test
def _cat(
    frontmatter_category=None,
    frontmatter_tags=None,
    name="",
    description="",
    repo_topics=None,
    skill_path="",
    readme_excerpt="",
    config=_CONFIG,
):
    return categorize(
        frontmatter_category=frontmatter_category,
        frontmatter_tags=frontmatter_tags or [],
        name=name,
        description=description,
        repo_topics=repo_topics or [],
        skill_path=skill_path,
        readme_excerpt=readme_excerpt,
        config=config,
    )


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------

class TestHelpers:

    def test_count_matches_exact(self):
        assert _count_matches("api rest server", ["api", "rest"]) == 2

    def test_count_matches_substring(self):
        # multi-word keyword
        assert _count_matches("deep learning model", ["deep learning", "model"]) == 2

    def test_count_matches_case_insensitive(self):
        assert _count_matches("API REST SERVER", ["api", "rest"]) == 2

    def test_count_matches_no_match(self):
        assert _count_matches("hello world", ["api", "rest"]) == 0

    def test_best_match_returns_none_on_no_match(self):
        assert _best_match("hello world", _CONFIG["categories"]) is None

    def test_best_match_skips_empty_keywords(self):
        # "other" has empty keywords — should never be returned
        result = _best_match("other stuff misc", _CONFIG["categories"])
        assert result != "other"
        assert result is None


# ---------------------------------------------------------------------------
# Level 1 — explicit front-matter
# ---------------------------------------------------------------------------

class TestLevel1Frontmatter:

    def test_exact_category_id(self):
        assert _cat(frontmatter_category="backend") == "backend"

    def test_category_id_case_insensitive(self):
        assert _cat(frontmatter_category="Backend") == "backend"
        assert _cat(frontmatter_category="BACKEND") == "backend"

    def test_category_full_name(self):
        # "Data & AI" → id "data_ai"
        assert _cat(frontmatter_category="Data & AI") == "data_ai"

    def test_category_full_name_case_insensitive(self):
        assert _cat(frontmatter_category="data & ai") == "data_ai"

    def test_tag_matching_id(self):
        assert _cat(frontmatter_tags=["devops_infra"]) == "devops_infra"

    def test_tag_matching_full_name(self):
        assert _cat(frontmatter_tags=["Security"]) == "security"

    def test_first_matching_tag_wins(self):
        # first valid tag returned
        result = _cat(frontmatter_tags=["unknown_tag", "backend", "data_ai"])
        assert result == "backend"

    def test_level1_takes_precedence_over_level2(self):
        # Even though description strongly matches data_ai, frontmatter wins
        result = _cat(
            frontmatter_category="backend",
            description="machine learning ai model training nlp pytorch embedding",
        )
        assert result == "backend"

    def test_unknown_frontmatter_falls_through(self):
        # Unknown category → falls through to level 2
        result = _cat(
            frontmatter_category="nonexistent",
            description="api rest server database",
        )
        assert result == "backend"


# ---------------------------------------------------------------------------
# Level 2 — name + description keyword match
# ---------------------------------------------------------------------------

class TestLevel2NameDescription:

    def test_description_match(self):
        assert _cat(description="Build a REST API with FastAPI and database migrations") == "backend"

    def test_name_match(self):
        assert _cat(name="Docker Deployment Workflow") == "devops_infra"

    def test_name_and_description_combined(self):
        # name contributes "api", description contributes "server" → backend
        assert _cat(name="API Helper", description="server-side logic") == "backend"

    def test_multi_word_keyword(self):
        assert _cat(description="machine learning model training with pytorch") == "data_ai"

    def test_level2_takes_precedence_over_level3(self):
        result = _cat(
            description="api rest server fastapi backend database",
            repo_topics=["docker", "kubernetes", "k8s", "deployment", "cloud"],
        )
        # Both match but level 2 fires first
        assert result == "backend"


# ---------------------------------------------------------------------------
# Level 3 — GitHub repo topics
# ---------------------------------------------------------------------------

class TestLevel3RepoTopics:

    def test_topics_match(self):
        assert _cat(repo_topics=["docker", "kubernetes"]) == "devops_infra"

    def test_topics_case_insensitive(self):
        # Topics are typically lowercase but guard against mixed case
        assert _cat(repo_topics=["Docker", "AWS"]) == "devops_infra"

    def test_level3_skipped_when_level2_matches(self):
        result = _cat(
            description="pytest coverage unit test mock debugging",
            repo_topics=["docker", "kubernetes", "aws", "helm"],
        )
        assert result == "testing_qa"

    def test_level3_fires_when_level2_empty(self):
        assert _cat(repo_topics=["react", "css", "tailwind", "ui"]) == "frontend_design"


# ---------------------------------------------------------------------------
# Level 4 — directory path heuristics
# ---------------------------------------------------------------------------

class TestLevel4PathHeuristics:

    def test_parent_dir_matches_category(self):
        assert _cat(skill_path="skills/backend/SKILL.md") == "backend"

    def test_nested_path(self):
        assert _cat(skill_path="collection/data_ai/model-training/SKILL.md") == "data_ai"

    def test_root_skill_falls_through_to_level5(self):
        # SKILL.md at repo root → no parent path component → falls through
        result = _cat(skill_path="SKILL.md", readme_excerpt="pytest unit test coverage mock")
        assert result == "testing_qa"

    def test_level4_fires_when_levels_1_to_3_empty(self):
        assert _cat(skill_path="monorepo/security/audit-tool/SKILL.md") == "security"


# ---------------------------------------------------------------------------
# Level 5 — README excerpt
# ---------------------------------------------------------------------------

class TestLevel5ReadmeExcerpt:

    def test_readme_match(self):
        assert _cat(readme_excerpt="This skill helps with pytest and unit testing.") == "testing_qa"

    def test_readme_only_first_500_chars_used(self):
        # Put a matching word past the 500-char boundary — should not match
        prefix = "x" * 500
        result = _cat(readme_excerpt=prefix + " api rest server backend database")
        assert result == "other"

    def test_readme_within_500_chars_matches(self):
        prefix = "x" * 490
        result = _cat(readme_excerpt=prefix + " api")
        assert result == "backend"

    def test_level5_fires_when_all_others_empty(self):
        assert _cat(readme_excerpt="machine learning ml model deep learning nlp") == "data_ai"


# ---------------------------------------------------------------------------
# Level 6 — default fallback
# ---------------------------------------------------------------------------

class TestLevel6Fallback:

    def test_all_empty_returns_other(self):
        assert _cat() == "other"

    def test_no_keyword_match_returns_other(self):
        assert _cat(
            name="Completely Unrelated Skill",
            description="Some random words with no category keywords",
        ) == "other"

    def test_unknown_tags_fall_through_to_other(self):
        assert _cat(frontmatter_tags=["unknown", "notacategory"]) == "other"


# ---------------------------------------------------------------------------
# Tiebreaker — most keyword hits wins
# ---------------------------------------------------------------------------

class TestTiebreaker:

    def test_more_hits_wins_over_fewer(self):
        # backend: 9 hits (api rest server backend database fastapi django flask sql)
        # data_ai: 2 hits (ml model)
        result = _cat(description="api rest server backend database fastapi django flask sql ml model")
        assert result == "backend"

    def test_config_order_breaks_equal_ties(self):
        # "graphql" → backend=1 hit, "helm" → devops_infra=1 hit; true tie (no substring overlap).
        # backend appears before devops_infra in _CONFIG → backend wins.
        result = _cat(description="graphql helm")
        assert result == "backend"

    def test_most_hits_wins_regardless_of_config_order(self):
        # data_ai is second in config but gets more hits → should win
        result = _cat(description="machine learning ml ai model training nlp deep learning pytorch")
        assert result == "data_ai"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_empty_config_returns_other(self):
        assert _cat(description="api rest server", config={"categories": []}) == "other"

    def test_frontmatter_category_whitespace_stripped(self):
        assert _cat(frontmatter_category="  backend  ") == "backend"

    def test_tags_whitespace_stripped(self):
        assert _cat(frontmatter_tags=["  security  "]) == "security"

    def test_windows_path_separator(self):
        assert _cat(skill_path="skills\\devops_infra\\SKILL.md") == "devops_infra"

    def test_none_topics_treated_as_empty(self):
        # Should not raise; falls through to level 6
        assert _cat(repo_topics=None) == "other"
