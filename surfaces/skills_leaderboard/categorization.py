"""
Skill categorisation via a 6-level cascade.

Each skill is assigned to exactly one of the categories defined in
config/categories.yaml.  The cascade is:

  1. Explicit frontmatter — `category` field or `tags` containing a known
     category id or name.
  2. Keyword match on skill name + SKILL.md description.
  3. Keyword match on GitHub repo topics.
  4. Keyword match on directory path components (monorepo heuristic).
  5. Keyword match on the first 500 characters of the README.
  6. Default → "other".

At every keyword-match level the category with the most hits wins.
Ties are broken by config order (first in categories.yaml).
"""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _count_matches(text: str, keywords: list[str]) -> int:
    """
    Return the number of keywords that appear as substrings of *text*
    (case-insensitive).  Multi-word keywords (e.g. "machine learning") work
    because we use plain substring search rather than word-boundary matching.
    """
    text_lower = text.lower()
    return sum(1 for kw in keywords if kw in text_lower)


def _best_match(text: str, categories: list[dict]) -> str | None:
    """
    Return the id of the category with the most keyword hits in *text*, or
    None if no category matches.  The `other` catch-all (empty keyword list)
    is never returned here.
    """
    best_id: str | None = None
    best_count = 0
    for cat in categories:
        keywords = cat.get("keywords") or []
        if not keywords:
            continue
        count = _count_matches(text, keywords)
        if count > best_count:
            best_count = count
            best_id = cat["id"]
    return best_id


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def categorize(
    frontmatter_category: str | None,
    frontmatter_tags: list[str],
    name: str,
    description: str,
    repo_topics: list[str],
    skill_path: str,
    readme_excerpt: str,
    config: dict[str, Any],
) -> str:
    """
    Assign exactly one category id to a skill.

    Parameters
    ----------
    frontmatter_category:
        Value of the ``category`` field in SKILL.md front-matter (or None).
    frontmatter_tags:
        List of tags from SKILL.md front-matter (may be empty).
    name:
        Skill name (from front-matter or file name).
    description:
        Skill description text from front-matter.
    repo_topics:
        List of GitHub repository topic strings.
    skill_path:
        Relative path to the SKILL.md file inside the repo
        (e.g. ``"skills/backend/SKILL.md"``).
    readme_excerpt:
        Raw text of the README (only the first 500 chars are used).
    config:
        Parsed ``categories.yaml`` as a dict (must contain a ``categories``
        list of ``{id, name, keywords}`` objects).

    Returns
    -------
    str
        A category id string (e.g. ``"backend"``, ``"data_ai"``).
        Falls back to ``"other"`` if nothing matches.
    """
    categories: list[dict] = config.get("categories", [])

    # Pre-build lookup structures for level-1 exact matching
    cat_ids: set[str] = {cat["id"] for cat in categories}
    name_to_id: dict[str, str] = {
        cat["name"].lower(): cat["id"] for cat in categories
    }

    # ------------------------------------------------------------------
    # Level 1 — explicit front-matter
    # ------------------------------------------------------------------
    if frontmatter_category:
        fc = frontmatter_category.strip().lower()
        if fc in cat_ids:
            return fc
        if fc in name_to_id:
            return name_to_id[fc]

    for tag in (frontmatter_tags or []):
        t = tag.strip().lower()
        if t in cat_ids:
            return t
        if t in name_to_id:
            return name_to_id[t]

    # ------------------------------------------------------------------
    # Level 2 — name + description keyword match
    # ------------------------------------------------------------------
    text_l2 = f"{name} {description}".strip()
    if text_l2:
        match = _best_match(text_l2, categories)
        if match:
            return match

    # ------------------------------------------------------------------
    # Level 3 — GitHub repo topics
    # ------------------------------------------------------------------
    if repo_topics:
        match = _best_match(" ".join(repo_topics), categories)
        if match:
            return match

    # ------------------------------------------------------------------
    # Level 4 — directory path heuristics
    # ------------------------------------------------------------------
    if skill_path:
        parts = skill_path.replace("\\", "/").split("/")[:-1]  # drop filename
        if parts:
            match = _best_match(" ".join(parts), categories)
            if match:
                return match

    # ------------------------------------------------------------------
    # Level 5 — README excerpt (first 500 chars)
    # ------------------------------------------------------------------
    if readme_excerpt:
        match = _best_match(readme_excerpt[:500], categories)
        if match:
            return match

    # ------------------------------------------------------------------
    # Level 6 — default
    # ------------------------------------------------------------------
    return "other"
