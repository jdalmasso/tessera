"""
Generic markdown and YAML parsing utilities.

This module knows nothing about Skills — it operates on arbitrary markdown
content. Skill-specific logic lives in surfaces/skills_leaderboard/.
"""

import re
from typing import Any, Optional

import yaml


def extract_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    """
    Extract YAML frontmatter from a markdown string.

    Returns a (frontmatter_dict, body) tuple where `body` is the content
    after the closing `---` delimiter. If no valid frontmatter is found,
    returns ({}, original_content).

    Frontmatter must begin on the very first line with `---`.
    """
    if not content.startswith("---"):
        return {}, content

    # Find the closing delimiter (must be on its own line)
    close = content.find("\n---", 3)
    if close == -1:
        return {}, content

    yaml_block = content[3:close].strip()
    body = content[close + 4:].lstrip("\n")

    try:
        data = yaml.safe_load(yaml_block)
    except yaml.YAMLError:
        return {}, content

    if not isinstance(data, dict):
        return {}, content

    return data, body


def has_section(content: str, *keywords: str) -> bool:
    """
    Return True if the markdown content contains an ATX heading (# through ######)
    whose text matches any of the given keywords (case-insensitive, full-word match).

    Example:
        has_section(text, "Usage", "How to use")  # matches "## Usage" or "## How to use"
    """
    if not keywords:
        return False
    pattern = re.compile(
        r"^#{1,6}\s+(" + "|".join(re.escape(k) for k in keywords) + r")\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    return bool(pattern.search(content))


def count_lines(content: str) -> int:
    """Return the number of non-empty lines in the content."""
    return sum(1 for line in content.splitlines() if line.strip())


def is_valid_skill(
    frontmatter: dict[str, Any],
    content: str,
    min_chars: int = 100,
) -> bool:
    """
    Return True if a SKILL.md meets minimum inclusion requirements:

    - Has non-empty YAML frontmatter
    - Frontmatter contains at least one of 'name' or 'description'
    - Total content length is >= min_chars characters
    """
    if not frontmatter:
        return False
    if "name" not in frontmatter and "description" not in frontmatter:
        return False
    if len(content) < min_chars:
        return False
    return True


def normalize_tags(value: Any) -> list[str]:
    """
    Coerce a frontmatter tags/category value into a list of lowercase strings.

    Handles:
      - None               → []
      - "single string"    → ["single string"]
      - ["a", "b"]         → ["a", "b"]
      - ["A", " B "]       → ["a", "b"]  (lowercased and stripped)
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip().lower()] if value.strip() else []
    if isinstance(value, list):
        return [str(v).strip().lower() for v in value if str(v).strip()]
    return []
