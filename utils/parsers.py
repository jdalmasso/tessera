"""
Generic markdown and YAML parsing utilities.

This module knows nothing about Skills — it operates on arbitrary markdown
content. Skill-specific logic lives in surfaces/skills_leaderboard/.
"""

import re
from typing import Any

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


def is_non_latin_char(c: str) -> bool:
    """
    Return True if *c* belongs to a non-Latin script.

    Covers the most common non-Latin writing systems: CJK (Chinese/Japanese),
    Hangul (Korean), Arabic, Cyrillic (Russian), Hebrew, Thai, and Devanagari
    (Hindi). Latin-script languages (Spanish, French, Portuguese, German, etc.)
    are not affected — their occasional accented characters (é, ñ, ü) are
    nowhere near these Unicode ranges.
    """
    cp = ord(c)
    return (
        0x4E00 <= cp <= 0x9FFF or  # CJK Unified Ideographs (Chinese)
        0x3040 <= cp <= 0x30FF or  # Hiragana + Katakana (Japanese)
        0xAC00 <= cp <= 0xD7AF or  # Hangul (Korean)
        0x0600 <= cp <= 0x06FF or  # Arabic
        0x0400 <= cp <= 0x04FF or  # Cyrillic (Russian, Ukrainian, etc.)
        0x0590 <= cp <= 0x05FF or  # Hebrew
        0x0E00 <= cp <= 0x0E7F or  # Thai
        0x0900 <= cp <= 0x097F     # Devanagari (Hindi, Nepali)
    )


def is_latin_script(text: str, threshold: float = 0.20) -> bool:
    """
    Return True if the fraction of non-Latin-script characters in *text*
    is at or below *threshold* (default 20%).

    An empty string is considered Latin-script (returns True).
    Designed to be applied to skill name + description to detect skills
    written primarily in a non-Latin writing system.
    """
    if not text:
        return True
    non_latin = sum(1 for c in text if is_non_latin_char(c))
    return (non_latin / len(text)) <= threshold


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
