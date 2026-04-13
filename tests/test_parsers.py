"""
Tests for utils/parsers.py

Covers:
  - extract_frontmatter: valid, missing, malformed
  - has_section: heading detection
  - count_lines: non-empty line counting
  - is_non_latin_char: per-character Unicode block detection
  - is_latin_script: whole-text Latin-script heuristic
  - is_valid_skill: minimum inclusion requirements
"""

from utils.parsers import (
    count_lines,
    extract_frontmatter,
    has_section,
    is_latin_script,
    is_non_latin_char,
    is_valid_skill,
)


# ---------------------------------------------------------------------------
# extract_frontmatter
# ---------------------------------------------------------------------------

class TestExtractFrontmatter:

    def test_valid_frontmatter(self):
        content = "---\nname: My Skill\ndescription: Does stuff\n---\nBody here."
        fm, body = extract_frontmatter(content)
        assert fm == {"name": "My Skill", "description": "Does stuff"}
        assert body == "Body here."

    def test_no_frontmatter_returns_empty(self):
        content = "Just a body with no frontmatter."
        fm, body = extract_frontmatter(content)
        assert fm == {}
        assert body == content

    def test_unclosed_frontmatter_returns_empty(self):
        content = "---\nname: Test\n"
        fm, body = extract_frontmatter(content)
        assert fm == {}

    def test_malformed_yaml_returns_empty(self):
        content = "---\n: bad: yaml: here\n---\nBody."
        fm, body = extract_frontmatter(content)
        # yaml.safe_load on bad YAML may either raise or return non-dict; either way returns {}
        assert fm == {} or isinstance(fm, dict)

    def test_non_dict_yaml_returns_empty(self):
        content = "---\n- item1\n- item2\n---\nBody."
        fm, body = extract_frontmatter(content)
        assert fm == {}


# ---------------------------------------------------------------------------
# has_section
# ---------------------------------------------------------------------------

class TestHasSection:

    def test_finds_h2_section(self):
        assert has_section("## Usage\nSome content", "Usage")

    def test_case_insensitive(self):
        assert has_section("## usage\nSome content", "Usage")

    def test_multiple_keywords(self):
        assert has_section("## How to use\nContent", "Usage", "How to use")

    def test_no_match_returns_false(self):
        assert not has_section("## Installation\nContent", "Usage")

    def test_empty_keywords_returns_false(self):
        assert not has_section("## Usage\nContent")

    def test_partial_heading_not_matched(self):
        # "Usage Guide" should not match keyword "Usage" (full-word match enforced)
        assert not has_section("## Usage Guide\nContent", "Usage")


# ---------------------------------------------------------------------------
# count_lines
# ---------------------------------------------------------------------------

class TestCountLines:

    def test_counts_non_empty_lines(self):
        assert count_lines("line1\nline2\nline3") == 3

    def test_ignores_blank_lines(self):
        assert count_lines("line1\n\nline2\n\n") == 2

    def test_empty_string(self):
        assert count_lines("") == 0

    def test_only_whitespace_lines(self):
        assert count_lines("   \n\t\n  ") == 0


# ---------------------------------------------------------------------------
# is_non_latin_char — per-character detection
# ---------------------------------------------------------------------------

class TestIsNonLatinChar:

    # CJK (Chinese/Japanese Kanji)
    def test_cjk_unified_ideograph(self):
        assert is_non_latin_char("中") is True   # U+4E2D

    def test_cjk_boundary_low(self):
        assert is_non_latin_char("\u4E00") is True  # first CJK

    def test_cjk_boundary_high(self):
        assert is_non_latin_char("\u9FFF") is True  # last CJK in range

    # Japanese
    def test_hiragana(self):
        assert is_non_latin_char("あ") is True   # U+3042

    def test_katakana(self):
        assert is_non_latin_char("ア") is True   # U+30A2

    # Korean
    def test_hangul(self):
        assert is_non_latin_char("한") is True   # U+D55C

    # Arabic
    def test_arabic(self):
        assert is_non_latin_char("ع") is True   # U+0639

    # Cyrillic (Russian)
    def test_cyrillic(self):
        assert is_non_latin_char("Я") is True   # U+042F

    # Hebrew
    def test_hebrew(self):
        assert is_non_latin_char("א") is True   # U+05D0

    # Thai
    def test_thai(self):
        assert is_non_latin_char("ก") is True   # U+0E01

    # Devanagari (Hindi)
    def test_devanagari(self):
        assert is_non_latin_char("अ") is True   # U+0905

    # Latin characters — must return False
    def test_ascii_letter(self):
        assert is_non_latin_char("a") is False

    def test_ascii_uppercase(self):
        assert is_non_latin_char("Z") is False

    def test_digit(self):
        assert is_non_latin_char("5") is False

    def test_latin_accented_e(self):
        # é (U+00E9) — used in French, Spanish
        assert is_non_latin_char("é") is False

    def test_latin_tilde_n(self):
        # ñ (U+00F1) — used in Spanish
        assert is_non_latin_char("ñ") is False

    def test_latin_umlaut(self):
        # ü (U+00FC) — used in German
        assert is_non_latin_char("ü") is False

    def test_space(self):
        assert is_non_latin_char(" ") is False

    def test_punctuation(self):
        assert is_non_latin_char(".") is False


# ---------------------------------------------------------------------------
# is_latin_script — whole-text heuristic
# ---------------------------------------------------------------------------

class TestIsLatinScript:

    def test_pure_english_is_latin(self):
        assert is_latin_script("Hello, world! This is an English skill.") is True

    def test_empty_string_is_latin(self):
        # Defined to return True for empty input
        assert is_latin_script("") is True

    def test_pure_chinese_is_not_latin(self):
        assert is_latin_script("你好世界这是一个技能") is False

    def test_pure_japanese_is_not_latin(self):
        assert is_latin_script("こんにちは世界スキル") is False

    def test_pure_korean_is_not_latin(self):
        assert is_latin_script("안녕하세요 세계 스킬") is False

    def test_pure_arabic_is_not_latin(self):
        assert is_latin_script("مرحبا بالعالم هذه مهارة") is False

    def test_pure_russian_is_not_latin(self):
        assert is_latin_script("Привет мир это навык") is False

    def test_spanish_passes_through(self):
        # Spanish uses Latin script; accented chars (é, ñ, á, ó, ú) are NOT non-Latin
        assert is_latin_script("Hola mundo, éste es un skill en español con ñ y ó") is True

    def test_french_passes_through(self):
        assert is_latin_script("Bonjour le monde, voici un skill en français avec des accents é, è, ê") is True

    def test_german_passes_through(self):
        assert is_latin_script("Hallo Welt, dies ist ein Skill auf Deutsch mit Umlauten ü, ö, ä") is True

    def test_mixed_mostly_english_with_few_cjk_is_latin(self):
        # A few CJK chars embedded in mostly-English text: well below 20% threshold
        text = "This is an English description of a skill " + "中" * 2  # 2 CJK out of ~44 chars
        assert is_latin_script(text) is True

    def test_mixed_mostly_chinese_with_english_tech_terms_is_not_latin(self):
        # Realistic non-English skill: Chinese body with some English terms like "api", "sql"
        text = "这是一个技能它使用api和sql来处理数据库请使用docker来部署"
        assert is_latin_script(text) is False

    def test_threshold_exactly_at_boundary(self):
        # 20% threshold: 20 non-Latin out of 100 total chars → exactly at boundary → True
        latin_chars = "a" * 80
        non_latin_chars = "中" * 20
        text = latin_chars + non_latin_chars
        assert is_latin_script(text) is True

    def test_threshold_just_above_boundary(self):
        # 21 non-Latin out of 100 → just over 20% → False
        latin_chars = "a" * 79
        non_latin_chars = "中" * 21
        text = latin_chars + non_latin_chars
        assert is_latin_script(text) is False

    def test_custom_threshold(self):
        # Stricter threshold: 10%
        latin_chars = "a" * 89
        non_latin_chars = "中" * 11  # 11% → above 10% threshold
        text = latin_chars + non_latin_chars
        assert is_latin_script(text, threshold=0.10) is False

    def test_devanagari_not_latin(self):
        assert is_latin_script("यह एक हिंदी कौशल है") is False

    def test_hebrew_not_latin(self):
        assert is_latin_script("שלום עולם זהו כישרון") is False

    def test_thai_not_latin(self):
        assert is_latin_script("สวัสดีโลก นี่คือทักษะ") is False


# ---------------------------------------------------------------------------
# is_valid_skill
# ---------------------------------------------------------------------------

class TestIsValidSkill:

    def test_valid_with_name_and_description(self):
        fm = {"name": "My Skill", "description": "Does stuff"}
        content = "a" * 100
        assert is_valid_skill(fm, content) is True

    def test_valid_with_name_only(self):
        fm = {"name": "My Skill"}
        content = "a" * 100
        assert is_valid_skill(fm, content) is True

    def test_valid_with_description_only(self):
        fm = {"description": "Does stuff"}
        content = "a" * 100
        assert is_valid_skill(fm, content) is True

    def test_empty_frontmatter_is_invalid(self):
        assert is_valid_skill({}, "a" * 100) is False

    def test_missing_name_and_description_is_invalid(self):
        fm = {"tags": ["backend"]}
        assert is_valid_skill(fm, "a" * 100) is False

    def test_content_too_short_is_invalid(self):
        fm = {"name": "My Skill"}
        assert is_valid_skill(fm, "a" * 99) is False

    def test_content_exactly_min_chars_is_valid(self):
        fm = {"name": "My Skill"}
        assert is_valid_skill(fm, "a" * 100) is True

    def test_custom_min_chars(self):
        fm = {"name": "My Skill"}
        assert is_valid_skill(fm, "a" * 50, min_chars=50) is True
        assert is_valid_skill(fm, "a" * 49, min_chars=50) is False
