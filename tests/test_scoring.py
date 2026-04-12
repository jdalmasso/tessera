"""
Tests for signals/github/scoring.py

All tests use known inputs and verify expected outputs to within a small
tolerance. Config values match the defaults in config/scoring.yaml.
"""

import math
import pytest

from signals.github.scoring import (
    score_adoption,
    score_code_quality,
    score_contributors,
    score_documentation,
    score_freshness,
    score_velocity,
)

APPROX = pytest.approx  # alias for readability
TOLERANCE = 1e-6


# ---------------------------------------------------------------------------
# Shared fixture — minimal config matching scoring.yaml defaults
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return {
        "thresholds": {
            "min_age_days": 14,
            "min_age_velocity_default": 0.5,
        },
        "velocity": {
            "acceleration_cap": 2.0,
            "consistency_weeks": 13,
        },
        "adoption": {
            "weights": {"stars": 0.5, "forks": 0.3, "watchers": 0.2},
        },
        "freshness": {
            "half_life_days": 30,
            "activity_sigmoid_mid": 5,
            "activity_sigmoid_sat": 20,
            "maturity_days": 30,
            "weights": {"decay": 0.5, "activity": 0.4, "maturity": 0.1},
        },
        "documentation": {
            "description_min_chars": 20,
            "line_count_low": 50,
            "line_count_mid": 100,
            "line_count_high": 300,
            "weights": {
                "has_frontmatter": 0.10,
                "has_name": 0.05,
                "has_description": 0.15,
                "line_count": 0.20,
                "has_examples": 0.15,
                "has_usage": 0.10,
                "has_readme": 0.10,
                "has_scripts": 0.075,
                "has_references": 0.075,
            },
        },
        "contributors": {
            "log_cap": 10,
        },
    }


# ---------------------------------------------------------------------------
# Velocity
# ---------------------------------------------------------------------------

class TestVelocity:
    def test_min_age_rule_returns_default(self, config):
        """Repos < 14 days old always return 0.5 regardless of commits."""
        score = score_velocity(100, 0, 13, repo_age_days=7, config=config)
        assert score == APPROX(0.5)

    def test_min_age_boundary_exact(self, config):
        """Repo exactly at min_age_days threshold is NOT subject to the rule."""
        score = score_velocity(10, 5, 13, repo_age_days=14, config=config)
        assert score != APPROX(0.5)

    def test_perfect_score(self, config):
        """Max acceleration (2x) + full consistency → 1.0."""
        # acceleration: 20/10 = 2.0 → capped → score = 1.0
        # consistency: 13/13 = 1.0
        # blend: 0.5*1.0 + 0.5*1.0 = 1.0
        score = score_velocity(20, 10, 13, repo_age_days=30, config=config)
        assert score == APPROX(1.0)

    def test_zero_commits_both_windows(self, config):
        """No activity in either window → 0.0 velocity."""
        # acceleration: 0/1 = 0 → score = 0
        # consistency: 0/13 = 0
        score = score_velocity(0, 0, 0, repo_age_days=30, config=config)
        assert score == APPROX(0.0)

    def test_acceleration_2x_capped(self, config):
        """Ratio above cap is clamped to cap."""
        # 100 vs 1 would be 100x, capped at 2.0 → acceleration_score = 1.0
        score_high = score_velocity(100, 1, 0, repo_age_days=30, config=config)
        score_cap  = score_velocity(20, 10, 0, repo_age_days=30, config=config)
        assert score_high == APPROX(score_cap)

    def test_acceleration_from_zero_prev(self, config):
        """New activity with no prior commits → max acceleration score."""
        # prev = max(0, 1) = 1; ratio = 5/1 = 5 → capped at 2 → accel = 1.0
        score = score_velocity(5, 0, 0, repo_age_days=30, config=config)
        assert score == APPROX(0.5 * 1.0 + 0.5 * 0.0)

    def test_deceleration_halves_acceleration_score(self, config):
        """Half the prior commits → acceleration score = 0.25."""
        # prev=10, current=5 → ratio=0.5 → accel_score = 0.5/2 = 0.25
        # consistency=0
        score = score_velocity(5, 10, 0, repo_age_days=30, config=config)
        assert score == APPROX(0.5 * 0.25 + 0.5 * 0.0)

    def test_consistency_only(self, config):
        """Full consistency but flat commit rate → 0.5 blend score."""
        # acceleration: 10/10 = 1.0 → score = 0.5
        # consistency: 13/13 = 1.0
        score = score_velocity(10, 10, 13, repo_age_days=30, config=config)
        assert score == APPROX(0.5 * 0.5 + 0.5 * 1.0)

    def test_consistency_capped_at_one(self, config):
        """More than 13 unique weeks is capped at 1.0."""
        score = score_velocity(10, 10, 20, repo_age_days=30, config=config)
        assert score == APPROX(0.5 * 0.5 + 0.5 * 1.0)


# ---------------------------------------------------------------------------
# Adoption
# ---------------------------------------------------------------------------

class TestAdoption:
    def test_zero_everything(self, config):
        """All zeros → 0.0."""
        score = score_adoption(0, 0, 0, 0, 0, 0, config=config)
        assert score == APPROX(0.0)

    def test_equals_corpus_max(self, config):
        """Equal to corpus max → 1.0."""
        score = score_adoption(100, 50, 20, 100, 50, 20, config=config)
        assert score == APPROX(1.0)

    def test_weighted_average(self, config):
        """Verify weights applied correctly when each signal differs."""
        # stars at max → stars_score=1.0, forks=0, watchers=0
        score = score_adoption(100, 0, 0, 100, 100, 100, config=config)
        assert score == APPROX(0.5 * 1.0 + 0.3 * 0.0 + 0.2 * 0.0)

    def test_log_normalisation(self, config):
        """Intermediate value normalised correctly via log."""
        # stars=10, max=100 → log(11)/log(101)
        stars_score = math.log(11) / math.log(101)
        score = score_adoption(10, 0, 0, 100, 1, 1, config=config)
        assert score == APPROX(0.5 * stars_score, rel=1e-4)

    def test_monorepo_dampening_reduces_score(self, config):
        """Same stars but skill_count > 1 should yield a lower adoption score."""
        single = score_adoption(1000, 0, 0, 1000, 1, 1, skill_count=1, config=config)
        multi  = score_adoption(1000, 0, 0, 1000, 1, 1, skill_count=10, config=config)
        assert multi < single

    def test_monorepo_dampening_single_skill_unchanged(self, config):
        """skill_count=1 means no dampening."""
        score_1  = score_adoption(100, 0, 0, 100, 1, 1, skill_count=1, config=config)
        score_nd = score_adoption(100, 0, 0, 100, 1, 1, config=config)  # default skill_count=1
        assert score_1 == APPROX(score_nd)

    def test_score_bounded_zero_to_one(self, config):
        """Score is always in [0, 1]."""
        for stars in [0, 1, 10, 100, 1000]:
            score = score_adoption(stars, stars // 2, stars // 5, 1000, 500, 200, config=config)
            assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------

class TestFreshness:
    def test_commit_today_high_decay(self, config):
        """0 days since last commit → decay = 1.0."""
        score = score_freshness(0, 20, 60, config)
        # decay=1.0, activity=1.0 (≥20 commits), maturity=1.0 (age≥30)
        expected = 0.5 * 1.0 + 0.4 * 1.0 + 0.1 * 1.0
        assert score == APPROX(expected)

    def test_half_life_decay(self, config):
        """30 days since last commit → decay = 0.5."""
        score = score_freshness(30, 0, 0, config)
        # decay=0.5, activity=0.0 (0 commits), maturity=0.0 (age<30)
        expected = 0.5 * 0.5 + 0.4 * 0.0 + 0.1 * 0.0
        assert score == APPROX(expected, rel=1e-5)

    def test_old_commit_low_decay(self, config):
        """180 days since last commit → decay ≈ 0.5^6 ≈ 0.016."""
        score = score_freshness(180, 0, 60, config)
        decay = math.pow(0.5, 180 / 30)  # ≈ 0.016
        expected = 0.5 * decay + 0.4 * 0.0 + 0.1 * 1.0
        assert score == APPROX(expected, rel=1e-5)

    def test_activity_midpoint(self, config):
        """5 commits in 90d → activity = 0.5."""
        score = score_freshness(0, 5, 60, config)
        expected = 0.5 * 1.0 + 0.4 * 0.5 + 0.1 * 1.0
        assert score == APPROX(expected)

    def test_activity_zero_commits(self, config):
        """0 commits in 90d → activity = 0.0."""
        score = score_freshness(0, 0, 60, config)
        expected = 0.5 * 1.0 + 0.4 * 0.0 + 0.1 * 1.0
        assert score == APPROX(expected)

    def test_activity_saturated(self, config):
        """≥20 commits → activity = 1.0."""
        score_20  = score_freshness(0, 20, 60, config)
        score_100 = score_freshness(0, 100, 60, config)
        assert score_20 == APPROX(score_100)

    def test_maturity_bonus_applied(self, config):
        """Repo ≥30d old gets maturity_score=1.0; younger gets 0.0."""
        old   = score_freshness(0, 0, repo_age_days=30, config=config)
        young = score_freshness(0, 0, repo_age_days=29, config=config)
        assert old > young
        assert old == APPROX(0.5 * 1.0 + 0.4 * 0.0 + 0.1 * 1.0)
        assert young == APPROX(0.5 * 1.0 + 0.4 * 0.0 + 0.1 * 0.0)

    def test_score_bounded_zero_to_one(self, config):
        for days, commits, age in [(0, 0, 0), (365, 0, 0), (0, 100, 365)]:
            score = score_freshness(days, commits, age, config)
            assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------

def _doc_score(config, **overrides):
    """Helper: build a full documentation score with sensible defaults."""
    defaults = dict(
        has_frontmatter=True,
        has_name=True,
        has_description=True,
        description_len=50,
        line_count=300,
        has_examples=True,
        has_usage=True,
        has_readme=True,
        has_scripts=True,
        has_references=True,
    )
    defaults.update(overrides)
    return score_documentation(**defaults, config=config)


class TestDocumentation:
    def test_perfect_score(self, config):
        """All signals present, 300+ lines → 1.0."""
        assert _doc_score(config) == APPROX(1.0)

    def test_no_signals(self, config):
        """Nothing present → 0.0."""
        score = score_documentation(
            False, False, False, 0, 0, False, False, False, False, False, config
        )
        assert score == APPROX(0.0)

    def test_description_too_short_not_counted(self, config):
        """description_len < 20 chars → has_description weight not applied."""
        with_short  = _doc_score(config, has_description=True, description_len=10)
        without     = _doc_score(config, has_description=False, description_len=10)
        assert with_short == APPROX(without)

    def test_description_length_threshold(self, config):
        """description_len exactly at min_chars counts; one below does not."""
        w = config["documentation"]["weights"]["has_description"]
        at_min    = _doc_score(config, description_len=20)
        below_min = _doc_score(config, description_len=19)
        assert at_min == APPROX(below_min + w)

    def test_line_count_below_low(self, config):
        """< 50 lines → line_count contribution = 0."""
        w = config["documentation"]["weights"]["line_count"]
        full = _doc_score(config, line_count=300)
        low  = _doc_score(config, line_count=49)
        assert full == APPROX(low + w)

    def test_line_count_at_midpoint(self, config):
        """100 lines → line_count contribution = 0.5 * weight."""
        w = config["documentation"]["weights"]["line_count"]
        full = _doc_score(config, line_count=300)
        mid  = _doc_score(config, line_count=100)
        assert full == APPROX(mid + 0.5 * w)

    def test_line_count_saturated(self, config):
        """≥ 300 lines → same score regardless of additional lines."""
        assert _doc_score(config, line_count=300) == APPROX(_doc_score(config, line_count=999))

    def test_each_binary_signal_contributes(self, config):
        """Toggling each boolean changes the score by its weight."""
        w = config["documentation"]["weights"]
        for field, key in [
            ("has_frontmatter", "has_frontmatter"),
            ("has_name", "has_name"),
            ("has_examples", "has_examples"),
            ("has_usage", "has_usage"),
            ("has_readme", "has_readme"),
            ("has_scripts", "has_scripts"),
            ("has_references", "has_references"),
        ]:
            with_signal    = _doc_score(config, **{field: True})
            without_signal = _doc_score(config, **{field: False})
            assert with_signal == APPROX(without_signal + w[key], rel=1e-5), field


# ---------------------------------------------------------------------------
# Contributors
# ---------------------------------------------------------------------------

class TestContributors:
    def test_zero_contributors(self, config):
        """0 contributors → 0.0."""
        assert score_contributors(0, config) == APPROX(0.0)

    def test_one_contributor(self, config):
        """1 contributor → log(2)/log(11) ≈ 0.289."""
        expected = math.log(2) / math.log(11)
        assert score_contributors(1, config) == APPROX(expected)

    def test_two_contributors(self, config):
        """2 contributors → log(3)/log(11) ≈ 0.458."""
        expected = math.log(3) / math.log(11)
        assert score_contributors(2, config) == APPROX(expected)

    def test_five_contributors(self, config):
        """5 contributors → log(6)/log(11) ≈ 0.748."""
        expected = math.log(6) / math.log(11)
        assert score_contributors(5, config) == APPROX(expected)

    def test_ten_contributors_max(self, config):
        """10 contributors → log(11)/log(11) = 1.0."""
        assert score_contributors(10, config) == APPROX(1.0)

    def test_over_cap_clamped(self, config):
        """More than cap contributors → same as cap."""
        assert score_contributors(100, config) == APPROX(score_contributors(10, config))

    def test_jump_from_one_to_two_larger_than_nine_to_ten(self, config):
        """Log curve: 1→2 jump > 9→10 jump, rewarding external validation."""
        jump_1_2 = score_contributors(2, config) - score_contributors(1, config)
        jump_9_10 = score_contributors(10, config) - score_contributors(9, config)
        assert jump_1_2 > jump_9_10


# ---------------------------------------------------------------------------
# Code Quality
# ---------------------------------------------------------------------------

class TestCodeQuality:
    def test_all_signals_present(self):
        assert score_code_quality(True, True, True, True, True) == APPROX(1.0)

    def test_no_signals_present(self):
        assert score_code_quality(False, False, False, False, False) == APPROX(0.0)

    def test_three_of_five(self):
        assert score_code_quality(True, True, True, False, False) == APPROX(0.6)

    def test_one_of_five(self):
        assert score_code_quality(True, False, False, False, False) == APPROX(0.2)

    def test_each_signal_worth_equal_weight(self):
        """Each signal contributes exactly 0.2 to the score."""
        signals = [
            (True,  False, False, False, False),
            (False, True,  False, False, False),
            (False, False, True,  False, False),
            (False, False, False, True,  False),
            (False, False, False, False, True),
        ]
        for args in signals:
            assert score_code_quality(*args) == APPROX(0.2)
