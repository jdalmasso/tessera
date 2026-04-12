"""
GitHub-specific dataclasses for raw signal collection.

These are populated by signals/github/client.py and consumed by
surfaces/skills_leaderboard/pipeline.py to build entities and scores.
They are intentionally generic — they know nothing about Skills specifically.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RepoSignals:
    """
    Raw signals collected from GitHub's repository and commit APIs for a single repo.

    Populated via:
      GET /repos/{owner}/{repo}            — metadata, stars, topics, license
      GET /repos/{owner}/{repo}/commits    — commit windows for velocity/freshness
      GET /repos/{owner}/{repo}/contributors — contributor count
      GET /repos/{owner}/{repo}/contents/  — structural checks (workflows, tests, etc.)
    """

    # Identity
    full_name: str                      # "owner/repo"
    default_branch: str

    # Repo state
    is_fork: bool
    is_archived: bool
    created_at: str                     # ISO-8601 UTC
    pushed_at: str                      # ISO-8601 UTC — last push

    # Adoption signals
    stars: int
    forks: int
    watchers: int

    # Topics (used for categorization and code quality signal)
    topics: list[str] = field(default_factory=list)

    # License
    has_license: bool = False
    license_name: Optional[str] = None

    # Contributor count (repo-level in v0.1)
    contributor_count: int = 0

    # Structural checks (code quality signals)
    has_workflows: bool = False         # .github/workflows/ exists
    has_tests: bool = False             # tests/ directory or test files present
    has_gitignore: bool = False         # .gitignore exists

    # Commit windows (velocity and freshness signals)
    commit_count_30d: int = 0           # commits in the last 30 days
    commit_count_prev_30d: int = 0      # commits in the 30 days before that
    commit_count_90d: int = 0           # commits in the last 90 days
    unique_commit_weeks_90d: int = 0    # unique ISO weeks with ≥1 commit in last 90d


@dataclass
class SkillFileSignals:
    """
    Raw signals collected from a single SKILL.md file (and its surrounding directory).

    For monorepos, one SkillFileSignals is produced per discovered SKILL.md path.
    `skill_path` is relative to the repo root (e.g. "skills/backend/SKILL.md").
    """

    # Identity
    repo_full_name: str                 # "owner/repo"
    skill_path: str                     # path to SKILL.md within the repo

    # Raw content
    content: str
    char_count: int
    line_count: int                     # non-empty lines

    # Frontmatter presence and fields
    has_frontmatter: bool = False
    frontmatter_name: Optional[str] = None
    frontmatter_description: Optional[str] = None
    frontmatter_category: Optional[str] = None
    frontmatter_tags: list[str] = field(default_factory=list)

    # Section detection (documentation signals)
    has_usage_section: bool = False
    has_examples_section: bool = False

    # Sibling file/directory checks (documentation signals)
    has_readme: bool = False            # README.md exists alongside SKILL.md
    has_scripts_dir: bool = False       # scripts/ directory exists
    has_references_dir: bool = False    # references/ directory exists
