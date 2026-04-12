# Categorization

> **Stub** — full content added in Phase 5.

This document describes how each skill is assigned to one of 15 categories.

## Categories

1. Backend
2. Frontend & Design
3. DevOps & Infra
4. Security
5. Testing & QA
6. Data & AI
7. Research
8. Documentation
9. Productivity & Workflow
10. Consulting & Strategy
11. Back-Office
12. Marketing & Content
13. Creative & Media
14. Integrations
15. Other

## Cascade Logic

Each skill is categorized via a six-level cascade (first match wins):

1. Explicit SKILL.md frontmatter `category` or `tags` field
2. Keyword match on SKILL.md description + name
3. GitHub repo topics
4. Directory path heuristics (monorepos)
5. README keyword scan (first 500 characters)
6. Default to **Other**

When multiple categories match at the same level, the category with the most keyword matches wins.

## Keyword Lists

See `config/categories.yaml` for the full keyword lists per category.

_Full narrative documentation to be completed in Phase 5._
