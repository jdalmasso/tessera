# Categorization

This document describes how each skill is assigned to one of 15 categories.

## Categories

| # | Category | Covers |
|---|----------|--------|
| 1 | Backend | Server frameworks, APIs, databases, system design, CLIs, SDKs |
| 2 | Frontend & Design | UI components, CSS, design systems, themes, responsive layouts |
| 3 | DevOps & Infra | CI/CD, Docker, K8s, cloud providers, Terraform, deployment |
| 4 | Security | Audits, OWASP, vulnerability scanning, compliance, pen testing |
| 5 | Testing & QA | Unit/integration/e2e testing, debugging, test automation |
| 6 | Data & AI | ML pipelines, model training, data processing, analytics |
| 7 | Research | Literature review, paper analysis, deep research, fact-checking, academic |
| 8 | Documentation | Docs generation, READMEs, changelogs, API docs, technical writing |
| 9 | Productivity & Workflow | Project management, Agile, Git workflows, task automation |
| 10 | Consulting & Strategy | Market research, competitive analysis, frameworks, due diligence, advisory |
| 11 | Back-Office | HR, finance, legal, internal comms, compliance operations |
| 12 | Marketing & Content | SEO, copywriting, social media, newsletters, content strategy |
| 13 | Creative & Media | Art, music, video, audio, generative content |
| 14 | Integrations | MCP servers, API connectors, third-party service automation |
| 15 | Other | Fallback for skills that don't match any above |

## Cascade Logic

Each skill is assigned to exactly one category via a six-level cascade. Each level fires only if the previous produced no match. First match wins.

1. **Explicit frontmatter** — `category` or `tags` field in SKILL.md YAML frontmatter maps directly to a category
2. **Keyword match on name + description** — SKILL.md `name` and `description` fields are scanned against per-category keyword lists
3. **GitHub repo topics** — repo topics are mapped to categories
4. **Directory path heuristics** — for monorepos, the parent directory name is used (e.g. `engineering/` → Backend)
5. **README keyword scan** — first 500 characters of README.md are scanned
6. **Default to Other** — fallback when no earlier level produced a match

When multiple categories match at the same cascade level, the category with the most keyword matches wins. When two categories tie on keyword count at the same cascade level, the one appearing earlier in `config/categories.yaml` wins.

## Representative Keywords

The following lists are illustrative rather than exhaustive. Each category matches against a broader set of terms defined in `config/categories.yaml`.

### Backend
`api`, `rest`, `graphql`, `fastapi`, `django`, `flask`, `database`, `postgresql`, `redis`, `grpc`, `cli`, `sdk`

### Frontend & Design
`react`, `vue`, `svelte`, `css`, `tailwind`, `ui`, `component`, `design-system`, `responsive`, `nextjs`

### DevOps & Infra
`docker`, `kubernetes`, `terraform`, `ci`, `github-actions`, `aws`, `gcp`, `azure`, `deployment`, `helm`

### Security
`owasp`, `vulnerability`, `audit`, `penetration`, `compliance`, `sast`, `authentication`, `encryption`

### Testing & QA
`pytest`, `jest`, `selenium`, `e2e`, `unit-test`, `coverage`, `debugging`, `qa`

### Data & AI
`ml`, `pytorch`, `tensorflow`, `pandas`, `etl`, `pipeline`, `analytics`, `llm`, `embedding`, `vector`

### Research
`literature`, `paper`, `academic`, `survey`, `analysis`, `fact-checking`, `citation`

### Documentation
`readme`, `changelog`, `docstring`, `api-docs`, `technical-writing`, `mkdocs`, `sphinx`

### Productivity & Workflow
`agile`, `jira`, `git`, `automation`, `workflow`, `project-management`, `notion`

### Consulting & Strategy
`market-research`, `competitive`, `due-diligence`, `framework`, `advisory`, `strategy`

### Back-Office
`hr`, `payroll`, `legal`, `compliance`, `finance`, `internal-comms`, `onboarding`

### Marketing & Content
`seo`, `copywriting`, `social-media`, `newsletter`, `content-strategy`, `blog`

### Creative & Media
`art`, `music`, `video`, `audio`, `generative`, `creative-writing`, `image`

### Integrations
`mcp`, `connector`, `webhook`, `api-integration`, `zapier`, `automation`, `third-party`

### Other
Fallback category. No keywords are matched; a skill lands here only when all earlier cascade levels produce no result.
