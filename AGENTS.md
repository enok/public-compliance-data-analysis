<!-- BEGIN SYNC-TOOL-CONFIGS RULES -->
## Shared Toolkit Rules

> Auto-generated from `.windsurf/rules/` by `sync-tool-configs.sh`.
> Curated by `docs/llm/toolkit-selection.txt` for this repository.
>
> When `.cursor/` is linked to the shared toolkit, keep shared `.cursor/rules/` generic and use
> `.cursorignore` plus `.cursorindexingignore` to hide non-selected items locally.

### Selected Rules Files
- `.windsurf/rules/analysis-artifact-governance.md`
- `.windsurf/rules/analytical-dataset-governance.md`
- `.windsurf/rules/analytics-reproducibility.md`
- `.windsurf/rules/aws-data-platform-ops.md`
- `.windsurf/rules/aws-airflow-terraform.md`
- `.windsurf/rules/best-practices.md`
- `.windsurf/rules/bilingual-doc-sync.md`
- `.windsurf/rules/code-rules.md`
- `.windsurf/rules/command-safety.md`
- `.windsurf/rules/data-artifact-hygiene.md`
- `.windsurf/rules/data-governance.md`
- `.windsurf/rules/data-pipeline-contracts.md`
- `.windsurf/rules/git-conventions.md`
- `.windsurf/rules/jupyter-notebook-hygiene.md`
- `.windsurf/rules/latex-in-notebooks.md`
- `.windsurf/rules/ml-evaluation-and-experimentation.md`
- `.windsurf/rules/ml-experiment-discipline.md`
- `.windsurf/rules/ml-experiment-governance.md`
- `.windsurf/rules/ml-experiment-rigor.md`
- `.windsurf/rules/multi-agent-orchestration.md`
- `.windsurf/rules/notebook-engineering.md`
- `.windsurf/rules/notebook-reproducibility.md`
- `.windsurf/rules/operational-doc-required.md`
- `.windsurf/rules/python-best-practices.md`
- `.windsurf/rules/research-rigor.md`
- `.windsurf/rules/repository-context-layers.md`
- `.windsurf/rules/review-and-fix-multi-agent.md`
- `.windsurf/rules/security.md`
- `.windsurf/rules/testing.md`
- `.windsurf/rules/error-driven-learning.md`
- `.windsurf/rules/evidence-based-reporting.md`
- `.windsurf/rules/security-check-required.md`

### Repo-Local Rules Files
- `docs/llm/rules/aws-airflow-terraform.md`
- `docs/llm/rules/course-material-grounding.md`
- `docs/llm/rules/data-pipeline-boundaries.md`
- `docs/llm/rules/documentation-and-governance.md`
- `docs/llm/rules/project-overview.md`
- `docs/llm/rules/security-check-required.md`
- `docs/llm/rules/tcc-deliverables-and-argument.md`
- `docs/llm/rules/usp-mba-course-context.md`


### Selected Workflow Files
- `.windsurf/workflows/aws-data-pipeline-ops.md`
- `.windsurf/workflows/aws-data-platform-ops.md`
- `.windsurf/workflows/aws-airflow-terraform-change.md`
- `.windsurf/workflows/bilingual-doc-sync.md`
- `.windsurf/workflows/data-pipeline-change.md`
- `.windsurf/workflows/dataset-onboarding.md`
- `.windsurf/workflows/document-creation.md`
- `.windsurf/workflows/environment-diagnose.md`
- `.windsurf/workflows/experiment-result-update.md`
- `.windsurf/workflows/ml-experiment.md`
- `.windsurf/workflows/ml-experiment-update.md`
- `.windsurf/workflows/notebook-analysis.md`
- `.windsurf/workflows/notebook-analysis-update.md`
- `.windsurf/workflows/notebook-latex-polish.md`
- `.windsurf/workflows/notebook-to-script.md`
- `.windsurf/workflows/paired-doc-sync.md`
- `.windsurf/workflows/pre-pr-check.md`
- `.windsurf/workflows/project-discovery.md`
- `.windsurf/workflows/research-analysis-cycle.md`
- `.windsurf/workflows/review.md`
- `.windsurf/workflows/review-and-fix.md`
- `.windsurf/workflows/run-tests.md`
- `.windsurf/workflows/capture-learning.md`
- `.windsurf/workflows/security-check-required.md`
- `.windsurf/workflows/security-report.md`
- `.windsurf/workflows/translation-sync.md`
- `.windsurf/workflows/update-docs.md`

### Repo-Local Workflow Files
- `docs/llm/workflows/aws-airflow-terraform-change.md`
- `docs/llm/workflows/course-material-grounding.md`
- `docs/llm/workflows/documentation-sync.md`
- `docs/llm/workflows/pipeline-change.md`
- `docs/llm/workflows/refresh-usp-mba-course-context.md`
- `docs/llm/workflows/security-check-required.md`
- `docs/llm/workflows/tcc-analysis-and-writing-sync.md`
- `docs/llm/workflows/tcc-method-selection.md`


### Tool Config Locations

| Tool | Location | Format |
|------|----------|--------|
| Windsurf / canonical | `.windsurf/rules/` or `rules/` | Individual files (source of truth) |
| Cursor | `.cursor/rules/`, `.cursor/agents/` | Shared rules auto-generated; project filtering happens via local ignore files |
| Claude Code | `CLAUDE.md`, `.claude/agents/` | Concatenated repo-local export from shared selection plus `docs/llm/` |
| GitHub Copilot | `.github/copilot-instructions.md` | Concatenated repo-local export from shared selection plus `docs/llm/` |
| Codex / Generic | `AGENTS.md`, optional `.codex/skills/`, optional `.codex/agents/` | `AGENTS.md` is primary; `.codex/` can expose Codex-oriented skill and agent mirrors when present |

To regenerate local exports after editing shared rules, `docs/llm/toolkit-selection.txt`, or repo-local `docs/llm/` files:
```bash
./scripts/sync-llm-configs.sh
```
On Windows PowerShell:
```powershell
./scripts/sync-llm-configs.ps1
```
<!-- END SYNC-TOOL-CONFIGS RULES -->

---
# Repository LLM Context

Toolkit bootstrap is complete.

Shared toolkit content is linked through:

- `.agents/`
- `.claude/`
- `.codex/`
- `.cursor/`
- `.windsurf/`
- `.setup/`

Do not place repository-only customizations inside those linked directories.

## Repo-Local LLM Configuration

Repository-specific LLM guidance lives in committed local files:

- `docs/llm/README.md`
- `docs/llm/references/`
- `docs/llm/toolkit-selection.txt`
- `docs/llm/rules/`
- `docs/llm/workflows/`
- `learnings/`
- `.cursorignore`
- `.cursorindexingignore`

Use the shared toolkit for generic rules and workflows. Use `docs/llm/` for project-specific context, conventions, and workflow guidance.

## Thesis-Specific Local Guidance

When a task touches the MBA thesis framing, method choice, chapter structure, or advisor-facing deliverables, start with these repo-local files before improvising:

- `docs/llm/references/usp-mba-course-map.md`
- `docs/llm/rules/usp-mba-course-context.md`
- `docs/llm/rules/tcc-deliverables-and-argument.md`
- `docs/llm/workflows/tcc-method-selection.md`
- `docs/llm/workflows/tcc-analysis-and-writing-sync.md`
- `docs/llm/workflows/refresh-usp-mba-course-context.md`

Use the shared `research-thesis-support` skill for reusable research discipline and the repo-local course map for USP MBA-specific method framing.

## Architecture Summary

- `config/` defines ingestion contracts and Silver schema expectations.
- `src/ingestion/` implements Bronze ingestion for IBGE and Transparency Portal sources.
- `src/processing/` implements Silver normalization and Gold aggregations.
- `src/analysis/` contains notebook-facing data access helpers, including localized loaders.
- `scripts/` is the primary orchestration surface in this repo for Bronze, Silver, and Gold execution.
- `dags/` is not the primary orchestration layer today; treat shell scripts as the source of truth unless real DAG code is added.
- `infra/` currently provisions the S3-backed data lake foundation and supporting AWS configuration, not a full MWAA or SageMaker stack.
- `notebooks/` contains bilingual EDA, statistics, machine learning, and clustering notebooks plus notebook usage guides.
- `tests/` contains pytest coverage for ingestion, processing, and analysis code.
- `docs/` contains technical and research documentation for Bronze, Silver, Gold, coverage reports, and translations.

## Key Services And Components

- `TransparencyIngestor` in `src/ingestion/transparency_client.py` handles paginated Transparency Portal ingestion, API key resolution, metadata checkpoints, and skip logic.
- `GoldTransformer` in `src/processing/gold_transformer.py` produces analysis-ready Gold outputs from Silver datasets.
- `scripts/01_bronze_ingestion.sh`, `scripts/02_silver_transformation.sh`, and `scripts/03_gold_transformation.sh` are the implemented stage entry points for local and scripted pipeline execution.
- `src/analysis/data_loader.py` and `src/analysis/pt_br_loader.py` power the English and localized notebook analysis flows.
- `infra/main.tf` provisions the S3-backed data lake foundation and related AWS resources.

## Domain Terms

- Bronze: raw source-aligned ingestion data with auditability.
- Silver: normalized and schema-aware datasets ready for downstream processing.
- Gold: analysis-ready aggregates and derived outputs.
- Municipality code: the stable municipal identifier used to join datasets across layers.
- Federal transfers: treatment-side spending records from the Transparency Portal.
- Sanctions datasets: compliance and integrity registries such as CEIS, CNEP, and CEPIM.

## Learnings

The `learnings/` directory contains committed markdown files that capture trial-and-error discoveries from LLM-assisted development. Any LLM tool can read these files to skip directly to the correct solution instead of repeating failed approaches.

Before starting a task, check whether `learnings/` contains anything relevant to the current work. When recovering from trial-and-error (two or more failed approaches before finding the right one), capture the discovery using the `capture-learning` workflow.

See `rules/error-driven-learning.md` for the governance rule and `workflows/capture-learning.md` for the step-by-step process.

## Rubrics

The shared toolkit includes structured review rubrics that guide code review quality:

- `rubrics/architecture.md` — layering, boundaries, separation of concerns, transaction safety
- `rubrics/code-review-checklist.md` — holistic review categories, evidence rules, and response format (Red/Yellow/Green)
- `rubrics/security.md` — injection, auth, secrets, web security, dependencies, PII, threat model

These rubrics are referenced by the `review` and `review-and-fix` workflows for structured, evidence-based review findings.

## Repository Conventions

- Keep repository-specific LLM configuration in English.
- Keep technical implementation guidance in English first.
- Update existing `pt-BR` companion docs when they are maintained in parallel with English docs.
- Treat notebook pairs and translated analysis helpers as maintained deliverables, not optional extras.
- Treat `config/*.json` files as system contracts, not casual data files.
- Preserve medallion boundaries between Bronze, Silver, and Gold.
- Prefer idempotent, testable, metadata-aware pipeline changes.
- Prefer reusable loaders and transformations in `src/` over notebook-only logic when code will be reused.
- Update docs and tests when pipeline behavior, schema shape, or operational commands change.
- Treat security review as mandatory for every new or modified code path, script, workflow, rule, skill, or operational document.
- Run [`scripts/security_check_this_repo.sh`](/mnt/hgfs/shared/data-science/tcc/code/public-compliance-data-analysis/scripts/security_check_this_repo.sh) before closing work on this repository.
- When working in `dev-tools`, run [`scripts/security_check_dev_tools.sh`](/mnt/hgfs/shared/data-science/tcc/code/public-compliance-data-analysis/scripts/security_check_dev_tools.sh) against the target checkout before closing work there.
- Do not commit secrets, credentials, or datasets.

## Shared Rule Selection

Use `docs/llm/toolkit-selection.txt` as the curated shared-toolkit profile for this repository.

The exact shared-file list lives in `docs/llm/toolkit-selection.txt`. Use that file as the single source of truth for what stays visible in repo-local exports.

The curated profile for this repository intentionally emphasizes:

- data-pipeline contracts, data governance, analytical artifact hygiene, and AWS data platform operations
- notebook engineering, Jupyter hygiene, notebook-to-script conversion, and LaTeX support inside notebooks
- ML evaluation and experiment governance for end-to-end data science work
- bilingual documentation maintenance for English and `pt-BR` deliverables
- multi-agent review, testing, security, and documentation workflows for day-to-day repository work
- error-driven learning and committed knowledge capture across LLM sessions
- structured review rubrics for architecture, security, and holistic code review

Treat these categories as intentionally deprioritized unless the task clearly requires them:

- generic API-surface guidance that is less relevant than pipeline contract guidance in this repository
- frontend, Java, and JS/TS implementation guidance
- release-only rules that do not map to the current local workflow
- migration or web-application feature workflows that do not match this repository's primary shape
- Jira and Confluence guidance, since this repository currently uses GitHub and AWS CLI but not Jira or Confluence

## Integrations

Integration guides remain available under `.setup/integrations/`.
GitHub CLI and AWS CLI are the active integrations for this repository.
Jira and Confluence are not used here; hide those linked guides locally instead of editing the shared toolkit.
