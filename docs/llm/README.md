# Repo-Local LLM Configuration

This directory contains repository-specific LLM rules and workflows that complement the shared toolkit linked through:

- `.agents/`
- `.claude/`
- `.codex/`
- `.cursor/`
- `.windsurf/`
- `.setup/`

Do not place repo-only customizations inside those linked directories. Doing so would modify the shared toolkit checkout instead of this repository.

## Local Layout

- `docs/llm/toolkit-selection.txt`: curated shared-toolkit profile for this repo
- `docs/llm/references/`: repository-specific reference material such as the USP MBA course map
- `docs/llm/rules/`: repository-specific rules
- `docs/llm/scripts/`: helper scripts that build or refresh local analytical references
- `docs/llm/workflows/`: repository-specific workflows
- `learnings/`: committed trial-and-error knowledge (see `rules/error-driven-learning.md`)

## Shared Toolkit Selection

This repository keeps generic reusable guidance in the shared toolkit and narrows local usage with:

- `docs/llm/toolkit-selection.txt`
- `.cursorignore`
- `.cursorindexingignore`

`docs/llm/toolkit-selection.txt` is the source of truth for which shared rules and workflows this repository wants to prioritize. Update that file when the thesis project changes scope, and then refresh generated exports from the repo root with:

```powershell
.\scripts\sync-llm-configs.ps1
```

Or, in Git Bash:

```bash
./scripts/sync-llm-configs.sh
```

## Repo-Local Guidance

Keep generic guidance in the shared toolkit. Add repo-only guidance here when it would be incorrect or too specific for other projects, for example:

- domain vocabulary for Brazilian public procurement and compliance data
- repository-specific data contracts and staging conventions
- notebook conventions that depend on this repo's folders, datasets, or outputs
- AWS orchestration guidance that reflects this repo's current shell-script ETL and minimal `infra/` baseline
- documentation expectations tied to the thesis deliverables
- USP MBA course-to-method mapping that only applies to this thesis project
- mandatory local security-check gates for code, scripts, workflows, rules, and other operational content
- trial-and-error discoveries captured as committed learnings for cross-session, cross-tool reuse

## Learnings

The `learnings/` directory at the repo root captures trial-and-error discoveries as committed markdown files. Any LLM tool can read these files to skip directly to the correct solution.

- Before starting a task, check `learnings/` for relevant prior discoveries.
- When recovering from trial-and-error, use the `capture-learning` workflow to save the knowledge.
- See `rules/error-driven-learning.md` for the governance rule.

## Rubrics

The shared toolkit includes structured review rubrics for architecture, security, and holistic code review. These are referenced by the `review` and `review-and-fix` workflows:

- `rubrics/architecture.md`
- `rubrics/code-review-checklist.md`
- `rubrics/security.md`

## Mandatory Security Gate

Security review is mandatory for every new or modified code path, script, workflow, rule, skill, or operational document in this repository.

Run:

```bash
./scripts/security_check_this_repo.sh
```

For related work in `dev-tools`, use:

```bash
./scripts/security_check_dev_tools.sh /path/to/dev-tools
```

See:

- `docs/llm/rules/security-check-required.md`
- `docs/llm/workflows/security-check-required.md`

## Example Templates

Templates remain available under `.setup/examples/`, but they are not the source of truth for this repository. Repo-local rules and workflows live under `docs/llm/rules/` and `docs/llm/workflows/`.

If the templates become noisy in the IDE, hide `.setup/examples/` locally instead of deleting anything from the linked toolkit.

## Integrations

The active integrations for this repository are:

- AWS CLI
- GitHub CLI

Jira and Confluence are not used for this project. Their guides may still exist in `.setup/integrations/` because that directory is linked from the shared toolkit, so hide them locally in the IDE if they are distracting.

## Workflow Customization

Do not edit shared toolkit workflows under `.windsurf/workflows/` for repo-only behavior. Use `docs/llm/workflows/` for local workflow guidance, and keep anything reusable in the shared toolkit instead.

## Recommended Local Entry Points

Start with these local files for thesis-specific support:

- `docs/llm/references/usp-mba-course-map.md`
- `docs/llm/rules/course-material-grounding.md`
- `docs/llm/rules/usp-mba-course-context.md`
- `docs/llm/rules/tcc-deliverables-and-argument.md`
- `docs/llm/workflows/course-material-grounding.md`
- `docs/llm/workflows/tcc-method-selection.md`
- `docs/llm/workflows/tcc-analysis-and-writing-sync.md`
- `docs/llm/workflows/refresh-usp-mba-course-context.md`
