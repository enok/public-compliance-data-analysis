# Project Rules (public-compliance-data-analysis)

> Auto-generated from `.windsurf/rules/` by `sync-tool-configs.sh`.
> Curated by `docs/llm/toolkit-selection.txt` for this repository.


---


# Analytics Discipline

Analytical work should be repeatable by another engineer without hidden notebook state, missing environment details, or undocumented data assumptions.

## Artifact Governance

- Define the source of truth for every artifact pair: English plus translated docs, notebook plus exported report, or code plus generated figure.
- When a paired artifact changes, update both sides or leave an explicit note describing the remaining drift and where it will be resolved.
- Prefer text-friendly committed summaries (Markdown, JSON, CSV, code) over opaque binary-only outputs when preserving results or review evidence.
- Keep filenames, numbering, and headings aligned with pipeline stages or analysis sequence so reviewers can map outputs back to code and data.
- Charts, tables, and conclusions should include enough context to reproduce them: dataset, time window, aggregation level, filters, and metric definitions.
- Do not commit heavy notebook outputs or generated datasets unless the repository already treats those artifacts as source-of-truth inputs.

## Dataset Governance

- Every analytical dataset should declare its entity grain, time grain, primary keys, and intended use before new features are added.
- Keep feature engineering aligned with the question being answered. Regression, classification, and clustering datasets may need different grains and filtering rules.
- Derived columns must be traceable to committed transformations, documented notebook cells, or config-driven rules.
- Name features so units and time meaning stay obvious. Prefer columns like `income_change_pct` or `sanctions_per_100k` over ambiguous short names.
- When aggregating from one entity level to another, validate that the numerator, denominator, and weighting strategy still match the analytical objective.
- Treat translation maps and loader dataset maps as part of the analytical contract when localized notebooks or reports depend on them.
- Add or update tests when new analytical datasets, feature columns, or dataset aliases are introduced.
- If a feature can leak future information or post-outcome signals, exclude it or document the reason it is still valid for the task.

## Data Artifact Hygiene

- Do not commit generated datasets, temporary exports, cache files, credentials, or local-only environment state.
- Large plots, tables, and reports should be committed only when they are intentionally reviewed artifacts and their regeneration path is documented.
- Prefer reproducible source code, configs, and small markdown summaries over opaque binary artifacts.
- Keep notebook outputs focused. Avoid committing noisy cell output that obscures the logic or inflates diffs unless the output itself is the reviewed artifact.
- Never hardcode local machine paths, personal buckets, or ad hoc file drops inside notebooks or scripts.
- Logs and audit artifacts should use stable names and timestamps so reruns are easy to compare.
- When the repo contains sensitive public-sector or compliance data, double-check masking, row-level exposure, and screenshots before committing anything derived from it.

## Reproducibility

- Set explicit random seeds for sampling, splitting, and model training when randomness is involved.
- Record the dataset snapshot, source path, or extraction date that supports the analysis.
- Make environment assumptions explicit: Python version, required packages, cloud profile, local paths, or feature flags.
- If logic is reused across notebooks, scripts, or production code, extract it into `src/`, a shared utility, or a documented pipeline step.
- Treat plots, tables, and markdown conclusions as derived artifacts that should be reproducible from committed code.
- Do not hand-edit generated tables, cached exports, or audit logs unless the task is specifically about repairing a broken artifact.
- When a result depends on filtering or exclusions, document the rule next to the result.

## Statistical Honesty

- State sample-size limitations and uncertainty clearly.
- Separate exploratory findings from confirmed conclusions.
- If a notebook changes project-facing conclusions, update the related docs in the same change.

---


# AWS Airflow Terraform

Use this rule when a task involves provisioning or evolving AWS-hosted Apache Airflow through Terraform or OpenTofu.

## Scope The Decision First

- Confirm whether the task is:
  - infrastructure-only
  - DAG introduction
  - ETL migration into Airflow
  - operational documentation only
- Keep platform provisioning concerns separate from pipeline business logic.

## Service Choice

- Choose the AWS Airflow hosting model explicitly:
  - Amazon MWAA
  - self-managed Airflow on ECS, EKS, or EC2
- Document the constraint that drives the choice, such as executor needs, plugin requirements, network model, cost envelope, or operational control.

## Terraform Responsibilities

- Use Terraform or OpenTofu for:
  - Airflow environment resources
  - DAG, plugin, requirements, and startup-script storage
  - IAM roles and policies
  - networking and security groups
  - logging, encryption, and observability dependencies
- Do not put business logic or credentials into Terraform.

## Code Boundaries

- DAG Python belongs in an Airflow code location such as `dags/`.
- Reusable pipeline logic belongs in application code or scripts, not copied into Terraform templates.
- Runtime secrets and connection material belong in managed secret stores or runtime configuration systems, not in source-controlled IaC.

## Terraform Structure

- Keep environment-specific values in variables, tfvars, or environment folders instead of hardcoding account, region, subnet, bucket, or role details.
- Introduce `versions.tf`, `outputs.tf`, and provider constraints when the infrastructure surface is more than trivial.
- Prefer explicit outputs for environment names, ARNs, bucket or prefix locations, log destinations, and network identifiers that operators or automation will need.
- Do not commit local state files, plan files, generated secrets, or credential artifacts.

## Security And Operations

- Apply least-privilege IAM to Airflow execution roles and supporting AWS integrations.
- Prefer private networking with explicit egress assumptions.
- Prefer encryption-at-rest and encryption-in-transit defaults for buckets, logs, and secret stores.
- Make logging and observability explicit enough to diagnose DAG parse failures, scheduler failures, and task failures.

## Change Discipline

- Preserve output contracts while moving scheduling or infrastructure into Airflow.
- Document how DAG deployment, Terraform deployment, and runtime operations fit together.

---


# AWS Data Platform Operations

Use this rule when a task involves AWS CLI commands, S3 objects, orchestration services, managed compute or analytics services, or infrastructure-backed pipeline operations.

## Verify Context First

- Confirm the AWS profile, region, account, bucket, environment, and target prefix before running write operations.
- Distinguish between inspect-only commands and commands that trigger pipelines, create objects, or alter infrastructure.
- Prefer repo scripts or documented entry points when they exist instead of ad hoc command sequences.

## Read Before Write

- Use list, head, or describe commands first when checking environment state.
- Narrow writes to explicit prefixes or resources instead of broad bucket-level actions.
- Do not assume the local cache reflects S3 truth. Verify current object presence and metadata when it matters.

## Operational Discipline

- Make expensive, long-running, or potentially destructive actions explicit before running them.
- For DAG or pipeline runs, note prerequisites, expected downstream writes, and how success will be verified.
- Preserve logs, report paths, or evidence links when the operation matters for reproducibility or audit.

## Follow-Through

- After an operational task, capture the result in a committed runbook or status doc if the workflow is likely to repeat.
- If a manual operational pattern keeps recurring, turn it into a script or documented workflow.

---


# Software Engineering & Architecture Best Practices

Universal principles that apply to every project regardless of tech stack. These supplement `code-rules.md` with higher-level guidance.

## Software Engineering Principles

### SOLID
- **Single Responsibility**: Each class/module/function does one thing well. If you can't describe its purpose in one sentence, split it.
- **Open/Closed**: Open for extension, closed for modification. Prefer adding new classes/strategies over modifying existing switch/if chains.
- **Liskov Substitution**: Subtypes must be substitutable for their base types without breaking behavior.
- **Interface Segregation**: Many small, focused interfaces over one large one. Clients shouldn't depend on methods they don't use.
- **Dependency Inversion**: Depend on abstractions, not concretions. High-level modules should not depend on low-level modules.

### Clean Code
- **Naming**: Names reveal intent. `calculateMonthlyRevenue()` not `calc()`. No abbreviations unless universally understood (`id`, `url`, `http`).
- **Functions**: Small (ideally < 20 lines), do one thing, one level of abstraction per function. If a function needs a comment to explain what it does, rename it.
- **No magic numbers/strings**: Extract to named constants. `MAX_RETRY_ATTEMPTS = 3` not bare `3`.
- **Early returns**: Reduce nesting. Validate and fail early instead of deep if/else chains.
- **DRY**: Don't Repeat Yourself — but don't over-abstract either. Duplication is better than the wrong abstraction (Rule of Three: abstract on the third occurrence).
- **Boy Scout Rule**: Leave code cleaner than you found it — but only within the scope of your task.

### Error Handling
- **Fail fast**: Validate inputs at the boundary. Don't let bad data propagate deep into the system.
- **Specific exceptions**: Catch the narrowest exception type possible. Never catch `Exception`/`Throwable` unless re-throwing.
- **No swallowed exceptions**: Every catch block must log, re-throw, or handle meaningfully.
- **Error messages**: Include context (what failed, what was expected, what was received). Never expose internal details to clients.

### Defensive Programming
- **Validate all inputs** at system boundaries (API endpoints, event handlers, message consumers).
- **Null safety**: Check for null/None/undefined before use. Prefer Optional/Maybe types where available.
- **Immutability**: Prefer immutable data structures. Mutable shared state is the #1 source of concurrency bugs.
- **Boundary conditions**: Test zero, one, many, negative, max-value, empty string, empty collection.

## Architecture Best Practices

### Separation of Concerns
- **Layered architecture**: Controller/Handler → Service/Business Logic → Repository/Data Access. Never skip layers.
- **Thin controllers**: Controllers parse input and delegate. No business logic in controllers.
- **Service layer**: All business logic lives here. Services are the only layer that knows the business rules.
- **Data access layer**: Isolate all database/external service calls. Services never build queries directly.

### API Design
- **Idempotency**: GET, PUT, DELETE should be idempotent. POST creates new resources.
- **Consistent error responses**: Standard error format across all endpoints (`{code, message, details}`).
- **Versioning**: Plan for API evolution. Breaking changes require version bumps or feature flags.
- **Input validation**: Validate at the API boundary. Return 400 for bad input, not 500.

### Data Design
- **Single source of truth**: Every piece of data has exactly one authoritative owner.
- **Schema evolution**: Design for forward/backward compatibility. Adding fields is safe; renaming/removing is breaking.
- **TTL/retention**: Data should have an expiration strategy. Don't store data indefinitely without a reason.
- **Composite keys**: Prefer natural composite keys over auto-increment when the combination is naturally unique.

### Resilience
- **Timeouts**: Every external call must have a timeout. No unbounded waits.
- **Retries with backoff**: Retry transient failures with exponential backoff + jitter. Cap max retries.
- **Circuit breakers**: For frequently-failing dependencies, stop calling and fail fast.
- **Dead letter queues**: For async processing, route failed messages to DLQ for later investigation.
- **Graceful degradation**: If a non-critical dependency fails, serve a degraded response rather than failing entirely.

### Observability
- **Structured logging**: Log in a parseable format (JSON or structured fields). Include correlation IDs.
- **Correlation IDs**: Propagate a request/trace ID across all services for cross-service debugging.
- **Metrics**: Track latency, error rates, throughput at every service boundary.
- **Alerting**: Alert on symptoms (error rate, latency), not causes. Avoid alert fatigue.

### Coupling & Cohesion
- **Low coupling**: Services/modules should be independently deployable and testable. Changes in one should not cascade.
- **High cohesion**: Related functionality lives together. If two things always change together, they belong together.
- **Contract-first**: Define interfaces/schemas before implementation. Changes to contracts require coordination.
- **Event-driven**: For cross-service communication, prefer events/messages over synchronous calls where possible.

## Anti-Patterns to Avoid

- **God class/function**: One class doing everything. Split by responsibility.
- **Premature optimization**: Measure first. Optimize only proven bottlenecks.
- **Premature abstraction**: Don't abstract until you have 3+ concrete cases. Wrong abstractions are worse than duplication.
- **Distributed monolith**: Microservices that must be deployed together. If services share a database or require synchronized deploys, they're a monolith.
- **Stringly typed**: Using raw strings where enums, constants, or typed objects would prevent errors.
- **Feature envy**: A method that uses more data from another class than its own. Move the method to where the data lives.
- **Shotgun surgery**: One change requires edits in many unrelated places. Consolidate the scattered logic.

---


# Bilingual Documentation Sync

When a repository maintains documentation or notebooks in more than one language, language drift becomes a product risk.

## Source Of Truth

- Keep the primary technical source in English unless the project documents a different policy.
- If a maintained localized counterpart exists, update it in the same change or explicitly note that it is temporarily behind.
- Do not silently change domain terms in one language only.

## Translation Scope

- Translate narrative markdown, titles, captions, and user-facing terminology.
- Keep code, dataset identifiers, schema keys, and command names stable unless the project intentionally provides a translated access layer.
- If the project uses translation dictionaries for dataset or column names, update those mappings together with the docs that depend on them.

## Review Checklist

When a change affects docs or notebooks, check:

- paired `README` or localized documents
- translated notebook markdown
- glossary or translation maps
- screenshots, charts, and table labels
- cross-links between language variants

## Acceptable Drift

If the localized version must lag temporarily, say so directly in the change summary or the document itself. Do not leave readers guessing whether the mismatch is intentional.

---


# CI Pipeline Feedback Loop — Continuous Improvement

## Principle

Every CI pipeline failure is an opportunity to improve the review process. When a PR pipeline fails, the root cause must be analyzed and the corresponding rule or workflow must be updated so that **future reviews catch the same class of error before the PR is even opened**.

## Mandatory Process

When validating a PR pipeline execution and encountering a failure:

1. **Diagnose the root cause** — identify exactly what caused the CI check to fail (e.g., missing config, lint violation, missing import, stale types, etc.).

2. **Fix the immediate issue** — apply the minimal fix to make CI green.

3. **Identify the prevention rule** — ask: *"What rule or checklist item, if it existed during the code review phase, would have caught this error before the PR was opened?"*

4. **Update the appropriate rule or workflow file:**
   - SQL/migration errors → update the review workflow or create a dedicated rule.
   - Lint/style errors → update the review workflow's code standards checklist.
   - Missing permissions → update security rules or review workflow.
   - Test failures → update testing rules or review workflow.
   - Build/compile errors → update code rules or backend/frontend rules.
   - New category → create a new rule file if no existing file covers the error class.

5. **Propagate the change** — if the rule applies to multiple repositories, update the rule in ALL relevant repos.

6. **Document** — add the CI failure and the rule update to the review or PR documentation.

## Key Rule

**Never let the same class of CI error happen twice.** If CI catches it once, the review workflow must catch it forever after.

---


# Code Rules

General coding discipline and change management. These rules apply to every task regardless of tech stack.

## Development Guidelines

- Follow the user's requirements carefully & to the letter.
- Always write correct, best-practice, DRY (Don't Repeat Yourself), bug-free, fully functional code.
- Fully implement all requested functionality — no TODOs, no placeholders, no missing pieces.
- **Solve the root cause, not the symptom.**
- Minimal and precise changes — only touch what's necessary.
- No unnecessary comments; document in the same style as the file you are modifying.

## Source Code Verification

- **Always read the actual source before modifying.** Never guess method signatures, return types, or field names.
- If a class/module is not in the local workspace (e.g., from a shared library, monorepo package, or external dependency), check **local caches first** (`.m2/repository`, `node_modules`, `vendor/`, `site-packages/`, etc.) before searching remotely or asking the user.
- **Never assume method signatures** — overloaded methods, inherited members, and final methods are common sources of failures when guessed.

## Dependency Injection Configuration

- When adding or modifying beans/services, **always verify the DI configuration** (Spring XML, Guice modules, Angular modules, etc.).
- Config-based wiring (XML, YAML, property files) is common in legacy and enterprise projects — do not assume annotation-based scanning.
- If the project uses config-based DI, check the config files before adding or changing any bean.

## Verify After Changes

- After any non-trivial edit, **verify** before claiming done: run affected tests or scripts, confirm exit codes, or inspect artifacts (e.g. reparse points, generated files).
- Prefer actually executing checks over telling the user what to run when the environment allows it.

## Change Discipline

- Match existing code style (formatting, naming, indentation).
- Every changed line should trace directly to the user's request.
- No features beyond what was asked.
- No abstractions for single-use code.
- The test: Would a senior engineer say this is overcomplicated? If yes, simplify.

## Line Endings — LF Only

- **All files MUST use LF (`\n`) line endings.** Never commit CRLF (`\r\n`).
- On Windows, Git's `core.autocrlf` can silently introduce CRLF. Always verify before committing.
- Every repo SHOULD have a `.gitattributes` file at the root enforcing LF:

```gitattributes
# Enforce LF line endings for all text files
* text=auto eol=lf
```

- **Before committing**, check for CRLF in changed files:

```bash
# Find CRLF in staged files
git diff --cached --name-only | xargs grep -Prl '\r$' 2>/dev/null
# Fix: convert CRLF → LF
git diff --cached --name-only | xargs sed -i 's/\r$//'
```

- If a repo lacks `.gitattributes`, add one as part of **Category 4 (Application configs/structure)** in the commit structure.
- Binary files (images, JARs, ZIPs) are unaffected — `.gitattributes` handles them automatically with `text=auto`.

## Files That Should Never Be Committed

Adjust per project, but common patterns:

- Build output directories (e.g., `target/`, `dist/`, `build/`, `out/`)
- IDE files (e.g., `.idea/`, `*.iml`, `.vscode/` settings)
- OS metadata (e.g., `desktop.ini`, `.DS_Store`, `Thumbs.db`)
- Test coverage output (e.g., `coverage/`)
- Secrets or credentials files (e.g., `.env.local`, `*.key`)
- **Cloud-sync duplicate files** (e.g., `file (1).md`, `script (2).sh`) — created by Google Drive, OneDrive, or Dropbox conflict resolution. These contain stale or contradictory content and are a **blocking** issue. See `rules/git-conventions.md § Duplicate-File Gate`.

If these appear in `git status`, do not include them in the commit.


---


# Command safety

Before running a local command that has side effects, review it for safety first.

## Always review

- Commands with network access, downloads, or uploads
- Commands with pipes, shell substitution, or inline interpreter execution
- Commands whose literal search patterns or regexes contain shell-active characters such as backticks, `$()`, `;`, `&`, `|`, `<`, or `>`
- Installers and dependency-management commands
- Destructive file operations or Git history rewrites
- Commands touching secrets, auth files, or system-managed paths
- Privileged commands such as `sudo` or `su`

## Review flow

1. Split chained commands into independent segments.
2. If the repository provides a command-assessment script or policy, follow it.
3. Block critical-risk commands by default until the user explicitly reconfirms.
4. Ask for confirmation on high-risk commands and propose a safer alternative.
5. Quote literal patterns so the shell does not execute them. Prefer single-quoted literals for `rg`, `sed`, and similar; escape backticks or `$` when single quotes are not possible.
6. Keep the final command as narrow as possible.

This is guidance for the agent, not kernel-level enforcement.

---


# Data Governance

Data science projects are accountable not only for code quality, but also for the legitimacy and traceability of the data they use.

## Provenance

- Know where each dataset comes from, how it was collected, and under what license or usage terms it can be used.
- Preserve lineage between raw data, cleaned data, analytical tables, and published outputs.
- Prefer append-only or replayable raw storage when reproducibility matters.

## Privacy And Sensitivity

- Minimize sensitive fields and identifiers to what the analysis truly needs.
- Do not expose credentials, raw secrets, or local auth material in code, notebooks, or generated outputs.
- If a project operates at aggregate level, do not add logic that turns it into individual profiling without an explicit decision.

## Evidence Integrity

- Treat audit logs, extraction logs, metadata files, and validation reports as evidence artifacts.
- Prefer fixing the generator or pipeline over hand-editing evidence artifacts.
- When exclusions, imputations, or masking rules are applied, document them where the data is transformed and where the result is interpreted.

## Governance Review

For new datasets or analytical outputs, review:

- licensing and permitted use
- retention expectations
- masking or de-identification
- downstream sharing scope
- documentation of assumptions and known gaps

---


# Data Pipeline Contracts

Treat data contracts as first-class code. In analytics platforms, schemas, keys, grain, partitions, filenames, storage paths, and metadata configs are part of the public surface area.

## Contract Sources

Before editing pipeline code, identify the authoritative source for:

- dataset metadata
- schema definitions
- partition strategy
- naming conventions
- translation or semantic dictionaries
- orchestration entrypoints

If behavior changes, update the contract artifact in the same change.

## Layer Boundaries

- Raw or Bronze layers preserve source fidelity, ingestion traceability, and replayability.
- Clean or Silver layers normalize types, keys, and schema shape.
- Gold, marts, or feature-ready layers hold analysis-facing aggregates, features, or summary tables.

Do not hide Silver or Gold logic inside raw ingestion unless the upstream source contract truly changed.

## Non-Negotiables

- Grain, primary keys, time semantics, and join semantics are contracts.
- Stable storage paths, partitions, and filenames are contracts once downstream readers depend on them.
- Backfill requirements, cache invalidation, and reprocessing cost must be reviewed before merging.
- Prefer idempotent, incremental, observable processing over one-off rewrites.
- Fail loudly on unexpected schema drift instead of silently coercing data away.

## Review Checklist

For pipeline changes, inspect the impact on:

- config and contract files
- ingestion or extraction code
- transformation or feature code
- orchestration scripts and jobs
- tests and fixtures
- documentation and runbooks
- generated evidence or audit artifacts

## Promotion Rule

If a transformation matters outside a single notebook or ad hoc analysis, move it into reusable code or a documented pipeline step.

---


# Documentation Sync

When a repository maintains documentation or notebooks in more than one language, language drift becomes a product risk.

## Source of Truth

- Treat one language version as the source of truth for structure and technical meaning (English unless the project documents a different policy).
- If a maintained localized counterpart exists, update it in the same change or explicitly note that it is temporarily behind.
- Do not silently change domain terms in one language only.
- When a repository has mirrored files such as `README.md` and `README.<locale>.md`, or English and localized notebook pairs, check both sides before concluding documentation is complete.

## Translation Scope

- Translate narrative markdown, titles, captions, and user-facing terminology.
- Keep code identifiers, dataset keys, stable API or schema names, and command names in their canonical language.
- If the project uses translation dictionaries for dataset or column names, update those mappings together with the docs that depend on them.
- Do not translate away domain-specific legal or statistical terms unless the repository already has an accepted glossary for them.

## Review Checklist

When a change affects docs or notebooks, check:

- paired `README` or localized documents
- translated notebook markdown
- glossary or translation maps
- screenshots, charts, and table labels
- cross-links between language variants

## Acceptable Drift

If the localized version must lag temporarily, say so directly in the change summary or the document itself. Do not leave readers guessing whether the mismatch is intentional.

---

# Error-Driven Learning

When an LLM finds the correct solution after trial-and-error, that knowledge must be captured so that any future LLM session — regardless of tool — goes directly to the happy path.

## When to capture

Capture a learning when:

- You tried two or more approaches before finding the one that works.
- A command, API call, or configuration behaved differently than expected.
- A platform-specific gotcha caused a failure (Windows vs Unix, shell differences, path handling).
- A build, test, or deploy step required a non-obvious flag, order, or workaround.
- An integration between components required specific wiring that was not obvious from the code alone.

Do **not** capture trivial typos, one-off user misunderstandings, or temporary environment blips.

## Where learnings live

Each repository keeps a `learnings/` directory at the root. Files inside are plain markdown with YAML frontmatter. The directory is committed to version control so every LLM tool and every developer benefits.

```
learnings/
  README.md              # Convention docs and template
  windows-symlinks.md    # Example learning
  sam-build-docker.md    # Example learning
```

## File format

```markdown

# Problem
What was being attempted and what went wrong.

# Failed Approaches
What was tried and why it didn't work. Be specific — include commands, error messages, or code snippets.

# Solution
The correct approach. This is the happy path a future LLM should take directly.

# Why
Root cause explanation so the reader understands, not just copies.
```

## How to consume

Before starting a task, check whether `learnings/` contains anything relevant:

1. Scan filenames and tags for topic overlap with the current task.
2. Read matching files before planning your approach.
3. Prefer the documented solution over re-discovering it from scratch.

If the repository does not have a `learnings/` directory yet, create one (with `README.md`) the first time you capture a learning.

## How to write

Follow the `capture-learning` workflow. Key principles:

- **One file per learning.** Keep each focused on a single problem–solution pair.
- **Name files descriptively.** Use lowercase kebab-case that summarizes the topic: `maven-windows-sh-plugin.md`, `dynamodb-batch-limit.md`.
- **Include failed approaches.** This is what prevents future LLMs from repeating the same mistakes.
- **Include the root cause.** A solution without explanation is fragile — it breaks the moment conditions change.
- **Keep it concise.** A learning is not a tutorial. Target 20–60 lines.

## Tool-specific memory is supplementary

Some LLM tools have built-in memory (Windsurf memories, Cursor context, etc.). Use those too — they provide faster retrieval within that tool. But the committed `learnings/` file is the **source of truth** because it works across all tools, survives tool migrations, and is code-reviewable.

When using tool-specific memory, reference the learning file:
> See `learnings/windows-symlinks.md` for details.

## Categories

| Category | Covers |
|---|---|
| **environment** | OS, shell, permissions, auth, path handling, locale |
| **build** | Compilation, packaging, dependencies, plugins, flags |
| **api** | SDK usage, method signatures, request/response, rate limits |
| **architecture** | Component wiring, data flow, service dependencies |
| **testing** | Test setup, fixtures, mocking, assertion patterns |
| **toolchain** | CLI tools, IDE config, linters, formatters |
| **deployment** | Deploy process, infra, config, rollback |

---


# Git Branch & Commit Conventions

## Branch Naming

- **Always** create feature branches using the ticket ID as the branch name.
- Format: `<TICKET-ID>` or `<TICKET-ID>-short-description`
- Examples: `ABC-123`, `ABC-123-add-user-endpoint`, `BUG-456-fix-search` (use your tracker’s real prefix)
- The ticket ID is extracted from the task context (Jira ticket, PR title, user request, etc.)
- **Never** use generic branch names like `feature/my-changes` or `fix/bug`
- **Never** commit directly to `main` or the default protected branch

## Commit Messages

- Write clear, concise commit messages describing what changed.
- If the project uses pre-commit hooks to prepend ticket IDs, write just the description.
- Otherwise, prefix with ticket ID: `ABC-123: Add user profile endpoint`
- Use imperative mood: "Add", "Fix", "Update" — not "Added", "Fixed", "Updated"

## Commit Structure (PR Organization)

When committing changes for a PR, organize commits into up to 5 categories in this order. **Only create commits for categories that have changes** — skip empty categories.

| Order | Category | What belongs here |
|-------|----------|-------------------|
| 1 | **LLM configs** | `.windsurf/`, `.cursor/`, `.agents/`, `.claude/`, `.codex/`, `AGENTS.md`, `CLAUDE.md`, `.gitignore` |
| 2 | **Documentation** | `README.md`, `docs/`, architecture diagrams, API specs, PlantUML |
| 3 | **Logs improvement** | Logger setup/format/level changes, log context, MDC, logging-only test files |
| 4 | **Application configs/structure** | Build config (`pom.xml`, `requirements.txt`), DI config, env config, `__init__.py`, dependency files |
| 5 | **Code changes** | Source code, business logic, tests for business logic |

**Rules:**
- Each commit is prefixed with the ticket ID: `TICKET-ID: <description>`
- Never mix categories in a single commit.
- If a source file has **both** logging and business logic changes, split them: commit logging changes (new/modified log lines, log levels, log format, log context calls, commented-out log removal) in commit 3, and remaining code changes in commit 5. Use `git add -p` for hunk-level splits or create intermediate file versions for interleaved changes.
- Tests follow their subject: logging tests → commit 3, business logic tests → commit 5.
- Restructure commits before pushing using `git reset --mixed` and selective `git add`.

## Draft PR Lifecycle (Mandatory)

- **Always** open PRs as drafts: `gh pr create --draft`
- A PR stays in draft until **all** of the following are true:
  1. All CI checks pass (green).
  2. All bot review comments are addressed (e.g., Cursor automation, linters).
  3. All human review comments are resolved or explicitly deferred with justification.
  4. The pre-PR check workflow (`workflows/pre-pr-check.md`) reports no blockers.
  5. The security check (`scripts/security-check-toolkit.sh`) passes or all failures are reviewed.
- Only then mark as ready: `gh pr ready <PR_NUMBER>`
- **Never** open a PR as ready-for-review on first push.
- **Never** mark a PR ready while unresolved comments exist — address or defer each one first.

## Duplicate-File Gate (Mandatory — Pre-Push)

Cloud-sync tools (Google Drive, OneDrive, Dropbox) silently create conflict-resolution copies named `file (1).ext`, `file (2).ext`, etc. These duplicates can contain **stale or contradictory content** and must **never** be committed.

**Before every push**, run:
```bash
git ls-files | grep -E ' \([0-9]+\)\.' && echo "BLOCKED: remove duplicate files before pushing" && exit 1
```

If duplicates are found:
1. Delete them: `git rm 'path/to/file (1).md'`
2. Verify the original file is correct.
3. Re-run `git ls-files | grep -E ' \([0-9]+\)\.'` — must return nothing.

This check is **blocking** — do not push while any duplicate exists.

## Rebase Before Push

- **Always** rebase onto the base branch before every `git push`.
- Steps (always in this order):
  1. `git fetch origin`
  2. `git rebase origin/main` (or the PR base branch)
  3. Resolve any conflicts
  4. Re-run the full test suite to ensure nothing broke
  5. Check for cloud-sync duplicates (see § Duplicate-File Gate above)
  6. `git push --force-with-lease`
- **Never** push without rebasing first — even if the branch was recently created.
- **Never** use `git merge` to integrate upstream changes — rebase only.

---


# LaTeX In Notebooks

Math-heavy notebooks and technical reports should express equations as editable, reviewable source rather than opaque screenshots or ad hoc notation.

## Notation Discipline

- Define symbols close to the equation that uses them.
- Keep notation consistent with code, dataset column names, and chart labels where practical.
- Prefer a small, stable notation set over clever shorthand that only the author understands.

## Renderer Compatibility

- Use MathJax-friendly LaTeX that renders in Jupyter markdown and common HTML exports.
- Avoid custom macros unless the project documents them and the target renderer supports them.
- Test long equations and aligned environments in the notebook itself, not only in your head.

## Reviewability

- Prefer markdown plus LaTeX over screenshots of formulas.
- Break complex derivations into smaller steps with short prose between them.
- Explain what an equation is doing and why it matters; do not leave raw math without interpretation.

## Export Safety

- If the notebook or doc will be exported to HTML, PDF, or slides, verify the equations render in the intended target.
- When exact notation matters to implementation, keep the equation and the code path easy to cross-reference.

---


# ML Experiment Discipline

Machine learning changes are not complete when the code runs. They are complete when the target, data boundaries, evaluation method, and limitations are all explicit.

## Define the Task First

Before changing features or models, write down:

- the prediction or decision objective
- the unit of analysis
- the target definition
- the inference or prediction time boundary
- the intended consumer of the output

Start with a clearly stated prediction or analytical objective, then establish a simple baseline before proposing a more complex model.

## Leakage Prevention

- Split data using the boundary that matches real-world inference: time, entity, geography, or experiment cohort.
- Split before fitting transforms whenever the transforms learn from data.
- Do not let features depend on information that would be unavailable at prediction time — no post-outcome fields, target-derived transforms, or future periods.
- Treat pre-aggregated analytical tables, marts, and notebook outputs as potentially leaky until feature provenance is verified.
- Be explicit about feature provenance for every derived variable used in modeling.

## Baselines and Metrics

- Always compare against a simple baseline before claiming improvement.
- Choose metrics that match the real decision cost, not just what is easiest to compute:
  - Regression: error metrics, residual behavior, and baseline comparisons.
  - Classification: class balance, threshold choice, confusion-matrix tradeoffs, and calibration when relevant.
  - Clustering: stability, silhouette or alternate diagnostics, and interpretability of cluster profiles.
- Include sample size, class balance, and important caveats with every evaluation summary.
- If thresholds are used, make threshold selection explicit and document the tradeoff.
- For small datasets, prefer robust validation and careful interpretation over aggressive optimization claims.

## Interpretation and Review

- Inspect feature importance, error slices, residual behavior, or confusion patterns before claiming success.
- Do not present predictive association as causal evidence.
- Document threshold choices, tradeoffs, and known failure modes if the model output is turned into a decision signal.
- Favor interpretable features and explainability when the output will influence public-sector, compliance, or anomaly discussions.

## Reproducibility

- Record reproducibility inputs: dataset version or time window, filters, target definition, feature list, random seeds, split strategy, and metric definitions.
- Keep feature generation and preprocessing deterministic where possible.
- Version the feature source or dataset snapshot used in the experiment.
- Prefer reusable pipelines or scripts over notebook-only model training logic when the work may be revisited.
- Reuse stable feature generation code from `src/` or other committed paths when notebook experiments become important to the project.

## Governance and Limits

- Report limitations honestly, especially for small samples, aggregate datasets, or proxy labels.
- Separate exploratory signal from production-ready evidence.
- Document major model limitations, fairness or bias caveats, and operational assumptions when outputs influence prioritization or decisions.
- If the project has governance, fairness, or public-policy constraints, document them with the experiment result.
- Document uncertainty, data limitations, and sample-size caveats before drawing strong policy or compliance conclusions.

## Artifact Discipline

- Keep exploration separate from reusable logic. When a loader, transform, feature builder, or metric becomes stable, move it into versioned source code and import it from notebooks.
- Distinguish analysis artifacts from operational artifacts. Figures and tables for reports are not a substitute for versioned training code, evaluation outputs, or deployment inputs.
- Version the code and document the data slice used for each reported result.
- Generated conclusions should be traceable back to code, data, parameters, and the exact artifact that produced them.

---


# Multi-Agent Orchestration

When a selected workflow has independent lanes, use parallel agents (Cursor subagents, Task tool, or IDE fanout) by default. Do not pause to ask for permission first unless there is a user-visible tradeoff, risky external side effect, or overlapping write ownership that needs a decision.

## Default fanout

- Launch the maximum safe parallel slices once scope is known.
- Keep the immediate blocking step local so the critical path keeps moving.
- Split only independent work: different subsystems, different files, different evidence sources.
- Do not assign overlapping write ownership to more than one agent.
- Use specialized agents for sidecar analysis, test mapping, docs drift, risk review, and other bounded tasks.
- Merge findings locally before implementing broad fixes.

## Typical parallel slices

- Ticket and doc context
- Diff or risk review
- Test coverage and validation planning
- Docs drift and release-note impact
- Cross-repo or downstream consumer impact
- CI failure log inspection and local repro mapping

## When to pause instead of fan out

- The user must choose between non-obvious product or design options
- A command or change has risky external side effects
- The write set cannot be split cleanly

## Finish locally

- Reconcile duplicate findings.
- Make the final implementation decisions.
- Run final verification locally.
- Summarize what changed, what was verified, and what remains risky.

## Workflows that should fan out when lanes are independent

When the active workflow has separate read-only or non-overlapping write lanes, parallelize by default (see each workflow’s steps): **review**, **ticket-review-and-fix**, **pre-pr-check**, **project-discovery**, **run-tests**, **ticket-research**, **gh-fix-ci**, **gh-address-comments**, **environment-diagnose**, **security-report**, **update-docs**, **cross-repo-impact**, **dependency-upgrade**, and **document-creation** (when phases are independent). **Enable Max Mode** (or equivalent) in the IDE so subagents using `model: inherit` run at full capability.

## Tool mapping

- **Cursor**: project subagents live in `.cursor/agents/` (see `.cursor/README.md`). Prefer `/agent-name` or natural-language delegation; combine with rules in `.cursor/rules/`.
- **Claude / Codex**: use `.claude/agents/` for shared agent files. Keep `AGENTS.md` as the primary repo-level surface, and expose shared skills through the thin `.codex/skills` compatibility symlink when needed.
- **Windsurf / generic**: follow the same principles using the IDE’s task or multi-chat capabilities.

---


# Notebook Discipline

Committed notebooks should remain understandable and rerunnable even after the original author is gone.

## Execution Discipline

- A committed notebook must restart cleanly and run all cells in order (`Restart Kernel and Run All`).
- Do not rely on hidden kernel state, out-of-order execution, or manually patched variables.
- Keep environment assumptions explicit: kernel, Python version, packages, credentials, data paths, and cloud profile.

## Structure

- Start with a short setup section that states data prerequisites, configuration, and assumptions.
- Use markdown cells to explain the question, the data inputs, the key assumptions, and the interpretation of results.
- Keep imports and configuration near the top so a reviewer can understand the runtime quickly.
- Group sections clearly: setup, data load, quality checks, analysis, interpretation, and next steps.
- Keep plots, tables, and markdown conclusions close to the code that produces them.

## Output Hygiene

- Keep large or noisy outputs out of committed notebooks unless they are essential to the review.
- Clear stale outputs when they no longer match the current code or data.
- Do not commit massive datasets, local cache artifacts, or screenshot-only evidence in place of reproducible code.
- Avoid hardcoded local machine paths, personal buckets, or ad hoc file drops inside notebooks or scripts.

## Reproducibility

- Every important notebook should make these items easy to find near the top: data source or loader entrypoint, key assumptions and filters, random seeds or deterministic settings, and expected outputs or saved artifacts.
- Set explicit random seeds for sampling, splitting, and model training when randomness is involved.
- Record the dataset snapshot, source path, or extraction date that supports the analysis.
- Statistical claims in markdown cells should match the code that produced them. Update narrative text when assumptions, samples, or metrics change.
- Re-run notebooks from a clean kernel before treating results as final.

## Reuse and Promotion Boundary

- Use notebooks for exploration, explanation, visualization, and result narration.
- Move reusable business logic, data loading, feature engineering, and evaluation code into `src/`, scripts, or documented pipeline steps.
- Avoid keeping the only copy of important transformation logic inside a notebook.
- Avoid copy-pasting business logic across notebooks. Prefer loaders and helper functions that make data access explicit and testable.
- If a notebook cell becomes operational, repeated, or testable logic, promote it into Python modules or scripts.
- If a finding affects product, pipeline, or report decisions, reflect it in committed docs or tests instead of leaving it only in notebook prose.
- If notebook pairs exist in more than one language, update both or document why only one changed.

---


# Operational documentation

If a deploy, mitigation, rollback, or triage flow is non-trivial or recurring:

1. Check whether a runbook or ops doc already exists.
2. If it exists, update it with the new learning.
3. If it does not exist, create one before the knowledge disappears from working memory.

---


# Python Best Practices

Language-specific best practices for Python projects. These apply to any Python codebase regardless of framework.

## Language Fundamentals

### Code Style (PEP 8)
- **4 spaces** for indentation (never tabs, unless matching an existing project that uses tabs).
- **snake_case** for functions, methods, variables. **PascalCase** for classes. **UPPER_SNAKE_CASE** for constants.
- **Line length**: 88–120 chars (match project's formatter — Black uses 88, most projects use 120).
- **Imports**: stdlib → third-party → local, separated by blank lines. One import per line. No wildcard imports (`from x import *`) except in `__init__.py` re-exports.

### Type Hints
```python
# GOOD — clear contracts
def calculate_score(threshold: float, values: list[float]) -> float:
    return sum(v for v in values if v > threshold)

# Constants with Final
from typing import Final
MAX_RETRIES: Final = 3
TABLE_NAME: Final = "orders"
```
- Use type hints on public function signatures — they serve as documentation and enable static analysis.
- Match existing project style — if the codebase doesn't use type hints on functions, don't add them selectively.
- Use `Final` for constants that should never be reassigned.

### String Handling
```python
# WRONG — concatenation
msg = "Processing " + str(count) + " items for " + user_id

# CORRECT — f-strings (Python 3.6+) for general code
msg = f"Processing {count} items for {user_id}"

# CORRECT — % formatting for logging (lazy evaluation)
logger.info("Processing %d items for %s", count, user_id)
```
- Use f-strings for general string building (readable, fast).
- Use `%s`/`%d` formatting for `logger` calls — avoids string construction when log level is disabled.
- Match whatever pattern the existing codebase uses.

### Collections
```python
# Prefer comprehensions over map/filter for readability
squares = [x**2 for x in numbers if x > 0]
lookup = {item.id: item for item in items}

# Use dict.get() with defaults instead of KeyError handling
value = config.get("key", "default")

# Use collections for specialized needs
from collections import defaultdict, Counter, OrderedDict
```
- Prefer `dict.get(key, default)` over `if key in dict: dict[key]`.
- Use `defaultdict` when building grouped/accumulated results.
- Never modify a dict/list while iterating — build a new one or collect keys to delete.

### Method Design

- **Keep functions small** — a function should do one thing. If you need a comment to explain a block of code inside a function, extract it into a named helper.
- **Target 10–20 lines per function.** Functions over 30 lines are a strong signal to refactor.
- **One level of abstraction per function** — don't mix high-level orchestration with low-level details.

```python
# WRONG — does too many things, hard to test individual parts
def process_order(order):
    if not order.items:
        raise ValidationException("No items")
    if order.total <= 0:
        raise ValidationException("Invalid total")
    discount = sum(item.discount for item in order.items if item.is_on_sale)
    order.discount = discount
    order_repository.save(order)
    email_service.send_confirmation(order)

# CORRECT — each function does one thing, independently testable
def process_order(order):
    validate_order(order)
    order.discount = calculate_discount(order.items)
    order_repository.save(order)
    email_service.send_confirmation(order)
```

- **Extract early returns** for validation (guard clauses) — reduces nesting.
- **Boolean parameters are a code smell** — prefer two clearly named functions or an enum.

### Algorithmic Complexity

- **Avoid nested loops over collections** — `O(n²)` or worse. Use a `dict` or `set` for lookup instead.

```python
# WRONG — O(n²), scans active_users for every order
for order in orders:
    for user in active_users:
        if user.id == order.user_id:
            # match
            pass

# CORRECT — O(n), build lookup dict first
user_by_id = {user.id: user for user in active_users}
for order in orders:
    user = user_by_id.get(order.user_id)
    if user:
        # match
        pass
```

- **Three levels of nesting is a red flag** — extract inner loops into named functions.
- **Use `set` for membership checks** — `O(1)` vs `list` at `O(n)`.
- **Batch external calls** — never call a database or HTTP API inside a loop. Collect IDs, batch-fetch, then process.

### Null Safety
```python
# WRONG — AttributeError on None
result = response.get("data").get("items")

# CORRECT — defensive chaining
data = response.get("data") or {}
items = data.get("items", [])

# CORRECT — early validation
if not request_payload:
    raise ValidationException(ERROR_CODE, "Missing payload")
```
- Check for `None`, empty strings, and empty collections explicitly.
- Use `or` for default values: `value = x or "default"` (but beware of falsy values like `0` or `False`).
- Use `is None` / `is not None` for explicit None checks — never `== None`.

## Error Handling

### Exception Patterns
```python
# WRONG — bare except
try:
    process(data)
except:
    pass

# WRONG — catching too broadly
try:
    process(data)
except Exception as e:
    logger.error("Failed: %s", e)

# CORRECT — specific, contextual, preserves cause
try:
    value = int(raw_value)
except ValueError as e:
    raise ValidationException(
        ERROR_CODE, f"Invalid numeric value: {raw_value}"
    ) from e
```
- Catch the most specific exception type.
- Use `raise ... from e` to preserve the cause chain.
- Never use bare `except:` — at minimum use `except Exception:`.
- Never swallow exceptions silently. Log or re-raise with context.
- Custom exceptions should inherit from a project-specific base (e.g., `ApplicationException`), not bare `Exception`.

### Context Managers
```python
# CORRECT — automatic cleanup
with open("file.txt") as f:
    data = f.read()

# For custom resources
from contextlib import contextmanager

@contextmanager
def managed_connection(url):
    conn = create_connection(url)
    try:
        yield conn
    finally:
        conn.close()
```
- Use `with` statements for all resources that need cleanup (files, connections, locks).
- Create custom context managers for project-specific resource management.

## Logging

### Parameterized Logging
```python
# WRONG — string constructed even if level is disabled
logger.debug(f"Processing user: {user_id} with {len(items)} items")

# CORRECT — lazy evaluation (string only constructed if DEBUG is enabled)
logger.debug("Processing user: %s with %d items", user_id, len(items))
```
- Use `%s`/`%d` formatting for `logger` calls — avoids string construction when log level is disabled.
- Use f-strings only when you're certain the log line will be emitted (e.g., inside an `if logger.isEnabledFor()` block), or in non-logging code.
- Use appropriate levels: ERROR (action needed), WARNING (concerning), INFO (milestones), DEBUG (diagnostics).
- Log at function boundaries: entry (DEBUG), result (INFO/DEBUG), exceptions (ERROR/WARNING).
- Never log PII, credentials, or full request/response bodies in production.

### Logging Exceptions with Context

When logging an exception, include enough context to diagnose the issue **without** reading the full stack trace:

```python
# WRONG — no business context
logger.error("Operation failed", exc_info=True)

# CORRECT — business context + root cause + full traceback
logger.error(
    "DynamoDB retrieval failed for requestId=%s [root cause: %s]",
    request_id, str(e),
    exc_info=True
)
```

**Pattern**: `logger.error("What failed for key=%s [root cause: %s]", key, str(e), exc_info=True)`

- **`exc_info=True`** — tells the logging framework to append the full traceback. Without it, only the message is logged.
- Include business context (IDs, operation name) in the message for quick log scanning.
- Never include PII in exception messages. Pseudonymous IDs (`requestId`, `orderId`, `userId`) are OK.

### Correlation IDs

Use a correlation ID to trace requests across log lines and services:

```python
import logging

# Add correlation ID to log records via a filter
class CorrelationFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.request_id = ""

    def filter(self, record):
        record.request_id = self.request_id
        return True

# Configure log format to include correlation ID
formatter = logging.Formatter(
    "%(asctime)s [%(request_id)s] %(levelname)s %(name)s %(message)s"
)
```

- Set the correlation ID at the request entry point (Lambda handler, Flask/FastAPI middleware).
- Pass the same ID to downstream service calls for cross-service tracing.

### Structured Logging (JSON)

For production services (especially Lambda/cloud), use JSON-formatted logs for easy parsing:

```python
import json
import logging

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "requestId": getattr(record, "request_id", ""),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)
```

- JSON logs are easier to query in CloudWatch, Datadog, Splunk, etc.
- Libraries like `python-json-logger` or `structlog` simplify this.
- Always include a correlation/request ID in structured logs.

## Design Patterns

### Dependency Injection (DI) — The Core Principle

**Every class receives its external collaborators as abstract types (ABCs or Protocols) through the constructor.** The application entry point (main, Lambda handler bootstrap, FastAPI/Flask factory) is the only place that knows which concrete implementation to wire. This is the single most important pattern for testability, loose coupling, and maintainability.

#### The Rule

1. **Define an interface** for every external dependency (database, HTTP client, message publisher, cache, file system, third-party SDK) — use `abc.ABC` or `typing.Protocol`.
2. **Accept the interface in `__init__`** — never instantiate collaborators inside a class.
3. **Let the application entry point wire** the concrete implementation.
4. **In tests, pass a mock** — no framework magic needed.

#### Full Example

```python
from abc import ABC, abstractmethod
from typing import Optional

# 1. INTERFACE — defines the contract. Lives in the service/domain layer.
class OrderRepository(ABC):
    @abstractmethod
    def find_by_user_id(self, user_id: str) -> Optional[dict]:
        ...

    @abstractmethod
    def save(self, order: dict) -> None:
        ...

class NotificationClient(ABC):
    @abstractmethod
    def send_order_confirmation(self, order: dict) -> None:
        ...

# 2. IMPLEMENTATION — lives in the infrastructure layer. Knows about DynamoDB.
class DynamoDbOrderRepository(OrderRepository):
    def __init__(self, table):
        self._table = table

    def find_by_user_id(self, user_id: str) -> Optional[dict]:
        response = self._table.get_item(Key={"userId": user_id})
        return response.get("Item")

    def save(self, order: dict) -> None:
        self._table.put_item(Item=order)

# 3. SERVICE — depends ONLY on interfaces. Knows nothing about DynamoDB, HTTP, etc.
class OrderService:
    def __init__(self, order_repo: OrderRepository, notification_client: NotificationClient):
        self._order_repo = order_repo
        self._notification_client = notification_client

    def place_order(self, order: dict) -> None:
        self._validate(order)
        self._order_repo.save(order)
        self._notification_client.send_order_confirmation(order)

# 4. CONFIGURATION — the ONLY place that knows which implementation to use.
# Application entry point (e.g., main.py, app factory, Lambda bootstrap):
def create_order_service() -> OrderService:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("orders")
    repo = DynamoDbOrderRepository(table)
    notifier = HttpNotificationClient(base_url=os.environ["NOTIFICATION_URL"])
    return OrderService(repo, notifier)

# FastAPI example:
def get_order_service() -> OrderService:
    return create_order_service()

@router.post("/orders")
async def create_order(order: OrderRequest, service: OrderService = Depends(get_order_service)):
    service.place_order(order.dict())

# Lambda example:
_order_service = None  # cached across warm starts

def initialize():
    global _order_service
    if _order_service is None:
        _order_service = create_order_service()

def lambda_handler(event, context):
    initialize()
    _order_service.place_order(parse_event(event))

# 5. TEST — trivial to mock, no framework needed.
from unittest.mock import Mock

class TestOrderService:
    def setup_method(self):
        self.mock_repo = Mock(spec=OrderRepository)
        self.mock_notifier = Mock(spec=NotificationClient)
        self.service = OrderService(self.mock_repo, self.mock_notifier)

    def test_place_order_saves_and_notifies(self):
        order = {"userId": "user-123", "total": 99.99}
        self.service.place_order(order)
        self.mock_repo.save.assert_called_once_with(order)
        self.mock_notifier.send_order_confirmation.assert_called_once_with(order)
```

#### Alternative: `typing.Protocol` (Structural / Duck-Typing DI)

```python
from typing import Protocol, Optional

# Protocol — no need to inherit. Any class with matching methods satisfies it.
class OrderRepository(Protocol):
    def find_by_user_id(self, user_id: str) -> Optional[dict]: ...
    def save(self, order: dict) -> None: ...
```
- Use `Protocol` when you want duck-typing compatibility (any class with the right methods works).
- Use `ABC` when you want explicit inheritance and enforcement.

#### Anti-Patterns (Never Do This)

```python
# WRONG — instantiating a concrete dependency inside the class. Untestable, tightly coupled.
class OrderService:
    def __init__(self):
        self.db = boto3.resource("dynamodb")  # coupled to AWS

# WRONG — importing and using a concrete class directly.
from myapp.infra.dynamo_repo import DynamoDbOrderRepository
class OrderService:
    def __init__(self):
        self.repo = DynamoDbOrderRepository()  # tied to DynamoDB

# WRONG — using module-level globals as hidden dependencies.
_repo = DynamoDbOrderRepository()  # hidden, untestable
class OrderService:
    def process(self, order):
        _repo.save(order)
```

#### Why This Matters

- **Testability** — swap real implementations for mocks in one line.
- **Loose coupling** — service logic never changes when you switch databases, HTTP clients, or message brokers.
- **Explicit dependencies** — the `__init__` signature is the dependency manifest. No hidden surprises.
- This is the **Gateway** / **Repository** / **Adapter** pattern from Clean Architecture.

### Constants Module
```python
# Constants.py — all magic values in one place
from typing import Final

TABLE_NAME: Final = "orders"
MAX_RETRIES: Final = 3
ERROR_MISSING_ID: Final = "4001"
ERROR_MISSING_ID_DESC: Final = "Missing required order ID"
```
- Centralize all constants — JSON keys, error codes, table names, env var names.
- Never hardcode strings or numbers inline. If a value appears more than once, it's a constant.

### Data Classes (Python 3.7+)
```python
from dataclasses import dataclass

@dataclass(frozen=True)  # frozen = immutable
class AppConfig:
    config_id: str
    endpoint: str
    threshold: float
```
- Use `@dataclass` for structured data — cleaner than plain dicts.
- Use `frozen=True` for immutable value objects.

## Performance

### Avoid Common Pitfalls
```python
# WRONG — mutable default argument (shared across calls!)
def process(items=[]):
    items.append("new")
    return items

# CORRECT
def process(items=None):
    items = items or []
    items.append("new")
    return items
```
- Never use mutable default arguments (`[]`, `{}`, `set()`).
- Use generators for large datasets — don't load everything into memory.
- Use `set` for membership testing (`O(1)`) instead of `list` (`O(n)`).
- Profile before optimizing — use `cProfile`, `line_profiler`, or `py-spy`.

### AWS/Lambda Specific
- **Cache boto3 clients** at module level (survive warm starts). Create in `initialize()`, not per-invocation.
- **Minimize cold start**: Keep imports lean. Avoid importing unused libraries.
- **Connection reuse**: boto3 handles HTTP connection pooling internally — don't recreate clients.
- **Payload size**: Lambda has a 6MB sync / 256KB async payload limit. Validate input size early.

## Testing

### Test Structure
```python
import pytest
from unittest.mock import Mock, patch, MagicMock

class TestOrderService:
    # Named constants — no magic values
    ORDER_ID = "ORD-12345"
    CUSTOMER_ID = "CUST-001"

    def setup_method(self):
        self.mock_db = Mock()
        self.service = OrderService(self.mock_db)

    def test_should_return_order_when_exists(self):
        # Given
        self.mock_db.get_item.return_value = {"id": self.ORDER_ID}
        # When
        result = self.service.get_order(self.ORDER_ID)
        # Then
        assert result["id"] == self.ORDER_ID
        self.mock_db.get_item.assert_called_once_with(self.ORDER_ID)

    def test_should_raise_when_order_not_found(self):
        self.mock_db.get_item.return_value = None
        with pytest.raises(NotFoundException):
            self.service.get_order(self.ORDER_ID)
```
- Use `pytest` over `unittest` (unless matching existing project style).
- Use `unittest.mock.Mock` / `MagicMock` for mocking. Use `moto` for AWS service mocking.
- Name tests: `test_should_[expected]_when_[condition]`.
- Given/When/Then structure. Named constants for all test data.

### What to Test
- Happy path, edge cases (None, empty, boundary values), error paths.
- For Lambda: all validation error codes, external service error handling (DynamoDB, S3, etc.), cold start + warm start behavior.
- Mock external services — never call real AWS services in unit tests.

## External Resource Access

### Database Connections

- **Always use a connection pool** — `sqlalchemy` with connection pooling, or reuse boto3 clients (thread-safe singletons).
- **Configure timeouts** — connection timeout, read timeout, and idle timeout to prevent connection leaks.
- **Use context managers** for connections — `with engine.connect() as conn:`.
- **Use parameterized queries** — never concatenate user input into SQL or DynamoDB expressions.

```python
# WRONG — SQL injection risk, no pool
import sqlite3
conn = sqlite3.connect("db.sqlite")
cursor = conn.execute(f"SELECT * FROM users WHERE id = '{user_id}'")

# CORRECT — parameterized, pooled (SQLAlchemy)
from sqlalchemy import create_engine, text
engine = create_engine("postgresql://...", pool_size=10, pool_timeout=30)
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM users WHERE id = :id"), {"id": user_id})
```

- **DynamoDB / NoSQL** — reuse boto3 `resource` / `client` as module-level singletons (thread-safe).
- **Batch operations** — use `batch_get_item` / `batch_write_item` instead of single-item calls in a loop.
- **Limit result sets** — always use `Limit` on queries. Never scan an entire table.

### HTTP Connections

- **Reuse HTTP sessions** — create one `requests.Session()` or `httpx.Client()` and share it. Never create per-request.
- **Configure timeouts** — always set both connect and read timeouts.

```python
import requests

# WRONG — no timeout, no session reuse
response = requests.get(f"https://api.example.com/data/{item_id}")

# CORRECT — session reuse, explicit timeouts
session = requests.Session()
session.timeout = (3, 10)  # (connect_timeout, read_timeout)
response = session.get(f"https://api.example.com/data/{item_id}")
response.raise_for_status()
```

- **Retry with exponential backoff** — use `urllib3.util.Retry` or `tenacity` for transient failures.
- **Never trust external input** — validate response status, content type, and body before processing.

### Caching

Use caching to avoid redundant calls to external resources:

```python
from functools import lru_cache
from cachetools import TTLCache

# Simple in-memory cache with TTL
config_cache = TTLCache(maxsize=100, ttl=600)  # 10 min TTL

def get_config(config_id: str) -> dict:
    if config_id not in config_cache:
        config_cache[config_id] = config_service.fetch_from_remote(config_id)
    return config_cache[config_id]

# For pure functions with hashable args — stdlib lru_cache
@lru_cache(maxsize=128)
def compute_expensive_result(key: str) -> str:
    return expensive_computation(key)
```

- **Always set a TTL** — stale data is usually acceptable for a few minutes. Never cache forever.
- **Bound the cache size** — use `maxsize` to prevent unbounded memory growth.
- **`functools.lru_cache`** — great for pure functions, but has no TTL. Use `cachetools.TTLCache` when TTL is needed.
- **Cache at the right layer** — cache in the service/helper layer, not in the handler or data access layer.

## Concurrency

### GIL: What It Does and Doesn't Protect

Python's Global Interpreter Lock (GIL) prevents two threads from executing Python bytecode simultaneously. This means:
- **Single bytecode operations** (e.g., reading/writing a single reference) are atomic.
- **Compound operations are NOT atomic** — read-modify-write (`counter += 1`), check-then-act (`if k not in d: d[k] = v`), and iterating + modifying a collection are all race conditions even with the GIL.
- The GIL does **not** eliminate race conditions. It only prevents memory corruption at the C level.

### First Defense: Immutability

The simplest way to prevent race conditions is to **make objects immutable** — if state can't change, it can't be corrupted.

```python
from dataclasses import dataclass

# THREAD-SAFE — frozen dataclass. No synchronization needed. Can be freely shared.
@dataclass(frozen=True)
class OrderSnapshot:
    order_id: str
    total: float
    item_ids: tuple[str, ...]  # tuple, not list — immutable

# Creating a new snapshot instead of mutating
def update_total(snapshot: OrderSnapshot, new_total: float) -> OrderSnapshot:
    return OrderSnapshot(
        order_id=snapshot.order_id,
        total=new_total,
        item_ids=snapshot.item_ids,
    )
```

- **`frozen=True`** — prevents attribute assignment after creation. `snapshot.total = 99` raises `FrozenInstanceError`.
- **Use `tuple` instead of `list`** for collection fields — tuples are immutable.
- **Create new objects** instead of mutating existing ones — return a new `OrderSnapshot` with the updated value.
- **`NamedTuple`** is also immutable by default: `class OrderSnapshot(NamedTuple): ...`

**Rule of thumb**: If an object is shared across threads, make it immutable. If it can't be immutable, protect it with a lock.

### Race Conditions: Read-Modify-Write

The most common race condition is **read-modify-write** — two threads read the same value, both modify it, and one write overwrites the other.

```python
import threading

# WRONG — race condition. Two threads can read count=5, both write 6. One increment lost.
# `+=` is NOT atomic: read count → add 1 → write count (3 bytecode ops).
counter = 0
def increment():
    global counter
    counter += 1  # RACE CONDITION

# CORRECT — use a Lock to make the operation atomic.
counter = 0
counter_lock = threading.Lock()
def increment():
    global counter
    with counter_lock:
        counter += 1  # only one thread at a time

# CORRECT — for simple counters, use a thread-safe wrapper.
import itertools
counter = itertools.count()  # thread-safe, but only increments by 1
```

### Race Conditions: Check-Then-Act

```python
# WRONG — another thread can insert between the `in` check and the assignment.
if key not in shared_dict:
    shared_dict[key] = compute_value()  # two threads can both enter here

# CORRECT — use a lock around the compound operation.
with dict_lock:
    if key not in shared_dict:
        shared_dict[key] = compute_value()

# CORRECT — for simple cases, use dict.setdefault() (atomic for CPython due to GIL,
# but NOT guaranteed by the language spec — use a lock for safety in production).
shared_dict.setdefault(key, compute_value())  # note: compute_value() always runs
```

### Race Conditions: Shared Mutable Objects

When an object's attributes are modified by multiple threads, **every access** (read and write) must be protected by the same lock.

```python
import threading
from copy import deepcopy

# WRONG — unsynchronized mutable state. Thread A calls set_status() while
# Thread B calls get_errors(). Thread B may see inconsistent state.
class OrderProcessor:
    def __init__(self):
        self.status = "NEW"
        self.errors = []  # list is not thread-safe for compound ops

    def set_status(self, s): self.status = s
    def get_status(self): return self.status
    def add_error(self, e): self.errors.append(e)
    def get_errors(self): return self.errors  # caller can mutate the list

# CORRECT — lock protects all access, defensive copies on reads.
class OrderProcessor:
    def __init__(self):
        self._lock = threading.Lock()
        self._status = "NEW"
        self._errors = []

    def set_status(self, s):
        with self._lock:
            self._status = s

    def get_status(self) -> str:
        with self._lock:
            return self._status

    def add_error(self, e):
        with self._lock:
            self._errors.append(e)

    def get_errors(self) -> tuple:
        with self._lock:
            return tuple(self._errors)  # defensive copy — caller can't mutate
```

**Key rules:**
- **Lock both reads and writes** — locking only writes is a bug. Without a locked read, the reading thread may see a partially updated or stale value.
- **Return defensive copies** from locked getters — returning a mutable `list` reference lets callers modify the object outside the lock.
- **Use `with lock:`** (context manager) — never use `lock.acquire()` / `lock.release()` manually (exception-unsafe).
- **One lock per shared resource** — don't use a single global lock for unrelated data (contention). Don't use multiple locks for the same data (deadlocks).

### Thread-Safe Data Structures

| Need | Use | Not |
|------|-----|-----|
| Thread-safe FIFO queue | `queue.Queue` | `list` + lock |
| Thread-safe dict operations | `threading.Lock` + `dict` | bare `dict` with compound ops |
| Thread-safe counter | `threading.Lock` + `int` | bare `int` with `+=` |
| Share data between processes | `multiprocessing.Queue` / `Manager` | shared `list` or `dict` |
| Thread-local storage | `threading.local()` | global variables |

```python
import queue
import threading

# queue.Queue — thread-safe producer/consumer pattern.
work_queue = queue.Queue(maxsize=100)

def producer():
    work_queue.put(item)  # blocks if full

def consumer():
    item = work_queue.get()  # blocks if empty
    try:
        process(item)
    finally:
        work_queue.task_done()

# threading.local() — each thread gets its own copy. No locking needed.
thread_data = threading.local()
thread_data.request_id = "abc-123"  # only visible to this thread
```

### Thread-Safe Lazy Initialization

```python
# WRONG — race condition. Two threads may both see _instance is None and create two instances.
_instance = None
def get_instance():
    global _instance
    if _instance is None:
        _instance = ExpensiveResource()  # two threads can enter here
    return _instance

# CORRECT — lock around initialization.
_instance = None
_init_lock = threading.Lock()
def get_instance():
    global _instance
    if _instance is None:  # fast path — no lock if already initialized
        with _init_lock:
            if _instance is None:  # double-check inside lock
                _instance = ExpensiveResource()
    return _instance
```

### Async Work

- **Use `concurrent.futures`** for parallel I/O — `ThreadPoolExecutor` for I/O-bound, `ProcessPoolExecutor` for CPU-bound.
- **Prefer `asyncio`** for high-concurrency I/O (web servers, many HTTP calls) — but only if the project already uses async.
- **Handle exceptions** in futures — unhandled exceptions are silently swallowed unless you call `.result()`.

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_all(ids: list[str]) -> list[dict]:
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_one, id_): id_ for id_ in ids}
        results = []
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                logger.error("Fetch failed for %s: %s", futures[future], e)
        return results
```

### Common Concurrency Pitfalls

| Pitfall | Example | Fix |
|---------|---------|-----|
| **Non-atomic `+=`** | `counter += 1` on a shared `int` | `with lock: counter += 1` |
| **Check-then-act** | `if k not in d: d[k] = v` | `with lock:` around both lines |
| **Publishing mutable object** | Returning an internal `list` | Return `tuple(list)` copy |
| **Locking writes but not reads** | Only `set` uses the lock | Lock both `get` and `set` |
| **Double-check without lock** | `if x is None: x = create()` | Double-check with `threading.Lock` |
| **Modifying collection while iterating** | `for item in lst: lst.remove(item)` | Build new list or collect indices |
| **GIL = thread-safe (myth)** | Assuming `dict[k] = v` is safe in compound ops | Always lock compound operations |

## Operational Best Practices

### Dynamic Log Levels for Production Troubleshooting

When troubleshooting in production, temporarily change log levels to get more diagnostic detail:

```python
import logging

# Change at runtime (e.g., triggered by an environment variable or config reload)
logging.getLogger("myapp.service").setLevel(logging.DEBUG)

# For Lambda: check an env var or SSM parameter at handler entry
import os
if os.environ.get("DEBUG_LOGGING") == "true":
    logging.getLogger().setLevel(logging.DEBUG)
```

- **Always revert** after troubleshooting — leaving DEBUG on in production causes log flooding and increased costs.
- **Prefer narrow scope** — change the level for a specific module, not the root logger.
- **For Lambda**: use an environment variable or SSM parameter to toggle debug logging without redeployment.

## Security

- **Never log** secrets, tokens, PII, or full request payloads in production.
- **json.loads() / json.dumps()** — never build JSON by string concatenation.
- **Validate inputs** before use — especially values used in S3 keys, DynamoDB queries, or log messages.
- **Pin dependencies** in `requirements.txt` — `boto3==1.34.0` not `boto3`.
- **Audit dependencies** for CVEs before adding (`pip-audit`, `safety`).

---


# Repository context layers

When multiple instruction sources apply (toolkit rules, workflows, repo `AGENTS.md`, optional `CLAUDE.md`, IDE rules):

1. **Merge in memory** during the turn: follow the most complete, **non-conflicting** workflow.
2. Keep the **union** of non-conflicting requirements; where they conflict, **stricter** verification, security, and documentation requirements win unless the user explicitly overrides.
3. **Repo-local** files (`AGENTS.md`, committed `docs/*`, local `skills/` playbooks) are **first-class** for that repository. Do not overwrite them when refreshing shared toolkit symlinks.
4. **Shared toolkit** content (`rules/`, `workflows/`, `skills/`) is the default for generic behavior across projects.
5. Provider skill directories should be thin compatibility symlinks back to the shared catalog, not separate authored copies.

---


# Research Discipline

Analytical research is not complete when a model runs or a chart looks plausible. It is complete when the question, evidence boundary, method choice, and limits of the conclusion are all explicit.

## Frame the Question First

- State the research or decision question before changing code, methods, or notebook structure.
- Name the unit of analysis, time boundary, target or outcome, comparison group or baseline, and intended reader.
- Distinguish predictive, descriptive, diagnostic, and causal goals. Do not let the language imply a stronger claim than the method supports.

## Match Method to Claim

- Choose statistical or ML methods that fit the actual data structure, not the most advanced method available.
- Prefer a simpler method when it answers the question with fewer assumptions.
- Treat robustness checks, diagnostics, and alternative specifications as part of the result, not optional polish.

## Protect Validity

- Make leakage, selection effects, survivorship bias, missingness, and temporal ordering explicit before trusting a result.
- Check whether aggregation level, geography, or entity linkage could distort the inference.
- Separate exploratory signal from evidence strong enough to support a written conclusion.

## Claim Discipline

- Every important claim should map to a reproducible artifact: code, notebook cell sequence, figure, table, or documented metric.
- Avoid summary language like "proved" or "demonstrated" when the underlying method only supports association, ranking, or exploratory signal.
- Keep assumptions, exclusions, and threshold choices close to the claim they affect.

## Figures and Tables

- Prefer figures and tables that answer a specific question instead of dumping intermediate diagnostics into the main narrative.
- Label the population, time window, aggregation level, and metric definition clearly.
- Regenerate derived visuals and tables from code when data or methods change; do not hand-edit outputs that should be reproducible.

## Narrative Sync

- Update the written argument in the same change when the result changes materially.
- Revise limitations and caveats whenever a method, dataset boundary, or interpretation changes.
- If a repo maintains translated or paired analytical documentation, keep the companion surface aligned or explain the intentional divergence.

## Reproducibility

- Record dataset versions, extraction dates, seeds, filters, joins, and major parameters.
- Keep stable transformations and repeated statistics in committed code, not hidden notebook state.
- Make figures, tables, and narrative claims traceable to committed analysis artifacts.

## State Limits Honestly

- Document sample-size limits, proxy-label caveats, and measurement issues.
- If the work touches public policy, compliance, fairness, or governance, call out those constraints explicitly.
- Prefer a narrower defensible claim over a broad but weak conclusion.
- Surface uncertainty, non-results, and failed approaches when they materially affect the recommendation.
- Leave enough traceability that another engineer or reviewer can rebuild the evidence chain without private context.

---

# Security Check Required

Use this rule together with:

- `rules/security.md`
- `rules/command-safety.md`
- `rules/code-rules.md`
- `rules/operational-doc-required.md`

This file defines the minimum security gate for any new or modified content in this repository.

## Mandatory Policy

- Every new or modified code path must receive a security check before the task is considered complete.
- Every new or modified shell script, PowerShell script, batch file, workflow file, rule file, skill file, or operational document must receive the same security check treatment.
- Do not treat prompts, rules, workflows, rubrics, or setup docs as automatically safe. They can introduce insecure commands, secrets exposure, or unsafe operational guidance.

## Required Command

Run:

```bash
./scripts/security-check-toolkit.sh
```

## Minimum Expectations

- Review failures before considering the task done.
- If a check is skipped because the file type is absent, record that as an intentional skip, not as a silent omission.
- If a tool is unavailable in the current environment, install it or explicitly document the gap and the risk.
- Do not suppress findings without a concrete justification.
- Treat secret-scanning failures, unsafe command patterns, dependency findings, and workflow security issues as blocking by default.

## Files In Scope

Apply the mandatory check to changes under:

- `rules/`
- `workflows/`
- `scripts/`
- `.github/`
- `.codex/`
- `.agents/`
- `.cursor/`
- `.windsurf/`
- `integrations/`
- `docs/`
- `README.md`, `AGENTS.md`, `CLAUDE.md`, `INTENTS.md`
- other operational or automation files added later

## Windows Environment Setup

On Windows (Git Bash), install tools before running the security check:

**Binary tools via winget** (no admin required):
```bash
winget install Gitleaks.Gitleaks koalaman.shellcheck rhysd.actionlint AquaSecurity.Trivy Google.OSVScanner Anchore.Grype Anchore.Syft hadolint.hadolint Microsoft.PowerShell --accept-package-agreements
```

**Python tools via pip** (no admin required):
```bash
pip install --user yamllint semgrep checkov cfn-lint pip-audit
```

**PSScriptAnalyzer module** (for PowerShell linting):
```bash
pwsh -NoLogo -NoProfile -Command "Install-Module -Name PSScriptAnalyzer -Force -Scope CurrentUser"
```

After installation, restart your shell for PATH updates. Winget tools land in `%LOCALAPPDATA%\Microsoft\WinGet\Packages\`, pip tools in `%APPDATA%\Python\PythonXXX\Scripts`.

## Known Platform Considerations

- **Python 3.14 + Windows**: semgrep requires `PYTHONUTF8=1` to avoid `cp1252` encoding errors. The script sets this automatically.
- **PSScriptAnalyzer paths**: The script uses relative paths (not POSIX `/c/...` paths) so pwsh can resolve them on Windows.
- **ShellCheck severity**: The script uses `-S warning` to avoid failing on info-level findings (e.g., SC1091 for unresolvable `source` paths, SC2016 for intentional single-quote usage).
- **Disk space**: grype and trivy download vulnerability databases (~90 MB each). Ensure sufficient disk space or these checks will fail.

## Completion Standard

A task that changes in-scope files is not complete until `./scripts/security-check-toolkit.sh` has been run and its results have been reviewed.

---


# Security Rules

Security principles that apply to any application handling user data.

## Data Sensitivity

- Treat all user data as sensitive by default.
- **PII must never be logged** — no names, SSNs, addresses, DOBs, or identifiers in log output.
- Audit logging required for all data modifications in sensitive domains.
- Comply with relevant standards (e.g., CJIS, HIPAA, SOC2, GDPR) as applicable to the project.

## Permission Checks — Always Required

- Every new endpoint or API must have explicit authorization checks.
- Never bypass permission checks — not even for admin users.
- Default to deny / least-privilege.
- Gate UI features on permissions — don't just hide elements; enforce server-side.

## Authentication

- Use established auth patterns (session-based, JWT, OAuth) — never roll your own crypto.
- Enforce session timeouts for inactivity.
- Use CSRF protection on state-changing operations.
- API tokens for service-to-service communication.

## Data Protection

- Encrypt sensitive fields at rest.
- Use prepared statements / parameterized queries — **never** string-concatenated SQL.
- Sanitize all user inputs before display.
- **Never** store credentials in code or configuration files.
- Use a secret store (e.g., AWS Secrets Manager, Vault) or environment variables for secrets.

## No-Log PII Rule

```python
# WRONG — PII in logs
logger.info("Processing user: id=%s name=%s ssn=%s", user_id, name, ssn)

# CORRECT — log only opaque/internal IDs
logger.info("Processing user: id=%s", user_id)
```

Internal IDs (UUIDs, auto-increment PKs) are acceptable in logs. Never log names, emails, SSNs, phone numbers, addresses, or raw user-submitted data.

## Input Validation

```python
# WRONG — using raw user input without validation
user_id = request.get("user_id")
amount = request.get("amount")  # passed directly to business logic

# CORRECT — validate type and range before use
try:
    user_id = int(request["user_id"])
    amount = float(request["amount"])
    if amount <= 0:
        raise ValueError("amount must be positive")
except (KeyError, ValueError, TypeError) as e:
    raise ValidationError(f"Invalid input: {e}")
```

- Data from external sources (user input, webhooks, message queues) = **untrusted**. Always validate.
- Data from server-side config (env vars, admin-managed settings) = **trusted config** — but still validate format.

## NoSQL Injection Prevention

```python
# CORRECT — use SDK methods (parameterized by design)
table.put_item(Item={"user_id": user_id, "status": status})

# WRONG — never build query expressions via string concatenation
expr = f"user_id = {user_id}"  # injection risk
```

Most NoSQL SDKs (boto3/DynamoDB, MongoDB drivers) are parameterized by design. Never build filter expressions or queries via string concatenation with user-supplied values.

## Dependency Security

- New or updated dependencies may introduce known vulnerabilities.
- Check advisories (Snyk, Dependabot, npm audit, OWASP) when adding dependencies.
- Pin dependency versions in production.

## Fail Securely

- On validation error, raise/throw — do not persist partial or invalid data.
- On external service error, classify and re-raise — never silently swallow and return success.
- Error messages returned to clients should be generic — log detailed errors internally.

## PR Security Checklist

Before approving any PR, verify:

- [ ] No hardcoded secrets, credentials, or API keys
- [ ] All user-controlled inputs validated before use
- [ ] Parameterized queries / SDK builders used (no string concatenation for queries/JSON)
- [ ] Error messages don't leak stack traces or infrastructure details
- [ ] Log messages contain no PII, tokens, or credentials
- [ ] New dependencies checked for CVEs
- [ ] Exception handlers don't swallow errors silently
- [ ] Redirect URLs validated against allowlist (if applicable)

## Red Flags (always block merge)

- Clear injection vectors with user-controlled input.
- Missing or bypassable authentication/authorization on sensitive paths.
- Hardcoded secrets or credentials.
- Unsafe deserialization of external data.
- SSRF or open redirect with user-controlled target/URL.

---


# Testing Rules

Testing discipline that applies to any project and tech stack.

## Non-Negotiable Requirements

- **Never commit code without ALL related tests passing at 100%.**
- **You are responsible for running and verifying tests** — never ask the user to test on your behalf.
- **Every new or modified method requires test coverage** — happy path + edge cases + exceptions.
- **Always create tests for edge cases** — null/empty inputs, boundary values (zero, negative, max), single-element and large collections, off-by-one, concurrent access, and unexpected types. Edge case tests catch the bugs that happy-path tests miss.
- **Create test file if none exists** — mirror the source path under the test directory.
- **Keep tests DRY** — use named constants, shared builders, reusable fixtures.

## AC-to-Test Traceability

For every ticket, map each acceptance criterion (AC) to its test(s) before writing code. This prevents untested ACs — one of the main drivers of endless review cycles.

**Template** (add as a comment at the top of the primary test file for the ticket):

```
// AC Coverage for <TICKET>:
// AC-1: "<AC text>" → testMethod_or_testName()
// AC-2: "<AC text>" → testMethod_1(), testEdgeCase_2()
// AC-3: "<AC text>" → [MISSING — add before merge]
```

**Rules:**
- Each AC maps to one or more test names.
- If an AC has no test yet, mark it `[MISSING]` — this is a blocker.
- If an AC is untestable as written, flag it to the ticket author before starting implementation.
- E2E tests count as AC coverage for user-facing, integration-level ACs.

## Test Patterns

### Unit Tests
- Use the project's standard test runner (JUnit, Jest, pytest, etc.).
- Named constants over magic literals — define once per test file, reference everywhere.
- Structure: given/when/then or arrange/act/assert.
- Cover: happy path, null/empty inputs, boundary conditions, exception paths, collection edge cases.

### Integration / E2E Tests
- For API, cross-service, or database changes — ensure integration tests exist.
- For user-facing UI changes — add or update E2E tests.

## Test Coverage Checklist

- [ ] Happy path (valid input, expected output)
- [ ] Null/empty inputs (guard clauses tested)
- [ ] Collection edge cases (empty list, single element, large list)
- [ ] Exception paths (verify exception type and message)
- [ ] Boundary conditions (zero, negative, max values)

## Test Discipline Summary

1. **Read source before writing tests** — verify method signatures, return types.
2. **Test file mirrors source path** — create it if it doesn't exist.
3. **Named constants over magic literals** — define once, reference everywhere.
4. **DRY fixtures** — builder methods or shared test data, no copy-paste.
5. **Verify results yourself** — run every test after every code change.

---

# Review-and-fix workflow — multi-agent execution

Use when running `workflows/ticket-review-and-fix.md` or a project copy under `.windsurf/workflows/ticket-review-and-fix.md` with **multiple LLM agents** (parallel subagents).

## Emit phase results as you go

After each phase completes, output a short **Phase N result** block (bullet summary + tables). Do not wait until the end of the run to show Phase 1–3. This matches reviewer expectations and catches scope mistakes early.

### Phase result template (copy per phase)

```markdown
### Phase N — <Name> — **DONE**
- **Outcome:** …
- **Artifacts / commands:** …
- **Blockers for next phase:** none | …
```

## GitHub CLI — PR fields that break `gh pr view --json`

- `reviewThreads` is **not** a valid `--json` field on current `gh` (use API below).
- For inline review comments:  
  `gh api repos/<owner>/<repo>/pulls/<N>/comments`
- For issue-style comments:  
  `gh pr view <N> --json comments`
- For reviews list:  
  `gh pr view <N> --json reviews,latestReviews`

## Parallel agent split (Phase 2)

| Agent | Signal | Task |
|-------|--------|------|
| A | **S1** | Map ticket ACs to code paths + tests; flag missing implementation/tests. |
| B | **S2** | PR comments / review threads vs current diff (use `gh pr view` in the **orchestrator**, not assumed by subagents unless they can run it). |
| C | **S3** | Rubric/security pass on changed files; SDK errors by `Error["Code"]`, not `__class__.__name__`; log redaction. |

Subagents **must read files from disk** in the repo path provided. Treat subagent summaries as **hypotheses** until the orchestrator spot-checks cited lines.

## Pitfalls (from production runs)

### Shell on Windows (PowerShell)

- Do **not** use Bash-only chaining: `&&`, `||`, `2>nul` as on `cmd` may not parse in older PowerShell.
- Prefer: `Set-Location <repo>; <cmd>` on separate statements or use `;` between commands.
- When computing merge-base for `git diff`, resolve the base ref first, then pass it to `git diff` (avoid nested `$(...)` that assumes Bash).

### Python exception handling

- A broad `except Exception` after `raise SomeDomainException(...)` **will catch that same exception** if it propagates through an inner call in some layouts. Prefer `except LambdaException: raise` (or your domain base) **before** the generic handler when intentional domain exceptions must escape.
- **Non-200 persistence / missing return:** if a `try` block only `return`s inside `if status == 200`, a non-200 path can fall through and return `None`. Always `raise` or `return` explicitly.

### Subagent accuracy

- Subagents may **hallucinate** line numbers or “already fixed” status. The orchestrator should **open the file** for any Blocker before editing.
- If the repo’s **canonical workflow** lives in a **separate dev-tools / skills repository** but the project only has `.windsurf/workflows/ticket-review-and-fix.md`, keep them in sync when process rules change.

### Datetime testing (Python 3.12+)

- Prefer `datetime.datetime.fromtimestamp(..., tz=datetime.timezone.utc).astimezone(tz)` over deprecated `utcfromtimestamp`.
- **Python 3.14+:** `unittest.mock.patch.object(datetime.datetime, "now", ...)` can fail (`immutable type`). Prefer:
  - deterministic tests using a **fixed `TS`** in the payload, or
  - dependency-injecting a clock, or
  - `freezegun` if the project already depends on it — not `patch` on built-in `datetime.datetime.now`.

### Phase 9 (git reset + five commits + force push)

- The workflow may mandate `git reset --mixed origin/<base>` and push to `stage`. These are **destructive** on shared branches. The orchestrator should **confirm with the user** before rewriting history or force-pushing, unless the user explicitly ordered full Phase 9.

### Allowlist-style HTTP gateways

- See **`rules/allowlist-controller-review-pitfalls.md`** — runtime allowlists vs test fixtures, logging around redirects/tokens, Windows build-plugin noise.

## Related

- `rules/ci-feedback-loop.md` — after CI failures, capture learnings in rules.
- `rules/allowlist-controller-review-pitfalls.md` — allowlist + test alignment and logging pitfalls.
- `workflows/ticket-review-and-fix.md` — full phase list and 5-category commit table.
