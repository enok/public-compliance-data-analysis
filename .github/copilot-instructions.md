# Copilot Instructions (public-compliance-data-analysis)

> Auto-generated from `.windsurf/rules/` by `sync-tool-configs.sh`.
> Curated by `docs/llm/toolkit-selection.txt` for this repository.


# Analysis Artifact Governance

Apply this rule when a repository contains notebooks, reports, translated docs, papers, or presentation-oriented deliverables.

1. Define the source of truth for every artifact pair, such as English plus translated docs, notebook plus exported report, or code plus generated figure.
2. When a paired artifact changes, update both sides or leave an explicit note describing the remaining drift and where it will be resolved.
3. Prefer text-friendly committed summaries such as Markdown, JSON, CSV, or code over opaque binary-only outputs when preserving results or review evidence.
4. Keep filenames, numbering, and headings aligned with pipeline stages or analysis sequence so reviewers can map outputs back to code and data.
5. Charts, tables, and conclusions should include enough context to reproduce them: dataset, time window, aggregation level, filters, and metric definitions.
6. Do not commit heavy notebook outputs or generated datasets unless the repository already treats those artifacts as source-of-truth inputs.

---


# Analytical Dataset Governance

- Every analytical dataset should declare its entity grain, time grain, primary keys, and intended use before new features are added.
- Keep feature engineering aligned with the question being answered. Regression, classification, and clustering datasets may need different grains and filtering rules.
- Derived columns must be traceable to committed transformations, documented notebook cells, or config-driven rules.
- Name features so units and time meaning stay obvious. Prefer columns like `income_change_pct` or `sanctions_per_100k` over ambiguous short names.
- When aggregating from one entity level to another, validate that the numerator, denominator, and weighting strategy still match the analytical objective.
- Treat translation maps and loader dataset maps as part of the analytical contract when localized notebooks or reports depend on them.
- Add or update tests when new analytical datasets, feature columns, or dataset aliases are introduced.
- If a feature can leak future information or post-outcome signals, exclude it or document the reason it is still valid for the task.

---


# Analytics Reproducibility

Analytical work should be repeatable by another engineer without hidden notebook state, missing environment details, or undocumented data assumptions.

## Notebook Discipline

- A notebook that matters must restart cleanly and run all cells in order.
- Keep narrative markdown, assumptions, and interpretation in the notebook; keep reusable transformations in code.
- Do not rely on manually mutated in-memory state across cells.
- Keep output-heavy notebooks reviewable by avoiding unnecessary binary output churn.

## Determinism

- Set explicit random seeds for sampling, splitting, and model training when randomness is involved.
- Record the dataset snapshot, source path, or extraction date that supports the analysis.
- Make environment assumptions explicit: Python version, required packages, cloud profile, local paths, or feature flags.

## Reuse Boundary

- If logic is reused across notebooks, scripts, or production code, extract it into `src/`, a shared utility, or a documented pipeline step.
- Avoid copy-pasting business logic across notebooks.
- Prefer loaders and helper functions that make data access explicit and testable.

## Evidence Hygiene

- Treat plots, tables, and markdown conclusions as derived artifacts that should be reproducible from committed code.
- Do not hand-edit generated tables, cached exports, or audit logs unless the task is specifically about repairing a broken artifact.
- When a result depends on filtering or exclusions, document the rule next to the result.

## Statistical Honesty

- State sample-size limitations and uncertainty clearly.
- Separate exploratory findings from confirmed conclusions.
- If a notebook changes project-facing conclusions, update the related docs in the same change.

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


# Data Artifact Hygiene

- Do not commit generated datasets, temporary exports, cache files, credentials, or local-only environment state.
- Large plots, tables, and reports should be committed only when they are intentionally reviewed artifacts and their regeneration path is documented.
- Prefer reproducible source code, configs, and small markdown summaries over opaque binary artifacts.
- Keep notebook outputs focused. Avoid committing noisy cell output that obscures the logic or inflates diffs unless the output itself is the reviewed artifact.
- Never hardcode local machine paths, personal buckets, or ad hoc file drops inside notebooks or scripts.
- Logs and audit artifacts should use stable names and timestamps so reruns are easy to compare.
- When the repo contains sensitive public-sector or compliance data, double-check masking, row-level exposure, and screenshots before committing anything derived from it.

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
| 1 | **LLM configs** | `.windsurf/`, `.agents/`, `.claude/`, `AGENTS.md`, `.gitignore` |
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

## Rebase Before Push

- **Always** rebase onto the base branch before every `git push`.
- Steps (always in this order):
  1. `git fetch origin`
  2. `git rebase origin/main` (or the PR base branch)
  3. Resolve any conflicts
  4. Re-run the full test suite to ensure nothing broke
  5. `git push --force-with-lease`
- **Never** push without rebasing first — even if the branch was recently created.
- **Never** use `git merge` to integrate upstream changes — rebase only.

---


# Jupyter Notebook Hygiene

Committed notebooks should remain understandable and rerunnable even after the original author is gone.

## Execution Discipline

- A committed notebook should restart cleanly and run all cells in order.
- Do not rely on hidden kernel state, out-of-order execution, or manually patched variables.
- Keep environment assumptions explicit: kernel, Python version, packages, credentials, data paths, and cloud profile.

## Structure

- Use markdown cells to explain the question, the data inputs, the key assumptions, and the interpretation of results.
- Keep imports and configuration near the top so a reviewer can understand the runtime quickly.
- Prefer loader functions and reusable utilities over repeated inline data-access code.

## Output Hygiene

- Keep large or noisy outputs out of committed notebooks unless they are essential to the review.
- Clear stale outputs when they no longer match the current code or data.
- Do not commit massive datasets, local cache artifacts, or screenshot-only evidence in place of reproducible code.

## Promotion Boundary

- If logic will be reused, tested, scheduled, or shared, move it into `src/`, a script, or a documented pipeline step.
- Let notebooks orchestrate analysis and interpretation; let reusable code handle stable transformations and business logic.

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


# ML Evaluation And Experimentation

Use this rule when work affects feature engineering, model training, hyperparameters, evaluation, thresholding, or model comparison.

## Problem Framing

- State the prediction target, unit of analysis, and decision use before changing features or models.
- Start with simple baselines before moving to more complex models.
- Match the split strategy to the problem. Respect temporal, geographic, and grouped leakage boundaries when they exist.

## Leakage Prevention

- Split before fitting transforms whenever the transforms learn from data.
- Do not leak post-outcome information, future periods, or target-derived features into training inputs.
- Be explicit about feature provenance for every derived variable used in modeling.

## Evaluation

- Report metrics that match the problem type and decision cost, not just what is easiest to compute.
- Include sample size, class balance, and important caveats with every evaluation summary.
- For small datasets, prefer robust validation and careful interpretation over aggressive optimization claims.
- Compare against a clear baseline and explain why a more complex model is justified.

## Interpretation And Review

- Inspect feature importance, error slices, residual behavior, or confusion patterns before claiming success.
- Do not present predictive association as causal evidence.
- Document threshold choices, tradeoffs, and known failure modes if the model output is turned into a decision signal.

## Artifact Discipline

- Version the code and document the data slice used for each reported result.
- Keep experiment conclusions in committed docs, scripts, or notebooks so future sessions can reconstruct what changed.

---


# ML Experiment Discipline

Apply this rule when a repository contains notebooks, training code, feature pipelines, model evaluation, or experiment artifacts.

1. Record reproducibility inputs: dataset version or time window, filters, target definition, feature list, random seeds, split strategy, and metric definitions.
2. Guard against leakage. Do not train on post-outcome fields, target-derived transforms, future information, or preprocessing that was fit on evaluation data.
3. Keep exploration separate from reusable logic. When a loader, transform, feature builder, or metric becomes stable, move it into versioned source code and import it from notebooks.
4. Compare against simple baselines and explain why the chosen metrics make sense for the sample size, class balance, and operational use case.
5. Distinguish analysis artifacts from operational artifacts. Figures and tables for reports are not a substitute for versioned training code, evaluation outputs, or deployment inputs.
6. Document major model limitations, fairness or bias caveats, and operational assumptions when outputs influence prioritization or decisions.
7. Generated conclusions should be traceable back to code, data, parameters, and the exact artifact that produced them.

---


# ML Experiment Governance

- Start with a clearly stated prediction or analytical objective, then establish a simple baseline before proposing a more complex model.
- Choose split strategies that match the data-generating process. Respect time, geography, entity grouping, and small-sample constraints.
- Check leakage explicitly whenever features are aggregated from downstream outcomes, future periods, or labels that would not be available at prediction time.
- Reuse stable feature generation code from `src/` or other committed paths when notebook experiments become important to the project.
- Report metrics that match the task:
  - Regression: error metrics, residual behavior, and baseline comparisons.
  - Classification: class balance, threshold choice, confusion-matrix tradeoffs, and calibration when relevant.
  - Clustering: stability, silhouette or alternate diagnostics, and interpretability of cluster profiles.
- Document uncertainty, data limitations, and sample-size caveats before drawing strong policy or compliance conclusions.
- Favor interpretable features and explainability when the output will influence public-sector, compliance, or anomaly discussions.

---


# ML Experiment Rigor

Machine learning changes are not complete when the code runs. They are complete when the target, data boundaries, evaluation method, and limitations are all explicit.

## Define the Task First

Before changing features or models, write down:

- the prediction or decision objective
- the unit of analysis
- the target definition
- the inference or prediction time boundary
- the intended consumer of the output

## Prevent Leakage

- Split data using the boundary that matches real-world inference: time, entity, geography, or experiment cohort.
- Do not let features depend on information that would be unavailable at prediction time.
- Treat pre-aggregated analytical tables, marts, and notebook outputs as potentially leaky until feature provenance is verified.

## Baselines And Metrics

- Always compare against a simple baseline before claiming improvement.
- Choose metrics that match the real decision: regression error, ranking quality, calibration, precision/recall, or class imbalance sensitivity.
- If thresholds are used, make threshold selection explicit and document the tradeoff.

## Reproducibility

- Keep feature generation and preprocessing deterministic where possible.
- Version the feature source or dataset snapshot used in the experiment.
- Prefer reusable pipelines or scripts over notebook-only model training logic when the work may be revisited.

## Interpretation And Limits

- Report limitations honestly, especially for small samples, aggregate datasets, or proxy labels.
- Separate exploratory signal from production-ready evidence.
- If the project has governance, fairness, or public-policy constraints, document them with the experiment result.

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

When the active workflow has separate read-only or non-overlapping write lanes, parallelize by default (see each workflow’s steps): **review**, **review-and-fix**, **pre-pr-check**, **project-discovery**, **run-tests**, **ticket-research**, **gh-fix-ci**, **gh-address-comments**, **environment-diagnose**, **security-report**, **update-docs**, **cross-repo-impact**, **dependency-upgrade**, and **document-creation** (when phases are independent). **Enable Max Mode** (or equivalent) in the IDE so subagents using `model: inherit` run at full capability.

## Tool mapping

- **Cursor**: project subagents live in `.cursor/agents/` (see `.cursor/README.md`). Prefer `/agent-name` or natural-language delegation; combine with rules in `.cursor/rules/`.
- **Claude / Codex**: use `.claude/agents/` for shared agent files. For Codex, keep `AGENTS.md` as the primary repo-level surface and use the optional `.codex/skills/` mirror when it helps discoverability.
- **Windsurf / generic**: follow the same principles using the IDE’s task or multi-chat capabilities.

---


# Notebook Engineering

Use this rule when editing or creating Jupyter notebooks for EDA, statistics, ML, or reporting.

## Notebook Role

- Use notebooks for exploration, explanation, visualization, and result narration.
- Move reusable business logic, data loading, feature engineering, and evaluation code into `src/` or scripts.
- Avoid keeping the only copy of important transformation logic inside a notebook.

## Structure

- Start with a short setup section that states data prerequisites, configuration, and assumptions.
- Keep cell order linear and restart-friendly. The notebook should work from top to bottom after `Restart Kernel and Run All`.
- Group sections clearly: setup, data load, quality checks, analysis, interpretation, and next steps.

## Hygiene

- Avoid secrets, personal paths, and machine-specific assumptions.
- Avoid committing large, noisy outputs unless the repo explicitly wants them versioned.
- Keep plots, tables, and markdown conclusions close to the code that produces them.

## Promotion Rule

- If a notebook cell becomes operational, repeated, or testable logic, promote it into Python modules or scripts.
- If a finding affects product, pipeline, or report decisions, reflect it in committed docs or tests instead of leaving it only in notebook prose.

---


# Notebook Reproducibility

- Use notebooks to explain and inspect analysis, not to hide production-only transformation logic.
- Keep reusable data loading, feature engineering, and utility code in committed Python modules once it becomes important beyond a single exploration.
- Every important notebook should make these items easy to find near the top:
  - data source or loader entrypoint
  - key assumptions and filters
  - random seeds or deterministic settings
  - expected outputs or saved artifacts
- Re-run notebooks from a clean kernel before treating results as final. Hidden state is not an acceptable dependency.
- Avoid hardcoded local paths and untracked data drops. Use repository loaders, config, or documented environment variables instead.
- Statistical claims in markdown cells should match the code that produced them. Update narrative text when assumptions, samples, or metrics change.
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


# Research Rigor

Analytical research is not complete when a model runs or a chart looks plausible. It is complete when the question, evidence boundary, method choice, and limits of the conclusion are all explicit.

## Frame The Question First

- State the research or decision question before changing code, methods, or notebook structure.
- Name the unit of analysis, time boundary, target or outcome, comparison group or baseline, and intended reader.
- Distinguish predictive, descriptive, diagnostic, and causal goals. Do not let the language imply a stronger claim than the method supports.

## Match Method To Claim

- Choose statistical or ML methods that fit the actual data structure, not the most advanced method available.
- Prefer a simpler method when it answers the question with fewer assumptions.
- Treat robustness checks, diagnostics, and alternative specifications as part of the result, not optional polish.

## Protect Validity

- Make leakage, selection effects, survivorship bias, missingness, and temporal ordering explicit before trusting a result.
- Check whether aggregation level, geography, or entity linkage could distort the inference.
- Separate exploratory signal from evidence strong enough to support a written conclusion.

## Keep Work Reproducible

- Record dataset versions, extraction dates, seeds, filters, joins, and major parameters.
- Keep stable transformations and repeated statistics in committed code, not hidden notebook state.
- Make figures, tables, and narrative claims traceable to committed analysis artifacts.

## State Limits Honestly

- Document sample-size limits, proxy-label caveats, and measurement issues.
- If the work touches public policy, compliance, fairness, or governance, call out those constraints explicitly.
- Prefer a narrower defensible claim over a broad but weak conclusion.

---


# Repository context layers

When multiple instruction sources apply (toolkit rules, workflows, repo `AGENTS.md`, optional `CLAUDE.md`, IDE rules):

1. **Merge in memory** during the turn: follow the most complete, **non-conflicting** workflow.
2. Keep the **union** of non-conflicting requirements; where they conflict, **stricter** verification, security, and documentation requirements win unless the user explicitly overrides.
3. **Repo-local** files (`AGENTS.md`, committed `docs/*`, local `skills/` playbooks) are **first-class** for that repository. Do not overwrite them when refreshing shared toolkit symlinks.
4. **Shared toolkit** content (`rules/`, `workflows/`, `.agents/skills/`) is the default for generic behavior across projects.
5. Some teams maintain a **provider-specific** skill directory (for example Codex-only copies). Treat those as additive; keep portable authoring in `.agents/skills/` when possible.

---

# Review-and-fix workflow — multi-agent execution

Use when running `workflows/review-and-fix.md` or a project copy under `.windsurf/workflows/review-and-fix.md` with **multiple LLM agents** (parallel subagents).

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
- If the repo’s **canonical workflow** lives in a **separate dev-tools / skills repository** but the project only has `.windsurf/workflows/review-and-fix.md`, keep them in sync when process rules change.

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
- `workflows/review-and-fix.md` — full phase list and 5-category commit table.

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


# Evidence-Based Reporting

Written conclusions must stay synchronized with the evidence that produced them.

## Claim Discipline

- Every important claim should map to a reproducible artifact: code, notebook cell sequence, figure, table, or documented metric.
- Avoid summary language like "proved" or "demonstrated" when the underlying method only supports association, ranking, or exploratory signal.
- Keep assumptions, exclusions, and threshold choices close to the claim they affect.

## Figures And Tables

- Prefer figures and tables that answer a specific question instead of dumping intermediate diagnostics into the main narrative.
- Label the population, time window, aggregation level, and metric definition clearly.
- Regenerate derived visuals and tables from code when data or methods change; do not hand-edit outputs that should be reproducible.

## Narrative Sync

- Update the written argument in the same change when the result changes materially.
- Revise limitations and caveats whenever a method, dataset boundary, or interpretation changes.
- If a repo maintains translated or paired analytical documentation, keep the companion surface aligned or explain the intentional divergence.

## Reader Trust

- Prefer concise, testable statements over persuasive or marketing-style phrasing.
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

## Completion Standard

A task that changes in-scope files is not complete until `./scripts/security-check-toolkit.sh` has been run and its results have been reviewed.

---

# Repo-Local Rules

# AWS Airflow Terraform Rule

Use this repo-local rule after the shared toolkit guidance in `.windsurf/rules/aws-airflow-terraform.md`.

This file should only capture how the shared Airflow-on-AWS Terraform guidance applies to this repository.

## Repository-Specific Deltas

- Treat Terraform in `infra/` as the source of truth for AWS infrastructure in this repo.
- Treat `scripts/01_bronze_ingestion.sh`, `scripts/02_silver_transformation.sh`, and `scripts/03_gold_transformation.sh` as the source of truth for implemented ETL entry points unless real DAG code is added.
- Do not assume Airflow orchestration already exists here just because the task mentions it. Confirm whether the change is:
  - infrastructure-only
  - DAG introduction
  - ETL task migration from shell orchestration into Airflow
  - documentation or operating-model guidance only
- Preserve Bronze, Silver, and Gold boundaries when translating script execution into Airflow tasks.
- Preserve current S3 naming and dataset contract assumptions unless the change is an intentional migration.
- If Airflow introduces a new orchestration surface, update docs to explain how it relates to existing shell scripts and whether those scripts remain canonical, wrapped, or deprecated.
- If ETL behavior changes, update:
  - `config/` contracts when needed
  - `tests/`
  - `docs/`
  - operating commands or deployment notes

---

# Course Material Grounding

Use this rule when the task asks about MBA classes, lecture materials, class-derived methods, or how the local `aulas/` corpus should influence analysis choices.

## Source Of Truth

- Treat `/mnt/hgfs/shared/data-science/aulas` as the primary source corpus in this environment.
- Start with `docs/llm/references/usp-mba-course-map.md` for the curated view.
- Use `docs/llm/references/usp-mba-course-inventory.generated.md` when you need full-course coverage, representative assets, or deeper traceability.

## Boundary

- Keep generic Data Science, notebook, ML, experimentation, and reproducibility guidance in the shared `dev-tools` toolkit.
- Keep USP MBA course mappings, thesis framing, and class-specific method preferences in `docs/llm/`.
- Do not restate class-specific course content as if it were universal best practice.

## How To Use The Corpus

- Match the user request to the nearest course family first: statistics, wrangling, engineering, supervised ML, unsupervised ML, NLP, visualization, governance, TCC, or writing.
- Prefer methods and terminology already present in the course corpus before introducing outside techniques.
- When a recommendation goes beyond the class material, say so explicitly and explain why the extra method is justified.
- Keep claims tied to the evidence level of the referenced class material and the actual repository data available.

## Repository Fit

- For thesis-facing tasks, pair course guidance with `docs/llm/rules/usp-mba-course-context.md` and `docs/llm/rules/tcc-deliverables-and-argument.md`.
- For data and modeling work, pair course guidance with shared rules on reproducibility, dataset governance, notebook engineering, and ML experimentation.

---

# Data Pipeline Boundaries

Use this repo-local rule after the shared toolkit guidance in `.windsurf/rules/data-pipeline-contracts.md`.

This file should only capture the contract sources and boundary decisions that are specific to this repository.

## Source Contracts

- `config/ibge_metadata.json` is the authoritative source for IBGE dataset definitions.
- `config/transparency_metadata.json` is the authoritative source for Transparency Portal dataset definitions.
- `config/silver_schemas.json` is the authoritative schema contract for Silver outputs.
- `scripts/01_bronze_ingestion.sh`, `scripts/02_silver_transformation.sh`, and `scripts/03_gold_transformation.sh` are the implemented orchestration entry points in this repo.

If code behavior changes, update the relevant metadata or schema file in the same change whenever the contract changed.

## Storage Conventions

- Preserve stable S3 key naming unless the change is a deliberate migration.
- Keep municipality identifiers and year semantics explicit in column names and output tables.
- Prefer narrow, explicit transformations over hidden side effects in utility helpers.
- Treat `src/analysis/` loaders as downstream Gold consumers, not as places to hide pipeline-side transformations.
- Do not assume `dags/` is the active orchestration source unless the task includes real DAG code changes.

## Repository-Specific Review Surfaces

For any pipeline change here, inspect the impact on:

- metadata files in `config/`
- source client code in `src/ingestion/`
- transformer code in `src/processing/`
- CLI or shell entry points in `scripts/`
- downstream analysis loaders in `src/analysis/` when Gold outputs or dataset names change
- tests under `tests/`
- technical docs under `docs/`

---

# Documentation And Governance

Use this repo-local rule after the shared toolkit guidance in:

- `.windsurf/rules/bilingual-doc-sync.md`
- `.windsurf/rules/security.md`
- `.windsurf/rules/evidence-based-reporting.md`

This file should only capture repository-specific documentation and governance constraints.

## Repository Documentation Policy

- Keep repository-specific LLM configuration in English.
- Keep technical implementation docs in English first for this repo.
- When an English document already has a maintained `pt-BR` counterpart, update both or explicitly note the drift.

## Research And Data Governance

- Treat public-sector data lineage and auditability as first-class requirements.
- Do not introduce changes that weaken provenance tracking, metadata completeness, or reproducibility.
- Do not turn public aggregate analysis into individual profiling logic.

## Logs And Generated Artifacts

- `docs/data_sources.log` and similar operational logs are evidence artifacts, not narrative documentation.
- Do not hand-edit generated logs unless the task explicitly requires correction or cleanup.
- Prefer updating the generator or the pipeline behavior over patching generated outputs manually.

## Documentation Update Triggers

Update documentation when a change affects:

- dataset coverage
- schema shape
- ingestion strategy
- transformation logic
- infrastructure setup
- operational commands
- reproducibility expectations

## Security Review Trigger

- Any new or modified code, script, workflow, rule, skill, or operational document must also pass the mandatory security-check workflow defined in `docs/llm/rules/security-check-required.md`.

---

# Project Overview

## Purpose

This repository supports an MBA thesis focused on public spending efficiency, compliance risk, and anomaly detection in Brazil.

The project combines:

- federal transfer data
- socioeconomic census indicators
- sanctions and compliance registries
- a medallion data pipeline on AWS
- bilingual analysis and documentation surfaces

## Technical Scope

- `config/`: dataset contracts and metadata definitions
- `src/ingestion/`: Bronze ingestion clients and helpers
- `src/processing/`: Silver and Gold transformations
- `src/analysis/`: notebook-facing data access helpers, including localized loaders
- `scripts/`: the implemented Bronze, Silver, and Gold orchestration entry points
- `dags/`: currently not the primary orchestration surface; do not assume MWAA DAG code exists here
- `infra/`: Terraform for the S3-backed data lake foundation and supporting AWS setup
- `notebooks/`: bilingual EDA, statistical analysis, machine learning, and clustering notebooks
- `tests/`: pytest suites for ingestion, processing, and analysis
- `docs/`: technical and research documentation

## Working Assumptions

- English is the default language for LLM configuration and technical guidance.
- Dataset metadata files in `config/` are part of the system contract and must be updated carefully.
- Bronze, Silver, and Gold boundaries are intentional and should not be blurred by convenience changes.
- This repository mixes engineering, analytics, and research documentation, so changes often require code and docs updates together.
- Repo code currently implements Python scripts plus S3-oriented data flows; references to MWAA or SageMaker in docs should be treated as adjacent context unless backed by committed code or infrastructure.

## Less Relevant Shared Rules

The shared toolkit includes rules for technologies that are not primary in this repository. Do not spend context on them unless the task truly needs them:

- frontend-specific guidance
- Java-specific guidance
- general JS/TS guidance outside shell tooling or documentation helpers

---

# Security Check Required

Use this repo-local rule together with:

- `.windsurf/rules/security.md`
- `.windsurf/rules/command-safety.md`
- `.windsurf/rules/code-rules.md`
- `.windsurf/rules/documentation-and-governance.md`

This file defines the repository-specific minimum security review gate for changes made in this repository and for related work in `dev-tools`.

## Mandatory Policy

- Every new or modified code path must receive a security check before the task is considered complete.
- Every new or modified shell script, PowerShell script, batch file, workflow file, rule file, skill file, or operational document must receive the same security check treatment.
- This applies to implementation code, infrastructure code, automation, local LLM configuration, and supporting operational content.
- Do not treat documentation-like files as automatically safe. Rules, workflows, prompts, scripts, and operational notes can introduce insecure commands, secrets exposure, or unsafe guidance.

## Required Commands

For this repository, run:

```bash
./scripts/security_check_this_repo.sh
```

For a `dev-tools` checkout, run:

```bash
./scripts/security_check_dev_tools.sh /path/to/dev-tools
```

## Minimum Expectations

- Review failures before considering the task done.
- If a tool is skipped because the file type is absent, record that as an intentional skip, not as a silent omission.
- If a tool is unavailable in the current environment, install it or explicitly document the gap and the risk.
- Do not suppress findings without a concrete justification.
- Treat secret-scanning failures, command-injection patterns, unsafe script behavior, and workflow security issues as blocking by default.

## Files In Scope

Apply the mandatory check to changes under:

- `src/`
- `scripts/`
- `tests/`
- `infra/`
- `.github/`
- `.codex/`
- `.agents/`
- `.windsurf/` when repo-local usage depends on it
- `docs/llm/`
- other operational or automation files added later

## Completion Standard

A task that changes in-scope files is not complete until the relevant security-check script has been run and its results have been reviewed.

---

# TCC Deliverables And Argument

Use this rule for any change that affects thesis-facing evidence, figures, tables, chapter conclusions, or methodology explanations.

Apply the shared toolkit guidance in `.windsurf/rules/research-rigor.md` and `.windsurf/rules/evidence-based-reporting.md` first. This file should only capture thesis-specific deliverable expectations for this repository.

## Deliverable Surfaces

For this repository, a meaningful thesis change may touch more than code. Check the full deliverable set:

- reusable pipeline or analysis code in `src/` or `scripts/`
- notebooks under `notebooks/`
- technical documentation under `docs/`
- repo-local LLM guidance under `docs/llm/`
- bibliography or chapter-oriented markdown when the analytical narrative changes

## Thesis-Specific Narrative Expectations

- If a result is exploratory, label it as exploratory in both notes and final narrative.
- If a result depends on exclusions, proxy labels, aggregation choices, or incomplete years, say that explicitly in the thesis-facing surfaces.
- For compliance-risk or public-spending findings, keep the narrative focused on indicators, patterns, or associations rather than accusations.

## Figure And Table Expectations

- Each thesis-facing figure or table should answer one clear question.
- Label municipality scope, time window, metric definition, and transformation choices.
- Prefer a smaller set of interpretable visuals over a large appendix of weak or redundant charts.

## Chapter Sync

- Methodology changes should update method notes and limitation notes together.
- Results changes should update interpretation and implication notes together.
- If a Portuguese or alternate audience-facing version exists, keep the companion narrative aligned or explain why it intentionally differs.

---

# USP MBA Course Context

Use this rule when the task mentions the thesis, TCC, USP MBA classes, methodology, chapter structure, advisor feedback, or "which method should I use".

## Working Reference

- Start with `docs/llm/rules/course-material-grounding.md` when the task needs full course-corpus grounding.
- Start with `docs/llm/references/usp-mba-course-map.md`.
- Use `docs/llm/references/usp-mba-course-inventory.generated.md` only when you need deeper traceability to the source class materials.

## Context For This Repository

- This repository is not a generic ML sandbox. It is a thesis repository about public spending efficiency, compliance risk, and anomaly detection with municipal-level public data.
- Prefer methods already covered in the MBA course map before introducing a more advanced technique from outside the course corpus, unless there is a clear gap the thesis genuinely needs.
- If the user asks about a specific class not already highlighted in the curated map, look it up in the generated inventory before answering from memory.
- Treat `11_tcc`, `15_TCC`, and `17_Fundamentos-de-redacao-tecnico-cientifica` as mandatory support modules whenever the work changes the written argument.
- Treat `14_Engenharia-de-dados`, `03_Data-Wrangling`, and the medallion pipeline rules as the default frame for new data engineering work in this repo.

## Method Selection Bias

- Default to interpretable baselines first: descriptive statistics, regression, spatial analysis, and transparent segmentation methods.
- Use deeper ML only when it adds decision-relevant signal beyond the simpler baseline and the thesis can explain it clearly.
- Keep exploratory clustering, factor analysis, correspondence analysis, or logistic variations as supporting analyses unless they become central to the research question.

## Writing And Defense Readiness

- When analytical evidence changes, update the nearest thesis-facing narrative in the same change.
- Keep claims consistent with the course framing: association is not causality, exploratory structure is not proof, and risk indicators are not direct accusations.
- Use LGPD and public-governance framing from the course context when discussing legal or ethical boundaries.

---

# Workflows


# AWS Data Pipeline Ops Workflow

Use this workflow for operational inspection of AWS-backed data pipelines, especially S3 data lakes, MWAA or Airflow orchestration, CloudWatch logs, and pipeline secrets.

---

## Step 1: Confirm environment targeting

Before running commands, identify:

- AWS profile or credential source
- Region
- Environment name
- Bucket names and prefixes
- MWAA environment, Airflow DAG, or job identifiers

Do not guess production resource names.

---

## Step 2: Start with read-only inspection

Prefer read-only commands first:

```bash
aws sts get-caller-identity
aws s3 ls
aws s3 ls s3://<bucket>/<prefix>/
aws mwaa get-environment --name <environment> --region <region>
aws logs describe-log-groups --query "logGroups[].logGroupName"
aws logs tail <log-group> --since 1h
aws secretsmanager describe-secret --secret-id <secret-name>
```

Reduce output with `--query`, `--output json`, or precise prefixes so results stay reviewable.

---

## Step 3: Inspect the specific failure surface

Choose the right inspection path:

- **Missing data**: S3 prefixes, object timestamps, partition layout
- **Failed orchestration**: MWAA environment health, Airflow DAG run state, scheduler or task logs
- **Auth or config issues**: caller identity, profiles, region mismatch, missing secret or env name
- **Slow or partial runs**: CloudWatch logs, task retries, throttling, pagination, checkpoint or metadata behavior

---

## Step 4: Correlate code and cloud state

Match what the cloud shows against repo truth:

- Config files and schema definitions
- DAG IDs and task names
- Expected bucket and prefix layout
- Time windows, backfill logic, and partitioning
- Recent code or docs that changed the contract

---

## Step 5: Escalate writes deliberately

If you need a non-read-only action such as rerun, upload, delete, or mutate a resource:

- State the exact command
- State the target resource
- State the expected outcome
- Get explicit user approval before execution

Default posture is inspect first, mutate second.

---


# AWS Data Platform Ops Workflow

Use when a task requires AWS CLI inspection, S3 verification, MWAA environment checks, pipeline triggers, or infrastructure-adjacent operational work.

## Steps

1. Confirm operating context:
   - AWS profile
   - region
   - account or environment
   - bucket and target prefix
   - MWAA environment name if applicable
2. Classify the action:
   - inspect only
   - pipeline execution
   - data validation or audit
   - infrastructure apply or teardown
3. Prefer narrow read commands first such as list, head, or describe.
4. Use repo scripts when available for repeatable operations.
5. Before any write or trigger action, state:
   - what will be affected
   - what success looks like
   - what evidence will be captured
6. After the operation, verify the result using the destination system rather than trusting command success alone.
7. If the operation is likely to repeat, update or create a runbook.

## Exit Criteria

- Scope and environment were verified.
- Result was checked in AWS or the produced artifact.
- Reusable operational knowledge was captured.

---


# AWS Airflow Terraform Change

Use when introducing or modifying Terraform-managed Apache Airflow on AWS.

## Steps

1. Classify the operating model:
   - Amazon MWAA
   - self-managed Airflow on AWS
   - infrastructure bootstrap only
   - full orchestration migration
2. Confirm what belongs in infrastructure versus code:
   - Terraform or OpenTofu for AWS resources, IAM, networking, storage, and environment settings
   - DAG code for orchestration logic
   - application code or scripts for reusable ETL logic
3. Design the minimum dependency set explicitly:
   - DAG, plugin, and requirements storage
   - execution roles and policies
   - VPC, subnets, security groups, and egress path
   - logging and encryption dependencies
   - optional secret-store integration
4. Decide the Terraform shape before writing resources:
   - simple root configuration extension
   - reusable modules
   - environment separation
5. Keep deployment inputs explicit:
   - Airflow version
   - environment class and scaling
   - network IDs
   - bucket names and prefixes
   - requirements, plugins, and startup-script object paths
   - tags and environment naming
6. Preserve migration safety:
   - keep orchestration changes separate from business logic changes when possible
   - defer deeper execution-code rewrites unless the task requires them
   - keep task naming and documentation traceable to the intended execution stages
7. Add or update support files as needed:
   - `variables.tf`
   - `outputs.tf`
   - `versions.tf`
   - `.gitignore`
   - deployment and operations docs
8. Validate with the narrowest meaningful checks first:
   - `terraform fmt`
   - `terraform validate`
   - targeted application tests for changed execution code
   - `terraform plan` when credentials and environment are available
9. Document:
   - how DAGs are deployed
   - how infrastructure is deployed
   - prerequisites and rollback considerations
   - whether previous orchestration entry points remain supported, wrapped, or deprecated

## Exit Criteria

- Airflow platform boundaries are clear.
- Deployment inputs and outputs are explicit.
- Security, networking, storage, and logging are intentional.
- Validation and operating docs are updated.

---


# Bilingual documentation sync workflow

Use when a repository maintains technical docs, reports, or notebooks in English plus one or more localized counterparts.

## Steps

1. Start from the English source document or notebook unless the project states a different source-of-truth policy.
2. Update the closest localized counterpart in the same change whenever the content is meant to stay in sync.
3. Keep code snippets, commands, dataset identifiers, and schema terms stable unless the project intentionally exposes translated names.
4. If the project uses translation dictionaries, glossaries, or helper loaders, update them together with the narrative docs.
5. Verify paired links, headings, captions, and file references across language variants.
6. If a localized version must lag, note that drift explicitly in the doc or change summary instead of leaving a silent mismatch.

---


# Data pipeline change workflow

Use when modifying ingestion, normalization, aggregation, storage layout, feature generation, or pipeline orchestration in an existing data platform.

## Steps

1. Classify the scope first: ingestion, schema, transformation, feature logic, orchestration, infrastructure, docs, or mixed.
2. Read the authoritative contract files before changing code: metadata, schemas, translation maps, job config, or storage conventions.
3. Trace the impacted path across config, pipeline code, scripts, tests, notebooks, and docs before editing.
4. Check whether the change alters grain, keys, partitions, filenames, S3 or warehouse paths, cache behavior, or downstream expectations.
5. Decide whether a backfill, reprocess, cache reset, or migration note is required and document that decision.
6. Implement the smallest safe change that preserves idempotency and observability.
7. Run the narrowest meaningful validation first, then broader tests if the change crosses layers.
8. Update docs, runbooks, and localized counterparts when behavior, commands, or outputs change.
9. Summarize operational impact clearly: what changed, what must be rerun, and what downstream readers should expect.

---


# Dataset onboarding workflow

Use when adding a new source dataset, external feed, or research extract to a data platform. This includes APIs, flat files, warehouse exports, and public open-data sources.

## Steps

1. Confirm source access, license, rate limits, refresh cadence, and permitted use.
2. Decide the raw landing contract first: path, filename, partitioning, replayability, and whether the source is immutable, append-only, or snapshot-based.
3. Add or update the metadata or config source of truth before writing downstream transformation logic.
4. Define downstream schema, keys, grain, nullability, and temporal semantics explicitly.
5. Implement ingestion with idempotency, retry behavior, and resumable or backfill-safe operation where feasible.
6. Add focused tests for contract parsing, schema validation, and at least one end-to-end happy path.
7. Update docs or runbooks with dataset purpose, refresh cadence, validation commands, and operational caveats.
8. If localized docs or translation maps exist, sync them in the same change or record intentional drift.
9. Call out cost, storage, and reprocessing impact when the dataset changes orchestration or infrastructure behavior.

---


# Documentation Creation Workflow

Create or update architecture documentation — diagrams, README files, and external docs (e.g., Confluence).

---

## Before Starting

1. **Read existing documentation** in the target `docs/` folder (diagrams, READMEs).
2. **Read source code** to verify service names, technology stacks, and data flows.
3. **Check external docs** (Confluence, wiki) for existing pages that may need updating.
4. **Check Jira** for related tickets if the documentation is tied to a feature.

---

## Principles

1. **Source of truth**: Diagram source files (PlantUML `.puml`, Mermaid `.md`) are the source of truth. Images (PNG, SVG) are generated artifacts.
2. **Production names**: Always use real production resource names — not shorthand or generic labels.
3. **Technology accuracy**: Verify stack labels against actual code (e.g., don't say "Spring Boot" if it's "Spring MVC").
4. **Consistency**: All diagrams in a folder should share the same theme, skinparams, and icon library version.
5. **Completeness**: Every endpoint, service, and data flow should be documented. Don't omit components for brevity.

---

## Diagram Types

| Type | Purpose | Shows |
|------|---------|-------|
| **Component** | Architecture / static structure | Services, resources, connections, packages |
| **Sequence** | Runtime flow / dynamic behavior | Request lifecycle, message passing, error handling |
| **Unified Flow** | End-to-end overview | All phases as a single diagram — the "executive summary" |

For each service or endpoint, create **both** a component and a sequence diagram.

**Unified Flow diagrams** belong in the root `README.md` for immediate visibility. Per-service diagrams go in `docs/`.

---

## Mermaid Diagram Standards

Use **Mermaid diagrams** (not ASCII art) with this color palette for consistency:

- **Red (primary change):** `fill:#e74c3c,color:#fff,stroke:#c0392b,stroke-width:2px`
- **Orange (review/secondary):** `fill:#f39c12,color:#000,stroke:#d68910,stroke-width:2px`
- **Green (tests/additions):** `fill:#2ecc71,color:#000,stroke:#27ae60,stroke-width:2px`
- **Blue (interfaces/read-only):** `fill:#3498db,color:#fff,stroke:#2980b9,stroke-width:2px`
- **Grey (context-only/external):** `fill:#bdc3c7,color:#000,stroke:#95a5a6,stroke-width:2px`

---

## PlantUML Standards (if project uses PlantUML)

- **AWS Icons**: Use `aws-icons-for-plantuml` latest stable (`!define AWSPuml https://raw.githubusercontent.com/awslabs/aws-icons-for-plantuml/v20.0/dist`).
- **Theme**: `!theme aws-orange` for sequence diagrams. Custom skinparams for component diagrams.
- **Participant aliases**: Must match exactly between declaration and usage — mismatched aliases create ghost participants.
- **Width control**: Keep `nodesep` ≤ 80 for sequence diagrams with many participants. Use `-DPLANTUML_LIMIT_SIZE=16384` when exporting.
- **Naming**: Files use `PascalCase_With_Underscores` matching diagram content.
- **Numbered arrows**: Use circled Unicode digits (①②③…) on arrow labels to show execution sequence.
- Always re-export images after any source file change.

---

## README Standards

### Root `README.md` — minimum sections:
- Title, one-line description, links to external docs (Confluence, Jira).
- **Overview**: What the service does.
- **Architecture**: Link to diagrams in `docs/`.
- **Project Structure**: File tree with descriptions.
- **Data Flow**: Text or diagram showing the request/data pipeline.
- **Deployment**: CI/CD pipeline, environments.
- **Related**: Links to related services, Confluence pages.

### `docs/` folder `README.md` — additional sections:
- **System Components** table: service, repo, production name, stack, role.
- **AWS Resources** table: resource type, production name, publisher/consumer.
- **Key Design Decisions** table: decision → rationale.
- **Generating Diagrams**: Commands to export from source files.

---

## External Documentation (Confluence)

- After updating README or diagrams, update the corresponding Confluence page.
- Use Confluence MCP tools or manual update.
- Keep Confluence in sync with repo documentation — repo is source of truth for code-level docs.

---

## Common Mistakes

- **Wrong technology labels** — always verify against actual source code.
- **Generic service names** — use actual production hostnames/resource names.
- **Missing components** — every participant referenced must be declared.
- **Stale diagrams** — re-export images after every source change.
- **Forgetting render flags** — wide PlantUML diagrams need `-DPLANTUML_LIMIT_SIZE=16384`.

---


# Environment diagnose workflow

Use for local setup failures, broken builds, missing auth, or service boot issues.

## Steps

1. Classify the failure surface: toolchain, auth, containers or local services, dependency install, build cache, or repo-specific setup.
2. Gather concrete evidence first: command output, config presence, running services, and expected versions.
3. Once the failure surface is known, split independent environment checks **immediately by default**.
4. Do not pause before internal fanout unless there is a user-visible tradeoff or risky external side effect.
5. Cluster likely causes and test the lowest-risk fixes first.
6. Re-run the failing workflow and confirm the environment is healthy before stopping.

See `rules/multi-agent-orchestration.md`.

---


# Experiment Result Update Workflow

Use when a notebook, model, statistic, or pipeline rerun changes a result that is referenced in docs, reports, or decision notes.

## Steps

1. Identify the changed result and the artifact that produced it.
2. Confirm whether the change came from:
   - new data
   - code changes
   - different filters or assumptions
   - bug fixes
3. Update the result where it is interpreted:
   - notebook markdown
   - report or README sections
   - thesis-supporting docs
   - translated counterparts
4. State the new limitations, sample size, and assumptions if they materially affect interpretation.
5. If the result changes a downstream threshold, feature choice, or operational decision, document that linkage explicitly.

## Exit Criteria

- The producing code and consuming narrative agree.
- Assumptions and caveats are current.
- Downstream docs are not quoting stale numbers.

---


# ML experiment workflow

Use when changing feature engineering, model training, evaluation, thresholds, or experiment reporting in a data science or AI/ML engineering project.

## Steps

1. Write down the objective, target, unit of analysis, inference boundary, and intended decision before changing code.
2. Inspect the dataset source, feature pipeline, and split strategy before adding new features or models.
3. Check for leakage and temporal or entity overlap before trusting any metric improvement.
4. Establish or preserve a simple baseline so the new approach has a meaningful comparison point.
5. Choose metrics that match the problem and document threshold or calibration decisions explicitly when classification is involved.
6. Keep feature generation and preprocessing in reusable code when the experiment may be revisited or promoted beyond a notebook.
7. Record the dataset version, seed, major parameters, and key results in a doc, notebook section, or experiment summary.
8. Update downstream docs if the experiment changes feature semantics, operational recommendations, or published conclusions.

---


# ML Experiment Update Workflow

Use this workflow for EDA, feature engineering, model training, statistical analysis, notebook revisions, and ML engineering changes.

---

## Step 1: Define the analytical change clearly

Write down the exact question before editing:

- What outcome or target is being modeled or explained?
- What data window or dataset version is in scope?
- Is the goal exploration, reporting, comparison, or operationalization?

If the objective is still fuzzy, avoid broad refactors until the experiment question is stable.

---

## Step 2: Inspect data, features, and splits

Confirm:

- Input datasets and filtering logic
- Feature definitions and derived columns
- Train, validation, and test split strategy
- Time-order or entity leakage risks
- Baselines already available in the repo

If sample size is small, be explicit about the limits of inference and the stability of metrics.

---

## Step 3: Choose notebook versus source-code placement

Use notebooks for iteration and narrative. Use versioned source code for stable logic:

- Move reusable loaders, transforms, metrics, and plotting helpers into `src/` once they are repeated
- Keep notebooks focused on orchestration, interpretation, and presentation
- Prefer deterministic parameters over hidden manual steps

---

## Step 4: Validate the experiment

Run the narrowest useful validation for the change:

- Notebook execution or the relevant scripted equivalent
- Unit tests for loaders, feature builders, or evaluators
- Baseline comparison
- Leakage and assumption checks
- Metric review with confidence or uncertainty caveats when appropriate

Do not present a model improvement without showing the baseline, metric definition, and evaluation slice.

---

## Step 5: Capture reproducibility and artifacts

Make the result reproducible by recording:

- Data source and time window
- Parameters and seeds
- Metrics and thresholds
- Output tables, figures, or reports
- Follow-up work needed to turn analysis into reusable code or deployment assets

---

## Step 6: Update paired deliverables

If the repo maintains translated notebooks, reports, or thesis chapters:

- Update both sides
- Keep commands, dataset names, and conclusions aligned
- Note any intentional drift instead of leaving silent mismatches

---

## Step 7: Hand off with engineering context

Summarize:

- What changed in the experiment
- Whether logic stayed in a notebook or moved into `src/`
- Reproducibility inputs
- Leakage or validity checks performed
- Remaining limitations and next steps

---


# Notebook Analysis Workflow

Use when creating or editing notebooks for EDA, statistical analysis, clustering, visualization, or report support.

## Steps

1. Confirm the data prerequisite:
   - which dataset or layer is needed
   - whether a pipeline step must run first
   - how the notebook locates data and credentials
2. Declare notebook inputs near the top: bucket or path, profile, cache choice, seed, and any filters.
3. Keep reusable logic out of cells:
   - move loaders, feature prep, repeated statistics, or plotting helpers into `src/` or scripts
   - keep the notebook focused on orchestration and interpretation
4. Run from a clean kernel and verify the notebook works top to bottom.
5. Add markdown conclusions close to the figures or tables they describe.
6. If the findings influence product, thesis, or operational decisions, reflect them in committed docs or code comments outside the notebook too.
7. If the repo maintains translated analysis surfaces, update the related labels or docs after changing the analytical narrative.

## Exit Criteria

- Notebook can be rerun from a clean state.
- Key logic is not trapped in cells only.
- Findings and limitations are documented where future sessions can find them.

---


# Notebook analysis update workflow

Use when changing Jupyter notebooks, exploratory analysis, statistical tests, visualizations, or notebook-driven reporting.

## Steps

1. Confirm the source dataset, snapshot date, loader path, or extraction command behind the notebook.
2. Read the notebook together with any helper modules or loaders it depends on before editing cells.
3. Separate scratch work from reportable analysis. If the notebook is project-facing, keep the narrative clear and the cell order deterministic.
4. Set or confirm random seeds for sampling, splits, and model training when applicable.
5. Extract reusable logic to code if it appears in multiple cells, multiple notebooks, or project-facing scripts.
6. Restart and run all before considering the change complete.
7. Keep outputs reviewable. Avoid unnecessary heavy binary output or stale cell results.
8. Update related docs, findings summaries, and localized notebook text when the conclusions or interpretation changed.

---


# Notebook LaTeX polish workflow

Use when a notebook or technical document contains math notation that needs cleanup, clearer prose, or better export compatibility.

## Steps

1. Identify the equations, derivations, and symbols that are central to the analysis or explanation.
2. Standardize notation so the same concept is not named three different ways across markdown, code, and charts.
3. Replace screenshot-based formulas or plain-text pseudo-math with markdown plus LaTeX when possible.
4. Add short prose around each important equation: what the variables mean, what the equation is used for, and any assumptions.
5. Verify the math renders in the actual notebook or export target, especially for multiline equations.
6. Keep the final notation aligned with the implementation and the dataset terminology.

---


# Notebook to script workflow

Use when a notebook contains logic that is no longer one-off exploration and should become reusable, testable, or schedulable code.

## Steps

1. Identify the stable logic to promote: loaders, preprocessing, feature engineering, validation, chart builders, or report generation helpers.
2. Separate reusable code from notebook-only narrative, visual interpretation, and scratch exploration.
3. Extract the logic into `src/`, a helper module, or a script with explicit inputs and outputs.
4. Replace duplicated notebook cells with imports or function calls so the notebook becomes a consumer of reusable code.
5. Add focused tests or validation checks for the promoted logic.
6. Update docs so future notebook work starts from the reusable path instead of re-copying cells.
7. Re-run the notebook end to end after the extraction to confirm the behavior still matches.

---


# Paired Doc Sync Workflow

Use this workflow when a repository keeps multiple versions of the same content, such as English and translated docs, paired notebooks, or report and presentation variants.

---

## Step 1: Identify the artifact pairs

Find the files that should move together, for example:

- `README.md` and `README.<locale>.md`
- `docs/*.md` and `docs/*.<locale>.md`
- `notebooks/*.ipynb` and localized notebook variants
- A narrative report plus the notebook or script that produced it

List any pairs that are intentionally one-sided.

---

## Step 2: Choose the source of truth

For each pair, state which version leads the change:

- Original language first
- Generated report first
- Notebook first
- Code first

If that source of truth is unclear, decide it before editing multiple artifacts.

---

## Step 3: Sync meaning, not just text

When updating the secondary artifact, verify all of the following:

- Commands and paths
- Dataset names and metric labels
- Table and figure references
- Dates, time windows, and counts
- Terminology and domain vocabulary

Do not let one version drift on technical details while matching only the prose.

---

## Step 4: Preserve reviewability

Prefer changes that reviewers can compare directly:

- Keep section ordering aligned across versions when possible
- Reuse filenames and numbering conventions
- Add a short note when content intentionally differs by audience

---

## Step 5: Record unresolved drift explicitly

If you cannot update both sides in one pass, leave a clear note describing:

- Which files are still out of sync
- What changed in the source of truth
- What remains to translate or adapt

Silent drift is worse than an explicit TODO.

---


# Pre-PR check workflow

Use when the user is ready for a PR, wants a full branch check, or wants a review-ready summary before pushing. Prefer the **pre-pr-check** skill when it matches the IDE.

## Steps

1. Establish scope: diff, base branch, ticket, and intended PR surface.
2. Once scope is known, split independent lanes **immediately by default**:
   - validation command selection and execution
   - diff review and risk clustering
   - docs or config drift
   - security or release-impact checks when relevant
3. Do not pause before internal fanout unless there is a user-visible tradeoff, risky external side effect, or overlapping write ownership.
4. Run the smallest safe validation set, then broaden only when needed.
5. Produce one summary with blockers, warnings, follow-up fixes, and readiness status.

See `rules/multi-agent-orchestration.md`.

---


# Project discovery workflow

Use when the user asks to understand a repo, get onboarded, map architecture, or document how to work in the codebase. Prefer the **project-discovery** skill when appropriate.

## Steps

1. Identify the destination repo, its stack, and key entry docs or build files.
2. Once scope is known, split independent discovery lanes **immediately by default**:
   - repository structure and entrypoints
   - architecture and request or data flows
   - local run, test, and dependency setup
3. Do not pause before internal fanout unless there is a user-visible tradeoff or risky external side effect.
4. Synthesize a concise overview: key paths, related services or repos, and recommended doc updates.
5. When asked, propose or apply committed docs (for example `README.md`, `docs/architecture.md`).

See `rules/multi-agent-orchestration.md`.

---


# Research Analysis Cycle

Use when updating a thesis, capstone, paper, or other evidence-heavy analytical project.

## Steps

1. Write down the current question, scope boundary, and audience before changing methods or code.
2. Confirm the available data, granularity, time coverage, and constraints that shape feasible methods.
3. Choose the simplest defensible method for the question, then define the baseline, diagnostics, or robustness checks that make the result trustworthy.
4. Keep reusable transforms and statistical helpers in committed code when they are no longer one-off notebook logic.
5. Generate or refresh the result artifacts that matter to the argument: tables, plots, metrics, and narrative notes.
6. Recheck the evidence chain:
   - each claim should map to a reproducible artifact
   - each figure or table should have explicit scope and definitions
   - each conclusion should carry the right caveats
7. Update the written deliverable, chapter notes, or repository docs in the same change when the evidence shifts.
8. Record open questions, limitations, and the next analytical step before stopping.

## Exit Criteria

- The question, method, and evidence boundary are explicit.
- The main claims are traceable to reproducible artifacts.
- The written narrative matches the current analytical result.

---


# Code Review Workflow

Review code changes for bugs, correctness, security issues, and test coverage. Works with PRs, branches, or uncommitted diffs.

Scope: PR number → use PR tools; branch name → use `git log`; current changes → `git diff --stat && git diff`.

---

## Phase 1 — Identify What to Review

**1. Determine scope** — PR / branch / uncommitted diff.

**2. Read the ticket** — extract ticket ID from PR title/branch/commits. Fetch from Jira (via CLI or MCP). Label every AC as `AC-1`, `AC-2`, … Flag vague/untestable ACs immediately.

---

## Phase 2 — Code Review Checklist

**3. Correctness & Logic** — code matches ticket intent; no NPEs, off-by-ones, or bad conditionals; edge cases (nulls, empty collections, missing records) handled.

**4. Backend Standards** — follow project-specific backend rules (see `rules/` and project rules). Check code style, framework patterns, dependency injection patterns.

**5. Frontend Standards** — follow project-specific frontend rules. No `any` without justification. Generated API types preferred. Strings externalized. No unsafe HTML insertion.

**6. Security (CRITICAL)** — see `rules/security.md` and `rubrics/security.md`. Authorization on every new endpoint. No PII in logs. No secrets in code. Parameterized queries only.

**7. Performance** — no N+1 queries; long ops offloaded to async workers; no blocking in request threads; appropriate log levels.

**8. Error Handling & Logging** — meaningful exception messages; no swallowed exceptions; no sensitive data in logs.

**8a. Backward Compatibility** — persisted payloads, event schemas, and public APIs may have **downstream consumers** (other services, jobs, mobile or web clients). Renaming or removing fields, enum-like string constants, or topic/message shapes without coordination is a **Blocker** unless versioned or feature-flagged per project policy. Adding optional fields is usually safe; verify migrations and consumers.

**9. Database Migrations** — correct naming convention; never modify committed migrations; backward compatible; indexes for FKs.

**10. Test Coverage** — every changed method has a unit test. Happy path, edge cases, error conditions. E2E updated if user-facing behavior changed. Named constants, no magic strings. Run tests to verify.

**11. Files That Must Not Be Committed** — build output, IDE files, OS metadata, credentials.

**12. AC Coverage** — for each AC, locate the code/test that satisfies it. No code → Blocker. No test → Blocker. Vague AC → Ticket Quality Issue.

---

## Phase 3 — Validate AC Coverage (100% Required)

**13. Verify every ticket AC is implemented** — map each AC to the code that satisfies it. Missing implementation = Blocker. Vague/untestable AC = flag and request clarification.

**14. Categorize findings** — Blockers (bugs, security, missing permissions, data loss, missing AC), Suggestions (style, refactors), Praise.

**15. Present the review:**

```
## Review Summary
**PR/Branch**: X  **Ticket**: TICKET-ID  **Files reviewed**: N  **Verdict**: Approve / Request Changes

### Blockers        ### Suggestions        ### What looks good

### AC Coverage
| AC | Description | Satisfied? | Code/Test |
|----|-------------|-----------|-----------|
| AC-1 | ... | ✅/❌ | path:line |

### Ticket Quality Issues    ### Test Assessment
```

---

## Phase 4 — Fix All Issues

**16. Fix every Blocker and inconsistency** — read the actual source file → apply minimal targeted fix → verify the fix compiles. One issue at a time, never batch unrelated fixes.

**17. Read and triage ALL PR comments (if PR exists):**
- Prioritize: Blockers → Required changes → Questions → Suggestions.
- Fix each actionable comment: read source → apply minimal fix → verify compile → run tests.
- Reply to every comment with what changed and why.

---

## Phase 5 — Add / Update Unit Tests

**18. Every changed/new method needs full unit test coverage.** Create test file if missing (mirror source path). Cover: happy path, edge cases, error conditions, all branches.

**19. Run and verify 100% pass.** Fix failures immediately — do NOT proceed until green.

---

## Phase 6 — Add / Update Integration Tests

**20. For API, cross-service, or DB changes** — ensure integration/system tests exist.

**21. Run and verify 100% pass.** Fix failures before proceeding.

---

## Phase 7 — Final Test Gate (100% Pass Required)

**22. Full related test suite — zero failures allowed.** If anything fails, go back to Phase 4.

// turbo
**22a. Run full test suite:**
```bash
pytest
```

---

## Phase 8 — Commit & Push

**23. Commit only after 100% green.** Verify `git status` excludes build output, IDE files, etc. Split unrelated changes into separate commits.

**24. Rebase before every push** (see `rules/git-conventions.md`).

**25. Push and create/update draft PR** with appropriate labels.

---

## Phase 9 — Monitor CI Until Green

**26. Poll CI** until all checks complete.

**27. CI green** → notify user, PR ready for human review.

**28. CI fails** → diagnose and fix immediately. Loop: read failure → diagnose root cause → fix locally → re-run tests → commit → push → poll again.

**29. After every CI failure, update rules/workflows** to prevent recurrence (see `rules/ci-feedback-loop.md`).

---

## Notes

- Always read actual code — never review from memory.
- API changes → regenerate types if applicable.
- Infrastructure/environment changes → verify all environment configs are updated.
- DI configuration changes (XML, modules, etc.) → run full test suite.
- Check test files too — tests can have bugs.
- **Only stop when CI is fully green.**

---


# Review and Fix Workflow

Review code changes, fix all issues, ensure tests pass, and push — in a single loop. Combines inspection, remediation, testing, and CI monitoring.

Scope: current branch changes only (diff against base branch).

---

## Phase 1 — Scope and Context

**1. Determine scope** — identify the current branch and its base (e.g., `main`). Only review changes in `git diff --merge-base <base>`.

**2. Extract ticket ID** — from branch name, PR title, or commits. Fetch from Jira (prefer CLI `acli`, fallback to MCP, then ask user). Label every Acceptance Criterion as `AC-1`, `AC-2`, … Flag vague or untestable ACs immediately.

**3. Fetch PR metadata (if PR exists)** — use `gh pr view` or GitHub MCP to get:
- PR description and linked ticket
- All review comments (resolved and unresolved)
- CI check status

---

## Phase 2 — Three-Signal Inspection

Run three independent inspections. Tag every finding with its signal source.

### S1: Inspect Against PR Acceptance Criteria

**4. Map each AC to code and tests.** For every AC:
- Locate the code that implements it.
- Locate the test that verifies it.
- Missing implementation = **Blocker (S1)**. Missing test = **Blocker (S1)**. Vague AC = **Ticket Quality Issue**.

### S2: Inspect Unresolved PR Comments

**5. Read ALL PR review comments** (via `gh pr view <N> --json reviews,comments` or GitHub MCP).
- Filter to unresolved / not-addressed comments.
- For each unresolved comment: verify whether the current code addresses it.
- Unaddressed actionable comment = **Blocker (S2)**.

### S3: Inspect Against Rules and Skills

**6. Load applicable rules and skills:**
- Generic rules: `rules/code-rules.md`, `rules/security.md`, `rules/testing.md`, `rules/best-practices.md`
- Language-specific rules: detect primary language from changed files and load the matching rule (e.g., `rules/java-best-practices.md`, `rules/python-best-practices.md`, `rules/js-ts-best-practices.md`)
- Rubrics: `rubrics/code-review-checklist.md`, `rubrics/security.md`, `rubrics/architecture.md`
- Project-specific rules: any rules in the project's `.windsurf/rules/`, `.cursor/rules/`, or similar

**7. Evaluate the diff against the full checklist:**
- **Correctness & Logic** — NPEs, off-by-ones, edge cases (nulls, empty collections, missing records)
- **Security** — authorization, injection, PII in logs, secrets, parameterized queries
- **Performance** — N+1 queries, blocking calls, log levels, payload sizes
- **Error Handling** — swallowed exceptions, meaningful messages, no sensitive data in logs
- **Backward Compatibility** — API contracts, message formats, shared databases
- **Operability** — logging quality, metrics impact, migration safety
- **Code Standards** — framework patterns, DI patterns, naming, duplication, dead code

Each confirmed issue = **Blocker or Suggestion (S3)** per `rubrics/code-review-checklist.md` severity rules.

---

## Phase 3 — Present Findings

**8. Categorize and present all findings before fixing:**

```
## Inspection Summary
**Branch**: X  **Ticket**: TICKET-ID  **Files reviewed**: N

### S1 — AC Coverage
| AC | Description | Implemented? | Tested? | Code/Test |
|----|-------------|-------------|---------|-----------|
| AC-1 | ... | Yes/No | Yes/No | path:line |

### S2 — Unresolved PR Comments
| # | Author | Comment | Status | Action |
|---|--------|---------|--------|--------|

### S3 — Rules/Skills Findings
| Severity | Signal | File:Line | Issue | Remediation |
|----------|--------|-----------|-------|-------------|
| Blocker  | S3     | ...       | ...   | ...         |

### Ticket Quality Issues
```

**9. Confirm with user before proceeding to fix** — if there are Blockers that require design decisions or clarifications, pause and ask.

---

## Phase 4 — Fix All Issues

**10. Fix every Blocker and actionable finding.** Process in order: S1 (AC gaps) -> S2 (PR comments) -> S3 (rules violations). For each fix:
- Read the actual source file (never fix from memory).
- Apply minimal, targeted fix — one issue at a time.
- Verify syntax with `python -m py_compile src/<file>.py`.
- Never batch unrelated fixes into one change.

**11. For S2 (PR comment) fixes** — note what changed and why, to reply to each comment after push.

---

## Phase 5 — Update PR and Documentation

**12. Update PR description** if scope or behavior changed during fixes.

**13. Update related documentation:**
- README, API docs, Swagger/OpenAPI specs if endpoints changed.
- Architecture docs if design changed.
- Configuration docs if new properties were added.
- Confluence pages if referenced in the ticket.

---

## Phase 6 — Unit Tests (100% Pass Required)

**14. Every changed/new method needs full unit test coverage.** Create test file if missing (mirror source path). Cover: happy path, edge cases, error conditions, all branches.

**15. Run unit tests and verify 100% pass:**
```bash
pytest
```

**16. Fix any failures immediately** — do NOT proceed until all unit tests are green.

---

## Phase 7 — Integration Tests (100% Pass Required)

**17. For API, cross-service, or DB changes** — ensure integration/system tests exist and cover the changed behavior.

**18. Run integration tests and verify 100% pass:**
```bash
# No separate integration test suite — all tests are in tests/
# Full suite covers unit + integration scenarios:
pytest
```

**19. Fix any failures immediately** — do NOT proceed until all integration tests are green.

---

## Phase 8 — Final Test Gate

// turbo
**20. Run the full related test suite — zero failures allowed.**
```bash
pytest
```

If anything fails, go back to Phase 4.

---

## Phase 9 — Commit and Push (5-Category Rule — MANDATORY)

> **⚠️ MANDATORY GATE — The 5-category commit structure is NOT optional.**
> Every execution of this workflow MUST produce a branch where ALL commits follow the 5-category rule below. No exceptions. No "just adding on top of existing commits." The ENTIRE branch must be reset and restructured from scratch every time.

**21. Reset the ENTIRE branch** — ALWAYS run this, unconditionally:
```bash
git reset --mixed origin/<base-branch>
```
This unstages ALL commits on the branch, keeping all changes in the working directory. You will now re-commit everything from scratch in the correct 5-category order. This step is **mandatory every time** — even if you believe the existing commits already follow the convention.

**21a. Check `git status` for LLM config files** — look for any changes (staged or unstaged) to:
- `.windsurf/` (project-specific rules/workflows)
- `AGENTS.md`
- `.gitignore`

These MUST be committed in Category 1 even if they were not part of the review findings.

**22. Re-commit ALL changes** in exactly this order. Skip categories with no changes. **Maximum 5 commits. Each category gets exactly ONE commit.**

| Order | Category | What belongs here |
|-------|----------|-------------------|
| 1 | **LLM configs** | `.windsurf/`, `.agents/`, `.claude/`, `AGENTS.md`, `.gitignore` |
| 2 | **Documentation** | `README.md`, `docs/`, architecture diagrams, API specs, PlantUML |
| 3 | **Logs improvement** | Logger setup/format/level changes, log context, MDC, logging-only test files |
| 4 | **Application configs/structure** | Config files, build config, DI config, env config, dependency files |
| 5 | **Code changes** | Source code, business logic, tests for business logic |

**Commit rules:**
- Each commit prefixed with ticket ID: `TICKET-ID: <description>`
- **Never mix categories** in a single commit.
- **If a source file has BOTH logging and business logic changes**, use the intermediate-file approach:
  1. Save the final version to a temp location (`cp file /tmp/file_final`)
  2. Edit the file to contain only original code + new logging changes
  3. Stage and commit in Category 3
  4. Restore the final version (`cp /tmp/file_final file`)
  5. Stage and commit in Category 5
- Tests follow their subject: logging tests → Cat 3, business logic tests → Cat 5.
- Use `git add -p` for hunk-level splits when changes are in separate, non-interleaved hunks.

**23. VALIDATE commit structure before pushing** — run:
```bash
git log --oneline $(git merge-base origin/<base-branch> HEAD)..HEAD
```
Verify ALL of the following — **if ANY check fails, go back to step 21 and redo**:
- [ ] Each commit belongs to exactly ONE category (1–5)
- [ ] Categories appear in ascending order — no category appears after a higher-numbered one
- [ ] No category is repeated — maximum 1 commit per category, maximum 5 commits total
- [ ] Each commit message starts with the ticket ID
- [ ] No build output, IDE files, secrets, or `.agents/` directory in any commit

**If validation fails, `git reset --mixed origin/<base-branch>` and restructure again.**

**24. Rebase before push** (mandatory):
```bash
git fetch origin
git rebase origin/<base-branch>
# Resolve any conflicts
# Re-run full test suite after rebase
git push --force-with-lease
```

**24a. Push to stage branch** (mandatory — enables stage environment testing):
```bash
git push origin HEAD:stage --force-with-lease
```
This pushes the current branch HEAD to the `stage` branch so the changes are deployed to the stage environment. If `--force-with-lease` fails (e.g., stage branch has diverged), use `--force` since stage is a transient deployment branch.

**25. Reply to PR comments** (S2 fixes) — for each addressed comment, reply with what changed and where.

---

## Phase 10 — Monitor CI Until Green

**26. Poll CI** using `gh pr checks <PR_NUMBER>` or GitHub MCP until all checks complete.

**27. CI green** — notify user. PR is ready for human review.

**28. CI fails** — diagnose and fix immediately:
1. Read the failure output.
2. Diagnose root cause.
3. Fix locally.
4. Re-run tests (Phase 8).
5. Commit and push (Phase 9).
6. Poll CI again.
7. **Loop until green. Do not stop on CI failure.**

**28a. If an automation PR review check fails** (e.g. Cursor-integrated bot; check name varies by org) — the bot often posts a review comment with Red/Yellow/Green findings. Run the **cursor-pr-validation** workflow (`workflows/cursor-pr-validation.md`) to triage, fix, and re-push. Then poll CI again.

**29. After every CI failure, update rules/workflows** to prevent recurrence (see `rules/ci-feedback-loop.md`).

---

## Notes

- Always read actual code — never review or fix from memory.
- Only changes in the branch diff are in scope — do not fix pre-existing issues outside the diff unless they are blocking.
- API changes -> regenerate types if applicable.
- Infrastructure/environment changes -> verify all environment configs are updated.
- DI configuration changes -> run full test suite.
- Check test files too — tests can have bugs.
- **Only stop when CI is fully green.**

### Multi-agent runs

- After **each phase** (1–10), print a concise **Phase N — result** summary before starting the next phase.
- See **`rules/review-and-fix-multi-agent.md`** for agent splits (S1/S2/S3), Windows shell pitfalls, exception-handling gotchas, datetime mocking limits, and Phase 9 safety.

---


# Run Tests Workflow

Run the right tests for a given change. The user will specify what changed or what they want to verify.

---

## Step 1: Identify what changed

Determine the scope of changes:
- **Specific file(s)** — run tests for those files
- **Specific feature/module** — run module-level tests
- **Ticket changes** — run all tests related to the ticket's scope
- **Full suite** — run everything

---

## Step 2: Run the appropriate tests

### All Tests (default — run from repo root)
```bash
pytest
```

### Single test file
```bash
pytest tests/test_<module>.py -v
pytest path/to/test_file.py -v
```

### Single test class
```bash
pytest tests/test_<module>.py::TestClassName -v
pytest path/to/test_file.py::TestClassName -v
```

### Single test method
```bash
pytest tests/test_<module>.py::TestClassName::test_case_name -v
```

### With coverage
```bash
pytest --cov=src --cov-report=term-missing
```

---

## Step 3: Which tests to run for a given change

| Changed area | Tests to run |
|---|---|
| One source file | The closest matching test file or module tests |
| Shared constants or core utilities | All dependent tests, often module-wide or suite-wide |
| Test fixtures, bootstrap, or global config | Full suite: `pytest` |
| New feature touching multiple modules | Module-level tests first, then broader suite |

---

## Step 4: Verify results

- **All tests must pass** before proceeding.
- If a test fails, diagnose the root cause and fix before continuing.
- Re-run after every fix to confirm.

---

## Troubleshooting

### Import errors (`ModuleNotFoundError`)
- Verify the repo’s test bootstrap is being loaded correctly.
- Run from repo root unless the project explicitly documents another entry point.

### Missing env vars (`KeyError` on import)
- Ensure test fixtures or env setup run before application imports.
- Provide the minimum required test configuration documented by the project.

### External dependencies not mocked or stubbed
- Verify the project’s mocking or fixture pattern is active for SDK clients, databases, queues, or HTTP calls.
- Reset shared module-level state between tests when the code caches clients or configuration.

---


# Capture Learning

Use this workflow after recovering from trial-and-error, discovering a non-obvious solution, or hitting a platform/toolchain gotcha. The output is a committed markdown file in `learnings/` that any LLM tool can consume.

## When to trigger

- You tried 2+ approaches before finding the correct one.
- A command, API, or config behaved unexpectedly.
- A platform-specific issue caused failures (OS, shell, path, permissions).
- A build/test/deploy step needed a non-obvious flag or order.

Do **not** capture trivial typos or one-off environment blips.

## Steps

### 1. Identify the learning

Summarize in one sentence what you just discovered. If you cannot state it clearly, the learning is not ready to capture yet.

### 2. Ensure the directory exists

If `learnings/` does not exist at the repo root, create it along with the `README.md` template:

```bash
mkdir -p learnings
```

Copy the README template from the toolkit's `learnings/README.md` or create one following the convention in `rules/error-driven-learning.md`.

### 3. Write the learning file

Create a new file in `learnings/` named with lowercase kebab-case summarizing the topic:

```
learnings/<topic>.md
```

Use this structure:

```markdown
---
title: Short descriptive title
category: environment | build | api | architecture | testing | toolchain | deployment
created: YYYY-MM-DD
tags: [relevant, searchable, terms]
---

# Problem
What was being attempted and what went wrong.

# Failed Approaches
What was tried and why it didn't work.
Include commands, error messages, or code snippets.

# Solution
The correct approach — the happy path.

# Why
Root cause explanation.
```

### 4. Quality check

Before saving, verify:

- [ ] Title is specific enough to match on a keyword scan.
- [ ] Failed approaches include concrete details (commands, errors), not just "it didn't work."
- [ ] Solution is copy-pasteable or directly actionable.
- [ ] Root cause is explained — not just "use X instead of Y" but **why**.
- [ ] File is 20–60 lines. If longer, split into multiple learnings.
- [ ] Tags cover the keywords a future LLM would search for.

### 5. Supplement tool-specific memory (optional)

If your LLM tool has built-in memory (Windsurf memories, Cursor context, etc.), also store a brief reference there pointing to the learning file:

> See `learnings/<topic>.md` for the full solution.

This gives faster retrieval within that tool while keeping the committed file as the shared source of truth.

### 6. Commit

Stage and commit the learning file. Use commit category 2 (Documentation) per `rules/git-conventions.md`:

```bash
git add learnings/<topic>.md
git commit -m "<TICKET-ID>: Add learning — <short description>"
```

If no ticket context exists, use a descriptive prefix:

```bash
git commit -m "docs: Add learning — <short description>"
```

---


# Security Check Workflow

Use this workflow with:

- `workflows/security-report.md`
- `workflows/review.md`
- `workflows/pre-pr-check.md`

## Steps

1. Identify whether the change touches code, scripts, workflows, rules, skills, integrations, or operational documentation.
2. If yes, run:
   - `./scripts/security-check-toolkit.sh`
3. Review every failure and blocking finding before considering the task complete.
4. If a check is skipped because the file type is absent, leave it skipped; do not fake coverage.
5. If a needed tool is missing, install it or document the gap and residual risk explicitly.
6. Treat secret leaks, unsafe command patterns, workflow security issues, and dependency findings as blocking unless there is a specific reviewed exception.

## Reporting Expectations

- Summarize that `./scripts/security-check-toolkit.sh` was run.
- Summarize failed checks and whether they were fixed or intentionally deferred.
- Call out skipped checks that reflect tooling or environment gaps rather than absent file types.

---


# Security report workflow

Use when the user asks for a security report, OWASP-style audit, AppSec review, or vulnerability assessment. Pair with the **security** skill and `rubrics/security.md`.

## Phase 1 — Scope

1. Determine the review surface: repo root, diff, service, frontend, backend, or specific path.
2. Identify runtime components, public entrypoints, trust boundaries, and the active stack.
3. Choose the lens: OWASP Top 10, API Top 10, ASVS, or a mixed review as appropriate.

## Phase 2 — Parallel inspection by default

4. Once scope is known, split independent audit lanes **immediately by default**:
   - authentication, authorization, and trust boundaries
   - injection, file handling, deserialization, and outbound calls
   - secrets, dependencies, config exposure, and operational hardening
   - frontend risks such as XSS, token handling, and client trust
   - shared libraries and type-safety or boundary gaps
5. Do not pause before internal fanout unless there is a user-visible tradeoff or risky external side effect.

## Phase 3 — Synthesize

6. Merge lanes into one report with severity, OWASP mapping, evidence, impact, and remediation.
7. Separate confirmed findings from assumption-backed risks and open questions.
8. Write the report where the user requests (for example `docs/security-review.md` or ticket notes), using clear severity labels and file references.

See `rules/multi-agent-orchestration.md`.

---


# Translation Sync Workflow

Use when the repo maintains paired docs, bilingual reports, or translated dataset labels.

## Steps

1. Identify the source change:
   - documentation update
   - notebook narrative update
   - translated loader or display label update
2. Find the paired artifacts that represent the same concept in other languages.
3. Update both sides while preserving:
   - identical commands and paths
   - identical factual claims and time ranges
   - stable code identifiers and dataset keys
4. If translation mappings exist in code, update them together with doc text.
5. Verify that examples and screenshots or figure captions still correspond to the same output.
6. If translation is intentionally delayed, note that explicitly in the task summary or doc stub.

## Exit Criteria

- The paired materials communicate the same engineering truth.
- Code-facing identifiers remain stable.
- Any intentional lag is documented instead of being silent.

---


# Update docs workflow

Use when the user asks to update docs, document a change, or keep README or runbooks in sync with code. Often pairs with the **doc-delta** skill.

## Steps

1. Establish scope: code or config change, affected user-facing behavior, and target docs.
2. Once scope is known, split independent discovery lanes **immediately by default**:
   - behavior and API changes
   - operational or runbook impact
   - README, architecture, or ticket-doc drift
3. Do not pause before internal fanout unless there is a user-visible tradeoff or risky external side effect.
4. Apply the smallest doc delta that keeps committed docs truthful and easy to use.

See `rules/multi-agent-orchestration.md` and `rules/operational-doc-required.md`.

---

# Repo-Local Workflows

# AWS Airflow Terraform Change Workflow

Use this repo-local workflow together with the shared workflow in `.windsurf/workflows/aws-airflow-terraform-change.md`.

## Repository-Specific Steps

1. Read the current repository boundaries before designing infrastructure:
   - `infra/`
   - `scripts/01_bronze_ingestion.sh`
   - `scripts/02_silver_transformation.sh`
   - `scripts/03_gold_transformation.sh`
   - `docs/llm/rules/data-pipeline-boundaries.md`
   - `docs/llm/rules/aws-airflow-terraform.md`
2. Decide whether the first migration step should wrap the existing shell scripts rather than rewrite ETL logic.
3. Keep Bronze, Silver, and Gold responsibilities traceable in DAG names, task names, and docs.
4. If the Airflow surface remains small, extending the current `infra/` root may be acceptable. If it becomes non-trivial, split into modules and explicit environment inputs.
5. Update repo artifacts that this migration affects:
   - `config/` contracts when behavior changes
   - `tests/`
   - `docs/`
   - `.gitignore` if local Terraform artifacts need coverage
6. Document whether shell scripts remain canonical, become task wrappers, or are deprecated.

---

# Course Material Grounding Workflow

Use this workflow when a task should be grounded in the USP MBA class corpus under `/mnt/hgfs/shared/data-science/aulas`, not just in generic best practices.

## Steps

1. Classify the request:
   - class summary or study support
   - thesis method selection
   - repository implementation guidance
   - governance, writing, or presentation support
2. Read `docs/llm/references/usp-mba-course-map.md` first and identify the closest course anchors.
3. If the request falls outside the curated map or spans several modules, inspect `docs/llm/references/usp-mba-course-inventory.generated.md` to locate the missing course folder and representative assets.
4. Separate generic guidance from class-specific guidance:
   - reusable Data Science guidance should come from shared `dev-tools` skills, rules, and workflows
   - course-specific framing, terminology, and method bias should come from local `docs/llm/`
5. For thesis or deliverable work, also review:
   - `docs/llm/rules/usp-mba-course-context.md`
   - `docs/llm/rules/tcc-deliverables-and-argument.md`
   - `docs/llm/workflows/tcc-method-selection.md`
   - `docs/llm/workflows/tcc-analysis-and-writing-sync.md`
6. Name the exact class modules that support the recommendation, and call out any meaningful gap between the class material and the proposed approach.

## Exit Criteria

- The answer identifies which course modules are relevant.
- Generic guidance and class-specific guidance are clearly separated.
- The recommendation is consistent with both the repository context and the available course material.

---

# Documentation Sync Workflow

Use this repo-local workflow together with the shared workflow in `.windsurf/workflows/update-docs.md`.

## Repository-Specific Steps

1. Identify which areas changed:
   - ingestion
   - processing
   - analysis outputs
   - infrastructure
   - tests
   - operational commands
2. Update the closest English source document first:
   - `README.md`
   - `docs/01_BRONZE_LAYER.md`
   - `docs/02_SILVER_LAYER.md`
   - `docs/03_GOLD_LAYER.md`
   - related coverage or summary documents
3. If a maintained Portuguese counterpart exists, update it in the same change or record why it intentionally diverges.
4. Keep generated logs and evidence artifacts separate from narrative documentation.
5. Prefer concise, operationally useful notes over high-level marketing language.

---

# Pipeline Change Workflow

Use this repo-local workflow together with the shared workflow in `.windsurf/workflows/data-pipeline-change.md`.

## Repository-Specific Steps

1. Read the relevant contract files first:
   - `config/ibge_metadata.json`
   - `config/transparency_metadata.json`
   - `config/silver_schemas.json`
2. Trace the impacted code path:
   - `src/ingestion/`
   - `src/processing/`
   - `src/analysis/`
   - `scripts/`
3. Verify the actual orchestration surface before assuming cloud-managed workflow changes:
   - shell entry points in `scripts/`
   - S3 path conventions
   - metadata and schema contracts
   - `dags/` only when the task includes real DAG code
4. Check whether the change requires:
   - new or updated tests
   - metadata or schema updates
   - documentation updates in `docs/`
   - reprocessing or backfill guidance
5. Document operational impact, especially when the change alters S3 keys, schemas, backfill expectations, notebook loader expectations, or CLI usage.

---

# Refresh USP MBA Course Context Workflow

Use this workflow when new class material is added under `/mnt/hgfs/shared/data-science/aulas` and the thesis repo's LLM guidance should stay current.

## Steps

1. Regenerate the source inventory:
   - Linux or WSL: `python ./docs/llm/scripts/build_usp_mba_course_inventory.py --source /mnt/hgfs/shared/data-science/aulas --output ./docs/llm/references`
   - Windows PowerShell: `python .\docs\llm\scripts\build_usp_mba_course_inventory.py --source C:\google-drive\cursos\usp\mba\data-science\aulas --output .\docs\llm\references`
2. Review the diff in `docs/llm/references/usp-mba-course-inventory.generated.md`:
   - identify new course folders
   - identify new representative assets
   - identify new method, governance, or writing topics that were not previously mapped
3. Update `docs/llm/references/usp-mba-course-map.md`:
   - add new course anchors
   - revise the method defaults if the new material changes the recommended baseline
   - add or replace representative source assets when the new classes are more relevant
4. Check whether the local thesis rules or workflows should change:
   - `docs/llm/rules/course-material-grounding.md`
   - `docs/llm/rules/usp-mba-course-context.md`
   - `docs/llm/rules/tcc-deliverables-and-argument.md`
   - `docs/llm/workflows/course-material-grounding.md`
   - `docs/llm/workflows/tcc-method-selection.md`
   - `docs/llm/workflows/tcc-analysis-and-writing-sync.md`
5. Decide whether anything belongs upstream in the shared toolkit:
   - if the new insight is generic and reusable across projects, add it to `bri-ai-dev-tools`
   - if it is specific to USP MBA coursework or this thesis, keep it in `docs/llm/`
6. Refresh repo-local exports after any shared-toolkit or `toolkit-selection.txt` changes:
   - `.\scripts\sync-llm-configs.ps1`
   - or `./scripts/sync-llm-configs.sh`
7. Re-read `AGENTS.md` and `docs/llm/README.md` only if the entry points or local layout changed.

## Exit Criteria

- The generated inventory reflects the current `aulas/` tree.
- The curated course map reflects the new classes that matter to the thesis.
- The local thesis rules and workflows still point to the right method and writing guidance.
- Any truly reusable insight has been promoted to the shared toolkit instead of being duplicated locally.

---

# Security Check Workflow

Use this repo-local workflow with:

- `.windsurf/workflows/security-report.md`
- `.windsurf/workflows/review.md`
- `.windsurf/workflows/pre-pr-check.md`

## Repository-Specific Steps

1. Identify whether the change touches any of these categories:
   - application code
   - pipeline code
   - infrastructure
   - shell or PowerShell automation
   - GitHub workflows
   - local LLM rules, skills, prompts, or workflows
   - operational documentation that contains commands or procedures
2. If yes, run the repository security check:
   - `./scripts/security_check_this_repo.sh`
3. If the work is being done in `dev-tools`, run:
   - `./scripts/security_check_dev_tools.sh /path/to/dev-tools`
4. Review every failure and blocking finding before considering the task complete.
5. If a check is skipped because the file type is absent, leave it skipped; do not fake coverage.
6. If a needed tool is missing, install it or document the gap and residual risk explicitly.
7. Treat secret leaks, unsafe command patterns, workflow security issues, and dependency findings as blocking unless there is a specific reviewed exception.

## Reporting Expectations

- Summarize which script was run.
- Summarize failed checks and whether they were fixed or intentionally deferred.
- Call out any skipped checks that reflect tooling or environment gaps rather than absent file types.

---

# TCC Analysis And Writing Sync Workflow

Use this repo-local workflow after the shared workflow in `.windsurf/workflows/research-analysis-cycle.md`.

This file should only capture the repository surfaces that must stay aligned when thesis-related analysis changes.

## Repository-Specific Steps

1. Identify what changed:
   - data boundary
   - preprocessing or feature logic
   - method or hyperparameters
   - figure or table outputs
   - interpretation or conclusion
2. Trace the changed evidence surfaces:
   - `src/`
   - `scripts/`
   - `notebooks/`
   - `docs/`
   - `docs/llm/` when the local guidance should change too
3. Update the nearest thesis-facing narrative in the same change:
   - methodology note
   - result summary
   - limitation or caveat
   - any repo-level summary that would otherwise become stale
4. Check whether `docs/llm/` should change too when the thesis method framing, chapter structure, or repo-local analytical guidance has shifted.

---

# TCC Method Selection Workflow

Use this workflow when the thesis question is clear enough to choose or refine the analytical method.

## Steps

1. Write the current question in one sentence:
   - what outcome or risk is being studied
   - what unit of analysis is used
   - what time boundary matters
   - what decision the result should support
2. Read `docs/llm/references/usp-mba-course-map.md` and shortlist the most relevant course anchors.
3. Confirm what the repository can currently support:
   - available Bronze, Silver, and Gold datasets
   - municipality and year coverage
   - missingness, linkage, and geospatial availability
   - whether the outcome is continuous, binary, categorical, count-based, hierarchical, or exploratory
4. Pick one primary method and one supporting diagnostic or robustness check.
5. Name the evidence artifacts that must exist after the analysis:
   - notebook or script path
   - figure or table
   - methodology note
   - interpretation or limitation note
6. If the method is more complex than the baseline alternative, explain why the simpler method is insufficient for this question.
7. If the result will support governance or compliance claims, add the caveats before running with the stronger narrative.

## Exit Criteria

- The chosen method has a clear reason tied to the thesis question.
- The required data and artifact outputs are explicit.
- The resulting claim strength matches the method's actual limits.

---

