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
