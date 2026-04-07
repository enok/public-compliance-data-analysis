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
