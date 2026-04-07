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
