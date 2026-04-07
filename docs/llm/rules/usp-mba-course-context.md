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
