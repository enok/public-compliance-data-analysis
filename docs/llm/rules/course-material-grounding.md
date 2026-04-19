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

## TCC Intensive Study Roadmap

For concentrated thesis preparation, follow the 12-day intensive plan from `aulas/TODO_TCC_ROADMAP.md`:

### Core Days (Priority Order)
1. **Days 1-2**: TCC framing and writing (`11_tcc`, `15_TCC`, `17_Fundamentos-de-redacao-tecnico-cientifica`)
2. **Days 3-4**: Statistical foundations and regression (`01_Fundamentos-de-Estatistica`, `29_Supervised Machine Learning`)
3. **Days 5-6**: Dimensionality and spatial analysis (`27_Analise Fatorial e PCA`, `08_Analise Estatistica e Espacial`)
4. **Days 7-8**: Complementary models and segmentation (`30_Modelos Logisticos`, `31_Modelos para Dados de Contagem`, `26_Clustering`)
5. **Days 9-10**: Data engineering and pipelines (`03_Data-Wrangling`, `14_Engenharia-de-dados`)
6. **Days 11-12**: Final writing and consolidation (`17_Fundamentos-de-redacao-tecnico-cientifica`, buffer)

### Key Assets by Course

| Course | Key Asset | Purpose |
|--------|-----------|---------|
| `11_tcc` | `03_Estrutura-do-TCC/*.pdf` | Chapter structure templates |
| `15_TCC` | `Nocoes gerais TCC 14 e 18.07.25.pdf` | Deliverable expectations |
| `17_Fundamentos-de-redacao-tecnico-cientifica` | `Fundamentos de redacao 29.07 e 01.08.2025_SL.pdf` | Scientific writing guidance |
| `01_Fundamentos-de-Estatistica` | `Fundamentos de Estatistica 11-16 e 18.10.2024.pdf` | Statistical foundations |
| `29_Supervised Machine Learning` | `Supervised M. Learning 03, 10, 24.02 e 03.03.2026_SL.pdf` | Regression methods |
| `26_Unsupervised Machine Learning Clustering` | Clustering scripts and slides | Segmentation techniques |
| `14_Engenharia-de-dados` | `Engenharia de Dados 24.06.2025.pdf` | Pipeline architecture |
| `03_Data-Wrangling` | `Data Wrangling Python.zip` | Pandas/prep patterns |
| `22_Analytics-e-Gestao-de-Riscos` | `Analytics e Gestão de Riscos 07.10.2025_SL.pdf` | Risk framing |
