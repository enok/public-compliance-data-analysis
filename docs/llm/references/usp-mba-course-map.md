# USP MBA Course Map For This Repository

Use this reference when a task touches the thesis methodology, chapter structure, or method selection and you want the answer grounded in the MBA course material rather than generic internet advice.

The full machine-generated inventory lives in [`usp-mba-course-inventory.generated.md`](usp-mba-course-inventory.generated.md). This curated map is the working layer for day-to-day TCC support.

The `aulas/` corpus currently spans opening material, statistics, programming, wrangling, supervised and unsupervised ML, NLP, spatial analysis, data engineering, cloud and deployment, governance, writing, TCC guidance, and leadership-oriented modules. Use the generated inventory whenever a task references a course that is not explicitly summarized below.

## High-Value Course Anchors

### Research framing and thesis structure

- `11_tcc`: topic areas, research framing, and examples of TCC-oriented problem definition
- `15_TCC`: thesis structure, expected deliverables, and how to organize the written argument
- `17_Fundamentos-de-redacao-tecnico-cientifica`: technical-scientific writing, argument flow, and document discipline

Use these modules when the task is about:

- narrowing the research question
- aligning a chapter with the evidence already produced
- strengthening methodology or limitations sections
- deciding what belongs in the thesis body versus appendices or technical docs

### Statistics and classical analytical methods

- `01_Fundamentos-de-Estatistica`: distributions, descriptive statistics, tests, and interpretation basics
- `08_Analise-Estatistica_e_Espacial`: spatial reasoning, regional analysis, and map-oriented interpretation
- `12_Pesquisa-Operacional`: optimization thinking and structured problem formulation
- `25_Modelagem_Matematica_e_Estruturacao_de_Problemas`: formal problem structuring before method choice
- `29_Supervised_Machine_Learning_Analise_de_Regressao_Simples_e_Multipla`: linear regression foundations for outcome modeling
- `32_Supervised Machine Learning Modelagem Multinível`: multilevel reasoning when geography or hierarchy matters

Use these modules when the task is about:

- baseline statistical analysis
- municipal-level outcome modeling
- geospatial comparisons across regions
- translating a policy question into a measurable analytical problem

### Data preparation and engineering

- `02_Introducao-a-Programacao-com-Python`: Python foundations and analytical tooling
- `03_Data-Wrangling`: joins, cleaning, reshaping, and pandas-heavy preparation work
- `10_Coleta-de-Dados-Crawlers-e-Web-Scraping`: external data collection patterns when the project expands beyond current sources
- `14_Engenharia-de-dados`: data architecture and pipeline thinking
- `19_Cloud-Computing`: infrastructure thinking around cloud-hosted analytical workflows
- `33_Big Data e Deployment de Modelos`: production-adjacent deployment mindset for analytical outputs

Use these modules when the task is about:

- Bronze or Silver dataset integration
- schema alignment and data preparation
- turning notebook logic into pipeline logic
- explaining why the repo uses medallion-style layers and AWS-backed storage

### Supervised and unsupervised modeling options

- `04_Arvores-Redes-e-Ensemble-Models`: tree-based models and ensemble alternatives
- `05_Deep-Learning`: neural-network material, mostly secondary for this repository unless a strong justification appears
- `06_SVM`: margin-based classification alternatives
- `07_Text-Mining_Sentiment-Analysis_e_NLP`: text features if narrative or legal text corpora enter scope
- `26_Unsupervised_Machine_Learning_Clustering`: clustering for municipality or risk-profile segmentation
- `27_Unsupervised_Machine_Learning_Analise_Fatorial_e_PCA`: dimensionality reduction and latent-structure analysis
- `28_Unsupervised_Machine_Learning_Analise_de_Correspondencia_Simples_e_Multipla`: categorical association structure
- `30_Supervised Machine Learning Modelos Logísticos Binários e Multinomiais`: event or category outcomes
- `31_Supervised Machine Learning Modelos para Dados de Contagem`: count outcomes such as incident or case volumes

Use these modules when the task is about:

- anomaly or municipality profiling
- dimension reduction before modeling
- binary, multinomial, or count outcomes
- exploratory segmentation that complements the main regression narrative

### Governance, communication, and policy context

- `09_Gestao-da-Mudanca-na-Era-Digital`: organizational-change framing for public-sector adoption
- `13_Data-Visualization`: result communication and figure selection
- `16_Metodologias-Ageis`: delivery slicing and iteration discipline
- `21_Analise-Da-Conjuntura-Economica-em-Cenarios-de-Tecnologias-Disruptivas`: macroeconomic and contextual interpretation
- `22_Analytics-e-Gestao-de-Riscos`: risk framing aligned with compliance and anomaly detection
- `23_Lideranca-em-Data-Science`: stakeholder communication and executive framing
- `24_Legislacao-no-Ambiente-Digital-LGPD`: legal and governance boundaries around public data use

Use these modules when the task is about:

- presenting findings to non-technical readers
- connecting anomaly results to governance or compliance risk
- stating LGPD and public-data constraints correctly
- keeping the thesis readable for both academic and practitioner audiences

### Advanced methods, platform, and adjacent class coverage

- `18_Computacao-Evolucionaria`: search and optimization heuristics when classical optimization or standard supervised methods are insufficient
- `19_Cloud-Computing`: cloud-service framing and infrastructure tradeoffs for analytical systems
- `20_Social_Network_Analysis`: graph-oriented exploratory analysis when entities and relationships matter more than tabular features
- `23_Lideranca-em-Data-Science`: communication, team alignment, and stakeholder framing for analytical work
- `33_Big Data e Deployment de Modelos`: productionization, delivery, and deployment framing for model-backed systems
- `Palestras`: supplemental context that may inform examples or framing, but not usually the methodological default

Use these modules when the task is about:

- choosing between analytical exploration and production deployment concerns
- explaining infrastructure or delivery tradeoffs to a non-specialist audience
- bringing graph, evolutionary, or big-data concepts into scope with explicit justification
- connecting technical outputs to leadership, adoption, or organizational execution

## Recommended Method Defaults For This Repo

Start with these defaults unless the task clearly calls for something else:

1. Use `01_Fundamentos-de-Estatistica` plus `29_Supervised_Machine_Learning_Analise_de_Regressao_Simples_e_Multipla` for baseline outcome analysis.
2. Add `08_Analise-Estatistica_e_Espacial` when the question has a geographic pattern or regional disparity angle.
3. Use `26`, `27`, or `28` for exploratory segmentation, latent structure, or association analysis that supports but does not replace the main thesis narrative.
4. Use `30` or `31` only when the dependent variable is naturally binary, multinomial, or count-based.
5. Use `14_Engenharia-de-dados` and `03_Data-Wrangling` whenever analytical work requires new source integration, contract changes, or reusable preprocessing.
6. Use `11_tcc`, `15_TCC`, and `17_Fundamentos-de-redacao-tecnico-cientifica` whenever a result changes the written argument or chapter structure.

## Representative Source Assets

These course files are especially relevant to the current repository shape:

- `aulas/01_Fundamentos-de-Estatistica/Fundamentos de Estatistica 11-16 e 18.10.2024.pdf`
- `aulas/03_Data-Wrangling/Folder módulos internacionais de DSA.pdf`
- `aulas/08_Analise-Estatistica_e_Espacial/Analise Estatistica Espacial 25.04.2025.pdf`
- `aulas/11_tcc/02_Areas-de-pesquisa/01-gestao-e-planejamento-estrategico-em-projetos-de-data-science/01_TCC_Data Science e Analytics.pdf`
- `aulas/14_Engenharia-de-dados/Engenharia de Dados 24.06.2025.pdf`
- `aulas/15_TCC/Nocoes gerais TCC 14 e 18.07.25.pdf`
- `aulas/17_Fundamentos-de-redacao-tecnico-cientifica/Fundamentos de redacao 29.07 e 01.08.2025_SL.pdf`
- `aulas/22_Analytics-e-Gestao-de-Riscos/Analytics e Gestão de Riscos 07.10.2025_SL.pdf`
- `aulas/29_Supervised_Machine_Learning_Analise_de_Regressao_Simples_e_Multipla/Supervised M. Learning 03, 10, 24.02 e 03.03.2026_SL (1).pdf`
- `aulas/30_Supervised Machine Learning Modelos Logísticos Binários e Multinomiais/02 - SCRIPT - MODELOS LOGÍSTICOS BINÁRIOS E MULTINOMIAIS.py`

## Pair With Shared Toolkit Assets

When working in this repository, combine this reference with:

- shared skill: `research-thesis-support`
- shared rules: `research-rigor.md`, `evidence-based-reporting.md`, `analytics-reproducibility.md`, `ml-experiment-governance.md`
- local rules: `../rules/usp-mba-course-context.md`, `../rules/tcc-deliverables-and-argument.md`
- local workflows: `../workflows/tcc-method-selection.md`, `../workflows/tcc-analysis-and-writing-sync.md`
