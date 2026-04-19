# TCC Method Selection Workflow

Use this workflow when the thesis question is clear enough to choose or refine the analytical method.

Based on the TCC course methodology framework (`11_tcc/04_Metodologias-de-pesquisa`), method selection should follow a structured reasoning path from question to evidence.

## Steps

### 1. Frame the Research Question

Write the current question in one sentence covering:
- What outcome or risk is being studied
- What unit of analysis is used (municipalities, individuals, transactions)
- What time boundary matters (cross-sectional, panel, time-series)
- What comparison or contrast is central
- What decision the result should support

### 2. Ground in Course Materials

Read `docs/llm/references/usp-mba-course-map.md` and shortlist the most relevant course anchors:
- Statistical foundations (`01_Fundamentos-de-Estatistica`)
- Outcome modeling (`29_Supervised_Machine_Learning_Analise_de_Regressao_Simples_e_Multipla`)
- Segmentation (`26_Unsupervised_Machine_Learning_Clustering`)
- Spatial analysis (`08_Analise-Estatistica_e_Espacial`)
- Risk framing (`22_Analytics-e-Gestao-de-Riscos`)

### 3. Assess Data Availability

Confirm what the repository can currently support:
- Available Bronze, Silver, and Gold datasets
- Municipality and year coverage (2010-2022 baseline)
- Missingness patterns and linkage feasibility
- Geospatial availability for spatial methods
- Outcome type: continuous, binary, categorical, count-based, hierarchical, or exploratory

### 4. Select Primary and Supporting Methods

Pick **one primary method** for the main evidence claim and **one supporting diagnostic**:

| Outcome Type | Primary Method Options | Supporting Diagnostic |
|--------------|------------------------|----------------------|
| Continuous (income, spending) | Multiple regression | Residual analysis, multicollinearity check |
| Binary (sanctioned yes/no) | Logistic regression | ROC/AUC, calibration check |
| Count (violations, transfers) | Poisson/Negative Binomial | Overdispersion test |
| Categorical (risk tier) | Multinomial logit | Classification accuracy |
| Exploratory (patterns) | Clustering + PCA | Silhouette scores, factor loadings |

### 5. Define Evidence Artifacts

Name the specific deliverables that must exist:
- **Notebook/script path**: Where the analysis lives
- **Figures**: Visualization of key relationships
- **Tables**: Summary statistics, coefficients, diagnostics
- **Methodology note**: Why this method fits the question
- **Interpretation note**: What the results mean (and don't mean)
- **Limitation note**: Validity threats and boundary conditions

### 6. Justify Complexity

If the chosen method is more complex than a simpler alternative (e.g., random forest vs. linear regression), explicitly state:
- What the simpler method cannot capture
- Why that limitation matters for this question
- How the additional complexity translates to better evidence

### 7. Pre-empt Governance Claims

If the analysis could inform policy, compliance, or enforcement:
- State association vs. causation limits before presenting results
- Note data gaps that could bias interpretation
- Frame findings as "risk indicators" not "proof of wrongdoing"
- Reference `24_Legislacao-no-Ambiente-Digital-LGPD` for data use boundaries

## Exit Criteria

- [ ] The chosen method has a clear reason tied to the thesis question
- [ ] The primary and supporting methods are explicitly named
- [ ] The required data and artifact outputs are explicit
- [ ] The justification for method complexity (if applicable) is documented
- [ ] The resulting claim strength matches the method's actual limits
- [ ] Governance and compliance caveats are pre-stated (if applicable)

## Course Material References

- `11_tcc/03_Estrutura-do-TCC/02-Material e Métodos.pdf`: Methods section structure
- `11_tcc/04_Metodologias-de-pesquisa/`: Research methodology selection
- `docs/llm/rules/tcc-deliverables-and-argument.md`: Deliverable expectations
