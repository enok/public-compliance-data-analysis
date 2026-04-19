# Thesis Bibliography Integration Workflow

Use this workflow to systematically insert bibliographical references throughout the thesis document to justify all methods, decisions, and frameworks.

## Purpose

Every analytical decision in the thesis must be supported by academic references. This workflow maps thesis sections to required citations.

## Bilingual Thesis Requirement

This thesis requires **two complete versions** with identical bibliographical support:

| Version | Language | Citation Format | Reference List |
|---------|----------|-----------------|----------------|
| **Primary** | Portuguese (pt-BR) | ABNT NBR 6023:2023 | Alphabetical by author surname |
| **Secondary** | English (en) | ABNT NBR 6023:2023 or APA 7th | Alphabetical by author surname |

**Synchronization rules:**
- Both versions must cite the **same sources** for the same claims
- Both versions must have **identical reference lists** (same works, possibly different formatting)
- Technical terms in references remain in original language
- If a source has Portuguese and English versions, cite the one you consulted

**Bilingual citation templates:**
```
Portuguese:
"A análise de regressão segue Wooldridge (2020), com erros padrão robustos."

English:
"The regression analysis follows Wooldridge (2020), with robust standard errors."
```

**Both versions require the same evidence strength and identical citations.**

## Thesis Section Citation Map

### Chapter 1: Introduction

| Section | Required Citation | Source |
|---------|-------------------|--------|
| Research problem | Gil (2010), Lakatos & Marconi (2010) | `.agents/skills/thesis-bibliography/SKILL.md` §1 |
| Scientific justification | Cervo & Bervian (2002) | §1 |
| Delimitation | Hernández et al. (2010) | §1 |
| Public spending context | Ferreira (2010), Affonso & Araújo (2016) | §7 |
| Compliance context | COSO (2013), Power (2007) | §7 |

**Citation example:**
> "A presente pesquisa segue a estrutura metodológica proposta por Gil (2010) para pesquisas quantitativas, com abordagem descritiva e correlacional."

---

### Chapter 2: Material and Methods

#### 2.1 Data Architecture (Bronze/Silver/Gold)

| Decision | Citation |
|----------|----------|
| Medallion architecture choice | Databricks (2022) |
| Data pipeline design | Kleppmann (2017), Reis & Housley (2022) |
| AWS/S3 storage | AWS (2024) |
| Data quality | McGilvray (2021) |

**Citation template:**
> "A arquitetura de dados segue o padrão medallion (Bronze, Silver, Gold) proposto por Databricks (2022), onde a camada Bronze armazena dados brutos, a Silver dados normalizados e a Gold dados agregados para análise."

#### 2.2 Data Sources

| Source Type | Citation |
|-------------|----------|
| IBGE data | IBGE (2022) - cite official documentation |
| Transparency Portal | CGU (2024) - cite official portal |
| Census methodology | IBGE (2010, 2022) |

#### 2.3 Statistical Methods

| Method | Primary Citation | Secondary Citation |
|--------|------------------|-------------------|
| Descriptive statistics | Bussab & Morettin (2017) | Fávero & Belfiore (2017) |
| Correlation analysis | Bussab & Morettin (2017) | Cohen et al. (2003) |
| Multiple regression | Wooldridge (2020) | Gujarati & Porter (2011) |
| Robust standard errors | Wooldridge (2020) | White (1980) - if cited |
| Log transformation | Kutner et al. (2004) | Chatterjee & Hadi (2012) |
| Multicollinearity check (VIF) | Gujarati & Porter (2011) | Kutner et al. (2004) |
| Cook's distance | Cook & Weisberg (1982) | Chatterjee & Hadi (2012) |

**Citation template for regression:**
> "A análise de regressão múltipla segue a especificação proposta por Wooldridge (2020), com erros padrão robustos à heterocedasticidade (HC3)."

#### 2.4 Machine Learning Methods

| Method | Citation |
|--------|----------|
| ML general approach | James et al. (2021) | Faceli et al. (2021) - PT |
| ElasticNet | James et al. (2021), Ch. 6 | Hastie et al. (2009), Ch. 3 |
| Random Forest | James et al. (2021), Ch. 8 | Breiman (2001) |
| Train/test split | James et al. (2021), Ch. 5 | Kuhn & Johnson (2013) |
| Cross-validation | Arlot & Celisse (2010) | James et al. (2021) |
| Feature importance | Hastie et al. (2009) | James et al. (2021) |
| Model evaluation (R², etc.) | James et al. (2021) | Kuhn & Johnson (2013) |

**Citation template:**
> "Para modelagem preditiva, adotamos as técnicas de ElasticNet e Random Forest conforme descrito por James et al. (2021), com validação cruzada k-fold (Arlot & Celisse, 2010)."

#### 2.5 Unsupervised Learning

| Method | Citation |
|--------|----------|
| K-means clustering | Hartigan & Wong (1979) | Lloyd (1982) |
| Silhouette score | Rousseeuw (1987) | James et al. (2021) |
| Optimal K selection | Tibshirani et al. (2001) | Xu & Tian (2015) |
| PCA | Jolliffe (2002) | Abdi & Williams (2010) |

**Citation template:**
> "A segmentação de municípios emprega o algoritmo K-means (Hartigan & Wong, 1979), com seleção do número ótimo de clusters pelo método da silhueta (Rousseeuw, 1987)."

#### 2.6 Software and Tools

| Tool | Citation |
|------|----------|
| Python | Van Rossum & Drake (2009) |
| Pandas | McKinney (2022) | VanderPlas (2016) |
| Scikit-learn | Pedregosa et al. (2011) |
| Statsmodels (if used) | Seabold & Perktold (2010) |
| Jupyter | Kluyver et al. (2016) |

---

### Chapter 3: Results and Discussion

#### 3.1 Descriptive Results

| Finding | Citation |
|---------|----------|
| Correlation interpretation | Cohen et al. (2003) - effect sizes |
| Distribution description | Bussab & Morettin (2017) |

#### 3.2 Regression Results

| Element | Citation |
|---------|----------|
| Coefficient interpretation | Wooldridge (2020) |
| Significance levels | Triola (2017) |
| R² interpretation | Chatterjee & Hadi (2012) |
| Model fit assessment | Gujarati & Porter (2011) |
| Outlier influence | Cook & Weisberg (1982) |

#### 3.3 ML Results

| Element | Citation |
|---------|----------|
| Model comparison | James et al. (2021) |
| Feature importance | Hastie et al. (2009) |
| Validation approach | Arlot & Celisse (2010) |

#### 3.4 Clustering Results

| Element | Citation |
|---------|----------|
| Cluster validation | Rousseeuw (1987) |
| Cluster profiling | Jain & Dubes (1988) |
| PCA interpretation | Jolliffe (2002) |

#### 3.5 Interpretation Framework

| Concept | Citation |
|---------|----------|
| Detection capacity | Liu et al. (2016), Power (2007) |
| Reporting bias | Thurman et al. (2020) |
| Regional disparities | Santos (1996), Anselin (1995) |
| Public governance | Ferreira (2010), Bardi (2011) |
| Institutional capacity | COSO (2013) |

**Critical interpretation citation:**
> "A interpretação de que sanções refletem capacidade de detecção institucional, não necessariamente níveis de conduta, alinha-se com a literatura de administração pública sobre 'reporting bias' (Power, 2007; Liu et al., 2016)."

---

### Chapter 4: Conclusion

| Element | Citation |
|---------|----------|
| Limitations (n=27) | Cohen et al. (2003) - statistical power |
| Cross-sectional limitations | Wooldridge (2020) - causality |
| Aggregation bias | Anselin (1988) - spatial analysis |
| Future work - spatial | Anselin (1988), Anselin (1995) |
| Future work - temporal | Wooldridge (2020) - panel data |
| Future work - causal | Wooldridge (2020) - causal inference |

---

## Common Citation Patterns

### When Introducing a Method
```
"Para [objetivo], empregamos [método], conforme descrito por [Autor (Ano)]."

Example:
"Para identificar padrões de agrupamento entre municípios, empregamos 
análise de clusters K-means, conforme descrito por Hartigan & Wong (1979)."
```

### When Justifying a Choice
```
"A escolha de [decisão] segue a recomendação de [Autor (Ano)], que 
demonstra [justificativa]."

Example:
"A escolha de erros padrão robustos (HC3) segue a recomendação de 
Wooldridge (2020) para amostras heterocedásticas."
```

### When Interpreting Results
```
"Este resultado é consistente com [Autor (Ano)], que observa [padrão] 
em contextos similares."

Example:
"Este resultado é consistente com Power (2007), que observa que 
jurisdições com maior capacidade institucional tendem a registrar 
mais infrações detectadas."
```

### When Acknowledging Limitations
```
"Conforme ressaltado por [Autor (Ano)], [limitação] impede 
[tipo de conclusão]."

Example:
"Conforme ressaltado por Wooldridge (2020), o desenho 
transversal impede inferências causais."
```

---

## References File Template

Create `docs/thesis_references.md` with this structure:

```markdown
# Thesis References (Referências Bibliográficas)

## Research Methodology
- GIL, A. C. **Como elaborar projetos de pesquisa**. 5. ed. São Paulo: Atlas, 2010.
- LAKATOS, E. M.; MARCONI, M. A. **Fundamentos de metodologia científica**. 7. ed. São Paulo: Atlas, 2010.
- CERVO, A. L.; BERVIAN, P. A. **Metodologia científica**. 6. ed. São Paulo: Pearson, 2002.

## Statistics
- BUSSAB, W. O.; MORETTIN, P. A. **Estatística básica**. 8. ed. São Paulo: Saraiva, 2017.
- WOOLDRIDGE, J. M. **Introductory econometrics: a modern approach**. 7th ed. Boston: Cengage, 2020.
- FÁVERO, L. P.; BELFIORE, P. **Manual de análise de dados**. São Paulo: LTC, 2017.

## Machine Learning
- JAMES, G. et al. **An introduction to statistical learning**. 2nd ed. New York: Springer, 2021.
- HASTIE, T.; TIBSHIRANI, R.; FRIEDMAN, J. **The elements of statistical learning**. 2nd ed. New York: Springer, 2009.

## Data Engineering
- KLEPPMANN, M. **Designing data-intensive applications**. Sebastopol: O'Reilly, 2017.
- DATABRICKS. **The data lakehouse: the next generation of data platforms**. 2022.

## Public Administration
- FERREIRA, F. **Governança pública e controle de gastos no Brasil**. Rio de Janeiro: FGV, 2010.
- AFFONSO, A. J.; ARAÚJO, J. **Fiscalidade, federalismo e políticas públicas no Brasil**. São Paulo: Editora UNESP, 2016.

## Citation Standards
- ABNT. **NBR 6023:2023 - Referências – Elaboração**. Rio de Janeiro: ABNT, 2023.
- ABNT. **NBR 14724:2020 - Trabalhos acadêmicos - Apresentação**. Rio de Janeiro: ABNT, 2020.
```

---

## Citation Audit Checklist

Before finalizing the thesis, verify:

### Introduction
- [ ] Research design citations (Gil, Lakatos)
- [ ] Context citations (Ferreira, Affonso)

### Material and Methods
- [ ] Data architecture citations (Databricks, Kleppmann)
- [ ] Each statistical method cited
- [ ] Each ML method cited
- [ ] Each clustering method cited
- [ ] Software citations (Python, pandas, scikit-learn)

### Results
- [ ] Effect size interpretation (Cohen)
- [ ] Model diagnostics cited
- [ ] Validation approach cited

### Discussion
- [ ] Detection capacity cited (Power, Liu)
- [ ] Regional analysis cited (Santos, Anselin)
- [ ] Governance cited (Ferreira, COSO)

### Conclusion
- [ ] Limitations cited (Wooldridge)
- [ ] Future work methods cited

### General
- [ ] ABNT format verified
- [ ] All in-text citations in reference list
- [ ] No orphan references (not cited in text)

---

## Related Resources

- `.agents/skills/thesis-bibliography/SKILL.md` — Complete bibliography database
- `.agents/skills/research-thesis-support/SKILL.md` — Evidence artifacts and chapter structure
- `docs/llm/workflows/thesis-completion-guide.md` — Full thesis guide
- `docs/llm/workflows/tcc-analysis-and-writing-sync.md` — Writing workflow
