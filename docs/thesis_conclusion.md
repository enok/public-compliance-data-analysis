# Thesis Conclusion — Public Compliance Data Analysis

> **MBA Thesis**: Public Spending Efficiency, Compliance Risk, and Anomaly Detection in Brazil
>
> **Data scope**: 5,570 municipalities, 27 states, Census 2010/2022, Transparency Portal sanctions (CEIS, CNEP, CEPIM), inflation-adjusted income (IPCA, base 2022 BRL)

## 1. Summary of Findings

This study investigated the relationship between socioeconomic indicators and public compliance sanctions across Brazilian states and municipalities. Four complementary analytical approaches were applied: exploratory data analysis, statistical inference, supervised machine learning, and unsupervised clustering. Together, they converge on a consistent set of findings.

### 1.1 Income Is the Dominant Predictor of Sanctions Rates

Across all methods, **average income** emerged as the single strongest predictor of sanctions per 100,000 population:

- Bivariate correlation: r = 0.74 (p < 0.001)
- OLS regression: log income coefficient β = 49.75 (p < 0.001), R² = 0.835
- Machine learning: ElasticNet and Random Forest both rank income-related features highest
- Clustering: income is a primary differentiator between the four municipality clusters

This finding is robust to model specification and method choice. However, the direction of the relationship is counterintuitive: **higher-income states have more sanctions per capita**, not fewer.

### 1.2 Sanctions Reflect Detection Capacity, Not Misconduct Levels

The most parsimonious explanation for the income–sanctions association is **institutional detection capacity**. States and municipalities with higher income tend to have:

- Stronger audit and oversight institutions
- More public servants trained in compliance procedures
- Greater digitization and reporting infrastructure
- Higher visibility of federal transfers subject to monitoring

This interpretation aligns with the public administration literature on "reporting bias" — jurisdictions with more capacity to detect and record violations produce more sanctions records, independent of actual misconduct levels.

### 1.3 Regional Effects Persist After Controlling for Income

OLS regression with regional dummies reveals that **Norte** (β = 20.94, p = 0.003) and **Nordeste** (β = 22.97, p = 0.009) have significantly higher sanctions rates than expected given their income levels. This suggests governance, institutional, or structural factors beyond socioeconomic development contribute to compliance risk in these regions.

In contrast, the **Sul** region shows a marginally significant negative effect (β = −9.72, p = 0.050), indicating lower-than-expected sanctions rates relative to its income level.

### 1.4 The Distrito Federal Is a Structural Outlier

With 65.45 sanctions per 100,000 population — more than double the next state (Rondônia at 31.56) — the Distrito Federal is the single most influential observation in every model. As the seat of federal government with concentrated oversight infrastructure, it represents a structural artifact of institutional concentration rather than a generalizable compliance pattern. Cook's distance analysis confirms it as the most influential point in the regression.

### 1.5 Brazil's Municipalities Exhibit a Dual Socioeconomic Structure

K-means clustering (K = 4, silhouette = 0.288) on 12 census-derived features across 5,565 municipalities reveals a clear dual structure:

- **Developed cluster** (Clusters 0, 2, 3 — 61.2%): Higher income, higher literacy, concentrated in Sudeste and Sul
- **Less-developed cluster** (Cluster 1 — 38.8%): Lower income, lower literacy, concentrated in Nordeste and Norte

PCA confirms this structure: three principal components explain 84.4% of total variance, with the first component (39.2%) loading on general socioeconomic development.

This dual structure contextualizes why compliance patterns differ regionally — the underlying socioeconomic gradient shapes both the generation of public transfers and the institutional capacity to monitor them.

### 1.6 Municipality-Level Supervised Extension Confirms Direction, With Lower Explanatory Power

On **April 8, 2026**, the municipality-level supervised workflow (`scripts/run_city_full_analysis.py`) was executed over `gold/analysis_compliance_municipality` with **n = 5,570 municipalities**:

- strongest linear correlation with sanctions rates: `log_income` (**r = 0.149**)
- OLS model (HC3 robust errors): **R² = 0.024**, adjusted **R² = 0.023**
- best ML benchmark model: ElasticNet with test **R² = 0.030**

The city-level extension keeps the same directional interpretation (income and structural capacity proxies remain relevant), but effect sizes are weaker and explained variance is low. This indicates substantial municipality-level heterogeneity and measurement noise in geolocated sanctions records.

### 1.7 Presentation Layer Consolidates the Final Thesis Evidence

On **April 9, 2026**, the presentation-assets workflow (`scripts/build_thesis_presentation_assets.py`) was executed to operationalize the final analytical evidence for defense and stakeholder communication:

- **QGIS package**: IBGE official state boundaries (`BR_UF_2022.zip`) plus enriched GeoJSON (`brazil_states_final_findings.geojson`) containing state-level sanctions metrics and city-cluster composition indicators.
- **Power BI package**: star-schema CSV tables, DAX starter measures, relationship guide, and storyboard aligned with the state/city findings and cluster benchmarking outputs.

This presentation layer does not introduce new inferential claims; it consolidates reproducible communication of already-established results and improves consistency between technical evidence and thesis narrative.

## 2. Limitations

### 2.1 Sample Size

State-level analysis operates with only **n = 27 observations**, severely limiting statistical power and the number of predictors that can be reliably estimated. Machine learning classification models performed poorly (best F1 = 0.51), and results should be treated as exploratory at this aggregation level.

### 2.2 Cross-Sectional Design

All analyses use a single temporal cross-section. **Causality cannot be established** — the income–sanctions association may reflect reverse causation (sanctions-driven institutional investment), confounding (third variables driving both), or the detection-capacity mechanism hypothesized above.

### 2.3 Detection Bias

Sanctions records measure **detected and recorded violations**, not actual misconduct. Jurisdictions with weaker oversight may have fewer recorded sanctions but equal or greater actual compliance failures. This fundamental measurement limitation applies to all findings.

### 2.4 Aggregation Level

State-level models mask within-state variation. Municipality-level clustering reveals substantial heterogeneity within states, and municipality-level supervised models currently achieve low explanatory power, suggesting that state-only conclusions may oversimplify the compliance landscape while city-level outcomes still require richer predictors.

### 2.5 Income Deflation

Inflation-adjusted income columns use IPCA deflation to 2022 BRL. Results depend on the deflator series chosen. Alternative deflators (IGP-M, regional CPIs) could shift the magnitude of income-related coefficients.

## 3. Policy Implications

### 3.1 Invest in Oversight Capacity, Not Punitive Measures

The finding that higher income predicts more sanctions — likely through detection capacity — implies that **strengthening audit infrastructure in lower-income regions** would improve compliance visibility without requiring punitive policy changes.

### 3.2 Regional Targeting

The Norte and Nordeste regional effects suggest these regions may benefit from targeted compliance programs that account for their specific governance challenges, beyond what income-based models predict.

### 3.3 Outlier-Aware Policy Design

The Distrito Federal's extreme position cautions against using national averages or rankings to compare states. Compliance benchmarks should account for structural differences in governance architecture.

## 4. Future Work

1. **Temporal analysis**: Incorporate multi-year sanctions data to test whether income–sanctions relationships are stable over time or driven by specific policy periods.
2. **Municipality-level regression (baseline now implemented)**: Use `gold/analysis_compliance_municipality` plus `scripts/run_city_full_analysis.py` to run city-level correlation, OLS, ML benchmarking, and conclusion addendum generation; extend this baseline with spatial econometric methods.
3. **Spatial econometrics**: Apply spatial lag and spatial error models to test whether sanctions in neighboring municipalities are correlated (spatial autocorrelation).
4. **Alternative clustering**: Test density-based methods (DBSCAN, HDBSCAN) that do not assume spherical clusters and can better handle the skewed distributions observed in the data.
5. **Text and legal analysis**: Incorporate sanction descriptions and legal categories to distinguish between types of compliance failures (fiscal, contractual, administrative).
6. **Causal identification**: Design difference-in-differences or instrumental-variable approaches to move beyond associational evidence toward causal claims about institutional capacity.
7. **Interactive evidence dissemination**: Evolve the current QGIS and Power BI assets into a versioned, periodically refreshed thesis companion dashboard directly linked to the medallion pipeline outputs.

---

*Evidence base: 4 executed notebooks (NB01–NB04), city-level workflow execution via `scripts/run_city_full_analysis.py` on April 8, 2026, and presentation-assets workflow execution via `scripts/build_thesis_presentation_assets.py` on April 9, 2026; Gold layer datasets from the public-compliance-data-analysis medallion pipeline; Census 2010/2022; Transparency Portal sanctions registries (CEIS, CNEP, CEPIM); official IBGE territorial boundaries (2022).*
