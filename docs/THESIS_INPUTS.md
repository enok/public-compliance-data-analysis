# Thesis Inputs — Complete Reference

> **TCC Title:** Data-Driven Public Compliance: Correlation between Federal Transfers and Municipal Socioeconomic Indicators
>
> **Author:** Enok Antônio de Jesus | **Advisor:** Prof. Dr. Carlos Nabil Ghobril
>
> **Institution:** USP / ESALQ — MBA in Data Science & Analytics
>
> **Date:** 2026-04-19

---

## Research Question

Does **corruption** (or **poor use of public money**) contribute to **lower HDI** in Brazilian municipalities, impacting the socioeconomic condition of society as a whole?

---

## Thesis Structure

```
┌──────────────────────────────────────────────────────────────────────┐
│  CHAPTER 1: Introduction                                             │
│    - Problem context (corruption in Brazilian public spending)       │
│    - Research objectives                                             │
│    - Relevance and justification                                     │
├──────────────────────────────────────────────────────────────────────┤
│  CHAPTER 2: Material and Methods                                     │
│    - Data sources (IBGE, Portal da Transparência, IPCA)              │
│    - ETL Pipeline (Bronze → Silver → Gold)                           │
│    - Analytical methods (EDA, OLS, ML, K-means, PCA)                 │
├──────────────────────────────────────────────────────────────────────┤
│  CHAPTER 3: Results and Discussion                                   │
│    - Exploratory findings (NB01)                                     │
│    - Statistical inference (NB02)                                    │
│    - Machine learning (NB03)                                         │
│    - Clustering analysis (NB04)                                      │
│    - Corruption vs HDI by cluster (NB06) ← NEW                       │
├──────────────────────────────────────────────────────────────────────┤
│  CHAPTER 4: Conclusion                                               │
│    - Main findings                                                   │
│    - Limitations                                                     │
│    - Policy implications                                             │
│    - Future work                                                     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Part 1: ETL Pipeline (Material and Methods — Section 2.2)

The data engineering pipeline itself is **a core methodological contribution** of this thesis.

### Architecture: Medallion (Bronze/Silver/Gold)

```
    ┌─────────┐      ┌─────────┐      ┌─────────┐
    │ BRONZE  │ ───► │ SILVER  │ ───► │  GOLD   │
    │  (raw)  │      │(normal.)│      │(analyt.)│
    └─────────┘      └─────────┘      └─────────┘
        │                │                │
        ▼                ▼                ▼
    S3 JSON          S3 Parquet      S3 Parquet
    API responses    Dim/Fact        Aggregates
```

### Bronze Layer — Raw Data Ingestion

| Source | Dataset | Period | Records |
|--------|---------|--------|---------|
| **IBGE SIDRA API** | Census 2010 (population, literacy, income, sanitation) | 2010 | 5,565 municipalities |
| **IBGE SIDRA API** | Census 2022 (population, literacy, income, sanitation) | 2022 | 5,570 municipalities |
| **Portal da Transparência** | Federal Transfers (Monthly) | 2013-12 to 2022-12 | ~109 monthly files |
| **Portal da Transparência** | CEIS (Sanctions - Inidôneas) | Cumulative | 22,545 records |
| **Portal da Transparência** | CNEP (Sanctions - Entes Privados) | Cumulative | 1,625 records |
| **Portal da Transparência** | CEPIM (Sanctions - Impedidas) | Cumulative | 3,579 records |
| **BCB SGS 433** | IPCA (Inflation) Monthly Index | 1980-2026 | 47 rows (yearly) |

**Key Evidence:** `2010-01` through `2013-11` revalidated as source-empty under forced refresh.

**Location:** `bronze/` in S3 bucket `enok-mba-thesis-datalake`
**Local Cache:** `data/bronze/` (available when needed)
**Scripts:** `scripts/01_bronze_ingestion.sh`

### Silver Layer — Normalization

| Table | Records | Description |
|-------|---------|-------------|
| `dim_municipalities` | 5,570 | Municipality lookup with state/region |
| `dim_municipality_lookup` | 5,570 | Normalized name lookup for matching |
| `dim_inflation_index` | 47 | IPCA deflator (base 2022 BRL) |
| `fact_population` | 11,135 | 2010 (5,565) + 2022 (5,570) |
| `fact_literacy` | 11,135 | Literacy rate by year |
| `fact_income` | 11,135 | Real + nominal income |
| `fact_sanitation` | 11,135 | Households by year |
| `fact_sanctions` | 27,749 | All sanctions (CEIS + CNEP + CEPIM) |
| `fact_federal_transfers` | 2,065 | Aggregated monthly transfers |

**Location:** `silver/` in S3 + `data/silver/` local
**Script:** `scripts/02_silver_transformation.sh`

### Gold Layer — Analysis-Ready Datasets

| Table | Records | Cols | Purpose |
|-------|---------|------|---------|
| `agg_municipality_socioeconomic` | 5,570 | 21 | Municipal-level socioeconomic features |
| `agg_state_summary` | 27 | 18 | State-level aggregations |
| `agg_sanctions_summary` | 3 | 8 | Summary by sanction type |
| `analysis_compliance` | 27 | 20 | State-level ML-ready |
| `analysis_compliance_municipality` | 5,570 | 22 | Municipal-level ML-ready |
| `consolidated_clustering` | 5,565 | 37 | Clustering features (12 normalized) |

**Location:** `gold/` in S3 + `data/gold/` local (2.0MB)
**Script:** `scripts/03_gold_transformation.sh`

### Data Pipeline Code

| Component | File | Purpose |
|-----------|------|---------|
| Bronze ingestors | `src/ingestion/` | API clients for IBGE, Transparency, BCB |
| Silver transformer | `src/processing/silver_transformer.py` | Schema validation, deflation |
| Gold transformer | `src/processing/gold_transformer.py` | Aggregations, feature engineering |
| Analysis helpers | `src/analysis/` | Clustering, ML, presentation |

---

## Part 2: Analytical Notebooks (Results — Chapter 3)

All notebooks are **bilingual (EN + pt-BR)**.

| Notebook | Purpose | Evidence for Thesis |
|----------|---------|----------------------|
| `01_exploratory_data_analysis` | Descriptive statistics + visualizations | Chapter 3.1 |
| `02_statistical_analysis` | OLS regression, hypothesis testing | Chapter 3.2 |
| `03_machine_learning` | ElasticNet, Random Forest, Logistic | Chapter 3.3 |
| `04_clustering_analysis` | K-means (K=4), PCA, Brazil maps | Chapter 3.4 |
| `05_corruption_hdi_clusters` | Corruption vs HDI by cluster | Chapter 3.5 |
| `06_complete_thesis_pipeline` | **MASTER:** Full ETL + Analysis + QGIS + Dashboard | Chapter 2.2 + 3 + 4 |

### Key Statistical Findings

| Finding | Evidence | Where |
|---------|----------|-------|
| **Income → Sanctions**: r = 0.74 (p < 0.001) | State level (n=27) | NB01, NB02 |
| **OLS R² = 0.835** | Income + regional dummies | NB02 |
| **Norte/Nordeste effect** | β = +22 after controlling for income | NB02 |
| **Distrito Federal outlier** | 65.45 sanctions/100k (2x next state) | NB02 |
| **K-means K=4**, silhouette=0.288 | Municipal clustering | NB04 |
| **PCA 3 PCs = 84.4% variance** | Dimensionality reduction | NB04 |
| **Municipal r = 0.149** (weaker) | n=5,570 cities | NB03 extended |

---

## Part 3: New Analysis - Corruption vs HDI (NB05)

### Hypothesis

**H1:** Municipalities with more sanctions per BRL transferred (corruption/inefficiency proxy) have lower HDI within the same cluster.

### Variables

**Corruption Proxy (X):**
- `sanctions_per_million_brl_transfers`: Number of sanctions per million BRL transferred

**HDI Components (Y):**
- `avg_income_real_2022_2022_brl` (IPCA-adjusted income)
- `literacy_rate_2022` (education proxy)
- `avg_income_2022` (nominal income)

**Control:**
- `cluster` (K-means, K=4)

### Execution

```bash
# Preferred: run the master notebook end-to-end
jupyter nbconvert --to notebook --execute notebooks/06_complete_thesis_pipeline.ipynb

# Alternative: standalone scripts
python3 scripts/run_corruption_hdi_analysis.py
python3 scripts/generate_map_geojson.py
```

### Results

**Analyzed:** 5,570 municipalities total, **469 with sanctions data**, 4 clusters

**Correlation by Cluster:** No statistically significant correlations (p < 0.05) detected within clusters. This suggests:
1. The corruption→HDI effect is **heterogeneous** across municipalities
2. The sanctions metric has **detection bias** (institutional capacity affects reporting)
3. Effect size is **low** at municipal level when controlling for socioeconomic similarity

**Cluster Distribution:**
| Cluster | N | Mean Vulnerability Index |
|---------|---|--------------------------|
| 0 | 233 | +1.70 (high vulnerability) |
| 1 | 119 | +0.59 |
| 2 | 115 | -0.85 (better management) |
| 3 | 2 | -2.48 (megacity outliers) |

**Top 10 States by Vulnerability (states with highest average index):**
1. Acre (3.25)
2. Santa Catarina (3.10)
3. Rondônia (2.97)
4. Paraná (2.25)
5. Espírito Santo (1.90)
6. Roraima (1.62)
7. Minas Gerais (1.59)
8. São Paulo (1.46)
9. Mato Grosso (1.26)
10. Rio Grande do Sul (1.15)

---

## Part 4: Thesis Deliverables

### Quantitative Outputs (CSV/JSON)

| File | Description | Use |
|------|-------------|-----|
| `docs/thesis_presentation_assets/correlation_by_cluster.csv` | Pearson r by cluster/HDI var | Tables Chapter 3.5 |
| `docs/thesis_presentation_assets/cluster_summary.csv` | Cluster statistics | Table Chapter 3.4 |
| `docs/thesis_presentation_assets/representative_sample_cities.csv` | 34 sample cities (EN) | Appendix / Presentation |
| `docs/thesis_presentation_assets/amostra_representativa_cidades.csv` | 34 cidades amostra (pt-BR) | Apêndice / Apresentação |
| `docs/thesis_presentation_assets/representative_sample.json` | Sample in JSON format | Dashboard |

### Geospatial Outputs (GeoJSON)

| File | Size | Description |
|------|------|-------------|
| `docs/thesis_presentation_assets/qgis/brazil_municipalities_vulnerability_index.geojson` | 3.6MB | **Main municipal map (5,570 cities)** - red=high vuln, blue=good mgmt |
| `docs/thesis_presentation_assets/qgis/brazil_states_final_findings.geojson` | 39MB | State-level findings |
| `docs/thesis_presentation_assets/qgis/BR_Municipios_2022.zip` | 195MB | IBGE official municipal boundaries |
| `docs/thesis_presentation_assets/qgis/BR_UF_2022.zip` | 14MB | IBGE official state boundaries |

### Visual Outputs (from notebook execution)

All plots/figures are generated when notebooks are executed with access to data:
- Distribution histograms
- Correlation heatmaps
- PCA scatter plots (2D and 3D)
- K-means cluster visualizations
- Box plots by cluster
- Silhouette analysis
- Elbow method plots
- Brazilian choropleth maps
- Radar charts for cluster profiles

---

## Part 5: Dashboard Recommendations

### Power BI / Tableau / QGIS Integration

**Primary Data Sources:**
- `correlation_by_cluster.csv` → Correlation table
- `cluster_summary.csv` → Cluster profiles
- `representative_sample_cities.csv` → Sample exploration
- `brazil_municipalities_vulnerability_index.geojson` → Map layer

**Suggested Dashboard Layout:**

```
┌────────────────────────────────────────────────────────────────┐
│  DASHBOARD: Public Compliance in Brazilian Municipalities      │
├────────────────────────────────────────────────────────────────┤
│  [MAP]                          [KPI Cards]                    │
│  Brazil Municipal Vulnerability  - Total municipalities: 5,570 │
│  (Red/Blue gradient)             - With sanctions: 469         │
│                                  - Total sanctions: 27,749     │
│                                                                │
├────────────────────────────────────────────────────────────────┤
│  [Cluster Profile]               [Top 10 Vulnerable States]    │
│  - Cluster 0: 233 cities         - Bar chart                   │
│  - Cluster 1: 119 cities                                       │
│  - Cluster 2: 115 cities                                       │
│  - Cluster 3: 2 cities (outliers)                              │
├────────────────────────────────────────────────────────────────┤
│  [Representative Sample Table]                                 │
│  - 17 best managed + 17 most vulnerable                        │
│  - Filterable by cluster/state                                 │
└────────────────────────────────────────────────────────────────┘
```

### QGIS Map Tutorial

1. Open QGIS
2. Layer → Add Layer → Add Vector Layer
3. Select `docs/thesis_presentation_assets/qgis/brazil_municipalities_vulnerability_index.geojson`
4. Right-click layer → Properties → Symbology
5. Change to **Graduated**
6. Column: `vulnerability_index`
7. Color ramp: **RdYlBu** inverted (red = high, blue = low)
8. Classification: Quantile, 5 classes
9. Apply

---

## Part 6: Project Structure for Thesis Writing

```
public-compliance-data-analysis/
├── config/                        # Data contracts (metadata)
├── data/                          # Local Gold cache (2MB)
│   ├── silver/                    # Normalized tables (1.7MB)
│   └── gold/                      # Analysis-ready (2.0MB)
├── docs/                          # Documentation + deliverables
│   ├── 01_BRONZE_LAYER.md         # Bronze layer doc (EN + pt-BR)
│   ├── 02_SILVER_LAYER.md         # Silver layer doc (EN + pt-BR)
│   ├── 03_GOLD_LAYER.md           # Gold layer doc (EN + pt-BR)
│   ├── thesis_conclusion.md       # Findings summary (EN + pt-BR)
│   ├── THESIS_INPUTS.md           # THIS FILE (EN + pt-BR)
│   └── thesis_presentation_assets/
│       ├── qgis/                  # Map files
│       └── *.csv / *.json         # Dashboard data
├── infra/                         # Terraform IaC
├── notebooks/                     # Bilingual (6 notebooks × 2)
├── scripts/                       # ETL + analysis scripts
├── src/                           # Python source code
└── tests/                         # Test suite (125 tests)
```

---

## Part 7: Reproducibility

**To reproduce all results from scratch:**

```bash
# 1. Clone and setup environment
git clone <repo>
cd public-compliance-data-analysis
python3 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# 2. Configure AWS profile
aws configure --profile mba-thesis
# Set credentials for read access to s3://enok-mba-thesis-datalake

# 3. Run full ETL (Bronze → Silver → Gold)
./scripts/01_bronze_ingestion.sh
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh

# 4. Run master notebook (analysis + QGIS + dashboard)
jupyter nbconvert --to notebook --execute notebooks/06_complete_thesis_pipeline.ipynb

# 5. Run all tests
python3 -m pytest tests/ -v

# 7. Execute notebooks (requires jupyter)
jupyter notebook notebooks/
```

**Regenerate entire pipeline from Bronze (requires API credentials):**

```bash
# 1. Set API keys in .env
cp .env.example .env
# Edit .env with TRANSPARENCY_API_KEY

# 2. Run Bronze ingestion
./scripts/01_bronze_ingestion.sh

# 3. Run Silver transformation
./scripts/02_silver_transformation.sh

# 4. Run Gold transformation
./scripts/03_gold_transformation.sh
```

---

## Part 8: Validation Status

| Check | Status | Count |
|-------|--------|-------|
| Security scan (no secrets) | PASS | 0 failed |
| Pytest suite | PASS | 137 passed, 3 skipped |
| Bilingual notebooks | PASS | 6 pairs matched |
| Local Silver data | PASS | 9 tables |
| Local Gold data | PASS | 6 tables |
| GeoJSON generated | PASS | 5,570 features |
| CSV exports | PASS | 5 files |

---

## Citation

For thesis references, cite this toolkit as:

> DE JESUS, E. A. *Compliance Público Baseado em Dados: Correlação entre Repasses Federais e Indicadores Socioeconômicos Municipais*. Trabalho de Conclusão de Curso (MBA em Data Science & Analytics) — USP/ESALQ, Piracicaba, 2026.

---

**File last updated:** 2026-04-19

**Contact:** See repository README.md
