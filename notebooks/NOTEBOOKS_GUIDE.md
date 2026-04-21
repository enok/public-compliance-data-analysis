# Analysis Notebooks Guide

This guide summarizes the recommended notebook flow and expected datasets.

---

## Notebook Sequence (Recommended Order)

The notebooks are organized in a logical sequence from data engineering to analysis to thesis compilation:

| Order | Notebook | Language | Purpose |
|-------|----------|----------|---------|
| 0 | `00_etl_pipeline.ipynb` | English | **ETL Pipeline**: Bronze → Silver → Gold layer execution |
| 0 | `00_etl_pipeline.pt-BR.ipynb` | Portuguese | **Pipeline ETL**: Execução completa Bronze → Silver → Gold |
| 1 | `01_exploratory_data_analysis.ipynb` | English | **EDA**: Data profiling, distributions, quality checks |
| 1 | `01_exploratory_data_analysis.pt-BR.ipynb` | Portuguese | **EDA**: Perfil de dados, distribuições, quality checks |
| 2 | `02_statistical_analysis.ipynb` | English | **Statistics**: Correlation, OLS regression, hypothesis tests |
| 2 | `02_statistical_analysis.pt-BR.ipynb` | Portuguese | **Estatística**: Correlação, regressão OLS, testes de hipótese |
| 3 | `03_machine_learning.ipynb` | English | **ML**: Supervised learning models (ElasticNet, Random Forest) |
| 3 | `03_machine_learning.pt-BR.ipynb` | Portuguese | **ML**: Modelos de aprendizado supervisionado |
| 4 | `04_clustering_analysis.ipynb` | English | **Clustering**: K-means segmentation, PCA visualization |
| 4 | `04_clustering_analysis.pt-BR.ipynb` | Portuguese | **Clusterização**: Segmentação K-means, visualização PCA |
| 5 | `05_corruption_hdi_clusters.ipynb` | English | **Corruption vs HDI**: Cluster-stratified correlation analysis |
| 5 | `05_corruption_hdi_clusters_pt-BR.ipynb` | Portuguese | **Corrupção vs IDH**: Análise de correlação estratificada |
| 6 | `06_complete_thesis_pipeline.ipynb` | English | **Master Notebook**: Complete ETL + all analyses + QGIS + Dashboard |
| 6 | `06_complete_thesis_pipeline.pt-BR.ipynb` | Portuguese | **Notebook Master**: ETL completo + todas análises + QGIS + Dashboard |

---

## Notebook Categories

### Category 0: Data Engineering (ETL)

**Notebooks**: `00_etl_pipeline.ipynb` (EN), `00_etl_pipeline.pt-BR.ipynb` (PT)

Run this first to materialize the data layers from raw sources:
1. **Bronze Layer**: Raw API ingestion (IBGE, Portal da Transparência)
2. **Silver Layer**: Normalization, deflation, star schema
3. **Gold Layer**: Analysis-ready aggregations

**Prerequisites**: AWS credentials, API keys in `.env` (for Bronze)

**Use Case**: Run this when:
- Starting from scratch (no local data)
- Need to refresh data from APIs
- Setting up a new environment

---

### Category 1-5: Individual Analysis Notebooks (Modular)

**Notebooks**: `01` through `05` (both languages)

Run these for focused analysis on specific topics after ETL is complete:

| Order | Notebook | Purpose |
|-------|----------|---------|
| 1 | `01_exploratory_data_analysis.ipynb` | Data profiling, distributions, quality checks |
| 2 | `02_statistical_analysis.ipynb` | Correlation, OLS regression, hypothesis tests |
| 3 | `03_machine_learning.ipynb` | Supervised learning models and evaluation |
| 4 | `04_clustering_analysis.ipynb` | Unsupervised segmentation, PCA visualization |
| 5 | `05_corruption_hdi_clusters.ipynb` | Corruption proxy vs HDI by cluster |

**Use Case**: Use these when:
- You've already run `00_etl_pipeline` and have Gold data
- You want to explore specific analyses in depth
- You need to iterate on a specific methodology

---

### Category 6: Master Thesis Notebook (Complete Workflow)

**Notebooks**: `06_complete_thesis_pipeline.ipynb` (EN), `06_complete_thesis_pipeline.pt-BR.ipynb` (PT)

Run this for a complete thesis analysis from data to conclusions in one notebook:
1. ETL (Bronze → Silver → Gold) - optional, can use existing data
2. Exploratory Data Analysis
3. Statistical Analysis
4. Machine Learning
5. Clustering Analysis
6. Corruption vs HDI Analysis
7. QGIS Map Generation
8. Dashboard Export
9. Conclusions & References

**Use Case**: Run this when:
- You want the complete thesis workflow in one document
- Generating final thesis evidence
- Need a single reproducible artifact

---

## Recommended Execution Paths

### Path A: Modular ETL + Individual Analyses (Recommended for Development)

Run notebooks in sequence for step-by-step execution:

```bash
# Step 0: Data Engineering (run once to materialize data)
jupyter notebook notebooks/00_etl_pipeline.ipynb

# Steps 1-5: Individual analyses (can run independently after ETL)
jupyter notebook notebooks/01_exploratory_data_analysis.ipynb
jupyter notebook notebooks/02_statistical_analysis.ipynb
jupyter notebook notebooks/03_machine_learning.ipynb
jupyter notebook notebooks/04_clustering_analysis.ipynb
jupyter notebook notebooks/05_corruption_hdi_clusters.ipynb
```

This path allows you to:
- Execute and verify each layer (Bronze, Silver, Gold)
- Iterate on individual analyses without re-running ETL
- Debug issues at specific stages

### Path B: Master Thesis Notebook (Recommended for Final Results)

Run the complete thesis pipeline in one notebook:

```bash
# For English
jupyter notebook notebooks/06_complete_thesis_pipeline.ipynb

# For Portuguese
jupyter notebook notebooks/06_complete_thesis_pipeline.pt-BR.ipynb
```

This single notebook will:
- Execute ETL (or use existing cached data)
- Run all analyses (EDA, Statistics, ML, Clustering, Corruption vs HDI)
- Generate QGIS maps and Dashboard exports
- Produce final conclusions with full bibliography

**Best for**: Generating the complete thesis evidence document.

---

## Notebook Goals

### 0) ETL Pipeline
- Ingest raw data from APIs (IBGE, Portal da Transparência)
- Normalize and transform (Bronze → Silver → Gold)
- Materialize analysis-ready datasets

### 1) EDA
- Profile data quality
- Inspect distributions and missingness
- Compare regions/states

### 2) Statistical Analysis
- Correlation and hypothesis testing
- OLS modeling and diagnostics
- Regression interpretation

### 3) Machine Learning
- Supervised baselines for regression/classification
- Model comparison and feature relevance

### 4) Clustering
- PCA dimensionality reduction
- K-means segmentation on consolidated municipal features

### 5) Corruption vs HDI
- Cluster-stratified correlation analysis
- Vulnerability index construction
- Policy implications

### 6) Master Notebook
- Complete thesis workflow in one document
- All bibliographic references (ABNT format)
- Reproducible from raw data to conclusions

---

## Environment Setup

```bash
pip install -r ../requirements.txt
./scripts/03_gold_transformation.sh
cd notebooks
jupyter notebook
```

---

## Data Access Pattern

```python
from src.analysis.data_loader import GoldDataLoader

loader = GoldDataLoader("enok-mba-thesis-datalake", aws_profile="mba-thesis")
datasets = loader.load_all()
```

Available dataset keys:
- `analysis_compliance`
- `municipality_socioeconomic`
- `state_summary`
- `sanctions_summary`
- `consolidated_clustering`

---

## Notebook Structure Standard

All notebooks follow the same top structure:
- `# Packages`
- `# Reproducibility` with fixed `SEED = 42`
- data loading block
- analysis sections
- findings summary

---

## Common Issues

### Missing data (no local Bronze/Silver/Gold)
Run the ETL pipeline first:

```bash
# Option 1: Via notebook
jupyter notebook notebooks/00_etl_pipeline.ipynb

# Option 2: Via shell scripts
./scripts/01_bronze_ingestion.sh
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh

# Or all at once
./scripts/run_pipeline.sh
```

### AWS auth problems
Verify active profile and credentials before opening notebooks:

```bash
aws configure --profile mba-thesis
aws sts get-caller-identity --profile mba-thesis
```

### Missing dependencies
All notebooks include an auto-install cell at the top. If skipped, run manually:

```bash
pip install -r requirements.txt
```
