# Analysis Notebooks Guide

This guide summarizes the recommended notebook flow and expected datasets.

---

## Notebook Categories

### Category 1: End-to-End Pipeline (Complete Workflow)

Run these notebooks for a complete analysis from data ingestion to final results:

| Notebook | Language | Purpose |
|----------|----------|---------|
| `05_end_to_end_pipeline.ipynb` | English | Complete ETL → Analysis → Results |
| `05_end_to_end_pipeline.pt-BR.ipynb` | Portuguese | Complete ETL → Analysis → Results |

**Use Case**: Run these when you want to execute the entire pipeline in one notebook:
1. Data Ingestion (Bronze layer from APIs)
2. Data Processing (Silver layer normalization)
3. Feature Engineering (Gold layer)
4. Exploratory Data Analysis
5. Statistical Analysis (OLS regression)
6. Machine Learning (ElasticNet, Random Forest)
7. Clustering Analysis (K-means + PCA)
8. Results Export

**Storage Modes**: These notebooks support `local-only`, `s3-only`, or `both` storage modes.

### Category 2: Individual Analysis Notebooks (Modular)

Run these for focused analysis on specific topics:

| Order | Notebook | Purpose |
|-------|----------|---------|
| 1 | `01_exploratory_data_analysis.ipynb` | Data profiling, distributions, quality checks |
| 2 | `02_statistical_analysis.ipynb` | Correlation, OLS regression, hypothesis tests |
| 3 | `03_machine_learning.ipynb` | Supervised learning models and evaluation |
| 4 | `04_clustering_analysis.ipynb` | Unsupervised segmentation, PCA visualization |

**Use Case**: Use these when:
- You've already run the pipeline and have Gold data
- You want to explore specific analyses in depth
- You need to iterate on a specific methodology

---

## Recommended Order

### Option A: Quick End-to-End (Recommended for Results)

Run **only** the end-to-end pipeline notebook:

```bash
# For English
jupyter notebook notebooks/05_end_to_end_pipeline.ipynb

# For Portuguese
jupyter notebook notebooks/05_end_to_end_pipeline.pt-BR.ipynb
```

This single notebook will:
- Ingest all data from APIs (or use existing data)
- Process through Bronze → Silver → Gold layers
- Run all analyses (EDA, Statistics, ML, Clustering)
- Export results and figures

### Option B: Modular Analysis (For Development)

Run notebooks 01-04 in sequence after data is ready:

1. `01_exploratory_data_analysis.ipynb`
2. `02_statistical_analysis.ipynb`
3. `03_machine_learning.ipynb`
4. `04_clustering_analysis.ipynb`

The final notebooks keep the aula-style structure directly in each canonical file.

---

## Notebook Goals

### 1) EDA
- profile data quality
- inspect distributions and missingness
- compare regions/states

### 2) Statistical analysis
- correlation and hypothesis testing
- OLS modeling and diagnostics
- regression interpretation

### 3) Machine learning
- supervised baselines for regression/classification
- model comparison and feature relevance

### 4) Clustering
- PCA dimensionality reduction
- K-means segmentation on consolidated municipal features

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

### Missing S3 dataset
Run:

```bash
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

### AWS auth problems
Verify active profile and credentials before opening notebooks.
