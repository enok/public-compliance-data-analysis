# Analysis Notebooks Guide

This guide summarizes the recommended notebook flow and expected datasets.

---

## Recommended Order

1. `01_exploratory_data_analysis.ipynb`
2. `02_statistical_analysis.ipynb`
3. `03_machine_learning.ipynb`
4. `04_clustering_analysis.ipynb`

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

## Common Issues

### Missing S3 dataset
Run:

```bash
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

### AWS auth problems
Verify active profile and credentials before opening notebooks.
