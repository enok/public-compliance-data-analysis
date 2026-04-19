# Analysis Notebooks

This directory contains the bilingual analytical notebooks for the thesis.

Portuguese version: [README.pt-BR.md](README.pt-BR.md)

---

## Notebooks

1. `01_exploratory_data_analysis.ipynb`
- Gold dataset exploration, distributions, missingness, regional patterns.

2. `02_statistical_analysis.ipynb`
- Correlation tests, hypothesis tests, OLS diagnostics, interpretation.

3. `03_machine_learning.ipynb`
- Regression/classification baselines, cross-validation, feature importance.

4. `04_clustering_analysis.ipynb`
- PCA and K-means using the consolidated municipality clustering dataset.

### Notebook Organization Style

The final notebooks keep the aula-inspired organization in a single canonical file per topic.

Each notebook includes explicit sections for:
- `# Packages`
- `# Reproducibility` (`SEED = 42`)
- dataset loading, analysis blocks, and summary

---

## Prerequisites

1. Install dependencies:

```bash
pip install -r ../requirements.txt
```

2. Ensure Gold outputs exist:

```bash
./scripts/03_gold_transformation.sh
```

3. Configure AWS credentials/profile for S3 reads.

---

## Running

```bash
cd notebooks
jupyter notebook
# or
jupyter lab
```

---

## Data Loader

All notebooks can use `GoldDataLoader`:

```python
from src.analysis.data_loader import GoldDataLoader

loader = GoldDataLoader(
    bucket_name="enok-mba-thesis-datalake",
    aws_profile="mba-thesis",
)

df = loader.load_dataset("analysis_compliance")
datasets = loader.load_all()
```

Portuguese-translated loading:

```python
from src.analysis.pt_br_loader import GoldDataLoaderPtBr

loader_pt = GoldDataLoaderPtBr(
    bucket_name="enok-mba-thesis-datalake",
    aws_profile="mba-thesis",
    use_display_names=False,
)
```

City clustering and same-cluster city comparison:

```python
from src.analysis.city_clustering import (
    build_same_cluster_peer_table,
    cluster_cities,
    compare_cities_in_same_cluster,
)

df_cluster = loader.load_dataset("consolidated_clustering")
result = cluster_cities(df_cluster, n_clusters=None, min_k=2, max_k=10)
df_clustered = result.clustered_df

# Compare one target city against nearest peers in the same cluster
city_peers = compare_cities_in_same_cluster(
    df_clustered,
    municipality_code="3550308",  # Sao Paulo
    top_n=5,
)

# Build a full city-to-city peer table inside each cluster
all_peers = build_same_cluster_peer_table(df_clustered, top_n=3)
```

---

## Available Gold Dataset Keys

- `analysis_compliance`
- `analysis_compliance_municipality`
- `municipality_socioeconomic`
- `state_summary`
- `sanctions_summary`
- `consolidated_clustering`

---

## City Full Analysis Workflow

To run a state-style complete municipal analysis (stats + ML + clustering peers + conclusion addendum):

```bash
python ../scripts/run_city_full_analysis.py
```

Artifacts are written by default to `docs/city_full_analysis/`.

---

## Typical Troubleshooting

### `Dataset not found in S3`
Run Silver then Gold:

```bash
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

### `AWS credentials not found`
Configure your profile and export/use it in the environment.

### `Module not found`
Install dependencies from `../requirements.txt`.
