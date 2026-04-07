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

---

## Available Gold Dataset Keys

- `analysis_compliance`
- `municipality_socioeconomic`
- `state_summary`
- `sanctions_summary`
- `consolidated_clustering`

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
