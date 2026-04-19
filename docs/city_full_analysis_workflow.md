# City Full Analysis Workflow (State-Style, Municipality-Level)

This workflow runs a full city-level analytical path aligned with the state-level thesis flow:
- statistical analysis (correlation + OLS)
- machine learning benchmark
- clustering outcome contrast
- same-cluster city peer comparisons
- thesis conclusion addendum generation

## Command

```bash
python scripts/run_city_full_analysis.py
```

Optional flags:

```bash
python scripts/run_city_full_analysis.py \
  --bucket-name enok-mba-thesis-datalake \
  --aws-profile mba-thesis \
  --output-dir docs/city_full_analysis \
  --min-k 2 \
  --max-k 10 \
  --peer-top-n 3
```

## Generated Artifacts

- `docs/city_full_analysis/city_correlations.csv`
- `docs/city_full_analysis/city_ols_coefficients.csv`
- `docs/city_full_analysis/city_ml_scores.csv`
- `docs/city_full_analysis/city_ml_feature_importance.csv`
- `docs/city_full_analysis/city_cluster_k_diagnostics.csv`
- `docs/city_full_analysis/city_cluster_outcome_summary.csv`
- `docs/city_full_analysis/city_same_cluster_peers.csv`
- `docs/city_full_analysis/city_thesis_conclusion_addendum.md`
- `docs/city_full_analysis/city_thesis_conclusion_addendum.pt-BR.md`

## Thesis Integration

After running the command, integrate the generated addendum into:
- `docs/thesis_conclusion.md`
- `docs/thesis_conclusion.pt-BR.md`

This closes the gap between state-only inference and municipality-level evidence.
