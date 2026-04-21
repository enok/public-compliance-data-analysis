# Thesis ↔ Notebook synchronization policy

> **Source of truth for every statistic, table and figure in the thesis DOCX
> is the Jupyter notebook that computes it. The thesis is a downstream
> consumer — never an independent source.**

Examiners (banca) may open any notebook, re-run it end-to-end, and verify
that the value printed by the notebook matches the value printed on the
corresponding thesis page. If they do not match, the thesis is wrong — not
the notebook.

This policy makes that invariant enforceable.

---

## 1. Ownership map

| Artifact in the thesis | Authoritative notebook | What the notebook exports |
|---|---|---|
| `TABELA_1_ROWS` (data sources) | `notebooks/00_etl_pipeline.ipynb` | Record counts from each Bronze ingestion |
| `TABELA_2_ROWS` (Pearson, municipal) | `notebooks/01_exploratory_data_analysis.ipynb` | `r`, `p-value`, significance |
| `TABELA_3_ROWS` (OLS coefficients) | `notebooks/02_statistical_analysis.ipynb` | coef, std-err (HC3), p-value |
| `TABELA_4_ROWS` (ML model metrics) | `notebooks/03_machine_learning.ipynb` | ROC-AUC, F1, test R² |
| `TABELA_5_ROWS` (cluster profiles) | `notebooks/04_clustering.ipynb` | Centroid means per cluster |
| `EDA_MAP_IMAGE_PATH`, `brazil_map_*.png` | `notebooks/01_exploratory_data_analysis.ipynb` | Choropleth → `generated/` |
| `EDA_STATE_RANKING_IMAGE_PATH`, `state_ranking_*.png` | `notebooks/01_exploratory_data_analysis.ipynb` | Bar chart → `generated/` |
| `EDA_REGIONAL_IMAGE_PATH`, `regional_summary_*.png` | `notebooks/01_exploratory_data_analysis.ipynb` | Small multiples → `generated/` |
| `OLS_DIAGNOSTICS_IMAGE_PATH`, `ols_diagnostics_*.png` | `notebooks/02_statistical_analysis.ipynb` | Residual plots → `generated/` |
| `ML_PERFORMANCE_IMAGE_PATH`, `ml_performance_*.png` | `notebooks/03_machine_learning.ipynb` | ROC + confusion → `generated/` |
| `CLUSTER_ELBOW_IMAGE_PATH`, `elbow_silhouette_*.png` | `notebooks/04_clustering.ipynb` | K-selection → `generated/` |
| `CLUSTER_PROFILE_IMAGE_PATH`, `cluster_profile_*.png` | `notebooks/04_clustering.ipynb` | Cluster radar/bar → `generated/` |
| `CLUSTER_CORR_IMAGE_PATH`, `intra_cluster_corr_*.png` | `notebooks/04_clustering.ipynb` | Intra-cluster corr → `generated/` |
| `correlations_state.png` (Appendix Figure C) | `scripts/regenerate_state_correlations.py` (replays notebook logic on Gold) | 5×5 heatmap from `analysis_compliance` |
| Prose claims: `r = 0.74` (state), `R² = 0.024`, `F = 18.5`, `ROC-AUC = 0.83`, `silhouette = 0.288`, `K = 4`, `n = 5,570`, etc. | The notebook that computes each | Printed cell output |

Hand-crafted assets that do **not** flow from notebooks:

| Artifact | Source |
|---|---|
| `MEDALLION_IMAGE_PATH` (Figure 1) | `_build/medallion-architecture.png` / `.pt-BR.png` — design asset, not computed |
| `EQUATION_1_IMAGE_PATH` | `generated/equation_*.png` — LaTeX render, regenerated manually |

---

## 2. Directional flow

```
Gold parquet (data/gold/*)
        │
        ▼
Jupyter notebook runs analysis, prints values, saves PNGs into
docs/thesis_presentation_assets/generated/
        │
        │  (manual transcription — values from printed cell output)
        ▼
tcc/final/_build/content_*.py  (TABELA_*_ROWS, prose numbers, IMAGE_PATHs)
        │
        ▼
build_tcc.py generates the DOCX
```

**Prohibited:** editing a number in `content_*.py` that was *not* first
verified against the notebook cell output. Even if you believe it is
"obviously right" — it is not the source of truth.

**Prohibited:** editing a PNG in `generated/` manually (e.g., in an image
editor). Always re-run the notebook cell that produces it.

---

## 3. Enforcement: `scripts/verify_thesis_notebook_sync.py`

Run before every commit that touches either the notebooks or
`content_*.py`:

```powershell
.venv\Scripts\python.exe scripts\verify_thesis_notebook_sync.py
```

What it checks:

1. **Figure provenance** — every `*_IMAGE_PATH` referenced in
   `content_pt[2-3].py` / `content_en[2-3].py` exists on disk and lives
   under `generated/` (or `_build/` for design assets), and is not older
   than the Gold parquet files.

2. **Pearson (municipal)** — recomputes the four correlations in
   `TABELA_2_ROWS` directly from
   `data/gold/analysis_compliance_municipality/data.parquet` and flags any
   drift larger than ±0.01.

3. **Pearson (state)** — verifies the `r = 0.74` claim for state-level
   mean income vs sanctions per 100k.

4. **OLS (municipal)** — re-fits the HC3-robust OLS specification in
   statsmodels and flags drift on R², adjusted R², F-statistic and every
   coefficient in `TABELA_3_ROWS` (tol 0.05 on coefs, 0.005 on R²).

Exit code `0` if everything matches, `1` if any drift exists.

The ML and k-means metrics are **not** automatically re-checked because
those models involve stochasticity (train/test split seed, initialisation).
For those, the notebook's printed output is the authoritative value and
the operator must manually confirm `TABELA_4_ROWS` and the K-means prose
against cell output after every notebook re-run.

---

## 4. Recovery: when the audit fails

Run the audit. For every `FAIL` entry:

1. Open the owning notebook from the table above.
2. Execute the cell that produces the metric. Read the printed value.
3. Compare with the value in `content_*.py`:

   - If the notebook value changed → **update `content_*.py`** in both
     PT and EN to match the notebook. Rebuild the DOCX.
   - If the thesis value was obviously correct and the notebook has
     regressed → investigate why. Do not edit `content_*.py` to paper
     over a broken notebook.

4. Re-run the audit. Confirm zero FAILs.
5. Commit the notebook changes, the `content_*.py` edits, and the
   regenerated DOCX together in a single commit so the three stay
   atomically synchronized.

---

## 5. Commit checklist (paste into PR description)

- [ ] Notebook re-runs cleanly end-to-end from a fresh kernel.
- [ ] All figures referenced by the thesis exist under `generated/` and
      were regenerated by the notebook in this session.
- [ ] `python scripts/verify_thesis_notebook_sync.py` exits with code 0.
- [ ] DOCX rebuild succeeds for both PT and EN.
- [ ] Spot-checked at least 2 numbers in the thesis against the notebook
      cell output (ideally one from each of: Pearson, OLS, ML, clustering).
- [ ] PT and EN theses contain the **identical** numeric values
      (difference in formatting only: `0,149` vs `0.149`).
