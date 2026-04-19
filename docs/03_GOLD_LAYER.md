# STEP 3: GOLD LAYER - Analytics & Aggregations

**Last Updated:** 2026-04-19 (UTC-03:00)  
**Project:** Public Compliance Data Analysis (MBA Thesis)  
**Primary Entrypoint:** `scripts/03_gold_transformation.sh`

---

## Overview

The Gold layer transforms Silver tables into analysis-ready datasets for statistical modeling, machine learning, and clustering.

Current Gold outputs:
1. `gold/agg_municipality_socioeconomic`
2. `gold/agg_state_summary`
3. `gold/agg_sanctions_summary`
4. `gold/analysis_compliance`
5. `gold/analysis_compliance_municipality`
6. `gold/consolidated_clustering`

All outputs include:
- `data.parquet`
- `data.json`
- `_metadata.json`

---

## Source Tables (Silver)

- `silver/dim_municipalities/data.parquet`
- `silver/fact_population/data.parquet`
- `silver/fact_sanitation/data.parquet`
- `silver/fact_literacy/data.parquet`
- `silver/fact_income/data.parquet`
- `silver/fact_federal_transfers/data.parquet`
- `silver/fact_sanctions/data.parquet`

---

## Gold Datasets

### 1) `agg_municipality_socioeconomic`
Municipality-level 2010->2022 socioeconomic panel with change metrics.

Core fields include:
- geography keys (`municipality_code`, `state_code`, `region_code`)
- population levels and `%` change
- literacy levels and `pp` change
- nominal + real income levels and change
- household levels and `%` change

### 2) `agg_state_summary`
State-level aggregation across municipalities with sanctions density.

Core fields include:
- state/regional keys
- municipality counts
- population and income summaries
- sanctions totals by entity type (`PF`, `PJ`)
- `sanctions_per_100k`

### 3) `agg_sanctions_summary`
Registry-level sanctions aggregation (`CEIS`, `CNEP`, `CEPIM`).

Core fields include:
- total sanctions
- `PF`/`PJ` split
- `pj_ratio_pct`
- date range (`earliest_sanction`, `latest_sanction`)
- unique sanctioning agencies

### 4) `analysis_compliance`
State-level modeling dataset for regression/correlation work.

Core fields include:
- socioeconomic controls (`population`, `avg_literacy_rate`, `avg_income`)
- sanctions targets (`n_sanctions`, registry breakdowns, `sanctions_per_100k`)
- engineered features (`log_population`, `log_income`, regional dummies)

### 5) `analysis_compliance_municipality`
Municipality-level modeling dataset aligned with state-level `analysis_compliance`.

Core fields include:
- municipality/state/region keys
- socioeconomic controls (`population_2022`, `literacy_rate_2022`, `avg_income_2022`, `avg_income_real_2022_2022_brl`)
- sanctions targets (`n_sanctions`, registry breakdowns, `sanctions_per_100k`)
- transfer controls (`total_transfers`, `n_transfer_records`, `avg_transfer_per_capita`)
- engineered features (`log_population`, `log_income`, `log_total_transfers`, `sanctions_per_million_brl_transfers`)

### 6) `consolidated_clustering`
Municipality-level dataset for unsupervised analysis.

Construction rules:
- one row per municipality
- rows missing clustering features are dropped
- z-score normalized fields (`*_norm`) for clustering
- additional log features for skewed metrics:
  - `log_population_2022`
  - `log_avg_income_2022`
  - `log_households_2022`

---

## Implementation

Main code:
- `src/processing/gold_transformer.py`

Primary methods:
- `_transform_municipality_socioeconomic()`
- `_transform_state_summary()`
- `_transform_sanctions_summary()`
- `_transform_analysis_compliance()`
- `_transform_analysis_compliance_municipality()`
- `_transform_clustering_dataset()`

Schema contracts are validated against Gold schema entries in `config/silver_schemas.json`.

---

## Execution

### Full Gold run

```bash
./scripts/03_gold_transformation.sh
```

### Direct module execution

```bash
python -m src.processing.gold_transformer
```

### Prerequisite

```bash
./scripts/02_silver_transformation.sh
```

---

## Smart Caching

Gold reuses the same metadata-based skip strategy:
- digest tracking in `_metadata.json`
- source comparison using SHA-256 metadata with ETag fallback
- automatic reprocessing only when Silver sources change

---

## Data Quality and Edge Handling

- schema validation and type enforcement on each output
- change metrics avoid divide-by-zero
- missing source tables handled gracefully (with warnings)
- clustering output logs retained vs dropped municipality counts

---

## Known Constraints

- sanctions geolocation coverage is incomplete in source systems
- census-derived outcome features remain limited to 2010 and 2022 comparisons
- municipality-level sanctions remain limited by geolocation coverage in source sanctions records

---

## Testing

```bash
pytest tests/processing/test_gold_transformer.py -v
pytest tests/processing/ -v
```

Current repository total: `140` collected tests, `137 passed, 3 skipped` (`pytest tests/ -q`, 2026-04-19).

---

## Downstream Links

- Notebooks guide: [../notebooks/README.md](../notebooks/README.md)
- Translation guide: [TRANSLATIONS.md](TRANSLATIONS.md)
