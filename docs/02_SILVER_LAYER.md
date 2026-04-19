# STEP 2: SILVER LAYER - Data Transformation

**Last Updated:** 2026-04-19 (UTC-03:00)  
**Project:** Public Compliance Data Analysis (MBA Thesis)  
**Primary Entrypoint:** `scripts/02_silver_transformation.sh`

---

## Overview

The Silver layer normalizes Bronze JSON into schema-validated analytical tables in Parquet/JSON.

Key objectives:
- standardize municipality/state/region keys
- enforce schema contracts from `config/silver_schemas.json`
- produce reusable facts and dimensions for Gold
- keep transformations idempotent with metadata-based skip logic

---

## Inputs and Outputs

### Bronze inputs

- `bronze/ibge/*.json`
- `bronze/economic/ipca_monthly.json`
- `bronze/transparency/federal_transfers_YYYY_MM.json`
- `bronze/transparency/{ceis,cnep,cepim}_compliance.json`

### Silver outputs

1. `silver/dim_municipalities`
2. `silver/dim_municipality_lookup`
3. `silver/fact_population`
4. `silver/fact_sanitation`
5. `silver/fact_literacy`
6. `silver/fact_income`
7. `silver/dim_inflation_index`
8. `silver/fact_federal_transfers`
9. `silver/fact_sanctions`

Each output path stores:
- `data.parquet`
- `data.json`
- `_metadata.json` (source digest tracking)

---

## Implementation

Core modules:
- `src/processing/base_transformer.py`
- `src/processing/ibge_transformer.py`
- `src/processing/transparency_transformer.py`

### `IBGETransformer`

Builds:
- municipality dimensions (`dim_municipalities`, `dim_municipality_lookup`)
- census facts (`fact_population`, `fact_sanitation`, `fact_literacy`, `fact_income`)
- annual inflation dimension (`dim_inflation_index`) from monthly IPCA

### `TransparencyTransformer`

Builds:
- `fact_federal_transfers` from dynamically discovered monthly files
- `fact_sanctions` from CEIS/CNEP/CEPIM
- municipality linking via normalized lookup table when source provides municipality name + UF
- CPF/CNPJ masking

---

## Smart Caching and Incremental Processing

Silver skip logic compares current source digests with `_metadata.json`.

Digest strategy (`BaseTransformer._get_object_digest`):
1. prefer S3 metadata `content-sha256` (set in Bronze ingestion)
2. fallback to `ETag` when custom metadata is absent

Processing is skipped only when:
- output exists
- no tracked source digest changed

This applies to Bronze->Silver and Silver->Gold dependency chains.

---

## Silver Schemas (Current)

### `dim_municipalities`
- `municipality_code`, `municipality_name`, `state_code`, `state_abbrev`, `state_name`, `region_code`, `region_name`

### `dim_municipality_lookup`
- `municipality_code`, `municipality_name_normalized`, `state_code`, `state_abbrev`

### `fact_population`
- `municipality_code`, `year`, `total_population`

### `fact_sanitation`
- `municipality_code`, `year`, `total_households`

### `fact_literacy`
- `municipality_code`, `year`, `literacy_rate`

### `fact_income`
- `municipality_code`, `year`, `avg_income`
- `annual_ipca_rate_pct`, `ipca_index_avg`, `deflator_to_2022`, `reference_base_year`
- `avg_income_real_2022_brl`

### `dim_inflation_index`
- `year`, `annual_ipca_rate_pct`, `ipca_index_avg`, `ipca_index_dec`
- `deflator_to_2022`, `reference_base_year`, `last_reference_date`

### `fact_federal_transfers`
- `municipality_code` (nullable), `year`, `month`, `transfer_amount`, `transfer_type`, `source_agency`

### `fact_sanctions`
- `sanction_id`, `registry_type`, `sanctioned_entity`, `entity_type`, `cpf_cnpj`
- `sanction_type`, `sanction_start_date`, `sanction_end_date`
- `sanctioning_agency`, `state_code`, `municipality_code`

---

## Execution

### Full Silver run

```bash
./scripts/02_silver_transformation.sh
```

### Scoped execution

```bash
./scripts/02_silver_transformation.sh --only-ibge
./scripts/02_silver_transformation.sh --only-transparency
```

### Direct module execution

```bash
python -m src.processing.ibge_transformer
python -m src.processing.transparency_transformer
```

---

## Data Quality and Privacy

Validation controls include:
- 7-digit municipality code validation
- schema enforcement by table
- duplicate removal on natural keys
- safe numeric/date parsing

Privacy controls:
- masked CPF/CNPJ in sanctions output
- no plain document values in Silver sanctioned entity fields

Operational log:
- transformation audit in `docs/processing.log`

---

## Known Constraints

- municipality linkage for federal transfers is not guaranteed for every raw record
- sanctions location fields remain sparse in source data
- census outcome indicators are only available for 2010 and 2022

---

## Testing

```bash
pytest tests/processing/ -v
pytest tests/processing/test_transformers.py -v
pytest tests/processing/test_smart_caching.py -v
pytest tests/processing/test_silver_integration.py -v
```

Current repository total: `140` collected tests, `137 passed, 3 skipped` (`pytest tests/ -q`, 2026-04-19).

---

## Downstream Links

- Gold layer: [03_GOLD_LAYER.md](03_GOLD_LAYER.md)
- Notebook guide: [../notebooks/README.md](../notebooks/README.md)
