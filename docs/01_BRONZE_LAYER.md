# STEP 1: BRONZE LAYER - Data Ingestion

**Last Updated:** 2026-04-07 (UTC-03:00)  
**Project:** Public Compliance Data Analysis (MBA Thesis)  
**Primary Entrypoint:** `scripts/01_bronze_ingestion.sh`

---

## Overview

The Bronze layer ingests raw public data into S3 with source-level auditability and checkpointed incremental behavior.

Current Bronze scope:
- Bronze I: IBGE census datasets (2010 and 2022)
- Bronze II: Transparency Portal federal transfers (monthly, `2010-01` to `2022-12`) + sanctions (CEIS, CNEP, CEPIM)
- Bronze III: Banco Central IPCA monthly series (`SGS 433`)

---

## Data Sources

### Bronze I - IBGE Census

Implementation:
- `src/ingestion/ibge_client.py`
- `config/ibge_metadata.json`

Datasets:
1. `pop_2010`
2. `pop_2022`
3. `sanitation_2010`
4. `sanitation_2022`
5. `literacy_2010`
6. `literacy_2022`
7. `income_2010`
8. `income_2022`

Behavior highlights:
- S3 fast-skip when file already exists (`IBGE_FAST_SKIP_IF_EXISTS=1` by default)
- SHA-256 object metadata (`content-sha256`) for integrity comparison
- API source logging to `docs/data_sources.log`

### Bronze II - Transparency Portal

Implementation:
- `src/ingestion/transparency_client.py`
- `config/transparency_metadata.json`

Datasets:
- `federal_transfers` (auto-expanded from one config entry into monthly files)
- `ceis_sanctions`
- `cnep_sanctions`
- `cepim_sanctions`

Coverage target:
- Federal transfers monthly files from `federal_transfers_2010_01.json` through `federal_transfers_2022_12.json`
- 156 months total

Behavior highlights:
- Pagination checkpointing with resume support
- `no_data` checkpoint state to avoid repeated empty calls
- incremental fetches when new pages appear
- configurable month overrides:
  - `TRANSPARENCY_START_MA=MM/YYYY`
  - `TRANSPARENCY_END_MA=MM/YYYY`
  - `TRANSPARENCY_FORCE_REFRESH=1` to recheck checkpointed months

### Bronze III - Inflation (BCB IPCA)

Implementation:
- `src/ingestion/ibge_client.py`
- `config/ibge_metadata.json` (`ipca_monthly`)

Dataset:
- `ipca_monthly` from Banco Central `SGS 433`

Execution mode:
- included in full Bronze run
- can run isolated with `./scripts/01_bronze_ingestion.sh --only-inflation`

---

## S3 Layout

```text
s3://enok-mba-thesis-datalake/bronze/
├── ibge/
│   ├── census_2010_pop.json
│   ├── census_2022_pop.json
│   ├── census_2010_sanitation.json
│   ├── census_2022_sanitation.json
│   ├── census_2010_literacy.json
│   ├── census_2022_literacy.json
│   ├── census_2010_income.json
│   └── census_2022_income.json
├── economic/
│   └── ipca_monthly.json
└── transparency/
    ├── federal_transfers_YYYY_MM.json
    ├── ceis_compliance.json
    ├── cnep_compliance.json
    ├── cepim_compliance.json
    └── .metadata/
        └── *.meta.json
```

---

## Execution

### Full Bronze run

```bash
./scripts/01_bronze_ingestion.sh
```

### Selective execution

```bash
./scripts/01_bronze_ingestion.sh --only-ibge
./scripts/01_bronze_ingestion.sh --only-inflation
./scripts/01_bronze_ingestion.sh --only-transparency
```

### Skip flags

```bash
./scripts/01_bronze_ingestion.sh --skip-ibge --skip-inflation
```

### Required runtime configuration

- AWS credentials/profile with S3 access
- `S3_BUCKET_NAME` (defaults to runtime config / `enok-mba-thesis-datalake`)
- `TRANSPARENCY_API_KEY` for Transparency ingestion

---

## Integrity, Idempotency, and Audit

- Idempotent re-runs: unchanged objects are skipped
- Bronze objects carry SHA-256 metadata for deterministic digest checks
- Transparency ingestion stores checkpoint metadata (`last_page`, `status`, record counters)
- API source and status logs are appended to `docs/data_sources.log`

---

## Troubleshooting

### `TRANSPARENCY_API_KEY` missing
Set it in `.env` or environment before running Transparency ingestion.

### 401/403 errors
Verify key validity and request header (`chave-api-dados`).

### Empty or partial Transparency coverage
Use targeted recheck with forced refresh:

```bash
TRANSPARENCY_FORCE_REFRESH=1 \
TRANSPARENCY_START_MA=01/2010 \
TRANSPARENCY_END_MA=12/2022 \
./scripts/01_bronze_ingestion.sh --only-transparency
```

### S3 access failures
Verify bucket, IAM permissions, and active AWS profile.

---

## Validation

```bash
pytest tests/ingestion/ -v
pytest tests/processing/test_silver_config_validation.py -v
```

---

## Downstream Links

- Silver layer: [02_SILVER_LAYER.md](02_SILVER_LAYER.md)
- Gold layer: [03_GOLD_LAYER.md](03_GOLD_LAYER.md)
- Notebook usage: [../notebooks/README.md](../notebooks/README.md)
