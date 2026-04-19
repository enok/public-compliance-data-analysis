## Project Overview

This repository contains the implementation for an MBA thesis in Data Science & Analytics (USP/Esalq) focused on public compliance and spending-efficiency analysis in Brazil.

The project integrates:
- IBGE census indicators (2010 and 2022)
- Transparency Portal federal transfers (monthly, 2010-01 to 2022-12)
- CGU sanctions registries (CEIS, CNEP, CEPIM)
- Banco Central IPCA series for real-income adjustment to 2022 BRL

Portuguese version: [README.pt-BR.md](README.pt-BR.md)

## Current Status

- Bronze I/II/III ingestion: complete and operational (`scripts/01_bronze_ingestion.sh`)
  - Bronze I: IBGE census datasets
  - Bronze II: Transparency federal transfers + sanctions
  - Bronze III: BCB IPCA monthly inflation
- Silver transformation: complete and operational (`scripts/02_silver_transformation.sh`)
- Gold transformation: complete and operational (`scripts/03_gold_transformation.sh`)
- Analysis notebooks (EN + pt-BR): available
- Automated tests: `130` tests collected (`pytest --collect-only -q tests`, 2026-04-09)

## Research Frame

Objective: identify socioeconomic and compliance anomalies associated with public transfers.

Temporal design:

```text
Census 2010 (baseline) -> Federal transfers 2010-2022 (treatment window) -> Census 2022 (outcome)
```

## Quick Start

```bash
git clone <repository-url>
cd public-compliance-data-analysis

python -m venv .venv
source .venv/bin/activate            # Linux/Mac
# .\\.venv\\Scripts\\Activate.ps1      # Windows PowerShell

pip install -r requirements.txt
cp .env.example .env
```

Set required environment variables (or use `config/runtime_config.json` defaults):

```bash
AWS_PROFILE=mba-thesis
S3_BUCKET_NAME=enok-mba-thesis-datalake
TRANSPARENCY_API_KEY=<your-api-key>
```

## Running the Pipeline

### Stage entry points (source of truth)

```bash
./scripts/01_bronze_ingestion.sh
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

### Useful Bronze options

```bash
./scripts/01_bronze_ingestion.sh --only-ibge
./scripts/01_bronze_ingestion.sh --only-inflation
./scripts/01_bronze_ingestion.sh --only-transparency
```

Optional targeted Transparency recheck:

```bash
TRANSPARENCY_FORCE_REFRESH=1 \
TRANSPARENCY_START_MA=01/2010 \
TRANSPARENCY_END_MA=12/2022 \
./scripts/01_bronze_ingestion.sh --only-transparency
```

## Repository Layout

```text
config/      Ingestion metadata, schemas, runtime config contracts
src/         Reusable ingestion, processing, analysis, and config code
scripts/     Primary orchestration entry points (Bronze/Silver/Gold)
notebooks/   Bilingual analysis notebooks and guides
tests/       Pytest suites for ingestion, processing, and analysis
docs/        Layer docs, coverage reports, translation guide
infra/       Terraform for S3-backed data lake foundation
```

## Data Architecture

### Bronze (raw)
- `bronze/ibge/*`
- `bronze/economic/ipca_monthly.json`
- `bronze/transparency/federal_transfers_YYYY_MM.json`
- `bronze/transparency/{ceis,cnep,cepim}_compliance.json`
- metadata checkpoints under `bronze/transparency/.metadata/`

### Silver (normalized)
- `dim_municipalities`
- `dim_municipality_lookup`
- `fact_population`
- `fact_sanitation`
- `fact_literacy`
- `fact_income`
- `dim_inflation_index`
- `fact_federal_transfers`
- `fact_sanctions`

### Gold (analysis-ready)
- `agg_municipality_socioeconomic`
- `agg_state_summary`
- `agg_sanctions_summary`
- `analysis_compliance`
- `analysis_compliance_municipality`
- `consolidated_clustering`

## Documentation Map

- Bronze: [docs/01_BRONZE_LAYER.md](docs/01_BRONZE_LAYER.md) | [pt-BR](docs/01_BRONZE_LAYER.pt-BR.md)
- Silver: [docs/02_SILVER_LAYER.md](docs/02_SILVER_LAYER.md) | [pt-BR](docs/02_SILVER_LAYER.pt-BR.md)
- Gold: [docs/03_GOLD_LAYER.md](docs/03_GOLD_LAYER.md) | [pt-BR](docs/03_GOLD_LAYER.pt-BR.md)
- City full analysis workflow: [docs/city_full_analysis_workflow.md](docs/city_full_analysis_workflow.md) | [pt-BR](docs/city_full_analysis_workflow.pt-BR.md)
- City thesis addendum: [docs/city_thesis_conclusion_addendum.md](docs/city_thesis_conclusion_addendum.md) | [pt-BR](docs/city_thesis_conclusion_addendum.pt-BR.md)
- Thesis presentation assets (QGIS + Power BI): [docs/thesis_presentation_assets.md](docs/thesis_presentation_assets.md) | [pt-BR](docs/thesis_presentation_assets.pt-BR.md)
- Notebooks: [notebooks/README.md](notebooks/README.md) | [pt-BR](notebooks/README.pt-BR.md)
- Tests: [tests/README.md](tests/README.md) | [pt-BR](tests/README.pt-BR.md)
- Translations: [docs/TRANSLATIONS.md](docs/TRANSLATIONS.md) | [pt-BR](docs/TRANSLATIONS.pt-BR.md)

## Testing

```bash
pytest tests/ -v
```

Targeted suites:

```bash
pytest tests/ingestion/ -v
pytest tests/processing/ -v
pytest tests/analysis/ -v
```

## Security

Run the mandatory repository security check before closing work:

```bash
./scripts/security_check_this_repo.sh
```

## References

- Full bibliography (EN): [BIBLIOGRAPHY.md](BIBLIOGRAPHY.md)
- Full bibliography (pt-BR): [BIBLIOGRAPHY.pt-BR.md](BIBLIOGRAPHY.pt-BR.md)
