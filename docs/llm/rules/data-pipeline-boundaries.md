# Data Pipeline Boundaries

Use this repo-local rule after the shared toolkit guidance in `.windsurf/rules/data-pipeline-contracts.md`.

This file should only capture the contract sources and boundary decisions that are specific to this repository.

## Source Contracts

- `config/ibge_metadata.json` is the authoritative source for IBGE dataset definitions.
- `config/transparency_metadata.json` is the authoritative source for Transparency Portal dataset definitions.
- `config/silver_schemas.json` is the authoritative schema contract for Silver outputs.
- `scripts/01_bronze_ingestion.sh`, `scripts/02_silver_transformation.sh`, and `scripts/03_gold_transformation.sh` are the implemented orchestration entry points in this repo.

If code behavior changes, update the relevant metadata or schema file in the same change whenever the contract changed.

## Storage Conventions

- Preserve stable S3 key naming unless the change is a deliberate migration.
- Keep municipality identifiers and year semantics explicit in column names and output tables.
- Prefer narrow, explicit transformations over hidden side effects in utility helpers.
- Treat `src/analysis/` loaders as downstream Gold consumers, not as places to hide pipeline-side transformations.
- Do not assume `dags/` is the active orchestration source unless the task includes real DAG code changes.

## Repository-Specific Review Surfaces

For any pipeline change here, inspect the impact on:

- metadata files in `config/`
- source client code in `src/ingestion/`
- transformer code in `src/processing/`
- CLI or shell entry points in `scripts/`
- downstream analysis loaders in `src/analysis/` when Gold outputs or dataset names change
- tests under `tests/`
- technical docs under `docs/`
