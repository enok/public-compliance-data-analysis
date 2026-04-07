# AWS Airflow Terraform Rule

Use this repo-local rule after the shared toolkit guidance in `.windsurf/rules/aws-airflow-terraform.md`.

This file should only capture how the shared Airflow-on-AWS Terraform guidance applies to this repository.

## Repository-Specific Deltas

- Treat Terraform in `infra/` as the source of truth for AWS infrastructure in this repo.
- Treat `scripts/01_bronze_ingestion.sh`, `scripts/02_silver_transformation.sh`, and `scripts/03_gold_transformation.sh` as the source of truth for implemented ETL entry points unless real DAG code is added.
- Do not assume Airflow orchestration already exists here just because the task mentions it. Confirm whether the change is:
  - infrastructure-only
  - DAG introduction
  - ETL task migration from shell orchestration into Airflow
  - documentation or operating-model guidance only
- Preserve Bronze, Silver, and Gold boundaries when translating script execution into Airflow tasks.
- Preserve current S3 naming and dataset contract assumptions unless the change is an intentional migration.
- If Airflow introduces a new orchestration surface, update docs to explain how it relates to existing shell scripts and whether those scripts remain canonical, wrapped, or deprecated.
- If ETL behavior changes, update:
  - `config/` contracts when needed
  - `tests/`
  - `docs/`
  - operating commands or deployment notes
