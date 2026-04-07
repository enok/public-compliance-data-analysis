# AWS Airflow Terraform Reference

Use this reference for the repository-specific starting point and migration posture.

Generic Airflow-on-AWS Terraform guidance belongs in the shared toolkit.

## Current Repository Starting Point

- `infra/main.tf` currently provisions only the S3-backed data lake foundation.
- `scripts/01_bronze_ingestion.sh`, `scripts/02_silver_transformation.sh`, and `scripts/03_gold_transformation.sh` are the implemented orchestration entry points.
- `dags/` is not yet the primary orchestration surface.

## Recommended Repository Posture

- Start by wrapping stable existing shell ETL scripts in Airflow rather than rewriting ETL logic immediately.
- Preserve existing S3-backed Bronze, Silver, and Gold paths unless a migration is intentional.
- Treat this repository as moving from script-driven orchestration toward Airflow-managed scheduling in stages, not in one rewrite.

## Useful Prompts This Reference Supports

- "Set up MWAA for this repo without rewriting the ETL logic."
- "Create an Airflow migration plan that wraps the current Bronze, Silver, and Gold shell scripts."
- "Refactor `infra/` for Airflow while keeping the current repo boundaries intact."
