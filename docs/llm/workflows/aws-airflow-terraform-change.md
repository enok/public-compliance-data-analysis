# AWS Airflow Terraform Change Workflow

Use this repo-local workflow together with the shared workflow in `.windsurf/workflows/aws-airflow-terraform-change.md`.

## Repository-Specific Steps

1. Read the current repository boundaries before designing infrastructure:
   - `infra/`
   - `scripts/01_bronze_ingestion.sh`
   - `scripts/02_silver_transformation.sh`
   - `scripts/03_gold_transformation.sh`
   - `docs/llm/rules/data-pipeline-boundaries.md`
   - `docs/llm/rules/aws-airflow-terraform.md`
2. Decide whether the first migration step should wrap the existing shell scripts rather than rewrite ETL logic.
3. Keep Bronze, Silver, and Gold responsibilities traceable in DAG names, task names, and docs.
4. If the Airflow surface remains small, extending the current `infra/` root may be acceptable. If it becomes non-trivial, split into modules and explicit environment inputs.
5. Update repo artifacts that this migration affects:
   - `config/` contracts when behavior changes
   - `tests/`
   - `docs/`
   - `.gitignore` if local Terraform artifacts need coverage
6. Document whether shell scripts remain canonical, become task wrappers, or are deprecated.
