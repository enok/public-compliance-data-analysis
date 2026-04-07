# Pipeline Change Workflow

Use this repo-local workflow together with the shared workflow in `.windsurf/workflows/data-pipeline-change.md`.

## Repository-Specific Steps

1. Read the relevant contract files first:
   - `config/ibge_metadata.json`
   - `config/transparency_metadata.json`
   - `config/silver_schemas.json`
2. Trace the impacted code path:
   - `src/ingestion/`
   - `src/processing/`
   - `src/analysis/`
   - `scripts/`
3. Verify the actual orchestration surface before assuming cloud-managed workflow changes:
   - shell entry points in `scripts/`
   - S3 path conventions
   - metadata and schema contracts
   - `dags/` only when the task includes real DAG code
4. Check whether the change requires:
   - new or updated tests
   - metadata or schema updates
   - documentation updates in `docs/`
   - reprocessing or backfill guidance
5. Document operational impact, especially when the change alters S3 keys, schemas, backfill expectations, notebook loader expectations, or CLI usage.
