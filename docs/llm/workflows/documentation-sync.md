# Documentation Sync Workflow

Use this repo-local workflow together with the shared workflow in `.windsurf/workflows/update-docs.md`.

## Repository-Specific Steps

1. Identify which areas changed:
   - ingestion
   - processing
   - analysis outputs
   - infrastructure
   - tests
   - operational commands
2. Update the closest English source document first:
   - `README.md`
   - `docs/01_BRONZE_LAYER.md`
   - `docs/02_SILVER_LAYER.md`
   - `docs/03_GOLD_LAYER.md`
   - related coverage or summary documents
3. If a maintained Portuguese counterpart exists, update it in the same change or record why it intentionally diverges.
4. Keep generated logs and evidence artifacts separate from narrative documentation.
5. Prefer concise, operationally useful notes over high-level marketing language.
