# Pipeline Change Workflow

Use this repo-local workflow together with the shared workflow in `.windsurf/workflows/data-pipeline-change.md`.

This repository uses a medallion architecture (Bronze → Silver → Gold) on AWS S3. Changes must respect layer boundaries.

## Layer Boundaries

| Layer | Purpose | Storage | Change Frequency | Contract Files |
|-------|---------|---------|------------------|----------------|
| **Bronze** | Raw source fidelity | `s3://<bucket>/bronze/` | Rare (source changes) | `config/ibge_metadata.json`, `config/transparency_metadata.json` |
| **Silver** | Normalized, typed, joined | `s3://<bucket>/silver/` | Medium (schema evolution) | `config/silver_schemas.json` |
| **Gold** | Analysis-ready features | `s3://<bucket>/gold/` | High (new features) | Implicit via `src/processing/gold_transformer.py` |

**Golden Rule**: Never hide Silver or Gold logic inside Bronze ingestion. Each layer has a single responsibility.

## Repository-Specific Steps

### 1. Identify the Layer Being Changed

**If Bronze (ingestion):**
- Focus: API client reliability, data completeness, audit trail
- Risk: Downstream breakage if schema changes
- Tests: `tests/ingestion/`

**If Silver (normalization):**
- Focus: Type consistency, join key standardization, deduplication
- Risk: Municipality code mismatches, temporal alignment
- Tests: `tests/processing/`

**If Gold (aggregation):**
- Focus: Feature engineering, analysis grain, derived metrics
- Risk: Feature leakage, spurious correlations
- Tests: `tests/processing/`, validation in notebooks

### 2. Read the Relevant Contract Files

```bash
# For Bronze changes
config/ibge_metadata.json          # IBGE census indicators
config/transparency_metadata.json  # Federal transfers and sanctions
config/ipca_metadata.json          # Inflation series

# For Silver changes
config/silver_schemas.json         # Expected Silver columns and types

# For Gold changes
src/processing/gold_transformer.py # Current feature definitions
```

Verify:
- [ ] Contract exists and is up-to-date
- [ ] Change aligns with contract (or update contract first)
- [ ] No breaking changes without versioning plan

### 3. Trace the Impacted Code Path

**Bronze layer:**
```
config/ → src/ingestion/ → scripts/01_bronze_ingestion.sh → S3 bronze/
```

**Silver layer:**
```
S3 bronze/ → src/processing/silver_transformer.py → scripts/02_silver_transformation.sh → S3 silver/
```

**Gold layer:**
```
S3 silver/ → src/processing/gold_transformer.py → scripts/03_gold_transformation.sh → S3 gold/
```

**Downstream consumers:**
- `src/analysis/data_loader.py` — notebook loaders
- `notebooks/` — all analysis notebooks
- `tests/` — validation logic

### 4. Verify the Orchestration Surface

**Current state**: Shell scripts are the primary orchestration.

```bash
scripts/01_bronze_ingestion.sh      # Bronze ingestion
scripts/02_silver_transformation.sh # Silver normalization
scripts/03_gold_transformation.sh   # Gold feature engineering
```

Only assume `dags/` MWAA when:
- The task explicitly mentions DAG code
- Real Airflow DAG files exist in `dags/`
- The change is about migrating shell → Airflow

### 5. Assess Backfill Requirements

Will this change require reprocessing historical data?

| Change Type | Backfill Required | Scope |
|-------------|-------------------|-------|
| New Bronze source | Yes | Full historical |
| New Bronze field | Yes | Full historical |
| Silver type fix | Yes | Affected partitions |
| Silver join fix | Yes | All Silver |
| New Gold feature | No | Future only (unless needed) |
| Gold metric fix | Maybe | Relevant partitions |

Document the backfill plan in:
- Change commit message
- `docs/` if operational impact is significant
- Runbook if manual steps required

### 6. Update Contracts and Documentation

**Required updates:**
- [ ] `config/*.json` — if schema changes
- [ ] `docs/01_BRONZE_LAYER.md` — if ingestion changes
- [ ] `docs/02_SILVER_LAYER.md` — if normalization changes
- [ ] `docs/03_GOLD_LAYER.md` — if features change
- [ ] `README.md` — if CLI or operational commands change
- [ ] `README.pt-BR.md` — Portuguese counterpart

### 7. Test Strategy

**Run layer-appropriate tests:**

```bash
# Bronze ingestion tests
pytest tests/ingestion/ -v

# Silver transformation tests
pytest tests/processing/test_silver*.py -v

# Gold transformation tests
pytest tests/processing/test_gold*.py -v

# Full pipeline (expensive)
./scripts/01_bronze_ingestion.sh --dry-run
./scripts/02_silver_transformation.sh --validate-only
./scripts/03_gold_transformation.sh --validate-only
```

**Integration check:**
- [ ] Notebooks can still load data
- [ ] Municipality codes still join correctly
- [ ] Time periods align
- [ ] No data loss (row count checks)

### 8. Document Operational Impact

If the change alters:
- S3 keys or paths → Update data discovery documentation
- Schemas → Version the change, document migration
- CLI usage → Update README and help text
- Notebook loaders → Notify users of breaking changes
- Backfill expectations → Create runbook

## Anti-Patterns to Avoid

| Anti-Pattern | Why It's Bad | Correct Approach |
|--------------|--------------|------------------|
| Silver logic in Bronze | Breaks separation of concerns | Bronze = raw only |
| Gold logic in Silver | Complicates normalization | Silver = clean typed data |
| Changing Bronze schema lightly | Breaks downstream consumers | Version Bronze changes |
| Silent data dropping | Unnoticed data loss | Explicit validation + logging |
| Hardcoding paths | Brittle across environments | Use `config/runtime_config.json` |
| Skipping tests on "simple" changes | Regressions go unnoticed | Always run affected tests |

## Quick Reference: Change Types

| You Want To... | Likely Layer | Contract Update | Tests |
|----------------|--------------|-----------------|-------|
| Add new data source | Bronze | Yes | `tests/ingestion/` |
| Fix municipality code parsing | Silver | Maybe | `tests/processing/` |
| Add income percentile feature | Gold | No | `tests/processing/`, notebooks |
| Handle new API field | Bronze | Yes | `tests/ingestion/` |
| Change clustering algorithm | Gold | No | Validation notebooks |
| Adjust for inflation | Gold | No | Compare to official IPCA |
| Add sanctions flag | Silver/Gold | Maybe | Cross-check with CGU |

## Related Workflows

- `docs/llm/workflows/data-source-ingestion.md` — For adding/modifying data sources
- `.windsurf/workflows/data-pipeline-change.md` — Generic pipeline guidance
- `.windsurf/workflows/dataset-onboarding.md` — For brand new datasets

