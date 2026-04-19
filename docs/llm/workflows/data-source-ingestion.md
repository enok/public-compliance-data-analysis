# Data Source Ingestion Workflow

Use this workflow when adding, modifying, or troubleshooting data ingestion from the project's core sources: IBGE, Transparency Portal, CGU sanctions, or BCB IPCA.

## Storage Backend Configuration

This ingestion pipeline supports **three storage modes**:

| Mode | S3 | Local Filesystem | Use Case |
|------|-----|------------------|----------|
| `s3-only` | ✅ | ❌ | Production, cloud-native, team sharing |
| `local-only` | ❌ | ✅ | Development, offline work, no AWS access |
| `both` | ✅ | ✅ | Backup strategy, hybrid workflows, migration |

### Configuration

Set storage mode via environment variable or config file:

**Option 1: Environment Variable (Recommended)**
```bash
# S3 only (default production)
export STORAGE_MODE=s3-only
export S3_BUCKET=enok-mba-thesis-datalake
export S3_PREFIX=bronze/

# Local only (development/offline)
export STORAGE_MODE=local-only
export LOCAL_DATA_DIR=/home/user/tcc-data

# Both (backup/redundancy)
export STORAGE_MODE=both
export S3_BUCKET=enok-mba-thesis-datalake
export LOCAL_DATA_DIR=/home/user/tcc-data-backup
```

**Option 2: Config File**
```json
// config/storage_config.json
{
  "storage_mode": "local-only",
  "s3": {
    "bucket": "enok-mba-thesis-datalake",
    "prefix": "bronze/",
    "region": "us-east-1"
  },
  "local": {
    "base_dir": "/home/user/tcc-data",
    "create_dirs": true
  },
  "sync": {
    "on_write": false,
    "on_read": false
  }
}
```

### Quick Start by Mode

#### Local-Only Mode
```bash
# Configure
export STORAGE_MODE=local-only
export LOCAL_DATA_DIR=/mnt/hgfs/shared/data-science/tcc/data

# Run ingestion (no AWS credentials needed)
./scripts/01_bronze_ingestion.sh --local-only

# Data lands in: $LOCAL_DATA_DIR/bronze/{source}/
```

#### S3-Only Mode (Original)
```bash
# Configure
export STORAGE_MODE=s3-only
export AWS_PROFILE=your-profile
export S3_BUCKET=enok-mba-thesis-datalake

# Run ingestion
./scripts/01_bronze_ingestion.sh

# Data lands in: s3://$S3_BUCKET/bronze/{source}/
```

#### Both Mode (Hybrid)
```bash
# Configure
export STORAGE_MODE=both
export LOCAL_DATA_DIR=/home/user/tcc-data
export S3_BUCKET=enok-mba-thesis-datalake

# Run ingestion (writes to both)
./scripts/01_bronze_ingestion.sh

# Data lands in both locations simultaneously
```

## Data Sources Overview

| Source | Type | Update Frequency | Contract File | Client Code |
|--------|------|------------------|---------------|-------------|
| IBGE Census | Static (2010, 2022) | Decadal | `config/ibge_metadata.json` | `src/ingestion/ibge_client.py` |
| Transparency Portal | Monthly (2010-2022) | Historical complete | `config/transparency_metadata.json` | `src/ingestion/transparency_client.py` |
| CGU Sanctions (CEIS, CNEP, CEPIM) | As updated | Periodic | `config/transparency_metadata.json` | `src/ingestion/transparency_client.py` |
| BCB IPCA | Monthly | Monthly | `config/ipca_metadata.json` | `src/ingestion/bcb_client.py` |

## When to Use

- Adding a new data source
- Modifying ingestion logic
- Handling API changes or rate limits
- Troubleshooting failed ingestion
- Adding new variables from existing sources
- Backfilling historical data

## Repository-Specific Steps

### 1. Identify the Data Source and Layer

What are you ingesting and at what medallion layer?

**Bronze Layer (Raw)**
- Preserve source fidelity
- Minimal transformation
- Audit trail of ingestion

**Silver Layer (Normalized)**
- Schema alignment
- Type normalization
- Join key standardization

**Gold Layer (Aggregated)**
- Analysis-ready features
- Municipality-year grain
- Derived metrics

### 2. Check the Contract

Before modifying ingestion:

```bash
# Read the relevant metadata
config/ibge_metadata.json          # IBGE census indicators
config/transparency_metadata.json  # Federal transfers and sanctions
config/ipca_metadata.json          # Inflation series
config/silver_schemas.json         # Silver layer expectations
```

Verify:
- Expected columns match source documentation
- Data types are appropriate
- Temporal coverage is documented
- Primary keys are identified

### 3. Trace the Ingestion Path

**For IBGE:**
1. `src/ingestion/ibge_client.py` — HTTP client
2. `scripts/01_bronze_ingestion.sh` — Orchestration
3. `src/processing/silver_transformer.py` — Normalization

**For Transparency Portal:**
1. `src/ingestion/transparency_client.py` — API client with pagination
2. `scripts/01_bronze_ingestion.sh` — Orchestration
3. `src/processing/silver_transformer.py` — Normalization

**For BCB IPCA:**
1. `src/ingestion/bcb_client.py` — BCB API client
2. `scripts/01_bronze_ingestion.sh` — Orchestration
3. `src/processing/gold_transformer.py` — CPI adjustment

### 4. Implement Changes

**If adding a new variable from existing source:**
1. Update the contract (`config/*.json`)
2. Modify the client to fetch new field
3. Update Silver transformation if normalization needed
4. Update Gold transformation if derived metric needed
5. Update loaders in `src/analysis/`

**If adding a new data source:**
1. Create new client in `src/ingestion/`
2. Add contract in `config/`
3. Add orchestration step in `scripts/01_bronze_ingestion.sh`
4. Create Silver normalization in `src/processing/`
5. Add tests in `tests/`

### 5. Test Ingestion

**Test by Storage Mode:**

```bash
# Local-only mode (no AWS needed)
export STORAGE_MODE=local-only
export LOCAL_DATA_DIR=/tmp/test-data
./scripts/01_bronze_ingestion.sh --source <source_name>

# S3-only mode (requires AWS credentials)
export STORAGE_MODE=s3-only
export S3_BUCKET=test-bucket
./scripts/01_bronze_ingestion.sh --source <source_name>

# Both modes (hybrid)
export STORAGE_MODE=both
export LOCAL_DATA_DIR=/tmp/test-data
export S3_BUCKET=test-bucket
./scripts/01_bronze_ingestion.sh --source <source_name>

# Or run unit tests
pytest tests/ingestion/test_<source>_client.py -v
pytest tests/ingestion/test_storage_backends.py -v  # New: test local/S3/both
```

**Check by Mode:**

| Check | Local | S3 | Both |
|-------|-------|-----|------|
| Data lands in correct path | `ls $LOCAL_DATA_DIR/bronze/` | `aws s3 ls s3://$BUCKET/bronze/` | Both locations |
| Schema matches contract | `head -5 local/file.csv` | `aws s3 cp s3://.../file.csv - \| head -5` | Compare both |
| No data loss | `wc -l local/file.csv` | `aws s3 cp ... \| wc -l` | Compare counts |
| Metadata checkpoint | `cat local/.metadata.json` | `aws s3 cp s3://.../.metadata.json -` | Both updated |
| Error handling | Test without write permissions | Test with invalid bucket | Verify both fail gracefully |

**Quick Verification:**
```bash
# For local-only
find $LOCAL_DATA_DIR/bronze -name "*.csv" -o -name "*.parquet" | head -10

# For S3
aws s3 ls s3://$S3_BUCKET/bronze/ --recursive | head -10

# For both - verify sync
diff <(find $LOCAL_DATA_DIR/bronze -type f | sort) \
     <(aws s3 ls s3://$S3_BUCKET/bronze/ --recursive | awk '{print $4}' | sort)
```

### 6. Validate Downstream Impact

Changes to ingestion may affect:
- Silver transformations (`src/processing/silver_transformer.py`)
- Gold features (`src/processing/gold_transformer.py`)
- Analysis notebooks (loaders may need updates)
- Tests (may need new fixtures)

Run Silver transformation:
```bash
./scripts/02_silver_transformation.sh
```

Verify no regressions:
```bash
pytest tests/processing/ -v
```

### 7. Document Changes

Update:
- `docs/01_BRONZE_LAYER.md` — If ingestion changes
- `docs/02_SILVER_LAYER.md` — If normalization changes
- `docs/03_GOLD_LAYER.md` — If derived features change
- `README.md` — If new data sources added
- `README.pt-BR.md` — Portuguese counterpart

## Source-Specific Guidance

### IBGE Census

- Municipality codes (`codigo_municipio`) are the join key
- 2010 and 2022 have different variable availability
- Handle missing values explicitly (not all municipalities have all indicators)
- Real income requires IPCA adjustment (handled in Gold layer)

### Transparency Portal

- API has rate limits; client implements backoff
- Historical data (2010-2022) is complete; no new fetches needed for thesis
- Municipality codes may need left-padding to 7 digits
- Action codes indicate transfer type (documented in metadata)

### CGU Sanctions

- CEIS: Ineligible companies
- CNEP: National registry of punished companies
- CEPIM: Ineligible municipalities
- Dates matter: sanctions have start and end dates
- Boolean flags should be time-aware (active at reference date)

### BCB IPCA

- Monthly series for inflation adjustment
- Base date matters (thesis uses 2022 BRL as reference)
- Used for real income calculations in Gold layer
- Validate against official BCB published values

## Error Handling Checklist

- [ ] API failures: retry with exponential backoff
- [ ] Partial data: log and continue, don't silently skip
- [ ] Schema drift: fail loudly if source adds/removes columns
- [ ] Missing data: distinguish "no data" from "zero"
- [ ] Date parsing: validate all dates parse correctly
- [ ] Municipality linkage: verify join keys exist in both datasets
- [ ] Storage failures: handle both local and S3 errors gracefully
- [ ] Disk space: verify local storage has sufficient space before ingestion

## Implementation: Adding Storage Backend Support

To implement the storage mode selection in code:

### 1. Create Storage Backend Module

```python
# src/ingestion/storage_backend.py
import os
from typing import Optional, List
import boto3
from pathlib import Path

class StorageBackend:
    """Abstracts S3 and local filesystem storage."""
    
    def __init__(self, mode: str = None):
        self.mode = mode or os.getenv('STORAGE_MODE', 's3-only')
        self.local_dir = os.getenv('LOCAL_DATA_DIR', './data')
        self.s3_bucket = os.getenv('S3_BUCKET', 'enok-mba-thesis-datalake')
        self.s3_prefix = os.getenv('S3_PREFIX', 'bronze/')
        
        if self.mode == 's3-only':
            self.s3_client = boto3.client('s3')
    
    def write(self, path: str, data: bytes, metadata: dict = None) -> bool:
        """Write data to configured storage(s)."""
        success = True
        
        if self.mode in ('local-only', 'both'):
            local_path = Path(self.local_dir) / path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(data)
            if metadata:
                (local_path.parent / '.metadata.json').write_text(
                    json.dumps(metadata)
                )
        
        if self.mode in ('s3-only', 'both'):
            s3_key = f"{self.s3_prefix}{path}"
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=data,
                Metadata=metadata or {}
            )
        
        return success
    
    def read(self, path: str) -> bytes:
        """Read data from storage (local preferred if both)."""
        if self.mode in ('local-only', 'both'):
            local_path = Path(self.local_dir) / path
            if local_path.exists():
                return local_path.read_bytes()
        
        if self.mode in ('s3-only', 'both'):
            s3_key = f"{self.s3_prefix}{path}"
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=s3_key
            )
            return response['Body'].read()
        
        raise FileNotFoundError(f"Path not found: {path}")
    
    def exists(self, path: str) -> bool:
        """Check if path exists in storage."""
        if self.mode in ('local-only', 'both'):
            if (Path(self.local_dir) / path).exists():
                return True
        
        if self.mode in ('s3-only', 'both'):
            try:
                s3_key = f"{self.s3_prefix}{path}"
                self.s3_client.head_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key
                )
                return True
            except:
                return False
        
        return False
```

### 2. Update Ingestion Clients

```python
# src/ingestion/ibge_client.py (example)
from .storage_backend import StorageBackend

class IBGEClient:
    def __init__(self, storage_mode: str = None):
        self.storage = StorageBackend(storage_mode)
    
    def fetch_and_store(self, indicator: str, year: int) -> str:
        # Fetch data from API
        data = self._fetch_from_api(indicator, year)
        
        # Store using configured backend
        path = f"ibge/{indicator}_{year}.csv"
        self.storage.write(path, data.encode(), metadata={
            'source': 'IBGE',
            'indicator': indicator,
            'year': str(year),
            'ingested_at': datetime.now().isoformat()
        })
        
        return path
```

### 3. CLI Options for Shell Script

Update `scripts/01_bronze_ingestion.sh`:

```bash
#!/bin/bash

# Parse storage mode from args or env
STORAGE_MODE=${STORAGE_MODE:-s3-only}
LOCAL_DATA_DIR=${LOCAL_DATA_DIR:-./data}
S3_BUCKET=${S3_BUCKET:-enok-mba-thesis-datalake}

# Command line options
for arg in "$@"; do
    case $arg in
        --local-only) STORAGE_MODE=local-only ;;
        --s3-only) STORAGE_MODE=s3-only ;;
        --both) STORAGE_MODE=both ;;
        --local-dir=*) LOCAL_DATA_DIR="${arg#*=}" ;;
        --s3-bucket=*) S3_BUCKET="${arg#*=}" ;;
    esac
done

# Export for Python clients
export STORAGE_MODE
export LOCAL_DATA_DIR
export S3_BUCKET

# Run ingestion
python -m src.ingestion.ibge_client
python -m src.ingestion.transparency_client
# ... etc
```

### 4. Usage Examples

```bash
# Full flexibility via CLI
./scripts/01_bronze_ingestion.sh --local-only --local-dir=/mnt/data
./scripts/01_bronze_ingestion.sh --s3-only --s3-bucket=my-bucket
./scripts/01_bronze_ingestion.sh --both --local-dir=/mnt/data --s3-bucket=my-bucket

# Or via environment
export STORAGE_MODE=local-only
export LOCAL_DATA_DIR=/home/user/tcc-data
./scripts/01_bronze_ingestion.sh

# In Python code
from src.ingestion.storage_backend import StorageBackend

# Explicit mode
storage = StorageBackend(mode='local-only')  # or 's3-only', 'both'
storage.write('bronze/ibge/data.csv', b'...')

# From environment
storage = StorageBackend()  # reads STORAGE_MODE env var
```

## CLI Reference

### 01_bronze_ingestion.sh

| Option | Description | Default |
|--------|-------------|---------|
| `--local-only` | Store data only in local filesystem | Disabled |
| `--s3-only` | Store data only in S3 | **Enabled** |
| `--both` | Store data in both local and S3 | Disabled |
| `--local-dir=PATH` | Local storage directory | `$LOCAL_DATA_DIR` or `./data` |
| `--s3-bucket=NAME` | S3 bucket name | `$S3_BUCKET` or `enok-mba-thesis-datalake` |
| `--only-ibge` | Ingest only IBGE data | All sources |
| `--only-inflation` | Ingest only inflation data | All sources |
| `--only-transparency` | Ingest only Transparency data | All sources |
| `--skip-ibge` | Skip IBGE ingestion | - |
| `--skip-inflation` | Skip inflation ingestion | - |
| `--skip-transparency` | Skip Transparency ingestion | - |

### Environment Variables

| Variable | Description | Required For |
|----------|-------------|------------|
| `STORAGE_MODE` | `local-only`, `s3-only`, or `both` | All modes |
| `LOCAL_DATA_DIR` | Base directory for local storage | `local-only`, `both` |
| `S3_BUCKET` | S3 bucket name | `s3-only`, `both` |
| `S3_PREFIX` | Prefix for S3 keys (e.g., `bronze/`) | `s3-only`, `both` |
| `AWS_PROFILE` | AWS credentials profile | `s3-only`, `both` |
| `AWS_REGION` | AWS region | `s3-only`, `both` |

## Related Workflows

- `.windsurf/workflows/data-pipeline-change.md` — For pipeline-wide changes
- `docs/llm/workflows/pipeline-change.md` — For this repo's specific pipeline
- `.windsurf/workflows/dataset-onboarding.md` — For brand new datasets
