#!/bin/bash
# =============================================================================
# STEP 2: SILVER LAYER - Data Transformation
# =============================================================================
# Transforms raw Bronze data into cleaned, normalized Silver layer tables.
#
# Transformations:
#   - Schema standardization and type enforcement
#   - Municipality code extraction and validation
#   - Geographic hierarchy (state, region) enrichment
#   - Duplicate removal and data quality checks
#
# Storage Modes:
#   local-only  - Read/write local filesystem only (no AWS required)
#   s3-only     - Read/write S3 only (AWS required, default)
#   both        - Read/write both local and S3
#
# Input:  bronze/  (local or s3://BUCKET/bronze/)
# Output: silver/  (local or s3://BUCKET/silver/)
#
# Usage:
#   ./scripts/02_silver_transformation.sh [OPTIONS]
#
# Options:
#   --local-only         Use local filesystem only (no AWS)
#   --s3-only            Use S3 only (requires AWS credentials)
#   --both               Use both local and S3
#   --local-dir=PATH     Set local data directory
#   --s3-bucket=NAME     Set S3 bucket name
#   --only-ibge          Transform only IBGE data
#   --only-transparency  Transform only Transparency data
#   --help               Show this help message
#
# Environment Variables:
#   STORAGE_MODE         local-only, s3-only, or both (default: local-only)
#   LOCAL_DATA_DIR       Base directory for local storage
#   S3_BUCKET_NAME       S3 bucket name
#   AWS_PROFILE          AWS credentials profile
#
# Previous Step: ./scripts/01_bronze_ingestion.sh
# Next Step:     ./scripts/03_gold_transformation.sh
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUNTIME_CONFIG="$PROJECT_ROOT/config/runtime_config.json"

# Change to project root
cd "$PROJECT_ROOT"

# Load environment variables
if [ -f .env ]; then
    set -a
    . ./.env
    set +a
    echo -e "${GREEN}✓ Loaded environment from .env${NC}"
fi

# Default: run all transformations
RUN_IBGE=true
RUN_TRANSPARENCY=true

# Parse command line arguments
for arg in "$@"; do
    case $arg in
        --only-ibge) RUN_TRANSPARENCY=false ;;
        --only-transparency) RUN_IBGE=false ;;
        --local-only) STORAGE_MODE=local-only ;;
        --s3-only) STORAGE_MODE=s3-only ;;
        --both) STORAGE_MODE=both ;;
        --local-dir=*) LOCAL_DATA_DIR="${arg#*=}" ; export LOCAL_DATA_DIR ;;
        --s3-bucket=*) S3_BUCKET_NAME="${arg#*=}" ; export S3_BUCKET_NAME ;;
        --help)
            echo "Silver Layer Transformation Script"
            echo ""
            echo "Usage: ./scripts/02_silver_transformation.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --local-only         Use local filesystem only (no AWS)"
            echo "  --s3-only            Use S3 only (requires AWS credentials)"
            echo "  --both               Use both local and S3"
            echo "  --local-dir=PATH     Set local data directory"
            echo "  --s3-bucket=NAME     Set S3 bucket name"
            echo "  --only-ibge          Transform only IBGE data"
            echo "  --only-transparency  Transform only Transparency data"
            echo "  --help               Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $arg${NC}"
            exit 1
            ;;
    esac
done

read_runtime_value() {
    local key_path="$1"
    python3 - "$RUNTIME_CONFIG" "$key_path" <<'PY'
import json
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
key_path = sys.argv[2].split(".")
if not config_path.exists():
    sys.exit(0)

with open(config_path, "r", encoding="utf-8") as f:
    data = json.load(f)

current = data
for key in key_path:
    if not isinstance(current, dict):
        sys.exit(0)
    current = current.get(key)
    if current is None:
        sys.exit(0)

print(current)
PY
}

# Storage mode
STORAGE_MODE="${STORAGE_MODE:-local-only}"
export STORAGE_MODE
echo "Storage Mode: ${STORAGE_MODE}"

# AWS configuration (only for s3-only or both modes)
if [ "${STORAGE_MODE}" != "local-only" ]; then
    AWS_PROFILE="${AWS_PROFILE:-$(read_runtime_value aws.profile)}"
    if [ -n "${AWS_PROFILE:-}" ]; then
        export AWS_PROFILE
    fi

    S3_BUCKET_NAME="${S3_BUCKET_NAME:-$(read_runtime_value aws.s3_bucket_name)}"
    S3_BUCKET_NAME="${S3_BUCKET_NAME:-enok-mba-thesis-datalake}"
    export S3_BUCKET_NAME

    if ! aws sts get-caller-identity &> /dev/null; then
        echo -e "${RED}❌ Error: AWS credentials not configured${NC}"
        echo "   Configure AWS (e.g., aws configure) or use STORAGE_MODE=local-only"
        exit 1
    fi

    echo -e "${GREEN}✓ AWS credentials configured${NC}"
    echo "Target S3 Bucket: ${S3_BUCKET_NAME}"

    if ! aws s3api head-bucket --bucket "${S3_BUCKET_NAME}" &> /dev/null; then
        echo -e "${RED}❌ Error: Cannot access S3 bucket '${S3_BUCKET_NAME}'${NC}"
        echo "   Verify bucket name and permissions."
        exit 1
    fi

    echo -e "${GREEN}✓ S3 bucket accessible${NC}"
else
    echo -e "${GREEN}✓ Local-only mode: Skipping AWS checks${NC}"
    S3_BUCKET_NAME="${S3_BUCKET_NAME:-enok-mba-thesis-datalake}"
    export S3_BUCKET_NAME
fi

LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-$PROJECT_ROOT/data}"
export LOCAL_DATA_DIR

# Detect Python executable (Windows vs Unix)
echo "🔌 Detecting virtual environment..."
if [ -f ".venv/bin/python" ]; then
    PYTHON=".venv/bin/python"
    echo "🔌 Using Unix-style environment (macOS/Linux)..."
elif [ -f ".venv/Scripts/python.exe" ] && { [ "$(uname -s)" = "MINGW64_NT" ] || [ "$(uname -s)" = "MSYS_NT" ] || [ "$(uname -s)" = "CYGWIN_NT" ]; }; then
    PYTHON=".venv/Scripts/python.exe"
    echo "🔌 Using Windows-style environment..."
elif command -v python3 >/dev/null 2>&1; then
    PYTHON="python3"
    echo "🔌 Using system python3..."
else
    echo -e "${RED}❌ Error: No usable Python interpreter found.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Virtual environment detected${NC}"

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}   STEP 2: SILVER LAYER - Transformation${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""

# Set PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"

# Run IBGE transformation
if [ "$RUN_IBGE" = true ]; then
    echo -e "${YELLOW}📊 Starting IBGE transformation...${NC}"
    if "$PYTHON" -c "
import os
from pathlib import Path
from src.processing.ibge_transformer import IBGETransformer

BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'enok-mba-thesis-datalake')
CONFIG_FILE = Path('config/silver_schemas.json')

transformer = IBGETransformer(BUCKET_NAME, str(CONFIG_FILE))
success = transformer.transform()
exit(0 if success else 1)
"
    then
        echo -e "${GREEN}✓ IBGE transformation completed${NC}"
    else
        echo -e "${RED}✗ IBGE transformation failed${NC}"
    fi
    echo ""
fi

# Run Transparency transformation
if [ "$RUN_TRANSPARENCY" = true ]; then
    echo -e "${YELLOW}📊 Starting Transparency transformation...${NC}"
    if "$PYTHON" -c "
import os
from pathlib import Path
from src.processing.transparency_transformer import TransparencyTransformer

BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'enok-mba-thesis-datalake')
CONFIG_FILE = Path('config/silver_schemas.json')

transformer = TransparencyTransformer(BUCKET_NAME, str(CONFIG_FILE))
success = transformer.transform()
exit(0 if success else 1)
"
    then
        echo -e "${GREEN}✓ Transparency transformation completed${NC}"
    else
        echo -e "${RED}✗ Transparency transformation failed${NC}"
    fi
    echo ""
fi

echo -e "${BLUE}=============================================${NC}"
echo -e "${GREEN}✓ Silver layer transformation pipeline finished${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""
echo "Output locations:"
if [ "${STORAGE_MODE}" = "local-only" ]; then
    echo "  ${LOCAL_DATA_DIR}/silver/dim_municipalities/"
    echo "  ${LOCAL_DATA_DIR}/silver/dim_municipality_lookup/"
    echo "  ${LOCAL_DATA_DIR}/silver/fact_population/"
    echo "  ${LOCAL_DATA_DIR}/silver/fact_sanitation/"
    echo "  ${LOCAL_DATA_DIR}/silver/fact_literacy/"
    echo "  ${LOCAL_DATA_DIR}/silver/fact_income/"
    echo "  ${LOCAL_DATA_DIR}/silver/fact_federal_transfers/"
    echo "  ${LOCAL_DATA_DIR}/silver/fact_sanctions/"
elif [ "${STORAGE_MODE}" = "both" ]; then
    echo "  ${LOCAL_DATA_DIR}/silver/  (local)"
    echo "  s3://${S3_BUCKET_NAME}/silver/  (S3)"
else
    echo "  s3://${S3_BUCKET_NAME}/silver/dim_municipalities/"
    echo "  s3://${S3_BUCKET_NAME}/silver/dim_municipality_lookup/"
    echo "  s3://${S3_BUCKET_NAME}/silver/fact_population/"
    echo "  s3://${S3_BUCKET_NAME}/silver/fact_sanitation/"
    echo "  s3://${S3_BUCKET_NAME}/silver/fact_literacy/"
    echo "  s3://${S3_BUCKET_NAME}/silver/fact_income/"
    echo "  s3://${S3_BUCKET_NAME}/silver/fact_federal_transfers/"
    echo "  s3://${S3_BUCKET_NAME}/silver/fact_sanctions/"
fi
echo ""
echo "Processing log: docs/processing.log"
