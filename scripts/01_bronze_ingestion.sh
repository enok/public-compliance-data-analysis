#!/bin/bash
# =============================================================================
# STEP 1: BRONZE LAYER - Data Ingestion
# =============================================================================
# Fetches raw data from external APIs and stores in S3 Bronze layer.
#
# Sources:
#   - IBGE SIDRA API: Census data (population, sanitation, literacy, income)
#   - Transparency Portal API: Federal transfers, compliance sanctions
#
# Output: s3://enok-mba-thesis-datalake/bronze/
#
# Usage:
#   ./scripts/01_bronze_ingestion.sh [OPTIONS]
#
# Options:
#   --only-ibge          Ingest only IBGE data
#   --only-inflation     Ingest only inflation data
#   --only-transparency  Ingest only Transparency Portal data
#
# Optional env vars for targeted Transparency rechecks:
#   TRANSPARENCY_FORCE_REFRESH=1  Recheck months even if metadata says no_data
#   TRANSPARENCY_START_MA=MM/YYYY Restrict Transparency expansion start month
#   TRANSPARENCY_END_MA=MM/YYYY   Restrict Transparency expansion end month
#
# Next Step: ./scripts/02_silver_transformation.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"
RUNTIME_CONFIG="$REPO_ROOT/config/runtime_config.json"

echo "============================================================"
echo "📥 STEP 1: BRONZE LAYER - Data Ingestion"
echo "============================================================"
echo "Fetches raw census, transparency, and inflation data → S3 Bronze layer"
echo "============================================================"
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

TOTAL_STEPS=0
PASSED_STEPS=0
FAILED_STEPS=0
SKIPPED_STEPS=0

SKIP_IBGE=${SKIP_IBGE:-0}
SKIP_INFLATION=${SKIP_INFLATION:-0}
SKIP_TRANSPARENCY=${SKIP_TRANSPARENCY:-0}

for arg in "$@"; do
    case $arg in
        --skip-ibge) SKIP_IBGE=1 ;;
        --skip-inflation) SKIP_INFLATION=1 ;;
        --skip-transparency) SKIP_TRANSPARENCY=1 ;;
        --only-ibge) SKIP_INFLATION=1; SKIP_TRANSPARENCY=1 ;;
        --only-inflation) SKIP_IBGE=1; SKIP_TRANSPARENCY=1 ;;
        --only-transparency) SKIP_IBGE=1; SKIP_INFLATION=1 ;;
    esac
done

run_step() {
    local step_name=$1
    shift

    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Running: $step_name${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    TOTAL_STEPS=$((TOTAL_STEPS + 1))

    if "$@"; then
        echo -e "${GREEN}✅ DONE: $step_name${NC}"
        PASSED_STEPS=$((PASSED_STEPS + 1))
        return 0
    else
        echo -e "${RED}❌ FAILED: $step_name${NC}"
        FAILED_STEPS=$((FAILED_STEPS + 1))
        return 1
    fi
}

skip_step() {
    local step_name=$1
    TOTAL_STEPS=$((TOTAL_STEPS + 1))
    SKIPPED_STEPS=$((SKIPPED_STEPS + 1))
    echo -e "${YELLOW}⏭️  Skipped: $step_name${NC}"
}

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

echo ""
echo "🔍 Checking configuration..."

if [ -f ".env" ]; then
    set -a
    . ./.env
    set +a
fi

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

if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  Warning: .env file not found${NC}"
    echo "   Copy .env.example to .env and configure your credentials."
fi

AWS_PROFILE="${AWS_PROFILE:-$(read_runtime_value aws.profile)}"
if [ -n "${AWS_PROFILE:-}" ]; then
    export AWS_PROFILE
fi

S3_BUCKET_NAME="${S3_BUCKET_NAME:-$(read_runtime_value aws.s3_bucket_name)}"
S3_BUCKET_NAME="${S3_BUCKET_NAME:-enok-mba-thesis-datalake}"

if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}❌ Error: AWS credentials not configured${NC}"
    echo "   Configure AWS (e.g., aws configure) and try again."
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

if [ -z "${TRANSPARENCY_API_KEY}" ]; then
    echo -e "${YELLOW}⚠️  Warning: TRANSPARENCY_API_KEY not set in environment${NC}"
    echo "   Transparency ingestion may fail or be rate-limited."
    TRANSPARENCY_READY=false
else
    TRANSPARENCY_READY=true
fi

echo ""
echo "============================================================"
echo "📋 Ingestion Plan"
echo "============================================================"
echo "1. Bronze I - IBGE (SIDRA) -> S3"
echo "2. Bronze II - Transparency Portal -> S3"
echo "3. Bronze III - Inflation (BCB IPCA) -> S3"
echo "============================================================"
echo ""

if [ "$SKIP_IBGE" = "1" ]; then
    skip_step "Bronze I - IBGE Ingestion"
else
    run_step "Bronze I - IBGE Ingestion" env IBGE_DATASET_NAMES=pop_2010,pop_2022,sanitation_2010,sanitation_2022,literacy_2010,literacy_2022,income_2010,income_2022 "$PYTHON" -m src.ingestion.ibge_client
fi

echo ""
if [ "$SKIP_TRANSPARENCY" = "1" ]; then
    skip_step "Bronze II - Transparency Ingestion"
elif [ "$TRANSPARENCY_READY" = true ]; then
    run_step "Bronze II - Transparency Ingestion" "$PYTHON" -m src.ingestion.transparency_client
else
    echo -e "${YELLOW}⚠️  TRANSPARENCY_API_KEY not set. Skipping Transparency ingestion.${NC}"
    skip_step "Bronze II - Transparency Ingestion"
fi

echo ""
if [ "$SKIP_INFLATION" = "1" ]; then
    skip_step "Bronze III - Inflation Ingestion"
else
    run_step "Bronze III - Inflation Ingestion" env IBGE_DATASET_NAMES=ipca_monthly "$PYTHON" -m src.ingestion.ibge_client
fi

echo ""
echo "============================================================"
echo "📊 INGESTION SUMMARY"
echo "============================================================"
echo -e "Total Steps: $TOTAL_STEPS"
echo -e "${GREEN}Done: $PASSED_STEPS${NC}"
echo -e "${YELLOW}Skipped: $SKIPPED_STEPS${NC}"
echo -e "${RED}Failed: $FAILED_STEPS${NC}"
echo "============================================================"

if [ $FAILED_STEPS -eq 0 ]; then
    echo -e "${GREEN}🎉 Ingestion runner finished.${NC}"
    exit 0
else
    echo -e "${RED}⚠️  Some steps failed. Review logs above.${NC}"
    exit 1
fi
