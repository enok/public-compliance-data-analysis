#!/bin/bash
# =============================================================================
# STEP 1: BRONZE LAYER - Data Ingestion
# =============================================================================
# Fetches raw data from external APIs and stores in Bronze layer.
#
# Sources:
#   - IBGE SIDRA API: Census data (population, sanitation, literacy, income)
#   - Transparency Portal API: Federal transfers, compliance sanctions
#   - BCB API: IPCA inflation series
#
# Storage Modes:
#   local-only  - Store data locally (no AWS required)
#   s3-only     - Store data in S3 (AWS required, default)
#   both        - Store in both locations (redundancy)
#
# Usage:
#   ./scripts/01_bronze_ingestion.sh [OPTIONS]
#
# Options:
#   --local-only         Use local filesystem only (no AWS)
#   --s3-only            Use S3 only (requires AWS credentials)
#   --both               Use both local and S3
#   --local-dir=PATH    Set local data directory
#   --s3-bucket=NAME     Set S3 bucket name
#   --only-ibge          Ingest only IBGE data
#   --only-inflation     Ingest only inflation data
#   --only-transparency  Ingest only Transparency Portal data
#   --skip-ibge          Skip IBGE ingestion
#   --skip-inflation     Skip inflation ingestion
#   --skip-transparency  Skip Transparency ingestion
#
# Environment Variables:
#   STORAGE_MODE         local-only, s3-only, or both (default: local-only)
#   LOCAL_DATA_DIR       Base directory for local storage
#   S3_BUCKET_NAME       S3 bucket name
#   AWS_PROFILE          AWS credentials profile
#   TRANSPARENCY_API_KEY API key for Transparency Portal
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

# Parse command line arguments
for arg in "$@"; do
    case $arg in
        --skip-ibge) SKIP_IBGE=1 ;;
        --skip-inflation) SKIP_INFLATION=1 ;;
        --skip-transparency) SKIP_TRANSPARENCY=1 ;;
        --only-ibge) SKIP_INFLATION=1; SKIP_TRANSPARENCY=1 ;;
        --only-inflation) SKIP_IBGE=1; SKIP_TRANSPARENCY=1 ;;
        --only-transparency) SKIP_IBGE=1; SKIP_INFLATION=1 ;;
        --local-only) STORAGE_MODE=local-only ;;
        --s3-only) STORAGE_MODE=s3-only ;;
        --both) STORAGE_MODE=both ;;
        --local-dir=*) LOCAL_DATA_DIR="${arg#*=}" ; export LOCAL_DATA_DIR ;;
        --s3-bucket=*) S3_BUCKET_NAME="${arg#*=}" ; export S3_BUCKET_NAME ;;
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

# Check storage mode
STORAGE_MODE="${STORAGE_MODE:-local-only}"
echo "Storage Mode: ${STORAGE_MODE}"

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

# AWS configuration (only for s3-only or both modes)
if [ "${STORAGE_MODE}" != "local-only" ]; then
    AWS_PROFILE="${AWS_PROFILE:-$(read_runtime_value aws.profile)}"
    if [ -n "${AWS_PROFILE:-}" ]; then
        export AWS_PROFILE
    fi

    S3_BUCKET_NAME="${S3_BUCKET_NAME:-$(read_runtime_value aws.s3_bucket_name)}"
    S3_BUCKET_NAME="${S3_BUCKET_NAME:-enok-mba-thesis-datalake}"

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
fi

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
if [ "${STORAGE_MODE}" = "local-only" ]; then
    echo "Storage: Local filesystem only"
    echo "1. Bronze I - IBGE (SIDRA) -> Local"
    echo "2. Bronze II - Transparency Portal -> Local"
    echo "3. Bronze III - Inflation (BCB IPCA) -> Local"
elif [ "${STORAGE_MODE}" = "both" ]; then
    echo "Storage: Local + S3 (hybrid)"
    echo "1. Bronze I - IBGE (SIDRA) -> Local + S3"
    echo "2. Bronze II - Transparency Portal -> Local + S3"
    echo "3. Bronze III - Inflation (BCB IPCA) -> Local + S3"
else
    echo "Storage: S3 only"
    echo "1. Bronze I - IBGE (SIDRA) -> S3"
    echo "2. Bronze II - Transparency Portal -> S3"
    echo "3. Bronze III - Inflation (BCB IPCA) -> S3"
fi
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# Pre-ingestion sync: bidirectional S3 <-> local (mode=both, data on both sides)
# ---------------------------------------------------------------------------
if [ "${STORAGE_MODE}" = "both" ]; then
    LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-$REPO_ROOT/data}"
    LOCAL_BRONZE_DIR="${LOCAL_DATA_DIR}/bronze"
    S3_BRONZE_URI="s3://${S3_BUCKET_NAME}/bronze"

    # Detect data presence on each side
    LOCAL_HAS_DATA=false
    if [ -d "${LOCAL_BRONZE_DIR}" ] && [ -n "$(ls -A "${LOCAL_BRONZE_DIR}" 2>/dev/null)" ]; then
        LOCAL_HAS_DATA=true
    fi

    S3_HAS_DATA=false
    if aws s3 ls "${S3_BRONZE_URI}/" --summarize 2>/dev/null | grep -q "Total Objects"; then
        S3_OBJECT_COUNT=$(aws s3 ls "${S3_BRONZE_URI}/" --recursive --summarize 2>/dev/null | grep "Total Objects" | awk '{print $3}')
        if [ "${S3_OBJECT_COUNT:-0}" -gt 0 ]; then
            S3_HAS_DATA=true
        fi
    fi

    if [ "${LOCAL_HAS_DATA}" = true ] && [ "${S3_HAS_DATA}" = true ]; then
        echo "============================================================"
        echo "🔄 Pre-ingestion Sync: Local ↔ S3 Bronze"
        echo "   Both sides have data — merging before ingestion."
        echo "============================================================"

        echo ""
        echo "  [1/2] Local → S3 (upload files missing or smaller on S3)..."
        if aws s3 sync "${LOCAL_BRONZE_DIR}/" "${S3_BRONZE_URI}/" --size-only; then
            echo -e "${GREEN}  ✓ Local → S3 sync complete${NC}"
        else
            echo -e "${RED}  ❌ Local → S3 sync failed. Aborting to avoid data loss.${NC}"
            exit 1
        fi

        echo ""
        echo "  [2/2] S3 → Local (download files missing or smaller locally)..."
        if aws s3 sync "${S3_BRONZE_URI}/" "${LOCAL_BRONZE_DIR}/" --size-only; then
            echo -e "${GREEN}  ✓ S3 → Local sync complete${NC}"
        else
            echo -e "${RED}  ❌ S3 → Local sync failed. Aborting to avoid data loss.${NC}"
            exit 1
        fi

        echo ""
        echo -e "${GREEN}✅ Pre-ingestion sync complete. Proceeding with ingestion.${NC}"
        echo ""
    elif [ "${LOCAL_HAS_DATA}" = true ]; then
        echo -e "${YELLOW}ℹ️  Only local data found — skipping pre-ingestion sync.${NC}"
        echo ""
    elif [ "${S3_HAS_DATA}" = true ]; then
        echo -e "${YELLOW}ℹ️  Only S3 data found — skipping pre-ingestion sync.${NC}"
        echo ""
    else
        echo -e "${YELLOW}ℹ️  No existing bronze data on either side — skipping pre-ingestion sync.${NC}"
        echo ""
    fi
fi

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
