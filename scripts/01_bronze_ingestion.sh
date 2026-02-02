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
#   --only-transparency  Ingest only Transparency Portal data
#
# Next Step: ./scripts/02_silver_transformation.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "============================================================"
echo "📥 STEP 1: BRONZE LAYER - Data Ingestion"
echo "============================================================"
echo "Fetches raw data from APIs → S3 Bronze layer"
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
SKIP_TRANSPARENCY=${SKIP_TRANSPARENCY:-0}

for arg in "$@"; do
    case $arg in
        --skip-ibge) SKIP_IBGE=1 ;;
        --skip-transparency) SKIP_TRANSPARENCY=1 ;;
        --only-ibge) SKIP_TRANSPARENCY=1 ;;
        --only-transparency) SKIP_IBGE=1 ;;
    esac
done

run_step() {
    local step_name=$1
    local step_command=$2

    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Running: $step_name${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    TOTAL_STEPS=$((TOTAL_STEPS + 1))

    if eval "$step_command"; then
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
if [ -f ".venv/Scripts/python.exe" ]; then
    PYTHON=".venv/Scripts/python.exe"
    echo "🔌 Using Windows-style environment..."
elif [ -f ".venv/bin/python" ]; then
    PYTHON=".venv/bin/python"
    echo "🔌 Using Unix-style environment (macOS/Linux)..."
else
    echo -e "${RED}❌ Error: Virtual environment not found. Run ./start_env.sh first.${NC}"
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

if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  Warning: .env file not found${NC}"
    echo "   Copy .env.example to .env and configure your credentials."
fi

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
echo "============================================================"
echo ""

if [ "$SKIP_IBGE" = "1" ]; then
    skip_step "Bronze I - IBGE Ingestion"
else
    run_step "Bronze I - IBGE Ingestion" "$PYTHON src/ingestion/ibge_client.py"
fi

echo ""
if [ "$SKIP_TRANSPARENCY" = "1" ]; then
    skip_step "Bronze II - Transparency Ingestion"
elif [ "$TRANSPARENCY_READY" = true ]; then
    run_step "Bronze II - Transparency Ingestion" "$PYTHON src/ingestion/transparency_client.py"
else
    echo -e "${YELLOW}⚠️  TRANSPARENCY_API_KEY not set. Skipping Transparency ingestion.${NC}"
    skip_step "Bronze II - Transparency Ingestion"
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
