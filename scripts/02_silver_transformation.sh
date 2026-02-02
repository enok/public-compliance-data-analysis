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
# Input:  s3://enok-mba-thesis-datalake/bronze/
# Output: s3://enok-mba-thesis-datalake/silver/
#
# Usage:
#   ./scripts/02_silver_transformation.sh [OPTIONS]
#
# Options:
#   --only-ibge          Transform only IBGE data
#   --only-transparency  Transform only Transparency data
#   --help               Show this help message
#
# Previous Step: ./scripts/01_bronze_ingestion.sh
# Next Step:     ./scripts/03_gold_transformation.sh
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

# Load environment variables
if [ -f .env ]; then
    set -a
    . ./.env
    set +a
    echo -e "${GREEN}✓ Loaded environment from .env${NC}"
fi

# Default: run both transformations
RUN_IBGE=true
RUN_TRANSPARENCY=true

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --only-ibge)
            RUN_IBGE=true
            RUN_TRANSPARENCY=false
            shift
            ;;
        --only-transparency)
            RUN_IBGE=false
            RUN_TRANSPARENCY=true
            shift
            ;;
        --help)
            echo "Silver Layer Transformation Script"
            echo ""
            echo "Usage: ./scripts/run_transformation.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --only-ibge          Transform only IBGE data"
            echo "  --only-transparency  Transform only Transparency data"
            echo "  --help              Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Detect Python executable (Windows vs Unix)
echo "🔌 Detecting virtual environment..."
if [ -f ".venv/Scripts/python.exe" ]; then
    PYTHON=".venv/Scripts/python.exe"
    echo "🔌 Using Windows-style environment..."
elif [ -f ".venv/bin/python" ]; then
    PYTHON=".venv/bin/python"
    echo "🔌 Using Unix-style environment (macOS/Linux)..."
else
    echo -e "${RED}❌ Error: Virtual environment not found.${NC}"
    echo "   Run: python -m venv .venv"
    echo "   Then: pip install -r requirements.txt"
    exit 1
fi

echo -e "${GREEN}✓ Virtual environment detected${NC}"

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}   STEP 2: SILVER LAYER - Transformation${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""

# Set PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Run IBGE transformation
if [ "$RUN_IBGE" = true ]; then
    echo -e "${YELLOW}📊 Starting IBGE transformation...${NC}"
    "$PYTHON" -c "
from pathlib import Path
from src.processing.ibge_transformer import IBGETransformer

BUCKET_NAME = 'enok-mba-thesis-datalake'
CONFIG_FILE = Path('config/silver_schemas.json')

transformer = IBGETransformer(BUCKET_NAME, str(CONFIG_FILE))
success = transformer.transform()
exit(0 if success else 1)
"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ IBGE transformation completed${NC}"
    else
        echo -e "${RED}✗ IBGE transformation failed${NC}"
    fi
    echo ""
fi

# Run Transparency transformation
if [ "$RUN_TRANSPARENCY" = true ]; then
    echo -e "${YELLOW}📊 Starting Transparency transformation...${NC}"
    "$PYTHON" -c "
from pathlib import Path
from src.processing.transparency_transformer import TransparencyTransformer

BUCKET_NAME = 'enok-mba-thesis-datalake'
CONFIG_FILE = Path('config/silver_schemas.json')

transformer = TransparencyTransformer(BUCKET_NAME, str(CONFIG_FILE))
success = transformer.transform()
exit(0 if success else 1)
"
    if [ $? -eq 0 ]; then
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
echo "  s3://enok-mba-thesis-datalake/silver/dim_municipalities/"
echo "  s3://enok-mba-thesis-datalake/silver/fact_population/"
echo "  s3://enok-mba-thesis-datalake/silver/fact_sanitation/"
echo "  s3://enok-mba-thesis-datalake/silver/fact_literacy/"
echo "  s3://enok-mba-thesis-datalake/silver/fact_income/"
echo "  s3://enok-mba-thesis-datalake/silver/fact_federal_transfers/"
echo "  s3://enok-mba-thesis-datalake/silver/fact_sanctions/"
echo ""
echo "Processing log: docs/processing.log"
