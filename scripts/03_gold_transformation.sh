#!/bin/bash
# =============================================================================
# STEP 3: GOLD LAYER - Analytics & Aggregations
# =============================================================================
# Transforms Silver data into analysis-ready Gold layer aggregations.
#
# Outputs:
#   - Municipality socioeconomic metrics (2010→2022 change indicators)
#   - State-level summaries with sanctions per capita
#   - Sanctions aggregations by registry type
#   - Analysis-ready dataset for regression/correlation
#
# Input:  s3://enok-mba-thesis-datalake/silver/
# Output: s3://enok-mba-thesis-datalake/gold/
#
# Usage:
#   ./scripts/03_gold_transformation.sh [OPTIONS]
#
# Options:
#   --help  Show this help message
#
# Previous Step: ./scripts/02_silver_transformation.sh
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

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help)
            echo "Gold Layer Transformation Script"
            echo ""
            echo "Usage: ./scripts/run_gold_transformation.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --help              Show this help message"
            echo ""
            echo "Prerequisites:"
            echo "  - Silver layer must be populated first"
            echo "  - Run ./scripts/run_transformation.sh before this script"
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
echo -e "${BLUE}   STEP 3: GOLD LAYER - Analytics${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""

# Set PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Run Gold transformation
echo -e "${YELLOW}📊 Starting Gold layer transformation...${NC}"
if "$PYTHON" -c "
from pathlib import Path
from src.processing.gold_transformer import GoldTransformer

BUCKET_NAME = 'enok-mba-thesis-datalake'
CONFIG_FILE = Path('config/silver_schemas.json')

transformer = GoldTransformer(BUCKET_NAME, str(CONFIG_FILE))
success = transformer.transform()
exit(0 if success else 1)
"
then
    echo -e "${GREEN}✓ Gold transformation completed${NC}"
else
    echo -e "${RED}✗ Gold transformation failed${NC}"
    exit 1
fi
echo ""

echo -e "${BLUE}=============================================${NC}"
echo -e "${GREEN}✓ Gold layer transformation pipeline finished${NC}"
echo -e "${BLUE}=============================================${NC}"
echo ""
echo "Output locations:"
echo "  s3://enok-mba-thesis-datalake/gold/agg_municipality_socioeconomic/"
echo "  s3://enok-mba-thesis-datalake/gold/agg_state_summary/"
echo "  s3://enok-mba-thesis-datalake/gold/agg_sanctions_summary/"
echo "  s3://enok-mba-thesis-datalake/gold/analysis_compliance/"
echo ""
echo "Processing log: docs/processing.log"
