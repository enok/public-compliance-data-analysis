#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "============================================================"
echo "🧪 MBA Thesis - Test Suite Runner"
echo "============================================================"
echo "Project: Public Compliance Data Analysis"
echo "Author: Enok Antônio de Jesus"
echo "============================================================"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test results tracking
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to run a test suite
run_test() {
    local test_name=$1
    shift
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Running: $test_name${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if "$@"; then
        echo -e "${GREEN}✅ PASSED: $test_name${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}❌ FAILED: $test_name${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# 1. Detect Virtual Environment Python
echo "🔌 Detecting virtual environment..."
# Windows uses .venv/Scripts/, while macOS/Linux use .venv/bin/
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

# 2. Check .env file exists
echo ""
echo "🔍 Checking configuration..."

if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  Warning: .env file not found${NC}"
    echo "   Copy .env.example to .env and configure your credentials."
    echo "   Some tests may be skipped."
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${YELLOW}⚠️  Warning: AWS credentials not configured${NC}"
    echo "   Integration tests requiring S3 will be skipped."
    SKIP_AWS=true
else
    echo -e "${GREEN}✓ AWS credentials configured${NC}"
    SKIP_AWS=false
fi

# Note: TRANSPARENCY_API_KEY will be loaded from .env by python-dotenv
SKIP_TRANSPARENCY=false

echo ""
echo "============================================================"
echo "📋 Test Execution Plan"
echo "============================================================"
echo "1. Bronze Layer - IBGE API Client (HTTP, retry logic, S3 upload)"
echo "2. Silver Layer - Data Transformers (schema validation, type conversion)"
echo "3. Silver Layer - Smart Caching (metadata tracking, change detection)"
echo "4. Gold Layer - Analytics Transformers (aggregations, change metrics)"
echo "5. Bronze Layer - API Endpoint Validation (IBGE metadata config)"
if [ "$SKIP_AWS" = false ]; then
    echo "6. Bronze Layer - End-to-End IBGE Ingestion (API → S3 validation)"
else
    echo "6. Bronze Layer - End-to-End IBGE Ingestion - SKIPPED (No AWS)"
fi
if [ "$SKIP_TRANSPARENCY" = false ] && [ "$SKIP_AWS" = false ]; then
    echo "7. Bronze Layer - End-to-End Transparency Ingestion (API → S3 validation)"
else
    echo "7. Bronze Layer - End-to-End Transparency Ingestion - SKIPPED"
fi
echo "============================================================"
echo ""

read -r -p "Press Enter to continue or Ctrl+C to cancel..."

# 3. Run Unit Tests - Bronze Layer (IBGE Client)
run_test "Bronze Layer - IBGE API Client" \
    "$PYTHON" -m unittest tests/ingestion/test_ibge_client.py -v

# 4. Run Endpoint Validation - Bronze Layer
run_test "Bronze Layer - IBGE Metadata Config Validation" \
    "$PYTHON" tests/test_ibge_metadata.py

# 5. Run Unit Tests - Silver Layer (Transformers)
run_test "Silver Layer - Data Transformers (BaseTransformer, IBGE, Transparency)" \
    "$PYTHON" -m pytest tests/processing/test_transformers.py -v

# 6. Run Unit Tests - Silver Layer (Smart Caching)
run_test "Silver Layer - Smart Caching (Metadata tracking, skip logic)" \
    "$PYTHON" -m pytest tests/processing/test_smart_caching.py -v

# 7. Run Unit Tests - Gold Layer (Transformers)
run_test "Gold Layer - Analytics Transformers (Aggregations, change metrics)" \
    "$PYTHON" -m pytest tests/processing/test_gold_transformer.py -v

# 8. Run Integration Tests (IBGE) - only if AWS is configured
if [ "$SKIP_AWS" = false ]; then
    echo ""
    echo -e "${YELLOW}⚠️  Next test will upload data to S3${NC}"
    read -p "Continue with IBGE integration test? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        run_test "Bronze Layer - End-to-End IBGE Ingestion (Full Pipeline)" \
            "$PYTHON" tests/test_ibge_ingestion.py
    else
        echo -e "${YELLOW}⏭️  Skipped: Bronze Layer - IBGE Integration Tests${NC}"
    fi
else
    echo -e "${YELLOW}⏭️  Skipped: Bronze Layer - IBGE Integration Tests (AWS not configured)${NC}"
fi

# 9. Run Integration Tests (Transparency) - only if API key and AWS are configured
if [ "$SKIP_TRANSPARENCY" = false ] && [ "$SKIP_AWS" = false ]; then
    echo ""
    echo -e "${YELLOW}⚠️  Next test will upload data to S3${NC}"
    read -p "Continue with Transparency Portal integration test? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        run_test "Bronze Layer - End-to-End Transparency Ingestion (Full Pipeline)" \
            "$PYTHON" tests/test_transparency_ingestion.py
    else
        echo -e "${YELLOW}⏭️  Skipped: Bronze Layer - Transparency Integration Tests${NC}"
    fi
else
    if [ "$SKIP_TRANSPARENCY" = true ]; then
        echo -e "${YELLOW}⏭️  Skipped: Bronze Layer - Transparency Integration Tests (API key not set)${NC}"
    else
        echo -e "${YELLOW}⏭️  Skipped: Bronze Layer - Transparency Integration Tests (AWS not configured)${NC}"
    fi
fi

# 8. Summary Report
echo ""
echo "============================================================"
echo "📊 TEST SUMMARY"
echo "============================================================"
echo -e "Total Test Suites: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
echo -e "${RED}Failed: $FAILED_TESTS${NC}"
echo "============================================================"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed! Ready for production.${NC}"
    exit 0
else
    echo -e "${RED}⚠️  Some tests failed. Review errors above.${NC}"
    exit 1
fi
