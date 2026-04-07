#!/bin/bash
# =============================================================================
# Manual Pipeline Execution Script - Bronze → Silver → Gold
# =============================================================================
# Triggers Airflow DAGs sequentially via AWS MWAA CLI
# Waits for each layer to complete before starting the next
#
# Usage:
#   ./scripts/run_pipeline.sh [OPTIONS]
#
# Options:
#   --bronze-only     Run only Bronze layer
#   --silver-only     Run only Silver layer (requires Bronze data)
#   --gold-only       Run only Gold layer (requires Silver data)
#   --skip-bronze     Skip Bronze, run Silver and Gold
#   --skip-silver     Skip Silver, run Bronze and Gold
#
# Cost Control:
#   - Run manually when needed (no daily costs)
#   - MWAA charges only when environment is running
#   - Consider shutting down MWAA when not in use
#
# Author: Enok Antônio de Jesus
# Institution: USP/Esalq - MBA Data Science & Analytics
# =============================================================================

set -euo pipefail

# Create logs directory if it doesn't exist
LOG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../logs"
mkdir -p "$LOG_DIR"

# Generate timestamped log file
LOG_FILE="$LOG_DIR/run-pipeline-$(date +%Y%m%d-%H%M%S).log"

# Redirect all output to both console and log file
exec > >(tee -a "$LOG_FILE") 2>&1

echo "📝 Logging to: $LOG_FILE"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
MWAA_ENV_NAME="${MWAA_ENV_NAME:-mba-thesis-airflow-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Parse command line arguments
RUN_BRONZE=true
RUN_SILVER=true
RUN_GOLD=true

for arg in "$@"; do
    case $arg in
        --bronze-only)
            RUN_SILVER=false
            RUN_GOLD=false
            ;;
        --silver-only)
            RUN_BRONZE=false
            RUN_GOLD=false
            ;;
        --gold-only)
            RUN_BRONZE=false
            RUN_SILVER=false
            ;;
        --skip-bronze)
            RUN_BRONZE=false
            ;;
        --skip-silver)
            RUN_SILVER=false
            ;;
        --help)
            echo "Manual Pipeline Execution Script"
            echo ""
            echo "Usage: ./scripts/run_pipeline.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --bronze-only     Run only Bronze layer"
            echo "  --silver-only     Run only Silver layer"
            echo "  --gold-only       Run only Gold layer"
            echo "  --skip-bronze     Skip Bronze, run Silver and Gold"
            echo "  --skip-silver     Skip Silver, run Bronze and Gold"
            echo "  --help            Show this help message"
            exit 0
            ;;
    esac
done

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}🚀 MANUAL PIPELINE EXECUTION${NC}"
echo -e "${BLUE}============================================================${NC}"
echo -e "Project: Public Compliance Data Analysis"
echo -e "MWAA Environment: ${MWAA_ENV_NAME}"
echo -e "AWS Region: ${AWS_REGION}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}❌ Error: AWS credentials not configured${NC}"
    echo "   Run: aws configure"
    exit 1
fi

echo -e "${GREEN}✓ AWS credentials validated${NC}"

# Check MWAA environment exists
if ! aws mwaa get-environment --name "${MWAA_ENV_NAME}" --region "${AWS_REGION}" &> /dev/null; then
    echo -e "${RED}❌ Error: MWAA environment '${MWAA_ENV_NAME}' not found${NC}"
    echo "   Deploy infrastructure first: ./scripts/infra-up.sh"
    exit 1
fi

echo -e "${GREEN}✓ MWAA environment found${NC}"
echo ""

# Function to trigger DAG
trigger_dag() {
    local dag_id=$1
    local dag_name=$2
    
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Triggering: ${dag_name}${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Trigger DAG via AWS CLI
    local run_id
    run_id=$(date +%Y%m%d_%H%M%S)

    local token
    token="$(aws mwaa create-cli-token \
        --name "${MWAA_ENV_NAME}" \
        --region "${AWS_REGION}" \
        --query 'CliToken' \
        --output text)"

    local webserver_url
    webserver_url="$(aws mwaa get-environment \
        --name "${MWAA_ENV_NAME}" \
        --region "${AWS_REGION}" \
        --query 'Environment.WebserverUrl' \
        --output text)"

    local response_file
    response_file="$(mktemp "${TMPDIR:-/tmp}/dag-response.XXXXXX.json")"

    # Trigger DAG
    if curl -fsS -X POST \
        "https://${webserver_url}/api/v1/dags/${dag_id}/dagRuns" \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: application/json" \
        -d "{\"conf\":{}, \"dag_run_id\":\"manual_${run_id}\"}" \
        -o "${response_file}"; then
        echo -e "${GREEN}✓ DAG triggered successfully${NC}"
        echo -e "Run ID: manual_${run_id}"
        rm -f "${response_file}"
        return 0
    else
        echo -e "${RED}✗ Failed to trigger DAG${NC}"
        rm -f "${response_file}"
        return 1
    fi
}

# Function to wait for DAG completion
wait_for_dag() {
    local dag_id=$1
    local dag_name=$2
    local max_wait=$3
    
    echo ""
    echo -e "${YELLOW}⏳ Waiting for ${dag_name} to complete...${NC}"
    echo -e "${YELLOW}   Max wait time: ${max_wait} minutes${NC}"
    
    local elapsed=0
    local check_interval=30
    
    while [ $elapsed -lt $((max_wait * 60)) ]; do
        # Check DAG status via AWS CLI
        local token
        token="$(aws mwaa create-cli-token \
            --name "${MWAA_ENV_NAME}" \
            --region "${AWS_REGION}" \
            --query 'CliToken' \
            --output text)"

        local webserver_url
        webserver_url="$(aws mwaa get-environment \
            --name "${MWAA_ENV_NAME}" \
            --region "${AWS_REGION}" \
            --query 'Environment.WebserverUrl' \
            --output text)"
        
        # Get latest DAG run status
        local status
        status="$(curl -fsS \
            "https://${webserver_url}/api/v1/dags/${dag_id}/dagRuns?limit=1" \
            -H "Authorization: Bearer ${token}" \
            | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4 || true)"
        
        if [ "$status" = "success" ]; then
            echo -e "${GREEN}✅ ${dag_name} completed successfully${NC}"
            return 0
        elif [ "$status" = "failed" ]; then
            echo -e "${RED}❌ ${dag_name} failed${NC}"
            return 1
        fi
        
        echo -e "${YELLOW}   Status: ${status} | Elapsed: ${elapsed}s${NC}"
        sleep "$check_interval"
        elapsed=$((elapsed + check_interval))
    done
    
    echo -e "${RED}⏱️  Timeout: ${dag_name} did not complete in ${max_wait} minutes${NC}"
    return 1
}

# Execute pipeline
START_TIME=$(date +%s)

if [ "$RUN_BRONZE" = true ]; then
    if ! trigger_dag "bronze_layer_ingestion" "Bronze Layer Ingestion"; then
        echo -e "${RED}❌ Pipeline failed while triggering Bronze layer${NC}"
        exit 1
    fi
    if ! wait_for_dag "bronze_layer_ingestion" "Bronze Layer" 120; then
        echo -e "${RED}❌ Pipeline failed at Bronze layer${NC}"
        exit 1
    fi
    echo ""
fi

if [ "$RUN_SILVER" = true ]; then
    if ! trigger_dag "silver_layer_transformation" "Silver Layer Transformation"; then
        echo -e "${RED}❌ Pipeline failed while triggering Silver layer${NC}"
        exit 1
    fi
    if ! wait_for_dag "silver_layer_transformation" "Silver Layer" 90; then
        echo -e "${RED}❌ Pipeline failed at Silver layer${NC}"
        exit 1
    fi
    echo ""
fi

if [ "$RUN_GOLD" = true ]; then
    if ! trigger_dag "gold_layer_analytics" "Gold Layer Analytics"; then
        echo -e "${RED}❌ Pipeline failed while triggering Gold layer${NC}"
        exit 1
    fi
    if ! wait_for_dag "gold_layer_analytics" "Gold Layer" 60; then
        echo -e "${RED}❌ Pipeline failed at Gold layer${NC}"
        exit 1
    fi
    echo ""
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}✅ PIPELINE EXECUTION COMPLETED SUCCESSFULLY${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo -e "${GREEN}📝 Full execution log saved to:${NC}"
echo -e "   $LOG_FILE"
echo -e "Total Duration: ${MINUTES}m ${SECONDS}s"
echo ""

echo -e "${BLUE}📊 Results:${NC}"
if [ "$RUN_BRONZE" = true ]; then
    echo "  ✓ Bronze Layer: s3://enok-mba-thesis-datalake/bronze/"
fi
if [ "$RUN_SILVER" = true ]; then
    echo "  ✓ Silver Layer: s3://enok-mba-thesis-datalake/silver/"
fi
if [ "$RUN_GOLD" = true ]; then
    echo "  ✓ Gold Layer: s3://enok-mba-thesis-datalake/gold/"
fi
echo ""

echo -e "${YELLOW}💰 Cost Control Reminder:${NC}"
echo "  • MWAA charges while environment is running (~\$350/month)"
echo "  • Consider: ./scripts/infra-down.sh to stop all resources"
echo "  • Redeploy when needed: ./scripts/infra-up.sh"
echo ""

echo -e "${BLUE}============================================================${NC}"
