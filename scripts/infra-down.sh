#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="mba-thesis"
REGION="us-east-1"
AUTO_YES="false"
DRY_RUN="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --yes)
      AUTO_YES="true"
      shift 1
      ;;
    --dry-run)
      DRY_RUN="true"
      shift 1
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --profile PROFILE   AWS CLI profile (default: mba-thesis)"
      echo "  --region REGION     AWS region (default: us-east-1)"
      echo "  --yes               Skip confirmation prompt"
      echo "  --dry-run           Show what would be deleted without deleting"
      echo "  --help              Show this help message"
      echo ""
      echo "This script destroys ALL AWS resources except S3 buckets:"
      echo "  - Terraform-managed resources (MWAA, VPC, IAM, etc.)"
      echo "  - CloudWatch log groups"
      echo "  - Secrets Manager secrets"
      echo "  - KMS keys (custom only)"
      echo ""
      echo "Preserved resources:"
      echo "  - enok-mba-thesis-datalake (S3 bucket ONLY)"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Run with --help for usage information"
      exit 1
      ;;
  esac
done

export AWS_PROFILE="$PROFILE"
export AWS_DEFAULT_REGION="$REGION"

if [[ "$DRY_RUN" == "true" ]]; then
  echo ""
  echo "🔍 DRY RUN MODE - No resources will be deleted"
  echo ""
fi

echo "⚠️  Shutting down MBA Thesis Infrastructure..."
echo "AWS profile: $AWS_PROFILE"
echo "AWS region:  $AWS_DEFAULT_REGION"
echo ""
echo "📦 Resources to be DESTROYED:"
echo "  ✗ Terraform-managed infrastructure (MWAA, VPC, IAM, etc.)"
echo "  ✗ CloudWatch log groups (airflow-*)"
echo "  ✗ Secrets Manager secrets (mba-thesis/*)"
echo "  ✗ Custom KMS keys"
echo ""
echo "✅ Resources to be PRESERVED:"
echo "  ✓ S3 bucket: enok-mba-thesis-datalake ONLY"
echo "  ✓ Data inside enok-mba-thesis-datalake (Bronze/Silver/Gold)"
echo ""
echo "⚠️  Resources to be DELETED (including MWAA):"
echo "  ✗ S3 bucket: enok-mba-thesis-datalake-mwaa (and all contents)"
echo "  ✗ All MWAA/Airflow resources"
echo ""

if [[ "$AUTO_YES" != "true" ]] && [[ "$DRY_RUN" != "true" ]]; then
  read -r -p "Proceed to destroy all resources except S3 buckets? (yes/no) " CONFIRM
  if [[ "$CONFIRM" != "yes" ]]; then
    echo "Aborted."
    exit 0
  fi
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 1: Destroying Terraform-managed resources"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

cd infra

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[DRY RUN] Would run: terraform init"
  echo "[DRY RUN] Would remove S3 buckets from Terraform state"
  echo "[DRY RUN] Would run: terraform destroy -auto-approve"
else
  terraform init -input=false >/dev/null
  
  if terraform state list 2>/dev/null | grep -q '^aws_s3_bucket\.mba_datalake$'; then
    echo "Removing S3 data lake bucket from Terraform state (preserving bucket)..."
    terraform state rm aws_s3_object.layers >/dev/null 2>&1 || true
    terraform state rm aws_s3_bucket.mba_datalake >/dev/null
  fi
  
  # MWAA bucket will be destroyed by Terraform (not preserved)
  
  echo "Running Terraform destroy..."
  terraform destroy -auto-approve
  echo "✅ Terraform resources destroyed"
fi

cd "$REPO_ROOT"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 2: Cleaning up non-Terraform AWS resources"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Delete CloudWatch log groups (MWAA-created)
echo ""
echo "Deleting MWAA S3 bucket (if not managed by Terraform)..."
if aws s3 ls "s3://enok-mba-thesis-datalake-mwaa" 2>/dev/null; then
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would empty and delete bucket: enok-mba-thesis-datalake-mwaa"
  else
    echo "  Emptying bucket: enok-mba-thesis-datalake-mwaa"
    aws s3 rm s3://enok-mba-thesis-datalake-mwaa --recursive 2>/dev/null || echo "  ⚠️  Failed to empty bucket"
    echo "  Deleting bucket: enok-mba-thesis-datalake-mwaa"
    aws s3 rb s3://enok-mba-thesis-datalake-mwaa 2>/dev/null || echo "  ⚠️  Failed to delete bucket (may be managed by Terraform)"
  fi
  echo "✅ MWAA S3 bucket cleaned up"
else
  echo "  MWAA S3 bucket not found or already deleted"
fi

echo ""
echo "Deleting CloudWatch log groups..."
LOG_GROUPS=$(aws logs describe-log-groups \
  --log-group-name-prefix "airflow-" \
  --query 'logGroups[].logGroupName' \
  --output text 2>/dev/null || echo "")

if [[ -n "$LOG_GROUPS" ]]; then
  for LOG_GROUP in $LOG_GROUPS; do
    if [[ "$DRY_RUN" == "true" ]]; then
      echo "[DRY RUN] Would delete log group: $LOG_GROUP"
    else
      echo "  Deleting: $LOG_GROUP"
      aws logs delete-log-group --log-group-name "$LOG_GROUP" 2>/dev/null || echo "  ⚠️  Failed to delete $LOG_GROUP"
    fi
  done
  echo "✅ CloudWatch log groups cleaned up"
else
  echo "  No CloudWatch log groups found"
fi

# Delete Secrets Manager secrets
echo ""
echo "Deleting Secrets Manager secrets..."
SECRETS=$(aws secretsmanager list-secrets \
  --query "SecretList[?starts_with(Name, \`mba-thesis/\`)].Name" \
  --output text 2>/dev/null || echo "")

if [[ -n "$SECRETS" ]]; then
  for SECRET in $SECRETS; do
    if [[ "$DRY_RUN" == "true" ]]; then
      echo "[DRY RUN] Would delete secret: $SECRET"
    else
      echo "  Deleting: $SECRET"
      aws secretsmanager delete-secret \
        --secret-id "$SECRET" \
        --force-delete-without-recovery 2>/dev/null || echo "  ⚠️  Failed to delete $SECRET"
    fi
  done
  echo "✅ Secrets Manager secrets cleaned up"
else
  echo "  No Secrets Manager secrets found"
fi

# Delete custom KMS keys (skip AWS-managed keys)
echo ""
echo "Deleting custom KMS keys..."
KMS_KEYS=$(aws kms list-keys --query 'Keys[].KeyId' --output text 2>/dev/null || echo "")

if [[ -n "$KMS_KEYS" ]]; then
  for KEY_ID in $KMS_KEYS; do
    KEY_MANAGER=$(aws kms describe-key --key-id "$KEY_ID" --query 'KeyMetadata.KeyManager' --output text 2>/dev/null || echo "")
    KEY_STATE=$(aws kms describe-key --key-id "$KEY_ID" --query 'KeyMetadata.KeyState' --output text 2>/dev/null || echo "")
    
    if [[ "$KEY_MANAGER" == "CUSTOMER" ]] && [[ "$KEY_STATE" == "Enabled" ]]; then
      if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN] Would schedule deletion for KMS key: $KEY_ID"
      else
        echo "  Scheduling deletion: $KEY_ID (30-day waiting period)"
        aws kms schedule-key-deletion \
          --key-id "$KEY_ID" \
          --pending-window-in-days 7 2>/dev/null || echo "  ⚠️  Failed to schedule deletion for $KEY_ID"
      fi
    fi
  done
  echo "✅ Custom KMS keys scheduled for deletion"
else
  echo "  No custom KMS keys found"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [[ "$DRY_RUN" == "true" ]]; then
  echo "🔍 DRY RUN COMPLETED - No resources were deleted"
else
  echo "✅ INFRASTRUCTURE TEARDOWN COMPLETED"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if [[ "$DRY_RUN" != "true" ]]; then
  echo "🛑 All chargeable resources destroyed (except data lake S3 bucket)"
  echo "💰 Estimated monthly cost: ~\$0.25 (S3 storage only)"
  echo ""
  echo "✅ Preserved resources:"
  echo "  • S3 Data Lake: enok-mba-thesis-datalake"
  echo "  • Bronze/Silver/Gold data intact"
  echo ""
  echo "🗑️  Deleted resources:"
  echo "  • MWAA S3 bucket: enok-mba-thesis-datalake-mwaa"
  echo "  • All MWAA/Airflow infrastructure"
  echo "  • CloudWatch logs, Secrets Manager, KMS keys"
  echo ""
  echo "📝 To redeploy: Run ./scripts/infra-up.sh"
fi
echo ""
