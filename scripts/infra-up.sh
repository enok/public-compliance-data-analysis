#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "🚀 Starting MBA Thesis Infrastructure..."
cd infra
terraform init
terraform apply -auto-approve
echo "✅ Infrastructure is UP. S3 Layers initialized."
