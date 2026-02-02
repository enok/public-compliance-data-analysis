#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "⚠️  Shutting down MBA Thesis Infrastructure..."
cd infra
terraform destroy -auto-approve
echo "🛑 Infrastructure is DOWN. Costs minimized."
