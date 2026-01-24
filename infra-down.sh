#!/bin/bash
echo "⚠️  Shutting down MBA Thesis Infrastructure..."
cd infra
terraform destroy -auto-approve
echo "🛑 Infrastructure is DOWN. Costs minimized."