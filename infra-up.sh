#!/bin/bash
echo "🚀 Starting MBA Thesis Infrastructure..."
cd infra
terraform init
terraform apply -auto-approve
echo "✅ Infrastructure is UP. S3 Layers initialized."