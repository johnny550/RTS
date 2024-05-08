#!/bin/bash

cd infra && terraform init
terraform plan -out plan_out
echo "Applying infra as code..."
terraform apply -auto-approve plan_out
