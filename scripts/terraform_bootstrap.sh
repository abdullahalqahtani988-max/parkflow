#!/usr/bin/env bash
# Creates the remote-state bucket once (versioned), then runs terraform init + plan.
#   bash scripts/terraform_bootstrap.sh
set -euo pipefail
PROJECT=${PROJECT:-parkflow-514314}
REGION=${REGION:-us-central1}
STATE="gs://${PROJECT}-tfstate"
gcloud storage buckets describe "$STATE" >/dev/null 2>&1 || \
  gcloud storage buckets create "$STATE" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update "$STATE" --versioning >/dev/null
cd infra
terraform init -input=false
terraform fmt -check || terraform fmt
terraform validate
terraform plan -input=false -out=tfplan
