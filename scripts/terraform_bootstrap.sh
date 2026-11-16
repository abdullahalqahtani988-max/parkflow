#!/usr/bin/env bash
# Creates the remote-state bucket once (versioned), then runs terraform init + plan.
#   bash scripts/terraform_bootstrap.sh
set -euo pipefail
PROJECT=${PROJECT:-parkflow-514314}
REGION=${REGION:-us-central1}
STATE="gs://${PROJECT}-tfstate"
TF_VERSION=${TF_VERSION:-1.9.8}

# Cloud Shell may only have a placeholder `terraform` that prints install help and exits 0.
export PATH="$HOME/bin:$PATH"
if ! terraform version 2>/dev/null | grep -q '^Terraform v'; then
  echo "Installing Terraform $TF_VERSION into ~/bin"
  mkdir -p "$HOME/bin"
  curl -sSLo /tmp/tf.zip "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_amd64.zip"
  unzip -oq /tmp/tf.zip -d "$HOME/bin"
fi
terraform version | head -1
gcloud storage buckets describe "$STATE" >/dev/null 2>&1 || \
  gcloud storage buckets create "$STATE" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update "$STATE" --versioning >/dev/null
cd infra
terraform init -input=false
terraform fmt -check || terraform fmt
terraform validate
terraform plan -input=false -out=tfplan
