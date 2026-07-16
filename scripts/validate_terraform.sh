#!/usr/bin/env bash
set -euo pipefail

echo "=========================================="
echo "Checking Terraform formatting..."
echo "=========================================="
terraform fmt -check -recursive terraform/

# Build map of tf modules, omitting any .terraform cache dirs
mapfile -t MODULE_DIRS < <(find terraform/ -name "*.tf" -not -path "*/.*" -exec dirname {} \; | sort -u)

if [ "${#MODULE_DIRS[@]}" -eq 0 ]; then
  echo "Error: No Terraform modules found under terraform/" >&2
  exit 1
fi

echo "Discovered ${#MODULE_DIRS[@]} Terraform module directory(ies)."

for dir in "${MODULE_DIRS[@]}"; do
  echo "=========================================="
  echo "Validating Terraform module in: $dir"
  echo "=========================================="
  terraform -chdir="$dir" init -backend=false -input=false
  terraform -chdir="$dir" validate
done
