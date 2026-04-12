#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/env.sh"

required_vars=(
  REPO_URL
  BRANCH
  INPUT_CSV
  COLUMNAR
  RESULTS
)

for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "Missing required environment variable: ${var_name}" >&2
    exit 1
  fi
done

git clone --branch "${BRANCH}" "${REPO_URL}" repo
cd repo

mkdir -p "$(dirname "${COLUMNAR}")" "${RESULTS}"

./script/setup.sh
./script/build.sh
./script/convert.sh "${INPUT_CSV}" "${COLUMNAR}"

for query_num in {0..42}; do
  output="${RESULTS}/query_${query_num}.csv"
  logs="${RESULTS}/query_${query_num}.log"
  ./script/run_query.sh "${query_num}" "${COLUMNAR}" "${output}" "${logs}"
done
