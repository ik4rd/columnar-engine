#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/env.sh"

log() {
  printf '[%s] %s\n' "$(date -Iseconds)" "$*"
}

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

log "Cloning ${REPO_URL} (branch: ${BRANCH})"
git clone --branch "${BRANCH}" "${REPO_URL}" repo
cd repo

mkdir -p "$(dirname "${COLUMNAR}")" "${RESULTS}"

log "Installing dependencies"
./script/setup.sh

log "Building project"
./script/build.sh

log "Converting CSV ${INPUT_CSV} -> ${COLUMNAR}"
./script/convert.sh "${INPUT_CSV}" "${COLUMNAR}"

for query_num in {0..42}; do
  output="${RESULTS}/query_${query_num}.csv"
  logs="${RESULTS}/query_${query_num}.log"
  log "Generating placeholder output for query ${query_num}"
  ./script/run_query.sh "${query_num}" "${COLUMNAR}" "${output}" "${logs}"
done

log "Completed. Columnar file: ${COLUMNAR}; results dir: ${RESULTS}"
