#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "Usage: $0 <query_num> <input.columnar> <output.csv> <log.txt>" >&2
  exit 1
fi

query_num="$1"
columnar_path="$2"
output_csv="$3"
log_path="$4"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"
build_dir="${BUILD_DIR:-build}"
binary="${repo_dir}/${build_dir}/columnar_engine"
query_file="${repo_dir}/benchmarks/queries/query_${query_num}.sql"

mkdir -p "$(dirname "${output_csv}")"
mkdir -p "$(dirname "${log_path}")"

: > "${output_csv}"

if [[ ! -f "${query_file}" ]]; then
  cat > "${log_path}" <<EOF
query_num=${query_num}
status=missing_query_definition
columnar_path=${columnar_path}
query_file=${query_file}
message=Add SQL text to benchmarks/queries/query_${query_num}.sql
EOF
  return 0 2>/dev/null
fi

if [[ ! -x "${binary}" ]]; then
  binary="${repo_dir}/${build_dir}/src/columnar_engine"
fi

if [[ ! -x "${binary}" ]]; then
  cat > "${log_path}" <<EOF
query_num=${query_num}
status=missing_binary
columnar_path=${columnar_path}
binary=${binary}
message=Run ./script/build.sh before executing benchmark queries.
EOF
  exit 1
fi

if "${binary}" run-query \
  --input "${columnar_path}" \
  --output "${output_csv}" \
  --table-name hits \
  --query-file "${query_file}" > "${log_path}" 2>&1; then
  printf 'query_num=%s\nstatus=ok\ncolumnar_path=%s\nquery_file=%s\n' \
    "${query_num}" "${columnar_path}" "${query_file}" >> "${log_path}"
else
  status=$?
  printf 'query_num=%s\nstatus=failed\ncolumnar_path=%s\nquery_file=%s\nexit_code=%s\n' \
    "${query_num}" "${columnar_path}" "${query_file}" "${status}" >> "${log_path}"
  exit "${status}"
fi
