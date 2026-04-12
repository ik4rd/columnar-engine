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

mkdir -p "$(dirname "${output_csv}")"
mkdir -p "$(dirname "${log_path}")"

: > "${output_csv}"

cat > "${log_path}" <<EOF
query_num=${query_num}
status=not_implemented
columnar_path=${columnar_path}
message=This repository currently implements CSV to columnar conversion only. Query execution placeholders are generated so the external harness completes without crashing.
EOF
