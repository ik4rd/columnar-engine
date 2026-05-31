#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <input.csv> <output.columnar>" >&2
  exit 1
fi

input_csv="$1"
columnar_path="$2"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"
build_dir="${BUILD_DIR:-build}"
row_group_size="${ROW_GROUP_SIZE:-50000}"
compression="${COLUMNAR_COMPRESSION:-lz4}"
schema_path="${COLUMNAR_SCHEMA:-${repo_dir}/benchmarks/scheme.csv}"
binary="${repo_dir}/${build_dir}/columnar_engine"

if [[ ! -x "${binary}" ]]; then
  binary="${repo_dir}/${build_dir}/src/columnar_engine"
fi

if [[ ! -x "${binary}" ]]; then
  echo "columnar_engine binary not found; run ./script/build.sh first." >&2
  exit 1
fi

if [[ ! -f "${schema_path}" ]]; then
  echo "schema file not found: ${schema_path}" >&2
  exit 1
fi

mkdir -p "$(dirname "${columnar_path}")"

"${binary}" convert \
  --schema "${schema_path}" \
  --input "${input_csv}" \
  --output "${columnar_path}" \
  --row-group-size "${row_group_size}" \
  --compression "${compression}"
