#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <input.csv> <output.columnar>" >&2
  exit 1
fi

input_csv="$1"
columnar_path="$2"
build_dir="${BUILD_DIR:-build}"
row_group_size="${ROW_GROUP_SIZE:-50000}"
schema_path="${COLUMNAR_SCHEMA:-${columnar_path}.schema.csv}"
binary="./${build_dir}/columnar_engine"

if [[ ! -x "${binary}" ]]; then
  binary="./${build_dir}/src/columnar_engine"
fi

if [[ ! -x "${binary}" ]]; then
  echo "columnar_engine binary not found; run ./script/build.sh first." >&2
  exit 1
fi

mkdir -p "$(dirname "${columnar_path}")"
mkdir -p "$(dirname "${schema_path}")"

"${binary}" infer-schema \
  --input "${input_csv}" \
  --output "${schema_path}"

"${binary}" convert \
  --schema "${schema_path}" \
  --input "${input_csv}" \
  --output "${columnar_path}" \
  --row-group-size "${row_group_size}"
