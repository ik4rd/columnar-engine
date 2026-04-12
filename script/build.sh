#!/usr/bin/env bash
set -euo pipefail

build_dir="${BUILD_DIR:-build}"
build_type="${BUILD_TYPE:-Release}"

cmake_args=(
  -S .
  -B "${build_dir}"
  -DCMAKE_BUILD_TYPE="${build_type}"
  -DENABLE_TESTS=OFF
  -DENABLE_BENCHMARKS=OFF
)

if command -v ninja >/dev/null 2>&1; then
  cmake_args+=(-G Ninja)
fi

cmake "${cmake_args[@]}"

cmake --build "${build_dir}" --target columnar_engine

if [[ -x "${build_dir}/src/columnar_engine" ]]; then
  ln -sf "src/columnar_engine" "${build_dir}/columnar_engine"
fi
