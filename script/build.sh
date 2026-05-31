#!/usr/bin/env bash
set -euo pipefail

build_dir="${BUILD_DIR:-build}"
build_type="${BUILD_TYPE:-Release}"
enable_benchmarks="${ENABLE_BENCHMARKS:-ON}"
enable_tests="${ENABLE_TESTS:-OFF}"

cmake_args=(
  -S .
  -B "${build_dir}"
  -DCMAKE_BUILD_TYPE="${build_type}"
  -DENABLE_TESTS="${enable_tests}"
  -DENABLE_BENCHMARKS="${enable_benchmarks}"
)

if command -v ninja >/dev/null 2>&1; then
  cmake_args+=(-G Ninja)
fi

cmake "${cmake_args[@]}"

build_targets=(columnar_engine)
if [[ "${enable_benchmarks}" == "ON" ]]; then
  build_targets+=(benchmark_queries)
fi

cmake --build "${build_dir}" --target "${build_targets[@]}"

if [[ -x "${build_dir}/src/columnar_engine" ]]; then
  ln -sf "src/columnar_engine" "${build_dir}/columnar_engine"
fi
