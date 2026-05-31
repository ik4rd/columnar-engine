#!/usr/bin/env bash

: "${REPO_URL:=https://github.com/ik4rd/columnar-engine}"
: "${BRANCH:=main}"
: "${INPUT_CSV:=/bench/sample_input.csv}"
: "${COLUMNAR:=/work/results/output.columnar}"
: "${RESULTS:=/work/results}"
: "${COLUMNAR_COMPRESSION:=lz4}"
: "${QUERY_MODE:=hardcoded}"

export REPO_URL
export BRANCH
export INPUT_CSV
export COLUMNAR
export RESULTS
export COLUMNAR_COMPRESSION
export QUERY_MODE
