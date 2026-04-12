#!/usr/bin/env bash

: "${REPO_URL:=https://github.com/ik4rd/columnar-engine}"
: "${BRANCH:=main}"
: "${INPUT_CSV:=/data/input.csv}"
: "${COLUMNAR:=/work/results/output.columnar}"
: "${RESULTS:=/work/results}"

export REPO_URL
export BRANCH
export INPUT_CSV
export COLUMNAR
export RESULTS
