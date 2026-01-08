#!/usr/bin/env bash
set -euo pipefail

APT_PACKAGES_FILE=".github/apt-packages.txt"
APT_CACHE_DIR="${HOME}/.cache/apt"
APT_ARCHIVES_DIR="${APT_CACHE_DIR}/archives"
APT_PARTIAL_DIR="${APT_ARCHIVES_DIR}/partial"

missing=0
while read -r pkg; do
  [[ -z "${pkg}" ]] && continue
  dpkg -s "${pkg}" >/dev/null 2>&1 || missing=1
done < "${APT_PACKAGES_FILE}"

if [[ "${missing}" -eq 0 ]]; then
  echo "All dependencies already installed — skipping apt install."
  exit 0
fi

sudo apt-get update
mkdir -p "${APT_PARTIAL_DIR}"

sudo DEBIAN_FRONTEND=noninteractive apt-get \
  -o Dir::Cache::archives="${APT_ARCHIVES_DIR}" \
  -o Dir::Cache::archives::partial="${APT_PARTIAL_DIR}" \
  install -y --no-install-recommends $(tr '\n' ' ' < "${APT_PACKAGES_FILE}")
