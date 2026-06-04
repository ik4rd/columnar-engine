#!/usr/bin/env bash
set -euo pipefail

packages=(
  build-essential
  cmake
  ninja-build
  git
  ca-certificates
  liblz4-dev
)

if ! command -v apt-get >/dev/null 2>&1; then
  echo "apt-get not found; assuming build dependencies are already installed."
  return 0 2>/dev/null
fi

missing=()
for package in "${packages[@]}"; do
  if ! dpkg -s "${package}" >/dev/null 2>&1; then
    missing+=("${package}")
  fi
done

if ((${#missing[@]} == 0)); then
  echo "Build dependencies already installed."
  return 0 2>/dev/null
fi

if [[ "$(id -u)" -eq 0 ]]; then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${missing[@]}"
  rm -rf /var/lib/apt/lists/*
  return 0 2>/dev/null
fi

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${missing[@]}"
