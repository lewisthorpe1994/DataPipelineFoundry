#!/usr/bin/env bash
set -euo pipefail

# JetBrains tools (RustRover) often default to a rustup-style per-user toolchain at:
#   /home/vscode/.cargo/bin/cargo
# The devcontainers Rust images place Rust in /usr/local/cargo/bin instead.
#
# Create stable links so the IDE toolchain path is valid without changing user settings.

home_dir="${HOME:-/home/vscode}"
cargo_home="${CARGO_HOME:-${home_dir}/.cargo}"
mkdir -p "${cargo_home}/bin"

if [[ -d /usr/local/cargo/bin ]]; then
  src_dir="$(cd /usr/local/cargo/bin && pwd -P)"
  dst_dir="$(cd "${cargo_home}/bin" && pwd -P)"

  # If CARGO_HOME already points at /usr/local/cargo (common in Rust devcontainer images),
  # then linking would try to overwrite files with themselves and fail under `set -e`.
  if [[ "${src_dir}" != "${dst_dir}" ]]; then
    for tool in /usr/local/cargo/bin/*; do
      name="$(basename "$tool")"
      ln -sf "/usr/local/cargo/bin/$name" "${cargo_home}/bin/$name"
    done
  fi
fi

if command -v cargo >/dev/null 2>&1; then
  cargo --version
fi
