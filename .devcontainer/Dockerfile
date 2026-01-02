FROM mcr.microsoft.com/devcontainers/rust:2-1-bullseye

WORKDIR /workspace
USER root

COPY zscaler-root-ca.crt /usr/local/share/ca-certificates/zscaler-root-ca.crt


ENV CARGO_HOME=/home/vscode/.cargo \
    CARGO_TARGET_DIR=/home/vscode/.cargo/target

ENV UV_INSTALL_DIR=/usr/local/bin \
    UV_CACHE_DIR=/opt/uv-cache

# Ensure shells/IDEs consistently see a writable Cargo home/target dir.
RUN set -eux; \
    printf '%s\n' \
      'export CARGO_HOME="${CARGO_HOME:-/home/vscode/.cargo}"' \
      'export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/vscode/.cargo/target}"' \
      > /etc/profile.d/10-cargo-home.sh

# System dependencies needed for crates that rely on OpenSSL and the web UI build
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev curl ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV UV_NATIVE_TLS=true

# Install uv globally and a shared Python 3.13 runtime for Python workflows
RUN set -eux; \
    mkdir -p "${UV_CACHE_DIR}"; \
    curl -LsSf https://astral.sh/uv/install.sh | sh; \
    uv --version; \
    UV_CACHE_DIR="${UV_CACHE_DIR}" uv python install 3.13; \
    ln -sf "$(UV_CACHE_DIR="${UV_CACHE_DIR}" uv python find 3.13)" /usr/local/bin/python3.13; \
    chown -R vscode:vscode "${UV_CACHE_DIR}"

ENV PATH="/usr/local/bin:${PATH}"

# The base devcontainers image sets `CARGO_HOME=/usr/local/cargo` in `/etc/bash.bashrc`,
# which causes permission issues for the `vscode` user. Respect the environment value
# passed in (from Docker/Compose/devcontainer.json) instead of clobbering it.
RUN set -eux; \
    sed -i 's|^export CARGO_HOME="/usr/local/cargo"$|export CARGO_HOME="${CARGO_HOME:-/home/vscode/.cargo}"|g' /etc/bash.bashrc; \
    mkdir -p "${CARGO_HOME}"; \
    chown -R vscode:vscode "${CARGO_HOME}"; \
    mkdir -p "${CARGO_TARGET_DIR}"; \
    chown -R vscode:vscode "${CARGO_TARGET_DIR}"; \
    if [[ -d /usr/local/cargo/registry ]]; then chgrp -R rustlang /usr/local/cargo/registry && chmod -R g+rwX /usr/local/cargo/registry; fi; \
    if [[ -d /usr/local/cargo/git ]]; then chgrp -R rustlang /usr/local/cargo/git && chmod -R g+rwX /usr/local/cargo/git; fi

ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt \
    NPM_CONFIG_CAFILE=/usr/local/share/ca-certificates/zscaler-root-ca.crt \
    NPM_CONFIG_STRICT_SSL=true

USER vscode

CMD ["/bin/bash"]
