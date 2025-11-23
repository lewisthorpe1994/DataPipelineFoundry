FROM mcr.microsoft.com/devcontainers/rust:2-1-bullseye

WORKDIR /workspace
USER root

ENV UV_INSTALL_DIR=/usr/local/bin \
    UV_CACHE_DIR=/opt/uv-cache

# System dependencies needed for crates that rely on OpenSSL and the web UI build
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv globally and a shared Python 3.13 runtime for Python workflows
RUN set -eux; \
    mkdir -p "${UV_CACHE_DIR}"; \
    curl -LsSf https://astral.sh/uv/install.sh | sh; \
    uv --version; \
    UV_CACHE_DIR="${UV_CACHE_DIR}" uv python install 3.13; \
    ln -sf "$(UV_CACHE_DIR="${UV_CACHE_DIR}" uv python find 3.13)" /usr/local/bin/python3.13; \
    chown -R vscode:vscode "${UV_CACHE_DIR}"

# Copy manifests first to leverage Docker layer caching when dependencies do not change
COPY Cargo.toml Cargo.lock ./

# Bring in the workspace source needed for building dpf
COPY dpf_core ./dpf_core
COPY dpf_python ./dpf_python
COPY foundry_web ./foundry_web

# Install UI dependencies so `npm run` commands used by the CLI succeed
#RUN cd foundry_web/ui && npm install

# Build and install the CLI during image build so it can be reused at runtime
RUN CARGO_TARGET_DIR=/tmp/devcontainer-target cargo install --path dpf_core/cli --locked --root /usr/local

ENV PATH="/usr/local/bin:${PATH}"

USER vscode

CMD ["/bin/bash"]
