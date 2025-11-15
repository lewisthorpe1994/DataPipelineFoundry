FROM mcr.microsoft.com/devcontainers/rust:2-1-bullseye

WORKDIR /workspace

# System dependencies needed for crates that rely on OpenSSL and the web UI build
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev nodejs npm \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first to leverage Docker layer caching when dependencies do not change
COPY Cargo.toml Cargo.lock ./

# Bring in the workspace source needed for building dpf
COPY crates ./crates
COPY foundry_web ./foundry_web

# Install UI dependencies so `npm run` commands used by the CLI succeed
RUN cd foundry_web/ui && npm install

# Build and install the CLI during image build so it can be reused at runtime
RUN CARGO_TARGET_DIR=/tmp/devcontainer-target cargo install --path crates/cli --locked --root /usr/local

ENV PATH="/usr/local/bin:${PATH}"

CMD ["/bin/bash"]
