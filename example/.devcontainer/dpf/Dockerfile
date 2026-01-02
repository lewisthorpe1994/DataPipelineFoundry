FROM node:20-bullseye-slim AS node

FROM mcr.microsoft.com/devcontainers/rust:2-1-bullseye

WORKDIR /workspace
USER root

COPY example/.devcontainer/zscaler-root-ca.crt /usr/local/share/ca-certificates/zscaler-root-ca.crt

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ARG PROXY_CA_CERT_B64
ENV HTTP_PROXY=$(HTTP_PROXY)
ENV HTTPS_PROXY=$(HTTPS_PROXY)
ENV NO_PROXY=$(NO_PROXY)
ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/zscaler-root-ca.crt \
    NPM_CONFIG_CAFILE=/usr/local/share/ca-certificates/zscaler-root-ca.crt \
    NPM_CONFIG_STRICT_SSL=true

ENV UV_INSTALL_DIR=/usr/local/bin \
    UV_CACHE_DIR=/opt/uv-cache

# Install Node.js 20 without using apt repos (proxy-friendly).
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules

# Recreate npm/npx entrypoints so their relative imports resolve correctly.
RUN set -eux; \
    ln -sf ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm; \
    ln -sf ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

# System dependencies needed for crates that rely on OpenSSL and for HTTPS downloads.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev curl ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Optional: trust a corporate/MITM proxy CA during build.
# Provide a base64-encoded PEM cert via `PROXY_CA_CERT_B64`.
RUN set -eux; \
    if [ -n "${PROXY_CA_CERT_B64:-}" ]; then \
      echo "${PROXY_CA_CERT_B64}" | base64 -d >/usr/local/share/ca-certificates/proxy-ca.crt; \
      update-ca-certificates; \
    fi

RUN set -eux; node --version; npm --version; npx --version


# Copy manifests first to leverage Docker layer caching when dependencies do not change
COPY Cargo.toml Cargo.lock ./

# Bring in the workspace source needed for building dpf
COPY dpf_core ./dpf_core
COPY dpf_python ./dpf_python
COPY dpf_web ./dpf_web


# Mirror `.github/workflows/release.yml` build sequence:
# 1) Build frontend bundle (npm ci + build)
# 2) Build CLI binary (cargo build --locked --release -p dpf)
RUN set -eux; \
    cd dpf_web/ui; \
    npm ci; \
    npm run build; \
    node -e "require('fs').writeFileSync('.npm-install.stamp','npm ci completed')"

RUN set -eux; \
    cargo build --locked --release -p dpf; \
    install -m 0755 target/release/dpf /usr/local/bin/dpf; \
    mkdir -p /usr/local/bin/web-ui-dist; \
    cp -R dpf_web/ui/dist/. /usr/local/bin/web-ui-dist/

ENV PATH="/usr/local/bin:${PATH}"

USER vscode

CMD ["/bin/bash"]
