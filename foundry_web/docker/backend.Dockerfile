FROM rust:1.82-bullseye

ENV PATH="/usr/local/cargo/bin:/usr/local/rustup/bin:${PATH}"

# Install system deps and cargo-watch for live reload
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-watch

WORKDIR /workspace/FlowFoundry

# Cache dependencies (optional but speeds up first run)
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY foundry_web ./foundry_web
RUN cargo fetch

CMD ["/usr/local/cargo/bin/cargo", "watch", "-x", "run -p foundry_backend -- --manifest example/foundry-project/.compiled/manifest.json --addr 0.0.0.0:8085"]
