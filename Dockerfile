# syntax=docker/dockerfile:1
# Dependencies are compiled in their own layer (cargo-chef), so rebuilds after a source change
# only recompile oxidle itself.
FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef --locked --version 0.1.78
WORKDIR /src

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY src src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY src src
RUN cargo build --release --locked --bin oxidle && cp target/release/oxidle /oxidle

# The dashboard: Inngest's dev-server UI (SSPL), built with bun from a pinned Inngest commit.
# Platform-independent, so it is built once on the build machine's own platform.
# Skip it with --build-arg UI=false.
FROM --platform=$BUILDPLATFORM oven/bun:1 AS ui
ARG UI=true
ARG INNGEST_REF
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY ui/INNGEST_REF ui/build-ui.sh /ui/
RUN mkdir -p /out && if [ "$UI" != "false" ]; then \
      git clone --filter=blob:none https://github.com/inngest/inngest.git /inngest \
      && git -C /inngest checkout -q "${INNGEST_REF:-$(cat /ui/INNGEST_REF)}" \
      && /ui/build-ui.sh /inngest /out/ui \
      && rm -rf /inngest; \
    else mkdir -p /out/ui; fi

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /data oxidle \
    && mkdir -p /data && chown oxidle /data
COPY --from=build /oxidle /usr/local/bin/oxidle
# Served automatically from the binary's directory; disable with --no-ui.
COPY --from=ui /out/ui /usr/local/bin/ui
USER oxidle
WORKDIR /data
VOLUME /data
EXPOSE 8288
HEALTHCHECK --interval=10s --timeout=6s --start-period=10s --retries=3 CMD ["oxidle", "healthcheck"]
ENTRYPOINT ["oxidle"]
CMD ["start"]
