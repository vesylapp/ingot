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

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /data oxidle \
    && mkdir -p /data && chown oxidle /data
COPY --from=build /oxidle /usr/local/bin/oxidle
# Optional dashboard: build with ./ui/build.sh, then mount or COPY ui/dist to /usr/local/bin/ui.
USER oxidle
WORKDIR /data
VOLUME /data
EXPOSE 8288
ENTRYPOINT ["oxidle"]
CMD ["start"]
