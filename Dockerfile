# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY src src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked && cp target/release/oxidle /oxidle

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /oxidle /usr/local/bin/oxidle
# Optional dashboard: build with ./ui/build.sh, then mount or COPY ui/dist to /usr/local/bin/ui.
WORKDIR /data
EXPOSE 8288
ENTRYPOINT ["oxidle"]
CMD ["start"]
