FROM rust:1.85-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p cds-relay-server

FROM debian:bookworm-slim
COPY --from=build /build/target/release/cds-relay-server /usr/local/bin/cds-relay-server
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/cds-relay-server"]
