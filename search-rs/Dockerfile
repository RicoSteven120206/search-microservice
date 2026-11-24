FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.loc[k] ./
COPY src ./src
RUN cargo build --release 

FROM debian:bookworm-slim
COPY --from=build /src/target/release/search-rs /app
ENTRYPOINT ["/app"]