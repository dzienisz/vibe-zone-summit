FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev gcc
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src target/release/deps/vibezone*
COPY src ./src
RUN cargo build --release && mkdir -p /out/data && cp target/release/vibezone-api /out/vibezone-api

FROM scratch
COPY --from=build /out/vibezone-api /vibezone-api
COPY --from=build --chown=10001:10001 /out/data /data
ENV PORT=8080 DATA_DIR=/data TMPDIR=/data
USER 10001:10001
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/vibezone-api"]
