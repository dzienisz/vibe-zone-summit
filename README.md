# Vibe Zone · The Last Summit — Rust + SQLite

Mini social API for DevFest Warsaw 2026 Vibe Zone (`GET /health`, `GET /feed`, `POST /posts`, `POST /like`),
following the contract in [bsisduck/vibe-zone docs/API.md](https://github.com/bsisduck/vibe-zone/blob/main/docs/API.md).

- Rust, hyper (HTTP/1.1), single-threaded Tokio runtime for 1 vCPU.
- SQLite (WAL) is the source of truth; one writer thread group-commits batches before acknowledging.
- Feed JSON and like counters are kept in memory and rebuilt after each committed batch, so reads never hit disk.
- Static musl binary in a `scratch` image (~1 MB), UID 10001, data in `$DATA_DIR` (`/data`).

```sh
docker buildx build --platform linux/amd64 --load -t vibezone-rust:local .
docker run --rm -p 8080:8080 -v vz-data:/data vibezone-rust:local
```
