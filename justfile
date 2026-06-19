# Development tasks. `just` (https://github.com/casey/just) is optional sugar —
# every recipe below is a plain command you can also run by hand.

# Local Worklane broker endpoints for `--broker postgres|redis`, matching
# docker-compose.yml's host ports (shifted off the defaults to avoid colliding with
# a local Postgres/Redis). Single source of truth: change a port here and in
# docker-compose.yml together.
pg_url := "postgres://triggerlane:triggerlane@localhost:55432/triggerlane"
redis_url := "redis://localhost:56379"

# List recipes.
default:
    @just --list

# Start the local Postgres + Redis broker services and wait until healthy.
up:
    docker compose up -d --wait

# Stop and remove the broker services (and their volumes).
down:
    docker compose down -v

# Run the test suite (in-memory; needs no live services).
test:
    cargo test --workspace

# Format + lint gate (mirrors the CI `lint` job).
lint:
    cargo fmt --all --check
    cargo clippy --all-targets -- -D warnings

# Apply formatting.
fmt:
    cargo fmt --all

# Supply-chain gate (mirrors the CI `deny` job): RustSec advisories, licenses,
# banned/wildcard deps, source allow-listing. Needs `cargo install cargo-deny`.
audit:
    cargo deny check advisories bans licenses sources

# Serve against the local Postgres broker (after `just up`). A Worklane worker must
# run against the same broker to execute the jobs Triggerlane enqueues.
serve-postgres addr="127.0.0.1:8080":
    cargo run -p triggerlane-cli -- --broker postgres --url "{{pg_url}}" serve {{addr}}
