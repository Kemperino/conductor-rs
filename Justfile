image := env_var_or_default("CONDUCTOR_RS_IMAGE_TAG", "conductor-rs:local")

default: verify

verify: fmt test clippy docker-smoke

fmt:
    cargo fmt --check

test:
    cargo test

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

docker-smoke:
    @if ! command -v docker >/dev/null 2>&1; then echo "Skipping Docker image check because docker is not on PATH."; exit 0; fi
    docker build -t "{{image}}" .
    docker run --rm "{{image}}" op-conductor --help
    docker run --rm "{{image}}" sh -c 'command -v op-conductor && command -v nc && command -v awk && command -v sh && command -v sleep && command -v ls'
