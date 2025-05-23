#!/bin/sh
set -e

(
  cd "$(dirname "$0")" 
  cargo build --release --target-dir=/tmp/sequel --manifest-path Cargo.toml
)

exec /tmp/sequel/release/sequel "$@"
