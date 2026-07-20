#!/usr/bin/env bash
# Regenerates the generated-code artifacts committed to the repo for every
# .proto file that has a non-Rust consumer.
#
# Rust (catalog.proto, format.proto, worker.proto) needs no step here — each
# crate's build.rs runs prost/tonic-build automatically on every `cargo
# build`/`cargo test`. plan.proto is deliberately never compiled at all (see
# its own header comment) — every consumer hand-writes a mirror of its shape
# instead.
#
# Go and Python codegen is not wired into any build step (no Makefile/buf
# config existed before this script), so run this manually after editing
# catalog.proto, worker.proto, or ai.proto, and commit the regenerated
# output alongside your .proto change, exactly like the existing *.pb.go
# files are committed.
#
# Requires: protoc (vendored at .tools/protoc/bin, matching the version the
# currently-committed .pb.go files were generated with), protoc-gen-go +
# protoc-gen-go-grpc on $PATH (`go install
# google.golang.org/protobuf/cmd/protoc-gen-go@latest` /
# `google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`), and grpcio-tools
# on $PATH for the Python step (`uv run` picks it up from ai-service's dev
# dependencies — run this script from the repo root either way).

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

PROTOC="${PROTOC:-$repo_root/.tools/protoc/bin/protoc}"
if [ ! -x "$PROTOC" ]; then
  PROTOC=protoc
fi

echo "== Go: catalog.proto, worker.proto, ai.proto =="
# --go_out/--go-grpc_out in "import path" mode (the default) place output at
# <go_out>/<go_package import path>/*.pb.go — since each proto's go_package
# is "atlas/coordinator/internal/<x>pb;<x>pb" and this repo's own root
# directory is named "atlas", passing the *parent* of the repo root as
# go_out resolves back onto the real coordinator/internal/<x>pb directories.
"$PROTOC" --proto_path=proto --go_out=.. --go-grpc_out=.. \
  proto/catalog.proto proto/worker.proto proto/ai.proto

echo "== Python: ai.proto (grpcio-tools) =="
( cd ai-service && uv run python -m grpc_tools.protoc \
    -I ../proto \
    --python_out=atlas_ai/pb \
    --grpc_python_out=atlas_ai/pb \
    --pyi_out=atlas_ai/pb \
    ../proto/ai.proto )
# grpc_tools always emits a bare `import ai_pb2 as ai__pb2` in the _grpc.py
# file, regardless of the output directory being a package — rewrite it to a
# package-relative import so `from atlas_ai.pb import ai_pb2_grpc` works
# without needing atlas_ai/pb on sys.path directly.
sed -i 's/^import ai_pb2 as ai__pb2$/from . import ai_pb2 as ai__pb2/' \
  ai-service/atlas_ai/pb/ai_pb2_grpc.py

echo "done."
