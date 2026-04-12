#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
proto_dir="${repo_root}/proto"
out_dir="${repo_root}/internal/proto"

if ! command -v protoc >/dev/null 2>&1; then
	echo "protoc is required but was not found in PATH" >&2
	exit 1
fi

if ! command -v protoc-gen-go >/dev/null 2>&1; then
	echo "protoc-gen-go is required but was not found in PATH" >&2
	echo "Install it with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" >&2
	exit 1
fi

mapfile -t proto_files < <(find "${proto_dir}" -maxdepth 1 -name '*.proto' -printf '%f\n' | sort)
if [[ ${#proto_files[@]} -eq 0 ]]; then
	echo "no .proto files found in ${proto_dir}" >&2
	exit 1
fi

mkdir -p "${out_dir}"

(
	cd "${proto_dir}"
	protoc \
		--go_out="${out_dir}" \
		--go_opt=paths=source_relative \
		"${proto_files[@]}"
)

echo "generated ${#proto_files[@]} proto file(s) into ${out_dir}"
