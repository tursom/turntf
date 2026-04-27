#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

require_command() {
	local name="$1"
	if ! command -v "${name}" >/dev/null 2>&1; then
		echo "required command not found: ${name}" >&2
		exit 1
	fi
}

json_get() {
	local path="$1"
	python3 -c '
import json
import sys

path = sys.argv[1].split(".")
value = json.load(sys.stdin)
for part in path:
    if isinstance(value, list):
        value = value[int(part)]
    else:
        value = value[part]
if isinstance(value, bool):
    print("true" if value else "false")
elif isinstance(value, (dict, list)):
    print(json.dumps(value, separators=(",", ":")))
else:
    print(value)
' "$path"
}

sqlite_node_id() {
	local db_path="$1"
	python3 - "$db_path" <<'PY'
import sqlite3
import sys

conn = sqlite3.connect(sys.argv[1])
try:
    row = conn.execute("SELECT value FROM schema_meta WHERE key = 'node_id'").fetchone()
    if not row or not row[0]:
        raise SystemExit(1)
    print(row[0])
finally:
    conn.close()
PY
}

wait_for_healthz() {
	local base_url="$1"
	local server_pid="$2"
	for _ in $(seq 1 100); do
		if ! kill -0 "${server_pid}" 2>/dev/null; then
			echo "turntf server exited before becoming healthy" >&2
			return 1
		fi
		if curl -fsS "${base_url}/healthz" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	echo "timed out waiting for ${base_url}/healthz" >&2
	return 1
}

wait_for_node_id() {
	local db_path="$1"
	for _ in $(seq 1 50); do
		if node_id="$(sqlite_node_id "${db_path}" 2>/dev/null)"; then
			printf '%s\n' "${node_id}"
			return 0
		fi
		sleep 0.2
	done
	echo "timed out waiting for node id in ${db_path}" >&2
	return 1
}

require_command curl
require_command go
require_command python3

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/turntf-smoke.XXXXXX")"
binary_path="${1:-}"
config_path="${tmpdir}/config.toml"
db_path="${tmpdir}/turntf.db"
server_log="${tmpdir}/server.log"
server_pid=""

if [[ -z "${GOCACHE:-}" ]]; then
	export GOCACHE="${tmpdir}/go-build-cache"
fi
mkdir -p "${GOCACHE}"

cleanup() {
	local status=$?
	if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" 2>/dev/null; then
		kill "${server_pid}" 2>/dev/null || true
		wait "${server_pid}" 2>/dev/null || true
	fi
	if [[ ${status} -ne 0 ]]; then
		echo "smoke test failed; temporary files kept at ${tmpdir}" >&2
		if [[ -f "${server_log}" ]]; then
			echo "--- server log ---" >&2
			sed -n '1,240p' "${server_log}" >&2
		fi
	elif [[ "${TURNTF_SMOKE_KEEP_TMP:-0}" != "1" ]]; then
		rm -rf "${tmpdir}"
	fi
}
trap cleanup EXIT

if [[ -z "${binary_path}" ]]; then
	binary_path="${tmpdir}/turntf"
	(
		cd "${repo_root}"
		go build -trimpath -o "${binary_path}" ./cmd/turntf
	)
fi

port="${TURNTF_SMOKE_PORT:-$((20000 + RANDOM % 20000))}"
base_url="http://127.0.0.1:${port}"

cat >"${config_path}" <<EOF
[services.http]
listen_addr = "127.0.0.1:${port}"

[store.sqlite]
db_path = "${db_path}"

[auth]
token_secret = "smoke-token-secret"

[auth.bootstrap_admin]
username = "root"
password_hash = "\$2a\$10\$1gGoT/pdOu8vX1W28skBPOB7ICjISmVgt9lMyZf9c6re6cMHU6mAa"
EOF

"${binary_path}" serve --config "${config_path}" >"${server_log}" 2>&1 &
server_pid="$!"

wait_for_healthz "${base_url}" "${server_pid}"
node_id="$(wait_for_node_id "${db_path}")"
echo "smoke server is healthy on ${base_url} with node_id=${node_id}"

login_response="$(
	curl -fsS \
		-X POST "${base_url}/auth/login" \
		-H 'Content-Type: application/json' \
		--data "{\"node_id\":${node_id},\"user_id\":1,\"password\":\"root\"}"
)"
admin_token="$(printf '%s' "${login_response}" | json_get "token")"

ops_response="$(
	curl -fsS \
		-H "Authorization: Bearer ${admin_token}" \
		"${base_url}/ops/status"
)"
ops_node_id="$(printf '%s' "${ops_response}" | json_get "node_id")"
if [[ "${ops_node_id}" != "${node_id}" ]]; then
	echo "ops status returned unexpected node id: got=${ops_node_id} want=${node_id}" >&2
	exit 1
fi

metrics_response="$(
	curl -fsS \
		-H "Authorization: Bearer ${admin_token}" \
		"${base_url}/metrics"
)"
if [[ "${metrics_response}" != *'notifier_event_log_last_sequence'* ]]; then
	echo "metrics response missing notifier_event_log_last_sequence" >&2
	exit 1
fi

smoke_username="smoke-user"
smoke_password="smoke-password"
create_user_response="$(
	curl -fsS \
		-X POST "${base_url}/users" \
		-H 'Content-Type: application/json' \
		-H "Authorization: Bearer ${admin_token}" \
		--data "{\"username\":\"${smoke_username}\",\"password\":\"${smoke_password}\"}"
)"
smoke_user_id="$(printf '%s' "${create_user_response}" | json_get "user_id")"
smoke_user_node_id="$(printf '%s' "${create_user_response}" | json_get "node_id")"

get_user_response="$(
	curl -fsS \
		-H "Authorization: Bearer ${admin_token}" \
		"${base_url}/nodes/${smoke_user_node_id}/users/${smoke_user_id}"
)"
if [[ "$(printf '%s' "${get_user_response}" | json_get "username")" != "${smoke_username}" ]]; then
	echo "created user lookup returned unexpected username" >&2
	exit 1
fi

user_login_response="$(
	curl -fsS \
		-X POST "${base_url}/auth/login" \
		-H 'Content-Type: application/json' \
		--data "{\"node_id\":${smoke_user_node_id},\"user_id\":${smoke_user_id},\"password\":\"${smoke_password}\"}"
)"
smoke_user_token="$(printf '%s' "${user_login_response}" | json_get "token")"

message_body_b64="c21va2UtbWVzc2FnZQ=="
curl -fsS \
	-X POST "${base_url}/nodes/${smoke_user_node_id}/users/${smoke_user_id}/messages" \
	-H 'Content-Type: application/json' \
	-H "Authorization: Bearer ${smoke_user_token}" \
	--data "{\"body\":\"${message_body_b64}\"}" >/dev/null

messages_response="$(
	curl -fsS \
		-H "Authorization: Bearer ${smoke_user_token}" \
		"${base_url}/nodes/${smoke_user_node_id}/users/${smoke_user_id}/messages?limit=10"
)"
stored_message_b64="$(printf '%s' "${messages_response}" | json_get "items.0.body")"
if [[ "${stored_message_b64}" != "${message_body_b64}" ]]; then
	echo "unexpected stored message body: got=${stored_message_b64} want=${message_body_b64}" >&2
	exit 1
fi

events_response="$(
	curl -fsS \
		-H "Authorization: Bearer ${admin_token}" \
		"${base_url}/events?after=0&limit=20"
)"
if [[ "$(printf '%s' "${events_response}" | json_get "count")" -lt 2 ]]; then
	echo "expected at least two events after smoke flow" >&2
	exit 1
fi

echo "smoke flow passed"
