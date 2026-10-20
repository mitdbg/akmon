#!/usr/bin/env bash
set -euo pipefail

: "${OKG_DSN:?set OKG_DSN before running this recipe}"

export OKG_DEPLOYMENTS_DIR="{{ deployments_dir }}"

uv run --extra code --extra quality okg migrate --deployment "{{ deployment_name }}" --apply
uv run --extra code --extra quality okg catalog load --deployment "{{ deployment_dir }}" --apply
uv run --extra code --extra quality okg doctor --check sources --deployment "{{ deployment_name }}"

uv run --extra code --extra quality okg ingest --deployment "{{ deployment_name }}" --progress
uv run --extra code --extra quality okg run --once --deployment "{{ deployment_name }}" --apply
uv run --extra code --extra quality okg status --deployment "{{ deployment_name }}"
uv run --extra code --extra quality okg doctor --check coding-context --deployment "{{ deployment_name }}"

uv run --extra code --extra quality okg alias seed-from-subtype --subtype open_spec_change --field change_id --apply

if [[ "${OKG_START_RUNTIME_WORKER:-0}" == "1" ]]; then
  uv run --extra code --extra quality okg runtime worker --deployment "{{ deployment_name }}" &
  echo "runtime worker pid: $!"
fi

cat <<COMMANDS
MCP commands:
uv run --extra code --extra quality okg mcp-serve --deployment "{{ deployment_name }}"
uv run --extra code --extra quality python "{{ deployment_dir }}/server.py"
COMMANDS
