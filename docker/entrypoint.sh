#!/bin/bash
# Entrypoint for the OKG MCP base image.
#
# Translates the container's env into an `okg mcp-serve` invocation. All
# deployment specifics (which deployment, which Postgres, which secrets) arrive
# as env + mounts, so the image itself stays deployment-agnostic.
set -euo pipefail

: "${OKG_MCP_TRANSPORT:=streamable-http}"
# Bind loopback by default: safe under --network host (the intended sidecar
# mode), where other host-network containers and the host itself still reach it
# via localhost, but the unauthenticated read surface is NOT exposed on every
# host interface. Set OKG_MCP_HOST=0.0.0.0 for bridge networking + published
# ports, where the port must be reachable from outside the container's netns.
: "${OKG_MCP_HOST:=127.0.0.1}"
: "${OKG_MCP_PORT:=8100}"
: "${OKG_DEPLOYMENT:=cms}"
: "${OKG_DEPLOYMENTS_DIR:=/deployments}"

# Env-var names are conventionally derived from the deployment name; normalize
# it to a valid shell-identifier stem (uppercase, '-' -> '_') so hyphenated
# deployments (e.g. intersystems-ops -> INTERSYSTEMS_OPS) work too.
_dep_stem="${OKG_DEPLOYMENT^^}"
_dep_stem="${_dep_stem//-/_}"

# The deployment package (deployment.yaml + its Python modules referenced by the
# alias resolver / enrichers) is mounted, not baked. resolve_deployment reads it
# via OKG_DEPLOYMENTS_DIR; make its modules importable too.
export OKG_DEPLOYMENTS_DIR
if [[ -d "$OKG_DEPLOYMENTS_DIR/$OKG_DEPLOYMENT" ]]; then
  export PYTHONPATH="${OKG_DEPLOYMENTS_DIR}/${OKG_DEPLOYMENT}${PYTHONPATH:+:${PYTHONPATH}}"
else
  echo "[entrypoint] ERROR: deployment package not found at" \
       "${OKG_DEPLOYMENTS_DIR}/${OKG_DEPLOYMENT}." >&2
  echo "[entrypoint] Mount the deployments dir, e.g. -v /host/okg-deployments:${OKG_DEPLOYMENTS_DIR}:ro" >&2
  exit 1
fi

# Optional env file (DSN and/or external-tool tokens such as MONIT). Resolve it
# generically: OKG_ENV_FILE, else <DEPLOYMENT>_ENV_FILE (e.g. CMS_ENV_FILE),
# else the legacy OKG_CMS_ENV_FILE. `okg mcp-serve` does not read any of these
# itself, so load it here -- without overriding values already passed via -e.
_env_file_var="${_dep_stem}_ENV_FILE"
_env_file="${OKG_ENV_FILE:-${!_env_file_var:-${OKG_CMS_ENV_FILE:-}}}"
if [[ -n "$_env_file" ]]; then
  if [[ ! -f "$_env_file" ]]; then
    echo "[entrypoint] ERROR: env file set but not a file: $_env_file" >&2
    exit 1
  fi
  echo "[entrypoint] loading env file $_env_file"
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    line="${raw#"${raw%%[![:space:]]*}"}"          # ltrim
    [[ -z "$line" || "$line" == \#* ]] && continue
    # strip an optional leading `export` keyword + any run of following spaces
    if [[ "$line" == export[[:space:]]* ]]; then
      line="${line#export}"
      line="${line#"${line%%[![:space:]]*}"}"      # ltrim again
    fi
    [[ "$line" != *=* ]] && continue
    key="${line%%=*}"; val="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"           # rtrim a full run of spaces
    [[ -z "$key" ]] && continue
    [[ -n "${!key:-}" ]] && continue               # don't override -e values
    val="${val%\"}"; val="${val#\"}"; val="${val%\'}"; val="${val#\'}"
    export "$key=$val"
  done < "$_env_file"
fi

# A published-generation DSN is required. Accept the generic OKG_DSN or the
# deployment's own DSN env, whose name deployment.yaml conventionally derives
# from the deployment name (example -> EXAMPLE_DSN, atlas -> ATLAS_DSN, ...). Deriving
# it keeps this gate deployment-agnostic instead of hardcoding one deployment name.
_dsn_var="${_dep_stem}_DSN"
if [[ -z "${OKG_DSN:-}" && -z "${!_dsn_var:-}" ]]; then
  echo "[entrypoint] ERROR: no DSN set. Provide OKG_DSN or ${_dsn_var}" \
       "pointing at a Postgres with a published ${OKG_DEPLOYMENT} generation." >&2
  exit 1
fi

# Read-only by construction. This image exposes the READ surface; the write
# tools (setup/curation) are an operator-supervised task, not a network-sidecar
# job. `okg mcp-serve` enables writes from OKG_MCP_ALLOW_WRITES and the
# container passes env straight through, so a stray -e (or an env-file entry)
# must not silently turn this into an unauthenticated remote write surface.
# Neutralize it here unless the operator DELIBERATELY opts in with
# OKG_MCP_ENABLE_WRITES; even then the server refuses writes over a non-loopback
# bind. Runs after env-file load so a file-sourced value is covered too.
if [[ "${OKG_MCP_ENABLE_WRITES:-}" =~ ^(1|true|TRUE|yes|YES)$ ]]; then
  export OKG_MCP_ALLOW_WRITES=1
  echo "[entrypoint] WARNING: write tools ENABLED via OKG_MCP_ENABLE_WRITES;" \
       "non-loopback binds are still refused by the server." >&2
else
  unset OKG_MCP_ALLOW_WRITES
fi

echo "[entrypoint] okg mcp-serve --deployment ${OKG_DEPLOYMENT}" \
     "--transport ${OKG_MCP_TRANSPORT} --host ${OKG_MCP_HOST} --port ${OKG_MCP_PORT}"

exec okg mcp-serve \
  --deployment "${OKG_DEPLOYMENT}" \
  --transport "${OKG_MCP_TRANSPORT}" \
  --host "${OKG_MCP_HOST}" \
  --port "${OKG_MCP_PORT}"
