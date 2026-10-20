#!/usr/bin/env bash
# The one documented command. From a clean checkout:
#
#     deployments/opensearch-reference/up.sh
#
# Brings up the local OpenSearch container, waits for it to be healthy,
# seeds three indices, then provisions the graph: migrate, apply the
# catalog, ingest, publish. Prints the read command at the end.
#
# Why `okg provision` and not `okg deployment up`: the latter is the
# post-merge refresh verb -- "current state to current code" -- and its
# first step reads the deployment's publish state, so against the empty
# database a clean checkout has it fails on a missing
# `okg.admission_envelopes`. `provision` is the bootstrap path and is
# idempotent, so it is also the right verb on the second run.
#
# OKG_DSN must name a database that already exists and that this
# deployment may own; the manifest declares `okg_opensearch_reference`,
# and `okg provision` refuses a DSN naming anything else.
#
# The container is single-node, capped at 2 GiB, bound to loopback, and
# unauthenticated: a local dev container with no credential anywhere.
# Stop it with `--down` when you are finished.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
COMPOSE="${HERE}/opensearch/docker-compose.yaml"
OS_URL="${OKG_OPENSEARCH_URL:-http://127.0.0.1:9209}"
HEALTH_TIMEOUT_SECONDS="${OKG_OPENSEARCH_HEALTH_TIMEOUT:-180}"

step() { printf '\n==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

if [ "${1:-}" = "--down" ]; then
  # -v removes the named volume too. The compose file documents the
  # volume as part of this stack, so leaving it behind would mean
  # `--down` quietly kept gigabytes of index data and the next `up.sh`
  # would seed on top of it rather than from clean.
  step "Stopping the reference cluster and removing its volume"
  docker compose -f "$COMPOSE" down -v
  exit 0
fi

command -v docker >/dev/null || die "docker is not on PATH"
command -v curl >/dev/null || die "curl is not on PATH"
[ -n "${OKG_DSN:-}" ] || die \
  "OKG_DSN is unset. Point it at an EXISTING Postgres database named okg_opensearch_reference that this deployment may own; see docs/operate/deployments/database.md"

step "Starting OpenSearch (single node, 2 GiB cap, 127.0.0.1:9209)"
docker compose -f "$COMPOSE" up -d

step "Waiting for cluster health (up to ${HEALTH_TIMEOUT_SECONDS}s)"
deadline=$(( $(date +%s) + HEALTH_TIMEOUT_SECONDS ))
until curl -fsS "${OS_URL}/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null 2>&1; do
  [ "$(date +%s)" -lt "$deadline" ] || die \
    "cluster did not reach yellow within ${HEALTH_TIMEOUT_SECONDS}s; check: docker compose -f ${COMPOSE} logs"
  sleep 3
done
printf '  healthy at %s\n' "$OS_URL"

step "Seeding three indices"
OKG_OPENSEARCH_URL="$OS_URL" "${HERE}/opensearch/seed.sh"

step "Migrating, applying the catalog, ingesting and publishing"
cd "$REPO_ROOT"
# NOT a bare `okg`. A globally installed okg on the machine belongs to
# whichever checkout installed it, and the CLI refuses to run against a
# deployment from a different tree -- correctly, and this script hit that
# refusal the first time it ran for real. Prefer this checkout's venv,
# fall back to `uv run` inside it.
# Exit 8 means the publish committed with a capacity warning (the lock hold
# passed the bar); provision prints it. Only that code proceeds.
provision_rc=0
if [ -x "${REPO_ROOT}/.venv/bin/okg" ]; then
  OKG="${REPO_ROOT}/.venv/bin/okg"
  OKG_OPENSEARCH_URL="$OS_URL" "$OKG" provision \
    --deployment opensearch-reference --dsn "$OKG_DSN" || provision_rc=$?
elif command -v uv >/dev/null; then
  OKG_OPENSEARCH_URL="$OS_URL" uv run --project "$REPO_ROOT" okg provision \
    --deployment opensearch-reference --dsn "$OKG_DSN" || provision_rc=$?
else
  die "no okg in ${REPO_ROOT}/.venv and no uv on PATH; run 'uv sync' first"
fi
if [ "$provision_rc" -ne 0 ] && [ "$provision_rc" -ne 8 ]; then
  exit "$provision_rc"
fi

cat <<'DONE'

==> Up. The graph holds 6 nodes and 7 edges:
      1 search_cluster, 3 search_index, 2 service
      3 contains (cluster -> index), 4 logs_to (service -> index)

    Read it:
      okg search --deployment opensearch-reference --query app-logs

    Stop the container when you are finished:
      deployments/opensearch-reference/up.sh --down
DONE
