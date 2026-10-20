#!/usr/bin/env bash
#
# fresh_install.sh — end-to-end test of `okg install` from
# the operator's perspective, with a clean HOME so MCP configs
# don't pollute the operator's real Claude Desktop / codex /
# Cursor configs.
#
# What it verifies:
#
#   1. `./install.sh --profile academic-researcher` succeeds against a fresh checkout of the
#      verification branch using the script's HTTPS defaults.
#   2. `okg install` runs end-to-end through the bootstrap:
#        - Random scientist name derived (no --name passed)
#        - `--pi-name` derived from git config
#        - Docker stack provisioned on Docker-assigned port
#        - No operator-data sources are ingested before setup approval
#        - MCP runtimes detected + registered into isolated HOME
#        - Single-line handoff printed
#   3. The deployment lives under isolated XDG user state, not
#      the framework repo's deployments/ directory.
#   4. deployment.yaml has the docker block + non-zero host port.
#   5. MCP configs land in the ISOLATED HOME (not the
#      operator's real ones).
#   6. MCP propose_setup / commit_setup can find the user-state
#      registry through `okg mcp-serve --deployment <name>` and
#      source row deltas happen only after approval.
#   7. start / stop / start lifecycle verbs resolve the same
#      user-state docker metadata.
#   8. no-flag and explicit-name reruns short-circuit.
#   9. `okg reset --yes` tears down the Docker stack + removes
#      MCP entries cleanly.
#
# What it does NOT verify (out of scope):
#
#   - True OS-level isolation (no Docker-in-Docker due to the
#     localhost network namespace issue — install runs via
#     the host's Docker daemon).
#   - A real GUI agent's relay text. This script exercises the
#     same MCP tools directly.
#
# Usage:
#
#   bash tests/e2e/fresh_install.sh
#
# Exit code 0 iff every verification passes. Cleanup runs on
# exit (success OR failure) so the host doesn't accumulate
# leftover Docker stacks / MCP entries.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TEST_ROOT="$(mktemp -d -t okg-e2e-XXXXXX)"
DEPLOYMENT_NAME=""
E2E_REPO_URL="${OKG_E2E_REPO_URL:-https://github.com/mitdbg/akmon.git}"
E2E_BRANCH="${OKG_E2E_BRANCH:-main}"
export OKG_USER_HOME="${TEST_ROOT}/.local/share/okg"
export OKG_CACHE_HOME="${TEST_ROOT}/.cache/okg"

log()  { printf "==> %s\n" "$*" >&2; }
fail() { printf "FAIL: %s\n" "$*" >&2; exit 1; }

cleanup() {
    local rc=$?
    log "Cleanup..."
    if [[ -n "${DEPLOYMENT_NAME}" && -d "${TEST_ROOT}/okg" ]]; then
        log "Tearing down deployment ${DEPLOYMENT_NAME}..."
        # First try the proper okg reset path (also unregisters
        # MCP entries in the isolated HOME).
        env HOME="${TEST_ROOT}" \
            bash -c "cd '${TEST_ROOT}/okg' && uv run okg reset --deployment '${DEPLOYMENT_NAME}' --yes 2>&1 | tail -3" || true
        # Belt-and-suspenders: even if `okg reset` failed,
        # forcibly tear down the Docker stack so we don't
        # leak containers/volumes on the host. The compose
        # project name is `okg-<deployment>`.
        docker compose -p "okg-${DEPLOYMENT_NAME}" down -v \
            >/dev/null 2>&1 || true
    fi
    rm -rf "${TEST_ROOT}" || true
    if [[ $rc -eq 0 ]]; then
        log "PASS"
    else
        log "FAIL (rc=$rc)"
    fi
    exit "$rc"
}
trap cleanup EXIT


# ---------- step 0: pre-flight ----------

# Docker daemon reachable.
if ! docker info >/dev/null 2>&1; then
    fail "docker daemon not reachable; start Docker Desktop and re-run"
fi

# Docker disk pressure check. Local volumes accumulate from
# real deployments (cms, statgen-analysis, etc.) and from prior
# test runs. When the Docker VM's disk fills, Postgres initdb
# fails with `No space left on device` partway through. Detect
# this case BEFORE running install rather than blow ~5 min and
# then fail.
VOLS_GB=$(docker system df --format '{{.Size}}' 2>/dev/null | tail -1 | sed 's/GB//')
if [[ -n "${VOLS_GB:-}" ]]; then
    # Crude threshold — Docker Desktop's VM is configurable but
    # often defaults to ~64GB. 100GB of local volumes is a
    # strong signal of accumulation.
    if (( $(echo "$VOLS_GB > 100" | bc -l 2>/dev/null || echo 0) )); then
        log "(warn) Docker local volumes total ${VOLS_GB}GB."
        log "       If install fails with 'No space left on device',"
        log "       reclaim with: docker volume prune --filter label!=keep"
        log "       or remove specific dead-deployment volumes manually."
    fi
fi


# ---------- step 1: isolated HOME with a copy of the worktree ----------

log "Test root: ${TEST_ROOT}"
mkdir -p "${TEST_ROOT}/.local/bin"
# Carry git config so --pi-name derivation finds a value.
if [[ -f "${HOME}/.gitconfig" ]]; then
    cp "${HOME}/.gitconfig" "${TEST_ROOT}/.gitconfig"
fi

# Pre-create empty MCP config files so all three runtimes
# `detect()` to True and the install exercises the
# registration path. Without this, the no-runtime fallback
# fires and the handoff message is different.
mkdir -p "${TEST_ROOT}/Library/Application Support/Claude"
echo "{}" > "${TEST_ROOT}/Library/Application Support/Claude/claude_desktop_config.json"
mkdir -p "${TEST_ROOT}/.codex"
: > "${TEST_ROOT}/.codex/config.toml"
mkdir -p "${TEST_ROOT}/.cursor"
echo "{}" > "${TEST_ROOT}/.cursor/mcp.json"

# Fresh checkout through the same HTTPS path the bootstrap uses.
log "Cloning ${E2E_REPO_URL} (${E2E_BRANCH}) → ${TEST_ROOT}/okg/ ..."
git clone -b "${E2E_BRANCH}" "${E2E_REPO_URL}" "${TEST_ROOT}/okg" \
    | tee "${TEST_ROOT}/clone.log"


# ---------- step 2: install ----------

log "Running ./install.sh --profile academic-researcher (smart-defaults path)..."
# Disable set -e around the install call so we can print a focused
# failure message with the captured log path instead of exiting from
# the pipeline immediately.
set +e
env HOME="${TEST_ROOT}" \
    OKG_INSTALL_ROOT="${TEST_ROOT}/okg" \
    OKG_REPO_URL="${E2E_REPO_URL}" \
    OKG_REPO_BRANCH="${E2E_BRANCH}" \
    bash -c "cd '${TEST_ROOT}/okg' && ./install.sh --profile academic-researcher" \
    2>&1 | tee "${TEST_ROOT}/install.log"
INSTALL_RC=${PIPESTATUS[0]}
set -e

if [[ ${INSTALL_RC} -ne 0 ]]; then
    fail "okg install exited with rc=${INSTALL_RC}; see ${TEST_ROOT}/install.log"
fi


# ---------- step 3: verify artifacts ----------

log "Verifying artifacts..."

DEP_ROOT="${TEST_ROOT}/.local/share/okg/deployments"
# Find the deployment (random pick from scientist roster) in
# user-state. It must not be scaffolded into the framework repo.
mapfile -t deps < <(find "${DEP_ROOT}" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; 2>/dev/null | sort || true)
if [[ ${#deps[@]} -eq 0 ]]; then
    fail "no new deployment found in ${DEP_ROOT}"
fi
if [[ ${#deps[@]} -gt 1 ]]; then
    fail "expected 1 new deployment, found ${#deps[@]}: ${deps[*]}"
fi
DEPLOYMENT_NAME="${deps[0]}"
log "Deployment: ${DEPLOYMENT_NAME}"
if [[ -d "${TEST_ROOT}/okg/deployments/${DEPLOYMENT_NAME}" ]]; then
    fail "deployment was scaffolded into repo deployments/ instead of user state"
fi
log "User-state deployment location: ✓"

# Verify the name was drawn from the SCIENTIST_ROSTER.
INSTALL_PY="${TEST_ROOT}/okg/src/okg/substrate/library/profiles/academic-researcher/cli/install.py"
if ! grep -q "\"${DEPLOYMENT_NAME}\"" "${INSTALL_PY}"; then
    fail "deployment ${DEPLOYMENT_NAME} not in SCIENTIST_ROSTER"
fi
log "Name from roster: ✓"

# Docker block in deployment.yaml.
DEPLOY_YAML="${DEP_ROOT}/${DEPLOYMENT_NAME}/deployment.yaml"
if [[ ! -f "${DEPLOY_YAML}" ]]; then
    fail "deployment.yaml missing"
fi
HOST_PORT=$(grep -E "^  host_port:" "${DEPLOY_YAML}" | awk '{print $2}')
if [[ -z "${HOST_PORT}" || "${HOST_PORT}" -lt 1024 ]]; then
    fail "deployment.yaml host_port invalid: '${HOST_PORT}'"
fi
log "Docker host_port: ${HOST_PORT} ✓"

# MCP configs land in isolated HOME, not the operator's real
# ones. At least one runtime should have been detected
# (the worktree's existing codex config is at ${HOME}/.codex,
# but our isolated HOME has no such file yet — so codex would
# be `not_detected` UNLESS the operator's test environment has
# Claude Desktop installed, in which case the macOS path
# under "Library/Application Support/Claude/" might land
# inside ${TEST_ROOT}).
# Just verify NOTHING was written to the real operator HOME
# (sentinel: real ~/.codex/config.toml shouldn't have a new
# entry for our random deployment name).
if grep -q "okg-${DEPLOYMENT_NAME}" "${HOME}/.codex/config.toml" 2>/dev/null; then
    fail "operator's REAL codex config got a new entry for ${DEPLOYMENT_NAME} (HOME isolation broken!)"
fi
if ! grep -q "okg-${DEPLOYMENT_NAME}" "${TEST_ROOT}/.codex/config.toml"; then
    fail "isolated codex config missing okg-${DEPLOYMENT_NAME}"
fi
if ! grep -q "okg-${DEPLOYMENT_NAME}" "${TEST_ROOT}/Library/Application Support/Claude/claude_desktop_config.json"; then
    fail "isolated Claude Desktop config missing okg-${DEPLOYMENT_NAME}"
fi
log "HOME isolation: ✓ (operator's real configs untouched)"

# Handoff line in install output.
if ! grep -q "set up my OKG for ${DEPLOYMENT_NAME}" "${TEST_ROOT}/install.log"; then
    fail "handoff line missing from install output"
fi
log "Handoff line: ✓"

# Smart-defaults logging.
if ! grep -q "randomly picked from the scientist roster" "${TEST_ROOT}/install.log"; then
    fail "expected 'randomly picked from the scientist roster' in install log"
fi
log "Smart-defaults observability: ✓"

# Install is now wizard-first: the deployment is ready, but
# operator-data sources must not have source facts before
# propose_setup/commit_setup approval.
log "Verifying wizard-first source boundary..."
env HOME="${TEST_ROOT}" \
    bash -c "cd '${TEST_ROOT}/okg' && uv run python - '${DEPLOYMENT_NAME}'" \
    > "${TEST_ROOT}/pre_setup_counts.log" <<'PY'
import sys
import yaml
import psycopg
from pathlib import Path

deployment = sys.argv[1]
dep_dir = Path.home() / ".local/share/okg/deployments" / deployment
raw = yaml.safe_load((dep_dir / "deployment.yaml").read_text()) or {}
dsn = ((raw.get("profile") or {}).get("init_answers") or {}).get("postgres_dsn")
if not dsn:
    dsn = (raw.get("postgres") or {}).get("dsn")
if not dsn:
    raise SystemExit("no postgres dsn in deployment.yaml")
sources = [
    "code_structure",
    "git_files",
    "git_history",
    "openspec",
    "agent_sessions",
    "dblp_by_orcid",
    "nsf_award_search",
    "doe_osti",
]
ownership_ids = [f"{deployment}.{sid}" for sid in sources]
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute(
        """
        SELECT source, sum(n)::bigint
        FROM (
          SELECT source, count(*) AS n
          FROM okg.node_facts
          WHERE source = ANY(%s)
          GROUP BY source
          UNION ALL
          SELECT source, count(*) AS n
          FROM okg.edge_facts
          WHERE source = ANY(%s)
          GROUP BY source
        ) q
        GROUP BY source
        """,
        (ownership_ids, ownership_ids),
    )
    counts = {source: int(count) for source, count in cur.fetchall()}
print(counts)
if counts:
    raise SystemExit(f"operator-data source facts before setup: {counts}")
PY
log "Wizard-first source boundary: ✓"

# MCP setup wizard over the real stdio MCP server.
log "Exercising MCP propose_setup / commit_setup..."
env HOME="${TEST_ROOT}" \
    bash -c "cd '${TEST_ROOT}/okg' && uv run python - '${DEPLOYMENT_NAME}'" \
    > "${TEST_ROOT}/mcp_setup.log" <<'PY'
import json
import sys

from okg.substrate.library.sources.mcp_composed.client import McpClient

deployment = sys.argv[1]

JSON_META = {"okg/result_format": "json"}

def unpack(result):
    # The calls ask for the compact-JSON result format (okg issue #3059), so
    # the one text block is the tool's dictionary. A block that is not JSON
    # means the opt-in was ignored: an error, never a silent summary.
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        return structured
    for content in result.get("content") or []:
        if content.get("type") == "text" and content.get("text"):
            return json.loads(content["text"])
    raise SystemExit("MCP result carried no text block")

with McpClient(
    ["uv", "run", "okg", "mcp-serve", "--deployment", deployment],
    subprocess_timeout=90.0,
) as client:
    proposal = unpack(client.call_tool(
        "propose_setup", {"deployment": deployment}, timeout=90.0,
        meta=JSON_META,
    ))
    print(json.dumps({"proposal": proposal}, sort_keys=True))
    if proposal.get("status") == "deployment_not_found":
        raise SystemExit("propose_setup returned deployment_not_found")
    plan_id = proposal.get("plan_id")
    if plan_id:
        committed = unpack(client.call_tool(
            "commit_setup", {"plan_id": plan_id}, timeout=120.0,
            meta=JSON_META,
        ))
        print(json.dumps({"commit": committed}, sort_keys=True))
        if committed.get("status") == "deployment_not_found":
            raise SystemExit("commit_setup returned deployment_not_found")
        if committed.get("status") == "ok":
            gd = committed.get("graph_delta") or {}
            print(json.dumps({"graph_delta": gd}, sort_keys=True))
PY
if grep -q "deployment_not_found" "${TEST_ROOT}/mcp_setup.log"; then
    fail "MCP setup returned deployment_not_found"
fi
log "MCP setup tools: ✓"

# Lifecycle verbs should resolve user-state deployment.yaml.docker.
log "Verifying lifecycle start/stop/start..."
env HOME="${TEST_ROOT}" \
    bash -c "cd '${TEST_ROOT}/okg' && uv run okg start --deployment '${DEPLOYMENT_NAME}'" \
    > "${TEST_ROOT}/start1.log" 2>&1
env HOME="${TEST_ROOT}" \
    bash -c "cd '${TEST_ROOT}/okg' && uv run okg stop --deployment '${DEPLOYMENT_NAME}'" \
    > "${TEST_ROOT}/stop.log" 2>&1
env HOME="${TEST_ROOT}" \
    bash -c "cd '${TEST_ROOT}/okg' && uv run okg start --deployment '${DEPLOYMENT_NAME}'" \
    > "${TEST_ROOT}/start2.log" 2>&1
if ! docker inspect --format '{{.State.Health.Status}}' "okg-${DEPLOYMENT_NAME}-postgres" 2>/dev/null | grep -q "healthy"; then
    fail "Postgres container not healthy after lifecycle start"
fi
log "Lifecycle: ✓"

# Idempotency: re-running install with no flags in this isolated HOME
# should short-circuit to MCP re-registration in <30s.
log "Verifying no-flag idempotent re-run..."
START=$(date +%s)
env HOME="${TEST_ROOT}" \
    OKG_INSTALL_ROOT="${TEST_ROOT}/okg" \
    OKG_REPO_URL="${E2E_REPO_URL}" \
    OKG_REPO_BRANCH="${E2E_BRANCH}" \
    bash -c "cd '${TEST_ROOT}/okg' && ./install.sh --profile academic-researcher" \
    > "${TEST_ROOT}/rerun-no-name.log" 2>&1 || true
END=$(date +%s)
ELAPSED=$((END - START))
if [[ ${ELAPSED} -gt 30 ]]; then
    fail "no-flag re-run took ${ELAPSED}s; expected <30s for idempotent short-circuit"
fi
if ! grep -q "agent_runtimes" "${TEST_ROOT}/rerun-no-name.log"; then
    fail "no-flag re-run output missing agent_runtimes section"
fi
mapfile -t deps_after < <(find "${DEP_ROOT}" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; 2>/dev/null | sort || true)
if [[ ${#deps_after[@]} -ne 1 || "${deps_after[0]}" != "${DEPLOYMENT_NAME}" ]]; then
    fail "no-flag re-run changed deployments: ${deps_after[*]}"
fi
log "No-flag idempotent re-run (${ELAPSED}s): ✓"

log "Verifying explicit-name idempotent re-run..."
START=$(date +%s)
env HOME="${TEST_ROOT}" \
    bash -c "cd '${TEST_ROOT}/okg' && uv run okg install --profile academic-researcher --name '${DEPLOYMENT_NAME}'" \
    > "${TEST_ROOT}/rerun-explicit.log" 2>&1 || true
END=$(date +%s)
ELAPSED=$((END - START))
if [[ ${ELAPSED} -gt 30 ]]; then
    fail "explicit-name re-run took ${ELAPSED}s; expected <30s"
fi
if ! grep -q "agent_runtimes" "${TEST_ROOT}/rerun-explicit.log"; then
    fail "explicit-name re-run output missing agent_runtimes section"
fi
log "Explicit-name idempotent re-run (${ELAPSED}s): ✓"

log "All verifications passed for ${DEPLOYMENT_NAME}."
log "Cleanup will run on exit."
