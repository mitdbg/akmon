#!/usr/bin/env bash
#
# academic_researcher_chronos_e2e.sh
#
# Deploy an isolated academic-researcher OKG and verify that the installed system
# uses Chronos end to end:
#
#   install -> ingest into Chronos branch -> publish checkpoint generation
#   -> pinned MCP/search/SQL/viz reads -> branch publish -> cleanup
#
# The script intentionally uses an isolated HOME and OKG user-state root so it
# does not write to the operator's real deployments or MCP client configs.
#
# Usage:
#
#   bash scripts/academic_researcher_chronos_e2e.sh
#   bash scripts/academic_researcher_chronos_e2e.sh --name chronos-e2e-test --keep
#
# By default the script tests the current checkout. That is the right mode for
# validating this branch before deploying it. Use --operator-install-sh to
# exercise the public operator wrapper path in a temp clone.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

MODE="current"
DEPLOYMENT_NAME="${OKG_E2E_DEPLOYMENT:-chronos-e2e-$(date +%Y%m%d%H%M%S)}"
PI_NAME="${OKG_E2E_PI_NAME:-Ada Lovelace}"
ORCID="${OKG_E2E_ORCID:-}"
GIT_REPOS="${OKG_E2E_GIT_REPOS:-${REPO_ROOT}}"
KEEP=0
CONTINUE_AFTER_INSTALL_INGEST_FAILURES=0
RUN_DOCTOR=1
RUN_LIFECYCLE=1
REPO_URL="${OKG_E2E_REPO_URL:-file://${REPO_ROOT}}"
REPO_BRANCH="${OKG_E2E_BRANCH:-$(git -C "${REPO_ROOT}" branch --show-current 2>/dev/null || echo dev)}"

usage() {
    cat <<'EOF'
Usage: scripts/academic_researcher_chronos_e2e.sh [options]

Options:
  --name NAME              Deployment slug. Default: chronos-e2e-<timestamp>.
  --pi-name NAME           PI name passed to okg install. Default: Ada Lovelace.
  --orcid ORCID            Optional ORCID passed to okg install.
  --git-repos PATH         Repo path templated into academic-researcher git sources.
                           Default: this OKG checkout.
  --operator-install-sh    Exercise install.sh in a temp clone instead of the
                           current checkout. Defaults to a file:// clone of
                           this repo and branch; override with OKG_E2E_REPO_URL
                           and OKG_E2E_BRANCH.
  --continue-after-install-ingest-failures
                           Continue if okg install returns the documented
                           install_complete_with_ingest_failures outcome.
  --skip-doctor            Skip okg doctor --deployment.
  --skip-lifecycle         Skip okg start/stop/start.
  --keep                   Keep temp HOME/deployment/container for debugging.
  -h, --help               Show this help.

Environment:
  OKG_E2E_DEPLOYMENT, OKG_E2E_PI_NAME, OKG_E2E_ORCID, OKG_E2E_GIT_REPOS
  OKG_E2E_REPO_URL, OKG_E2E_BRANCH
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --name)
            DEPLOYMENT_NAME="$2"
            shift 2
            ;;
        --pi-name)
            PI_NAME="$2"
            shift 2
            ;;
        --orcid)
            ORCID="$2"
            shift 2
            ;;
        --git-repos)
            GIT_REPOS="$2"
            shift 2
            ;;
        --operator-install-sh)
            MODE="operator-install-sh"
            shift
            ;;
        --continue-after-install-ingest-failures)
            CONTINUE_AFTER_INSTALL_INGEST_FAILURES=1
            shift
            ;;
        --skip-doctor)
            RUN_DOCTOR=0
            shift
            ;;
        --skip-lifecycle)
            RUN_LIFECYCLE=0
            shift
            ;;
        --keep)
            KEEP=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf "unknown argument: %s\n\n" "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

TEST_ROOT="$(mktemp -d -t okg-academic-researcher-chronos-e2e-XXXXXX)"
WORK_REPO="${REPO_ROOT}"
export HOME="${TEST_ROOT}/home"
export OKG_USER_HOME="${TEST_ROOT}/state/okg"
export OKG_CACHE_HOME="${TEST_ROOT}/cache/okg"
export XDG_DATA_HOME="${TEST_ROOT}/home/.local/share"
export XDG_CACHE_HOME="${TEST_ROOT}/home/.cache"

log() { printf "==> %s\n" "$*" >&2; }
fail() { printf "FAIL: %s\n" "$*" >&2; exit 1; }

cleanup() {
    local rc=$?
    if [[ "${KEEP}" == "1" ]]; then
        log "Keeping test root for debugging: ${TEST_ROOT}"
        log "Deployment: ${DEPLOYMENT_NAME}"
        exit "${rc}"
    fi
    log "Cleanup..."
    if [[ -d "${WORK_REPO}" ]]; then
        (
            cd "${WORK_REPO}"
            uv run okg reset --deployment "${DEPLOYMENT_NAME}" --yes \
                >"${TEST_ROOT}/reset.log" 2>&1 || true
        )
    fi
    docker compose -p "okg-${DEPLOYMENT_NAME}" down -v \
        >"${TEST_ROOT}/docker-compose-down.log" 2>&1 || true
    rm -rf "${TEST_ROOT}" || true
    if [[ ${rc} -eq 0 ]]; then
        log "PASS"
    else
        log "FAIL (rc=${rc})"
    fi
    exit "${rc}"
}
trap cleanup EXIT

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "$1 is required on PATH"
}

run_okg() {
    (
        cd "${WORK_REPO}"
        uv run okg "$@"
    )
}

log "Test root: ${TEST_ROOT}"
log "Deployment: ${DEPLOYMENT_NAME}"
mkdir -p "${HOME}" "${OKG_USER_HOME}/deployments" "${OKG_CACHE_HOME}"

require_cmd docker
require_cmd git
require_cmd uv

if ! docker info >/dev/null 2>&1; then
    fail "Docker daemon is not reachable; start Docker Desktop and rerun"
fi

if [[ "${MODE}" == "operator-install-sh" ]]; then
    log "Cloning ${REPO_URL} (${REPO_BRANCH}) into isolated install root..."
    git clone --quiet --branch "${REPO_BRANCH}" "${REPO_URL}" "${TEST_ROOT}/okg"
    WORK_REPO="${TEST_ROOT}/okg"
fi

log "Preparing isolated agent runtime config files..."
mkdir -p "${HOME}/.codex" \
         "${HOME}/.cursor" \
         "${HOME}/Library/Application Support/Claude"
: > "${HOME}/.codex/config.toml"
printf '{}\n' > "${HOME}/.cursor/mcp.json"
printf '{}\n' > "${HOME}/Library/Application Support/Claude/claude_desktop_config.json"

install_args=(--name "${DEPLOYMENT_NAME}" --pi-name "${PI_NAME}" --git-repos "${GIT_REPOS}" --json)
if [[ -n "${ORCID}" ]]; then
    install_args+=(--orcid "${ORCID}")
fi

log "Installing academic-researcher deployment..."
set +e
if [[ "${MODE}" == "operator-install-sh" ]]; then
    (
        cd "${WORK_REPO}"
        OKG_INSTALL_ROOT="${WORK_REPO}" \
        OKG_REPO_URL="${REPO_URL}" \
        OKG_REPO_BRANCH="${REPO_BRANCH}" \
        ./install.sh "${install_args[@]}"
    ) >"${TEST_ROOT}/install.log" 2>&1
else
    (
        cd "${WORK_REPO}"
        uv sync --quiet
        uv run okg install "${install_args[@]}"
    ) >"${TEST_ROOT}/install.log" 2>&1
fi
install_rc=$?
set -e

if [[ ${install_rc} -ne 0 ]]; then
    if [[ "${CONTINUE_AFTER_INSTALL_INGEST_FAILURES}" == "1" ]] \
        && grep -q "install_complete_with_ingest_failures" "${TEST_ROOT}/install.log"
    then
        log "Continuing after install_complete_with_ingest_failures by request."
    else
        tail -120 "${TEST_ROOT}/install.log" >&2 || true
        fail "academic-researcher install failed with rc=${install_rc}"
    fi
fi
log "Install completed."

DEPLOYMENT_DIR="${OKG_USER_HOME}/deployments/${DEPLOYMENT_NAME}"
[[ -d "${DEPLOYMENT_DIR}" ]] || fail "deployment directory missing: ${DEPLOYMENT_DIR}"
[[ ! -d "${WORK_REPO}/deployments/${DEPLOYMENT_NAME}" ]] \
    || fail "deployment was written into the repo instead of isolated user state"

if ! grep -q "okg-${DEPLOYMENT_NAME}" "${HOME}/.codex/config.toml"; then
    fail "isolated Codex MCP config was not registered"
fi
log "Deployment and isolated MCP registration exist."

if [[ "${RUN_DOCTOR}" == "1" ]]; then
    log "Running okg doctor --deployment ${DEPLOYMENT_NAME}..."
    run_okg doctor --deployment "${DEPLOYMENT_NAME}" --json \
        >"${TEST_ROOT}/doctor.log" 2>&1 \
        || { tail -120 "${TEST_ROOT}/doctor.log" >&2 || true; fail "okg doctor failed"; }
fi

if [[ "${RUN_LIFECYCLE}" == "1" ]]; then
    log "Verifying lifecycle start/stop/start..."
    run_okg start --deployment "${DEPLOYMENT_NAME}" \
        >"${TEST_ROOT}/start1.log" 2>&1
    run_okg stop --deployment "${DEPLOYMENT_NAME}" \
        >"${TEST_ROOT}/stop.log" 2>&1
    run_okg start --deployment "${DEPLOYMENT_NAME}" \
        >"${TEST_ROOT}/start2.log" 2>&1
fi

log "Running Chronos-native graph assertions..."
(
    cd "${WORK_REPO}"
    OKG_E2E_DEPLOYMENT="${DEPLOYMENT_NAME}" \
    OKG_E2E_GRAPH_BRANCH="experiment:chronos-e2e" \
    uv run python - <<'PY'
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import psycopg
import yaml

from okg.substrate.deployments import find_deployment_dir
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.docsite import DocsiteSource
from okg.substrate.mcp import open_session
from okg.substrate.mcp.run_sql import run_sql
from okg.substrate.mcp.search import search
from okg.substrate.publisher.factory import build_deployment_publisher
from okg.viz import reads as viz_reads


deployment = os.environ["OKG_E2E_DEPLOYMENT"]
branch = os.environ["OKG_E2E_GRAPH_BRANCH"]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def read_dsn() -> str:
    dep_dir = find_deployment_dir(deployment)
    require(dep_dir is not None, f"deployment not found: {deployment}")
    raw = yaml.safe_load((Path(dep_dir) / "deployment.yaml").read_text()) or {}
    profile = raw.get("profile") or {}
    answers = profile.get("init_answers") or {}
    postgres = raw.get("postgres") or {}
    dsn = answers.get("postgres_dsn") or postgres.get("dsn")
    require(bool(dsn), "deployment.yaml does not contain postgres DSN")
    return str(dsn)


def write_fixture(prefix: str, text: str) -> Path:
    path = Path(tempfile.mkdtemp(prefix="okg-chronos-fixture-")) / "fixture.json"
    path.write_text(json.dumps({
        "nodes": [
            {
                "node_id": f"{prefix}:a",
                "subtype": "document",
                "attrs": {"text": text},
            },
            {
                "node_id": f"{prefix}:b",
                "subtype": "document",
                "attrs": {"text": f"{text} linked"},
            },
        ],
        "edges": [
            {"src": f"{prefix}:a", "dst": f"{prefix}:b", "edge_type": "links_to"},
        ],
    }))
    return path


dsn = read_dsn()
deployment_dir = find_deployment_dir(deployment)
require(deployment_dir is not None, f"deployment not found: {deployment}")
store = ChronosGraphStore.connect(dsn, ensure_schema=False)

install_checkpoint = store.latest_generation(
    deployment_name=deployment,
    branch="default",
    status="published",
)
require(install_checkpoint is not None, "install did not publish a Chronos checkpoint")
require(
    install_checkpoint.metadata.get("kind") == "okg_generation",
    f"unexpected checkpoint metadata: {install_checkpoint.metadata}",
)
require(
    install_checkpoint.metadata.get("publisher") == "chronos_native",
    f"publisher metadata is not chronos_native: {install_checkpoint.metadata}",
)

with psycopg.connect(dsn, autocommit=True) as conn:
    with conn.cursor() as cur:
        for table in (
            "node_facts",
            "edge_facts",
            "nodes_live",
            "edges_live",
            "graph_generations",
            "branches",
        ):
            cur.execute("SELECT to_regclass(%s)", (f"okg.{table}",))
            require(cur.fetchone()[0] is None, f"legacy table still exists: okg.{table}")

install_generation_id = install_checkpoint.checkpoint_id

default_prefix = f"chronos:e2e:{deployment}:default"
default_text = f"chronos e2e default smoke {deployment}"
SourceRunner(
    dsn,
    source_name="chronos_e2e_default",
    deployment_name=deployment,
).run(DocsiteSource(fixture_path=str(write_fixture(default_prefix, default_text))), mode="scope_complete")
default_publish = build_deployment_publisher(
    dsn=dsn,
    deployment_name=deployment,
    deployment_dir=deployment_dir,
    command="academic-researcher Chronos E2E default publish",
    branch_name="default",
).cycle()
require(default_publish.get("status") == "published", f"default publish failed: {default_publish}")
default_generation_id = default_publish["generation_id"]

with psycopg.connect(dsn) as conn:
    session = open_session(conn, deployment_name=deployment)
    require(session.generation_id == default_generation_id, "open_session did not pin latest checkpoint")
    sql = run_sql(
        conn,
        session,
        "SELECT node_id, subtype FROM okg.v_nodes "
        f"WHERE node_id = '{default_prefix}:a'",
    )
    require(sql["rows"] == [[f"{default_prefix}:a", "document"]], f"run_sql mismatch: {sql}")
    lexical = search(conn, session, default_text, method="lexical", limit=5)
    ids = {row["node_id"] for row in lexical.get("results", [])}
    require(f"{default_prefix}:a" in ids, f"lexical search missed smoke node: {lexical}")
    viz_node = viz_reads.fetch_node(conn, session, f"{default_prefix}:a")
    require(viz_node is not None and viz_node.id == f"{default_prefix}:a", "viz fetch_node missed smoke node")

old_graph = store.checkout_generation(install_generation_id)
old_rows = old_graph.query(
    f"SELECT node_id FROM okg.graph_nodes WHERE node_id = '{default_prefix}:a'"
)
require(old_rows == [], "old install checkpoint changed after later branch write")

subprocess.run([
    "uv", "run", "okg", "branch", "create", branch,
    "--role", "experiment",
    "--deployment", deployment,
], check=True)

branch_prefix = f"chronos:e2e:{deployment}:branch"
branch_text = f"chronos e2e branch smoke {deployment}"
SourceRunner(
    dsn,
    source_name="chronos_e2e_branch",
    deployment_name=deployment,
    graph_branch=branch,
).run(DocsiteSource(fixture_path=str(write_fixture(branch_prefix, branch_text))), mode="scope_complete")
branch_publish = build_deployment_publisher(
    dsn=dsn,
    deployment_name=deployment,
    deployment_dir=deployment_dir,
    command="academic-researcher Chronos E2E branch publish",
    branch_name=branch,
).cycle()
require(branch_publish.get("status") == "published", f"branch publish failed: {branch_publish}")
branch_generation_id = branch_publish["generation_id"]

with psycopg.connect(dsn) as conn:
    default_session = open_session(conn, deployment_name=deployment)
    branch_session = open_session(conn, deployment_name=deployment, branch_name=branch)
    default_sql = run_sql(
        conn,
        default_session,
        "SELECT node_id FROM okg.v_nodes "
        f"WHERE node_id = '{branch_prefix}:a'",
    )
    require(default_sql["rows"] == [], "default branch can see branch-only smoke node")
    branch_sql = run_sql(
        conn,
        branch_session,
        "SELECT node_id FROM okg.v_nodes "
        f"WHERE node_id = '{branch_prefix}:a'",
    )
    require(branch_sql["rows"] == [[f"{branch_prefix}:a"]], f"branch SQL mismatch: {branch_sql}")
    branch_search = search(conn, branch_session, branch_text, method="lexical", limit=5)
    branch_ids = {row["node_id"] for row in branch_search.get("results", [])}
    require(f"{branch_prefix}:a" in branch_ids, f"branch search missed smoke node: {branch_search}")

branch_checkpoint = store.get_generation(branch_generation_id)
require(
    branch_checkpoint.metadata.get("branch") == branch,
    f"branch checkpoint metadata did not record branch: {branch_checkpoint.metadata}",
)
store.close()

summary = {
    "deployment": deployment,
    "install_generation": install_generation_id,
    "default_generation": default_generation_id,
    "branch_generation": branch_generation_id,
    "default_smoke_node": f"{default_prefix}:a",
    "branch_smoke_node": f"{branch_prefix}:a",
}
print(json.dumps(summary, sort_keys=True))
PY
) >"${TEST_ROOT}/chronos-assertions.log" 2>&1 || {
    tail -160 "${TEST_ROOT}/chronos-assertions.log" >&2 || true
    fail "Chronos-native assertions failed"
}

cat "${TEST_ROOT}/chronos-assertions.log"
log "Chronos integration assertions passed."
log "Academic-pi Chronos E2E completed for ${DEPLOYMENT_NAME}."
