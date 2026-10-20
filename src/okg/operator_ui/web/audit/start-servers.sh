#!/usr/bin/env bash
# Bring up the two servers the audit drives, against LIVE okg-workspace:
#   backend  : okg.viz read API on :8765 (resolves okg-workspace from the
#              deployments registry — canonical okg_workspace DB)
#   frontend : vite dev on :5173, proxying /viz/api -> the backend
# Both are backgrounded (nohup) and survive this script; PIDs + logs land under
# audit/.servers/ so stop-servers.sh can reap them and we can read logs.
# Idempotent: if a server already answers, it is reused.
set -euo pipefail

EXPECTED_GRAPH_CANVAS_RESET_AUTHORITY="allow-reset:127.0.0.1:5544/okg_audit_fixture"

# These two branches are intentionally side-effect-free.  The test harness
# executes them with PATH sentinels before permitting any reset, server,
# network, build, or filesystem setup command to run.
# Their validator delegates to seed_fixture.validate_destructive_fixture_authority
# and writes only okg.operator_ui.audit_reset_guard.v1 or
# okg.operator_ui.audit_no_reset_guard.v1 receipts in check-only mode.
run_graph_canvas_authority_check_only() {
if [ "${OKG_AUDIT_AUTHORITY_CHECK_ONLY:-}" = "1" ]; then
uv run python "${BASH_SOURCE[0]%/*}/seed_fixture.py" --validate-authority-only
exit 0
fi
}

run_graph_canvas_no_reset_check_only() {
if [ "${OKG_AUDIT_NO_RESET_CHECK_ONLY:-}" = "1" ]; then
uv run python "${BASH_SOURCE[0]%/*}/seed_fixture.py" --validate-no-reset-receipt-only
exit 0
fi
}

run_graph_canvas_authority_check_only
run_graph_canvas_no_reset_check_only

AUDIT_DIR="$(cd "$(dirname "$0")" && pwd)"
# audit -> web -> operator_ui -> okg -> src -> repo/worktree root
ROOT="$(cd "$AUDIT_DIR/../../../../.." && pwd)"
SRV="$AUDIT_DIR/.servers"
BACK_LOG="$SRV/backend.log"
FRONT_LOG="$SRV/frontend.log"
BACK_PIDF="$SRV/backend.pid"
FRONT_PIDF="$SRV/frontend.pid"
DEPLOYMENT="${OKG_AUDIT_DEPLOYMENT:-okg-workspace}"
mkdir -p "$SRV"
cd "$ROOT"

# ---- seeded fixture mode (deterministic; the basis for the CI gate) ----
# OKG_AUDIT_FIXTURE=1 seeds a throwaway deployment + DB and points the backend at
# it instead of live okg-workspace -- so the audit runs deterministically with no
# live DSN and no ~100s reads. seed_fixture.py drops + recreates the DB for a
# fresh, reproducible graph each run; the fixture deployment dir lives beside this
# harness so the backend's deployment-dir resolver finds it via OKG_DEPLOYMENTS_DIR.
if [ -n "${OKG_AUDIT_FIXTURE:-}" ]; then
  FIXTURE_DSN="${OKG_AUDIT_FIXTURE_DSN:-postgres://postgres:okg@127.0.0.1:5544/okg_audit_fixture}"
  : "${OKG_AUDIT_RESET_AUTHORITY:?OKG_AUDIT_RESET_AUTHORITY is required for the disposable fixture}"
  if [ "$OKG_AUDIT_RESET_AUTHORITY" != "$EXPECTED_GRAPH_CANVAS_RESET_AUTHORITY" ]; then
    echo "refusing fixture authority: expected $EXPECTED_GRAPH_CANVAS_RESET_AUTHORITY" >&2
    exit 1
  fi
  if [ "${OKG_AUDIT_NO_RESET:-}" = "1" ]; then
    echo "fixture mode: validating published receipt (no reset) ..."
    export OKG_TEST_DSN="$FIXTURE_DSN"
    uv run python "$AUDIT_DIR/seed_fixture.py" --validate-no-reset-receipt-only
  else
    echo "fixture mode: seeding $FIXTURE_DSN (deployment=audit-fixture) ..."
    uv run --extra postgres --extra viz python "$AUDIT_DIR/seed_fixture.py" \
      --dsn "$FIXTURE_DSN" --deployment audit-fixture \
      --reset-authority "$OKG_AUDIT_RESET_AUTHORITY" \
      || { echo "fixture seed FAILED"; exit 1; }
  fi
  export OKG_DSN="$FIXTURE_DSN"
  export OKG_DEPLOYMENTS_DIR="$AUDIT_DIR/fixture-deployment"
  DEPLOYMENT=audit-fixture
fi

# Source the okg-workspace DSN if not already in the environment. Default looks
# at a sibling okg checkout's .okg/local.env; override with OKG_AUDIT_DSN_ENV or
# just export OKG_DSN yourself. Keeps the secret out of this script.
DSN_ENV="${OKG_AUDIT_DSN_ENV:-$HOME/projects/mit/okg/.okg/local.env}"
if [ -z "${OKG_DSN:-}" ] && [ -f "$DSN_ENV" ]; then
  set -a; . "$DSN_ENV"; set +a
fi
if [ -z "${OKG_DSN:-}" ]; then
  echo "no OKG_DSN — export it, or point OKG_AUDIT_DSN_ENV at a local.env"; exit 1
fi

wait_for() {
  local url="$1" name="$2" tries="${3:-60}"
  for i in $(seq 1 "$tries"); do
    if curl -fsS "$url" >/dev/null 2>&1; then echo "  $name up (${i}s)"; return 0; fi
    sleep 1
  done
  echo "  $name DID NOT COME UP after ${tries}s"; return 1
}

# ---- backend ----
if curl -fsS http://127.0.0.1:8765/viz/api/health >/dev/null 2>&1; then
  echo "backend :8765 already up — reusing"
else
  echo "starting backend :8765 (deployment=$DEPLOYMENT) ..."
  nohup uv run --extra postgres --extra viz --with msgpack python -m okg.viz --deployment "$DEPLOYMENT" --port 8765 \
    >"$BACK_LOG" 2>&1 &
  echo $! >"$BACK_PIDF"
  wait_for http://127.0.0.1:8765/viz/api/health "backend" 90 || { tail -25 "$BACK_LOG"; exit 1; }
fi

# ---- frontend ----
if curl -fsS http://127.0.0.1:5173/ >/dev/null 2>&1; then
  echo "frontend :5173 already up — reusing"
else
  if [ -n "${OKG_AUDIT_PRODUCTION:-}" ]; then
    echo "building production frontend ..."
    npm run operator-ui:build || { echo "production build FAILED"; exit 1; }
    echo "starting production preview :5173 (api -> :8765) ..."
    OKG_OPERATOR_API_BASE=http://127.0.0.1:8765 nohup npm run operator-ui:preview -- \
      --port 5173 --strictPort >"$FRONT_LOG" 2>&1 &
  else
    echo "starting frontend dev server :5173 (api -> :8765) ..."
    OKG_OPERATOR_API_BASE=http://127.0.0.1:8765 nohup npm run operator-ui:dev -- \
      --port 5173 --strictPort >"$FRONT_LOG" 2>&1 &
  fi
  echo $! >"$FRONT_PIDF"
  wait_for http://127.0.0.1:5173/ "frontend" 60 || { tail -25 "$FRONT_LOG"; exit 1; }
fi

echo "servers ready: http://127.0.0.1:5173  (api http://127.0.0.1:8765/viz/api)"
