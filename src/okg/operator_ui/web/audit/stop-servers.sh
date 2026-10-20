#!/usr/bin/env bash
# Reap the audit servers started by start-servers.sh (by recorded PID, then by
# port as a fallback). Safe to run when nothing is up.
set -uo pipefail
AUDIT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRV="$AUDIT_DIR/.servers"

for f in backend.pid frontend.pid; do
  if [ -f "$SRV/$f" ]; then
    pid="$(cat "$SRV/$f")"
    if kill "$pid" 2>/dev/null; then echo "killed $f ($pid)"; fi
    rm -f "$SRV/$f"
  fi
done

# Fallback: anything still holding the ports.
for port in 8765 5173; do
  pids="$(lsof -ti :$port 2>/dev/null || true)"
  if [ -n "$pids" ]; then kill $pids 2>/dev/null && echo "killed stragglers on :$port ($pids)"; fi
done
echo "stopped."
