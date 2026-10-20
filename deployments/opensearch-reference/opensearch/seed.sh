#!/usr/bin/env bash
# Seed the reference cluster with three indices and deterministic documents.
#
# Deterministic on purpose: the expected node, edge, and document counts in
# the tests and the docs are exact, and the live proof's counts are diffed
# against the recorded-fixture arm's. Timestamps are fixed offsets from a
# pinned epoch so the live histogram in the second half of this change is
# reproducible too.
#
# Idempotent: every index is deleted first, so a second run leaves the same
# cluster rather than doubling the documents.
set -euo pipefail

OS_URL="${OKG_OPENSEARCH_URL:-http://127.0.0.1:9209}"
EPOCH="2026-09-01T00:00:00Z"

say() { printf '  %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

req() {
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -fsS -X "$method" "${OS_URL}${path}" \
      -H 'Content-Type: application/json' -d "$body" >/dev/null
  else
    curl -fsS -X "$method" "${OS_URL}${path}" >/dev/null
  fi
}

drop() { curl -fsS -X DELETE "${OS_URL}/$1" >/dev/null 2>&1 || true; }

# ---- app-logs: 40 documents, 9 mapping fields, alias app-logs -----------
drop 'app-logs-2026.09'
req PUT '/app-logs-2026.09' '{
  "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
  "mappings": {"properties": {
    "@timestamp": {"type": "date"},
    "level": {"type": "keyword"},
    "message": {"type": "text"},
    "service": {"type": "keyword"},
    "trace_id": {"type": "keyword"},
    "http": {"properties": {
      "method": {"type": "keyword"},
      "status": {"type": "integer"},
      "path": {"type": "keyword"}
    }},
    "duration_ms": {"type": "float"}
  }}
}'
req POST '/_aliases' '{"actions":[{"add":{"index":"app-logs-2026.09","alias":"app-logs"}}]}'

# ---- ingest-events: 25 documents, 7 mapping fields ----------------------
drop 'ingest-events-2026.09'
req PUT '/ingest-events-2026.09' '{
  "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
  "mappings": {"properties": {
    "@timestamp": {"type": "date"},
    "source_name": {"type": "keyword"},
    "run_id": {"type": "keyword"},
    "records": {"type": "long"},
    "outcome": {"type": "keyword"},
    "duration_ms": {"type": "float"},
    "generation": {"type": "long"}
  }}
}'

# ---- audit-trail: 12 documents, 5 mapping fields, alias audit-trail -----
drop 'audit-trail-2026.09'
req PUT '/audit-trail-2026.09' '{
  "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
  "mappings": {"properties": {
    "@timestamp": {"type": "date"},
    "principal": {"type": "keyword"},
    "action": {"type": "keyword"},
    "decision": {"type": "keyword"},
    "resource": {"type": "keyword"}
  }}
}'
req POST '/_aliases' '{"actions":[{"add":{"index":"audit-trail-2026.09","alias":"audit-trail"}}]}'

# ---- lifecycle policies -------------------------------------------------
# app-logs and ingest-events carry one; audit-trail deliberately does NOT,
# so the graph shows a read-and-absent retention alongside two present ones.
# Delete first, like the indices above, so a second run is idempotent.
# The first version of this loop swallowed every error behind
# `|| say "ISM plugin unavailable"`, which then reported a plain
# version conflict -- the policy already existed -- as a missing plugin.
# A wrong diagnosis printed as fact is worse than no message.
for policy in 'okg-logs-30d:30d' 'okg-events-90d:90d'; do
  name="${policy%%:*}"; age="${policy##*:}"
  curl -fsS -X DELETE "${OS_URL}/_plugins/_ism/policies/${name}" \
    >/dev/null 2>&1 || true
  curl -fsS -X PUT "${OS_URL}/_plugins/_ism/policies/${name}" \
    -H 'Content-Type: application/json' -d "{
      \"policy\": {
        \"description\": \"OKG reference retention: ${age}\",
        \"default_state\": \"hot\",
        \"states\": [
          {\"name\": \"hot\", \"actions\": [], \"transitions\": [
            {\"state_name\": \"delete\", \"conditions\": {\"min_index_age\": \"${age}\"}}]},
          {\"name\": \"delete\", \"actions\": [{\"delete\": {}}], \"transitions\": []}
        ]
      }}" >/dev/null || die "could not create ISM policy ${name}"
done
req PUT '/app-logs-2026.09/_settings' \
  '{"index": {"plugins.index_state_management.policy_id": "okg-logs-30d"}}'
req PUT '/ingest-events-2026.09/_settings' \
  '{"index": {"plugins.index_state_management.policy_id": "okg-events-90d"}}'

# ---- documents ----------------------------------------------------------
# A _bulk call returns HTTP 200 even when every document in it was
# REJECTED; the per-item failures live in the body behind "errors":true.
# `curl -fsS` is happy either way, so a seed that only checked the exit
# status would report success and leave the index short -- which is
# exactly what happened here before this check existed: one document
# carrying an out-of-range hour was dropped and the script said nothing.
bulk() {
  local response
  response=$(curl -fsS -X POST "${OS_URL}/_bulk?refresh=wait_for" \
    -H 'Content-Type: application/x-ndjson' --data-binary "@-")
  case "$response" in
    *'"errors":true'*)
      printf 'error: _bulk rejected at least one document:\n%s\n' \
        "$response" >&2
      exit 1
      ;;
  esac
}

{
  for i in $(seq 0 39); do
    printf '{"index":{"_index":"app-logs-2026.09"}}\n'
    printf '{"@timestamp":"2026-09-01T%02d:%02d:00Z","level":"%s","message":"request %d handled","service":"okg-api","trace_id":"trace-%04d","http":{"method":"GET","status":%d,"path":"/v1/search"},"duration_ms":%d.5}\n' \
      "$(( i / 4 ))" "$(( (i % 4) * 15 ))" \
      "$([ $(( i % 8 )) -eq 0 ] && echo warn || echo info)" "$i" "$i" \
      "$([ $(( i % 8 )) -eq 0 ] && echo 503 || echo 200)" "$(( 12 + i ))"
  done
} | bulk

{
  for i in $(seq 0 24); do
    printf '{"index":{"_index":"ingest-events-2026.09"}}\n'
    # Half-hour steps, not hourly: 25 hourly slots starting at 00 would
    # need hour 24, which is not a time. OpenSearch rejects it, the bulk
    # call still returns 200, and the index silently holds 24 documents.
    printf '{"@timestamp":"2026-09-01T%02d:%02d:00Z","source_name":"opensearch-catalog","run_id":"run-%04d","records":%d,"outcome":"%s","duration_ms":%d.0,"generation":%d}\n' \
      "$(( i / 2 ))" "$(( (i % 2) * 30 ))" "$i" "$(( 6 + i ))" \
      "$([ $(( i % 5 )) -eq 0 ] && echo retry || echo ok)" "$(( 100 + i ))" "$(( 1 + i ))"
  done
} | bulk

{
  for i in $(seq 0 11); do
    printf '{"index":{"_index":"audit-trail-2026.09"}}\n'
    printf '{"@timestamp":"2026-09-01T%02d:30:00Z","principal":"service:okg-runtime","action":"external.opensearch.search","decision":"%s","resource":"search_index"}\n' \
      "$(( i * 2 ))" "$([ $(( i % 6 )) -eq 0 ] && echo deny || echo allow)"
  done
} | bulk

req POST '/app-logs-2026.09,ingest-events-2026.09,audit-trail-2026.09/_refresh'
say "seeded: app-logs-2026.09 (40), ingest-events-2026.09 (25), audit-trail-2026.09 (12)"
