# Role-enforcement tests

Verify the three substrate roles (`app_rw`, `app_ro`,
`catalog_admin`) have the exact grants the design specifies — and
no more.

These are drafted at task `0.h.1`; they run against a freshly-
migrated Postgres 17 with all four extensions installed and the
`okg.*` schema in place.

## What each test proves

| File | Role | Asserts (positive) | Asserts (negative) |
|---|---|---|---|
| `app_rw_grants.sql` | `app_rw` | INSERT on `source_watermarks`, `link_cache`, `mcp_trace_events`; UPDATE on `source_watermarks` | INSERT denied on `subtypes`, `narrowings`, `aliases`, `invariants`, `catalog_versions`; DDL denied (CREATE TABLE, ALTER TABLE, TRUNCATE); DELETE from facts denied |
| `app_ro_read_only.sql` | `app_ro` | SELECT on the agent-facing `okg.v_*` view layer | SELECT on physical tables denied; INSERT/UPDATE/DELETE denied on every `okg.*` table; DDL denied |
| `catalog_admin_grants.sql` | `catalog_admin` | INSERT on `catalog_versions`, `subtypes`, `narrowings`, `aliases`, `invariants`; UPDATE on `catalog_versions` | INSERT denied on `node_facts`, `edge_facts`, `source_watermarks`, `graph_generations`; DDL denied |

Fail-loud discipline: every `assert_ok` and `assert_denied` block is wrapped in `DO $$ ... EXCEPTION ... RAISE EXCEPTION $$`, so a silent grant regression (e.g. an unexpected INSERT succeeding) aborts the test with `FAIL:` rather than being swallowed by transaction rollback. `\set ON_ERROR_STOP on` ensures the script aborts on the first unexpected failure.

The `okg_mcp` MCP read-pool role is covered separately by
`tests/sql/schema/schema_contracts.sql`, which asserts the read boundary added
by `constrain-mcp-and-role-bootstrap` (#366): `okg_mcp` is denied SELECT on the
sensitive raw tables (`node_facts`, `edge_facts`, `nomos_config_sets`,
`poros_outbox_messages`, `federation_mailbox_messages`, `source_cursor_blobs`)
and on arbitrary newly-created `public` tables, while keeping its explicit
graph/catalog/status allow-list. The behavioural counterpart lives in
`tests/substrate/test_least_privilege_mcp.py`.

## How to run

```sh
docker compose -f ops/pg/docker-compose.yaml up -d --build   # if not already running
export OKG_TEST_DSN=postgres://postgres:okg@localhost:5433/okg_test_schema_assertions
python scripts/run_sql_assertions.py
```

Each test is self-contained: it uses `SET LOCAL ROLE` to adopt the
role under test, runs a series of expected-pass and expected-fail
operations, and raises an exception on the first unexpected
outcome. Exit code 0 = all assertions held.

The tests assume the DB already has the v1 schema (`okg migrate`
has run) and a fresh, empty state. On a live deployment they'll
write and roll back small sentinel rows; they're safe but noisy.

## Why not pytest fixtures

These tests exercise role-level permissions, which require a real
Postgres connection as the specific role. Writing them as
`.sql` files keeps the assertion surface in SQL close to the
intent (a row was or wasn't allowed); a Python wrapper only adds
translation.

A thin Python runner may land at M5; for now, `psql -f` is the
test harness.
