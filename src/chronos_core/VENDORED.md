# Vendored Chronos Core

This package is copied from `mitdbg/chronos` commit
`5d73c55`.

OKG vendors `chronos_core` so GitHub Actions, academic-researcher installs,
and private OKG deployments do not need cross-repository access to a
private Chronos repository. Chronos remains the upstream source of
truth for branch/checkpoint/query semantics.

Refresh procedure:

```bash
rm -rf src/chronos_core
mkdir -p src
git -C ../chronos archive <chronos-ref> packages/chronos-core/src/chronos_core | tar -x -C /tmp
mv /tmp/packages/chronos-core/src/chronos_core src/chronos_core
rm -rf /tmp/packages
```

After refreshing, run the Chronos-backed OKG tests and update this
file with the new source ref.

## OKG-local additions (NOT in upstream — must be re-applied on refresh)

This copy is NOT a pure mirror of `5d73c55`: it carries an OKG-local
backward-compatibility shim that upstream Chronos does not have.
`branching/_interval_backend.py`'s
`_ensure_interval_physical_storage_columns` (called from `register_table`)
migrates OKG's *already-deployed* interval backing tables to the current
shape — converting a legacy INTEGER `deleted` tombstone to BOOLEAN and
adding `writer_segment_id`. Verified: upstream `main` (= `5d73c55`) has
`register_table` but none of this shim
(`_ensure_interval_physical_storage_columns`, the `deleted` ALTER, or the
`writer_segment_id` ADD are all OKG-only).

This index-drop fix is part of that shim: before the `deleted` INTEGER->
BOOLEAN `ALTER`, it drops any partial index whose predicate references
`deleted` (e.g. a legacy `... WHERE (deleted = 0)` visibility index) —
which otherwise aborts the ALTER with
`operator does not exist: integer = boolean`, blocking the upgrade of any
deployment provisioned by a pre-boolean-`deleted` shape (OKG hit this on
okg-workspace and okg-ci). See OKG PR #101.

**On refresh:** upstream will NOT carry this shim, so after copying from
upstream you MUST re-apply the OKG-local
`_ensure_interval_physical_storage_columns` function **and** its
`register_table` call sites (including this index-drop), or OKG
deployments on the legacy shape can no longer upgrade. Do NOT delete it
expecting upstream to provide it.

## OKG-local patch: session name and redacted DSN errors (okg #2027, #2285)

Local edits in `branching/sql_adapters.py`, to be offered upstream to
`mitdbg/chronos`. Neither imports `okg`.

1. `PostgresDatabaseAdapter.connect` used to pass
   `application_name="chronos-branching"` to `psycopg.connect` on every
   call. A keyword argument overrides the value in the connection string,
   so a caller that named its own sessions in the DSN (OKG stamps
   `application_name=okg-chronos-store`) lost that name. The adapter now
   passes its name only when the DSN, in URL or keyword form, names none
   (`_dsn_application_name`, `CHRONOS_APPLICATION_NAME`).
2. `connect_sql_database` and the SQLite adapter used to quote the whole
   connection string in their refusals, password included. They now name
   only the scheme, host, port, database and user (`_describe_dsn`).
3. `PostgresDatabaseAdapter.connect` let `psycopg.connect` re-raise libpq's
   parse errors, which quote the text around the fault -- often the
   password (`missing "=" after "<word>"`, `invalid percent-encoded token`).
   The adapter now parses first (`_parse_postgres_dsn`) and refuses an
   unparseable DSN with a `psycopg.ProgrammingError` naming only
   `_describe_dsn`'s fields, with no chained error. Connection failures
   (network, authentication) still carry the server's message.
4. `_describe_dsn` read a URL with `urlparse`, so a raw `/`, `?` or `#` in
   the password ended the authority early and the part after it was shown
   as the database (`mysql://u:pw/TAIL@h/d` -> `dbname=TAIL@h/d`). When an
   `@` follows the authority it now names the scheme alone. A raw `[` or
   `]` in the password made `urlparse` raise `Invalid IPv6 URL` from inside
   the describer; that is now caught, and the string is described without
   any of its content.

Not patched here (okg #2285): `transaction/shim_postgres.py` `shim_id`
embeds the full DSN (okg does not import `chronos_core.transaction`), and
`pg_session.dsn_options` logs libpq's parse error at DEBUG level for
callers other than this adapter.

**On refresh:** if upstream has not taken these, re-apply all four and keep
the tests in `tests/substrate/test_db_pool_health.py` and
`tests/substrate/test_dsn_form_routing.py` green.

## OKG-local addition: delete markers keep only a column set (okg #3284)

Local edit in `branching/_interval_backend.py`. It does not import `okg`.

Upstream writes a delete marker (a physical row with `deleted = TRUE`) by
copying every column of the previous row, so a deleted row's whole payload
(`attrs`, `embedding`, ...) stays on disk for as long as the marker does.
The OKG copy adds a process-wide option:

- `set_tombstone_keep(table, columns)` and `set_tombstone_keep_default(columns)`
  (module functions; `tombstone_keep_settings()` reads them back). A table
  with a keep set, or with none while a default is set, writes markers that
  keep the primary key, the keep set and the interval columns (`live_lo`,
  `live_hi`, `writer_segment_id`, `deleted`); every other column is NULL.
  With neither set (the upstream state), markers are a full copy as before.
- Backend helpers `_tombstone_keep`, `_tombstone_row`, `_tombstone_sql` and
  `_tombstone_null_assignments`, used at every place a marker is written:
  `_splice_row` (single-row exact update, inherited tail, and the general
  loop, including left and right remainders), the single-row inherited-tail
  statement (`_try_splice_single_inherited_tail_postgres`: inherited marker,
  exact delete `SET`, direct marker), `_splice_rows_batch` (inherited-tail
  fast path and the general path: replacement, left/right remainders,
  direct markers) and `_splice_rows_batch_mixed`. A remainder copies its old
  row and drops the columns only when that old row is itself a marker; a live
  remainder keeps every column. Each edit carries an `okg#3284` comment.
- Not changed: `_try_update_private_schema_table_in_place` (schema branching
  only; OKG runs with schema branching off).
- Known non-clearing path: the set-based UPDATE
  `_try_update_full_table_with_batch_splice` (the `UPDATE ... SET` path for
  a whole table) copies the left and right remainders of an existing physical
  row verbatim (`old_cols`, with its own `deleted` flag). When that row is a
  marker written before this option (or before the backfill cleared it), the
  remainder is a marker that still carries the full payload. It never adds
  payload to a key-only marker; the backfill script clears such remainders
  like any other marker.

- With `enable_schema_branching` on, a configured keep set raises
  `BranchingError` at the first splice that could write or copy a marker,
  instead of writing a key-only marker that the private-schema in-place
  update path could revive as a row of NULLs.

OKG sets the keep sets at import of
`okg/substrate/graph_store_internal/schema.py` and in every
`ChronosGraphStore` construction. Existing markers are cleared
only by `scripts/storage/clear_tombstone_payloads.sql`, run by hand.

**On refresh:** re-apply the module functions, the helpers and every call
site listed above, or OKG deletes write full-payload markers again (no test
outside `tests/substrate/test_chronos_tombstone_payload.py` would notice).
