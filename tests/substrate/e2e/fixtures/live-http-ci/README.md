# live-http-ci — a CI fixture, not a deployment

This tree is copied into a temporary directory by
`tests/substrate/e2e/test_live_relation_ci_walk.py`. Nothing here ships and
nothing here is operated.

## What the test proves

That the generic HTTP live-relation adapter works against a **real
third-party HTTP service** it was not written for — end to end, in hosted CI,
with no stub anywhere in the path.

The service is [Glances](https://github.com/nicolargo/glances) in web-server
mode, a pinned dev dependency that the test starts and stops itself. It was
chosen for how much it differs from OpenSearch, the system the adapter was
written against:

| | OpenSearch | Glances |
| --- | --- | --- |
| method | `POST` | `GET` |
| request body | a slotted JSON document | none at all |
| response root | an object, rows at `hits.hits` | a top-level JSON array |
| credential | a token | none |

Pointing the shipped adapter at it found three refusals that no stub would
have: `rows_path` could not name the document root, the node cap was charged
on the whole parsed body rather than the sliced rows, and a `GET` may declare
no argument. The first two are fixed in #2166; the third is why both
relations here take none.

The walk asserts, through the registered MCP tools over a real client session
against a real published generation:

* `inspect` lists both relations with their argument schemas;
* `expand(live=cpu_per_core)` returns one typed row per logical core;
* `expand(live=top_processes)` returns rows, `policy: "inline"` and not
  degraded to a digest;
* a relation the Nomos bundle does not name is denied and the adapter is
  entered **zero** times;
* one call sample per call lands in `okg.call_sample_hot`, and
  `okg.call_sample_warm` is empty — which is what proves the declared
  `retention_class` routes the write.

It also proves a deployment can declare a live relation on a **shared**
subtype without editing the shared ontology: `compute_env`'s `Machine` is
untouched, and `schemas/machine_live_relations.yaml` here is what the
composer reads.

## Running it locally

It needs PostgreSQL and nothing else — the test starts Glances on a free
loopback port itself, waits for it, and kills it in a `finally`. Point
`OKG_TEST_DSN` at a disposable database built from **the repository's own
image** (`docs/develop/index.md` has the container command):

```bash
OKG_TEST_DSN="postgresql://postgres:okg@127.0.0.1:5704/okg_test" \
  uv run pytest tests/substrate/e2e/test_live_relation_ci_walk.py
```

There is no opt-in flag. If Glances cannot start, the test **fails** and says
so; it never skips, because a skipped arm reads as a passed one.

**Use the repository's image, not a stock `postgres` one.** Two things break
on a bare image, and the second is the confusing one:

* `CREATE EXTENSION timescaledb` fails outright with `extension
  "timescaledb" is not available` — loud, and easy to diagnose.
* the `okg_mcp` role does not exist, and the MCP server's read pool cannot
  log in. That surfaces as a **`PoolTimeout`** with nothing in it naming a
  role, so it reads like a hung database rather than a missing grant.
  `ops/pg/initdb/00-extensions.sql` creates `okg_mcp` without LOGIN (issue
  #3273); `ensure_builtin_dev_logins` in `tests/substrate/conftest.py` gives
  it LOGIN with the built-in test-server password when the session's test
  database is built. Managed deployments supply `OKG_MCP_RO_DSN` instead.

## The files

| | |
| --- | --- |
| `deployment.yaml` | modules, Nomos posture, agent surface, and the two `live_bindings`. The port in `base_url` is a placeholder — the walk rewrites it to the port it just bound, so two runs on one runner cannot race. |
| `schemas/machine_live_relations.yaml` | the deployment-local declaration of `cpu_per_core` and `top_processes` on `machine`. |
| `nomos_external_probe.json` | deny by default; one allow rule naming the action, the service principal **and both relations**, so a third relation reusing the action arrives denied. |
| `source_registry.yaml` | one source, `compute_env`'s existing `LocalEnvSource`, which mints the `machine` node. No new source adapter. |
