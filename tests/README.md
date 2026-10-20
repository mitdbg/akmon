# Tests

Test classification for the OKG repo, per the `deployment-qa`
capability (proposal `qa-stabilize-main`).

## Four test classes

Tests are classified by **intent of assertion**, not by directory.
Every test under `tests/` belongs to exactly one of:

| Class | What it asserts | Where it tends to live |
|---|---|---|
| **functional** | substrate pipeline behavior — sources, projector, enrichers, publisher, jobs queue, MCP tool semantics | `tests/substrate/test_*.py` (most of them); `tests/<deployment>/test_*.py` |
| **database** | schema correctness, gates, invariants, ledger integrity, advisory-lock fencing, narrowing-trigger acceptance, view-contract conformance | `tests/substrate/test_invariants_*.py`, `test_publisher.py`, `test_narrowing*.py`, `test_gates.py`, `tests/sql/` |
| **viz** | operator UI boundaries and rendered behavior when an active app exists | No active lane; future single-app tests will get their own active path |
| **performance** | wallclock and throughput — publish-cycle wallclock, projector batch throughput, MCP latency | `tests/<deployment>/perf/test_*.py` |

### Pattern → class mapping

| If the test asserts… | Class |
|---|---|
| a row count in `nodes_live` / `edges_live` | functional |
| a `narrowing_violation` rejection at insert | database |
| a unique-constraint failure | database |
| `okg.graph_generations.status` transitions to `published` | database |
| a Phase-B invariant evaluates green / red | database |
| `view_cites_*` narrowings hold for a refreshed view | database |
| projector emits derived edges from cache-miss extraction | functional |
| MCP `search` returns expected hits | functional |
| Playwright canvas is painted with N nodes | viz |
| a viz HTTP route returns 200 + non-empty body | viz |
| publish-cycle wallclock under a bound | performance |
| MCP `traverse_path` p95 latency under a bound | performance |

## Directory inventory

Classification is by intent of assertion (the table above), not directory;
this map is descriptive. Top-level directories under `tests/`:

| Path | Class | Notes |
|---|---|---|
| `tests/substrate/` | functional + database | the bulk of the test code; `test_invariants_*.py`, `test_publisher.py`, `test_narrowing*.py`, `test_gates.py` are database; the rest are functional. Property tests under `tests/substrate/property/` |
| `tests/catalog/` | database | catalog composition / apply tests |
| `tests/sql/` | database | SQL-shape tests |
| `tests/views/` | database | views-contract tests |
| `tests/security/` | database + functional | security and access-boundary tests |
| `tests/pact/` | functional | PACT protocol tests (substrate-owned; in the coverage lane) |
| `tests/optimizer/` | functional | Althea optimizer and rewrite-safety tests |
| `tests/ci/` | functional | CI-artifact ingestion tests |
| `tests/scripts/` | functional | repo-script tests |
| `tests/docs/` | functional | docs-site contract and guidance-consistency tests |
| `tests/acceptance/`, `tests/e2e/`, `tests/integration/` | integration | end-to-end and cross-component flows |
| `tests/deployments/` | functional | deployment-config and bootstrap tests |
| `tests/okg-workspace/`, `tests/okg-ci/` | functional + performance | deployment-owned dogfood/integration suites (slow); `perf/` subdirs hold performance tests |
| `tests/af_pubs/`, `tests/af_pubs_pdf_index/` | functional + database | af_pubs deployment-owned tests (skip when the af_pubs deployment is absent) |
| `tests/bench/` | performance | benchmark-harness tests |
| `tests/pytest/`, `tests/fixtures/`, `tests/_template/` | **NOT TESTS** | shared helpers (`tests.pytest.optional_deployments`), fixtures, and the new-test scaffold |

## How to run

The canonical local interface is the QA lane runner. Lanes are defined in
[`qa_lanes.yaml`](qa_lanes.yaml), and every run writes a JSON report under
`.scratch/qa/`.

```sh
uv run python scripts/qa.py list
uv run python scripts/qa.py smoke          # collection + tiny sanity lane
uv run python scripts/qa.py fast           # broad no-DB lane, xdist-enabled
uv run python scripts/qa.py substrate-db   # Postgres-backed substrate lane
uv run python scripts/qa.py views-core
uv run python scripts/qa.py coverage
uv run python scripts/qa.py full-ci --dry-run
```

Semantic classes still describe what a test asserts. Execution lanes describe
when and how the test runs. For example, a functional test that uses
`clean_runtime` belongs to the Postgres-backed execution lane even if it lives
under `tests/substrate/`.

**First run:** bring up the dev Postgres compose stack. The QA runner
defaults Postgres lanes to worker-isolated test databases named
`okg_test_{workerid}` on the same instance and installs the
substrate's required extensions; you do NOT need to run `createdb`
or set `OKG_TEST_DSN` manually. The runner marks only these generated
default worker databases as ephemeral so stale rows from prior failed
runs can be cleaned safely. `OKG_DSN` is reserved for live deployment
access and is never a test database fallback.

That default comes from the QA runner (`scripts/qa.py`), not from the
fixtures. Running `pytest` directly, you set `OKG_TEST_DSN` or
`OKG_TEST_DSN_TEMPLATE` yourself: the fixtures have no default of their
own, so with neither set every database test skips and names the
variable. Point it at a database you own and may destroy — the fixtures
truncate, drop and recreate.

```sh
docker compose -f ops/pg/docker-compose.yaml up -d
uv run python scripts/qa.py smoke
uv run python scripts/qa.py substrate-db
```

For custom DB placement, set `OKG_TEST_DSN_TEMPLATE` with
`{workerid}` substitution:

```sh
OKG_TEST_DSN_TEMPLATE=postgres://postgres:okg@localhost:5433/okg_test_{workerid}
```

The `substrate-db` lane runs the parallel-safe DB subset with xdist
and runs `serial_db` tests separately. If you export a single
`OKG_TEST_DSN`, the runner refuses DB xdist because every worker would
share the same database; use `OKG_TEST_DSN_TEMPLATE` or pass
`--serial` for debugging.

**Per-class runs** still work via `pytest -k`, but prefer lanes for routine
work because they carry marker policy, runtime budgets, and reports:

| Class | Command |
|---|---|
| functional | `pytest tests/substrate -k 'not test_invariants and not test_publisher and not test_narrowing and not test_gates' && pytest tests/af_pubs tests/af_pubs_pdf_index tests/optimizer tests/ci` |
| database | `pytest tests/substrate -k 'test_invariants or test_publisher or test_narrowing or test_gates' && pytest tests/sql tests/views` |
| performance | `pytest tests/*/perf -m slow` |

Tests that assert latency, storage size, throughput, or explicit budget
behavior must carry `@pytest.mark.perf` even when they also need Postgres.
Correctness lanes such as `fast`, `substrate-db`, and `views-core` exclude
`perf`; the `perf` lane is the explicit place for these checks.

Optional deployment tests must use
`tests.pytest.optional_deployments` for availability checks, path injection,
collection ignores, and `optional_deployment` marking. Missing optional
deployments should skip intentionally and appear in the QA lane report, not
raise import or collection errors.

### Coverage Floors

Coverage is substrate-scoped. `[tool.coverage.run]` measures
`okg.substrate`, and `[tool.coverage.report] fail_under` in
`pyproject.toml` matches the integer in
[`.coverage-baseline`](.coverage-baseline). The `coverage` lane runs
`tests/substrate` plus the substrate-owned protocol tests in
`tests/pact`, because those directories cover code that lives under
`okg.substrate`. (`tests/okg-workspace` was dropped from the coverage
lane: it is a slow dogfood/integration suite that pushed the lane past
its CI timeout — so coverage.json was never produced — and it adds
little `okg.substrate` line coverage.) The CI workflow ratchets the
baseline up on merges to `main` when observed substrate coverage
exceeds it.

The global floor intentionally excludes external-service adapter shells and
operational runtime entrypoints whose behavior is better checked by source
contract tests, deployment smoke tests, or runtime-specific gates. Critical
substrate areas still have explicit floors in `tests/conftest.py`.

The current global floor is 70% (retuned to the substrate+pact scope
after `tests/okg-workspace` was removed from the coverage lane). Pytest also enforces these
substrate area floors whenever a coverage-enabled run measures
`okg.substrate`: catalog 75%, CLI `main.py` 60%, MCP 80%, library
enrichers 70%, and `_http_cache.py` 75%.

## Operator UI Tests

The old Playwright/Vite suites were archived under
`archive/ui/legacy-viz-2026-06-17/`, now only in git (commit `3253beb069ba`). The
old UI is gone for good, so no lane
guards that archive any more. When the single operator app lands, add its browser tests
under a fresh active path and update this section with the new gate command.

## Property + Mutation testing

Pure substrate functions whose invariants are stateable have
[`hypothesis`](https://hypothesis.readthedocs.io/) property tests
under [`tests/substrate/property/`](substrate/property/). These
sweep randomized inputs against asserted invariants
(idempotence, round-trip, monotonic visibility, row-cap respected,
etc.) and surface bugs that example-based tests miss.

Mutation testing via [`mutmut`](https://mutmut.readthedocs.io/)
captures the gap between coverage (lines executed) and assertion
(outputs checked). Run `mutmut run` to refresh; baseline is at
[`.mutmut-baseline.json`](.mutmut-baseline.json). See
[the runbook](../docs/runbooks/quality/ci-and-qa.md) for the
workflow + triage rule.

If a property test or mutation finds a real bug in production,
the fix lives in production code, not in the test. Loosening
the property to silence the signal is forbidden.

## Adding a new test

1. Decide the class from the table above (by what the assertion is
   about, not where the test happens to live).
2. Place the test under the existing convention (substrate /
   `<deployment>` / `playwright/specs` / `<deployment>/perf`).
3. If the test asserts a deployment-specific signal, scope it under
   `tests/<deployment>/`. If it asserts substrate-level behavior,
   put it under `tests/substrate/`.
4. Per-deployment performance tests go under
   `tests/<deployment>/perf/` because bounds depend on data shape.

## QA gate reference

The `deployment-qa` capability defines a per-deployment QA gate
(`docs/runbooks/quality/ci-and-qa.md`). The gate runs all four test classes
plus a contract validator. See
[the runbook](../runbooks/quality/ci-and-qa.md) for invocation.
