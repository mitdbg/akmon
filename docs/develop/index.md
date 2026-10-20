---
audience: developer
type: overview
canonical_id: develop-home
section: develop
visibility: primary
status: active
owner: documentation
---
# Develop AKMON

First identify which system boundary owns the behavior.
A change can involve a source, working state, a derived result, and a published
read, each with different authority. The wrong package can make a retry look
like a new fact or completed work look published.

Start with [Set up a contributor checkout](#set-up-a-contributor-checkout) for locked Python
dependencies, a database-free test loop, a disposable database check, and a
small behavior-to-code-to-test map.

Read [System architecture](architecture/overview.md) first if records,
relationships, evidence, working state, and published generations are new.
Read [Code architecture](architecture/pipeline.md) when you need the
package and call-path map.
Read the [Responsibility map](architecture/responsibility-map.md) when you
already know which behavior you want to change: it lists, for ten common
maintenance tasks, the module that owns the decision, the tests that pin it,
and whether running those tests needs a database.

## Put the change at the boundary that owns it

| Desired behavior | Extension point | Receives | Must return or prove |
|---|---|---|---|
| Read another file format, API, database, or service | [Source adapter](#add-a-source-adapter) | A bounded source definition, credentials when required, and previous saved cursor or revision | Stable record or relationship facts, evidence, and the next cursor or revision |
| Add a record type, relationship type, or allowed relationship shape | [Ontology module](extensions/ontologies.md) | A versioned schema and its module dependencies | A catalog that composes cleanly and accepts only declared graph shapes |
| Package ontology modules, source defaults, checks, and views for reuse | [Profile](extensions/ontologies.md#profile-contract) | Reusable defaults and setup inputs | A deployment scaffold that can be inspected and changed before use |
| Derive a relationship from accepted records | Relationship builder in the [publication pipeline](architecture/pipeline.md) | One fixed graph view and configured matching rules | Evidenced relationship facts sent through normal validation |
| Block an invalid candidate graph | [Consistency check](extensions/consistency-api.md) | One fixed publication candidate | A bounded pass, warning, or blocking failure with evidence |
| Add a programmatic deployment operation | [Python SDK](sdk/index.md) | An explicit deployment and operation input | A typed result or structured failure without hidden authority |
| Change search, traversal, or another graph read | [MCP read interface](../reference/mcp.md) | Reader authority, deployment, generation pin, and bounds | Generation-pinned results, evidence, and explicit truncation or failure |
| Change long-running imports or publication work | [Runtime architecture](architecture/runtime.md) | A durable task identity and retry policy | Stored progress, retries, and a visible terminal result |

For example, a laboratory inventory API begins in a source adapter, not in
graph storage. A new `Sample` record type is an ontology change. Relating
a sample to a paper from an identifier is a relationship-builder change.

## Add a source adapter

Write a reader when no source type covers the format or service:
`okg deployment source-types` lists the ones that exist. Write it as an
external connector, a Python package of its own that imports only from
`okg.deployment`. The working example is
`examples/json-record-connector`.
It reads one JSON file of records and adds each record, plus one node for the
whole file, to the graph.

The reader's `run(ctx)` reads its input and returns a `ConnectorRun`. For each
record it names what the record owns, which is how OKG later finds a deleted
record, and it emits facts only for records that changed since the saved
checkpoint (`examples/json-record-connector/src/okg_example_records/reader.py`,
lines 132-156):

```python
            if not isinstance(row.get("text"), str):
                raise ValueError("record text must be a string")
            digest = _digest(row)
            node_id = "example-record:" + key
            identity, revision = {"id": key}, {"sha256": digest}
            # The containment edge is part of this record's footprint, so a
            # record that disappears from a complete scan takes its edge with it.
            records[key] = RecordEmission(
                primary=RecordNode(node_id, "example_record", identity, revision),
                edges=(EdgeKey(
                    src=COLLECTION_NODE_ID, dst=node_id, edge_type=CONTAINS,
                    source_record_id=identity, source_revision=revision,
                    confidence=CERTAIN,
                ),),
            )
            revisions[key] = digest
            if previous.get(key) != digest:
                facts.append(NodeFact(
                    node_id, "example_record", {"text": row["text"]}, identity, revision,
                ))
                facts.append(EdgeFact(
                    src=COLLECTION_NODE_ID, dst=node_id, edge_type=CONTAINS,
                    source_record_id=identity, source_revision=revision,
                    confidence=CERTAIN,
                ))
```

An incomplete read keeps the old checkpoint and returns no record set, so it
can never delete anything (lines 180-191):

```python
        return ConnectorRun(
            facts=facts,
            completed_scope=complete,
            next_checkpoint=Checkpoint({
                "revisions": revisions, "collection": collection_revision,
            }) if complete else ctx.checkpoint,
            record_set=RecordSet(records) if complete else None,
            health=ConnectorHealth(status="ok" if complete else "partial",
                                   reason=None if complete else "snapshot incomplete or rejected"),
            run_mode="scope_complete" if complete else "scope_incomplete",
            rejected=tuple(rejected),
        )
```

`RecordSource`, below it, is the class the source registry constructs. It
also sets a change probe, a cheap check of whether the input changed;
`okg deployment add-source` refuses a reader without one. `REGISTRATION`
declares the connector's name, settings and output, and
`deployment/schemas/records.yaml` declares its node types.

To add the example to a graph built by [Install OKG](../operate/install.md),
run these from the `okg` directory with its environment active. `<dir>` is a
new directory, given as an absolute path:

```bash
uv pip install -e examples/json-record-connector
python -m okg_example_records.setup <dir>
```

`setup` writes the registration (`connector.json`), sample input
(`records.json`) and the schemas. `uv sync` removes the editable install; run
the first command again after it. Run the connector conformance kit, which
expects `11 passed`:

```bash
uv pip install pytest
python -m pytest -q -p okg.deployment.conformance \
  --pyargs okg.deployment.conformance \
  --okg-connector okg_example_records.conformance:case \
  --okg-conformance-receipt <dir>/conformance.json
```

Its `change_probe` check fails a reader without a change probe, the same
reader `add-source` refuses. Its `narrowing_rejection` check fails unless the
case's input has at least one row the reader rejects and the case names that
row in `expected_rejections`; the example's `data/conformance.json` has one,
`excluded`. Give your own connector's case such a row too.

Then add the source. Without `--apply` it previews, as for a built-in type,
and lists the schema files it will copy from `<dir>/schemas/` into the
deployment:

```bash
okg deployment add-source <name> recs --registration <dir>/connector.json \
  --setting path=<dir>/records.json --apply
```

`add-source` copies the schemas, applies the catalog and reports
`Source 'recs': published` on the first `--apply`, and
`okg search --deployment <name> --query 'alpha original'` finds
`example-record:A`. After you edit `records.json`,
`okg ingest --deployment <name>` publishes the change.

For your own format, copy the package, rename it, and change the parsing in
`reader.py`, the `REGISTRATION` and the schema. Then run the conformance kit
and `add-source` again. The example's
README (`examples/json-record-connector/README.md`)
covers packaging it for a deployment installed without a checkout.

`okg new-source` is retired. It used to write a reader skeleton that was not
a working reader; it now refuses with
`command_retired` and writes nothing. Start from the example instead.

Every reader follows the same rules. Return the same stable identifier for a
record on every read, never one based on its position. Report a partial read
as partial, so that nothing is deleted and the saved position does not move.
Keep credentials out of settings: take the name of an environment variable.
Readers do not write graph tables, publish, or widen their configured scope;
the [pipeline](architecture/pipeline.md) shows where the returned facts go. A
reader built into this repository lives under
`src/okg/substrate/library/sources/`; add it to
[Built-in capabilities](../reference/capabilities.md) only when its tests
exist. [Connect your data](../use/sources/index.md#connect-the-source) covers the
operator's side.

## Preserve the boundaries

The architecture pages define the boundaries every implementation must
preserve: source reads do not grant graph-write authority, accepted facts are
not reader-visible until publication, and a query stays on one authorized
generation. Missing configuration, authority, runtime state, or evidence is
an explicit failure, never an implicit default. These rules matter more than
whether two components currently share a process or database.

## Work from an observable result

Define the result before editing. A source change should name the input record,
the facts and evidence it will emit, the cursor behavior on success and failure,
and the published row or query result that proves the change reached readers.
An ontology change should name both allowed and rejected relationship shapes.
A query change should name the generation, authority, bounds, and failure
response.

The usual development loop is:

1. Reproduce the behavior with the smallest source or graph fixture that keeps
   the real boundary.
2. Add a focused contract test at that boundary, including a failure case.
3. Implement the change in the owning extension point.
4. Run a real import and publication cycle when graph output changes; confirm
   the generation published and the expected record, relationship, or
   metric changed.
5. Update the explanatory page when behavior changes and the owning
   [reference](../README.md#reference) when a public command, field, or result
   changes.

Do not treat compilation, a mocked return value, or a completed background job
as proof that graph readers can see the intended result. Publication and a
generation-pinned read are separate evidence.

## Set up a contributor checkout

Use this path to change OKG itself or inspect its connector contract tests.
End-user installation and chat configuration belong to the
[installation guide](../operate/install.md), not this checkout workflow.

The initial workstation acceptance target is **macOS with Python 3.12**.
The setup, test and database steps below have been executed and timed on
macOS 26.6.2, Apple Silicon (`arm64`), with uv 0.7.11 and Python 3.12.11:
locked setup (from `uv sync` onward), the three-file focused loop (72 passed,
0 skipped), the `arm64` image build (both layers cache-served), the
disposable database node (1 passed, 0 skipped) and cleanup. The unfamiliar human
contributor exercise still needs execution; an agent replay is supplementary
to it, not a substitute. Python metadata currently allows `>=3.12,<3.14`; that
is not evidence that every allowed version or platform has completed this
walkthrough. Windows, Python 3.13, and macOS Intel are unverified here.

### Choose the extension boundary

External connector authors implement the public `okg.deployment.Connector`
contract in their own package: `name`, `profile`, and `run(ctx)`, returning
`ConnectorRun`. Import facts, context, signals, registration and conformance
helpers from `okg.deployment`, not `okg.substrate` or `_internal` modules.
Registration is a package-v2 connector asset, not an entry-point scan.
See the [connector API](../reference/sdk-api.md#connector-protocol) and
[distribution contract](extensions/distributions.md).
[Add a source adapter](#add-a-source-adapter) walks through the working
example in `examples/json-record-connector`.

Core maintainers can edit the implementation modules in the code map below.
An external connector normally needs neither a patch to those modules nor a
new built-in source under `src/okg/substrate/library/sources/`.

### Create the locked environment

Prerequisites: Git, [uv](https://docs.astral.sh/uv/getting-started/installation/),
network access to obtain Python and locked packages, and a shell supporting
these commands (macOS zsh or bash). PostgreSQL, Docker, API keys, a deployed
graph, and chat setup are not prerequisites for the first test loop.

The clone command names its branch and its directory, `akmon`; the rest of
this page runs from that directory.

```sh
git clone -b main https://github.com/mitdbg/akmon.git akmon
cd akmon
git switch -c codex/my-small-change
uv sync --locked --python 3.12 --extra dev
.venv/bin/python --version
.venv/bin/python -c 'import okg.deployment; print(okg.deployment.__file__)'
```

`dev` is a **project extra**, not a dependency group: use `--extra dev`, not
`--dev`. `--locked` checks `uv.lock` against `pyproject.toml` and refuses to
resolve a different dependency set. Do not repair a setup failure by dropping
`--locked`, updating the lock, or installing random packages with pip. Keep
`uv.lock` unchanged. The project is installed editable, so source edits take
effect in this interpreter without reinstalling. Do not use `--all-extras`:
optional model/provider integrations are unnecessary for this loop.

For an existing shared checkout, inspect `git status` and `git worktree list`
first, fetch `origin/dev`, and create your own worktree/branch from it. Never
reuse another contributor's dirty checkout or virtual environment. Running
`uv sync` in the new worktree creates its own `.venv`.

### Run a small database-free loop

Run from the repository root. These exact selectors run serially and clear
both inherited `PYTEST_ADDOPTS` and repository `addopts`; disabling the xdist
and coverage plugins prevents automatic workers or inherited coverage policy.
No `-m` or `-k` filter hides a test.

```sh
PYTEST_ADDOPTS= .venv/bin/python -m pytest -o addopts= -p no:xdist -p no:cov -q -ra \
  tests/substrate/runtime/test_connector_protocol.py \
  tests/substrate/runtime/test_connector_registration.py \
  tests/substrate/ingest/test_planning_boundary.py \
  --junitxml=.scratch/contributor-focused.xml
```

Read the final summary: skipped, deselected, failed, or collection-error cases
are not executed passing checks. JUnit is a durable result, not permission to
ignore that summary. The named files exercise the connector contract and the
already-landed deterministic planner; neither requires a scheduler extraction.

| Representative behavior | Implementation to read | Test to run or extend |
|---|---|---|
| Connector run/context/signal semantics | `src/okg/deployment/__init__.py` public exports; `src/okg/deployment/_internal/protocol.py` implementation | `tests/substrate/runtime/test_connector_protocol.py` |
| Refuse invalid connector registration or SDK bounds | `src/okg/deployment/_internal/registration.py` | `tests/substrate/runtime/test_connector_registration.py` |
| Preserve predecessor order, exclusions and cycle refusal | `src/okg/substrate/ingest/planning.py`: `plan_ingest_sources`, `topo_layers` | `tests/substrate/ingest/test_planning_boundary.py` |
| Create required graph tables and remove legacy tables | `src/okg/substrate/db/migrate.py` and its SQL inputs | `tests/substrate/test_migrate.py::test_migrate_creates_expected_tables` (database required) |
| Change a derived-artifact dependency rule: which tasks are admissible, which upstreams must be known, the deterministic order, or the canonical plan hash | `src/okg/substrate/derived_artifacts/dependency_planning.py`: `build_task_map`, `plan_dependency_graph` | `tests/substrate/test_derived_dependency_planning.py` (no database) |

For a first small change, pick one agreed behavior in the registration or
planning row. Read the existing rejection/edge-case assertion, reproduce it,
add a test describing the intended change, then modify the owning function.
During that edit run its exact node, for example:

```sh
PYTEST_ADDOPTS= .venv/bin/python -m pytest -o addopts= -p no:xdist -p no:cov -q -ra \
  tests/substrate/ingest/test_planning_boundary.py::test_cycles_remain_loud_and_topology_helpers_return_none
```

Run the three-file loop again before review. Read the root `AGENTS.md` and
`pact/AGENTS.md` before a nontrivial change; read the
[pipeline map](architecture/pipeline.md) before changing graph-building code.
Use an existing PACT or author the smallest change record, obtain independent
design/code review, attach proof and run `okg pact check <id> --strict`.
Then run the tests that cover the files you
changed, not the whole suite. Do not run an entire test
directory or automatic workers as a first attempt. Coordinate heavy checks on
a shared machine. A focused local pass does not replace required hosted CI.

### Run a disposable PostgreSQL check

Do this only for a change that needs database behavior. This smoke check
proves migrations create the expected schema; it does not prove publication
or every database operation. A change to graph output also needs a real import,
publish, and generation-pinned read as described in the pipeline map.

Prerequisites: a working Docker engine, capacity to build the repository's
extended PostgreSQL 17 image, and free loopback port `5661`. Stock `postgres:17`
lacks required extensions. The following uses the existing `ops/pg/Dockerfile`
and initialization scripts with a separate container and anonymous data volume.
It does not start or reset the shared `okg-pg` Compose service.

Docker builds for its own architecture: `amd64` on Intel, `arm64` on Apple
Silicon. Beyond the digest-pinned `timescaledb-ha` base image, the build
downloads one file: upstream's `pg_textsearch` 1.5.1 release package for that
architecture, checked against the sha256 pinned in `ops/pg/Dockerfile`. The
build then asserts that the installed package, the control-file version, the
`pg_textsearch--<version>.sql` script and the `pg_textsearch.so` library all
match `ARG PG_TEXTSEARCH_VERSION`. An `amd64` build on a lab host (podman)
produced a server reporting `pg_textsearch` 1.5.1. The `arm64` package is
pinned the same way but its build is unverified here; an earlier macOS
Docker Desktop (`linux/arm64`) build shipped the base image's 1.3.0. macOS
Intel is unverified.

```sh
docker build -t okg-contributor:local ops/pg
docker run -d --name okg-contributor-test --shm-size=2g \
  -p 127.0.0.1:5661:5432 \
  -e POSTGRES_PASSWORD=okg -e POSTGRES_DB=okg \
  -e 'POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8' \
  -v /home/postgres/pgdata/data \
  -v "$PWD/ops/pg/initdb:/docker-entrypoint-initdb.d:ro" \
  okg-contributor:local postgres \
  -c shared_preload_libraries=timescaledb,pg_cron,pg_textsearch \
  -c cron.database_name=okg -c max_connections=100 \
  -c max_worker_processes=32
```

`max_worker_processes=32` gives TimescaleDB room for one scheduler per
database plus its jobs; with PostgreSQL's default of 8, a few databases for okg
on one container make job launches fail and retry without limit (see
`ops/pg/README.md`).

The mounted scripts create the extensions twice: once in the `okg`
database and once in `template1`, so every database later created on this
container -- including the scratch databases several test fixtures build
with a bare `CREATE DATABASE` -- inherits them. Without the `template1`
pass those tests fail with `access method "bm25" does not exist`. `pg_cron` stays in
`okg` alone: PostgreSQL accepts it only in the database
`cron.database_name` names.

If the name or port is occupied, choose a fresh pair and update every command;
do not remove the existing container. Inspect startup and repeat the readiness
probe until it succeeds, or diagnose the log if startup fails:

```sh
docker logs okg-contributor-test
docker exec okg-contributor-test pg_isready -U postgres -d okg
```

The test fixture creates `okg_test_contributor` on this disposable server,
then migrates it. Set only `OKG_TEST_DSN`; `OKG_DSN` points to deployments and
is deliberately ignored by these fixtures. Never set
`OKG_TEST_DSN_TRUST_AS_EPHEMERAL` to bypass database ownership guards.

Use the URL form, `postgres://user:password@host:port/dbname`. libpq also
accepts the keyword form (`host=... dbname=...`), but the graph store selects
its backend by URL scheme, so a scheme-less DSN used to be read as a SQLite
file path: the publish path then wrote a file named after the whole
connection string. Both `require_okg_test_dsn` and the store's connect refuse
the keyword form now, naming the URL to set instead.

There is no default. With neither `OKG_TEST_DSN` nor `OKG_TEST_DSN_TEMPLATE`
set, every database test skips and names the variable to set; nothing
connects. The fixtures truncate, drop and recreate, so they will not guess at
a database. A maintainer who
deliberately wants the shared `okg-pg` Compose service must name it in
`OKG_TEST_DSN`, and should not: another session may be using it.

Two things outside the fixtures still supply a database. The QA lane runner
`scripts/qa.py` injects its own worker-isolated default, which is how the
lanes work; and the opt-in live-deployment fixtures, which run only when you
set `OKG_RUN_LIVE_DEPLOYMENT_TESTS=1`, read an operator's own graph. Neither
runs during a plain `pytest` invocation with the variables unset.

```sh
OKG_TEST_DSN=postgres://postgres:okg@127.0.0.1:5661/okg_test_contributor \
PYTEST_ADDOPTS= .venv/bin/python -m pytest -o addopts= -p no:xdist -p no:cov -q -ra \
  tests/substrate/test_migrate.py::test_migrate_creates_expected_tables \
  --junitxml=.scratch/contributor-postgres.xml
```

Require **1 passed, 0 skipped, 0 failed/errors**. If PostgreSQL is unavailable,
`tests/substrate/conftest.py` can skip this test and still exit zero: that is
missing database proof. An unreachable database can also produce a setup error
(the Linux replay did); that likewise means no database assertion ran. Read
`-ra` and JUnit. An assertion failure is different:
the check executed and found wrong behavior. Extension or migration errors
need diagnosis; do not turn them into skips. This fixture exempts `pg_cron`
in the secondary test database because cron is instance-scoped; it does not
certify scheduled jobs.

After collecting results, clean up only the disposable container and its
anonymous volume, including after a failed test:

```sh
docker rm -f -v okg-contributor-test
```

No shared Compose volume, deployment database or other contributor's resources
should be removed. Keep the image for another run or remove the specific
`okg-contributor:local` tag when no longer needed.

### Finish review and record remaining acceptance

The full preflight also needs the pinned `actionlint` binary; the Python dev
extra does not install it. Follow the existing
workflow installation step (`.github/workflows/test.yml`)
(version `v1.7.12`). The verified Linux x86_64 setup is:

```sh
mkdir -p .scratch/actionlint
curl -sSfL https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_amd64.tar.gz \
  -o .scratch/actionlint/actionlint.tar.gz
tar -xzf .scratch/actionlint/actionlint.tar.gz -C .scratch/actionlint actionlint
.scratch/actionlint/actionlint --version
```

On macOS, use the matching macOS release artifact at that same version: on
Apple Silicon that is `actionlint_1.7.12_darwin_arm64.tar.gz`, which was
fetched, checksummed and run (`1.7.12`, `darwin/arm64`) during the macOS
replay. Do not run the Linux binary there. Keep the binary at `.scratch/actionlint/actionlint`
or set the existing `ACTIONLINT_BIN` environment variable to its executable path.
A missing binary makes preflight refuse, rather than silently skip workflow lint.
Also run `.venv/bin/python scripts/check_docs_site.py` and
`.venv/bin/python scripts/gen_llms_txt.py --check` when editing this guide:
the machine index includes each page's opening description.

Use a PR against `dev`, the repository PR template, `git diff --check`, and
`.venv/bin/python scripts/ci/agent_preflight.py --pact-id <id> --pr-body <body-file> --run`
through the locked interpreter. Record exact test counts and unresolved checks;
a missing approval or pending hosted run is not a pass. See the
agent reporting contract (`docs/runbooks/agents/reporting.md` in the repository).

Still to collect:

1. **macOS/Python 3.12:** Apple Silicon is done. A fresh checkout at commit
   `df0ad8b8a8` on macOS 26.6.2 (`arm64`, uv 0.7.11, Python 3.12.11, lockfile
   sha256 `dff663ab…`) executed the locked setup, the three-file focused loop,
   the `arm64` image build (both layers cache-served) and container start, the database node, the
   documented absent-port negative case and the cleanup, with the counts given
   above. macOS Intel is still open:
   record its macOS version, `uv --version`, Python patch version, checkout
   commit and lockfile hash, execute the same steps in a fresh checkout, and
   record command output, executed/skipped/failed counts and elapsed
   setup/test time. Record failures rather than claiming another architecture
   covers them.
2. **Unfamiliar human contributor:** give a developer who has not maintained
   this code only this guide and one agreed small registration/planning task.
   Record their decisions, every file visited/changed, exact commands/results,
   elapsed time, and all assistance or instruction corrections. Retain their
   diff and test evidence. An agent replay is supplementary, not this exercise.
3. **External example:** the connector example is
   `examples/json-record-connector`; [Add a source adapter](#add-a-source-adapter)
   gives its steps, run on a fresh clone. Keep release/installation and chat
   decisions with their existing owners.
