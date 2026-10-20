# Write a JSON record connector

This package is a complete reader for a bounded JSON snapshot. Its runtime code
uses only `okg.deployment` — no `okg.substrate` import anywhere. Its packaging
module uses the public `okg.distributions` producer. The source registry
constructs `okg_example_records.reader.RecordSource`, which adapts
`RecordReader.run(ctx)` to OKG's existing ingestion pipeline.

## Add it to a graph you already have

On a graph built by
[`docs/operate/install.md`](../../docs/operate/install.md), from the `okg`
directory with its environment active. `<dir>` is a new directory, given as an
absolute path; `<deployment dir>` is `~/.local/share/okg/deployments/<name>/`,
or `$XDG_DATA_HOME/okg/deployments/<name>/` when that variable is set:

```bash
uv pip install -e examples/json-record-connector
python -m okg_example_records.setup <dir>
okg deployment add-source <name> recs --registration <dir>/connector.json \
  --setting path=<dir>/records.json --apply
```

`okg_example_records.setup` writes the connector registration
(`connector.json`), the sample input (`records.json`), the schemas under
`schemas/` and the fixture states under `data/`. `add-source` copies the
schemas next to `connector.json` into the deployment's `schemas/` directory
and applies the catalog; without `--apply` its preview lists each file it
will copy. It refuses, and copies nothing, if the deployment already has a
different file under the same name, and refuses a schema file that is a
symbolic link. If a later step fails, the copied files stay in the deployment
and the failure lists them; the next run finds them unchanged. The first
`--apply` publishes 3 records
and 2 relationships, and
`okg search --deployment <name> --query 'alpha original'` finds
`example-record:A`. If a step fails, fix the cause and run the same command
again. Copy `<dir>/data/edited.json` or `<dir>/data/deleted.json`
over `records.json` and run `okg ingest --deployment <name>` to publish the
change. A later `uv sync`
removes the editable install; install it again after one.

`add-source` accepts the reader because `RecordSource` sets a change probe
(`change_probe_kind = "content_hash"` and a `ContentHashProbe` over the input
file). Without one it refuses the reader as "not runnable here".

The rest of this page installs the example from wheels, outside any checkout.

## Last pinned build

The wheel route below was last run against **okg commit
`88fb064f883f1bdd73e27c587042b8f1a63a5503`** on macOS arm64 with Python
3.12.11. Its digests are stale: the example's code has changed since (it now
sets a change probe), so a rebuild produces different ones. Re-pin them from
your own build rather than assuming the old values. The wheel route and the
measured table at the end of this page have not been re-run since the change
probe was added.

## What is proven, and what is not

Proven from wheels, outside any checkout:

- The example builds, installs beside the `okg` wheel into a clean virtual
  environment, and imports only from that environment's `site-packages`.
- The ten checks the **connector conformance kit** had then passed, with a
  receipt. The kit now has eleven: the eleventh, `change_probe`, checks the
  change probe this example added later.
  (That kit is part of the public SDK. It is not the distribution conformance
  exchange, and this example makes no distribution-conformance claim.)
- `python -m okg_example_records.package` builds a package-v2 distribution, and
  `okg deployment install-plan` resolves it: the connector asset reads back as a
  registration, and both subtypes compose from the example's own module.
- A real install applies extensions and the full schema migration (1,431
  statements) to an empty database.

**Publication is proven on plain `dev`.** The fix for
Internal issue 1921 merged as internal PR 1930, so no
patched framework is needed. The baseline install publishes 3 nodes and 2 edges,
and each incremental state behaves as the table below records: an edit
republishes, an incomplete scan refuses without deleting, a recovery
republishes, a deletion retracts the missing record and its edge, and an
unchanged retry refuses `update_publication_missing`.

## Why the example carries a relationship

Because the shape is honest, not because the framework demands it. An installed
deployment must publish records; it must publish relationships only when one of
its sources declares relationship output in its `admission_policy`
(`output_signature.edges`, or `edge` in `allowed_record_kinds`). A records-only
connector — every source declaring node types and no edge types — installs and
republishes with zero relationships. A source that declares relationships and
publishes none is still refused, and so is a source that declares no output
signature at all. [`docs/operate/install.md`](../../docs/operate/install.md)
states the rule; internal issue 1914 settled it.

This example declares a relationship, so the strict rule applies to it. The
snapshot is itself a node — `example_collection` — and every record hangs off it
by a `contains` edge: a complete scan is an assertion about a whole collection,
and the collection's revision covers its whole membership, so adding, editing or
removing any record moves it exactly once.

## Install the wheels outside the checkout

Prerequisites: Python 3.12, `uv`. Set `OKG_CHECKOUT` to the checkout that
supplies the OKG wheel; it is only a build input.

```bash
: "${OKG_CHECKOUT:?Set the OKG source checkout path}"
DEMO=$(mktemp -d)
mkdir -p "$DEMO/wheels"
# Build from a clean tree: see "Traps" below.
find "$OKG_CHECKOUT/src" -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
uv build --wheel --project "$OKG_CHECKOUT" --out-dir "$DEMO/wheels"
cp -R "$OKG_CHECKOUT/examples/json-record-connector" "$DEMO/authoring"
uv build --wheel --project "$DEMO/authoring" --out-dir "$DEMO/wheels"
rm -rf "$DEMO/authoring"
cd "$DEMO"
unset PYTHONPATH
uv venv --python 3.12 .venv
# okg-*: the one okg wheel built above, whatever version the checkout declares.
uv pip install --python .venv/bin/python \
  wheels/okg-*-py3-none-any.whl \
  wheels/okg_example_records-0.1.0-py3-none-any.whl pytest
.venv/bin/python -I -c 'import okg, okg_example_records; print(okg.__file__); print(okg_example_records.__file__)'
```

Both import paths must be under this virtual environment's `site-packages`. Use
`-I` for that check: an ordinary import writes bytecode into the environment,
which the installer later refuses (trap 4).

## Run the connector conformance kit

```bash
.venv/bin/python -m pytest -q -p okg.deployment.conformance \
  --pyargs okg.deployment.conformance \
  --okg-connector okg_example_records.conformance:case \
  --okg-conformance-receipt conformance.json
```

Expect `11 passed`. The `narrowing_rejection` check needs at least one input
row the reader rejects, named in the case's `expected_rejections`; this
example's `data/conformance.json` has one (`excluded`), and a connector of
your own needs one too. `conformance.json` is the **connector conformance kit
receipt**. It states its own limits rather than overclaiming: lineage is asserted
by the publisher and not by the kit, capability *refusal* is provable only in the
brokered tier (the kit proves capability *declaration*), and credential rotation
belongs to the platform, because the SDK hands a connector a reference and never
a value. This in-process SDK is not a sandbox.

## Build the distribution package

An installed connector publishes through the packaged-install path, which reads
its inputs from a verified distribution package rather than from a checkout.

```bash
mkdir -p "$DEMO/packages"
.venv/bin/python -m okg_example_records.package \
  --wheel wheels/okg_example_records-0.1.0-py3-none-any.whl \
  --output "$DEMO/packages"
```

The wheel is the package's payload object; the bundle files
(`bundles/example-records/`) are addressed as members of it; the connector
registration and the merged ontology schema are separate objects.

Optionally inspect the resolution:

```bash
echo '{"name":"example-records","modules":[],"secrets":{}}' > instance.json
.venv/bin/python -m okg.substrate.cli.main --json deployment install-plan \
  --package "$DEMO/packages"/*.okgdist.tar --bundle example-records \
  --instance instance.json --output "$DEMO/install-plan"
```

## Assemble the release entry

There is no CLI for the release producer, so assemble the entry yourself from
the two wheels, a hash-pinned offline wheelhouse, and a pinned `pip` wheel.

The producer is `okg.distributions.build_release_entry`, so this step stays on
the public SDK like every other step here.
Internal issue 1953 exported it; the pinned
build above predates that export, so the release step needs an `okg` wheel
built at or after it.

The wheelhouse must be **hash-pinned**: the launcher installs dependencies with
`pip --require-hashes --only-binary :all: --no-index`
(`install_launcher.py:219`), so a `uv export --no-hashes` requirements file
refuses. Build the wheels first, then derive the lock from what you built —
that way the hashes are the hashes of the files the installer will actually
read, and packages that ship no wheel (`bibtexparser==1.4.4` today) still work,
because `pip wheel` builds them from source while `pip download` cannot.

```bash
mkdir -p "$DEMO/wheelhouse/wheels" "$DEMO/installer"
uv export --no-dev --no-emit-project --format requirements-txt --no-hashes \
  --project "$OKG_CHECKOUT" -o "$DEMO/wheelhouse/requirements.txt"
uv venv --python 3.12 "$DEMO/.pip"
uv pip install --python "$DEMO/.pip/bin/python" pip==26.2.1
"$DEMO/.pip/bin/python" -m pip wheel --no-deps \
  -r "$DEMO/wheelhouse/requirements.txt" -w "$DEMO/wheelhouse/wheels"
"$DEMO/.pip/bin/python" -m pip download pip==26.2.1 -d "$DEMO/installer" \
  --only-binary :all:
```

Then write `runtime-wheels.txt` and `manifest.json` from the built wheelhouse
and call `build_release_entry`:

```python
import hashlib, json, re
from pathlib import Path
from okg.distributions import build_release_entry

demo = Path("<your $DEMO>")
house = demo / "wheelhouse"
pin = lambda p: "sha256:" + hashlib.sha256(p.read_bytes()).hexdigest()

lines, entries = [], []
for wheel in sorted((house / "wheels").glob("*.whl")):
    name, version = wheel.name.split("-")[:2]
    name = re.sub(r"[-_.]+", "-", name).lower()
    lines.append(f"{name}=={version} \\\n    --hash={pin(wheel)}")
    entries.append({"wheel": wheel.name, "wheel_digest": pin(wheel)})
(house / "runtime-wheels.txt").write_text("\n".join(lines) + "\n")
(house / "manifest.json").write_text(json.dumps(
    {"requirements_digest": pin(house / "runtime-wheels.txt"), "entries": entries}))

# The one okg wheel built from the checkout, at whatever version it declares;
# unpacking refuses if the glob finds none or more than one.
(framework,) = (demo / "wheels").glob("okg-*-py3-none-any.whl")
consumer = demo / "wheels/okg_example_records-0.1.0-py3-none-any.whl"
installer = demo / "installer/pip-26.2.1-py3-none-any.whl"
build_release_entry(
    framework=framework, framework_digest=pin(framework),
    consumer=consumer, consumer_digest=pin(consumer),
    consumer_package="okg_example_records",
    wheelhouse=house, wheelhouse_digest=pin(house / "manifest.json"),
    installer=installer, installer_digest=pin(installer),
    output=demo / "release")
```

`build_release_entry` refuses `release_output_exists` if its output directory
already exists, so remove `$DEMO/release` before rebuilding. Identical inputs
produce an identical release entry, so a rebuild can be compared to the build
it replaces.

## Install

From outside any checkout:

```bash
mkdir -p ~/.okg/example-records
cp <data>/baseline.json ~/.okg/example-records/records.json
export INSTALL_DSN='postgresql://installer:...@127.0.0.1:5433/example_records'
python3.12 -I -B "$DEMO/release/install.py" \
  --target "$DEMO/instance" --deployment-name example-records \
  --distribution-package "$DEMO/packages"/*.okgdist.tar --bundle example-records \
  --install-dsn-ref env:INSTALL_DSN --authorize-cluster-bootstrap \
  --verify-query 'alpha original' --json
```

The database must be **empty** and carry no extensions the installer did not
create — build it with `TEMPLATE template0`, or the install refuses
`initial_database_foreign_extension`. Do not name the connecting role `okg` (see
"Traps"). The launcher clones the interpreter that runs `install.py`, so run it
with Python 3.12.

### Update steps

`--update-source`, `--search-query` and `--search-generation` are live on `dev`
and **every step below was run** — see "What the incremental states do,
measured". The contract, as its owner states it: writes to a completed,
published installation go back through the retained release entry: repeat the original release-entry and
selection arguments, and add `--update-source <existing source id>` together with
`--verify-query <phrase in the changed record>`. The installation must already be
completed and published; `--no-publish`, `--dry-run` and `--scaffold-only`
refuse. A source whose input has not changed may refuse with
`update_publication_missing`. A successful receipt carries `published: true` and
a new `generation_id`, with the reader check pinned to that generation. Reads use
`--search-query`, optionally with `--search-generation`.

Every install and update run prints `deployment declares no sync_root; any
checkout may project file-derived truth onto the canonical database`. That is
expected here and not a failure: this example declares no sync root because its
input is a single local file, and the warning exists to flag deployments where a
checkout could overwrite the database.

Walk the fixture states in `data/` by copying each over `records.json` between
update runs: `edited.json`, `incomplete.json`, `recovered.json`, `deleted.json`,
then repeat the last input unchanged to prove a retry adds nothing — the
unchanged repeat is the one that should refuse with `update_publication_missing`.

## What it needs from the framework

A packaged install could not apply a consumer-owned ontology module at all:
`package_module_providers` was reached only during plan composition, in memory,
so provision resolved built-in owners only and catalog load refused
`module_provider_unresolved`. Internal issue 1921
fixed that with a `module_providers` deployment-manifest key, which the
installer writes into the immutable configuration. It **merged as PR #1930**, so
this example installs and publishes against plain `dev`. You can see the key in
your own installed `configuration/deployment.yaml`:

```yaml
module_providers:
  example-records: modules/example-records
```

The interface gap this example found is closed: assembling the release entry
had no public producer, and
Internal issue 1953 exported one, so no step of
this recipe imports `okg.substrate`.

## What the policy declarations mean

An installed deployment enforces policy, so the bundle declares a posture and
the source declares its own policy. Both are audited before the first publish;
an incomplete declaration refuses rather than defaulting. What a real connector
should change:

| Declaration | This example | A real source |
| --- | --- | --- |
| `deployment_class` | `personal_okg` — one operator, one machine | `org_operational_private` or wider, which widens the allow-lists below |
| `read_principals` / `write_principals` | operator and the deployment's own runtime, plus `source_adapter` for this connector's publishes | add `mcp_client` only if agents should read it |
| `mcp_write_default` | `review` — an agent write is held for a person | keep `review` unless writes are provably safe to apply unattended |
| `export_policy` / `allowed_exportability` | `no_export` — nothing leaves; `personal_okg` requires this | a wider class may say `internal_only` |
| `secret_handling` | `secret_ref_required` — credentials are named, never inlined | unchanged; this is how a connector takes an API token |
| `live_call_default` / `live_call_allowed` | `review` / `false` — this reader opens a local file | `true` for an API reader, which then also needs a credential reference |
| `sensitivity` / `data_classification` | `local_private` / `internal` | must stay inside the posture's allow-lists, or admission refuses |
| `store_raw` | `true` — the text is kept as ingested | `false` needs a pseudonym vault key and a `pii_classes` map, and masks every attribute the map does not declare |
| `pii_classes` | empty — the snapshot carries no personal data | name each attribute that does, or it is masked |
| `privacy_obligations` | `audit`, `no_export` | from a closed vocabulary: `audit`, `review`, `redaction`, `no_export`, `quarantine`, `declassification`, `secret_ref_required`, `source_retention`, `break_glass_review`. An invented value refuses |
| `retention` / `provenance` | `source_defined` / `local_filesystem` — the upstream snapshot decides how long its records live, and they come off this machine's disk | a provenance that names how the records were actually obtained, such as an API or a crawl |

## Traps

Five things that cost real time, none of them obvious from an error message.
Traps 4 and 5 are framework defects, tracked as
internal issue 1922; the first three are contract
details this example now encodes.

1. **`provenance.revision` must be a 40-hex commit.** This example builds from an
   installed wheel and has no source revision, so it declares the neutral zero
   revision rather than inventing a commit id.
2. **The package reader refuses a symlinked path anchor.** The platform temp root
   is a symlink on macOS, so the packaging module resolves its staging directory.
3. **`okg_compatibility` is the OKG *release* window** (`okg.__version__`),
   not the connector's *SDK* window (`okg.deployment.__version__`, 1.0.0). They
   are checked separately; declaring the SDK window as `okg_compatibility`
   refuses every real build.
4. **A wheel built from a used checkout cannot be installed.** `okg` sets
   `include-package-data = true`, and gitignored `__pycache__` under
   non-package directories is swept into the wheel; the installer then refuses
   any payload containing a `.pyc` (`installed_wheel.py:259-264`). Clear
   `__pycache__` before `uv build`.
5. **Do not name the database role `okg`.** The migration creates a schema named
   `okg`, PostgreSQL's default `search_path` is `"$user", public`, and
   `chronos_core`'s `sql_adapters.py:486-490` hardcodes schema `public` when
   reading a table's columns. With a role named `okg` the guard misses and the
   migration re-adds an existing column, failing with
   `DuplicateColumn: column "parent_branch_id" ... already exists`.

Also note: a profile may declare `source_admission_contract: strict_v1`
(internal issue 1915), and the rendered
`source_registry.yaml` then carries that contract at its top level. This
example does **not** declare it, so its registry is admitted **legacy**.

What that does and does not cost. The connector's declared `admission_policy`
is enforced either way: it is parsed under both contracts, and a fact outside
the declared `output_signature` is refused on the legacy path too. What
`strict_v1` adds is document-level admission — the whole registry validated
against `source_registry.schema.json`, strict JSON scalar rules, refusal of a
duplicate `ownership_id` or `producer_id` across sources, frozen entries, and
every runnable adapter planned before any adapter constructor runs.

Why this example stays legacy: as shipped it would be refused. Its
`source_policy.pii_classes: {}` fails the strict registry schema, which
requires that mapping to be non-empty when present. Dropping the key admits
the registry strictly, but that edits a declared privacy field and the
measured publish proof below was taken on the legacy path, so it would have to
be re-run before the numbers could be quoted again. The refusal is not latent:
a profile that declares the contract and cannot satisfy it is refused while
`okg init` is still running, not at the first ingest.

## What the incremental states do, measured

Each row was run through `--update-source` against a real installed deployment.
The generation ids, counts and artifact names are in
`pact/changes/installed-connector-example/evidence/publish-proof.md`:

| Step | Measured result |
| --- | --- |
| Baseline | Published 3 nodes, 2 edges: the snapshot, A and B, and their containment |
| Edit | Republished at a new generation, still 3 / 2; `alpha edited` returned |
| Incomplete | **Refused** at source admission, cause `snapshot incomplete or rejected`; the next read still saw 3 / 2, so B survived a partial scan |
| Complete recovery | Republished, 3 / 2, no duplicates |
| Complete deletion | Republished at 2 / 1 — B and its edge retracted; A still present |
| Unchanged retry | **Refused** `update_publication_missing` |

## How the reader decides authority

`id` is the upstream stable key, never a row number or content hash. A revision
hashes canonical record content. Every complete run returns the full
`RecordSet` — including the snapshot node — even when no facts changed; OKG
decides retractions from that authority, and each record's containment edge is
part of its footprint, so a record that disappears takes its edge with it. A
partial read, a cancellation, or a rejection of a previously owned record retains
the checkpoint and withholds the record set, so an incomplete scan can never
delete. The input's `complete` flag is the upstream completeness contract; a real
paginated API reader must establish that condition by reaching the end of its
authoritative scope.

These behaviours are covered by the tests in
`tests/substrate/runtime/test_connector_conformance.py` and
`tests/substrate/runtime/test_connector_registration.py`.
