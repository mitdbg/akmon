---
audience: operator
type: how-to
canonical_id: task-install-without-checkout
section: operations
visibility: primary
status: active
owner: documentation
---
# Install from a wheel or a release directory

Most people should follow the [Quickstart](../start/quickstart.md), which
installs a released version. This page is for two other cases: you have an
OKG wheel, or a producer built a release directory and handed it to you.

## Install from a wheel

Each OKG release attaches its wheel, `okg-<version>-py3-none-any.whl`, to its
[GitHub release](https://github.com/mitdbg/akmon/releases). Download it from
the release page, for example:

```bash
export AKMON_VERSION=0.4.0
curl -fLO "https://github.com/mitdbg/akmon/releases/download/v${AKMON_VERSION}/okg-${AKMON_VERSION}-py3-none-any.whl"
```

Or build one yourself from a source checkout instead (see
[Install OKG](install.md) for getting the checkout):

```bash
uv build --wheel
```

That writes `dist/okg-<version>-py3-none-any.whl`. Install the wheel into its
own virtual environment. The package requires Python `>=3.12,<3.14`:

```bash
python3.12 -m venv ~/okg-venv
~/okg-venv/bin/pip install /path/to/okg-<version>-py3-none-any.whl
~/okg-venv/bin/okg --version
```

Then follow [Install OKG](install.md) from step 2, using this environment's
`okg`. The database in step 2 comes from the checkout's `ops/pg` directory.
Without a checkout, start the published database image with the `docker run`
command in [Quickstart step 1](../start/quickstart.md#1-get-akmon-and-start-the-database).

To upgrade later, install the newer wheel into the same environment and run
`okg up <name>`; see
[Upgrade an installed deployment](lifecycle.md#upgrade-an-installed-deployment).

## Install a producer-built release

A producer calls `okg.distributions.build_release_entry`, which assembles retained framework,
consumer and dependency wheels into a release directory whose `install.py`
carries the pins and invokes `okg install`. Python, the host, the producer and
the build tools are trusted prerequisites; a wheel hash asserts neither Git
provenance nor consumer approval.

Supply a compatible PostgreSQL service with an empty database, or the same
database as before. Runtime bootstrap changes cluster roles, so use a dedicated
cluster or explicit authority. The installer creates its private environment,
checks database identity, capabilities and ownership, then creates
configuration, schema, runtime state and catalog itself.

Run the release entry from outside source checkouts:

```bash
python /path/to/release/install.py \
  --target /path/to/instance --deployment-name <name> \
  --distribution-package /path/to/selected-package --bundle <bundle> \
  --install-dsn-ref env:INSTALL_DSN --authorize-cluster-bootstrap \
  --verify-query '<phrase in the selected data>' --json
```

Resolve `INSTALL_DSN` through your secret store. The entry creates a sibling
`<instance>.environment` and refuses foreign environments or changed release
inputs.

For a private instance, add `--personal-private --source-policy
/path/to/source-policy.yaml`; `--personal-private` alone is refused. Field
classifications live in that deployment file, never in a framework default; OKG
supplies the shared Presidio detector. The policy must cover every final
registry source, and changed policy refuses reapplication.
Declared text is scanned for personal data; undeclared fields are masked. The
offline release bundles the privacy dependencies and language model; unusable
detection refuses before database changes. Source snapshots stay private input
files that redaction does not erase.

Success requires publication, a positive record count, active source policy,
queue checks and an authorized search result with original record provenance;
MCP readiness is separate. A positive relationship count is required only when
some source in the deployment's registry declares relationship output. A
records-only connector -- every source's declared output signature lists node
types and no edge types -- completes an install with records and zero
relationships, and a later repeat of that install completes too. A source that
declares no output signature at all keeps the strict rule, so an undeclared
connector still has to publish a relationship, and a source that declares
relationships but publishes none is still refused. The installer reads that
declaration once per run; an unreadable source registry refuses the install
rather than guessing either way. `--no-publish` returns a staged instance
with `published:false`; the same command later publishes that exact stage, and a
changed stage, release or configuration refuses. A repeat may publish zero
additions only when it binds to the generation observed before refresh, keeps
live counts equal, and every source reports an authorized unchanged replay. A
source that re-emits every record on each run reports no unchanged replay, so
its repeat is refused with that cause. Stage journals keep completed publication evidence after a
later failure. One owned pseudonymization key persists outside immutable
configuration and never rotates. A crash before the first ownership receipt
leaves a partial directory for inspection, not adoption.

### Ontology modules the package supplies

A package may declare ontology modules of its own, not just the built-in `okg`
ones. The entry writes each declared module into the installed configuration and
names the owners in the deployment manifest:

```text
configuration/
  deployment.yaml
  modules/<owner>/<module>/schema.yaml      (and module.yaml, when declared)
```

```yaml
module_providers:
  example-records: modules/example-records
```

The key adds owners; it never replaces the built-in library. The installed files
are part of the immutable configuration, so the release identity covers them and
they are rechecked every time the release entry runs again. A package that uses
only built-in modules writes no declaration and installs as before.

Before the target directory, the database or any migration is touched, the
installer composes the configuration it is about to write and refuses
`packaged_module_provider_drift` unless it composes exactly what the install
plan recorded: the same modules, the same owner for each subtype, the same kind
of provider, and module files whose bytes still match what the plan listed. A
refusal therefore leaves nothing behind. The check speaks only for the modules
the plan listed; anything else in the tree is inert and covered by the
configuration digest. Extraction separately refuses a module stored inside
another packaged file, an owner the plan never named, and a module of a named
owner that the plan did not list.

The manifest key is validated wherever a deployment is loaded, not only on this
path; [external deployments](deployments/external.md#manifest-relative-module-providers)
lists what it refuses. A packaged install cannot trip those refusals, because
the installer writes the key itself.

### Sealing and activating a release

Sealing a release, activating one and planning an upgrade all resolve the key.
Each of those commands composes against the ontology library of the revision it
is moving to, and the declared owners are added to it, so the catalog they
compose is the one the install composed.

A declared owner's modules seal with the owner and a `package` provider kind
and no per-module digest, because the declaration carries none. That record is
therefore not what binds the module bytes: editing a declared module's schema
leaves it byte-identical. The bytes are bound by the deployment tree the release
seals, and, whenever the composed catalog changes, by the catalog fingerprint,
and by the configuration digest of a packaged install. The
provenance record catches a different thing: a module that a declared owner no
longer supplies, or that now comes from a different owner or provider kind.


## Update an existing packaged source

For a completed packaged installation, repeat its original release entry and
selection arguments with `--update-source`. Change source data in its configured
input location; keep the release, selected package, policy and immutable
configuration unchanged. For example:

```bash
python /path/to/release/install.py \
  --target /path/to/instance --deployment-name <name> \
  --distribution-package /path/to/selected-package --bundle <bundle> \
  --install-dsn-ref env:INSTALL_DSN --authorize-cluster-bootstrap \
  --update-source <existing-source-id> \
  --verify-query '<distinctive phrase in the changed record>' --json
```

The entry rechecks the installed wheels, original configuration and database
identity, then runs scoped ingestion and publication in that verified process.
It includes the source's declared predecessors. It performs no environment
installation, database bootstrap or catalog provisioning during an update;
the original database authorization argument remains required for identity
inspection. A missing, incomplete or staged installation refuses. `--no-publish`,
`--dry-run` and `--scaffold-only` cannot be combined with an update; they refuse
before source execution.

Success returns `published:true`, the selected sources, a `generation_id` from
this invocation and an authorized reader check pinned to that version. Inspect
the changed record and its source evidence; a matching phrase alone does not
identify a particular record. A source with no changed input may skip publication;
this entry then refuses with `update_publication_missing` rather than claiming a
new published version. Failed updates retain the existing installation receipt.
Execution-stage failures record their cause in stage journals; earlier identity,
configuration and database refusals return directly. Use the previous published
version for reads while diagnosing a failed publication.

An ordinary `okg ingest` invocation does not establish packaged-release
verification. Use the retained entry above for packaged writes. A source and
its predecessors requiring different run modes currently refuse explicitly.
Immutable input snapshots in a verified configuration cannot be edited into a
new daily source: changing them remains a verification failure.
This command does not implement release upgrades or source onboarding.

For subsequent searches, repeat the same retained entry and original installation
selection, replacing `--update-source` and `--verify-query` with `--search-query`:

```bash
python /path/to/release/install.py \
  --target /path/to/instance --deployment-name '<deployment-name>' \
  --distribution-package /path/to/selected.okgdist.tar --bundle '<bundle-name>' \
  --install-dsn-ref env:INSTALL_DSN --authorize-cluster-bootstrap \
  --search-query '<distinctive phrase in the changed record>' \
  --search-generation '<returned-generation-id>' --json
```

If the original installation used `--source-policy` or `--personal-private`,
repeat them: a different policy changes the configuration and refuses. Omit
`--search-generation` to read the current published version. This entry verifies
the same installed configuration and database, then calls the existing lexical
search under the configured Nomos policy. It returns up to 20 results with
record identity, attributes, source provenance and revision; an authorized
empty result is valid. Check `nomos_effect`, `nomos_decision_id`, `generation_id`
and the expected record. Search performs no ingestion or publication; normal
authorization audit records may be written.

Keep the original installation's principal selection. An explicit subject
override is evaluated by the same policy and can be denied. Search refuses an
incomplete installation, an unenforced policy, foreign or blocked generations,
and combinations with update, dry-run, scaffold-only or no-publish. Do not
substitute a standalone deployment name that has no resolvable manifest: it
does not establish this retained-release reader boundary.

