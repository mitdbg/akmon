---
audience: developer, operator
type: explanation
canonical_id: external-deployments
---
# External deployments


An *external* deployment is defined outside the OKG source tree, typically in
a downstream repository that consumes okg as a git submodule. Code under
`src/okg/substrate/` assumes no specific `deployments/<name>/` directory
exists in-tree. The resolution surface is `OKG_AGENT=1` plus each command's
deployment argument; the operator flow is in
`src/okg/AGENTS.md`.
Deployments are keyed by the `name:` field in `deployment.yaml` and persist
under one Postgres DSN per deployment (one publisher per database).

Downstream-repository integrations must:

- Pin the okg submodule to a specific commit and bump deliberately.
- For a split-repository `okg deployment up`, pin the framework authority in
  `release.pin.code_repository` (a local Git checkout; relative paths resolve
  from the deployment repository root) together with `release.pin.code_revision`.
  Optionally pin the deployment definition with `release.pin.deployment_revision`;
  legacy manifests use the deployment repository's exact `HEAD`.
- Never rely on an invalid explicit framework repository or revision falling
  back to the deployment repository: backup admission refuses and reports the
  failed authority.
- Set `OKG_AGENT=1` in CI or scripted automation so observational
  commands return JSON.
- Pass the deployment manifest path explicitly after the command, for example
  `okg doctor --deployment /abs/path/to/deployment.yaml`,
  OR set `OKG_DEPLOYMENTS_DIR` to the directory containing
  the deployment and reference it by name.
- Keep persistent state keyed by the deployment's `name:` so the
  graph survives cross-repo moves.

## Bootstrap workflow

Create a bootstrap plan in the downstream repository, then run `okg
deployment bootstrap status`, `verify`, and `apply` with the absolute
deployment path and that deployment's database connection. Commit the staged
files and the bootstrap plan before `verify`, and commit the promoted files
after `apply`. Runtime
files stay under `.okg_bootstrap/staged/` until `verify` publishes a real
generation; `apply` promotes them only when their hashes match the most recent
successful verification. For a deployment that already has authored runtime
files, follow [Adopt an existing deployment](../../runbooks/deployments/deployment-bootstrap.md)
instead of building an import command.

Deployment-owned edge narrowings belong only in direct
`<schema_dir>/bridges/*.yaml` files. A narrowing-marked `.yaml` or `.yml`
elsewhere under the resolved schema directory is refused by catalog composition
and `okg deployment lint`; move it into `bridges/` with a `.yaml` suffix rather
than relying on recursive schema loading. The `bridges/` path must be a real directory rather
than a directory symlink; direct `.yaml` file symlinks inside it keep their
lexical bridge location.

## Manifest-relative module providers

Most manifest paths are resolved against the deployment directory without a
containment check. `module_providers` is the exception: it is checked where it
is parsed, for every deployment, so nothing depends on a caller remembering to
look.

It maps an owner to the directory inside the deployment that supplies that
owner's ontology modules, as `<dir>/<module>/schema.yaml`. It refuses:

- the owner `okg` in any spelling — the built-in library always supplies it;
- an owner that is not a single path segment, or two owners that fold together;
- two owners pointing at one directory;
- a value that is absolute, carries a `.` or `..` segment, crosses a symlink on
  any component, or names a directory that is not there;
- a declaration that is present but empty or null.

Declared owners are added to the built-ins, never substituted for them. Sealing
a release, activating one and planning an upgrade add them to the ontology
library of the revision being moved to, in the same way; a declared owner that
collides with a provider one of those commands already named refuses rather
than shadowing it. An environment definition may carry the key as well as a
`deployment.yaml`. A sealed release records which owner supplied each module,
not a digest of the module files; the sealed deployment tree is what changes
when those files change, together with the catalog fingerprint whenever the
edit changes the composed catalog.

## Loader cascade

`okg.substrate.deployments` locates the deployments root by taking the first
of: an explicit override, `OKG_DEPLOYMENTS_DIR`, the `deployments/` directory
next to the `pyproject.toml` or `.git` sentinel found by walking up from the
working directory, then OKG's own repository `deployments/`. The first two
fail loudly when set to a missing directory rather than falling through. On
resolution the deployment directory is placed at the front of `sys.path`, so
`module: <relative.module>` entries in `source_registry.yaml`,
`enricher_registry.yaml`, and `agent_tools` import without `PYTHONPATH`
changes. The implementation is
`src/okg/substrate/deployments/__init__.py`;
`okg.ingest.deployment` is a compatibility re-export, not an extension point.

## Run okg from an installed wheel

A deployment in its own Git repository can run an okg that was installed from
a wheel (`pip install okg-<version>.whl`), with no checkout of okg and no
`release.pin.code_repository`. The framework detects this itself: when the
running okg is a file its installed wheel lists, it names itself by that wheel.

- Every file of the installed okg is checked against the hashes the wheel recorded.
  An edited file refuses the run with `installed_framework_modified`;
  reinstall the wheel to clear it.
- Each published generation records `repository_provenance.status:
  distribution`. The deployment side is exact: its repository commit and
  tree. The framework side is `core_distribution`: the wheel's name, version and
  `payload_digest`, plus the commit and tree the build says it came from
  (`source_revision`, `source_tree`, `source_dirty`). That commit is the
  builder's claim, so `core_revision` stays null.
- The deployment repository must still be committed and clean. The framework's
  run-time directories (`profiles/`, `.okg/up/`, `.okg/backups/`) carry a
  `.gitignore` of their own, so running the framework there does not dirty it. Keep
  `.okg/local.env`, which holds secrets, in your `.gitignore`.
- A prod deployment pins the wheel instead of a revision:

  ```yaml
  release:
    channel: prod
    pin:
      release_kind: distribution
      framework_payload_digest: sha256:<from okg deployment release --json>
      schema_checksum: <...>
      catalog_fingerprint: <...>
      skill_bundle_hash: <...>
  ```

  `okg deployment release <name> --json` prints the current values under
  `current`. Prod refuses a wheel whose build commit is unknown or whose build
  tree was dirty (`distribution_build_unverified`).
- An editable install (`pip install -e`, `uv sync`) is not a wheel install and
  keeps the checkout rules above. So does any install that can resolve an okg
  Git checkout, such as a sealed release: Git authority is tried first.
- `source_dirty` is true when the build tree had any change, untracked files
  included, or shipped a file Git does not track. Build release wheels from a
  clean checkout.
- What the check does not cover: byte-compiled caches (`__pycache__/*.pyc`)
  are not verified, the same limit the checkout proof has. Anyone who can
  write into the environment's site-packages can change what runs.
- `okg up` upgrades a deployment that runs from a wheel: install the newer
  wheel, then run `okg up <name>`
  ([Upgrade an installed deployment](../lifecycle.md#upgrade-an-installed-deployment)).
  A prod pin names the old wheel, so first update `framework_payload_digest`
  from `okg deployment release <name> --json`.

### The runtime container image

The runtime image (`Dockerfile`) installs the framework from a wheel built in the image
build, so a deployment repository mounted outside `/opt/okg` publishes from
the image with no okg `.git` and no token.

- The build records what the builder supplies as the wheel's build claim:
  `--build-arg VCS_REF=<full commit id>` as `source_revision`,
  `OKG_BUILD_SOURCE_TREE=<full tree id>` as `source_tree`, and
  `OKG_BUILD_SOURCE_DIRTY=true|false` as `source_dirty`. The image build has
  no `.git` to check them, so the builder computes them from its checkout. A
  malformed value stops the build. Anything not supplied is recorded as null
  (unknown). The dev channel accepts unknown values; prod refuses a wheel
  whose commit is unknown or whose `source_dirty` is not `false`
  (`distribution_build_unverified`).
- `/opt/okg` still holds the source tree for the files read from it
  (`deployments/`, `ops/`, `scripts/`). Python imports okg from
  `/opt/okg/.venv`, not from `/opt/okg/src`. Setting
  `PYTHONPATH=/opt/okg/src` brings the checkout rules back.
- `OKG_FRAMEWORK_TREE=/opt/okg` tells the framework where that tree is. A wheel
  install cannot find it from its own location, and without it the Nomos
  enforce-mode database scan would skip `/opt/okg/deployments` and the
  environment descriptors in `/opt/okg/ops/environments` would not be found.
  A value that is not the tree stops the command with
  `framework_tree_unavailable`.
- `UV_NO_SYNC=1` makes `uv run` use the installed environment as it is. Without
  it, `uv run` in `/opt/okg` syncs the project, and so does `uv sync`: either
  one reinstalls okg as an editable checkout of `/opt/okg/src`, and publishing
  from a deployment outside `/opt/okg` refuses again. Do not run `uv sync` in
  the image.
- `/opt/okg/.venv` is owned by root, so the runtime user (10001) cannot edit
  the installed files in place. The rest of `/opt/okg` stays owned by that
  user, who could still rename or replace `.venv` as a whole. The protection
  therefore needs a read-only root filesystem, as the Kubernetes chart sets
  (`readOnlyRootFilesystem: true`), or a root-owned `/opt/okg`.
- A derived image can still add packages as root with
  `uv pip install --python /opt/okg/.venv/bin/python <package>`. A package
  that writes files into a package okg's wheel installs (`okg/`,
  `chronos_core/`, `pact/`) makes every run refuse with
  `installed_framework_modified`.
- The image build pins setuptools to the version `uv.lock` locks, so a
  rebuild of one commit does not pick up a newer build backend that could
  change the files, and so the `payload_digest`, a prod pin names.

## What the framework provenance proof compares

Every release the publisher seals records the framework revision it ran, and
before it does it proves the installed `okg` package is that revision: it
lists every file git tracks under `src/okg` at the pinned revision and
compares each one, by its git object id, with the same file in the installed
package. A file that differs refuses the release with `framework release
provenance is not proven`; so does a file that is missing.

One tracked tree is exempt from the missing-file half of that rule:
`src/okg/operator_ui/web/`, the operator UI's TypeScript sources and its
screenshot baselines. It is built by npm and served from its own origin, no
Python in the package reads it, and the distribution does not carry it. Be
exact about what that gives up: a file DELETED from inside that tree is no
longer detected. Nothing in the installed package reads those files, so a
deletion there cannot change what a publish does. Everything else holds: the
files are skipped only when absent, so in a source checkout, where they are
all present, they are compared like everything else, and a tampered byte
still refuses.

Everything else git tracks under `src/okg` must be in the distribution.
That is a packaging obligation, not a suggestion: adding a data file under
`src/okg/` that no `[tool.setuptools.package-data]` glob matches makes every
wheel-installed publish fail on it.
`tests/substrate/test_wheel_ships_framework_files.py` builds the wheel and
compares its members against the tracked tree, so the gap is a test failure
rather than an operator's failed publish.

## Deployments outside a Git checkout

`okg install` writes a new deployment to the user state directory
(`${XDG_DATA_HOME:-$HOME/.local/share}/okg/deployments/`) unless
`--deployments-root` or `OKG_DEPLOYMENTS_DIR` says otherwise. That directory is
not a Git working tree, and by default the publisher stamps each published
generation with the Git revision of the repository holding the deployment, so
a deployment that declares nothing about its release cannot publish from
there.

`okg install` now writes that declaration itself. Every scaffold gets
`release: {channel: dev}`, and a scaffold outside every Git working tree also
gets `kind: standalone`:

```yaml
release:
  channel: dev
  kind: standalone
```

An install done by `okg install` therefore needs no hand edit before its first
publish. Write the block by hand only for a deployment scaffolded before that
behaviour existed, or written by something other than `okg install`. Write
both keys: a `release` block carrying `kind` and no `channel` is reported by
`okg deployment lint` and refused by every command that loads the release
contract.

The publisher then records `repository_provenance.status: unavailable` on every
generation it publishes, which `okg generation describe <id> --json` reports.
This is the same value a packaged release records, and it is how a reader tells
that no repository revision backs the generation's contents.

The declaration is deliberate and narrow:

- Without it, a deployment outside a repository still refuses to publish. The
  publisher never waives provenance because Git happens to be missing.
- With it, a deployment that *is* inside a repository still takes the ordinary
  tracking path and keeps every refusal that comes with it, including the
  refusal to publish from a repository with uncommitted or untracked files. The
  declaration cannot be used to silence drift in a real checkout. One exception:
  `okg up` and `okg deployment backup verify` read only the declaration. `okg
  up` upgrades such a deployment as an installed package
  ([Upgrade an installed deployment](../lifecycle.md#upgrade-an-installed-deployment)),
  and `okg deployment backup verify` refuses it with
  `standalone_upgrade_unsupported`, even inside a repository. Remove the
  declaration to upgrade it from its Git checkout.
- Combining it with a packaged release or a sealed worker release is refused
  with `standalone_release_conflict`: each of those asserts a release authority
  that standalone says is absent.
- Any other `release.kind` value is refused with `release_kind_invalid` rather
  than ignored.

`okg runtime worker`, `okg mcp-serve` and `okg deployment release` serve a
standalone deployment too. Each of them checks the release contract before it
starts, and for a standalone deployment that check reports no repository
revision instead of refusing.

`okg deployment release <name> --json` shows what it resolved:

```json
{
  "ok": true,
  "current": {
    "code_revision": null,
    "release_kind": "standalone",
    "schema_checksum": "sha256:...",
    "catalog_fingerprint": "..."
  }
}
```

`release_kind` is the reason `code_revision` is empty. Without it, an empty
revision could equally mean "packaged release" or "not measured", so read
`release_kind` rather than the empty revision when you need to know why.

Only the repository half of the identity is waived. The schema checksum,
catalog fingerprint and skill bundle hash are still measured and still gate the
release, so a schema skew or a catalog drift refuses exactly as it does for a
deployment tracked in Git.

A standalone deployment can still be pinned for `channel: prod`. Its pin
declares the same thing the deployment does, and names no revision:

```yaml
release:
  channel: prod
  kind: standalone
  pin:
    release_kind: standalone
    code_revision: null
    schema_checksum: sha256:...
    catalog_fingerprint: ...
    skill_bundle_hash: sha256:...
```

A pin that declares `release_kind: standalone` alongside a `code_revision`,
`deployment_revision` or `code_repository` is refused with
`standalone_release_conflict`; the two describe different deployments. A prod
pin that names a revision for a deployment that records none is reported as
`standalone_release_pin_unsatisfiable`, which says the pin can never be met --
not that code has drifted and a sync would fix it.

Keep the deployment inside a committed Git checkout instead when you need each
generation to name the exact revision that produced it -- for auditing a shared
graph, or for reproducing a generation's inputs later.

## Share rollout status safely

Colleague-facing rollout reports should contain only aggregate operating
state: the deployment name, published generation identifiers, readiness
results, queue and dead-letter counts, source names and freshness, and redacted
next actions with an owner. Do not share raw assistant transcripts, local
environment files, database connection strings, credentialed URLs, workflow
payloads, or unredacted runtime logs. Build reports with
`okg.substrate.support.rollout_sharing.build_colleague_rollout_report`; it
redacts local paths, connection strings, tokens, private remotes, and home
directories. Inspect the rendered report before sharing: no local paths or
credentials, and an owner for every next action.

The deployment manifest, source registry, catalog, and profile validators
define the exact fields.
