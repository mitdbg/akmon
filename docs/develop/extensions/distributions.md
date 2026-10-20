---
audience: developer, operator
type: reference
canonical_id: immutable-distribution-packages
---
# Immutable distribution packages

An immutable distribution package carries a release, tested bundles, typed
assets, payload objects, identities, extension metadata and provenance; the
deployment lifecycle handles everything from migration to readiness.

## Public interface

`okg.distributions` exposes strict models, stable `DistributionPackageError.code`
values, canonical digest helpers and the two producers:

```python
package_path = build_distribution_package(manifest, sources, output_directory)
```

The output directory must exist and be a real directory. The builder writes
`<64-lowercase-hex>.okgdist.tar`, named by transport digest, without overwriting.

```python
install_entry = build_release_entry(
    framework=framework_wheel, framework_digest=pin(framework_wheel),
    consumer=consumer_wheel, consumer_digest=pin(consumer_wheel),
    consumer_package="my_connector_package",
    wheelhouse=wheelhouse, wheelhouse_digest=pin(wheelhouse / "manifest.json"),
    installer=pip_wheel, installer_digest=pin(pip_wheel),
    output=release_directory)
```

Every argument is keyword-only and every wheel is pinned: `pin` is any
`"sha256:" + sha256(bytes)` of the file, and each input is checked against its
pin as it is copied. `consumer_package` is the consumer's import name, which
must be an identifier other than `okg`. `wheelhouse` holds `manifest.json`,
`runtime-wheels.txt` and a `wheels/` directory; the manifest's
`requirements_digest` pins the lock and each entry pins one wheel.

`build_release_entry` returns the path of the `install.py` it writes into a
release directory that must not already exist, and refuses with the same
`DistributionPackageError.code` contract as the package producer:
`release_output_exists`, `release_consumer_package_invalid`,
`release_artifact_digest_mismatch`, `release_wheelhouse_manifest_malformed`,
`release_wheelhouse_entry_malformed`, `release_wheel_name_invalid` and
`release_wheelhouse_empty`. A refusal leaves no release directory behind. The
producer assembles bytes only: nothing is installed or executed, and identical
inputs produce an identical release entry.

`verify_distribution_package(path)` returns a `VerifiedDistributionPackage`,
which reads assets by exact `owner:id` or atomically materializes verified
manifest/objects into a new directory without executing payload code.

Use `derive_distribution_conformance_authority(..., protocol_version="v2")` for
package-v2; default v1 refuses it. Discover schemas with
`okg deployment conformance schema --kind request --protocol-version v2 --json`;
validation dispatches by wire tag and refuses mixed versions.

Acknowledgement, credential and legacy-input rules are under
[External conformance rules](#external-conformance-rules).

## Planning an install

`okg deployment install-plan` produces a plan and lock for one verified
selection. The plan records package, bundle, `owner:module` provider,
compatibility and asset digests; composition stays in memory. The lock detects
drift, refusing changes without `--replace-lock`; identical inputs produce
identical bytes. An instance secret is a mapping -- `{reference: env:NAME,
declaration: owner:asset-id}` -- and the declaration must be a `source` or
`tool` asset the selected bundle carries. The plan records the declaration,
its TypedAsset digest and an opaque `secretref:<uuid5>`; it never records the
variable name or its value. A secret is required unless it says `required:
false`; `aliases: [env:OTHER]` names other variables the source also reads. An
unset required variable refuses, and the refusal names its aliases too. An
optional one is recorded the same way whether or not it is set now
(`required: false`, `readiness_policy: deferred`, a fixed `supply_later`
instruction), because the entry is digested into the plan, the lock and the
packaged release identity. So exporting a deferred credential later does not
read as lock or release drift. Whether each optional credential is set now is
reported, not recorded: the plan and `okg install` results list
`deferred_credentials` (secret name, source id, declaration, token, how to
supply it, and `okg doctor sources` to check). Code that emits supplied tokens
must take them from `install_plan.split_plan_secrets`, never from the plan
directly. An unselected or non-credential declaration and the older bare
`name: env:NAME` form refuse.
Adding a secret to an existing install changes the plan digest, so re-planning
refuses as lock drift until you pass `--replace-lock`.

The packaged `okg install` passes the selected profile's credential
declarations into this plan. Each `credential_refs` variable in a selected
`source-defaults/*.yaml` entry becomes one secret, bound to the package asset
that carries that file. It is required when the source sets
`required_for_baseline: true`, the rule `okg doctor` uses for a required source,
and optional otherwise. Its `credential_aliases` become aliases. The package
asset that carries a file with `credential_refs` must be of kind `source`,
`source-binding`, `source-definition` or `source-template`. `source-template`
is accepted because the example connector and the daily-update fixture give
every bundle file that kind; protocol v2 accepts it for a credential binding
and v1 does not. Any other kind refuses with
`source_credential_declaration_kind_unsupported` naming the file.

Every selected `connector` asset is read back into a `ConnectorRegistration`,
never imported; the lock records its object digest, so a repackaged connector
reads as drift. Resolve refuses a malformed asset, an unimplemented capability
or auth mode, an unresolved required module, and an SDK window excluding the
running `okg.deployment`.

## Provenance in a sealed release

A release records, per packaged module, the owner, provider kind, package
release digest and object digests; the release digest covers that record.
Activation re-resolves and refuses `release_module_provenance_mismatch` on
disagreement. An upgrade reports added, removed and digest-changed modules,
refusing `upgrade_module_provider_owner_dropped` rather than dropping an owner
silently. A release built from the in-tree library records nothing.

## Lifecycle receipts

`okg deployment install-receipt` records one event over planned installs and
runs nothing: an upgrade receipt listing module-provider changes; a rollback
receipt naming both sealed releases and the baseline package it checked is
still present; or a drift receipt per module. An upgrade outside the deployment's OKG window refuses
`upgrade_okg_range_broken`; a missing baseline package refuses
`rollback_baseline_package_missing`.

## Schema versions

`okg.deployment_product.distribution_package/v1` keeps its canonical bytes
and digests. Version `/v2` adds two optional fields, forbidden in v1:

- `okg_compatibility` is `{min_version, max_version_exclusive}`: plain PEP 440
  releases, minimum inclusive, maximum exclusive. `check_okg_compatibility`
  reads the running build by base release (`0.1.0rc2` counts as `0.1.0`) and
  refuses `okg_compatibility_unsupported` outside the window.
- `module_declarations` names each supplied ontology module by one
  owner-qualified `owner:module` id, with the asset references carrying it.

## Archive and identity model

The transport is an uncompressed deterministic POSIX USTAR archive with only:

```text
manifest.json
objects/sha256/<64-lowercase-hex>
```

Identities are separate: an asset digest names the exact selected bytes (a
wheel member is independent of its wheel); a bundle digest names its
canonical descriptor without its own digest field; a release digest names the
canonical manifest without `distribution_release.release_digest`; a transport
digest names the USTAR bytes and supplies the filename. Objects are stored once by digest.

## Resource envelope

The package is bounded metadata, not a corpus container; its limits are fixed:
a 4 MiB manifest, a 512 MiB transport, 4,096 objects, 8,192
typed assets, a 64 MiB object, a 32 MiB selected asset, 8,192 entries per
wheel, and a 100x bounded ZIP expansion ratio. Hashing, ZIP verification and
materialization use chunks of at most one MiB.

`read_asset` checks the manifest-bound size before allocating and while
streaming; a malformed manifest, oversized object or ZIP bomb fails with a
stable resource code naming the limit, observed value and path.

## Wheel members

A wheel member locator does not trust the ZIP central directory. It requires
unique normalized member names, matching central and local headers, one
regular selected member and a complete `*.dist-info/RECORD` matching the
streamed bytes, and rejects aliases, links, special files, encryption,
ambiguous metadata, unsupported compression and over-bound expansion.

## Filesystem authority

Authoring paths are inputs, never serialized authority. The builder walks from
an opened source-root descriptor, resolves only hop-bounded relative symlinks
inside that root, and revalidates identity around capture; repository-authored
links are permitted while the package holds ordinary content-addressed files.
Outputs use exclusive writes then an atomic no-replace publication; a path
that changes during capture refuses.

## Metadata boundary

The package serializes only declarative public identity. Secret and endpoint
placeholders carry only `kind`, `owner` and `ref`, never a value, URL, host,
DSN, credential or environment lookup.

`TypedAsset.override_slots` must be empty. `compatibility` allows only
`contract_versions` and `capabilities` identifier lists; `provenance` only a
public source class, HTTPS repository URL, exact revision and owner-qualified
artifact reference. These checks neither interpret nor execute payloads.

## External conformance rules

These rules are settled; `tests/substrate/distributions/test_conformance_diagnostics.py`
and `tests/substrate/contracts/test_external_distribution_conformance_contract.py`
pin them; `okg deployment conformance validate` refuses
`conformance_profile_refused` after the wire check. The v1 catalog digest is
unchanged; the v2 digest moved once, when protocol v2 made the provider
connection optional (see Credentials). Both catalogs are readable fixtures a consumer can digest
without OKG code: `schema_catalog_v1.json` is the v1 catalog behind
`sha256:db38e9bbbe571dbb9efec29192f786d851095ca412f5a5fda17d1af81f62e662`, and
`schema_catalog_v2.json` is the v2 catalog behind
`sha256:13dd7b4cf8e4d5e9b8a0ffeea42e259835e8499f6545ca8331260aeaa05317ca`.
A consumer shipping a `distribution_package/v2` package exchanges v2 documents
and must acknowledge the v2 digest; the v1 digest does not cover them.

**Credentials.** An optional credential binding names a source or tool
declaration the request selects; a shipped but unselected source carries no
binding or obligation, and selecting it later is a new request. A package need
not carry a `provider-connection` asset: in protocol v2 a binding may omit
`provider_connection_ref` and `provider_connection_digest`. A binding that
names one must give both, and the asset must be selected, of kind
`provider-connection` and match the digest. Protocol v1 still requires both
fields. A selected
declaration with a deferred binding and no supplied credential blocks
`first_publish` (`conformance_declared_optional_credential_unavailable`,
mutation none). A credential that fails at ingest (reference unsupplied or invalid,
authentication failed, permission denied, second factor required) fails
`first_publish` in phase `apply` with `conformance_secret_reference_invalid`:
owner operator, retry safe, next action `revise_request`, one closed `observed`
token, `reference` a selected declaration without a deferred binding; arm
mutation is `none` when the plan refused an unset `env:NAME` before apply and
`mutating` when a run was retracted. Missing tooling, dependencies, caches,
endpoints or runtime block with `conformance_runtime_environment_unavailable`
(owner harness, operator or system, never distribution or consumer); a missing
declared asset is a distribution failure. The gap token is optional: a
`conformance_runtime_environment_unavailable` diagnostic may omit `observed`
entirely, and the profile check accepts that
(`distributions/conformance_diagnostics.py:526-535`). An absent token records
the same outcome with less detail -- something the run needed was missing --
and never a different one; supply a token when the harness knows which of the
four gaps it hit. The blocked lifecycle-receipt fixture omits it. Its
tooling-missing counterpart fixture is that receipt with
its environment diagnostic replaced by the producer's full output, which adds
both `observed: tooling-missing` and `expected: runtime-environment-available`,
and with `receipt_digest` re-sealed over the changed bytes; every other field
is identical. Both validate, and both stay `blocked`.
`diagnostic_from_source_health`
maps the ingest record's run-time `source_health`; it yields only
`reference-unsupplied` and `authentication-failed`, and `missing_credential`
naming no credential reference is tooling. The install plan produces that
`secretref:` and declaration: `declaration_ref` and `declaration_digest` are
the owner-qualified id and TypedAsset digest of the selected declaration, and
`reference` is `secretref:` plus `uuid5` over a fixed OKG namespace and
`release_digest\ninstance_name\ndeclaration_ref\nvariable`. The token is
therefore stable across runs, unique per declaration, instance and release,
and carries no recoverable variable name
(`distributions/install_plan.py::secret_reference_token`). `run_install` passes
the profile's credential declarations into the plan (see
[Planning an install](#planning-an-install)). A credential failure driven by a
real consumer ingest has not been run yet.

**Admission digests.** The request's four `admission` fields --
`extension_boundary_digest`, `package_admission_digest`, `capability_digest`
and `resource_policy_digest` -- are consumer-asserted opaque tokens. Nothing in
OKG produces them, no design note assigns them a source, and the validator
never interprets them. Their only contract is that the receipt's `authority`
block echoes the request's values exactly; a mismatch raises
`conformance_admission_digest_mismatch`
(`contracts/deployment_product/conformance.py:1868-1893`). Treat them as
labels the consumer chose and repeated, not as evidence of an admission
decision. No schema change follows from this, so no catalog digest moves.

**Legacy inputs.** The inventory is one readable JSON document whose entries
carry a unique `input_id`, a `kind` (`authoring_checkout_path`,
`legacy_install_invocation`, `copied_schema_authority`), a `migrated` or
`refused` `disposition` and an `evidence_ref`. The denominator is every unique
entry; an unresolved entry refuses the audit. Request declaration and
`legacy_authority` evidence digest the same sorted entries
(`legacy_input_inventory_declaration`), so a consumer can recompute it. Two
readable fixtures carry the shape: an example inventory (three entries, one
per kind) and a two-entry inventory, the file the golden team-consumer
requests declare. A real consumer's audit needs that consumer's host.
The install plan's `declared_inventory_digest` counts selected package assets
and never legacy inputs; the two fields share a name, are computed by different
functions (`distributions/install_plan.py:653` and
`distributions/lifecycle_receipts.py:600`) over different inputs, and neither
accepts the other's.

**Acknowledgements.** A current acknowledgement is reusable while its five
fields match the trusted review, its `source_revision` equals the request
subject's, and its schema digest equals the protocol's current catalog digest;
`acknowledgement_reissue_reasons` names what changed. An acknowledgement is
issued once per (consumer source revision, protocol catalog digest) pair with
status current -- not per conformance run -- so one acknowledgement covers
every run of that pair. A new request id, receipt,
selection or harness revision needs no re-issue; a changed consumer revision,
catalog or protocol does. The `source_revision` binding to the request
subject's revision is deliberate and stays: an acknowledgement says a named
consumer commit was reviewed, so a later consumer commit is a different
subject and needs its own. The digest moves only with the models or rule lists.
Validation does not authenticate reviewers; changed bindings need a matching
trusted acknowledgement; reuse supplies no execution, credential or publication
proof, so exchange digests and lifecycle evidence remain required. Not proven
here: a real consumer run, a real ingest failure on a consumer host, that
consumer's legacy dispositions and a final-run acknowledgement.

**Evidence producers.** `okg.substrate.distributions.conformance_evidence` is
the only thing that builds a receipt's typed proof blocks from a run.
`build_publish_evidence` takes the published generation id and status, the
definition, lock, composition, plan and sealed-release digests, the applied
instance, and live node/edge counts read before and after the publish;
`build_reapply_evidence` takes the active generation either side of a reapply,
the lock and plan digests, the live counts, the generations created and any
pointer move; `build_readiness_evidence` takes readiness, the doctor result,
queue and dead-letter depth and whether an MCP graph read happened;
`build_materialization_evidence` compares the manifest's declared asset digests
against the digests of the files a materialization wrote;
`build_count_signal_evidence` binds one requested count expectation to its
read-back. Each refuses `conformance_evidence_refused` rather than supply a
value: an absent measurement is `measurement_missing`, a claim that differs
from the read-back is `count_disagrees_with_readback`, and an outcome the
measurements contradict names itself (`generation_not_published`,
`reapply_mutated_the_graph`, `ready_contradicts_doctor`,
`asset_digest_mismatch`, `asset_not_materialized`). Lock drift is not
re-detected here: `install_lock_drift` and the lifecycle receipts own it, and
the reapply block binds the exact lock and plan digests they produced, which is
what the wire contract defines for `idempotent_reapply`.
`ConformanceRecoveryEvidence` has no producer: an executed interruption and
restore needs a real host, and an inert plan is refused rather than recorded.
`tests/substrate/distributions/test_conformance_evidence.py` pins the producers
without a database.

`tests/substrate/e2e/test_external_distribution_hermetic_arm.py` emits a v2
`ci_fixture` receipt for its own run over all six non-legacy arms, records the
request and receipt as JUnit properties, and checks both through
`validate_conformance_exchange`, the profile check and
`okg deployment conformance validate` run from the isolated interpreter with no
DSN. `contract`, `artifact_materialization` and `idempotent_reapply` pass on
measured proof. Three arms are blocked with
`conformance_runtime_environment_unavailable`, so the verdict is `blocked` and
the claim unsatisfied: `first_publish` because the arm publishes through
`Publisher` and has no sealed-release digest, `readback_health` because it opens
no MCP session, and `update_rollback_recovery` because recovery needs a real
host.

Two values in that arm are placeholders, and neither reaches the receipt as
proof. The request declares a baseline/candidate sealed-release pair because
the `ci_fixture` profile makes `update_rollback_recovery` mandatory and the
request schema requires the pair; no sealed release exists in the arm, so the
two ids are digests over the two real, distinct install resolutions the run
computes from the same package. They prove nothing: the exchange validator
binds them only when that arm passes, and passing it would also require typed
`ConformanceRecoveryEvidence` binding those exact artifact ids to a restored
generation. Separately, the arm shows its real publish measurements satisfying
`build_publish_evidence` by supplying a digest-shaped placeholder for
`sealed_release_digest`; that block is discarded, never recorded.

The receipt carries no readiness block. This deployment has no DBOS runtime, so
`okg.v_runtime_queue_health` returns a row reporting itself unavailable, with a
reason and a zero in every count; the arm passes no depth and
`build_readiness_evidence` refuses `measurement_missing` rather than record
zeros nothing measured. Also not proven there: the legacy-authority arm, a v1
authority over these bytes, and any real consumer host.

## Installed assets

`verify_installed_package_assets` (`okg.substrate.distributions`) walks an installed or copied package without importing it, loads every `bundles/*/profile.yaml` through the installer's loader and refuses, with an `installed_asset_*` code, any symlink, any profile, `skills_dir`, `skill_triggers` or `chat.preset.system_prompt_ref` reference that does not resolve inside the bundle, any skill named by `skill-triggers.yaml` without a playbook, a bundle playbook that differs from its package-level copy, and any text asset naming a home directory or a declared authoring root. `tests/substrate/distributions/test_installed_package_assets.py` drives a fixture wheel through each defect class named above.

A bundle's `profile.yaml` may set `source_admission_contract`. Its only valid
value is `strict_v1`; omitting the key is how a profile selects legacy
admission. `legacy_v1` is refused as an explicit value, matching the registry
admitter, which refuses a spelled-out legacy marker for the same reason: the
legacy contract is the absence of a declaration, not a thing to select, and
accepting the word would give two spellings of one state. Any other value is
refused when the profile loads.

With `strict_v1` the scaffolder renders the installed `source_registry.yaml`
with a top-level `admission_contract: strict_v1`, so the package's sources are
admitted through the strict document contract: the registry is validated
against `source_registry.schema.json`, its scalars must survive a strict JSON
boundary, duplicate `ownership_id` or `producer_id` across sources is refused,
admitted entries are frozen, and every runnable adapter is planned before any
adapter constructor runs. Output-signature narrowing is not part of the
difference -- a declared `admission_policy` bounds what a source may write
under either contract. The scaffolder admits the registry it just rendered and
refuses the install if that fails, so a strict declaration cannot produce a
deployment that only breaks at its first ingest. Omit the key and the rendered
registry is byte-identical to what the scaffolder wrote before.

## Hermetic runtime proof

`tests/substrate/e2e/test_external_distribution_hermetic_arm.py` plans package
modules and registration JSON, installs a separate public-SDK fixture wheel,
constructs it through the strict source registry and publishes exact counts.
Only the playbook executes. Default v1 authority refuses package-v2.

The arm runs an installed OKG, not this checkout. It builds OKG's own wheel
with `uv build --wheel`, installs it non-editably with `--no-deps`, and takes
its dependencies from `uv export --locked --no-dev --no-emit-project`,
installed under `--require-hashes`. The connector runner is copied outside the
checkout and executed by absolute path from a working directory outside it,
under the same scrubbed environment as the install-plan subprocesses: no
`PYTHONPATH`, no `OKG_*` variable, and no `PATH` entry inside the repository.
Every subprocess runs under `-I`, which also keeps the script directory and
the working directory off `sys.path`, so the only import roots are the
isolated interpreter's own stdlib and site-packages. The arm asserts that
`okg` resolves in that site-packages, that no `sys.path` entry there lies
under the checkout, and that no editable finder was installed; it records the
wheel's name, version and sha256 in the `hermetic_publish_proof` JUnit
property. That digest is per run, not a fixed value: the setuptools wheel
embeds timestamps and is not byte-reproducible. Two harness-side facts stay
true: `uv build` writes setuptools' intermediate build and egg-info
directories into the checkout while it runs, which the fixture clears before
the build and again on teardown so a deleted source file cannot survive into
the wheel and the checkout is left as found; and resolving the exported
requirements reads uv's cache and, for anything missing there, the network.
The proof is isolation from the checkout, not from the package index.

What an installed OKG does not add: the plan still neither installs nor binds
wheel bytes, the arm still seals no release, and it still opens no MCP read
surface, so `first_publish`, `readback_health` and `update_rollback_recovery`
remain blocked exactly as described above.

## Packaged action types under review

A packaged action type is a declaration, not code. To make it govern
anything, a deployment turns the declaration into a review contract.
`tests/substrate/e2e/test_packaged_action_type_through_package.py` shows that
path end to end against a deterministic in-process provider: the action type
is read back through the package's verifying reader, a proposal is created,
a reviewer approves it with an edit to the payload, and a worker claims and
executes. Only the edited payload reaches the provider. A rejected proposal,
an undecided one, and one whose execution window has elapsed each reach it
not at all.

The declaration supplies four things and nothing else may override them: the
action type's identity, the approval gate, the side-effecting flag, and the
closed list of payload fields, which becomes both the payload schema and the
only fields a reviewer may edit. The action schema digest is the digest of
the packaged bytes, so editing the declaration invalidates every proposal
bound to the old one. A declaration that drops the approval gate or the
side-effecting flag, empties its payload fields, or bumps its schema version
is refused; none of those defaults to a permissive contract.

The provider binding digests, the target constraint, and the review and
execution limits are deployment-supplied. The fixture distribution declares
none of them, so treating them as packaged authority would overstate what a
package carries today. The executor is deployment-supplied too: a
package-supplied worker waits on the packaged-extension loader
(`blocked.external-automation-and-action-proposals`).

## Producer checklist

Pin the authoring revision and build reproducibly; turn every selected file or
wheel member into a typed asset with an exact digest and size; never ship
links; compute identities with the public helpers; verify the returned package
independently; and preserve the bytes, because installation and migration
consume this immutable input, never the authoring checkout.
