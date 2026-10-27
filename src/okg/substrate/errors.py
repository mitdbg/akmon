"""Substrate error classes — 1:1 with docs/reference/error_codes.yaml.

Each class has a stable `code` attribute that is emitted in JSON
error payloads. The CLI renders these as:

    {"error": {"code": "<code>", "message": "...", "fix_hint": "...", "data": {...}}}

Adding a new exception here requires adding a matching row to
docs/reference/error_codes.yaml.
"""
from __future__ import annotations

from typing import Any


class SubstrateError(Exception):
    """Base class for every error the substrate raises.

    Subclasses set `code` (the wire code from error_codes.yaml).
    `data` is an optional structured payload for the JSON error
    envelope; stringifying to `str(err)` gives the human-readable
    message.
    """

    code: str = "substrate_error"

    def __init__(
        self,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        fix_hint: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.data = data or {}
        self.fix_hint = fix_hint

    def to_json(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                **({"fix_hint": self.fix_hint} if self.fix_hint else {}),
                "data": self.data,
            }
        }


# ---------- extension / host problems ----------

class ExtensionMissing(SubstrateError):
    code = "extension_missing"


class ExtensionVersionMismatch(SubstrateError):
    code = "extension_version_mismatch"


class PostgresVersionTooOld(SubstrateError):
    code = "postgres_version_too_old"


class DatabaseCloneError(SubstrateError):
    """A development database clone could not preserve its safety contract."""

    code = "database_clone_error"


class EmbedderDimMismatch(SubstrateError):
    code = "embedder_dim_mismatch"


class EmbedderModelMissing(SubstrateError):
    code = "embedder_model_missing"


# ---------- publisher / leases ----------
#
# Per `add-publisher-leases-with-heartbeat` (2026-04-29).
# `PublisherLeaseHeld` replaces `PublisherLockHeld`; the alias
# stays for one release cycle so existing catch-sites keep working.

class PublisherLeaseHeld(SubstrateError):
    code = "publisher_lease_held"


# Alias for back-compat. Drop after the 2026-Q3 release window.
PublisherLockHeld = PublisherLeaseHeld


class SourceSyncAlreadyRunning(SubstrateError):
    """Another source sync held the deployment's source-sync signal for the
    whole bounded wait (issue #3003)."""

    code = "source_sync_already_running"


class SourceSyncSignalLost(SubstrateError):
    """A run's session holding the source-sync signal ended before the run
    published, so publishing could include another run's partial work
    (issue #3003)."""

    code = "source_sync_signal_lost"


class CatalogApplyLeaseHeld(SubstrateError):
    code = "catalog_apply_lease_held"


class CatalogOwnershipMismatch(SubstrateError):
    code = "catalog_ownership_mismatch"


class DeploymentOwnerMismatch(SubstrateError):
    """The connected database is owned by a different deployment than the one
    attempting to publish. Fail closed before any generation-producing write —
    the cross-deployment contamination guard (P0 #419; the 2026-06-10 clone-DB
    incident)."""
    code = "deployment_owner_mismatch"


class DeploymentEnvironmentInvalid(SubstrateError):
    """A long-lived environment descriptor or resolved topology is invalid."""

    code = "deployment_environment_invalid"


class SourceActivationProbeInvalid(SubstrateError):
    """A dormant source was selected before its probe contract was valid."""

    code = "source_activation_probe_invalid"


class SourceConfigurationAuthorityInvalid(SubstrateError):
    """A configured source root cannot authorize source execution."""

    code = "source_configuration_authority_invalid"


class SourceRootMissing(SubstrateError):
    """A source's configured filesystem root does not exist.

    Reading a missing root as an empty directory would publish nothing (or,
    for a complete-scope run, retract everything) and report success, so the
    reader refuses instead. ``data`` names the source, the setting, and the
    resolved path.
    """

    code = "source_root_missing"
    # One hint for every raise site; docs/reference/error_codes.yaml carries
    # the same text as this code's fix_hint_template.
    FIX_HINT = (
        "point repo_root at an existing directory, or remove the source "
        "from the registry"
    )


class SourceRegistrySelectorAbsent(SubstrateError):
    """An explicit runtime source selector is absent from current authority."""

    code = "source_registry_selector_absent"


class SourceConnectionBudgetExhausted(SubstrateError):
    """A source child could not start within the database connection budget."""

    code = "source_connection_budget_exhausted"


class SourceMemoryLimitExceeded(SubstrateError):
    """A source child was killed for using more memory than its limit."""

    code = "source_memory_limit_exceeded"


class SourceRunAlreadyActive(SubstrateError):
    """Another run of the same source still holds its per-source run lock."""

    code = "source_run_already_active"


class SourceProcessBindingChanged(SubstrateError):
    """A source child rebuilt a strict adapter whose binding differs from the
    one the parent admitted."""

    code = "source_process_binding_changed"


class SealedReleaseInvalid(SubstrateError):
    """A sealed release cannot be prepared, reused, or verified safely."""

    code = "sealed_release_invalid"


class LeaseLostError(SubstrateError):
    """Raised when the holder's heartbeat detects its lease was
    stolen (lease_seq mismatch on UPDATE) — the publisher's main
    thread polls this between phases and aborts cleanly."""
    code = "lease_lost"


class PhaseATimeout(SubstrateError):
    code = "phase_a_timeout"


class GateTimeout(SubstrateError):
    code = "gate_timeout"


# ---------- fact / projection correctness ----------

class NarrowingViolation(SubstrateError):
    code = "narrowing_violation"


class UndeclaredFindingIdentity(SubstrateError, ValueError):
    """A consistency check reached the executor without declaring
    ``normalized_value_columns`` — the closure that keys each finding's
    ``observation_id``. The executor refuses to key finding identity on the
    whole offender row (the retired ``sorted(row)`` fallback): a volatile
    projected column would then churn ``observation_id`` every generation and
    mint phantom duplicate findings for one real problem (#691). The check must
    declare ``normalized_value_columns`` explicitly.

    Also a ``ValueError`` so the executor's undeclared-identity refusal (this
    branch's typed error) is catchable by the broad ``ValueError`` handlers the
    dev-side finding-identity engine (PACT consistency-finding-identity) raises
    and tests for on the same path — the two contracts converge rather than
    diverge on exception type."""

    code = "undeclared_finding_identity"


class PostPublishGenerationUnresolved(SubstrateError):
    """A post-publish consistency check could not open the published
    generation it was asked to evaluate.

    Post-publish checks judge one published generation. Reading the live
    branch head instead would judge writes made after that generation and
    record the result against the wrong generation, so the check refuses.
    """

    code = "post_publish_generation_unresolved"


class ChronosSqlMirrorMismatch(SubstrateError):
    """A post-publish SQL check was about to read the Chronos SQL mirror
    (``okg.chronos_graph_{nodes,edges}_current``) while it did not hold the
    generation being judged -- the refresh failed, or the copy's stamp names
    another generation. The check is not run, so no finding is recorded
    against the wrong generation.
    """

    code = "chronos_sql_mirror_mismatch"


class EnricherBackfillSelectionError(SubstrateError):
    code = "enricher_backfill_selection_error"


class EnricherBackfillBranchUnsupported(SubstrateError):
    code = "enricher_backfill_branch_unsupported"


class EnricherBackfillUnsafeEnricher(SubstrateError):
    code = "enricher_backfill_unsafe_enricher"


class EnricherBackfillUnsupportedOutput(SubstrateError):
    code = "enricher_backfill_unsupported_output"


class EnricherBackfillAdmissionRejected(SubstrateError):
    code = "enricher_backfill_admission_rejected"


class EnricherBackfillChunkFailed(SubstrateError):
    code = "enricher_backfill_chunk_failed"


class BranchNotRegistered(SubstrateError):
    """The publisher was asked to publish on a non-default branch
    that does not yet exist in `okg.branches`. Per
    `fix-akmon-design-mistakes`: only the `default` branch
    lazy-materializes on first publish; all other branches
    (quarantine:*, experiment:*, free-form) MUST be created
    explicitly via `okg branch create` so operator typos surface
    at branch-creation time instead of silently absorbing into a
    stranded branch.
    """

    code = "branch_not_registered"


class RetractionNotPermitted(SubstrateError):
    code = "retraction_not_permitted"


class InvariantSampleUnidentifiable(SubstrateError):
    code = "invariant_sample_unidentifiable"


class NodeNotFound(SubstrateError):
    """The requested node_id has no row in `nodes_live` at the
    pinned generation. Raised by MCP `get_node` so agents see a
    structured response instead of empty content."""
    code = "node_not_found"


class GenerationCompacted(SubstrateError):
    """The requested Chronos checkpoint was deliberately retired by policy."""

    code = "generation_compacted"


class GenerationNotFound(SubstrateError):
    """No OKG generation checkpoint matches the requested id.

    Distinct from `GenerationOutOfScope`: the id resolves to nothing at
    all, rather than to a checkpoint owned by another deployment or
    branch. A caller that copied an id out of a wider listing needs to
    tell "this never existed" from "you asked from the wrong scope";
    one shared message collapses both into a guess.
    """

    code = "generation_not_found"


class GenerationOutOfScope(SubstrateError):
    """The requested generation checkpoint exists, but belongs to a
    different deployment or branch than the reading session.

    Raised instead of a bare not-found so an operator is told WHICH
    scope owns the id they asked about.
    """

    code = "generation_out_of_scope"


class GenerationPinInvalid(SubstrateError):
    """The okg.pin_generation session GUC holds a value the read path
    cannot interpret as a generation.

    Raised instead of silently serving the latest published generation:
    a caller who pinned a SPECIFIC generation must never get a
    DIFFERENT one back because the pin failed to parse.
    """

    code = "generation_pin_invalid"


# ---------- authored-file validation ----------

class ProfileMismatch(SubstrateError):
    code = "profile_mismatch"


class LibraryRefMissing(SubstrateError):
    code = "library_ref_missing"


class SchemaValidationFailed(SubstrateError):
    code = "schema_validation_failed"


# ---------- deployment bootstrap ----------

class BootstrapPlanInvalid(SubstrateError):
    code = "bootstrap_plan_invalid"


class BootstrapSourceSampleFailed(SubstrateError):
    code = "bootstrap_source_sample_failed"


class BootstrapDraftInvalid(SubstrateError):
    code = "bootstrap_draft_invalid"


class BootstrapCatalogRejected(SubstrateError):
    code = "bootstrap_catalog_rejected"


class BootstrapPublishBlocked(SubstrateError):
    code = "bootstrap_publish_blocked"


class BootstrapUnverifiedApply(SubstrateError):
    code = "bootstrap_unverified_apply"


class ExternalDeploymentsDirNotConfigured(SubstrateError):
    """The external okg-deployments directory was needed for a rollout
    audit but is not configured: no explicit ``--deployment-dir``, no
    ``OKG_DEPLOYMENTS_DIR``, and no fetched cache. We fail loudly here
    instead of falling back to an operator-specific default path so the
    audit's "deployment missing" finding can never be a silent artifact
    of an unconfigured machine."""
    code = "external_deployments_dir_not_configured"


# ---------- catalog migration ----------

class CatalogMigrationRefused(SubstrateError):
    code = "catalog_migration_refused"


class CatalogMigrationInProgress(SubstrateError):
    code = "catalog_migration_in_progress"


class CatalogModuleMissing(SubstrateError):
    """A deployment referenced a module whose substrate-library
    schema doesn't exist."""
    code = "catalog_module_missing"


class BridgeSubtypeUnknown(SubstrateError):
    """A bridge-narrowing references a subtype that none of the
    declared modules / deployment schemas define."""
    code = "bridge_subtype_unknown"


class NarrowingSchemaMisplaced(SubstrateError):
    """A deployment narrowing schema is outside the one active bridge path."""

    code = "narrowing_schema_misplaced"


class NarrowingBridgeDirectorySymlink(SubstrateError):
    """The canonical deployment bridge directory is a symlink."""

    code = "narrowing_bridge_directory_symlink"


# ---------- migration apply-time hardening ----------
#
# Per `schema-migration-hardening` (db-schema-freeze-readiness child):
# migration semantics are a public upgrade contract at OSS, so apply-time
# failures must be typed, fail-closed refusals — never a wedge, never an
# ungated fallback.

class MigrationLockTimeout(SubstrateError):
    """migrate() could not acquire a lock it needs (the per-database
    advisory migration lock, or a relation lock taken by the schema
    body) within the configured ``lock_timeout``/``statement_timeout``
    budget across its bounded retries. The migrate transaction was
    rolled back cleanly; nothing was applied and no ledger row moved."""

    code = "migration_lock_timeout"


class MigrationManifestViolation(SubstrateError):
    """Base class for apply-time migrations/manifest.yaml refusals.

    Deliberately carries no code of its own: every concrete refusal
    below has a distinct registered code, and catch-sites that want
    "any manifest refusal" catch this base type."""


class MigrationManifestEntryMissing(MigrationManifestViolation):
    """A pending ordered migration file has no migrations/manifest.yaml
    entry. The manifest is the apply-time authority (destructive
    classification, checksum, prerequisites); an unlisted file is
    refused, never run on the filename heuristic alone."""

    code = "migration_manifest_entry_missing"


class MigrationManifestChecksumMismatch(MigrationManifestViolation):
    """A pending ordered migration file's sha256 disagrees with the
    ``checksum_sha256`` its migrations/manifest.yaml entry declares —
    the file or the manifest was edited without the other."""

    code = "migration_manifest_checksum_mismatch"


class MigrationManifestDestructiveDisagreement(MigrationManifestViolation):
    """The manifest's ``destructive`` flag disagrees with the filename
    heuristic (slug prefix ``retire_``/``drop_``/``remove_``) for a
    pending ordered migration. Either the file was renamed or the
    manifest was mis-edited; a human must reconcile before apply."""

    code = "migration_manifest_destructive_disagreement"


class MigrationManifestPrerequisiteUnmet(MigrationManifestViolation):
    """A pending ordered migration declares a machine-checkable
    prerequisite of the form ``migration <id> is applied`` that is
    neither in the ledger nor scheduled to run earlier in the same
    ordered batch. Prose prerequisites (relation existence, operator
    confirmations) are informational and not evaluated here."""

    code = "migration_manifest_prerequisite_unmet"


class MigrationClassInvalid(MigrationManifestViolation):
    """A migrations/manifest.yaml entry declares a ``class`` value that is
    not a recognized migration class (``single_transaction`` /
    ``multi_phase``), or an unsupported combination (a destructive
    multi-phase migration). An unrecognized class is refused, never
    defaulted to single-transaction — a default would silently pull a
    declared-online migration back inside migrate's one transaction."""

    code = "migration_class_invalid"


class MigrationClassUndeclared(MigrationManifestViolation):
    """A pending ordered migration is a multi-statement data migration
    (two or more top-level INSERT/UPDATE/DELETE/MERGE statements) but its
    migrations/manifest.yaml entry does not declare
    ``class: multi_phase``. The single-transaction contract is sized for
    DDL and bounded seeds; batched data motion must declare itself so it
    runs checkpointed and resumable instead of inside one transaction."""

    code = "migration_class_undeclared"


class MultiphaseMigrationInvalid(SubstrateError):
    """The multi-phase migration driver refused to run: the progress
    table is absent (``okg migrate`` has not installed the current
    schema), the supplied phase spec does not match the migration, the
    manifest entry does not declare ``class: multi_phase``, or the
    durable progress state contradicts the ledger. Fail-closed: the
    driver never improvises schema or reinterprets progress rows."""

    code = "multiphase_migration_invalid"


class MigrateAuthorityUnresolvable(SubstrateError):
    """The deployment-authority reverse mapping for a gated operation
    could not be resolved: the DSN failed conninfo parsing, or the
    deployments root / a deployment manifest could not be read. The
    gate fails CLOSED — this refusal is raised instead of proceeding
    ungated (the audit F5 `except Exception: return None` defect)."""

    code = "migrate_authority_unresolvable"


# ---------- operator safety ----------

class RequiresConfirm(SubstrateError):
    code = "requires_confirm"


class SourceRetirementRefused(SubstrateError):
    """A reviewed source-retirement transition failed a safety precondition."""

    code = "source_retirement_refused"


class CommandRetired(SubstrateError):
    """A retired CLI command was run; it writes nothing and names its replacement.

    `okg new-source` (issue #2479) is the first: its reader skeleton was not
    a working reader, so the command now refuses instead of writing one.
    """

    code = "command_retired"


# ---------- MCP serve path ----------

class MCPPoolUnavailable(SubstrateError):
    """MCP connection-pool acquisition failed (server unreachable,
    exhausted pool, or stale credentials). Distinguishable from policy
    errors so an infrastructure failure is never dressed as a denial
    (issue #950)."""
    code = "mcp_pool_unavailable"


# ---------- v2+ stubs (defined for registry parity) ----------

class PrimaryOverload(SubstrateError):
    """v2+ replica-routing tier overload. Not raised in v1."""
    code = "primary_overload"


# ---------- connector registration (#1181 slice 4) ----------

class ConnectorRegistrationError(SubstrateError):
    """Base for every connector-registration refusal.

    It defines no `code` of its own so that catching the whole family
    stays one `except`, while each refusal below carries the code an
    operator sees. Exported as `okg.deployment.ConnectorRegistrationError`,
    so a connector author never imports this module to catch one.
    """


class ConnectorCapabilityUnknown(ConnectorRegistrationError):
    code = "connector_capability_unknown"


class ConnectorAuthModeUnknown(ConnectorRegistrationError):
    code = "connector_auth_mode_unknown"


class ConnectorOutputSignatureInvalid(ConnectorRegistrationError):
    code = "connector_output_signature_invalid"


class ConnectorConfigSchemaInvalid(ConnectorRegistrationError):
    code = "connector_config_schema_invalid"


class ConnectorRequiredModuleUnqualified(ConnectorRegistrationError):
    code = "connector_required_module_unqualified"


class ConnectorSdkVersionUnsupported(ConnectorRegistrationError):
    code = "connector_sdk_version_unsupported"


class ConnectorRegistrationDeprecated(ConnectorRegistrationError):
    code = "connector_registration_deprecated"


# ---------- connector conformance kit (#1181 slice 7) ----------

class ConnectorConformanceCaseInvalid(SubstrateError):
    """The conformance kit was pointed at something that is not a case.

    Not a `ConnectorRegistrationError`: the registration is fine, the
    `--okg-connector` reference is not. Kept separate so a harness
    misconfiguration is never reported as a connector defect.
    """

    code = "connector_conformance_case_invalid"


# ---------- connector declaration read back (#1181 slice 8) ----------

class ConnectorAssetPayloadInvalid(ConnectorRegistrationError):
    """A declared `connector` asset body is not a registration.

    `from_asset_payload` used to let a malformed body raise a bare
    `TypeError` from the dataclass constructor, which no caller could
    catch as a registration refusal and no registry listed.
    """

    code = "connector_asset_payload_invalid"


# ---------- guided add-source flow (#1802) ----------

class AddSourceError(SubstrateError):
    """A guided `okg deployment add-source` step refused.

    `data` names the step and its cause; `fix_hint` is the in-flow
    correction. Raised when a plan with an unresolved problem is applied
    or when the deployment cannot be resolved; nothing is saved.
    """

    code = "add_source_refused"


class RemoveSourceError(SubstrateError):
    """A guided `okg deployment remove-source` step refused (issue #2477).

    `data` names the deployment, the source, and the cause; `fix_hint` names
    the next command. Raised before anything changes.
    """

    code = "remove_source_refused"


class ConnectorSdkBoundInvalid(ConnectorRegistrationError):
    """A declared SDK bound is not a plain PEP 440 release version.

    The window is compared against the running build's *base release*
    (`1.1.0rc1` counts as `1.1.0`), so a bound carrying an epoch,
    pre-release, post-release, dev or local segment could never change a
    verdict. Package v2 refuses such a bound on `okg_compatibility` for
    the same reason; a connector's own window is refused, not silently
    normalized, so the two windows read alike.
    """

    code = "connector_sdk_bound_invalid"


# ---------- source admission: grouped signature check ----------
#
# PACT lane-h-admission-readback. The producer-signature check of a source
# run that wrote straight to the graph evaluates groups of equivalent
# records in SQL.


class SignatureGroupingInconsistent(SubstrateError):
    """Two records the grouping called equivalent got different verdicts.

    The run is not admitted: applying one record's verdict to records it
    does not describe could admit what the signature forbids.
    """

    code = "signature_grouping_inconsistent"


class SourceRunGroupingTimeout(SubstrateError):
    """A grouping statement of the signature check ran past its time limit.

    The run is not admitted. A statement that is still running is not
    bounded by the idle-in-transaction timeout, so the check bounds it.
    """

    code = "source_run_grouping_timeout"


class CompensationRowSetChanged(SubstrateError):
    """A failed run's rows changed between compensation's key read and a page.

    Compensation reads the run's keys first, then fetches and retracts full
    rows a page at a time. When a row's key still exists but the row no
    longer belongs to that source and run, another writer re-attributed it
    in between; retracting the rest would undo a different set than the run
    wrote, so the compensation stops and its transaction rolls back. (A row
    that is simply gone was retracted by a racing compensation of the same
    run and is skipped, as before.)
    """

    code = "compensation_row_set_changed"


class SourceRunUndoUnfinished(SubstrateError):
    """A failed source run's undo has not finished; publishing is refused.

    Issue #2993: a run that fails partway has already changed the working
    branch, and its undo puts the keys it touched back to the run's
    run-base checkpoint. Until that undo completes -- or an operator keeps
    the current rows with ``okg runtime finish-undo --accept-current`` --
    any publish of the deployment would show readers the half-undone
    state, so the publisher refuses every publish.
    """

    code = "source_run_undo_unfinished"


class SourceRunBaseMissing(SubstrateError):
    """A failed run applied chunks but its run-base checkpoint is gone.

    Every run on a branch where its deployment has published cuts a run base
    before its first graph write. Without it the run cannot be undone
    exactly, so compensation refuses rather than retract only the run's own
    rows; the receipts stay ``compensating`` and publishing is refused until
    ``okg runtime finish-undo --accept-current`` resolves it.
    """

    code = "source_run_base_missing"


class SourceRunUndoTooLarge(SubstrateError):
    """A failed run's undo would touch more keys than one undo may hold.

    The undo holds the branch write lock for its whole length (about 90 s
    for 100,000 keys, growing faster than linearly). Above the bound it
    refuses before writing; the receipts stay ``compensating`` and
    publishing is refused until an operator resolves it.
    """

    code = "source_run_undo_too_large"


class SourceRunInProgress(SubstrateError):
    """Another source run of the deployment is still writing.

    Its committed chunks are on the working branch but are not a complete
    run yet, so a publish now would show readers its unfinished records
    (issue #2993 round 6). The run holds a session lock while it is alive,
    so this refusal lasts only as long as that session: once the run ends it
    is either finished (its run base goes) or an unfinished undo.
    """

    code = "source_run_in_progress"


class SourceRerunRequired(SubstrateError):
    """An operator kept a failed run's partial rows; the source must rerun.

    ``okg runtime finish-undo --accept-current`` keeps the rows of a run whose
    undo did not finish. Those rows are still the failed run's partial state,
    so every publish of the deployment stays refused until the source's next
    run succeeds, or the source is retired with ``okg remediate
    retire-source`` (issue #2993, option 2).
    """

    code = "source_rerun_required"


# ---------- operator UI launcher (`okg ui`) ----------

class OperatorUiBundleMissing(SubstrateError):
    """`okg ui` found no built operator UI bundle to serve.

    The frontend is TypeScript built by Vite; the wheel does not ship it, so a
    bundle exists only where someone ran `npm run operator-ui:build` (PACT
    okg-ui-command). The command refuses rather than serving the read API
    alone, which would leave the operator at a JSON 410 page.
    """

    code = "operator_ui_bundle_missing"


class OperatorUiReadOnly(SubstrateError):
    """`okg ui` refused a request whose method is not GET, HEAD or OPTIONS.

    The operator UI issues only reads. Its front door answers every other
    method with HTTP 405 and this code, so no write route of the read API is
    reachable through it (PACT okg-ui-command).
    """

    code = "operator_ui_read_only"


class OperatorUiBindRefused(SubstrateError):
    """`okg ui` refused a bind that is not loopback, or an unusable port.

    The command serves loopback only (localhost, 127.0.0.0/8, ::1), with or
    without an operator token; remote access goes through an SSH tunnel. A
    port outside 1-65535 or a non-integer OKG_VIZ_PORT is refused here too
    (PACT okg-ui-command).
    """

    code = "operator_ui_bind_refused"


# ---------- registry self-check ----------

class VizReadAuthRequired(SubstrateError):
    """The read UI/API for a sensitive deployment was started without a token
    (issue #3278). Sensitive means the manifest turns Nomos runtime
    enforcement on, or the mode cannot be determined."""

    code = "viz_read_auth_required"


def all_error_codes() -> set[str]:
    """Every code defined by a SubstrateError SUBCLASS.

    The root SubstrateError.code = 'substrate_error' is excluded
    deliberately: it is the catch-all for ad-hoc errors raised via
    `SubstrateError(message, ...)` (e.g. dsn_not_set,
    bootstrap_refused) where the code is set per-instance, not by
    class. The parity test in tests/substrate/test_error_codes_parity.py
    cross-checks this against docs/reference/error_codes.yaml in BOTH
    directions.
    """
    seen: set[str] = set()
    stack: list[type] = list(SubstrateError.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls.code != SubstrateError.code:
            seen.add(cls.code)
        stack.extend(cls.__subclasses__())
    return seen
