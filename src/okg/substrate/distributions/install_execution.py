"""Verified bootstrap and shared execution for the existing install entry point.

This module is framework-owned. It verifies installed origins before importing
CLI discovery or a consumer module. Package preparation feeds the shared
execution spine; checkpoint metadata alone never authorizes it.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, redirect_stdout
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import sysconfig
from typing import Mapping

from .packaged_release import (
    WheelInput, PackagedReleaseIdentity, configure_packaged_imports,
    verify_prospective_package, verified_release_scope,
)
from okg.substrate.errors import SubstrateError


@dataclass(frozen=True)
class BootstrapInputs:
    framework: WheelInput
    consumer: WheelInput

    def verify(self) -> None:
        for item in (self.framework, self.consumer):
            verify_prospective_package(item.package, wheel_path=item.wheel_path,
                                      expected_wheel_digest=item.digest,
                                      installed_root=item.installed_root)


_BOOTSTRAP: ContextVar[BootstrapInputs | None] = ContextVar("install_bootstrap", default=None)


def active_bootstrap() -> BootstrapInputs | None:
    result = _BOOTSTRAP.get()
    if result is not None:
        result.verify()
    return result


_INSTALL_REASON_CODES = frozenset({
    'authorized_verification_query_returned_no_matching_evidence',
    'bundle_profile_identity_mismatch',
    'configuration_missing_required_files',
    'configuration_path_invalid',
    'consumer_must_be_distinct',
    'deployment_manifest_unreadable',
    'deployment_source_registry_unreadable',
    'database_and_explicit_cluster_bootstrap_authority_required',
    'database_secret_reference_required',
    'database_secret_reference_unresolved',
    'database_version_or_bootstrap_privileges_unsupported',
    'initial_database_foreign_extension',
    'initial_database_not_empty',
    'install_stage_result_malformed',
    'installation_database_drift',
    'installation_directory_foreign',
    'isolated_bytecode_disabled_launcher_required',
    'package_bundle_name_and_target_required',
    'packaged_module_member_asset_unsupported',
    'packaged_module_owner_reserved',
    'packaged_module_owner_unplanned',
    'packaged_module_unplanned',
    'packaged_module_provider_drift',
    'packaged_release_declaration_required',
    'personal_private_requires_source_policy',
    'pinned_launcher_required',
    'profile_dependency_not_selected',
    'profile_dependency_outside_package',
    'profile_file_overlaps_package_module',
    'profile_module_selection_drift',
    'profile_modules_not_selected',
    'profile_not_selected',
    'published_generation_readback_failed',
    'required_database_extensions_unavailable',
    'required_publication_proof_failed',
    'selected_bundle_profile_absent',
    'selected_install_publication_required',
    'source_credential_declaration_kind_unsupported',
    'source_credential_declaration_malformed',
    'verification_query_source_evidence_missing',
    'verification_requires_manifest_read_principal',
    'update_requires_published_installation',
    'update_source_invalid',
    'search_mode_unsupported',
    'search_requires_published_installation',
    'search_requires_enforced_policy',
    'search_authorization_missing',
    'update_requires_publication',
    'update_source_modes_incompatible',
    'update_publication_missing',
    'update_mode_unsupported',
})

def _refuse(reason: str, **detail: str) -> None:
    """Refuse with a known reason code, plus optional identifier detail.

    ``detail`` values name a condition inside the reason (for example which of
    the published-generation readback's checks fired); they are internal
    identifiers, never external text.
    """
    if reason not in _INSTALL_REASON_CODES:
        raise ValueError("unknown internal installation reason")
    raise SubstrateError("packaged installation refused", data={"reason": reason, **detail})


def _configuration_digest(files: Mapping[str, bytes]) -> str:
    from okg.substrate.contracts.deployment_product.lock import canonical_json_bytes, sha256_digest
    return sha256_digest(canonical_json_bytes({
        name: "sha256:" + hashlib.sha256(content).hexdigest()
        for name, content in sorted(files.items())
    }))


def _write_install_receipt(path: Path, receipt: Mapping) -> None:
    """Persist finite runtime measurements without partially replacing a receipt."""
    import tempfile
    content = json.dumps(receipt, sort_keys=True, allow_nan=False)
    descriptor, temporary = tempfile.mkstemp(prefix=".install-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _database_identity(dsn: str, *, initial: bool, authorize_cluster_bootstrap: bool) -> dict:
    """Read-only prerequisite check before deployment/catalog/source mutations."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.db.migrate import REQUIRED_EXTENSIONS
    if not dsn or not authorize_cluster_bootstrap:
        _refuse("database_and_explicit_cluster_bootstrap_authority_required")
    with db_connect(dsn, connect_timeout=5, autocommit=True, application_name="okg-install") as conn:
        database, oid, system_id, version, superuser, create_role = conn.execute(
            "SELECT current_database(), (SELECT oid FROM pg_database WHERE datname=current_database()), "
            "(SELECT system_identifier FROM pg_control_system()), current_setting('server_version_num')::int, "
            "rolsuper, rolcreaterole FROM pg_roles WHERE rolname=current_user"
        ).fetchone()
        if version < 150000 or not (superuser or create_role):
            _refuse("database_version_or_bootstrap_privileges_unsupported")
        available = {row[0] for row in conn.execute("SELECT name FROM pg_available_extensions")}
        if not set(REQUIRED_EXTENSIONS).issubset(available):
            _refuse("required_database_extensions_unavailable")
        if initial:
            installed = {row[0] for row in conn.execute("SELECT extname FROM pg_extension")}
            if installed - set(REQUIRED_EXTENSIONS) - {"plpgsql"}:
                _refuse("initial_database_foreign_extension")
            populated = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
                "AND n.nspname NOT LIKE 'pg_toast%%' AND c.relkind IN ('r','p','v','m','S') "
                "AND NOT EXISTS(SELECT 1 FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid "
                "WHERE d.classid='pg_class'::regclass AND d.objid=c.oid "
                "AND d.refclassid='pg_extension'::regclass AND d.deptype='e' "
                "AND e.extname=ANY(%s)))", (list(REQUIRED_EXTENSIONS),),
            ).fetchone()[0]
            if populated:
                _refuse("initial_database_not_empty")
    return {"database": database, "oid": oid, "system_identifier": str(system_id)}


_STAGE_RECEIPTS: ContextVar[tuple[Path, list] | None] = ContextVar("install_stage_receipts", default=None)


@contextmanager
def _installation_receipts(root: Path):
    """One durable private stage journal per invocation, separate from identity."""
    import uuid
    path = root / ("install-stages-" + uuid.uuid4().hex + ".json")
    token = _STAGE_RECEIPTS.set((path, []))
    try:
        yield
    finally:
        _STAGE_RECEIPTS.reset(token)


def _stage_receipt(stage: str, status: str, result: Mapping | None = None):
    from datetime import datetime, timezone
    from okg.substrate.sources.redaction import redact_mapping
    active = _STAGE_RECEIPTS.get()
    if active is None:
        return
    path, entries = active
    sanitized = redact_mapping(dict(result or {}))
    reason = (result or {}).get("reason")
    if type(reason) is str and reason in _INSTALL_REASON_CODES:
        sanitized["reason"] = reason
    entries.append({"stage": stage, "status": status,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "result": sanitized})
    _write_install_receipt(path, {"schema": "okg.install-stage-receipts/v1", "stages": entries})


@contextmanager
def _stage(name: str):
    """Keep an actionable, redacted cause across the CLI error boundary."""
    from okg.substrate.sources.redaction import redact_text, redact_mapping
    _stage_receipt(name, "started")
    try:
        yield
    except (OSError, ValueError, SubstrateError) as exc:
        data = {"exception": type(exc).__name__}
        if isinstance(exc, SubstrateError):
            data.update(redact_mapping(exc.data))
            if type(exc.data.get("reason")) is str and exc.data["reason"] in _INSTALL_REASON_CODES:
                data["reason"] = exc.data["reason"]
            data["cause_code"] = exc.code
        from .package import DistributionPackageError
        if isinstance(exc, DistributionPackageError) and exc.code == "installed_wheel_path_unsafe":
            data["package_code"] = "installed_wheel_path_unsafe"
            cause = exc.__cause__
            if isinstance(cause, OSError) and type(cause.errno) is int:
                data["filesystem_errno"] = cause.errno
        data["stage"] = name
        _stage_receipt(name, "failed", data)
        raise SubstrateError(
            redact_text(str(exc)), data=data,
            fix_hint=redact_text(exc.fix_hint) if isinstance(exc, SubstrateError) and exc.fix_hint
            else "Correct this stage's reported prerequisite and retry with the same installation inputs.",
        ) from exc
    else:
        _stage_receipt(name, "completed")


def _capture(command, args) -> dict:
    output = io.StringIO()
    with redirect_stdout(output):
        result = command(args, as_json=True)
    try:
        payload = json.loads(output.getvalue())
    except (ValueError, IndexError) as exc:
        raise SubstrateError("install stage returned no structured result") from exc
    if not isinstance(payload, dict):
        _refuse("install_stage_result_malformed")
    summary = payload.get("payload", payload)
    if command.__name__ == "cmd_search":
        summary = {key: summary.get(key) for key in ("generation_id", "nomos_effect", "nomos_decision_id", "status", "reason")}
        rows = payload.get("results")
        summary["result_count"] = len(rows) if isinstance(rows, list) else None
    # Decision 23a: provision's capacity-warning exit (8) means the publish
    # committed; the stage proceeds and its receipt keeps the warning.
    from okg.substrate.cli.run_summary import EXIT_CAPACITY_WARNING

    succeeded = result == 0 or (
        command.__name__ == "_cmd_provision" and result == EXIT_CAPACITY_WARNING
    )
    _stage_receipt(command.__name__, "returned" if succeeded else "refused", summary)
    if not succeeded:
        from okg.substrate.sources.redaction import redact_text, redact_mapping
        envelope = payload.get("payload", payload)
        error = envelope.get("error", {}) if isinstance(envelope, dict) else {}
        code = error.get("code", "install_stage_failed") if isinstance(error, dict) else "install_stage_failed"
        hint = error.get("fix_hint") if isinstance(error, dict) else None
        raise SubstrateError(
            "packaged installation stage refused",
            data={"stage": command.__name__, "reason": redact_text(code),
                  "stage_result": redact_mapping(envelope)},
            fix_hint=redact_text(hint) if hint else "correct the reported stage prerequisite and retry the same install",
        )
    return payload


def _registry_declares_relationships(deployment: str) -> bool:
    """Does any source in this deployment's registry declare relationship output?

    Reads the declaration, never the published rows: a source declares
    relationships when its admission policy lists ``output_signature.edges`` or
    allows the ``edge`` record kind. Every registry entry counts, not only the
    sources this install selected, so a deployment that also carries an
    edge-declaring source keeps the strict rule.

    Fail-closed: a legacy entry, or one whose admission policy carries no
    output signature, reads as declaring relationships. An undeclared connector
    therefore never loosens the gate.
    """
    from okg.substrate.deployments import (
        DeploymentConfigError, DeploymentResolutionError, resolve_deployment)
    from okg.substrate.sources.registry import load_source_registry
    # The declaration decides whether zero relationships is a success, so a
    # failure to read it refuses, never guesses in either direction. The two
    # failures name different files, so they carry different reasons: an
    # operator told the registry is unreadable should not go looking at the
    # registry when the deployment manifest is what is broken.
    try:
        resolved = resolve_deployment(str(deployment))
        registry_path = resolved.source_registry
    except (DeploymentConfigError, DeploymentResolutionError,
            OSError, ValueError, AttributeError):
        # DeploymentResolutionError is a sibling of DeploymentConfigError, not a
        # subclass, so it needs naming: an unresolvable deployment must refuse
        # by reason like an unreadable one, not escape raw.
        _refuse("deployment_manifest_unreadable")
    try:
        entries = load_source_registry(registry_path)
    except (OSError, ValueError):
        _refuse("deployment_source_registry_unreadable")
    for entry in entries.values():
        policy = getattr(entry, "admission_policy", None)
        if policy is None or policy.is_compat_summary_only:
            return True
        if policy.output_signature.get("edges") or "edge" in policy.allowed_record_kinds:
            return True
    return False


def _verify_installed_reader(*, dsn: str, deployment: str, generation: str, query: str,
                            declares_relationships: bool | None = None) -> dict:
    """Exercise the selected CLI reader and active policy on the exact publish.

    ``declares_relationships`` is the deployment's registry declaration, read
    once by the caller so the publish gate and this readback judge the same
    install by the same rule. ``None`` reads it here instead.
    """
    from okg.substrate.cli._read_side import cmd_search, _resolve_cli_read_subject
    from okg.substrate.cli.provision import _generation_row_counts
    from okg.substrate.db.connection import connect
    from okg.substrate.nomos.doctor import source_lineage_readiness_report
    subject, subject_metadata = _resolve_cli_read_subject(deployment=deployment, default_kind="cli_search")
    if subject_metadata.get("source") != "manifest_read_principal":
        _refuse("verification_requires_manifest_read_principal")
    observed, nodes, edges = _generation_row_counts(dsn, generation_id=generation, deployment_name=deployment)
    if str(observed) != str(generation):
        _refuse("published_generation_readback_failed", case="generation_identity_mismatch")
    if nodes is None or edges is None:
        _refuse("published_generation_readback_failed", case="counts_unavailable")
    if nodes <= 0:
        _refuse("published_generation_readback_failed", case="records_absent")
    # Issue #1914: a records-only connector is a supported installed source, so
    # zero relationships is only a defect when a source declares relationship
    # output. The registry is consulted lazily, on the zero-relationship path
    # alone, so an ordinary install never pays for it.
    if edges <= 0:
        declared = (_registry_declares_relationships(deployment)
                    if declares_relationships is None else declares_relationships)
        if declared:
            _refuse("published_generation_readback_failed", case="relationships_absent")
    with connect(dsn) as conn:
        lineage = source_lineage_readiness_report(conn, deployment_name=deployment, generation_id=generation)
    if (lineage.get("ok") is not True or lineage.get("runtime_enforcement_mode") != "enforce"
            or not lineage.get("active_policy_count")):
        raise SubstrateError("installed source lineage is not ready", data={"lineage": lineage})
    _stage_receipt("publication_readback", "completed", {"generation_id": generation,
        "node_count": nodes, "edge_count": edges, "lineage": lineage})
    result = _capture(cmd_search, argparse.Namespace(
        deployment=deployment, dsn=dsn, generation_id=generation, query=query,
        method="lexical", subtype=None, limit=10, offset=0,
        select=["node_id,subtype,attrs,provenance,source_record_id"], nomos_access_context={},
    ))
    rows = result.get("results")
    if (str(result.get("generation_id")) != str(generation) or result.get("nomos_effect") != "allow"
            or not isinstance(rows, list) or not rows):
        raise SubstrateError("authorized verification query returned no matching evidence",
            data={"reason": "authorized_verification_query_returned_no_matching_evidence",
                  "expected_generation_id": str(generation), "observed_generation_id": result.get("generation_id"),
                  "nomos_effect": result.get("nomos_effect"), "result_count": len(rows) if isinstance(rows, list) else None,
                  "reader_status": result.get("status"), "reader_reason": result.get("reason")},
            fix_hint="verify the selected reader policy and search phrase against the published source data")
    if any(not isinstance(row, dict) or not row.get("node_id") or not isinstance(row.get("provenance"), dict)
           or not row["provenance"].get("source") for row in rows):
        _refuse("verification_query_source_evidence_missing")
    if any(not isinstance(row.get("source_record_id"), dict) or not row["source_record_id"] for row in rows):
        _refuse("verification_query_source_evidence_missing")
    evidence_rows = []
    for row in rows:
        text = row.get("attrs", {}).get("text") if isinstance(row.get("attrs"), dict) else None
        if not isinstance(text, str) or not text.strip() or text == "[nomos:masked]":
            _refuse("verification_query_source_evidence_missing")
        # Verify the actual returned text, but retain only identity and a hash
        # in installation evidence; source content stays out of receipts.
        evidence_rows.append({key: row[key] for key in ("node_id", "subtype", "provenance", "source_record_id") if key in row})
        evidence_rows[-1]["returned_text_sha256"] = hashlib.sha256(text.encode()).hexdigest()
    return {"surface": "cli_search", "subject": subject, "generation_id": generation,
            "node_count": nodes, "edge_count": edges, "lineage": lineage,
            "nomos_decision_id": result.get("nomos_decision_id"),
            "results": evidence_rows}


def _require_redaction_runtime(registry: Mapping) -> None:
    """Refuse missing offline model inputs before Presidio can download them."""
    policies = [entry.get("source_policy", {}) for entry in registry.get("sources", {}).values()
                if isinstance(entry, dict)]
    if not any(policy.get("store_raw") is False and "free_text" in policy.get("pii_classes", {}).values()
               for policy in policies):
        return
    import importlib.metadata
    import importlib.util
    for distribution, module in (("presidio-analyzer", "presidio_analyzer"), ("en-core-web-lg", "en_core_web_lg")):
        try:
            importlib.metadata.version(distribution)
            if importlib.util.find_spec(module) is None:
                raise importlib.metadata.PackageNotFoundError(distribution)
        except (ImportError, ValueError):
            raise SubstrateError("the pinned offline redaction runtime is unavailable",
                fix_hint="use a release containing the privacy dependencies and en_core_web_lg model; no raw-storage fallback is allowed") from None
    from okg.substrate.ingest.presidio_detector import build_presidio_detector
    detector = build_presidio_detector()
    detector.detect("technical installation readiness")


def _pseudonym_secret(root: Path, *, create: bool, expected: Mapping | None) -> tuple[str, dict]:
    """Retain one private instance key; missing or changed keys never rotate."""
    import secrets
    import stat
    import tempfile
    from .install_launcher import _open_regular
    path = root / "pseudonym.key"
    if create:
        key = secrets.token_hex(32)
        fd, temporary = tempfile.mkstemp(prefix=".pseudonym-", dir=root)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(key.encode("ascii"))
                stream.flush()
                os.fsync(stream.fileno())
            # Link is atomic and refuses an existing destination, including a
            # dangling symlink. A crashed temporary file is never adopted.
            os.link(temporary, path)
        finally:
            os.unlink(temporary)
    try:
        with _open_regular(path) as stream:
            info = os.fstat(stream.fileno())
            raw = stream.read(65)
            if stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise ValueError("private key ownership")
        if len(raw) != 64 or any(c not in b"0123456789abcdef" for c in raw):
            raise ValueError("private key format")
        binding = {"schema": "okg.install-pseudonym-key/v1", "sha256": hashlib.sha256(raw).hexdigest()}
        if not create and expected != binding:
            raise ValueError("private key binding")
        return raw.decode("ascii"), binding
    except (OSError, ValueError):
        raise SubstrateError("owned instance pseudonym key is missing or changed",
            fix_hint="restore the original private instance key; installation never rotates it on retry") from None


@contextmanager
def _pseudonym_environment(key: str | None):
    name = "OKG_PSEUDONYM_HMAC_KEY"
    previous = os.environ.get(name)
    if key is not None:
        os.environ[name] = key
    try:
        yield
    finally:
        if key is not None:
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous


def profile_credential_secrets(
    profile, *, assets, installed_root: Path, consumer_digest: str,
) -> dict[str, dict[str, object]]:
    """The install-plan secrets the selected profile's sources declare.

    Every entry in a selected ``source-defaults/*.yaml`` file that names
    ``credential_refs`` contributes one secret per variable, bound to the
    selected package asset that carries that file. A source with
    ``required_for_baseline: true`` makes its credentials required (an unset
    variable refuses the plan), the same rule ``okg doctor`` uses to fail a
    required source; any other source's credentials are optional and are
    recorded as deferred when unset. ``credential_aliases`` become aliases.
    Only ``env:NAME`` references are passed; the plan turns each into an
    opaque ``secretref:`` token and never records the variable name.

    The package asset that carries such a file must have a source
    declaration kind (``source``, ``source-binding``, ``source-definition``
    or ``source-template``), the set the protocol-v2 conformance validator
    accepts for a credential binding. ``source-template`` is accepted
    because the example connector and the daily-update fixture give every
    bundle file that kind. Any other kind refuses here with the file and
    kind named, so a package author sees which asset to re-kind instead of
    a generic planner refusal.
    """
    from okg.substrate.deployment_bootstrap.profile_init import (
        source_default_entries,
        source_entries_from_default,
    )
    from .install_plan import _SOURCE_DECLARATION_KINDS
    directory = profile.source_defaults_dir
    if directory is None or not directory.is_dir():
        return {}
    by_member = {item["member_path"]: item for item in assets
                 if item.get("object_digest") == consumer_digest and item.get("member_path")}
    paths = sorted(directory.glob("*.yaml"))
    texts = {path.stem: path.read_text(encoding="utf-8") for path in paths}
    # The scaffold's own reader: two files declaring one source id raise
    # DuplicateSourceIdError here, before any credential enters the plan.
    source_default_entries(profile, texts)
    secrets: dict[str, dict[str, object]] = {}
    for path in paths:
        entries = source_entries_from_default(path.stem, texts[path.stem])
        declared = {}
        for source_id, entry in sorted(entries.items()):
            if not isinstance(entry, Mapping) or not entry.get("credential_refs"):
                continue
            refs = entry["credential_refs"]
            aliases = entry.get("credential_aliases") or {}
            required = entry.get("required_for_baseline", False)
            if (isinstance(refs, str) or not isinstance(refs, list)
                    or not all(isinstance(item, str) and item.isidentifier() for item in refs)
                    or not isinstance(aliases, Mapping) or not isinstance(required, bool)):
                _refuse("source_credential_declaration_malformed", source=str(source_id))
            declared[str(source_id)] = (refs, aliases, required)
        if not declared:
            continue
        if not path.is_relative_to(installed_root):
            _refuse("profile_dependency_outside_package")
        asset = by_member.get(path.relative_to(installed_root).as_posix())
        if asset is None:
            _refuse("profile_dependency_not_selected")
        if asset.get("kind") not in _SOURCE_DECLARATION_KINDS:
            _refuse("source_credential_declaration_kind_unsupported",
                    source_defaults=path.name, kind=str(asset.get("kind")),
                    accepted_kinds=",".join(sorted(_SOURCE_DECLARATION_KINDS)))
        declaration = f"{asset['owner']}:{asset['id']}"
        for source_id, (refs, aliases, required) in declared.items():
            for index, variable in enumerate(refs):
                alias_names = aliases.get(variable) or []
                if isinstance(alias_names, str) or not all(
                        isinstance(item, str) and item.isidentifier() for item in alias_names):
                    _refuse("source_credential_declaration_malformed", source=source_id)
                secrets[f"{source_id}.credential.{index}"] = {
                    "reference": f"env:{variable}",
                    "declaration": declaration,
                    "required": required,
                    "aliases": [f"env:{item}" for item in alias_names],
                }
    return secrets


def _packaged_module_files(package, planned_modules) -> dict[str, bytes]:
    """The planned ontology modules, as configuration bytes.

    Read through `package_module_providers` -- the same verified reader the
    install plan composes from, with the owner and module collision guards it
    already carries. The staging tree is temporary; only the bytes travel, into
    the immutable configuration, where the release identity binds them.

    `planned_modules` is the plan's own module list. Only those modules are
    written: an owner the plan does not name supplies nothing, and the built-in
    owner is refused outright, both before a byte is written.

    Returns `{}` for a package that declares no modules, so a built-in-only
    install writes no declaration and behaves exactly as before.
    """
    import tempfile

    from okg.substrate.catalog.module_providers import BUILTIN_OWNER, parse_module_ref
    from okg.substrate.catalog.package_module_provider import package_module_providers

    planned: dict[str, set[str]] = {}
    for ref in planned_modules or ():
        owner, module_name = parse_module_ref(str(ref))
        if owner == BUILTIN_OWNER:
            # Built-ins come from the shipped library; a package cannot supply
            # them, and the manifest key refuses the owner anyway.
            continue
        planned.setdefault(owner, set()).add(module_name)
    if not planned:
        return {}

    # A module asset stored as a member of another object -- a wheel, say --
    # cannot be checked against the plan later: the plan records the enclosing
    # object's digest, not the member's, so every install would refuse. Refuse
    # here, where the cause is still visible.
    for item in package.manifest.assets:
        if getattr(item.locator, "member_path", None) is None:
            continue
        for declaration in tuple(getattr(package.manifest, "module_declarations", ()) or ()):
            owner, module_name = parse_module_ref(str(declaration.module_id))
            if module_name not in planned.get(owner, ()):
                continue
            if any(ref.qualified_id == item.asset.qualified_id
                   for ref in declaration.asset_refs):
                _refuse("packaged_module_member_asset_unsupported")

    files: dict[str, bytes] = {}
    with tempfile.TemporaryDirectory(prefix="okg-install-modules-") as scratch:
        providers = package_module_providers(package, Path(scratch) / "modules")
        for owner, provider in sorted(providers.items()):
            if owner == BUILTIN_OWNER:
                _refuse("packaged_module_owner_reserved")
            if owner not in planned:
                _refuse("packaged_module_owner_unplanned")
            for item in sorted(provider.root.rglob("*")):
                if not item.is_file():
                    continue
                relative = item.relative_to(provider.root)
                if relative.parts[0] not in planned[owner]:
                    # The owner is planned; this module of theirs is not.
                    _refuse("packaged_module_unplanned")
                files[f"modules/{owner}/{relative.as_posix()}"] = item.read_bytes()
    return files


def _check_module_provider_drift(files: Mapping[str, bytes], plan: Mapping) -> None:
    """Refuse unless the configuration composes exactly what the plan recorded.

    Runs on the rendered bytes, before anything is created: a refusal must not
    leave a migrated database behind. The configuration is materialized into a
    private scratch tree and composed there; the target is never touched.

    The plan is what the operator was shown and what the lock binds. Different
    modules, a different provider kind, different module bytes or a different
    subtype owner all refuse.
    """
    import tempfile

    from okg.substrate import fastyaml
    from okg.substrate.catalog.module_providers import parse_module_ref
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.deployments import resolve_declared_module_providers

    manifest = fastyaml.safe_load(files["deployment.yaml"])
    if not isinstance(manifest, dict) or not manifest.get("module_providers"):
        # Nothing declared: a built-in-only install, unchanged from before.
        return
    planned_catalog = plan.get("catalog") if isinstance(plan, Mapping) else None
    planned_modules = plan.get("modules") if isinstance(plan, Mapping) else None
    if not isinstance(planned_catalog, Mapping) or not isinstance(planned_modules, list):
        _refuse("packaged_module_provider_drift")

    def qualified(ref: object) -> str:
        # The manifest spells a built-in module bare and the plan spells it
        # `okg:<name>`; compare them in one spelling, not two.
        owner, module_name = parse_module_ref(str(ref))
        return owner + ":" + module_name

    with tempfile.TemporaryDirectory(prefix="okg-install-drift-") as scratch:
        base = Path(scratch).resolve() / "configuration"
        base.mkdir(mode=0o700)
        for relative, body in files.items():
            destination = base / relative
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            destination.write_bytes(body)
        declared = resolve_declared_module_providers(
            manifest, base, manifest_path=base / "deployment.yaml",
        )
        composed = compose_catalog(base)

        if (sorted(qualified(ref) for ref in composed.modules)
                != sorted(qualified(ref) for ref in planned_catalog.get("module_refs") or ())):
            _refuse("packaged_module_provider_drift")

        planned_origin = planned_catalog.get("subtype_origin")
        if not isinstance(planned_origin, Mapping):
            _refuse("packaged_module_provider_drift")
        # `deployment:<name>` origins come from the installed schemas/
        # directory, which the plan's in-memory composition never had.
        # Compare only the subtypes a module owns.
        observed = {
            subtype: qualified(origin)
            for subtype, origin in composed.subtype_origin.items()
            if not str(origin).startswith("deployment:")
        }
        expected = {
            subtype: qualified(origin)
            for subtype, origin in planned_origin.items()
            if not str(origin).startswith("deployment:")
        }
        if observed != expected:
            _refuse("packaged_module_provider_drift")

        for row in planned_modules:
            if not isinstance(row, Mapping) or not row.get("module_id"):
                _refuse("packaged_module_provider_drift")
            origins = {
                qualified(ref): value
                for ref, value in composed.module_origins.items()
            }
            origin = origins.get(qualified(row["module_id"]))
            if not isinstance(origin, Mapping):
                _refuse("packaged_module_provider_drift")
            if origin.get("provider_kind") != row.get("provider_kind"):
                _refuse("packaged_module_provider_drift")
            if row.get("provider_kind") != "package":
                continue
            planned_digests = row.get("object_digests")
            if not isinstance(planned_digests, list) or not planned_digests:
                _refuse("packaged_module_provider_drift")
            # The declaration carries no per-module digests -- the
            # configuration digest binds these bytes, and a second copy of a
            # digest can only disagree with the first. Compare the bytes: the
            # installer wrote each module file straight from the package
            # object, so a module file still hashes to its object digest.
            owner, module_name = parse_module_ref(str(row["module_id"]))
            module_dir = declared.get(owner)
            if module_dir is None:
                _refuse("packaged_module_provider_drift")
            observed_digests = sorted(
                "sha256:" + hashlib.sha256(item.read_bytes()).hexdigest()
                for item in sorted((module_dir / module_name).rglob("*"))
                if item.is_file()
            )
            if observed_digests != sorted(str(d) for d in planned_digests):
                _refuse("packaged_module_provider_drift")


def execute_prepared_install(
    *, bootstrap: BootstrapInputs, root: Path, files: Mapping[str, bytes],
    plan: Mapping, lock: Mapping, translation: str, dsn: str,
    authorize_cluster_bootstrap: bool,
    verification_query: str,
    no_publish: bool = False,
    source_modes: Mapping[str, str] | None = None,
    update_source: str | None = None,
    search_query: str | None = None,
    search_generation: str | None = None,
) -> dict:
    """Run finalized package configuration through real bootstrap and provision.

    ``files`` is renderer output, never an operator-selected configuration
    subset. Immutable configuration and mutable installation receipts are
    siblings. Reapply accepts only exactly the original release and database.
    """
    from .packaged_release import verify_plan_binding
    from okg.substrate import fastyaml
    bootstrap.verify()
    if not {"deployment.yaml", "source_registry.yaml"}.issubset(files):
        _refuse("configuration_missing_required_files")
    for name in files:
        path = Path(name)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            _refuse("configuration_path_invalid")
    manifest = fastyaml.safe_load(files["deployment.yaml"])
    if not isinstance(manifest, dict) or manifest.get("release", {}).get("kind") != "packaged":
        _refuse("packaged_release_declaration_required")
    identity = PackagedReleaseIdentity(
        bootstrap.framework.digest, bootstrap.consumer.digest,
        _configuration_digest(files), lock["lock_digest"], translation,
    )
    verify_plan_binding(identity, plan, lock)
    # Before anything is created: a refusal here must not leave a configuration
    # tree, a bootstrapped cluster or a migrated database behind.
    with _stage("module_provider_drift"):
        _check_module_provider_drift(files, plan)
    root = root.absolute()
    marker = root / "install.json"
    if update_source is not None:
        if not update_source or update_source.strip() != update_source or "," in update_source:
            _refuse("update_source_invalid")
        if no_publish:
            _refuse("update_requires_publication")
        if not root.exists():
            _refuse("update_requires_published_installation")
    if search_query is not None or search_generation is not None:
        if (not isinstance(search_query, str) or not search_query.strip()
                or update_source is not None or no_publish
                or (search_generation is not None and not search_generation.strip())):
            _refuse("search_mode_unsupported")
        if not root.exists():
            _refuse("search_requires_published_installation")
    existing = None
    if root.exists():
        if root.is_symlink() or marker.is_symlink() or not marker.is_file():
            _refuse("installation_directory_foreign")
        existing = json.loads(marker.read_text())
        if (not isinstance(existing, dict) or existing.get("schema") != "okg.install-receipt/v1"
                or existing.get("state") not in {"prepared", "staged", "resumed", "published"}):
            raise SubstrateError("invalid installer receipt schema or state")
        if existing["state"] == "staged":
            from .install_stage import InstallStage
            InstallStage.from_dict(existing.get("stage"))
            if existing.get("published") is not False or not isinstance(existing.get("provision"), dict):
                raise SubstrateError("invalid staged installer receipt")
        elif existing["state"] in {"published", "resumed"}:
            required = ("reader", "provision") if existing["state"] == "published" else ("stage_publication", "stage_publish_proof")
            if existing.get("published") is not True or any(not isinstance(existing.get(key), dict) for key in required):
                raise SubstrateError("invalid published installer receipt")
        identity.require_same(PackagedReleaseIdentity.from_metadata(existing.get("release")))
    if update_source is not None and (existing is None or existing["state"] != "published"):
        _refuse("update_requires_published_installation")
    if search_query is not None and (existing is None or existing["state"] != "published"):
        _refuse("search_requires_published_installation")
    registry = fastyaml.safe_load(files["source_registry.yaml"])
    _require_redaction_runtime(registry)
    needs_pseudonym = any(isinstance(entry, dict) and
        entry.get("source_policy", {}).get("store_raw") is False
        for entry in registry.get("sources", {}).values())
    key = None
    key_binding = None
    if existing is not None and needs_pseudonym:
        key, key_binding = _pseudonym_secret(root, create=False, expected=existing.get("pseudonym_key"))
    database = _database_identity(dsn, initial=existing is None,
                                  authorize_cluster_bootstrap=authorize_cluster_bootstrap)
    if existing is not None and existing.get("database") != database:
        _refuse("installation_database_drift")
    deployment = root / "configuration"
    if existing is None:
        root.mkdir(mode=0o700)
        deployment.mkdir(mode=0o700)
        for name, content in files.items():
            destination = deployment / name
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with destination.open("xb") as stream:
                stream.write(content)
            destination.chmod(0o600)
        if needs_pseudonym:
            key, key_binding = _pseudonym_secret(root, create=True, expected=None)
        _write_install_receipt(marker, {"schema": "okg.install-receipt/v1", "pseudonym_key": key_binding,
                           "release": identity.to_dict(), "database": database, "state": "prepared"})
    with _installation_receipts(root), _pseudonym_environment(key), verified_release_scope(deployment_dir=deployment, identity=identity,
                                framework=bootstrap.framework, consumer=bootstrap.consumer,
                                configuration_files=tuple(files), plan=plan, lock=lock):
        if search_query is not None:
            return _execute_search(deployment=deployment, dsn=dsn,
                query=search_query, generation=search_generation, manifest=manifest)
        if update_source is not None:
            return _execute_update(deployment=deployment, source=update_source, dsn=dsn,
                query=verification_query, source_modes=source_modes or {}, identity=identity,
                database=database)
        from okg.substrate.db.migrate import ensure_required_extensions, migrate
        from okg.substrate.runtime import bootstrap_dbos_schema
        from okg.substrate.deployments import resolve_deployment
        from okg.substrate.cli.provision import _cmd_provision
        # Issue #1914: one reading of the deployment's registry declaration
        # decides, for this whole install, whether zero relationships is a
        # success -- the staged resume's publish proof, the ordinary publish
        # gate, the repeat's predecessor observation and the readback all judge
        # by it. Read before the staged resume, which runs its own proof.
        with _stage("output_declaration"):
            declares_relationships = _registry_declares_relationships(deployment)
            records_only = not declares_relationships
        stage_resume = None
        if existing is not None and existing.get("state") == "staged":
            from .install_stage import InstallStage, capture_stage
            stage = InstallStage.from_dict(existing.get("stage"))
            if stage.deployment != manifest.get("name"):
                raise SubstrateError("installer stage deployment mismatch")
            capture_stage(dsn, deployment=stage.deployment, admission_ids=stage.admission_ids, expected=stage)
            if no_publish:
                return existing
            from okg.substrate.ingest.service import IngestOptions, _publish
            with _stage("staged_publication"):
                resumed = _publish(IngestOptions(dsn=dsn, deployment=stage.deployment, deployment_dir=deployment),
                    "installer-stage", expected_latest_checkpoint_id=stage.latest_checkpoint_id,
                    expected_branch_mutation_token=stage.branch_mutation_token,
                    require_expected_latest_checkpoint=True, expected_admission_ids=stage.admission_ids,
                    expected_install_stage=stage)
                if resumed.get("status") != "published":
                    raise SubstrateError("installer stage publication failed", data={"publication": resumed})
            from okg.substrate.cli.provision import _provision_publish_proof
            resumed_proof = _provision_publish_proof(dsn, resumed, deployment_name=stage.deployment,
                                                     records_only=records_only)
            if resumed_proof.get("ok") is not True:
                raise SubstrateError("installer stage required publication proof failed", data={"proof": resumed_proof})
            stage_resume = {"publication": resumed, "publish_proof": resumed_proof, "admission_ids": list(stage.admission_ids)}
            # Decision 23a: an over-bar lock hold passes; keep it visible.
            from okg.substrate.publish_proof import surface_capacity_warning
            resumed_warning = surface_capacity_warning(resumed_proof, surface="installer staged publication")
            if resumed_warning is not None:
                stage_resume["capacity_warning"] = resumed_warning
            # Durably mark the consumed stage before any subsequent refresh writes.
            _write_install_receipt(marker, {**existing, "state": "resumed", "published": True,
                                           "stage_publication": resumed, "stage_publish_proof": resumed_proof})
        with _stage("database_extensions"):
            ensure_required_extensions(dsn)
        with _stage("database_migrations"):
            migrate(dsn)
        with _stage("deployment_resolution"):
            resolved = resolve_deployment(str(deployment))
        with _stage("runtime_bootstrap"):
            bootstrap_dbos_schema(deployment=resolved.name, dsn=dsn, runtime=resolved.runtime)
        # A repeat over an installer-owned database (same release, same
        # database identity, any prior receipt state) observes the populated
        # published generation BEFORE its sources run. Provision binds a
        # zero-addition republish to exactly that observation and to every
        # requested source's authorized caught-up verdict; a first install has
        # no receipt and stays strict. Nothing is observed for staging.
        replay_predecessor = None
        if existing is not None and not no_publish:
            with _stage("replay_predecessor_observation"):
                from okg.substrate.cli.provision import _published_generation_row_counts
                observed_id, observed_nodes, observed_edges = _published_generation_row_counts(
                    dsn, deployment_name=resolved.name)
                if (observed_id is not None
                        and type(observed_nodes) is int and observed_nodes > 0
                        and type(observed_edges) is int
                        and (observed_edges > 0 or records_only)):
                    replay_predecessor = {"generation_id": str(observed_id),
                                          "node_count": observed_nodes, "edge_count": observed_edges}
        with _stage("provision"):
            provision = _capture(_cmd_provision, argparse.Namespace(deployment=resolved.name, dsn=dsn,
                                 include=list(source_modes) if source_modes else None, readiness_sources=False,
                                 _packaged_stage_only=no_publish, _packaged_source_modes=source_modes or {},
                                 _packaged_replay_predecessor=replay_predecessor,
                                 _packaged_records_only=records_only))
        if no_publish:
            from .install_stage import capture_stage
            staged = provision.get("payload", provision)
            if staged.get("published") is not False or staged.get("state") != "staged":
                raise SubstrateError("installer staging result invalid")
            stage = capture_stage(dsn, deployment=resolved.name,
                                  admission_ids=tuple(staged["admission_ids"]))
            receipt = {"schema": "okg.install-receipt/v1", "pseudonym_key": key_binding, "release": identity.to_dict(),
                       "database": database, "state": "staged", "published": False,
                       "stage": stage.to_dict(), "provision": provision}
            _write_install_receipt(marker, receipt)
            return receipt
        proof = provision.get("payload", provision).get("publish_proof")
        if not isinstance(proof, dict) or proof.get("ok") is not True:
            _refuse("required_publication_proof_failed")
        with _stage("reader_verification"):
            reader = _verify_installed_reader(dsn=dsn, deployment=resolved.name,
                generation=provision.get("payload", provision)["generation_id"], query=verification_query,
                declares_relationships=declares_relationships)
        # provision already printed its own capacity-warning line; lift the
        # block (or the resumed stage's) to the top of the install receipt.
        capacity_warning = proof.get("capacity_warning") or (stage_resume or {}).get("capacity_warning")
        receipt = {"schema": "okg.install-receipt/v1", "pseudonym_key": key_binding, "release": identity.to_dict(),
                   **({"capacity_warning": capacity_warning} if capacity_warning else {}),
                   "database": database, "state": "published", "published": True,
                   "replay_predecessor": replay_predecessor,
                   "provision": provision, "reader": reader,
                   **({"stage_resume": stage_resume} if stage_resume is not None else {})}
        # Runtime measurements are ordinary JSON numbers, not package identity
        # fields (whose canonical format deliberately excludes floats).
        with _stage("receipt_write"):
            _write_install_receipt(marker, receipt)
        return receipt


def _execute_search(*, deployment: Path, dsn: str, query: str,
                    generation: str | None, manifest: Mapping) -> dict:
    """Use the existing reader under the verified installation's actual policy."""
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode, _raw_enforcement_mode_value,
        _normalize_enforcement_mode_value)
    from okg.substrate.cli._read_side import cmd_search
    resolved = resolve_deployment(str(deployment))
    nomos = manifest.get("nomos")
    configured_mode = (_normalize_enforcement_mode_value(_raw_enforcement_mode_value(nomos))
                       if isinstance(nomos, Mapping) else None)
    if (configured_mode != "enforce"
            or deployment_nomos_enforcement_mode(resolved.name) != "enforce"):
        _refuse("search_requires_enforced_policy")
    # _capture journals only search decision/count metadata, never query/results.
    result = _capture(cmd_search, argparse.Namespace(
        deployment=resolved.name, dsn=dsn, generation_id=generation, query=query,
        method="lexical", subtype=None, limit=20, offset=0,
        select=["node_id,attrs,provenance,source_record_id,source_revision"],
        nomos_access_context={},
    ))
    if result.get("nomos_effect") != "allow" or not result.get("nomos_decision_id"):
        _refuse("search_authorization_missing")
    return result


def _execute_update(*, deployment: Path, source: str, dsn: str, query: str,
                    source_modes: Mapping[str, str], identity: PackagedReleaseIdentity,
                    database: Mapping) -> dict:
    """Use ordinary scoped ingest only after the original release was verified."""
    from okg.substrate.cli.main import _cmd_ingest, _sync_source_run_mode
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.sources.registry import load_source_registry
    from okg.substrate.ingest.planning import plan_ingest_sources
    resolved = resolve_deployment(str(deployment))
    registry = load_source_registry(resolved.source_registry)
    selected = plan_ingest_sources(registry, include={source})
    modes = {source_modes.get(name, _sync_source_run_mode(
        selected.runnable[name].sync, requested_mode="reconcile")) for name in selected.ordered}
    if len(modes) != 1:
        _refuse("update_source_modes_incompatible")
    with _stage("source_update"):
        result = _capture(_cmd_ingest, argparse.Namespace(
            deployment=str(deployment), dsn=dsn, include=source, inline=True,
            no_publish=False, mode=modes.pop(),
        ))
        report = result.get("payload", result)
        publishes = report.get("publishes")
        if (not isinstance(publishes, list) or not publishes
                or any(not isinstance(item, dict) or item.get("status") != "published"
                       or not item.get("generation_id") for item in publishes)):
            _refuse("update_publication_missing")
        generation = str(publishes[-1]["generation_id"])
    with _stage("reader_verification"):
        reader = _verify_installed_reader(dsn=dsn, deployment=resolved.name,
            generation=generation, query=query)
    # Keep initial installation identity and receipt unchanged. Stage journals
    # retain failed updates too; only a successful new publish yields this result.
    return {"schema": "okg.source-update-receipt/v1", "release": identity.to_dict(),
            "database": dict(database), "source": source, "sources": list(selected.ordered),
            "published": True, "generation_id": generation, "ingest": result, "reader": reader}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    for name in ("framework-wheel", "framework-digest", "consumer-wheel", "consumer-digest"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--consumer-package", required=True)
    options, remaining = parser.parse_known_args(argv)
    try:
        if not sys.flags.isolated or not sys.dont_write_bytecode:
            _refuse("isolated_bytecode_disabled_launcher_required")
        root = Path(sysconfig.get_path("purelib"))
        if options.consumer_package == "okg":
            _refuse("consumer_must_be_distinct")
        configure_packaged_imports()
        bootstrap = BootstrapInputs(
            WheelInput("okg", Path(options.framework_wheel), options.framework_digest, root),
            WheelInput(options.consumer_package, Path(options.consumer_wheel), options.consumer_digest, root),
        )
        bootstrap.verify()
        token = _BOOTSTRAP.set(bootstrap)
        try:
            from okg.substrate.cli.main import main as cli_main
            return cli_main(["install", *remaining])
        finally:
            _BOOTSTRAP.reset(token)
    except (OSError, ValueError, SubstrateError):
        print(json.dumps({"ok": False, "reason": "packaged_install_refused"}), file=sys.stderr)
        return 1


def render_verified_profile(
    *, bootstrap: BootstrapInputs, bundle: str, name: str, root: Path,
    plan: Mapping, answers: dict, module_files: Mapping[str, bytes] | None = None,
) -> dict[str, bytes]:
    """Render the selected installed profile using only bound package members."""
    from okg.substrate.catalog.profile import load_profile
    from okg.substrate.deployment_bootstrap.profile_init import build_plan, planned_writes
    from okg.substrate import fastyaml
    import yaml
    bootstrap.verify()
    consumer = bootstrap.consumer
    payload = verify_prospective_package(consumer.package, wheel_path=consumer.wheel_path,
                                         expected_wheel_digest=consumer.digest,
                                         installed_root=consumer.installed_root)
    inventory = {item.path for item in payload.files}
    member = f"{consumer.package}/bundles/{bundle}/profile.yaml"
    selected = {item["member_path"] for item in plan["assets"]
                if item["object_digest"] == consumer.digest}
    if member not in inventory or member not in selected:
        _refuse("selected_bundle_profile_absent")
    profile = load_profile(consumer.installed_root / member)
    if profile.name != bundle:
        _refuse("bundle_profile_identity_mismatch")
    reads = []
    for attribute in ("modules_file", "invariants_file", "deployment_defaults_file"):
        path = getattr(profile, attribute)
        if path is not None:
            reads.append(path)
    for attribute in ("source_defaults_dir", "schemas_dir", "dashboards_dir", "skills_dir"):
        path = getattr(profile, attribute)
        if path is not None:
            if not path.is_relative_to(consumer.installed_root / consumer.package):
                _refuse("profile_dependency_outside_package")
            pattern = "*.yaml" if attribute == "source_defaults_dir" else "**/*"
            reads.extend(item for item in path.glob(pattern) if item.is_file())
    # `files:` entries are installed verbatim, so each must be a selected
    # member of the package like every other profile dependency.
    reads.extend(source for _rel, source in profile.extra_files)
    for path in reads:
        if not path.is_relative_to(consumer.installed_root):
            _refuse("profile_dependency_outside_package")
        relative = path.relative_to(consumer.installed_root).as_posix()
        if relative not in inventory or relative not in selected:
            _refuse("profile_dependency_not_selected")
    target = root / "configuration"
    scaffold = build_plan(profile=profile, deployment_name=name,
                          deployments_root=root, raw_answers=answers)
    # Preserve renderer behavior while selecting the installer's immutable tree.
    from dataclasses import replace
    scaffold = replace(scaffold, deployment_dir=target)
    from okg.substrate.catalog.module_providers import parse_module_ref
    if [":".join(parse_module_ref(item)) for item in scaffold.modules] != list(plan["catalog"]["module_refs"]):
        _refuse("profile_module_selection_drift")
    result = {}
    for item in planned_writes(scaffold):
        relative = item.path.relative_to(target).as_posix()
        result[relative] = item.data if item.data is not None else item.text.encode()
    manifest = fastyaml.safe_load(result["deployment.yaml"])
    # Installation identity has no clock value: repeats render identical bytes.
    manifest["profile"].pop("initialized_at", None)
    manifest["release"] = {"kind": "packaged", "channel": "dev"}
    manifest["postgres"] = {"dsn": "${OKG_INSTALL_DSN}"}
    if module_files:
        # The package's own ontology modules become part of the immutable
        # configuration, and the manifest names the owner directories that
        # supply them. Declared, never discovered: a directory that appeared
        # in the tree without a declaration supplies nothing.
        owners = sorted({member.split("/")[1] for member in module_files})
        manifest["module_providers"] = {
            owner: f"modules/{owner}" for owner in owners
        }
        _merge_module_files(result, module_files)
    result["deployment.yaml"] = yaml.safe_dump(manifest, sort_keys=False).encode()
    return result


def _merge_module_files(
    result: dict[str, bytes], module_files: Mapping[str, bytes],
) -> None:
    """Add the package's module files to the rendered configuration.

    A path the profile already rendered (for example a `files:` entry) is
    refused rather than silently replaced by the module bytes. The `files:`
    allow-list keeps profile files out of `modules/`, so this should never
    fire; it is asserted anyway.
    """
    overlap = sorted(set(result) & set(module_files))
    if overlap:
        _refuse("profile_file_overlaps_package_module", path=overlap[0])
    result.update(module_files)


def run_install(args: argparse.Namespace, *, as_json: bool) -> int:
    """Packaged mode of the existing CLI install handler."""
    from .install_plan import resolve_install_plan, split_plan_secrets
    from okg.substrate.cli.main import _emit_error, _emit_ok
    bootstrap = active_bootstrap()
    try:
        search_query = getattr(args, "search_query", None)
        search_generation = getattr(args, "search_generation", None)
        if search_query is not None or search_generation is not None:
            if (not isinstance(search_query, str) or not search_query.strip()
                    or getattr(args, "update_source", None) is not None
                    or any(getattr(args, flag, False) for flag in ("dry_run", "scaffold_only", "no_publish"))
                    or (search_generation is not None and not search_generation.strip())):
                _refuse("search_mode_unsupported")
        if getattr(args, "update_source", None) is not None and any(
            getattr(args, flag, False) for flag in ("dry_run", "scaffold_only", "no_publish")
        ):
            _refuse("update_mode_unsupported")
        if bootstrap is None:
            _refuse("pinned_launcher_required")
        if not all((args.deployment_name, args.target)):
            _refuse("package_bundle_name_and_target_required")
        reference = args.install_dsn_ref
        if not reference.startswith("env:") or not reference[4:].isidentifier():
            _refuse("database_secret_reference_required")
        dsn = os.environ.get(reference[4:])
        if not dsn:
            _refuse("database_secret_reference_unresolved")
        query = search_query if search_query is not None else getattr(args, "verify_query", None)
        if not query and sys.stdin.isatty():
            query = input("Search phrase to verify in the installed graph: ")
        if not isinstance(query, str) or not query.strip():
            raise SubstrateError("choose a verification search phrase", fix_hint="pass --verify-query with text expected in the selected source data")
        if getattr(args, "personal_private", False) and not getattr(args, "source_policy", None):
            # The flag only tightens a supplied policy; alone it would promise
            # redaction that nothing applies.
            _refuse("personal_private_requires_source_policy")
        answers = {"deployment_name": args.deployment_name, "postgres_dsn": "${OKG_INSTALL_DSN}"}
        if not args.distribution_package or not args.bundle:
            _refuse("package_bundle_name_and_target_required")
        from .package import verify_distribution_package
        from okg.substrate.deployment_product_lifecycle import package_selection_from_verified
        from okg.substrate.catalog.profile import load_profile, load_profile_modules
        from okg.substrate.catalog.module_providers import parse_module_ref
        package = verify_distribution_package(args.distribution_package)
        module_files: dict[str, bytes] = {}
        try:
            selection = package_selection_from_verified(package, bundle_id=args.bundle)
            selected = {item["member_path"] for item in selection.assets
                        if item["object_digest"] == bootstrap.consumer.digest}
            member = f"{bootstrap.consumer.package}/bundles/{args.bundle}/profile.yaml"
            if member not in selected:
                _refuse("profile_not_selected")
            profile = load_profile(bootstrap.consumer.installed_root / member)
            modules_file = profile.modules_file
            if (modules_file is None or not modules_file.is_relative_to(bootstrap.consumer.installed_root)
                    or modules_file.relative_to(bootstrap.consumer.installed_root).as_posix() not in selected):
                _refuse("profile_modules_not_selected")
            modules = [":".join(parse_module_ref(item)) for item in load_profile_modules(profile)]
            # The profile's own credential declarations enter the plan, so
            # an unset optional credential is recorded as deferred and an
            # unset required one refuses before anything is written.
            secrets = profile_credential_secrets(
                profile, assets=selection.assets,
                installed_root=bootstrap.consumer.installed_root,
                consumer_digest=bootstrap.consumer.digest,
            )
            # Read the declared modules while the package is still open; the
            # verified reader closes with it.
            module_files = _packaged_module_files(package, modules)
        finally:
            package.close_runtime_authority()
        instance = {"name": args.deployment_name, "modules": modules, "secrets": secrets}
        plan, lock = resolve_install_plan(args.distribution_package, args.bundle, instance)
        # Whether each optional credential is set in THIS environment. It is
        # reported with the result and kept out of the plan, the lock and
        # the release identity, so exporting it later does not read as
        # release drift on the next install, update or search run.
        _, deferred_credentials = split_plan_secrets(plan, instance)
        root = Path(args.target).absolute()
        files = render_verified_profile(bootstrap=bootstrap, bundle=args.bundle,
                                        name=args.deployment_name, root=root,
                                        plan=plan, answers=answers,
                                        module_files=module_files)
        if getattr(args, "source_policy", None):
            from .install_policy import apply_source_policy, read_source_policy
            files = apply_source_policy(files, read_source_policy(Path(args.source_policy)),
                                        require_redaction=bool(args.personal_private))
        previous = os.environ.get("OKG_INSTALL_DSN")
        os.environ["OKG_INSTALL_DSN"] = dsn
        try:
            receipt = execute_prepared_install(
                bootstrap=bootstrap, root=root, files=files, plan=plan, lock=lock,
                translation="installed-profile/v1", dsn=dsn,
                authorize_cluster_bootstrap=args.authorize_cluster_bootstrap,
                verification_query=query.strip(), no_publish=args.no_publish,
                update_source=getattr(args, "update_source", None),
                search_query=search_query, search_generation=search_generation,
            )
        finally:
            if previous is None:
                os.environ.pop("OKG_INSTALL_DSN", None)
            else:
                os.environ["OKG_INSTALL_DSN"] = previous
        if secrets:
            # JSON keeps each entry's fields; text prints one readable line
            # per deferred credential instead of a raw mapping.
            receipt = {**receipt, "deferred_credentials": deferred_credentials if as_json else [
                f"{item['name']} (source {item['source_id']}, declaration "
                f"{item['declaration_ref']}): {item['supply_later']}; check with "
                f"{item['check_with']}"
                for item in deferred_credentials
            ]}
        _emit_ok(receipt, as_json=as_json)
        return 0
    except (OSError, ValueError, SubstrateError) as exc:
        if isinstance(exc, SubstrateError):
            return _emit_error(exc, as_json=as_json)
        return _emit_error(SubstrateError("packaged installation inputs refused"), as_json=as_json)
