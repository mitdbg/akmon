"""Authenticated cross-host transport for sealed promotion candidates.

The staging host exports immutable release bytes plus generation-pinned
readiness, supervision, and query receipts.  Production authenticates the
envelope, admits the bytes into its own content-addressed artifact store, then
re-runs the ordinary sealed-release verifier.  The promotion controller still
enforces that the admitted release is physically parented by the production
artifact store; transport never relaxes that boundary.
"""
from __future__ import annotations

import fcntl
import hashlib
import hmac
import io
import json
import os
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.deployments.environment import shared_definition_substance_problems
from okg.substrate.sealed_release import SealedRelease, verify_sealed_release

TRANSFER_SCHEMA = "okg.sealed_promotion_transfer/v2"
READINESS_SCHEMA = "okg.environment_readiness/v1"
SUPERVISION_SCHEMA = "okg.environment_service_supervision_evidence/v1"
QUERY_SCHEMA = "okg.environment_query_evidence/v1"
MAX_ARCHIVE_MEMBERS = 20_000
MAX_ARCHIVE_EXPANDED_BYTES = 8 * 1024 * 1024 * 1024


def _error(message: str, **data: object) -> SubstrateError:
    return SubstrateError(message, data=data)


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _signature(payload: Mapping[str, Any], secret: bytes) -> str:
    if len(secret) < 32:
        raise _error("promotion transfer authentication secret is too short")
    return "hmac-sha256:" + hmac.new(secret, _canonical(payload), hashlib.sha256).hexdigest()


def export_staged_release(
    *,
    release: SealedRelease,
    staging_environment_id: str,
    definition_digest: str,
    candidate_revision: str,
    promotion_id: str,
    readiness: Mapping[str, Any],
    supervision: Mapping[str, Any],
    query_evidence: Mapping[str, Any],
    archive_path: Path,
    envelope_path: Path,
    secret: bytes,
    staging_posture: Mapping[str, Any],
    staging_definition_substance: Mapping[str, str],
    created_at: datetime | None = None,
) -> dict[str, Any]:
    verified = verify_sealed_release(
        release.artifact_root, expected_artifact_id=release.artifact_id
    )
    _validate_staging_posture(staging_posture)
    _validate_definition_substance(staging_definition_substance)
    if verified.inputs.code_revision != candidate_revision:
        raise _error("staged release candidate revision differs from export request")
    if verified.inputs.definition_digest != definition_digest:
        raise _error("staged release definition differs from export request")
    evidence = {
        "readiness.json": dict(readiness),
        "supervision.json": dict(supervision),
        "query.json": dict(query_evidence),
    }
    _validate_evidence(
        evidence,
        staging_environment_id=staging_environment_id,
        artifact_id=verified.artifact_id,
        candidate_revision=candidate_revision,
        now=created_at or datetime.now(timezone.utc),
    )
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = archive_path.with_name(f".{archive_path.name}.{os.getpid()}.tmp")
    with tarfile.open(temporary, "w:gz") as archive:
        archive.add(verified.artifact_root, arcname="artifact", recursive=True)
        for name, payload in evidence.items():
            encoded = _canonical(payload)
            info = tarfile.TarInfo(f"evidence/{name}")
            info.size = len(encoded)
            info.mode = 0o600
            info.mtime = 0
            archive.addfile(info, io.BytesIO(encoded))
    os.chmod(temporary, 0o600)
    os.replace(temporary, archive_path)
    now = (created_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    payload: dict[str, Any] = {
        "schema": TRANSFER_SCHEMA,
        "promotion_id": promotion_id,
        "staging_environment_id": staging_environment_id,
        "artifact_id": verified.artifact_id,
        "candidate_revision": candidate_revision,
        "definition_digest": definition_digest,
        "staging_posture": dict(staging_posture),
        "staging_definition_substance": dict(staging_definition_substance),
        "archive_digest": _digest_file(archive_path),
        "evidence_digests": {
            name: "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()
            for name, value in evidence.items()
        },
        "created_at": now.isoformat(),
    }
    envelope = {**payload, "authentication": _signature(payload, secret)}
    _atomic_json(envelope_path, envelope)
    return envelope


def import_staged_release(
    *,
    archive_path: Path,
    envelope_path: Path,
    production_artifact_store: Path,
    replay_ledger: Path,
    expected_promotion_id: str,
    expected_staging_environment_id: str,
    expected_candidate_revision: str,
    expected_definition_digest: str,
    secret: bytes,
    expected_staging_posture: Mapping[str, Any],
    expected_production_definition_substance: Mapping[str, str],
    now: datetime | None = None,
    max_age_seconds: int = 900,
) -> tuple[SealedRelease, dict[str, Any]]:
    raw = json.loads(envelope_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("schema") != TRANSFER_SCHEMA:
        raise _error("promotion transfer envelope has the wrong schema")
    authentication = raw.pop("authentication", None)
    if not isinstance(authentication, str) or not hmac.compare_digest(
        authentication, _signature(raw, secret)
    ):
        raise _error("promotion transfer authentication failed")
    expected = {
        "promotion_id": expected_promotion_id,
        "staging_environment_id": expected_staging_environment_id,
        "candidate_revision": expected_candidate_revision,
        "definition_digest": expected_definition_digest,
    }
    _validate_staging_posture(expected_staging_posture)
    _validate_definition_substance(expected_production_definition_substance)
    if raw.get("staging_posture") != dict(expected_staging_posture):
        raise _error(
            "promotion transfer staging posture differs from trusted request",
            expected=dict(expected_staging_posture),
            observed=raw.get("staging_posture"),
        )
    staging_substance = raw.get("staging_definition_substance")
    if not isinstance(staging_substance, Mapping):
        raise _error("promotion transfer staging definition substance is missing")
    _validate_definition_substance(staging_substance)
    substance_problems = shared_definition_substance_problems(
        {str(key): str(value) for key, value in staging_substance.items()},
        expected_production_definition_substance,
    )
    if substance_problems:
        raise _error(
            "staging and production shared foundation substance differs",
            problems=list(substance_problems),
        )
    mismatches = {
        key: {"expected": value, "observed": raw.get(key)}
        for key, value in expected.items()
        if raw.get(key) != value
    }
    if mismatches:
        raise _error("promotion transfer identity differs from trusted request", mismatches=mismatches)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    try:
        created = datetime.fromisoformat(str(raw["created_at"]))
    except (KeyError, ValueError) as exc:
        raise _error("promotion transfer timestamp is invalid") from exc
    if created.tzinfo is None or not 0 <= (current - created).total_seconds() <= max_age_seconds:
        raise _error("promotion transfer evidence is stale or from the future")
    if _digest_file(archive_path) != raw.get("archive_digest"):
        raise _error("promotion transfer archive digest changed")
    store = production_artifact_store.resolve()
    store.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".promotion-import-", dir=store) as tmp:
        root = Path(tmp)
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            if len(members) > MAX_ARCHIVE_MEMBERS:
                raise _error("promotion transfer archive has too many members")
            expanded = sum(member.size for member in members)
            if expanded > MAX_ARCHIVE_EXPANDED_BYTES:
                raise _error("promotion transfer archive exceeds expanded byte limit")
            if any(member.issym() or member.islnk() or member.isdev() for member in members):
                raise _error("promotion transfer archive contains unsupported links or devices")
            archive.extractall(root, members=members, filter="data")
        candidate = root / "artifact"
        release = verify_sealed_release(
            candidate, expected_artifact_id=str(raw.get("artifact_id") or "")
        )
        if release.inputs.code_revision != expected_candidate_revision:
            raise _error("imported release candidate revision changed")
        if release.inputs.definition_digest != expected_definition_digest:
            raise _error("imported release definition changed")
        evidence = {
            name: json.loads((root / "evidence" / name).read_text(encoding="utf-8"))
            for name in ("readiness.json", "supervision.json", "query.json")
        }
        for name, payload in evidence.items():
            observed = "sha256:" + hashlib.sha256(_canonical(payload)).hexdigest()
            if observed != (raw.get("evidence_digests") or {}).get(name):
                raise _error("imported staging evidence digest changed", evidence=name)
        _validate_evidence(
            evidence,
            staging_environment_id=expected_staging_environment_id,
            artifact_id=release.artifact_id,
            candidate_revision=expected_candidate_revision,
            now=current,
            max_age_seconds=max_age_seconds,
        )
        # Admit replay identity only after every authenticated byte and evidence
        # contract validates. A malformed but authenticated staging export must
        # not poison the promotion id and block a corrected export.
        _admit_replay_identity(
            replay_ledger,
            expected_promotion_id,
            authentication,
        )
        final = store / release.artifact_id.removeprefix("sha256:")
        if final.exists():
            admitted = verify_sealed_release(final, expected_artifact_id=release.artifact_id)
        else:
            os.replace(candidate, final)
            admitted = verify_sealed_release(final, expected_artifact_id=release.artifact_id)
    # The caller passes this release to create_promotion_approval, which repeats
    # the production artifact-store parent check rather than trusting transport.
    return admitted, {**raw, "authentication": authentication, "evidence": evidence}


def _validate_evidence(
    evidence: Mapping[str, Mapping[str, Any]],
    *,
    staging_environment_id: str,
    artifact_id: str,
    candidate_revision: str,
    now: datetime,
    max_age_seconds: int = 900,
) -> None:
    contracts = {
        "readiness.json": READINESS_SCHEMA,
        "supervision.json": SUPERVISION_SCHEMA,
        "query.json": QUERY_SCHEMA,
    }
    generation_ids: set[str] = set()
    for name, schema in contracts.items():
        row = evidence.get(name)
        if not isinstance(row, Mapping) or row.get("schema") != schema:
            raise _error("staging evidence has the wrong schema", evidence=name)
        if row.get("environment_id") != staging_environment_id:
            raise _error("staging evidence environment changed", evidence=name)
        if row.get("artifact_id") != artifact_id:
            raise _error("staging evidence artifact changed", evidence=name)
        if row.get("status") not in {"ready", "completed", "passed"}:
            raise _error("staging evidence is not successful", evidence=name)
        generation_id = row.get("generation_id")
        if not isinstance(generation_id, str) or not generation_id:
            raise _error("staging evidence has no generation", evidence=name)
        generation_ids.add(generation_id)
        try:
            captured = datetime.fromisoformat(str(row["captured_at"]))
        except (KeyError, ValueError) as exc:
            raise _error("staging evidence timestamp is invalid", evidence=name) from exc
        current = now.astimezone(timezone.utc)
        if captured.tzinfo is None or not 0 <= (current - captured).total_seconds() <= max_age_seconds:
            raise _error("staging evidence is stale or from the future", evidence=name)
    if evidence["query.json"].get("candidate_revision") != candidate_revision:
        raise _error("staging query did not prove the candidate revision")
    if len(generation_ids) != 1:
        raise _error("staging evidence generations are not aligned")


def _validate_staging_posture(posture: Mapping[str, Any]) -> None:
    expected = {
        "export_policy": "no_export",
        "live_call_default": "deny",
        "allowed_exportability": ["no_export"],
    }
    mismatches = {
        field: {"expected": wanted, "observed": posture.get(field)}
        for field, wanted in expected.items()
        if posture.get(field) != wanted
    }
    if mismatches:
        raise _error("staging deployment posture is not hardened", mismatches=mismatches)


def _validate_definition_substance(substance: Mapping[str, str]) -> None:
    if not isinstance(substance, Mapping) or not substance:
        raise _error("promotion transfer definition substance is missing")
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in substance.items()):
        raise _error("promotion transfer definition substance is invalid")


def _admit_replay_identity(path: Path, promotion_id: str, signature: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        rows: dict[str, str] = {}
        if path.is_file():
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise _error("promotion transfer replay ledger is invalid")
            rows = {str(key): str(value) for key, value in loaded.items()}
        existing = rows.get(promotion_id)
        if existing is not None and existing != signature:
            raise _error("promotion id was already used by a different transfer")
        # Exact replay is an idempotent transport reconnect. A different signed
        # identity for the same promotion id is rejected above.
        rows[promotion_id] = signature
        _atomic_json(path, rows)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(_canonical(payload) + b"\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="okg-ci-staged-release-transfer")
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export")
    export.add_argument("--artifact-root", type=Path, required=True)
    export.add_argument("--staging-environment", required=True)
    export.add_argument("--definition-digest", required=True)
    export.add_argument("--candidate-revision", required=True)
    export.add_argument("--promotion-id", required=True)
    export.add_argument("--readiness", type=Path, required=True)
    export.add_argument("--supervision", type=Path, required=True)
    export.add_argument("--query-evidence", type=Path, required=True)
    export.add_argument("--archive", type=Path, required=True)
    export.add_argument("--envelope", type=Path, required=True)
    export.add_argument("--staging-posture", type=Path, required=True)
    export.add_argument("--staging-definition-substance", type=Path, required=True)
    import_cmd = sub.add_parser("import")
    import_cmd.add_argument("--archive", type=Path, required=True)
    import_cmd.add_argument("--envelope", type=Path, required=True)
    import_cmd.add_argument("--production-artifact-store", type=Path, required=True)
    import_cmd.add_argument("--replay-ledger", type=Path, required=True)
    import_cmd.add_argument("--promotion-id", required=True)
    import_cmd.add_argument("--staging-environment", required=True)
    import_cmd.add_argument("--candidate-revision", required=True)
    import_cmd.add_argument("--definition-digest", required=True)
    import_cmd.add_argument("--staging-posture", type=Path, required=True)
    import_cmd.add_argument("--production-definition-substance", type=Path, required=True)
    import_cmd.add_argument("--max-age-seconds", type=int, default=900)
    parser.add_argument(
        "--secret-env", default="OKG_CI_PROMOTION_TRANSFER_SECRET"
    )
    args = parser.parse_args(argv)
    secret = os.environ.get(args.secret_env, "").encode()
    try:
        if args.command == "export":
            release = verify_sealed_release(args.artifact_root)
            envelope = export_staged_release(
                release=release,
                staging_environment_id=args.staging_environment,
                definition_digest=args.definition_digest,
                candidate_revision=args.candidate_revision,
                promotion_id=args.promotion_id,
                readiness=_read_json(args.readiness),
                supervision=_read_json(args.supervision),
                query_evidence=_read_json(args.query_evidence),
                archive_path=args.archive,
                envelope_path=args.envelope,
                secret=secret,
                staging_posture=_read_json(args.staging_posture),
                staging_definition_substance=_read_json(args.staging_definition_substance),
            )
            print(json.dumps(envelope, sort_keys=True))
        else:
            release, receipt = import_staged_release(
                archive_path=args.archive,
                envelope_path=args.envelope,
                production_artifact_store=args.production_artifact_store,
                replay_ledger=args.replay_ledger,
                expected_promotion_id=args.promotion_id,
                expected_staging_environment_id=args.staging_environment,
                expected_candidate_revision=args.candidate_revision,
                expected_definition_digest=args.definition_digest,
                secret=secret,
                expected_staging_posture=_read_json(args.staging_posture),
                expected_production_definition_substance=_read_json(
                    args.production_definition_substance
                ),
                max_age_seconds=args.max_age_seconds,
            )
            print(json.dumps({
                "ok": True,
                "artifact_id": release.artifact_id,
                "artifact_root": str(release.artifact_root),
                "transfer": receipt,
            }, sort_keys=True))
    except (OSError, ValueError, SubstrateError, tarfile.TarError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 1
    return 0


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise _error("staging evidence must be a JSON object", path=str(path))
    return value


if __name__ == "__main__":
    raise SystemExit(main())
