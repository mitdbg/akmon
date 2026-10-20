"""Non-recursive identity for code proof and metadata-only PACT closure.

The tracked receipt names the code-bearing ancestor and its exact tree
entries.  It deliberately does not name the later closure head: CI supplies
that external identity and validates that the intervening diff contains only
the receipt's exact metadata paths.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
import subprocess
from typing import Any, Iterable

from okg.substrate.pact.models import (
    PactCodeProofReceipt,
    PactCodeTuple,
    PactManifest,
)
from okg.substrate.pact.home_repository import home_repository
from okg.substrate.pact.store import intent_digest, load_manifest_bytes


CODE_PROOF_RECEIPT_SCHEMA = "okg.pact_code_proof_receipt/v1"
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_OBJECT_RE = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_GIT_MODE_RE = re.compile(r"^[0-7]{6}$")
_GIT_TIMEOUT_SECONDS = 20
_EXECUTABLE_CLOSURE_SUFFIXES = {
    ".bash",
    ".c",
    ".cc",
    ".cpp",
    ".go",
    ".java",
    ".js",
    ".jsx",
    ".mjs",
    ".py",
    ".pyi",
    ".rs",
    ".sh",
    ".ts",
    ".tsx",
}


class ProofIdentityError(ValueError):
    """A closed, machine-readable reason a code proof cannot be reused."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data or {}

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, **self.data}


@dataclass(frozen=True)
class CodeProofValidationResult:
    applies: bool
    repository: str
    closure_sha: str | None
    closure_tree: str | None
    proven_code_heads: tuple[str, ...] = field(default_factory=tuple)
    receipt_digests: tuple[str, ...] = field(default_factory=tuple)
    local_closure_paths: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "applies": self.applies,
            "repository": self.repository,
            "closure_sha": self.closure_sha,
            "closure_tree": self.closure_tree,
            "proven_code_heads": list(self.proven_code_heads),
            "receipt_digests": list(self.receipt_digests),
            "local_closure_paths": list(self.local_closure_paths),
        }


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value)).hexdigest()


def code_tuple_payload(row: PactCodeTuple) -> dict[str, str]:
    return row.model_dump(mode="json")


def receipt_payload(
    receipt: PactCodeProofReceipt,
    *,
    include_receipt_digest: bool = True,
) -> dict[str, Any]:
    payload = receipt.model_dump(mode="json", by_alias=True)
    if not include_receipt_digest:
        payload.pop("receipt_digest", None)
    return payload


def code_proof_receipt_digests(manifest: PactManifest) -> list[str]:
    return sorted(receipt.receipt_digest for receipt in manifest.proof_receipts.code)


def static_code_proof_errors(manifest: PactManifest) -> list[ProofIdentityError]:
    """Validate receipt bytes that do not require a Git checkout."""
    errors: list[ProofIdentityError] = []
    receipts = manifest.proof_receipts.code
    digests = [receipt.receipt_digest for receipt in receipts]
    if digests != sorted(digests) or len(digests) != len(set(digests)):
        errors.append(
            ProofIdentityError(
                "code_proof_receipts_not_sorted",
                "proof_receipts.code must be uniquely sorted by receipt_digest",
                data={"receipt_digests": digests},
            )
        )
    for index, receipt in enumerate(receipts):
        prefix = f"proof_receipts.code[{index}]"
        errors.extend(_static_receipt_errors(manifest, receipt, prefix=prefix))
    return errors


def _static_receipt_errors(
    manifest: PactManifest,
    receipt: PactCodeProofReceipt,
    *,
    prefix: str,
) -> list[ProofIdentityError]:
    errors: list[ProofIdentityError] = []
    for field_name in ("base_sha", "base_tree", "proven_code_sha", "proven_code_tree"):
        value = str(getattr(receipt, field_name))
        if not _GIT_OBJECT_RE.fullmatch(value):
            errors.append(
                ProofIdentityError(
                    "invalid_code_proof_git_object",
                    f"{prefix}.{field_name} must be a lowercase full Git object id",
                    data={"path": f"{prefix}.{field_name}", "value": value},
                )
            )
    if not _DIGEST_RE.fullmatch(receipt.pact_intent_digest):
        errors.append(
            ProofIdentityError(
                "invalid_code_proof_intent_digest",
                f"{prefix}.pact_intent_digest must be sha256:<64 lowercase hex>",
                data={"path": f"{prefix}.pact_intent_digest"},
            )
        )
    allowlist = receipt.closure_allowlist
    if allowlist != sorted(allowlist) or len(allowlist) != len(set(allowlist)):
        errors.append(
            ProofIdentityError(
                "invalid_code_proof_closure_allowlist_order",
                f"{prefix}.closure_allowlist must be a unique sorted exact path list",
                data={"path": f"{prefix}.closure_allowlist"},
            )
        )
    verification_paths = _verification_paths(manifest)
    for path in allowlist:
        path_error = _path_error(path)
        if path_error is not None:
            errors.append(
                ProofIdentityError(
                    "invalid_code_proof_path",
                    f"{prefix}.closure_allowlist contains {path_error}",
                    data={"path": path},
                )
            )
        elif path in verification_paths:
            errors.append(
                ProofIdentityError(
                    "code_proof_closure_path_is_verification",
                    f"{path} contains verification bytes and cannot be closure metadata",
                    data={"path": path},
                )
            )
        elif not _allowed_closure_path(manifest.change.id, path):
            errors.append(
                ProofIdentityError(
                    "code_proof_closure_path_not_metadata",
                    f"{path} is not admissible closure metadata for {manifest.change.id}",
                    data={"path": path},
                )
            )
    tuple_payloads = [code_tuple_payload(row) for row in receipt.code_tuples]
    canonical_rows = [canonical_json(row) for row in tuple_payloads]
    if canonical_rows != sorted(canonical_rows):
        errors.append(
            ProofIdentityError(
                "code_proof_tuples_not_sorted",
                f"{prefix}.code_tuples must sort by canonical JSON",
                data={"path": f"{prefix}.code_tuples"},
            )
        )
    tuple_paths = [row.path for row in receipt.code_tuples]
    if len(tuple_paths) != len(set(tuple_paths)):
        errors.append(
            ProofIdentityError(
                "duplicate_code_proof_tuple_path",
                f"{prefix}.code_tuples contains a duplicate path",
                data={"paths": tuple_paths},
            )
        )
    for row in receipt.code_tuples:
        path_error = _path_error(row.path)
        if path_error is not None:
            errors.append(
                ProofIdentityError(
                    "invalid_code_proof_path",
                    f"{prefix}.code_tuples contains {path_error}",
                    data={"path": row.path},
                )
            )
        if not _GIT_MODE_RE.fullmatch(row.mode):
            errors.append(
                ProofIdentityError(
                    "invalid_code_proof_mode",
                    f"{prefix}.code_tuples mode must be an exact six-digit Git mode",
                    data={"path": row.path, "mode": row.mode},
                )
            )
        if row.object_id != "deletion" and not _GIT_OBJECT_RE.fullmatch(row.object_id):
            errors.append(
                ProofIdentityError(
                    "invalid_code_proof_object_id",
                    f"{prefix}.code_tuples object_id must be a Git object or deletion",
                    data={"path": row.path, "object_id": row.object_id},
                )
            )
    expected_code_digest = canonical_digest(tuple_payloads)
    if receipt.code_digest != expected_code_digest:
        errors.append(
            ProofIdentityError(
                "code_proof_code_digest_mismatch",
                f"{prefix}.code_digest does not match code_tuples",
                data={"expected": expected_code_digest, "actual": receipt.code_digest},
            )
        )
    expected_receipt_digest = canonical_digest(
        receipt_payload(receipt, include_receipt_digest=False)
    )
    if receipt.receipt_digest != expected_receipt_digest:
        errors.append(
            ProofIdentityError(
                "code_proof_receipt_digest_mismatch",
                f"{prefix}.receipt_digest does not match the receipt payload",
                data={
                    "expected": expected_receipt_digest,
                    "actual": receipt.receipt_digest,
                },
            )
        )
    return errors


def validate_code_proof_receipts(
    repo_root: Path,
    manifest: PactManifest,
    closure_ref: str,
) -> CodeProofValidationResult:
    """Validate tracked code receipts against an externally supplied head.

    ``closure_ref`` is intentionally not a tracked receipt field.  CI passes
    its trusted checkout or GitHub head here, breaking the self-referential
    cycle where recording the closure SHA creates a new SHA.
    """
    receipts = manifest.proof_receipts.code
    if not receipts:
        return CodeProofValidationResult(
            applies=False,
            repository=home_repository(),
            closure_sha=None,
            closure_tree=None,
        )
    static_errors = static_code_proof_errors(manifest)
    if static_errors:
        raise static_errors[0]

    closure_sha = _resolve_commit(repo_root, closure_ref, field="closure_ref")
    closure_tree = _tree_for_commit(repo_root, closure_sha)
    closure_manifest = _manifest_at_ref(repo_root, closure_sha, manifest.change.id)
    closure_intent = intent_digest(closure_manifest)
    current_intent = intent_digest(manifest)
    if closure_intent != current_intent:
        raise ProofIdentityError(
            "code_proof_closure_manifest_mismatch",
            "the closure head manifest does not match the manifest being gated",
            data={"closure_intent_digest": closure_intent, "intent_digest": current_intent},
        )
    closure_receipts = code_proof_receipt_digests(closure_manifest)
    current_receipts = code_proof_receipt_digests(manifest)
    if closure_receipts != current_receipts:
        raise ProofIdentityError(
            "code_proof_closure_receipt_mismatch",
            "the closure head does not contain the exact checked code-proof receipts",
            data={"closure": closure_receipts, "manifest": current_receipts},
        )

    proven_heads: list[str] = []
    for receipt in receipts:
        _validate_receipt_git(
            repo_root,
            manifest,
            receipt,
            closure_sha=closure_sha,
            closure_intent=closure_intent,
        )
        proven_heads.append(receipt.proven_code_sha)

    return CodeProofValidationResult(
        applies=True,
        repository=home_repository(),
        closure_sha=closure_sha,
        closure_tree=closure_tree,
        proven_code_heads=tuple(sorted(set(proven_heads))),
        receipt_digests=tuple(current_receipts),
    )


def validate_local_code_proof_receipts(
    repo_root: Path,
    manifest: PactManifest,
    closure_ref: str = "HEAD",
) -> CodeProofValidationResult:
    """Validate a receipt plus uncommitted local closure metadata.

    This is the task/evidence freshness boundary. It deliberately requires no
    hosted check authority; preflight supplies that separate external proof.
    """
    result = validate_code_proof_receipts(repo_root, manifest, closure_ref)
    if not result.applies:
        return result
    local_paths = _working_tree_paths(repo_root, closure_ref)
    for receipt in manifest.proof_receipts.code:
        unexpected = sorted(set(local_paths) - set(receipt.closure_allowlist))
        if unexpected:
            raise ProofIdentityError(
                "code_proof_local_closure_scope_changed",
                "the working tree changes non-metadata paths after the closure head",
                data={"unexpected_paths": unexpected},
            )
    for path in local_paths:
        local_path = repo_root / path
        try:
            mode = local_path.lstat().st_mode
        except FileNotFoundError:
            # An allowlisted metadata deletion remains an exact closure path.
            continue
        if not stat.S_ISREG(mode) or mode & 0o111:
            raise ProofIdentityError(
                "code_proof_local_closure_metadata_mode_changed",
                "local closure metadata must remain a regular non-executable file",
                data={"path": path, "mode": oct(mode & 0o777777)},
            )
    return CodeProofValidationResult(
        applies=result.applies,
        repository=result.repository,
        closure_sha=result.closure_sha,
        closure_tree=result.closure_tree,
        proven_code_heads=result.proven_code_heads,
        receipt_digests=result.receipt_digests,
        local_closure_paths=tuple(local_paths),
    )


def build_code_proof_receipt(
    repo_root: Path,
    manifest: PactManifest,
    *,
    base_ref: str,
    proven_code_ref: str,
    closure_allowlist: Iterable[str],
    repository: str | None = None,
) -> PactCodeProofReceipt:
    """Build a receipt for explicit authoring; this function performs no write.

    ``repository`` defaults to the package's home repository and must equal it.
    """
    expected_repository = home_repository()
    if repository is None:
        repository = expected_repository
    if repository != expected_repository:
        raise ProofIdentityError(
            "code_proof_repository_mismatch",
            f"code proof repository must be {expected_repository}",
            data={"repository": repository},
        )
    base_sha = _resolve_commit(repo_root, base_ref, field="base_ref")
    proven_sha = _resolve_commit(repo_root, proven_code_ref, field="proven_code_ref")
    _require_ancestor(repo_root, base_sha, proven_sha, relation="base_to_proven")
    allowlist = sorted(closure_allowlist)
    changed = _changed_paths(repo_root, base_sha, proven_sha)
    _require_changed_scope(manifest, changed)
    tuples = _code_tuples(repo_root, base_sha, proven_sha, changed, set(allowlist))
    tuple_payloads = [code_tuple_payload(row) for row in tuples]
    payload: dict[str, Any] = {
        "schema": CODE_PROOF_RECEIPT_SCHEMA,
        "repository": repository,
        "base_sha": base_sha,
        "base_tree": _tree_for_commit(repo_root, base_sha),
        "proven_code_sha": proven_sha,
        "proven_code_tree": _tree_for_commit(repo_root, proven_sha),
        "pact_intent_digest": intent_digest(manifest),
        "closure_allowlist": allowlist,
        "code_tuples": tuple_payloads,
        "code_digest": canonical_digest(tuple_payloads),
    }
    payload["receipt_digest"] = canonical_digest(payload)
    receipt = PactCodeProofReceipt.model_validate(payload)
    errors = _static_receipt_errors(manifest, receipt, prefix="proof_receipts.code[0]")
    if errors:
        raise errors[0]
    return receipt


def _validate_receipt_git(
    repo_root: Path,
    manifest: PactManifest,
    receipt: PactCodeProofReceipt,
    *,
    closure_sha: str,
    closure_intent: str,
) -> None:
    base_sha = _resolve_commit(repo_root, receipt.base_sha, field="base_sha")
    proven_sha = _resolve_commit(
        repo_root, receipt.proven_code_sha, field="proven_code_sha"
    )
    if base_sha != receipt.base_sha or proven_sha != receipt.proven_code_sha:
        raise ProofIdentityError(
            "code_proof_abbreviated_or_indirect_ref",
            "receipt commit identities must be exact resolved commit ids",
        )
    base_tree = _tree_for_commit(repo_root, base_sha)
    proven_tree = _tree_for_commit(repo_root, proven_sha)
    if base_tree != receipt.base_tree:
        raise ProofIdentityError(
            "code_proof_base_tree_mismatch",
            "base_tree does not match base_sha",
            data={"expected": base_tree, "actual": receipt.base_tree},
        )
    if proven_tree != receipt.proven_code_tree:
        raise ProofIdentityError(
            "code_proof_proven_tree_mismatch",
            "proven_code_tree does not match proven_code_sha",
            data={"expected": proven_tree, "actual": receipt.proven_code_tree},
        )
    _require_ancestor(repo_root, base_sha, proven_sha, relation="base_to_proven")
    _require_ancestor(repo_root, proven_sha, closure_sha, relation="proven_to_closure")

    proven_manifest = _manifest_at_ref(repo_root, proven_sha, manifest.change.id)
    proven_intent = intent_digest(proven_manifest)
    if (
        receipt.pact_intent_digest != proven_intent
        or receipt.pact_intent_digest != closure_intent
        or receipt.pact_intent_digest != intent_digest(manifest)
    ):
        raise ProofIdentityError(
            "code_proof_intent_digest_mismatch",
            "proven and closure manifests must keep the receipt's intent digest",
            data={
                "receipt": receipt.pact_intent_digest,
                "proven": proven_intent,
                "closure": closure_intent,
                "manifest": intent_digest(manifest),
            },
        )

    base_to_proven = _changed_paths(repo_root, base_sha, proven_sha)
    _require_changed_scope(manifest, base_to_proven)
    expected_tuples = _code_tuples(
        repo_root,
        base_sha,
        proven_sha,
        base_to_proven,
        set(receipt.closure_allowlist),
    )
    actual_payload = [code_tuple_payload(row) for row in receipt.code_tuples]
    expected_payload = [code_tuple_payload(row) for row in expected_tuples]
    if actual_payload != expected_payload:
        raise ProofIdentityError(
            "code_proof_tuple_mismatch",
            "base-to-proven code tuples no longer match the receipt",
            data={"expected": expected_payload, "actual": actual_payload},
        )

    closure_changed = _changed_paths(repo_root, proven_sha, closure_sha)
    unexpected = sorted(set(closure_changed) - set(receipt.closure_allowlist))
    if unexpected:
        raise ProofIdentityError(
            "code_proof_closure_scope_changed",
            "the closure head changes non-metadata paths after the proven code head",
            data={"unexpected_paths": unexpected},
        )
    for path in closure_changed:
        entry = _tree_entry(repo_root, closure_sha, path) or _tree_entry(
            repo_root, proven_sha, path
        )
        if entry is None or entry[0] != "100644":
            raise ProofIdentityError(
                "code_proof_closure_metadata_mode_changed",
                "closure metadata must remain a regular non-executable file",
                data={"path": path, "mode": entry[0] if entry else None},
            )


def _require_changed_scope(manifest: PactManifest, changed_paths: list[str]) -> None:
    declared = set(manifest.change.affected_code)
    unexpected = sorted(set(changed_paths) - declared)
    if unexpected:
        raise ProofIdentityError(
            "code_proof_changed_path_outside_affected_code",
            "base-to-proven changed paths must be exact change.affected_code members",
            data={"unexpected_paths": unexpected},
        )


def _code_tuples(
    repo_root: Path,
    base_sha: str,
    proven_sha: str,
    changed_paths: list[str],
    closure_allowlist: set[str],
) -> list[PactCodeTuple]:
    rows: list[PactCodeTuple] = []
    for path in changed_paths:
        if path in closure_allowlist:
            continue
        entry = _tree_entry(repo_root, proven_sha, path)
        if entry is None:
            base_entry = _tree_entry(repo_root, base_sha, path)
            if base_entry is None:
                raise ProofIdentityError(
                    "code_proof_missing_tree_entry",
                    "a changed path is absent from both base and proven trees",
                    data={"path": path},
                )
            mode, _ = base_entry
            object_id = "deletion"
        else:
            mode, object_id = entry
        rows.append(PactCodeTuple(path=path, mode=mode, object_id=object_id))
    return sorted(rows, key=lambda row: canonical_json(code_tuple_payload(row)))


def _allowed_closure_path(change_id: str, path: str) -> bool:
    for tree in ("changes", "archive"):
        root = f"pact/{tree}/{change_id}"
        if path == f"{root}/pact.yaml":
            return True
        if (
            path.startswith(f"{root}/reports/")
            and path != f"{root}/reports/"
            and PurePosixPath(path).suffix.lower() not in _EXECUTABLE_CLOSURE_SUFFIXES
        ):
            return True
        if (
            path.startswith(f"{root}/evidence/")
            and path != f"{root}/evidence/"
            and PurePosixPath(path).suffix.lower() not in _EXECUTABLE_CLOSURE_SUFFIXES
        ):
            return True
    return False


def _verification_paths(manifest: PactManifest) -> set[str]:
    paths: set[str] = set()

    def add(value: str | None) -> None:
        if not value:
            return
        candidates = [value]
        try:
            candidates.extend(shlex.split(value))
        except ValueError:
            # Keep the unsplit value so malformed verification prose is still
            # checked as a path instead of aborting proof-scope validation.
            pass
        for candidate in candidates:
            normalized = candidate.strip("'\"(),").removeprefix("./")
            normalized = normalized.split("::", 1)[0]
            if _path_error(normalized) is None and "/" in normalized:
                paths.add(normalized)

    for task in manifest.tasks:
        add(task.verification)
    for entry in manifest.test_plan:
        add(entry.test_id)
        add(entry.command)
        add(entry.fixture)
    for binding in manifest.formal:
        add(binding.module)
        add(binding.config)
        add(binding.checker_command)
        for test in binding.refinement_tests:
            add(test)
    return paths


def _path_error(path: str) -> str | None:
    if not path or "\\" in path or "\x00" in path or "\n" in path or "\r" in path:
        return "an empty, escaped, or control-character path"
    candidate = PurePosixPath(path)
    if candidate.is_absolute() or candidate.as_posix() != path or ".." in candidate.parts:
        return "a non-canonical repository-relative path"
    return None


def _manifest_at_ref(repo_root: Path, ref: str, change_id: str) -> PactManifest:
    found: list[tuple[str, bytes]] = []
    for tree in ("changes", "archive"):
        path = f"pact/{tree}/{change_id}/pact.yaml"
        payload = _git_bytes(repo_root, "show", f"{ref}:{path}", allow_failure=True)
        if payload is not None:
            found.append((path, payload))
    if len(found) != 1:
        raise ProofIdentityError(
            "code_proof_manifest_at_ref_ambiguous",
            "a proven or closure head must contain exactly one active/archive manifest",
            data={"ref": ref, "paths": [path for path, _ in found]},
        )
    path, payload = found[0]
    try:
        return load_manifest_bytes(payload, source_path=Path(path))
    except Exception as exc:  # noqa: BLE001 - normalize as typed proof refusal.
        raise ProofIdentityError(
            "code_proof_manifest_at_ref_invalid",
            "a proven or closure manifest cannot be parsed",
            data={"ref": ref, "path": path, "reason": str(exc)},
        ) from exc


def _resolve_commit(repo_root: Path, ref: str, *, field: str) -> str:
    payload = _git_bytes(repo_root, "rev-parse", "--verify", f"{ref}^{{commit}}")
    value = payload.decode("ascii", errors="strict").strip()
    if not _GIT_OBJECT_RE.fullmatch(value):
        raise ProofIdentityError(
            "code_proof_ref_not_commit",
            f"{field} did not resolve to one full commit id",
            data={"field": field, "ref": ref, "resolved": value},
        )
    return value


def _tree_for_commit(repo_root: Path, commit: str) -> str:
    payload = _git_bytes(repo_root, "rev-parse", "--verify", f"{commit}^{{tree}}")
    value = payload.decode("ascii", errors="strict").strip()
    if not _GIT_OBJECT_RE.fullmatch(value):
        raise ProofIdentityError(
            "code_proof_tree_unresolved",
            "commit tree did not resolve to one full object id",
            data={"commit": commit, "tree": value},
        )
    return value


def _require_ancestor(repo_root: Path, ancestor: str, descendant: str, *, relation: str) -> None:
    proc = _run_git(repo_root, "merge-base", "--is-ancestor", ancestor, descendant)
    if proc.returncode == 0:
        return
    if proc.returncode == 1:
        raise ProofIdentityError(
            "code_proof_nonancestor",
            f"required {relation} ancestry does not hold",
            data={"ancestor": ancestor, "descendant": descendant, "relation": relation},
        )
    raise _git_error(proc, args=("merge-base", "--is-ancestor", ancestor, descendant))


def _changed_paths(repo_root: Path, left: str, right: str) -> list[str]:
    payload = _git_bytes(
        repo_root,
        "diff",
        "--name-only",
        "--no-renames",
        "-z",
        left,
        right,
        "--",
    )
    paths = [part.decode("utf-8", errors="strict") for part in payload.split(b"\x00") if part]
    for path in paths:
        path_error = _path_error(path)
        if path_error is not None:
            raise ProofIdentityError(
                "invalid_code_proof_path",
                f"Git returned {path_error}",
                data={"path": path},
            )
    return sorted(paths)


def _working_tree_paths(repo_root: Path, closure_ref: str) -> list[str]:
    tracked = _git_bytes(
        repo_root,
        "diff",
        "--name-only",
        "--no-renames",
        "-z",
        closure_ref,
        "--",
    )
    untracked = _git_bytes(
        repo_root,
        "ls-files",
        "--others",
        "--exclude-standard",
        "-z",
        "--",
    )
    paths = {
        part.decode("utf-8", errors="strict")
        for payload in (tracked, untracked)
        for part in payload.split(b"\x00")
        if part
    }
    for path in paths:
        path_error = _path_error(path)
        if path_error is not None:
            raise ProofIdentityError(
                "invalid_code_proof_path",
                f"Git returned {path_error}",
                data={"path": path},
            )
    return sorted(paths)


def _tree_entry(repo_root: Path, ref: str, path: str) -> tuple[str, str] | None:
    payload = _git_bytes(repo_root, "ls-tree", "-z", ref, "--", path)
    rows = [part for part in payload.split(b"\x00") if part]
    if not rows:
        return None
    if len(rows) != 1:
        raise ProofIdentityError(
            "code_proof_tree_entry_ambiguous",
            "exact path resolved to multiple Git tree entries",
            data={"ref": ref, "path": path},
        )
    metadata, returned_path = rows[0].split(b"\t", 1)
    mode, _object_type, object_id = metadata.decode("ascii").split(" ", 2)
    if returned_path.decode("utf-8", errors="strict") != path:
        raise ProofIdentityError(
            "code_proof_tree_path_mismatch",
            "Git tree entry did not return the requested exact path",
            data={"ref": ref, "path": path},
        )
    return mode, object_id


def _run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProofIdentityError(
            "code_proof_git_unavailable",
            "Git authority is unavailable for code-proof validation",
            data={"args": list(args), "reason": str(exc)},
        ) from exc


def _git_error(
    proc: subprocess.CompletedProcess[bytes],
    *,
    args: tuple[str, ...],
) -> ProofIdentityError:
    return ProofIdentityError(
        "code_proof_git_refused",
        "Git could not evaluate code-proof identity",
        data={
            "args": list(args),
            "returncode": proc.returncode,
            "stderr": proc.stderr.decode("utf-8", errors="replace").strip(),
        },
    )


def _git_bytes(
    repo_root: Path,
    *args: str,
    allow_failure: bool = False,
) -> bytes | None:
    proc = _run_git(repo_root, *args)
    if proc.returncode != 0:
        if allow_failure:
            return None
        raise _git_error(proc, args=args)
    return proc.stdout
