"""File IO for PACT v0 manifests."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
from pathlib import Path
from typing import Any

import yaml
from okg.substrate import fastyaml
from pydantic import ValidationError

from okg.substrate.errors import SubstrateError
from okg.substrate.pact.models import PactManifest, dump_manifest


def pact_root(repo_root: Path) -> Path:
    return repo_root / "pact"


# `new-change` is a RETIRED alias. The audited fix_hint told an agent whose
# real problem was cwd drift to scaffold a DUPLICATE of a change that already
# exists one worktree over — with a verb the surface cut removed. A hint is
# state-checked here before it is offered
# (pact-self-repairing-refusals req.refusal.complete).
RETIRED_VERBS: frozenset[str] = frozenset({"new-change"})


def _sibling_worktrees(repo_root: Path) -> list[Path]:
    """Other checkouts of this repo that might hold the change.

    Worktrees live beside each other under `.claude/worktrees/`, and every
    `okg pact` verb resolves `--root` from the CWD — so "change not found" is
    far more often "you are in the wrong tree" than "it does not exist".
    """
    candidates: list[Path] = []
    try:
        root = Path(repo_root).resolve()
    except OSError:
        return candidates
    seen: set[Path] = set()
    for parent in (root, *root.parents):
        worktrees = parent / ".claude" / "worktrees"
        if not worktrees.is_dir():
            continue
        for entry in sorted(worktrees.iterdir()):
            if entry.is_dir() and entry != root and entry not in seen:
                seen.add(entry)
                candidates.append(entry)
        break
    return candidates


def change_not_found(repo_root: Path, change_id: str) -> SubstrateError:
    """The one `change not found` refusal — state-checked before it is raised.

    Names near-miss ids from the corpus that IS on disk, points at a sibling
    worktree when the change lives there, and never names a retired verb.
    """
    from okg.substrate.pact import repair

    path = manifest_path(repo_root, change_id)
    known = sorted(
        entry.name
        for entry in (pact_root(repo_root) / "changes").glob("*")
        if (entry / "pact.yaml").is_file()
    )
    elsewhere = [
        str(tree)
        for tree in _sibling_worktrees(repo_root)
        if (tree / "pact" / "changes" / change_id / "pact.yaml").is_file()
    ]
    archived = (pact_root(repo_root) / "archive" / change_id / "pact.yaml").is_file()
    candidates, candidate_source = repair.near_miss_candidates(change_id, known)
    if elsewhere:
        hint = (
            f"{change_id} exists in another worktree ({elsewhere[0]}) — this "
            f"root is {repo_root}; rerun from there or pass "
            f"--root {elsewhere[0]}"
        )
    elif archived:
        hint = (
            f"{change_id} is archived; reopen it with "
            f"`okg pact decide {change_id} --verdict reopen`"
        )
    elif candidates:
        hint = f"did you mean: {', '.join(candidates)}?"
    else:
        hint = (
            f"no change named {change_id} in {pact_root(repo_root) / 'changes'}; "
            "author one with `okg pact new <id> --title \"...\"`"
        )
    assert not (RETIRED_VERBS & set(hint.split())), "hint names a retired verb"
    return SubstrateError(
        f"PACT change not found: {change_id}",
        data={
            "path": str(path),
            "change": change_id,
            "candidates": candidates,
            # A difflib hit is a correction; a truncated listing is a menu.
            # Only the former may be re-invoked with (repair.py L5).
            "candidate_source": candidate_source,
            "found_in_worktrees": elsewhere,
            "archived": archived,
        },
        fix_hint=hint,
    )


def change_dir(repo_root: Path, change_id: str) -> Path:
    return pact_root(repo_root) / "changes" / change_id


def manifest_path(repo_root: Path, change_id: str) -> Path:
    return change_dir(repo_root, change_id) / "pact.yaml"


@contextmanager
def change_creation_lock(repo_root: Path, change_id: str):
    """Serialize creation before a change directory exists.

    The lock lives outside ``pact/changes/<id>`` so a fully staged local
    template can be published with one atomic directory rename.  Hashing the
    id keeps the lock path confined even when a caller supplies a malformed id;
    manifest validation remains responsible for rejecting that id itself.
    """
    lock_root = pact_root(repo_root) / ".locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(change_id.encode("utf-8")).hexdigest()
    lock_path = lock_root / f"create-{digest}.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        except OSError as exc:
            raise SubstrateError(
                "failed to acquire PACT creation lock",
                data={"change": change_id, "lock_path": str(lock_path)},
            ) from exc
        try:
            yield lock_path
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@contextmanager
def manifest_write_lock(repo_root: Path, change_id: str, *, must_exist: bool = True):
    """Hold the single-writer lock for one PACT manifest mutation.

    By default the change must already have a manifest: if it doesn't,
    raise a clean not-found error BEFORE creating anything, so a bad
    change_id can't fabricate a phantom ``pact/changes/<id>/`` directory
    (and a stray ``pact.yaml.lock`` inside it). Operations that bring a
    change into being — ``create_change`` / ``migrate_openspec`` — pass
    ``must_exist=False``.

    The lock is advisory (``fcntl.flock``) and always released on context
    exit via the ``finally`` below; the 0-byte ``.lock`` file is left in
    place as the reusable lock handle (unlinking it on release would race
    a concurrent acquirer). That file is gitignored.
    """
    path = manifest_path(repo_root, change_id)
    if must_exist and not path.is_file():
        # Mirror load_manifest's error so callers / endpoints see one
        # consistent "not found" shape — and crucially, create nothing.
        raise change_not_found(repo_root, change_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f"{path.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        except OSError as exc:
            raise SubstrateError(
                "failed to acquire PACT manifest lock",
                data={"path": str(path), "lock_path": str(lock_path)},
            ) from exc
        try:
            yield lock_path
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@contextmanager
def manifest_file_write_lock(path: Path):
    """Lock an existing active or archived manifest at its exact path.

    Lifecycle decisions can refresh an archived artifact, where the normal
    active-change lock cannot be resolved.  Keeping the lock beside the
    manifest also means the same open lock inode moves with an
    active-to-archive directory rename.
    """
    if not path.is_file():
        raise SubstrateError(
            f"PACT manifest not found: {path}",
            data={"path": str(path)},
        )
    lock_path = path.with_name(f"{path.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        except OSError as exc:
            raise SubstrateError(
                "failed to acquire PACT manifest lock",
                data={"path": str(path), "lock_path": str(lock_path)},
            ) from exc
        try:
            yield lock_path
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def file_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def state_digest(manifest: PactManifest) -> str:
    data = dump_manifest(manifest)
    data["edits"] = []
    # dump_manifest suppresses the empty prose key (polish-pact-agent-ux
    # req.polish.doc-holes); pin it back for digest stability so the shape
    # change does not invalidate every outstanding digest match.
    data.setdefault("prose", {})
    raw = yaml.safe_dump(data, sort_keys=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def intent_digest(manifest: PactManifest) -> str:
    data = dump_manifest(manifest)
    if isinstance(data.get("change"), dict):
        data["change"]["status"] = "__lifecycle_status_ignored__"
    for task in data.get("tasks", []):
        if isinstance(task, dict):
            task["status"] = "__task_status_ignored__"
            task["evidence"] = []
            task["blocked_reason"] = None
    data["evidence"] = []
    # Code-proof receipts are closure state, not reviewed intent. Including
    # them here makes attaching the receipt recursively invalidate the intent
    # digest it is required to bind.
    data.pop("proof_receipts", None)
    data["edits"] = []
    # Same digest-stability pin as state_digest: outstanding ledger
    # approvals match on this digest and must survive the prose suppression.
    data.setdefault("prose", {})
    raw = yaml.safe_dump(data, sort_keys=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def load_manifest(repo_root: Path, change_id: str) -> PactManifest:
    path = manifest_path(repo_root, change_id)
    if not path.is_file():
        raise change_not_found(repo_root, change_id)
    return load_manifest_file(path)


def load_manifest_file(path: Path) -> PactManifest:
    return load_manifest_bytes(
        path.read_bytes(),
        source_path=path,
    )


def load_manifest_bytes(
    payload: bytes,
    *,
    source_path: Path,
) -> PactManifest:
    """Parse one already-captured manifest without reopening live state."""
    try:
        raw = fastyaml.safe_load(payload.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise SubstrateError(
            f"invalid YAML in {source_path}",
            data={"path": str(source_path), "yaml_error": str(exc)},
        ) from exc
    if not isinstance(raw, dict):
        raise SubstrateError(
            f"PACT manifest must be a mapping: {source_path}",
            data={"path": str(source_path)},
        )
    if raw.get("pact_version") == 5:
        # PACT v5 manifests are authored for the self-contained `pact`
        # package; v4 readers see them through a read-only adapted view.
        # Archive membership lives in the path, not the manifest body, so the
        # reader supplies it: without it a merged v5 change reads back as a
        # fresh proposal.
        from okg.substrate.pact.v5_adapter import is_archived_path, to_v4

        raw = to_v4(raw, archived=is_archived_path(source_path))
    try:
        return PactManifest.model_validate(raw)
    except ValidationError as exc:
        # The refusal carries the offending value AND the legal vocabulary,
        # read off the model itself. Raw pydantic named the path and the rule
        # ("literal_error on dependencies[14].kind") and nothing an author
        # could choose from, so every audited session fell back to grepping
        # the schema or copying a sibling PACT
        # (pact-self-repairing-refusals req.refusal.complete).
        from okg.substrate.pact import schema_errors

        explained = schema_errors.explain(exc.errors())
        raise SubstrateError(
            f"PACT manifest failed schema validation: {source_path} — "
            + schema_errors.summary(explained),
            data={
                "path": str(source_path),
                "errors": exc.errors(),
                "schema_errors": explained,
            },
        ) from exc


def save_manifest(repo_root: Path, manifest: PactManifest) -> Path:
    path = manifest_path(repo_root, manifest.change.id)
    return save_manifest_file(path, manifest)


def save_manifest_file(path: Path, manifest: PactManifest) -> Path:
    """Atomically save a manifest at an exact active or archive path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = render_manifest(manifest)
    tmp = path.with_suffix(".yaml.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
    return path


def render_manifest(manifest: PactManifest) -> str:
    return yaml.safe_dump(
        dump_manifest(manifest),
        sort_keys=False,
        allow_unicode=False,
    )


def load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        raw = fastyaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise SubstrateError(
            f"invalid YAML in {path}",
            data={"path": str(path), "yaml_error": str(exc)},
        ) from exc
    if not isinstance(raw, dict):
        raise SubstrateError(
            f"YAML document must be a mapping: {path}",
            data={"path": str(path)},
        )
    return raw
