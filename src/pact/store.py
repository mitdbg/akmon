"""Find, load, and write manifests. Active changes live under
``pact/changes/<id>/pact.yaml``; merged ones under ``pact/archive/<id>/``."""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

import yaml
from pydantic import ValidationError

from pact.schema import Manifest


class PactError(Exception):
    def __init__(self, token: str, message: str, **data) -> None:
        super().__init__(message)
        self.token = token
        self.message = message
        self.data = data


def manifest_path(root: Path, change_id: str) -> Path:
    for branch in ("changes", "archive"):
        candidate = root / "pact" / branch / change_id / "pact.yaml"
        if candidate.is_file():
            return candidate
    raise PactError("unknown_change", f"no manifest for {change_id!r} under pact/changes or pact/archive")


def inside(root: Path, relative: str) -> Path | None:
    """The artifact path resolved inside ``root`` and outside ``.git``; None
    when it escapes (absolute, ``..``, ``~``, a symlink out, or the git
    directory). Every reader of a manifest-supplied path goes through this:
    a migration that hashed `/etc/hosts` would mint a passing receipt for a
    file the repository does not contain."""
    if not relative or relative.startswith(("/", "~")) or "\x00" in relative:
        return None
    if relative.split("/", 1)[0] == ".git":
        return None
    try:
        resolved = (root / relative).resolve()
        root_resolved = root.resolve()
    except OSError:
        return None
    if resolved != root_resolved and root_resolved not in resolved.parents:
        return None
    return resolved


GENERATED_ARTIFACT_HINT = (
    "an evidence artifact must be a file PACT generated for this change: "
    "under pact/changes/<id>/ (or pact/archive/<id>/), or under pact/evidence/. "
    "To prove something about a living file, write a note under "
    "pact/changes/<id>/evidence/ that cites the file by path and heading and "
    "quotes the lines it relies on, and attach the note."
)


def generated_artifact(root: Path, change_id: str, relative: str) -> bool:
    """Whether ``relative`` names a GENERATED artifact this change owns.

    A receipt pins its artifact's sha256, so binding a file the repository
    keeps editing -- a runbook, a docs page, a source or test module -- freezes
    that file: the next ordinary edit makes the digest stale and refuses an
    unrelated change forever. Proof therefore has to be an artifact the change
    produced, and a claim about a living file has to be written down as a note
    that cites it.

    Allowed: anything under the change's own PACT directory except the
    manifest itself (a manifest naming itself can never digest to a stable
    value), and the shared ``pact/evidence/`` tree. Refused: another change's
    PACT directory -- one change must not spend another's proof -- and every
    path outside ``pact/``.

    The path is RESOLVED first, through the same guard every other reader
    uses, so ``pact/changes/<id>/evidence/../../../../src/okg/foo.py`` cannot
    walk out of an allowed prefix by starting inside one.
    """
    resolved = inside(root, relative)
    if resolved is None:
        return False
    try:
        normalized = resolved.relative_to(root.resolve()).as_posix()
    except (OSError, ValueError):
        return False
    if normalized.startswith("pact/evidence/"):
        return True
    for scope in ("changes", "archive"):
        prefix = f"pact/{scope}/{change_id}/"
        if normalized.startswith(prefix):
            return normalized[len(prefix):] not in ("", "pact.yaml")
    return False


def is_archived(path: Path) -> bool:
    return path.parent.parent.name == "archive"


def read_raw(path: Path) -> dict:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise PactError("invalid_yaml", f"{path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise PactError("invalid_yaml", f"{path}: manifest is not a mapping")
    return raw


def pact_version_of(path: Path) -> int:
    raw = read_raw(path)
    value = raw.get("pact_version")
    return int(value) if isinstance(value, int) else 0


def _validate(raw: dict, where: str) -> Manifest:
    try:
        return Manifest.model_validate(raw)
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        raise PactError("invalid_manifest", f"{where}: {problems}", errors=exc.errors()) from exc


def load(path: Path) -> Manifest:
    return _validate(read_raw(path), str(path))


def load_text(text: str, *, where: str = "<memory>") -> Manifest:
    """Validate a manifest held in memory, e.g. one read out of git history
    with ``git show <commit>:<path>``. Same refusals as :func:`load`."""
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise PactError("invalid_yaml", f"{where}: {exc}") from exc
    if not isinstance(raw, dict):
        raise PactError("invalid_yaml", f"{where}: manifest is not a mapping")
    return _validate(raw, where)


def dump(manifest: Manifest) -> str:
    data = manifest.model_dump(mode="json", exclude_none=True)
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=88)


def write(path: Path, manifest: Manifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dump(manifest), encoding="utf-8")


_EVIDENCE_KEY = re.compile(r"^evidence:[ \t]*(\[\][ \t]*)?(#.*)?$")


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def append_evidence(path: Path, row: dict) -> None:
    """Append one receipt to ``evidence`` by editing the text, not the model.

    Dumping the whole model rewrote every block scalar the author wrote: a
    ``|`` ``why`` or ``statement`` came back quoted or folded and without its
    final newline, so the YAML-parsed reviewed text changed under a command
    that only records proof. Here every byte outside the inserted row stays as
    written. The result is re-parsed and must equal the old mapping with
    exactly this row appended; anything else is refused and nothing is
    written.
    """
    with path.open(encoding="utf-8", newline="") as handle:
        text = handle.read()
    before = read_raw(path)
    # Line endings are kept as written: a CRLF manifest stays CRLF.
    eol = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines(keepends=True)
    keys = [i for i, line in enumerate(lines) if _EVIDENCE_KEY.match(line.rstrip("\r\n"))]
    if "evidence" in before and len(keys) != 1:
        raise PactError("evidence_layout_unsupported", f"{path}: write `evidence:` as a block list (one `- kind: ...` row per receipt) and attach again")

    def insert(at: int, indent: int) -> None:
        block = yaml.safe_dump([row], sort_keys=False, allow_unicode=True, width=88)
        new = [(" " * indent + line if line.strip() else line).replace("\n", eol) for line in block.splitlines(keepends=True)]
        if at == len(lines) and lines and not lines[-1].endswith("\n"):
            # Appending at the end of a file with no final newline: the
            # separator moves to the line before, and the file still ends
            # without one.
            lines[-1] += eol
            new[-1] = new[-1].rstrip("\r\n")
        lines[at:at] = new

    if "evidence" not in before:
        at = len(lines)
        insert(at, 0)
        lines.insert(at, "evidence:" + eol)
    else:
        key = keys[0]
        match = _EVIDENCE_KEY.match(lines[key].rstrip("\r\n"))
        if match.group(1) or before["evidence"] is None:
            # `evidence: []` opens into a block list, keeping a comment on
            # that line; a bare `evidence:` line is left exactly as written.
            if match.group(1):
                ending = lines[key][len(lines[key].rstrip("\r\n")):] or eol
                comment = match.group(2)
                lines[key] = "evidence:" + (" " + comment if comment else "") + ending
            insert(key + 1, 0)
        else:
            # The list runs to the first content line that is neither a row
            # (`-` at the item indent) nor indented under one. A line indented
            # under a row is part of it even when it starts with `#`: that is
            # a quoted scalar's continuation (`    #1698, ...`), not a comment.
            start = key + 1
            content = [i for i in range(start, len(lines)) if lines[i].strip()]
            indent = next((_indent_of(lines[i]) for i in content if not lines[i].lstrip().startswith("#")), 0)
            end = start
            for i in content:
                line = lines[i]
                if _indent_of(line) > indent or (_indent_of(line) == indent and line.lstrip().startswith("-")):
                    end = i + 1
                elif not line.lstrip().startswith("#"):
                    break
            insert(end, indent)

    updated = "".join(lines)
    try:
        after = yaml.safe_load(updated)
    except yaml.YAMLError as exc:
        raise PactError("evidence_append_unverified", f"{path}: appending the row would not parse ({exc}); nothing was written") from exc
    expected = {**before, "evidence": [*(before.get("evidence") or []), row]}
    if after != expected:
        raise PactError("evidence_append_unverified", f"{path}: appending the row would change more than the evidence list, or the row's own text does not survive YAML (a NEL or lone surrogate in --note); nothing was written")
    _validate(after, str(path))
    # A private temporary file in the same directory, given the manifest's
    # mode, then renamed over it: no shared `.tmp` name to collide on or to
    # be a symlink, and no partial manifest on failure.
    # O_EXCL never follows or reuses an existing path (a symlink included).
    tmp_name = str(path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"))
    fd = os.open(tmp_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(updated)
        os.chmod(tmp_name, path.stat().st_mode & 0o7777)
        os.replace(tmp_name, path)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise
