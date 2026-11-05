"""The setup record and its digest: what makes two replays the same replay.

``setup_digest`` is sha256 over canonical JSON (sorted keys, no spaces, UTF-8)
of every field that can change a score: the okg commit, the deployment tree,
branch and generation, the served tools and instructions, the exact system
text and tool list the answerer is sent, the question set, every answerer
and judge setting, the extra endpoints' tool lists, the names and prefixes of the
server's ``env_files`` variables and its inherited-variable allowlist, the
tries, the price table
and the runner's own fixed limits. Host values (paths, URLs, the ledger), the
run id, worker count, caps and timestamps stay out, so the same setup on
another host or day gives the same digest.

``diff_setups`` names each dotted field that differs; compare uses it for the
one-change rule.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .errors import refuse

SETUP_SCHEMA = "okg.replay.setup.v1"
DIGEST_PREFIX = "sha256:"
#: Directory names skipped when the deployment tree is hashed: VCS metadata,
#: bytecode, and the worktree-local ``.okg`` state (none of it is served).
TREE_EXCLUDES = (".git", "__pycache__", ".okg", ".pytest_cache")
#: Paths of the okg checkout that must be committed: the code, and what decides the installed dependencies.
DIRTY_PATHS = ("src", "pyproject.toml", "uv.lock")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(text: str) -> str:
    return DIGEST_PREFIX + hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_text(canonical_json(value))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return DIGEST_PREFIX + h.hexdigest()


def tree_digest(root: Path, excludes: Iterable[str] = TREE_EXCLUDES) -> str:
    """sha256 over (relative path, file sha256) of every regular file under ``root``.

    A symlink is hashed as its target text, so a link that points elsewhere
    changes the digest without the runner following it out of the tree.
    """
    skip = frozenset(excludes)
    rows: list[list[str]] = []
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root)
        if any(part in skip for part in rel.parts):
            continue
        if path.is_symlink():
            rows.append([rel.as_posix(), "symlink:" + str(path.readlink())])
        elif path.is_file():
            rows.append([rel.as_posix(), sha256_file(path)])
    return sha256_json(rows)


def okg_source_root() -> Path:
    import okg

    return Path(okg.__file__).resolve().parents[2]


def okg_identity(root: Path | None = None) -> dict[str, str]:
    """The okg commit and the runner's own file digest; refuse a dirty or unknown tree.

    The served code and the runner are both read from this checkout, so an
    uncommitted edit under ``src/`` (or to ``pyproject.toml`` / ``uv.lock``, which
    decide the installed dependencies) would change behavior the commit does not
    name: it is refused, not recorded as a warning.
    """
    root = okg_source_root() if root is None else root
    try:
        commit = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True, timeout=60,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "-C", str(root), "--no-optional-locks", "status", "--porcelain", "--untracked-files=all", "--", *DIRTY_PATHS],
            capture_output=True, text=True, check=True, timeout=120,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise refuse("okg_commit_unknown",
                     f"the okg source at {root} is not a readable git checkout ({type(exc).__name__}); "
                     "the setup must name the commit it runs") from exc
    if dirty:
        count = len(dirty.splitlines())
        raise refuse("okg_tree_dirty",
                     f"{count} uncommitted path(s) under src/, pyproject.toml or uv.lock in the okg checkout; "
                     "commit or remove them first")
    return {"commit": commit, "runner_tree": tree_digest(Path(__file__).resolve().parent)}


def build_setup(
    *,
    okg: Mapping[str, str],
    deployment: Mapping[str, Any],
    served: Mapping[str, Any],
    model_input: Mapping[str, str],
    question_set: Mapping[str, Any],
    answerer: Mapping[str, Any],
    judge: Mapping[str, Any],
    extra_endpoints: Mapping[str, Mapping[str, Any]],
    serve_env: Mapping[str, Any],
    tries: int,
    prices: str,
    runner: Mapping[str, Any],
) -> dict[str, Any]:
    """The setup fields and their digest (``{"schema", "digest", "fields"}``)."""
    fields = {
        "okg": dict(okg),
        "deployment": dict(deployment),
        "served": dict(served),
        "model_input": dict(model_input),
        "question_set": dict(question_set),
        "answerer": dict(answerer),
        "judge": dict(judge),
        "extra_endpoints": {name: dict(value) for name, value in extra_endpoints.items()},
        "serve_env": dict(serve_env),
        "tries": tries,
        "prices": prices,
        "runner": dict(runner),
    }
    # Round-trip through JSON so the record and its digest see the same values.
    fields = json.loads(canonical_json(fields))
    return {"schema": SETUP_SCHEMA, "digest": sha256_json(fields), "fields": fields}


def check_setup(record: Mapping[str, Any]) -> str:
    """The digest of a stored setup record, after checking it was not edited."""
    if record.get("schema") != SETUP_SCHEMA or not isinstance(record.get("fields"), Mapping):
        raise refuse("setup_record_invalid", f"setup record must have schema {SETUP_SCHEMA!r} and fields")
    digest = sha256_json(record["fields"])
    if digest != record.get("digest"):
        raise refuse("setup_record_edited", "the stored setup digest does not match its fields")
    return digest


def flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """Dotted leaf paths of a nested mapping; lists are leaves."""
    if isinstance(value, Mapping) and value:
        out: dict[str, Any] = {}
        for key in sorted(value):
            out.update(flatten(value[key], f"{prefix}.{key}" if prefix else str(key)))
        return out
    return {prefix: value}


_MISSING = object()


def diff_setups(a: Mapping[str, Any], b: Mapping[str, Any]) -> list[str]:
    """The dotted field names whose values differ between two setups' fields."""
    fa, fb = flatten(a), flatten(b)
    return sorted(k for k in set(fa) | set(fb) if fa.get(k, _MISSING) != fb.get(k, _MISSING))


def field_table(fields: Mapping[str, Any]) -> list[tuple[str, str]]:
    """(field, value) rows for the dry-run print; values are digests, names and numbers only."""
    return [(k, canonical_json(v) if not isinstance(v, str) else v) for k, v in flatten(fields).items()]
