"""Run manifests — the reproducibility record.

Every run directory carries ``manifest.json`` recording model, Claude CLI
version, arm, benchmark + pinned dataset ref, prompt digests, budgets, and
ordering. Published numbers must reproduce from the scripted runner, so a
``--resume`` under a manifest whose identity fields no longer match the
requested invocation refuses loudly (ManifestMismatch) instead of quietly
continuing a different experiment.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .errors import ArmDriverError, ManifestMismatch
from .model import dump_json, load_json

# Fields that define run identity: a resume must match on ALL of these.
# `corpus` and `judge` are identity: a resume against a different corpus
# deployment or judge model would silently continue a different experiment.
IDENTITY_FIELDS = (
    "schema_version",
    "benchmark",
    "arm",
    "model",
    "dataset_ref",
    "subset",
    "pilot",
    "limit",
    "prompt_digest",
    "ordering",
    "budgets",
    "native_memory_mode",
    "corpus",
    "judge",
)


@dataclass(frozen=True)
class RunManifest:
    schema_version: str
    run_id: str
    created_at: str
    benchmark: str
    arm: str
    model: str
    claude_cli_version: str | None
    framework_git_sha: str | None
    dataset_ref: dict[str, Any]
    subset: str | None
    pilot: bool
    limit: int | None
    prompt_digest: str
    ordering: dict[str, Any]
    budgets: dict[str, Any]
    native_memory_mode: str | None
    judge: dict[str, Any] | None
    corpus: dict[str, Any] | None
    argv: list[str] = field(default_factory=list)

    def identity(self) -> dict[str, Any]:
        d = asdict(self)
        return {k: d[k] for k in IDENTITY_FIELDS}


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def new_run_id(benchmark: str, arm: str) -> str:
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    return f"{benchmark}--{arm}--{stamp}--{uuid.uuid4().hex[:6]}"


def prompt_digest(prompts: list[str]) -> str:
    h = hashlib.sha256()
    for p in prompts:
        h.update(p.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()


def claude_cli_version(claude_bin: str) -> str:
    """The pinned CLI version for the manifest. Missing binary is fatal for
    claude-backed arms — never recorded as 'unknown'."""
    proc = subprocess.run(
        [claude_bin, "--version"], capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise ArmDriverError(
            f"cannot determine claude CLI version ({claude_bin} --version failed): "
            f"{proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def framework_git_sha() -> str | None:
    proc = subprocess.run(
        ["git", "-C", str(Path(__file__).resolve().parent), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.stdout.strip() if proc.returncode == 0 else None


def write_manifest(run_dir: Path, manifest: RunManifest) -> None:
    dump_json(run_dir / "manifest.json", asdict(manifest))


def load_manifest(run_dir: Path) -> RunManifest:
    path = run_dir / "manifest.json"
    if not path.exists():
        raise ManifestMismatch(f"run {run_dir} has no manifest.json")
    return RunManifest(**load_json(path))


def check_resume(existing: RunManifest, requested: RunManifest) -> None:
    """Refuse a resume whose identity diverges from the recorded manifest."""
    mismatches = {
        k: (e, r)
        for k in IDENTITY_FIELDS
        if (e := existing.identity()[k]) != (r := requested.identity()[k])
    }
    if mismatches:
        lines = "\n".join(
            f"  {k}: recorded={e!r} requested={r!r}" for k, (e, r) in mismatches.items()
        )
        raise ManifestMismatch(
            f"refusing to resume run {existing.run_id}: manifest mismatch on\n{lines}"
        )


def digest_of(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
