"""Answer-key leakage firewall (design §9) — fail closed.

The scan hunts **synthetic identifier classes only** — ``okgmut_`` mutation
ids, ``okgfam_`` family ids, and the manifest-only ``okgcanary_`` token, all
from a reserved namespace guaranteed absent from natural text
(``pre_text``/``post_text`` are corpus content by construction and are not
scanned for). Surfaces:

- every mutated corpus file's bytes;
- the run's **captured pipeline inputs** — per ring: ring 0 has no prompts,
  so its surface is the bytes the source reads plus the published
  node/attribute content of the mutated generation; LLM rings add the
  audited prompt bytes actually sent.

A clean scan writes ``leakage-check.json`` binding the exact artifact
digests {mutated tree, mutations.jsonl, benchmark-manifest.json}; a dirty
scan raises and writes nothing. ``load_leakage_pass`` re-digests the
artifacts before handing the pass to the scorer, so a post-check tamper is
detected; the scorer refuses to emit a scorecard without a valid pass.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from okg.bench.injection.manifest import (
    MANIFEST_FILENAME,
    MUTATED_CORPUS_DIRNAME,
    MUTATIONS_FILENAME,
    REJECTED_FILENAME,
    canonical_json_bytes,
)
from okg.bench.injection.snapshot import CorpusSnapshot
from okg.bench.injection.types import InjectionError

LEAKAGE_FILENAME = "leakage-check.json"
LEAKAGE_SCHEMA = "okg.injection.leakage-check.v1"


def _artifact_digests(out_dir: Path) -> dict[str, str]:
    import hashlib

    mutated_tree = CorpusSnapshot.load(out_dir / MUTATED_CORPUS_DIRNAME).tree_digest
    return {
        "mutated_tree": mutated_tree,
        "mutations_jsonl": hashlib.sha256(
            (out_dir / MUTATIONS_FILENAME).read_bytes()
        ).hexdigest(),
        "rejected_sites_jsonl": hashlib.sha256(
            (out_dir / REJECTED_FILENAME).read_bytes()
        ).hexdigest(),
        "benchmark_manifest": hashlib.sha256(
            (out_dir / MANIFEST_FILENAME).read_bytes()
        ).hexdigest(),
    }


def _answer_key_tokens(out_dir: Path) -> tuple[str, ...]:
    manifest = json.loads((out_dir / MANIFEST_FILENAME).read_text(encoding="utf-8"))
    tokens = {manifest["canary"]}
    for line in (out_dir / MUTATIONS_FILENAME).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        tokens.add(row["mutation_id"])
        tokens.add(row["family_id"])
    return tuple(sorted(tokens))


def run_leakage_check(
    out_dir: Path | str,
    *,
    captured_inputs: Iterable[bytes] = (),
) -> Path:
    """Scan the run's surfaces for answer-key tokens; write the pass artifact.

    Raises :class:`InjectionError` on any hit (and writes nothing) — the
    check fails closed.
    """
    root = Path(out_dir)
    tokens = _answer_key_tokens(root)
    token_bytes = [token.encode("utf-8") for token in tokens]

    corpus = CorpusSnapshot.load(root / MUTATED_CORPUS_DIRNAME)
    for relpath, _digest in corpus.files:
        data = corpus.read_bytes(relpath)
        for token in token_bytes:
            if token in data:
                raise InjectionError(
                    f"answer-key token {token.decode()!r} leaked into mutated corpus "
                    f"file {relpath!r} — leakage check failed closed"
                )
    for index, chunk in enumerate(captured_inputs):
        for token in token_bytes:
            if token in chunk:
                raise InjectionError(
                    f"answer-key token {token.decode()!r} leaked into captured "
                    f"pipeline input #{index} — leakage check failed closed"
                )

    artifact = {
        "schema": LEAKAGE_SCHEMA,
        "status": "pass",
        "digests": _artifact_digests(root),
        "scanned_token_count": len(tokens),
        "scanned_corpus_files": len(corpus.files),
    }
    path = root / LEAKAGE_FILENAME
    path.write_bytes(canonical_json_bytes(artifact))
    return path


def load_leakage_pass(out_dir: Path | str) -> Mapping[str, Any]:
    """Load and re-verify the pass artifact; raise loudly on any mismatch."""
    root = Path(out_dir)
    path = root / LEAKAGE_FILENAME
    if not path.is_file():
        raise InjectionError(
            f"no leakage-check artifact at {path} — the scorer refuses without one"
        )
    artifact = json.loads(path.read_text(encoding="utf-8"))
    if artifact.get("schema") != LEAKAGE_SCHEMA or artifact.get("status") != "pass":
        raise InjectionError(f"leakage-check artifact at {path} is not a valid pass")
    current = _artifact_digests(root)
    recorded = artifact.get("digests", {})
    if recorded != current:
        raise InjectionError(
            "leakage-check artifact digests do not match the run artifacts "
            f"(recorded {recorded}, current {current}) — re-run the leakage check"
        )
    return artifact
