"""Frozen inputs of an injection run: the corpus tree and the claim state.

``CorpusSnapshot`` pins a document tree by per-file sha256 + a tree digest;
the source tree is never edited (mutation writes a new versioned artifact
directory), and ``verify_unchanged`` re-digests to prove it.

``ClaimSnapshot`` is the generation-pinned targeting state: the effective
claims of the pinned clean generation (``okg.claim_effective`` value
columns; design §2/§5). Built with ``from_rows``; the live DB loader lands
with the ring-0 fixture task, where its column use is proven against a real
published generation.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from okg.bench.injection.types import ClaimRow, InjectionError


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _tree_digest(files: Sequence[tuple[str, str]]) -> str:
    material = "\n".join(f"{relpath}\x00{digest}" for relpath, digest in files)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CorpusSnapshot:
    """An immutable, digest-pinned view of one corpus directory."""

    root: Path
    files: tuple[tuple[str, str], ...]
    tree_digest: str
    suffixes: tuple[str, ...] = ()

    @classmethod
    def load(cls, root: Path | str, *, suffixes: tuple[str, ...] = ()) -> "CorpusSnapshot":
        """Walk ``root`` (sorted, deterministic) and digest every file.

        ``suffixes`` optionally restricts membership (e.g. ``(".md",)``);
        empty means every regular file belongs to the corpus.
        """
        root_path = Path(root).resolve()
        if not root_path.is_dir():
            raise InjectionError(f"corpus root {root_path} is not a directory")
        entries: list[tuple[str, str]] = []
        for dirpath, dirnames, filenames in os.walk(root_path):
            dirnames.sort()
            for name in sorted(filenames):
                path = Path(dirpath) / name
                relpath = path.relative_to(root_path).as_posix()
                if suffixes and not relpath.endswith(suffixes):
                    continue
                entries.append((relpath, _file_sha256(path)))
        if not entries:
            raise InjectionError(f"corpus root {root_path} contains no corpus files")
        files = tuple(sorted(entries))
        return cls(
            root=root_path,
            files=files,
            tree_digest=_tree_digest(files),
            suffixes=tuple(suffixes),
        )

    @property
    def file_digests(self) -> Mapping[str, str]:
        return dict(self.files)

    def has_file(self, relpath: str) -> bool:
        return relpath in self.file_digests

    def read_bytes(self, relpath: str) -> bytes:
        if not self.has_file(relpath):
            raise InjectionError(f"unknown corpus file {relpath!r}")
        return (self.root / relpath).read_bytes()

    def read_text(self, relpath: str) -> str:
        data = self.read_bytes(relpath)
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InjectionError(f"corpus file {relpath!r} is not valid UTF-8: {exc}") from exc

    def verify_unchanged(self) -> None:
        """Re-digest the tree; raise loudly if the source snapshot drifted."""
        current = CorpusSnapshot.load(self.root, suffixes=self.suffixes)
        if current.tree_digest != self.tree_digest:
            raise InjectionError(
                "source corpus changed during injection: "
                f"expected tree digest {self.tree_digest}, found {current.tree_digest}"
            )


@dataclass(frozen=True)
class ClaimSnapshot:
    """Generation-pinned effective-claim state used for graph-conditioned targeting."""

    generation_id: str
    claims: tuple[ClaimRow, ...]

    @classmethod
    def from_checkout(cls, checkout: object, generation_id: str) -> "ClaimSnapshot":
        """Load the effective claims of a pinned generation from its checkout.

        ``checkout`` is a Chronos checkout session (``checkout_generation``)
        whose ``query`` reads generation-scoped kept tables. Claims whose
        ``source_id`` carries no corpus document mapping cannot be targeted
        and raise loudly rather than being dropped.
        """
        rows = checkout.query(  # type: ignore[attr-defined]
            "SELECT row_key, predicate, normalized_key, value, scope, scope_state,"
            " claim_semantics, source_id FROM okg.claim_effective "
            "WHERE row_kind = :kind"
            " ORDER BY row_key",
            {"kind": "effective_claim"},
        )
        claims: list[ClaimRow] = []
        missing: list[str] = []
        malformed: list[str] = []
        for row in rows:
            source_id = row["source_id"] or ""
            if not source_id:
                missing.append(str(row["row_key"]))
                continue
            # A NULL claim column would silently widen targeting (an empty
            # scope means "whole document" to the localizer) — refuse, never
            # coerce (this repo's P0 posture on default-value fallbacks).
            columns = ("predicate", "normalized_key", "value", "scope", "scope_state")
            if any(row[column] is None for column in columns):
                malformed.append(str(row["row_key"]))
                continue
            claims.append(
                ClaimRow(
                    claim_id=str(row["row_key"]),
                    predicate=str(row["predicate"]),
                    normalized_key=str(row["normalized_key"]),
                    value=str(row["value"]),
                    scope=str(row["scope"]),
                    scope_state=str(row["scope_state"]),
                    source_doc=source_id,
                    claim_semantics=row["claim_semantics"],
                )
            )
        if missing or malformed:
            raise InjectionError(
                "unusable claim rows in targeting snapshot: "
                f"missing source_id {missing[:5]}, NULL claim columns {malformed[:5]}"
            )
        return cls.from_rows(generation_id, claims)

    @classmethod
    def from_rows(cls, generation_id: str, rows: Iterable[ClaimRow]) -> "ClaimSnapshot":
        if not generation_id:
            raise InjectionError("ClaimSnapshot.generation_id must be non-empty")
        ordered = tuple(sorted(rows, key=lambda row: row.claim_id))
        seen: set[str] = set()
        for row in ordered:
            if row.claim_id in seen:
                raise InjectionError(f"duplicate claim_id {row.claim_id!r} in snapshot")
            seen.add(row.claim_id)
        return cls(generation_id=generation_id, claims=ordered)

    def digest(self) -> str:
        """Content digest of the targeting state — the manifest's deterministic
        pin (generation ids embed wall-clock mint times and live only in the
        volatile run-binding)."""
        material = "\n".join(
            "\x00".join(
                (
                    claim.claim_id,
                    claim.predicate,
                    claim.normalized_key,
                    claim.value,
                    claim.scope,
                    claim.scope_state,
                    claim.source_doc,
                    claim.claim_semantics or "",
                )
            )
            for claim in self.claims
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    def by_bucket(self) -> Mapping[tuple[str, str], tuple[ClaimRow, ...]]:
        """Claims grouped by (predicate, normalized_key) — the bucket structure."""
        buckets: dict[tuple[str, str], list[ClaimRow]] = {}
        for claim in self.claims:
            buckets.setdefault((claim.predicate, claim.normalized_key), []).append(claim)
        return {key: tuple(values) for key, values in sorted(buckets.items())}
