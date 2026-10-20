"""Deterministic claim extraction over a markdown corpus (ring 0, design §10).

``ClaimCorpusSource`` is the LLM-free claim extractor of the ring-0 fixture:
a real ``SourceAdapter`` that walks a corpus directory and emits one
``consistency_claim`` node per ``Label: value`` line (and per numbered
procedure step), plus one functional ``predicate_policy`` node per
configured predicate. It shares :class:`MarkdownScopeIndex` with the
injector, so the heading-path scopes it stamps on claims are byte-identical
to the scopes the injector localizes against — extraction and targeting
cannot drift apart.

Determinism contract: claim node ids are value-free
(``claim:<doc>:<predicate>:<key>:<occurrence>``) so an injected value
rewrite UPDATES the same node (same engine claim identity, new value), and
``source_revision`` is the file's sha256 so the runner re-reads exactly the
mutated files.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

from okg.bench.injection.scope_index import MarkdownScopeIndex
from okg.bench.injection.types import InjectionError
from okg.substrate.library.sources.base import NodeFact, SourceRun

_KV_LINE_RE = re.compile(r"^(?P<label>[A-Za-z][A-Za-z0-9_ ]*?):\s+(?P<value>\S.*?)\s*$")
_STEP_LINE_RE = re.compile(r"^(?P<number>\d+)\.\s+(?P<value>\S.*?)\s*$")
_H1_RE = re.compile(r"^#\s+(?P<title>.+?)\s*$", re.MULTILINE)


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


@dataclass(frozen=True)
class ClaimCorpusSource:
    """SourceAdapter: markdown corpus → consistency_claim / predicate_policy facts."""

    repo_root: Path | str
    policies: tuple[str, ...] = ()
    source_class: str = "wiki"
    name: str = "claim_corpus"
    profile: str = "discovery_crawl"

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        sync_scope: Optional[Mapping[str, Any]] = None,
    ) -> SourceRun:
        return SourceRun(facts=self._facts(), completed_scope=True, run_mode=mode)

    def _facts(self) -> Iterator[NodeFact]:
        root = Path(self.repo_root).resolve()
        if not root.is_dir():
            raise InjectionError(f"claim corpus root {root} is not a directory")
        docs = sorted(path.relative_to(root).as_posix() for path in root.rglob("*.md"))
        if not docs:
            raise InjectionError(f"claim corpus root {root} contains no .md documents")
        for predicate in sorted(self.policies):
            yield NodeFact(
                f"policy:functional:{predicate}",
                "predicate_policy",
                {
                    "enabled": "true",
                    "policy_id": f"functional_{predicate}",
                    "policy_kind": "functional",
                    "predicate": predicate,
                },
                {"id": f"policy:functional:{predicate}"},
                {"v": "1"},
            )
        for relpath in docs:
            text = (root / relpath).read_text(encoding="utf-8")
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            yield from self._document_claims(relpath, text, digest)

    def _document_claims(self, relpath: str, text: str, digest: str) -> Iterator[NodeFact]:
        yield from parse_document_claims(
            relpath, text, digest=digest, source_class=self.source_class
        )


def _line_claim(entity: str, line: str) -> tuple[str, str, str] | None:
    if line.startswith("#"):
        return None
    step = _STEP_LINE_RE.match(line)
    if step is not None:
        return ("step", f"{entity}:step_{step.group('number')}", step.group("value"))
    kv = _KV_LINE_RE.match(line)
    if kv is not None:
        return (_slug(kv.group("label")), entity, kv.group("value"))
    return None


def parse_document_claims(
    relpath: str, text: str, *, digest: str = "0", source_class: str = "wiki"
) -> Iterator[NodeFact]:
    """The deterministic extraction rule, callable without an adapter.

    Public so the identity-stability probe (design §5) can re-derive a
    mutated document's claims against the same rule the fixture ingests
    with. Raises :class:`InjectionError` on an unparseable document (no H1).
    """
    h1 = _H1_RE.search(text)
    if h1 is None:
        raise InjectionError(f"claim corpus document {relpath!r} has no H1 title")
    entity = _slug(h1.group("title"))
    index = MarkdownScopeIndex(text)
    occurrences: dict[tuple[str, str], int] = {}
    offset = 0
    for line in text.splitlines(keepends=True):
        stripped = line.rstrip("\n")
        claim = _line_claim(entity, stripped)
        if claim is not None:
            predicate, key, value = claim
            occurrence = occurrences.get((predicate, key), 0)
            occurrences[(predicate, key)] = occurrence + 1
            node_id = f"claim:{relpath}:{predicate}:{key}:{occurrence}"
            yield NodeFact(
                node_id,
                "consistency_claim",
                {
                    "predicate": predicate,
                    "normalized_key": key,
                    "value": value,
                    "scope": index.section_path_at(offset),
                    "scope_state": "known",
                    "claim_provenance": "derived",
                    "source_id": relpath,
                    "source_class": source_class,
                },
                {"path": relpath, "claim": node_id},
                {"sha": digest},
            )
        offset += len(line)
