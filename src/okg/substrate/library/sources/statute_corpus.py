"""Statute corpus source adapter — frozen local statute-pool TSV.

Profile: `reference_catalog`.

Ingests an operator-frozen slice of a statute passage pool (the
reglab/housing_qa ``statutes.tsv`` shape: one row per passage with
``citation``, ``path``, ``state``, ``text``, ``idx`` columns) with NO
live network calls. This is the legal/gov vertical's ingestion: the
full pool is 3.36 GB, so the operator downloads it once and feeds
either the whole file or a pre-cut slice; ``state_filter`` narrows
emission to one state's passages (the pilot path per
`okg-bench-domain-adapters`, req.dom.housing).

Emits, per `okg-corpus-legal`:

  * `statute_passage` nodes — statute_idx, citation, state,
    header_path, text (truncated to 4000 chars with a `truncated`
    flag) and `unresolved_references`;
  * `statute_section` nodes — one per DISTINCT PREFIX of a row's
    `path` trail, carrying label / depth / trail / state. The node id
    is keyed by (state, prefix) and NEVER by label, so Alaska's
    "Title 34" and Wisconsin's "Title 34" are distinct nodes;
  * `jurisdiction` nodes — one per distinct `state` value;
  * `statute_passage -part_of-> statute_section` (to the DEEPEST
    prefix of the passage's own trail),
    `statute_section -part_of-> statute_section` (the parent chain),
    `statute_passage -in_jurisdiction-> jurisdiction` (every passage),
    `statute_section -in_jurisdiction-> jurisdiction` (root sections),
    and the derived
    `statute_passage -cross_references-> statute_passage`.

Reading the `path` trail and the `state` column is INGESTING the
source, not presupposing retrieval structure (`okg-corpus-design`,
"INGESTING IS NOT PRESUPPOSING"): both columns are shipped by the
publisher and were previously flattened into strings.

The header row drives column mapping; unknown or missing headers
raise — half-parsing a mislabeled column would silently corrupt the
corpus every arm shares. Zero passages after filtering raises so an
over-narrow filter cannot masquerade as an empty healthy corpus. A
slice in which no row's `path` splits on `path_separator` while rows
carry some OTHER separator character raises rather than flattening
the whole hierarchy into one-segment sections. A slice of at least
`_CROSSREF_MIN_PASSAGES` passages that resolves ZERO cross-references
raises, so "no matches" cannot masquerade as "no cross-references
exist".

Unresolvable references are SKIPPED AND COUNTED — never a stub node
(which would enter the retrieval candidate pool the benchmark scores)
and never a dangling edge (which would block the publish). The
unresolved citation strings stay on the citing passage as
`unresolved_references`.

Per `okg-bench-domain-adapters` (2026-07-27) and `okg-corpus-legal`
(2026-07-28).
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._cti_snapshot import (
    read_snapshot_bytes,
    record_set_from_facts,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)


__all__ = ["StatuteCorpusSource"]


_REQUIRED_HEADERS = frozenset({"citation", "path", "state", "text", "idx"})
_TEXT_MAX_CHARS = 4000

#: Characters that separate a hierarchy trail in the wild. Only consulted
#: when NOT ONE row in the slice split on the configured `path_separator`
#: — a corpus-level signal that the slice uses a different separator,
#: which would otherwise flatten every trail into a single section.
_SUSPECT_PATH_SEPARATORS = ("|", "\\", "/", "»", "›", "→", "≫", "::")

#: req.legal.crossref: at or above this passage count, zero resolved
#: cross-references FAILS the build. Below it a slice may honestly
#: contain none. The pilot slice is 60 passages, so the gate is live for
#: every real build.
_CROSSREF_MIN_PASSAGES = 25

#: Citation tokenization. A citation is a sequence of alphabetic words
#: and section numbers; everything else (§, periods, commas) is
#: punctuation. Applied IDENTICALLY to the citation column and to the
#: strings found in passage text.
_CITATION_TOKEN_RE = re.compile(r"[0-9]+(?:[.\-][0-9A-Za-z]+)*|[A-Za-z]+")

#: A statute section number: digit runs joined by `.` or `-`.
_SECTION_NUMBER = r"[0-9]+(?:[.\-][0-9A-Za-z]+)*"

#: The cues that mark a section number in running text.
_REFERENCE_CUE = r"(?:§{1,2}|\bsections?\b\.?|\bsecs?\b\.?)"

_CUED_REFERENCE_RE = re.compile(
    _REFERENCE_CUE + r"\s*(" + _SECTION_NUMBER + r")",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _Passage:
    """One parsed TSV row, before any fact is emitted."""

    idx: int
    node_id: str
    citation: str
    state: str
    state_key: str
    header_path: str
    trail: tuple[str, ...]
    text: str
    truncated: bool


@dataclass
class StatuteCorpusSource:
    """Statute passage pool adapter over one frozen local TSV.

    `statutes_tsv` is the operator-downloaded pool (possibly a
    pre-cut slice file). `state_filter` limits emission to one
    state's passages. `repo` names the upstream dataset the TSV
    came from (recorded in node identity + revision).
    `path_separator` is the character the slice's `path` column
    uses between trail segments. `cross_references` disables the
    derived cross-reference pass — it exists ONLY for the contract's
    negative-control build (okg-corpus-legal, task.legal.contract),
    which must show a corpus WITHOUT the enricher failing the
    contract. There is no live mode.
    """

    name: str = "statute_corpus"
    profile: str = "reference_catalog"
    repo: str = "reglab/housing_qa"
    statutes_tsv: Optional[Path | str] = None
    state_filter: Optional[str] = None
    path_separator: str = ">"
    cross_references: bool = True

    #: Counts from the most recent run: passages, sections,
    #: jurisdictions, every edge family, the section-orphan roster and
    #: the resolved/unresolved cross-reference tallies.
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    # Incremental-source contract (issue #323): one frozen local file
    # -> the content-hash floor over its bytes.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if not self.path_separator.strip():
            raise SubstrateError(
                f"source {self.name!r}: path_separator must be a "
                f"non-blank string (got {self.path_separator!r})",
                data={"adapter": self.name},
                fix_hint="path_separator='>' for the released statute pool",
            )
        self.change_probe = self._build_change_probe()
        # The run consumes exactly the hashed bytes of the one TSV;
        # state_filter is part of the probe config, so a matching token
        # proves identical emission.
        self.probe_short_circuit_safe = True

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, Path]]:
            if self.statutes_tsv is None:
                return []
            path = Path(self.statutes_tsv)
            return [(str(path), path)] if path.is_file() else []

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "statutes_tsv": (
                    str(self.statutes_tsv)
                    if self.statutes_tsv is not None else None
                ),
                "state_filter": self.state_filter,
                # Both change WHICH facts the same bytes produce, so
                # they belong in the probe config: a token that matched
                # across a separator change would prove nothing.
                "path_separator": self.path_separator,
                "cross_references": self.cross_references,
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        if self.statutes_tsv is None:
            raise SubstrateError(
                f"source {self.name!r}: pass statutes_tsv",
                fix_hint="statutes_tsv=Path('.../statutes.tsv') "
                         "(the pool or a pre-cut slice)",
            )
        path = Path(self.statutes_tsv)
        raw = read_snapshot_bytes(path, adapter=self.name)
        facts = self._normalize_tsv(raw, path=path)

        completed_scope = mode in {"reconcile", "scope_complete", "release_new"}
        record_set = (
            record_set_from_facts(facts) if completed_scope else None
        )
        applied_raw_token = self.change_probe.token_from_item_hashes({
            str(path): hashlib.sha256(raw).hexdigest(),
        })

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            yield from facts

        return SourceRun(
            facts=_stream(),
            completed_scope=completed_scope,
            applied_raw_token=applied_raw_token,
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode=mode,
            record_set=record_set,
        )

    # ---------- node identity ----------

    def _passage_node_id(self, idx: int) -> str:
        return f"statute_passage:{self.repo}:{idx}"

    def _section_node_id(
        self, state_key: str, prefix: tuple[str, ...],
    ) -> str:
        """Node id for one trail prefix, keyed by (state, prefix).

        The digest covers the WHOLE prefix, and the state code sits in
        the id, so two states that both publish "Title 34" — or two
        chapters that both end in "General Provisions" — never collide.
        """
        digest = hashlib.sha256(
            "\x1f".join(prefix).encode("utf-8"),
        ).hexdigest()[:16]
        return f"statute_section:{self.repo}:{state_key}:{digest}"

    def _jurisdiction_node_id(self, state_key: str) -> str:
        return f"jurisdiction:{self.repo}:{state_key}"

    # ---------- TSV parsing ----------

    def _normalize_tsv(
        self, raw: bytes, *, path: Path,
    ) -> list[NodeFact | EdgeFact]:
        passages, empty_segment_rows = self._parse_rows(raw, path=path)
        return self._build_graph(
            passages, path=path, empty_segment_rows=empty_segment_rows,
        )

    def _parse_rows(
        self, raw: bytes, *, path: Path,
    ) -> tuple[list[_Passage], int]:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SubstrateError(
                f"source {self.name!r}: {path} is not valid UTF-8",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-download / re-slice the statute TSV",
            ) from exc
        reader = csv.reader(io.StringIO(text), delimiter="\t")
        try:
            header = next(reader)
        except StopIteration:
            raise SubstrateError(
                f"source {self.name!r}: {path} is empty (no header row)",
                data={"adapter": self.name, "path": str(path)},
                fix_hint="re-download / re-slice the statute TSV",
            ) from None
        header = [h.strip() for h in header]
        missing = sorted(_REQUIRED_HEADERS - set(header))
        unknown = sorted(set(header) - _REQUIRED_HEADERS)
        if missing or unknown:
            raise SubstrateError(
                f"source {self.name!r}: {path} header does not match the "
                f"statute-pool schema (missing={missing}, unknown={unknown}; "
                f"expected exactly {sorted(_REQUIRED_HEADERS)})",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "missing": missing,
                    "unknown": unknown,
                },
                fix_hint="point statutes_tsv at the housing_qa statutes.tsv "
                         "(or a slice cut from it, header preserved)",
            )
        col = {name: header.index(name) for name in header}

        wanted_state = (
            self.state_filter.strip().upper()
            if self.state_filter is not None else None
        )
        splitter = re.compile(r"\s*" + re.escape(self.path_separator) + r"\s*")
        passages: list[_Passage] = []
        total = 0
        empty_segment_rows = 0
        states_seen: set[str] = set()
        for line_no, row in enumerate(reader, start=2):
            if not row or (len(row) == 1 and not row[0].strip()):
                continue  # trailing blank line
            if len(row) != len(header):
                raise SubstrateError(
                    f"source {self.name!r}: {path} line {line_no} has "
                    f"{len(row)} field(s), expected {len(header)}",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "line": line_no,
                    },
                    fix_hint="the TSV slice is malformed; re-cut it",
                )
            total += 1
            citation = row[col["citation"]].strip()
            state = row[col["state"]].strip()
            passage_text = row[col["text"]]
            idx_raw = row[col["idx"]].strip()
            if not citation or not state or not passage_text.strip():
                raise SubstrateError(
                    f"source {self.name!r}: {path} line {line_no} has an "
                    f"empty citation/state/text field",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "line": line_no,
                    },
                    fix_hint="the TSV slice is malformed; re-cut it",
                )
            try:
                idx = int(idx_raw)
            except ValueError as exc:
                raise SubstrateError(
                    f"source {self.name!r}: {path} line {line_no} has "
                    f"non-integer idx {idx_raw!r}",
                    data={
                        "adapter": self.name,
                        "path": str(path),
                        "line": line_no,
                        "idx": idx_raw,
                    },
                    fix_hint="the TSV slice is malformed; re-cut it",
                ) from exc
            states_seen.add(state)
            if wanted_state is not None and state.upper() != wanted_state:
                continue
            header_path = row[col["path"]].strip()
            segments = [s.strip() for s in splitter.split(header_path)]
            trail = tuple(s for s in segments if s)
            if len(trail) != len(segments) and trail:
                # "A > > B": an interior segment was blank. The trail is
                # still usable, but the row is reported rather than
                # quietly reshaped.
                empty_segment_rows += 1
            passages.append(_Passage(
                idx=idx,
                node_id=self._passage_node_id(idx),
                citation=citation,
                state=state,
                state_key=state.upper(),
                header_path=header_path,
                trail=trail,
                text=passage_text[:_TEXT_MAX_CHARS],
                truncated=len(passage_text) > _TEXT_MAX_CHARS,
            ))
        if not passages:
            raise SubstrateError(
                f"source {self.name!r}: {path} yielded zero statute "
                f"passages"
                + (
                    f" for state_filter {self.state_filter!r} "
                    f"({total} row(s) in other states: "
                    f"{sorted(states_seen)})"
                    if wanted_state is not None
                    else " (no data rows)"
                ),
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "state_filter": self.state_filter,
                    "rows_seen": total,
                },
                fix_hint="check the state_filter code against the states "
                         "present in the slice",
            )
        return passages, empty_segment_rows

    # ---------- hierarchy ----------

    def _assert_separator_is_the_slice_s(
        self, passages: list[_Passage], *, path: Path,
    ) -> None:
        """Refuse a slice whose `path` column uses a separator we do not
        parse.

        Fires only at CORPUS level: if not one row split into more than
        one segment AND some row carries a different separator
        character, every trail would collapse into a single flat
        section. That is a dropped hierarchy dressed as a shallow one,
        so it raises instead.
        """
        if any(len(p.trail) > 1 for p in passages):
            return
        suspects = [
            p for p in passages
            if p.header_path
            and any(s in p.header_path for s in _SUSPECT_PATH_SEPARATORS)
        ]
        if not suspects:
            return
        samples = [
            {"idx": p.idx, "path": p.header_path} for p in suspects[:5]
        ]
        raise SubstrateError(
            f"source {self.name!r}: {path} — not one row's `path` splits "
            f"on {self.path_separator!r}, yet {len(suspects)} row(s) carry "
            f"a different separator (e.g. {samples[0]['path']!r}); "
            f"refusing to flatten every trail into one section",
            data={
                "adapter": self.name,
                "path": str(path),
                "path_separator": self.path_separator,
                "rows_with_other_separator": len(suspects),
                "samples": samples,
            },
            fix_hint="pass path_separator='<the separator this slice "
                     "uses>' (the released pool uses '>')",
        )

    def _build_graph(
        self,
        passages: list[_Passage],
        *,
        path: Path,
        empty_segment_rows: int,
    ) -> list[NodeFact | EdgeFact]:
        self._assert_separator_is_the_slice_s(passages, path=path)

        source_revision = {"repo": self.repo, "corpus": "statutes_tsv"}

        # --- sections: one per DISTINCT PREFIX of a trail, per state ---
        sections: dict[str, dict[str, Any]] = {}
        for passage in passages:
            for depth in range(len(passage.trail)):
                prefix = passage.trail[: depth + 1]
                node_id = self._section_node_id(passage.state_key, prefix)
                if node_id not in sections:
                    sections[node_id] = {
                        "label": prefix[-1],
                        "depth": depth,
                        "trail": prefix,
                        "state": passage.state,
                        "state_key": passage.state_key,
                    }

        # --- jurisdictions: one per distinct state ---
        jurisdictions: dict[str, str] = {}
        for passage in passages:
            jurisdictions.setdefault(
                self._jurisdiction_node_id(passage.state_key),
                passage.state_key,
            )

        # --- edges ---
        passage_part_of: list[tuple[_Passage, str]] = []
        orphan_idx: list[int] = []
        for passage in passages:
            if not passage.trail:
                # No released path: the passage still gets its
                # jurisdiction edge. Counted and reported, never given a
                # fabricated section.
                orphan_idx.append(passage.idx)
                continue
            passage_part_of.append((
                passage,
                self._section_node_id(passage.state_key, passage.trail),
            ))

        section_part_of: list[tuple[str, str, dict[str, Any]]] = []
        for node_id, info in sections.items():
            if info["depth"] == 0:
                continue
            parent_id = self._section_node_id(
                info["state_key"], info["trail"][:-1],
            )
            section_part_of.append((node_id, parent_id, info))

        root_sections = [
            (node_id, info)
            for node_id, info in sections.items()
            if info["depth"] == 0
        ]

        cross_edges, unresolved_by_node, crossref_stats = (
            self._resolve_cross_references(passages, path=path)
        )

        facts: list[NodeFact | EdgeFact] = []
        for passage in passages:
            facts.append(NodeFact(
                node_id=passage.node_id,
                subtype="statute_passage",
                attrs={
                    "statute_idx": passage.idx,
                    "citation": passage.citation,
                    "state": passage.state,
                    "header_path": passage.header_path,
                    "text": passage.text,
                    "truncated": passage.truncated,
                    "unresolved_references": unresolved_by_node.get(
                        passage.node_id, [],
                    ),
                },
                source_record_id={"repo": self.repo, "idx": passage.idx},
                source_revision=source_revision,
                op="I",
            ))
        for node_id, info in sections.items():
            trail_text = self._join_trail(info["trail"])
            facts.append(NodeFact(
                node_id=node_id,
                subtype="statute_section",
                attrs={
                    "label": info["label"],
                    "depth": info["depth"],
                    "trail": trail_text,
                    "state": info["state"],
                },
                source_record_id={
                    "repo": self.repo,
                    "state": info["state_key"],
                    "trail": trail_text,
                },
                source_revision=source_revision,
                op="I",
            ))
        for node_id, state_key in jurisdictions.items():
            facts.append(NodeFact(
                node_id=node_id,
                subtype="jurisdiction",
                attrs={"code": state_key},
                source_record_id={"repo": self.repo, "state": state_key},
                source_revision=source_revision,
                op="I",
            ))

        for passage, section_id in passage_part_of:
            facts.append(EdgeFact(
                src=passage.node_id,
                dst=section_id,
                edge_type="part_of",
                attrs={},
                source_record_id={"repo": self.repo, "idx": passage.idx},
                source_revision=source_revision,
                op="I",
            ))
        for node_id, parent_id, info in section_part_of:
            facts.append(EdgeFact(
                src=node_id,
                dst=parent_id,
                edge_type="part_of",
                attrs={},
                source_record_id={
                    "repo": self.repo,
                    "state": info["state_key"],
                    "trail": self._join_trail(info["trail"]),
                },
                source_revision=source_revision,
                op="I",
            ))
        for passage in passages:
            facts.append(EdgeFact(
                src=passage.node_id,
                dst=self._jurisdiction_node_id(passage.state_key),
                edge_type="in_jurisdiction",
                attrs={},
                source_record_id={"repo": self.repo, "idx": passage.idx},
                source_revision=source_revision,
                op="I",
            ))
        for node_id, info in root_sections:
            facts.append(EdgeFact(
                src=node_id,
                dst=self._jurisdiction_node_id(info["state_key"]),
                edge_type="in_jurisdiction",
                attrs={},
                source_record_id={
                    "repo": self.repo,
                    "state": info["state_key"],
                    "trail": self._join_trail(info["trail"]),
                },
                source_revision=source_revision,
                op="I",
            ))
        for src, dst, citation_text in cross_edges:
            facts.append(EdgeFact(
                src=src,
                dst=dst,
                edge_type="cross_references",
                attrs={"citation": citation_text},
                provenance="derived",
                source_record_id={"repo": self.repo, "src": src, "dst": dst},
                source_revision=source_revision,
                op="I",
            ))

        self.last_run_stats = {
            "passages_emitted": len(passages),
            "passages_without_path": len(orphan_idx),
            "section_orphan_statute_idx": orphan_idx,
            "paths_with_empty_segment": empty_segment_rows,
            "sections_emitted": len(sections),
            "root_sections": len(root_sections),
            "jurisdictions_emitted": len(jurisdictions),
            "passage_part_of_edges": len(passage_part_of),
            "section_part_of_edges": len(section_part_of),
            "passage_in_jurisdiction_edges": len(passages),
            "section_in_jurisdiction_edges": len(root_sections),
            **crossref_stats,
        }
        return facts

    def _join_trail(self, trail: tuple[str, ...]) -> str:
        return f" {self.path_separator} ".join(trail)

    # ---------- cross-references (derived) ----------

    def _resolve_cross_references(
        self, passages: list[_Passage], *, path: Path,
    ) -> tuple[
        list[tuple[str, str, str]], dict[str, list[str]], dict[str, Any],
    ]:
        """Derive `cross_references` edges from `text` + `citation` only.

        The citation index, the section-number shapes accepted in text
        and the citation prefixes recognised in text are ALL derived
        from the slice's own `citation` column — there is no
        hand-written per-state citation grammar. A reference whose
        target is not in the slice emits nothing and is counted; its
        string is returned for the citing passage's
        `unresolved_references`.
        """
        if not self.cross_references:
            return [], {}, {
                "cross_references_enabled": False,
                "cross_reference_candidates": 0,
                "cross_references_resolved": 0,
                "cross_references_unresolved": 0,
                "cross_references_self_skipped": 0,
                "cross_references_ambiguous": 0,
            }

        # --- index, derived from the citation column ---
        by_state_number: dict[tuple[str, str], set[str]] = {}
        prefix_states: dict[str, set[str]] = {}
        number_shapes: set[int] = set()
        citations_without_number = 0
        for passage in passages:
            prefix, number = _split_citation(passage.citation)
            if number is None:
                citations_without_number += 1
                continue
            by_state_number.setdefault(
                (passage.state_key, number), set(),
            ).add(passage.node_id)
            number_shapes.add(len(re.split(r"[.\-]", number)))
            if prefix:
                prefix_states.setdefault(prefix, set()).add(passage.state_key)

        # A prefix that names two states cannot disambiguate a reference,
        # so it is dropped from the cue vocabulary rather than guessed at.
        unambiguous_prefixes = {
            prefix: next(iter(states))
            for prefix, states in prefix_states.items()
            if len(states) == 1
        }
        dropped_prefixes = len(prefix_states) - len(unambiguous_prefixes)
        prefix_re = _build_prefix_regex(sorted(unambiguous_prefixes))

        edges: list[tuple[str, str, str]] = []
        seen_edges: set[tuple[str, str]] = set()
        unresolved_by_node: dict[str, list[str]] = {}
        candidates_total = 0
        unresolved_total = 0
        self_skipped = 0
        ambiguous = 0
        for passage in passages:
            candidates: dict[tuple[str, str], str] = {}
            for match in _CUED_REFERENCE_RE.finditer(passage.text):
                key = (passage.state_key, match.group(1))
                candidates.setdefault(key, _collapse(match.group(0)))
            if prefix_re is not None:
                for match in prefix_re.finditer(passage.text):
                    prefix = _normalize_citation(match.group("prefix"))
                    state_key = unambiguous_prefixes.get(prefix)
                    if state_key is None:
                        continue
                    key = (state_key, match.group("number"))
                    candidates.setdefault(key, _collapse(match.group(0)))
            unresolved: list[str] = []
            for (state_key, number), raw in candidates.items():
                if len(re.split(r"[.\-]", number)) not in number_shapes:
                    # Not a section-number shape this corpus's own
                    # citation column exhibits — prose, not a citation.
                    continue
                candidates_total += 1
                targets = by_state_number.get((state_key, number))
                if not targets:
                    unresolved.append(raw)
                    continue
                if len(targets) > 1:
                    # Two passages in one state share a citation: the
                    # reference is real but its target is undetermined.
                    ambiguous += 1
                    continue
                dst = next(iter(targets))
                if dst == passage.node_id:
                    self_skipped += 1
                    continue
                if (passage.node_id, dst) in seen_edges:
                    continue
                seen_edges.add((passage.node_id, dst))
                edges.append((passage.node_id, dst, raw))
            unresolved_total += len(unresolved)
            unresolved_by_node[passage.node_id] = sorted(set(unresolved))

        if len(passages) >= _CROSSREF_MIN_PASSAGES and not edges:
            raise SubstrateError(
                f"source {self.name!r}: {path} — {len(passages)} passages "
                f"resolved ZERO cross-references "
                f"({candidates_total} citation-shaped reference(s) found in "
                f"text, {unresolved_total} unresolved, {ambiguous} "
                f"ambiguous); a slice this size that cites nothing means "
                f"the extraction missed the slice's citation format, not "
                f"that no cross-references exist",
                data={
                    "adapter": self.name,
                    "path": str(path),
                    "passages": len(passages),
                    "candidates": candidates_total,
                    "unresolved": unresolved_total,
                    "ambiguous": ambiguous,
                    "citations_without_section_number": (
                        citations_without_number
                    ),
                },
                fix_hint="check the slice's citation column format against "
                         "the references its text actually carries; a "
                         "single-state slice whose statutes only cite other "
                         "states has no in-slice targets and needs a wider "
                         "cut",
            )

        return edges, unresolved_by_node, {
            "cross_references_enabled": True,
            "cross_reference_candidates": candidates_total,
            "cross_references_resolved": len(edges),
            "cross_references_unresolved": unresolved_total,
            "cross_references_self_skipped": self_skipped,
            "cross_references_ambiguous": ambiguous,
            "citations_without_section_number": citations_without_number,
            # Prefixes naming two states (e.g. two slices both using
            # "STAT") cannot disambiguate a reference, so they are
            # dropped from the cue vocabulary — reported, not silent.
            "ambiguous_citation_prefixes_dropped": dropped_prefixes,
        }


# ---------- citation normalization (module-level: identical on both
# sides of the match) ----------


def _collapse(value: str) -> str:
    return " ".join(value.split())


def _normalize_citation(value: str) -> str:
    """Uppercase token join — `AK Stat. § 34.03.010` -> `AK STAT 34.03.010`.

    Section signs, periods and commas are punctuation and drop out;
    the digits of a section number stay joined because
    `_CITATION_TOKEN_RE` matches `34.03.010` as ONE token.
    """
    return " ".join(
        token.upper() for token in _CITATION_TOKEN_RE.findall(value)
    )


def _split_citation(citation: str) -> tuple[str, Optional[str]]:
    """Split a citation into its alphabetic prefix and section number.

    `AK Stat. 34.03.010` -> `("AK STAT", "34.03.010")`. A citation with
    no digit-leading token has no section number and returns
    `(prefix, None)` — it is counted, never defaulted.
    """
    prefix_tokens: list[str] = []
    for token in _normalize_citation(citation).split(" "):
        if token and token[0].isdigit():
            return " ".join(prefix_tokens), token
        if token:
            prefix_tokens.append(token)
    return " ".join(prefix_tokens), None


def _build_prefix_regex(prefixes: list[str]) -> Optional[re.Pattern[str]]:
    """Regex matching `<observed citation prefix> [cue] <number>` in text.

    Built from the prefixes the slice's OWN citation column exhibits, so
    a reference written in full (`WI Stat. § 704.17` inside an Alaska
    passage) resolves to the right state instead of the citing one.
    """
    if not prefixes:
        return None
    alternation = "|".join(
        r"[\s.]*".join(re.escape(token) for token in prefix.split(" "))
        for prefix in sorted(prefixes, key=len, reverse=True)
    )
    return re.compile(
        r"\b(?P<prefix>" + alternation + r")[\s.]*"
        r"(?:" + _REFERENCE_CUE + r"\s*)?"
        r"(?P<number>" + _SECTION_NUMBER + r")",
        re.IGNORECASE,
    )
