"""STaRK-Prime SKB source adapter — the biomedical knowledge graph only.

Profile: `reference_catalog`.

Ingests the `snap-stanford/stark` Prime *semi-structured knowledge base*
(`skb/prime/processed.zip`): 10 entity types and 18 relation types over a
PrimeKG-derived biomedical graph. Nothing else. No network at run time.

THE GOLD PATH IS NOT DISABLED — IT IS ABSENT.
=============================================
This adapter is promoted from `deployments/okg-testbed/sources/stark_prime.py`
on the unmerged branch `codex/stark-prime-agentic-optimizer`, where the same
class ALSO read `qa/prime/stark_qa/stark_qa.csv` and, per question, emitted a
`stark_prime_query` node plus one `query -targets-> entity` edge for every id
in `answer_ids`, stamped `source_relation: "stark.gold_answer"`, behind a
constructor flag `include_queries: bool = True` — defaulting ON. That is the
right shape for an agentic-optimizer testbed, where the graph is allowed to
know the answers. It is corpus-destroying for an A/B, where the graph the okg
arm reads must not contain the mapping the arm is being scored on.

The flag is not re-exported with a safer default. A default can be flipped
back by a caller, an env var, or a copy-paste; a codepath that does not exist
cannot. This module contains no reader for `stark_qa.csv`, no query subtype,
no `targets` archetype, and no expression that touches `answer_ids`. It reads
the six files of `skb/prime/processed/` and no others:

    node_type_dict.pkl    entity-type id -> released type name
    edge_type_dict.pkl    relation id    -> released relation name
    node_types.pt         per-entity type id
    edge_index.pt         2 x E endpoint indices
    edge_types.pt         per-edge-row relation id
    node_info.pkl         per-entity name / source / details

On top of that absence the adapter is FAIL-CLOSED against its own snapshot:
`_refuse_gold_artifacts` walks the snapshot root and raises if any
gold-bearing STaRK artifact (`stark_qa.csv`, the human-generated eval table,
or a `split/*.index` line-list) is present, because an operator pointing the
vertical at the download cache instead of a purpose-built snapshot would
otherwise mount the answer key read-only into every arm. The benchmark
adapter carries the same names in its `forbidden_names`; this is the second,
independent gate.

COMPOSITION IS DETERMINED BY THE RELEASE
========================================
Subtypes are derived from `node_type_dict.pkl` and edge types from
`edge_type_dict.pkl` by normalising the released names, never from a
hand-written list. The declared sets below are a DRIFT GUARD, not the
derivation: if a future release renames, adds or drops a type, the derived
set stops matching and the run raises naming the difference, instead of
quietly minting a subtype no ontology module declares (which the narrowing
catalog would then reject one edge at a time) or falling back to a generic
`stark_prime_entity` bucket, as the promoted-from code did.

MEASURED ON THE 2023-12-29 RELEASE (sha256 f6f265f6…, 28,013,696 bytes):
129,375 entities, 8,100,498 edge rows, 56 distinct
(src_type, relation, dst_type) triples, 0 self-loops, 0 isolated entities,
minimum degree 2. Every directed row's reverse is also present — 8,100,498
rows over 4,050,249 undirected relations, checked exhaustively rather than
sampled — so the paper's "8,100,498 relations" counts each biological
relation once per direction. The rows are emitted as the release states
them, both directions, because that is what the release says; the
symmetry is recorded here so nobody reads the edge count as a relation
count.

STRANDED ENTITIES. The full release strands none, so `no_relations_reason`
has population 0 on a complete build and is exercised only under
`max_entities`. It is computed from a degree census over the WHOLE
`edge_index` taken BEFORE emission — from what the release declares, never
from whether an edge came out — so an entity that loses its edges to a
parsing regression is NOT stamped and fails the isolation gate, which is
the whole point of the rule.
"""

from __future__ import annotations

import hashlib
import json
import math
import pickle
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._stark_pt import read_int_tensor
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceHealth,
    SourceRun,
)

__all__ = [
    "GOLD_ARTIFACT_NAMES",
    "PINNED_PRIME_RELEASE",
    "PRIME_EDGE_ROW_COUNT",
    "PRIME_EDGE_TYPES",
    "PRIME_ENTITY_COUNT",
    "PRIME_RELEASE_TRIPLES",
    "PRIME_SUBTYPES",
    "PRIME_UNDIRECTED_RELATION_COUNT",
    "ReleaseExpectation",
    "StarkPrimeError",
    "StarkPrimeSource",
    "node_id_for",
]


class StarkPrimeError(SubstrateError):
    """The Prime release did not match what this adapter can ingest."""


DATASET = "stark-prime"

#: The pinned release's own size, measured over `node_types.pt` and
#: `edge_types.pt`. Stated ONCE here and imported by the benchmark adapter,
#: which needs the entity count to bound a ranked answer: STaRK's gold is a
#: set of indices into `0..PRIME_ENTITY_COUNT-1`, so an emitted id outside
#: that range is a malformed answer rather than a wrong one. A run whose
#: release disagrees raises via `_refuse_release_size`.
PRIME_ENTITY_COUNT = 129375
PRIME_EDGE_ROW_COUNT = 8100498

#: Undirected relations behind those rows: every directed row's reverse is
#: present, exhaustively checked, with 0 self-loops. Recorded so the edge
#: count is not mistaken for a relation count.
PRIME_UNDIRECTED_RELATION_COUNT = 4050249

#: The four processed files this adapter reads. A missing one is fatal.
REQUIRED_FILES = (
    "node_type_dict.pkl",
    "edge_type_dict.pkl",
    "node_types.pt",
    "edge_index.pt",
    "edge_types.pt",
    "node_info.pkl",
)

#: Gold-bearing STaRK artifacts. NONE of these may sit anywhere under the
#: snapshot root: `stark_qa.csv` carries `answer_ids` for EVERY row
#: including test (verified — the table has exactly three columns, id /
#: query / answer_ids, and there is no answer-free copy in the release),
#: and the split index files are the train/test separation itself, so a
#: snapshot holding them hands an arm both the questions and which of them
#: it will be scored on.
GOLD_ARTIFACT_NAMES = (
    "stark_qa.csv",
    "stark_qa_human_generated_eval.csv",
    "train.index",
    "val.index",
    "test.index",
    "test-0.1.index",
)

#: Subtypes the `stark_prime` ontology module declares, in the release's
#: own type order. DERIVED at run time from `node_type_dict.pkl`; this
#: tuple only says which derivation the module is built for.
PRIME_SUBTYPES = (
    "stark_prime_disease",
    "stark_prime_gene_protein",
    "stark_prime_molecular_function",
    "stark_prime_drug",
    "stark_prime_pathway",
    "stark_prime_anatomy",
    "stark_prime_effect_phenotype",
    "stark_prime_biological_process",
    "stark_prime_cellular_component",
    "stark_prime_exposure",
)

#: Edge types, likewise derived from `edge_type_dict.pkl` at run time.
PRIME_EDGE_TYPES = (
    "ppi",
    "carrier",
    "enzyme",
    "target",
    "transporter",
    "contraindication",
    "indication",
    "off_label_use",
    "synergistic_interaction",
    "associated_with",
    "parent_child",
    "phenotype_absent",
    "phenotype_present",
    "side_effect",
    "interacts_with",
    "linked_to",
    "expression_present",
    "expression_absent",
)

#: The 56 (src_subtype, edge_type, dst_subtype) triples the release states,
#: with the row count measured over the whole `edge_index` on the
#: 2023-12-29 release. This is the release FINGERPRINT: a complete run
#: asserts the triples it emitted are exactly these keys, so a release that
#: gained or lost a triple stops the build rather than publishing a graph
#: whose shape silently differs from the one the ontology declares.
PRIME_RELEASE_TRIPLES: dict[tuple[str, str, str], int] = {
    ("stark_prime_drug", "synergistic_interaction", "stark_prime_drug"): 2672628,
    ("stark_prime_gene_protein", "expression_present", "stark_prime_anatomy"): 1518203,
    ("stark_prime_anatomy", "expression_present", "stark_prime_gene_protein"): 1518203,
    ("stark_prime_gene_protein", "ppi", "stark_prime_gene_protein"): 642150,
    ("stark_prime_disease", "phenotype_present", "stark_prime_effect_phenotype"): 150317,
    ("stark_prime_effect_phenotype", "phenotype_present", "stark_prime_disease"): 150317,
    ("stark_prime_gene_protein", "interacts_with", "stark_prime_biological_process"): 144805,
    ("stark_prime_biological_process", "interacts_with", "stark_prime_gene_protein"): 144805,
    ("stark_prime_biological_process", "parent_child", "stark_prime_biological_process"): 105772,
    ("stark_prime_gene_protein", "interacts_with", "stark_prime_cellular_component"): 83402,
    ("stark_prime_cellular_component", "interacts_with", "stark_prime_gene_protein"): 83402,
    ("stark_prime_gene_protein", "associated_with", "stark_prime_disease"): 80411,
    ("stark_prime_disease", "associated_with", "stark_prime_gene_protein"): 80411,
    ("stark_prime_gene_protein", "interacts_with", "stark_prime_molecular_function"): 69530,
    ("stark_prime_molecular_function", "interacts_with", "stark_prime_gene_protein"): 69530,
    ("stark_prime_drug", "side_effect", "stark_prime_effect_phenotype"): 64784,
    ("stark_prime_effect_phenotype", "side_effect", "stark_prime_drug"): 64784,
    ("stark_prime_disease", "parent_child", "stark_prime_disease"): 64388,
    ("stark_prime_gene_protein", "interacts_with", "stark_prime_pathway"): 42646,
    ("stark_prime_pathway", "interacts_with", "stark_prime_gene_protein"): 42646,
    ("stark_prime_effect_phenotype", "parent_child", "stark_prime_effect_phenotype"): 37472,
    ("stark_prime_drug", "contraindication", "stark_prime_disease"): 30675,
    ("stark_prime_disease", "contraindication", "stark_prime_drug"): 30675,
    ("stark_prime_anatomy", "parent_child", "stark_prime_anatomy"): 28064,
    ("stark_prime_molecular_function", "parent_child", "stark_prime_molecular_function"): 27148,
    ("stark_prime_gene_protein", "expression_absent", "stark_prime_anatomy"): 19887,
    ("stark_prime_anatomy", "expression_absent", "stark_prime_gene_protein"): 19887,
    ("stark_prime_drug", "target", "stark_prime_gene_protein"): 16380,
    ("stark_prime_gene_protein", "target", "stark_prime_drug"): 16380,
    ("stark_prime_cellular_component", "parent_child", "stark_prime_cellular_component"): 9690,
    ("stark_prime_drug", "indication", "stark_prime_disease"): 9388,
    ("stark_prime_disease", "indication", "stark_prime_drug"): 9388,
    ("stark_prime_drug", "enzyme", "stark_prime_gene_protein"): 5317,
    ("stark_prime_gene_protein", "enzyme", "stark_prime_drug"): 5317,
    ("stark_prime_pathway", "parent_child", "stark_prime_pathway"): 5070,
    ("stark_prime_exposure", "parent_child", "stark_prime_exposure"): 4140,
    ("stark_prime_gene_protein", "associated_with", "stark_prime_effect_phenotype"): 3330,
    ("stark_prime_effect_phenotype", "associated_with", "stark_prime_gene_protein"): 3330,
    ("stark_prime_drug", "transporter", "stark_prime_gene_protein"): 3092,
    ("stark_prime_gene_protein", "transporter", "stark_prime_drug"): 3092,
    ("stark_prime_drug", "off_label_use", "stark_prime_disease"): 2568,
    ("stark_prime_disease", "off_label_use", "stark_prime_drug"): 2568,
    ("stark_prime_exposure", "linked_to", "stark_prime_disease"): 2304,
    ("stark_prime_disease", "linked_to", "stark_prime_exposure"): 2304,
    ("stark_prime_exposure", "interacts_with", "stark_prime_biological_process"): 1625,
    ("stark_prime_biological_process", "interacts_with", "stark_prime_exposure"): 1625,
    ("stark_prime_exposure", "interacts_with", "stark_prime_gene_protein"): 1212,
    ("stark_prime_gene_protein", "interacts_with", "stark_prime_exposure"): 1212,
    ("stark_prime_disease", "phenotype_absent", "stark_prime_effect_phenotype"): 1193,
    ("stark_prime_effect_phenotype", "phenotype_absent", "stark_prime_disease"): 1193,
    ("stark_prime_drug", "carrier", "stark_prime_gene_protein"): 864,
    ("stark_prime_gene_protein", "carrier", "stark_prime_drug"): 864,
    ("stark_prime_exposure", "interacts_with", "stark_prime_molecular_function"): 45,
    ("stark_prime_molecular_function", "interacts_with", "stark_prime_exposure"): 45,
    ("stark_prime_exposure", "interacts_with", "stark_prime_cellular_component"): 10,
    ("stark_prime_cellular_component", "interacts_with", "stark_prime_exposure"): 10,
}


@dataclass(frozen=True)
class ReleaseExpectation:
    """Which STaRK-Prime release a run is allowed to ingest.

    Every guard in this adapter compares the release it just read against
    ONE of these, so "what we expect" is a named, printable object rather
    than a scatter of module constants a test would have to monkeypatch.
    A fixture states its own expectation explicitly; there is no mode in
    which the checks are skipped, and no default that silently widens.
    """

    label: str
    entity_count: int
    edge_row_count: int
    subtypes: frozenset[str]
    edge_types: frozenset[str]
    triples: frozenset[tuple[str, str, str]]


#: The release every pinned number in this module, in the `stark_prime`
#: ontology, and in the vertical's contract floors was measured on:
#: `skb/prime/processed.zip`, 28,013,696 bytes, sha256
#: f6f265f60b761784fb7c2f052359f9cd3bf9ea20c4a307865f62ce173b294dfc,
#: file mtimes 2023-12-29.
PINNED_PRIME_RELEASE = ReleaseExpectation(
    label="snap-stanford/stark skb/prime/processed.zip (2023-12-29)",
    entity_count=PRIME_ENTITY_COUNT,
    edge_row_count=PRIME_EDGE_ROW_COUNT,
    subtypes=frozenset(PRIME_SUBTYPES),
    edge_types=frozenset(PRIME_EDGE_TYPES),
    triples=frozenset(PRIME_RELEASE_TRIPLES),
)

#: Per-entity `text` bound (chars). The released `details` blocks run to
#: several kilobytes for a well-annotated disease; an unbounded blob in
#: node attrs is a substrate problem, not a fidelity gain.
TEXT_MAX_CHARS = 4000

#: Per-`details`-value bound (chars), applied before the text is joined.
DETAIL_VALUE_MAX_CHARS = 2000

#: `details` keys carried verbatim onto the node in addition to `text`.
#: Deliberately the identifier-ish ones: the prose lives in `text`, and
#: duplicating kilobytes of it into structured attrs helps nothing.
_DETAIL_ID_KEYS = ("mondo_id", "mondo_name", "group_id_bert", "stId",
                   "dbId", "atc_1", "atc_2", "atc_3", "atc_4", "category",
                   "state", "alias", "_id")

#: `details` keys whose values make up a node's retrieval text, in the
#: order they are joined. Anything not listed is skipped, so a release
#: that adds a field does not silently change every node's text.
_DETAIL_TEXT_KEYS = (
    # disease
    "mondo_definition", "umls_description", "orphanet_definition",
    "orphanet_prevalence", "orphanet_epidemiology",
    "orphanet_clinical_description", "orphanet_management_and_treatment",
    "mayo_symptoms", "mayo_causes", "mayo_risk_factors",
    "mayo_complications", "mayo_prevention", "mayo_see_doc",
    # drug
    "description", "half_life", "indication", "mechanism_of_action",
    "protein_binding", "pharmacodynamics",
    # gene/protein
    "summary",
    # pathway
    "displayName", "summation", "speciesName",
)

_NON_WORD = re.compile(r"[^a-z0-9]+")


def _normalise(value: str) -> str:
    """Released type/relation name -> substrate token. `gene/protein` ->
    `gene_protein`, `off-label use` -> `off_label_use`."""
    token = _NON_WORD.sub("_", str(value).strip().lower()).strip("_")
    if not token:
        raise StarkPrimeError(
            f"the release names a type or relation {value!r} that normalises "
            "to an empty token"
        )
    return token


@dataclass(frozen=True)
class _Release:
    """Everything read off the four processed files, checked."""

    subtype_by_type_id: dict[int, str]
    edge_type_by_rel_id: dict[int, str]
    node_type_ids: list[int]
    src: list[int]
    dst: list[int]
    rel: list[int]
    node_info: dict[int, dict[str, Any]]

    @property
    def node_count(self) -> int:
        return len(self.node_type_ids)

    @property
    def edge_row_count(self) -> int:
        return len(self.rel)


class StarkPrimeSource:
    """SourceAdapter for the STaRK-Prime SKB. Graph only, never the QA."""

    name = "stark-prime"
    profile = "reference_catalog"

    def __init__(
        self,
        *,
        processed_dir: str,
        snapshot_root: str | None = None,
        max_entities: int | None = None,
        expect: ReleaseExpectation = PINNED_PRIME_RELEASE,
    ) -> None:
        """`processed_dir` holds the unpacked `skb/prime/processed/` files.
        `snapshot_root` is the directory scanned for gold-bearing artifacts
        (defaults to `processed_dir`'s grandparent, i.e. the snapshot the
        arms mount). `max_entities` bounds the build to the first N entity
        indices and their induced subgraph; a bounded run reports
        `completed_scope=False` and stamps its bound, so it can never be
        read as the full release. `expect` names the release this run may
        ingest — every drift guard compares against it, and a caller
        wanting a different release must say which."""
        self.processed_dir = Path(processed_dir)
        self.expect = expect
        if not snapshot_root:
            # REQUIRED, because the gate that reads it is a leakage refusal
            # and a leakage refusal must not guess its own scope. The former
            # default was `processed_dir.parent.parent`, which for the shipped
            # layout `<snap>/skb/prime/processed` resolves to `<snap>/skb` --
            # one level short of the snapshot the arms actually mount. A
            # `stark_qa.csv` sitting at `<snap>/` was therefore outside the
            # scanned tree, and the gate reported the snapshot clean. Every
            # in-tree caller already passes this explicitly; the default
            # existed only to be silently wrong.
            raise StarkPrimeError(
                "snapshot_root is required: it is the root the gold-artifact "
                "refusal scans, and it must name the snapshot the arms mount "
                "(the directory CONTAINING skb/), not a subdirectory of it"
            )
        self.snapshot_root = Path(snapshot_root)
        if max_entities is not None and int(max_entities) <= 0:
            raise StarkPrimeError(
                f"max_entities must be a positive integer, got {max_entities!r}"
            )
        self.max_entities = None if max_entities is None else int(max_entities)
        #: populated by `run`; the operator-facing census of the run.
        self.last_run_stats: dict[str, Any] = {}
        #: the release is a pinned, immutable artifact and the probe reads
        #: every byte of it, so an unchanged verdict is exact.
        self.probe_short_circuit_safe = True

    # -- change probe ---------------------------------------------------
    def _build_change_probe(self) -> Any:
        """Content-identity probe over the six processed files.

        Every byte is hashed — no `max_bytes_per_item` cap — because the
        edge tensors are exactly where a truncated read would permit a
        false-unchanged verdict: a change in the last megabyte of
        `edge_index.pt` is 130,000 relations this corpus would silently
        keep serving from the previous generation."""
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, Path]]:
            return [
                (name, self.processed_dir / name)
                for name in REQUIRED_FILES
                if (self.processed_dir / name).is_file()
            ]

        return ContentHashProbe(
            content_items=_items,
            config={
                # `max_entities` changes WHICH facts the same bytes
                # produce, so an unchanged token across a bound change
                # would prove nothing.
                "max_entities": self.max_entities,
                "expect": self.expect.label,
                "processed_dir": str(self.processed_dir),
            },
            emit_targets=emission_identity_targets(
                self.name, sys.modules[__name__]
            ),
        )

    # -- run ------------------------------------------------------------
    def run(
        self,
        run_id: str,
        *,
        mode: str = "release_new",
        sync_scope: Any = None,
    ) -> SourceRun:
        del run_id, sync_scope
        self._refuse_gold_artifacts()
        release = self._load_release()
        selected, degree, kept_degree = self._select(release)
        if selected is None:
            _refuse_release_isolates(release, degree)
        stats = self._census(release, selected, degree, kept_degree)
        self.last_run_stats = stats
        content_hash = _digest(stats)
        complete = self.max_entities is None
        return SourceRun(
            facts=self._emit(release, selected, degree, kept_degree),
            completed_scope=complete,
            next_cursor={
                "dataset": DATASET,
                "mode": "release" if complete else "bounded_release",
                "max_entities": self.max_entities,
                "content_hash": content_hash,
                "entity_count": stats["entities_emitted"],
                "edge_row_count": stats["edge_rows_emitted"],
            },
            run_mode=mode,
            health=SourceHealth(
                status="ok",
                mode="cache" if complete else "mixed",
                record_count=stats["entities_emitted"],
                cache_path=str(self.processed_dir),
                content_hash=content_hash,
                reason=None if complete else (
                    f"bounded build: first {self.max_entities} entity indices "
                    "and their induced subgraph; NOT the full release"
                ),
            ),
        )

    # -- gold refusal ---------------------------------------------------
    def _refuse_gold_artifacts(self) -> None:
        """Refuse a snapshot that carries any part of the QA release.

        Fail-closed and independent of the benchmark adapter's own
        `refuse_gold_in_snapshot`: an operator who points the vertical at
        the download cache would otherwise mount `stark_qa.csv` — which
        carries `answer_ids` for every row, test included — read-only into
        every arm."""
        root = self.snapshot_root
        if not root.is_dir():
            # RAISE, never return. This is a leakage refusal: returning early
            # reports "no gold found" for a root the gate could not read,
            # which is the same sentence a clean snapshot produces. An
            # unreadable root is an unknown, and an unknown here must not be
            # spelled the same way as a pass.
            raise StarkPrimeError(
                f"snapshot root {root} is not a directory, so the gold-artifact "
                "refusal could not run. Refusing rather than reporting a "
                "snapshot clean on the strength of being unable to look at it"
            )
        hits = sorted(
            str(path.relative_to(root))
            for name in GOLD_ARTIFACT_NAMES
            for path in root.rglob(name)
        )
        if hits:
            raise StarkPrimeError(
                f"snapshot root {root} carries STaRK gold-bearing artifacts "
                f"{hits}. `stark_qa.csv` holds answer_ids for EVERY row "
                "including test, and the split index files are the "
                "train/test separation itself. Build a snapshot holding "
                "skb/prime/processed/ alone; never point a corpus at the "
                "dataset cache"
            )

    # -- loading --------------------------------------------------------
    def _load_release(self) -> _Release:
        missing = [
            name for name in REQUIRED_FILES
            if not (self.processed_dir / name).is_file()
        ]
        if missing:
            raise StarkPrimeError(
                f"STaRK-Prime processed dir {self.processed_dir} is missing "
                f"{missing}; unpack skb/prime/processed.zip there"
            )
        node_type_dict = _load_pickle(
            self.processed_dir / "node_type_dict.pkl", "node_type_dict")
        edge_type_dict = _load_pickle(
            self.processed_dir / "edge_type_dict.pkl", "edge_type_dict")
        node_info = _load_pickle(
            self.processed_dir / "node_info.pkl", "node_info")

        subtype_by_type_id = {
            int(k): f"stark_prime_{_normalise(v)}"
            for k, v in node_type_dict.items()
        }
        edge_type_by_rel_id = {
            int(k): _normalise(v) for k, v in edge_type_dict.items()
        }
        _refuse_drift(
            "entity subtype", set(subtype_by_type_id.values()),
            set(self.expect.subtypes), self.expect)
        _refuse_drift(
            "edge type", set(edge_type_by_rel_id.values()),
            set(self.expect.edge_types), self.expect)

        node_shape, node_type_ids = read_int_tensor(
            self.processed_dir / "node_types.pt")
        edge_shape, edge_index = read_int_tensor(
            self.processed_dir / "edge_index.pt")
        rel_shape, rel = read_int_tensor(
            self.processed_dir / "edge_types.pt")
        if len(node_shape) != 1:
            raise StarkPrimeError(
                f"node_types.pt has shape {node_shape}; expected 1-D")
        if len(edge_shape) != 2 or edge_shape[0] != 2:
            raise StarkPrimeError(
                f"edge_index.pt has shape {edge_shape}; expected (2, E)")
        if len(rel_shape) != 1 or rel_shape[0] != edge_shape[1]:
            raise StarkPrimeError(
                f"edge_types.pt has shape {rel_shape} but edge_index.pt "
                f"declares {edge_shape[1]} edges; the two must agree"
            )
        if len(node_info) != node_shape[0]:
            raise StarkPrimeError(
                f"node_info.pkl holds {len(node_info)} entities but "
                f"node_types.pt declares {node_shape[0]}"
            )
        edges = edge_shape[1]
        release = _Release(
            subtype_by_type_id=subtype_by_type_id,
            edge_type_by_rel_id=edge_type_by_rel_id,
            node_type_ids=node_type_ids,
            src=edge_index[:edges],
            dst=edge_index[edges:],
            rel=rel,
            node_info={int(k): dict(v) for k, v in node_info.items()},
        )
        _check_dense_index(release)
        _refuse_release_size(release, self.expect)
        return release

    # -- selection + census --------------------------------------------
    def _select(
        self, release: _Release
    ) -> tuple[set[int] | None, Counter, Counter]:
        """Return (selected entity indices or None for all, DECLARED degree
        per entity, degree WITHIN the selection).

        The declared degree is counted over the WHOLE `edge_index` before
        anything is filtered; the kept degree counts only what survives the
        bound. On a full build the two are identical by construction, which
        is stated plainly rather than dressed up: both read the same array,
        so a stamp derived from the declared degree could NOT discriminate
        an entity the release strands from one this reader stranded by
        losing rows. That is why `_refuse_release_isolates` refuses an
        isolate on a full build outright instead of stamping it — the
        release states minimum degree 2 over all 129,375 entities, so there
        is no admissible isolate to excuse. The stamp exists only for
        `max_entities`, where the exclusion is decided by a bound this
        adapter was handed and not by anything it parsed."""
        declared: Counter = Counter()
        for i in range(release.edge_row_count):
            declared[release.src[i]] += 1
            declared[release.dst[i]] += 1
        if self.max_entities is None:
            return None, declared, declared
        selected = set(range(min(self.max_entities, release.node_count)))
        kept: Counter = Counter()
        for i in range(release.edge_row_count):
            s, d = release.src[i], release.dst[i]
            if s in selected and d in selected:
                kept[s] += 1
                kept[d] += 1
        return selected, declared, kept

    def _census(
        self,
        release: _Release,
        selected: set[int] | None,
        declared: Counter,
        kept: Counter,
    ) -> dict[str, Any]:
        entities = (
            release.node_count if selected is None else len(selected))
        by_subtype: Counter = Counter()
        stranded: Counter = Counter()
        for index in (range(release.node_count) if selected is None
                      else sorted(selected)):
            subtype = release.subtype_by_type_id[release.node_type_ids[index]]
            by_subtype[subtype] += 1
            if kept[index] == 0:
                stranded[_strand_reason(declared[index])] += 1
        triples: Counter = Counter()
        rows = 0
        for i in range(release.edge_row_count):
            s, d = release.src[i], release.dst[i]
            if selected is not None and (s not in selected or d not in selected):
                continue
            triples[(
                release.subtype_by_type_id[release.node_type_ids[s]],
                release.edge_type_by_rel_id[release.rel[i]],
                release.subtype_by_type_id[release.node_type_ids[d]],
            )] += 1
            rows += 1
        emitted_triples = set(triples)
        declared_triples = set(self.expect.triples)
        unknown = sorted(emitted_triples - declared_triples)
        if unknown:
            raise StarkPrimeError(
                f"the release realises {len(unknown)} "
                "(src_subtype, edge_type, dst_subtype) triple(s) that "
                f"{self.expect.label} does not declare: {unknown[:8]}. The "
                "stark_prime ontology declares one narrowing per declared "
                "triple, so an undeclared one would be rejected at insert "
                "edge by edge with no statement of why; regenerate both "
                "before publishing"
            )
        if selected is None and emitted_triples != declared_triples:
            raise StarkPrimeError(
                f"the full release is missing triples {self.expect.label} "
                f"declares: {sorted(declared_triples - emitted_triples)[:8]}"
            )
        return {
            "release_label": self.expect.label,
            "release_entities": release.node_count,
            "release_edge_rows": release.edge_row_count,
            "max_entities": self.max_entities,
            "entities_emitted": entities,
            "edge_rows_emitted": rows,
            "entities_by_subtype": dict(sorted(by_subtype.items())),
            "edge_rows_by_triple": {
                "|".join(key): count
                for key, count in sorted(triples.items())
            },
            "stranded_entities_by_reason": dict(sorted(stranded.items())),
            "distinct_triples": len(emitted_triples),
        }

    # -- emission -------------------------------------------------------
    def _emit(
        self,
        release: _Release,
        selected: set[int] | None,
        declared: Counter,
        kept: Counter,
    ) -> Iterator[NodeFact | EdgeFact]:
        indices = (
            range(release.node_count) if selected is None
            else sorted(selected)
        )
        for index in indices:
            yield _node_fact(release, index, declared[index], kept[index])
        for i in range(release.edge_row_count):
            s, d = release.src[i], release.dst[i]
            if selected is not None and (
                s not in selected or d not in selected
            ):
                continue
            yield _edge_fact(release, s, d, release.rel[i])


# -- helpers -------------------------------------------------------------


def node_id_for(entity_index: int) -> str:
    """The graph node id for a STaRK entity index.

    STaRK's answer unit is a set of entity INDICES, so a gold id resolves
    to a node id by formatting alone — no lookup, no type prefix, nothing
    the arm has to already know."""
    return f"stark_prime:entity:{int(entity_index)}"


def _refuse_release_size(
    release: _Release, expect: ReleaseExpectation
) -> None:
    """The release must be the one every pinned number here was measured on.

    The contract floors, the 56-triple fingerprint and the benchmark
    adapter's answer-id bound are all keyed to 129,375 entities /
    8,100,498 edge rows. A release that quietly changed size under a
    running A/B would move every one of them at once, so it stops here
    with instructions rather than publishing a differently-shaped corpus
    under the same name."""
    if (release.node_count == expect.entity_count
            and release.edge_row_count == expect.edge_row_count):
        return
    raise StarkPrimeError(
        f"this Prime release holds {release.node_count} entities and "
        f"{release.edge_row_count} edge rows; {expect.label} declares "
        f"{expect.entity_count} / {expect.edge_row_count}. Re-measure the "
        "release, regenerate the stark_prime ontology module and "
        "PRIME_RELEASE_TRIPLES, and re-derive the vertical's contract floors "
        "before publishing it"
    )


def _refuse_release_isolates(release: _Release, declared: Counter) -> None:
    """A full build may not publish an isolated entity, at all.

    Measured over the pinned release: 129,375 entities, minimum degree 2,
    zero isolates. So on a complete build an entity with no incident edge
    is not an upstream fact to be excused with a stamp — it is edge rows
    this reader failed to read, and the honest response is to stop. Held
    deliberately WITHOUT an escape hatch: a stamp here would be derived
    from the same `edge_index` the emission walks, so it could never tell
    the two apart, and a rule that cannot discriminate is decoration."""
    isolated = [
        index for index in range(release.node_count) if declared[index] == 0
    ]
    if not isolated:
        return
    raise StarkPrimeError(
        f"{len(isolated)} of {release.node_count} entities have no incident "
        f"edge (first: {isolated[:8]}). The pinned release declares minimum "
        "degree 2 and zero isolates, so this means edge rows were lost "
        "rather than that the release strands them. No stamp is emitted for "
        "this case on purpose: it would be derived from the same edge_index "
        "the emission walks, and so could not tell a lost row from an "
        "upstream gap"
    )


def _strand_reason(declared_degree: int) -> str:
    """Why a BOUNDED build left an entity with no edge.

    Reached only under `max_entities`. `declared_degree` is the entity's
    degree in the whole release; a positive one means every incident edge
    left the bounded subgraph, which is a property of the bound this
    adapter was handed and not of anything it parsed. A zero one cannot
    occur on a bounded build either, because `_refuse_release_isolates`
    covers the full-release case and the bound only removes edges — it is
    kept as a distinct value so an unexpected release change reports as
    itself rather than as the bound's doing."""
    if declared_degree == 0:
        return "unreferenced_in_release"
    return "all_declared_edges_leave_the_bounded_subgraph"


def _node_fact(
    release: _Release, index: int, declared_degree: int, kept_degree: int
) -> NodeFact:
    raw = release.node_info.get(index)
    if raw is None:
        raise StarkPrimeError(
            f"entity index {index} is declared by node_types.pt but absent "
            "from node_info.pkl"
        )
    subtype = release.subtype_by_type_id[release.node_type_ids[index]]
    released_type = str(raw.get("type") or "")
    if released_type and f"stark_prime_{_normalise(released_type)}" != subtype:
        raise StarkPrimeError(
            f"entity {index}: node_info says type {released_type!r} but "
            f"node_types.pt says {subtype!r}; the release disagrees with "
            "itself"
        )
    name = str(raw.get("name") or "").strip()
    if not name:
        raise StarkPrimeError(f"entity {index} has no name in node_info.pkl")
    raw_details = raw.get("details")
    if raw_details is not None and not isinstance(raw_details, dict):
        raise StarkPrimeError(
            f"entity {index}: node_info 'details' is "
            f"{type(raw_details).__name__}, expected a mapping or nothing"
        )
    details: dict[str, Any] = dict(raw_details or {})
    text, truncated = _entity_text(name, details)
    attrs: dict[str, Any] = {
        "entity_index": int(index),
        "name": name,
        "stark_node_type": released_type or subtype,
        "primekg_id": str(raw.get("id") or ""),
        "source_db": str(raw.get("source") or ""),
        "text": text,
        "text_truncated": truncated,
    }
    for key in _DETAIL_ID_KEYS:
        value = details.get(key)
        scalar = _jsonable_scalar(value)
        if scalar not in (None, "", []):
            attrs[f"detail_{key}"] = scalar
    if kept_degree == 0:
        attrs["no_relations_reason"] = _strand_reason(declared_degree)
        attrs["declared_degree"] = int(declared_degree)
    return NodeFact(
        node_id=node_id_for(index),
        subtype=subtype,
        attrs=attrs,
        source_record_id={
            "dataset": DATASET,
            "record_type": "entity",
            "entity_index": int(index),
        },
        source_revision={"content_hash": _digest(attrs)},
    )


def _edge_fact(release: _Release, src: int, dst: int, rel_id: int) -> EdgeFact:
    edge_type = release.edge_type_by_rel_id[rel_id]
    return EdgeFact(
        src=node_id_for(src),
        dst=node_id_for(dst),
        edge_type=edge_type,
        attrs={"released_relation": edge_type},
        provenance="explicit",
        confidence=1.0,
        source_record_id={
            "dataset": DATASET,
            "record_type": "relation",
            "src": int(src),
            "edge_type": edge_type,
            "dst": int(dst),
        },
        source_revision={
            "content_hash": _digest(
                {"src": int(src), "edge_type": edge_type, "dst": int(dst)}
            )
        },
    )


def _entity_text(name: str, details: dict[str, Any]) -> tuple[str, bool]:
    """Name plus the release's own prose, in a fixed key order.

    The key order is declared (`_DETAIL_TEXT_KEYS`), so a release that adds
    a `details` field does not silently rewrite every entity's retrieval
    text — which would change the corpus under a benchmark run without
    anything saying so."""
    parts = [name]
    for key in _DETAIL_TEXT_KEYS:
        value = details.get(key)
        if value is None:
            continue
        rendered = _render_detail(value)
        if not rendered:
            continue
        if len(rendered) > DETAIL_VALUE_MAX_CHARS:
            rendered = rendered[:DETAIL_VALUE_MAX_CHARS]
        parts.append(f"{key}: {rendered}")
    text = "\n".join(parts)
    if len(text) > TEXT_MAX_CHARS:
        return text[:TEXT_MAX_CHARS], True
    return text, False


def _render_detail(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, (list, tuple)):
        return ", ".join(
            part for part in (_render_detail(v) for v in value) if part
        )
    if isinstance(value, dict):
        return ", ".join(
            f"{k}: {part}"
            for k, v in sorted(value.items())
            for part in (_render_detail(v),)
            if part
        )
    return ""


def _jsonable_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, str) and len(value) > DETAIL_VALUE_MAX_CHARS:
            return value[:DETAIL_VALUE_MAX_CHARS]
        # isfinite, not isnan: the guard's job is that no non-finite float
        # reaches an attr, and +/-inf is the half that was getting through.
        # `json.dumps(float("inf"))` emits a bare `Infinity`, which is not
        # valid JSON — so an inf in the release would have produced an attrs
        # blob no conforming parser can read, silently, at write time rather
        # than where anyone would look for it.
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    if isinstance(value, (list, tuple)):
        return [
            item for item in (_jsonable_scalar(v) for v in value)
            if item not in (None, "")
        ]
    return None


def _refuse_drift(
    label: str,
    derived: set[str],
    declared: set[str],
    expect: ReleaseExpectation,
) -> None:
    if derived == declared:
        return
    raise StarkPrimeError(
        f"the release's {label} set does not match {expect.label}. Only in "
        f"the release: {sorted(derived - declared)}; only in the "
        f"expectation: {sorted(declared - derived)}. Regenerate the ontology "
        "module and PRIME_RELEASE_TRIPLES against the new release; nothing "
        "is inferred and no unknown type falls back to a generic bucket"
    )


def _check_dense_index(release: _Release) -> None:
    expected = set(range(release.node_count))
    got = set(release.node_info)
    if got != expected:
        missing = sorted(expected - got)[:8]
        extra = sorted(got - expected)[:8]
        raise StarkPrimeError(
            "node_info.pkl is not keyed by a dense 0..N-1 entity index "
            f"(missing {missing}, unexpected {extra}); the QA answer ids "
            "index into exactly that space, so a non-dense keying would "
            "silently mis-resolve every gold answer"
        )
    unknown_types = sorted(
        {t for t in release.node_type_ids} - set(release.subtype_by_type_id)
    )
    if unknown_types:
        raise StarkPrimeError(
            f"node_types.pt uses type ids {unknown_types} that "
            "node_type_dict.pkl does not name"
        )
    unknown_rels = sorted(
        {r for r in release.rel} - set(release.edge_type_by_rel_id)
    )
    if unknown_rels:
        raise StarkPrimeError(
            f"edge_types.pt uses relation ids {unknown_rels} that "
            "edge_type_dict.pkl does not name"
        )
    limit = release.node_count
    for endpoints, label in ((release.src, "src"), (release.dst, "dst")):
        if not endpoints:
            continue
        low, high = min(endpoints), max(endpoints)
        if low < 0 or high >= limit:
            raise StarkPrimeError(
                f"edge_index.pt names {label} entities in [{low}, {high}], "
                f"outside the 0..{limit - 1} entity index space"
            )


def _load_pickle(path: Path, label: str) -> dict:
    try:
        obj = pickle.loads(path.read_bytes())
    except (OSError, pickle.UnpicklingError, EOFError, AttributeError) as exc:
        raise StarkPrimeError(f"{label} at {path} is not readable: {exc}") from exc
    if not isinstance(obj, dict) or not obj:
        raise StarkPrimeError(
            f"{label} at {path} is {type(obj).__name__}, expected a non-empty dict"
        )
    return obj


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
