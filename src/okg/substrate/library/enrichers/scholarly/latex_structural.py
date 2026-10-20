"""LatexStructuralEnricher — deterministic LaTeX paper anatomy.

Reads Paper nodes that carry `attrs.source_tex_path` and have
not yet been decomposed (no `paper_contains_paper_section`
edges in `edges_live` rooted at the Paper). Parses the `.tex`
source, emits PaperSection / Paragraph / Figure / Table /
Algorithm / Equation / Theorem / Proof / Citation nodes plus
the intra-scholarly containment narrowings.

Provenance discipline (S-A1-R2, S-A1-R6):
  - Every emitted node and edge carries
    `attrs.provenance = derived_deterministic`.
  - Every emitted node carries `attrs.confidence = 1.0`.
  - Every emission carries `attrs.extractor_version`.

Failure discipline (S-A1-R2):
  - Catastrophic parse failure (file not UTF-8, unbalanced
    environments, missing file, etc.) emits exactly one
    `observation` NodeFact with
    `attrs.kind = extraction_error`,
    `attrs.extractor = latex_structural`,
    `attrs.source_paper_ref = <paper_node_id>`,
    `attrs.reason = <parser-reported-reason>`,
    `attrs.attempted_at = <ISO>`,
    `attrs.extractor_version = <version>`.
    Zero anatomy nodes emitted in this case.
  - The Paper node itself is never modified or removed.

The parser is regex-based and intentionally conservative: it
identifies LaTeX environments by their `\\begin{...}` /
`\\end{...}` markers, and sectioning by `\\section{}` /
`\\subsection{}` / `\\subsubsection{}` / `\\paragraph{}`. It
does NOT attempt to render math, expand macros, or interpret
semantic content. Pretty-printing of pseudocode is downstream
of this enricher.
"""

from __future__ import annotations

import dataclasses
import hashlib
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import psycopg

from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.derived_node_liveness import (
    resolve_declared_support_policy,
)
from okg.substrate.library.enrichers import DerivedSupportPolicy

__all__ = [
    "LatexStructuralEnricher",
    "LatexParseError",
    "parse_latex",
    "EXTRACTOR_NAME",
    "EXTRACTOR_VERSION",
    "ParsedAnatomy",
    "ParsedNode",
    "ParsedEdge",
]

logger = logging.getLogger(__name__)


EXTRACTOR_NAME = "latex_structural"
EXTRACTOR_VERSION = "1.0.0"


class LatexParseError(Exception):
    """Raised when LaTeX source cannot be parsed deterministically.

    The enricher catches this exception and emits an
    `extraction_error` Observation against the offending Paper.
    The Paper node itself is preserved.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclasses.dataclass(frozen=True)
class ParsedNode:
    """One anatomy node from the LaTeX parser."""

    node_id: str
    subtype: str
    attrs: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class ParsedEdge:
    """One anatomy edge from the LaTeX parser."""

    src: str
    dst: str
    edge_type: str
    attrs: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class ParsedAnatomy:
    """Bundle of nodes + edges produced by the LaTeX parser.

    The enricher pushes these into okg.{node,edge}_facts with
    the substrate-required provenance + dedupe_key attrs added.
    """

    nodes: list[ParsedNode]
    edges: list[ParsedEdge]
    n_sections: int
    n_paragraphs: int
    n_figures: int
    n_tables: int
    n_algorithms: int
    n_equations: int
    n_theorems: int
    n_proofs: int
    n_citations: int


# ---------------------------------------------------------------- #
# Pure-function parser. Tested standalone — no DB.                  #
# ---------------------------------------------------------------- #


_RE_SECTION = re.compile(
    r"\\(section|subsection|subsubsection|paragraph)\*?"
    r"(?:\[[^\]]*\])?\{([^}]*)\}",
)
_RE_BEGIN_ENV = re.compile(
    r"\\begin\{(figure|table|algorithm|theorem|lemma|proposition|"
    r"definition|corollary|proof|equation|align)\*?\}",
)
_RE_END_ENV = re.compile(
    r"\\end\{(figure|table|algorithm|theorem|lemma|proposition|"
    r"definition|corollary|proof|equation|align)\*?\}",
)
_RE_LABEL = re.compile(r"\\label\{([^}]+)\}")
_RE_CAPTION = re.compile(r"\\caption\{((?:[^{}]|\{[^{}]*\})*)\}", re.DOTALL)
_RE_CITE = re.compile(
    r"\\(?:cite|citep|citet|citealp|citealt|citeauthor|citeyear)"
    r"(?:\[[^\]]*\])?(?:\[[^\]]*\])?\{([^}]+)\}",
)
# Sectioning hierarchy: ordinal depth of each section command.
# `paragraph` is treated as a section at depth 4 (under
# subsubsection) so it can carry paragraphs as children; the LaTeX
# convention varies, but for graph-anatomy this gives consistent
# nesting.
_SECTION_DEPTH = {
    "section": 1,
    "subsection": 2,
    "subsubsection": 3,
    "paragraph": 4,
}


def _slug(text: str) -> str:
    """Kebab-cased slug capped at 60 chars."""
    cleaned = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return cleaned[:60] or "unnamed"


# Per dogfood-canary follow-up (2026-05-18): map the section title
# to the canonical section_kind values downstream A2 extractors
# expect (hypothesis_extractor scopes to {introduction, discussion,
# conclusion}). Conservative — unknown titles get `None`.
_SECTION_KIND_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^\s*(introduction|intro)\b", re.I), "introduction"),
    (re.compile(r"^\s*(related\s*work|prior\s*art|background)\b", re.I),
     "related_work"),
    (re.compile(r"^\s*(method(s|ology)?|approach|design|system)\b", re.I),
     "methods"),
    (re.compile(r"^\s*(experiments?|evaluation|results?|empirical)\b",
                re.I), "experiments"),
    (re.compile(r"^\s*(discussion|threats?\s*to\s*validity|limitations?)\b",
                re.I), "discussion"),
    (re.compile(r"^\s*(conclusion|conclusions|summary|future\s*work)\b",
                re.I), "conclusion"),
    (re.compile(r"^\s*(abstract)\b", re.I), "abstract"),
    (re.compile(r"^\s*(acknowledg(e?ments?))\b", re.I), "acknowledgments"),
    (re.compile(r"^\s*(references|bibliography)\b", re.I), "references"),
    (re.compile(r"^\s*(appendix|appendices)\b", re.I), "appendix"),
]


def _classify_section_kind(title: str) -> str | None:
    """Return one of the canonical section_kind values for the
    given title, or `None` if no pattern matches."""
    for pat, kind in _SECTION_KIND_PATTERNS:
        if pat.search(title or ""):
            return kind
    return None


def _short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def _balanced_environments(text: str) -> None:
    """Raise `LatexParseError` if environment begin/end counts mismatch."""
    begins: list[str] = []
    for m in _RE_BEGIN_ENV.finditer(text):
        begins.append(m.group(1))
    ends: list[str] = []
    for m in _RE_END_ENV.finditer(text):
        ends.append(m.group(1))
    # Counts per env-name must match.
    from collections import Counter
    diff = Counter(begins) - Counter(ends)
    extra_begins = list(diff.elements())
    diff2 = Counter(ends) - Counter(begins)
    extra_ends = list(diff2.elements())
    if extra_begins or extra_ends:
        raise LatexParseError(
            f"unbalanced_environments: extra begins={sorted(set(extra_begins))} "
            f"extra ends={sorted(set(extra_ends))}"
        )


def parse_latex(*, paper_id: str, source: str) -> ParsedAnatomy:
    """Deterministically decompose a LaTeX `.tex` body.

    Raises `LatexParseError` on catastrophic failure (unbalanced
    environments, etc.). Returns a `ParsedAnatomy` on success.

    Argument shape:
      - `paper_id`: canonical Paper node id (e.g.
        `paper:vision-paper`); becomes the `paper_ref` on every
        emitted child node.
      - `source`: UTF-8 LaTeX body (post-Path.read_text).

    Identity convention:
      - Section: `section:<paper_id>:<idx>` where idx is 0-indexed
        across the linear section order.
      - Each environment instance gets `<kind>:<paper_id>:<idx>`.
      - Citation: `cite:<paper_id>:<idx>:<short_hash(key)>`.

    Sectioning model: every sectioning command creates a
    PaperSection; depth-based parent_section_ref links produce a
    tree. Top-level (depth 1) sections attach to the Paper via
    `paper_contains_paper_section`; deeper sections attach to
    their nearest-ancestor section via
    `paper_section_contains_paper_section`.

    Paragraph emission: between consecutive `\\section{}` /
    `\\subsection{}` etc. (and inside no environment), paragraphs
    are blocks of non-empty text separated by `\\n\\n`. Each
    paragraph emits one Paragraph node. Citations inside a
    paragraph link via `paragraph_contains_citation`.
    """
    _balanced_environments(source)

    nodes: list[ParsedNode] = []
    edges: list[ParsedEdge] = []
    section_stack: list[tuple[int, str]] = []  # (depth, section_node_id)
    section_idx = 0
    fig_idx = 0
    tbl_idx = 0
    alg_idx = 0
    eqn_idx = 0
    thm_idx = 0
    prf_idx = 0
    cite_idx = 0
    para_idx = 0

    # Track theorem-proof pairs by adjacency: every \begin{proof}
    # links to the most-recently-emitted Theorem.
    last_theorem_id: Optional[str] = None

    counts = {
        "n_sections": 0, "n_paragraphs": 0, "n_figures": 0,
        "n_tables": 0, "n_algorithms": 0, "n_equations": 0,
        "n_theorems": 0, "n_proofs": 0, "n_citations": 0,
    }

    def _current_section() -> Optional[str]:
        return section_stack[-1][1] if section_stack else None

    def _emit_citations_in(*, text: str, container_id: str,
                           container_subtype: str) -> None:
        nonlocal cite_idx
        sec = _current_section() or f"paper:{paper_id}"
        for m in _RE_CITE.finditer(text):
            cite_keys = [k.strip() for k in m.group(1).split(",") if k.strip()]
            for key in cite_keys:
                cite_node_id = (
                    f"cite:{paper_id}:{cite_idx}:{_short_hash(key)}"
                )
                cite_idx += 1
                counts["n_citations"] += 1
                # Crude context window: ~80 chars around the match.
                lo = max(0, m.start() - 80)
                hi = min(len(text), m.end() + 80)
                ctx = text[lo:hi].replace("\n", " ").strip()
                nodes.append(ParsedNode(
                    node_id=cite_node_id,
                    subtype="citation",
                    attrs={
                        "citation_id": cite_node_id,
                        "citing_section_ref": sec.split(":", 1)[-1],
                        "context_text": ctx,
                        "cited_paper_ref": f"paper:{key}",
                    },
                ))
                edges.append(ParsedEdge(
                    src=container_id, dst=cite_node_id,
                    edge_type="contains",
                ))

    # Split source into "blocks" delimited by sectioning commands.
    # Use a linear scan with a cursor; for each scan position,
    # determine whether we're inside an environment or at top-level.
    pos = 0
    n = len(source)

    def _consume_until_end_env(start: int, env: str) -> tuple[int, str]:
        """Find the matching \\end{env}; return (next_pos, body)."""
        # Find the next matching end at the same nesting level.
        # Allow nested begin/end of the same env name.
        depth = 1
        i = start
        end_pat = re.compile(rf"\\end\{{{env}\*?\}}")
        begin_pat = re.compile(rf"\\begin\{{{env}\*?\}}")
        while i < n:
            m_end = end_pat.search(source, i)
            m_begin = begin_pat.search(source, i)
            if not m_end:
                raise LatexParseError(
                    f"unbalanced_environments: missing \\end{{{env}}}"
                )
            if m_begin and m_begin.start() < m_end.start():
                depth += 1
                i = m_begin.end()
                continue
            depth -= 1
            if depth == 0:
                body = source[start:m_end.start()]
                return m_end.end(), body
            i = m_end.end()
        raise LatexParseError(
            f"unbalanced_environments: missing \\end{{{env}}}"
        )

    while pos < n:
        # Find next event: section command or environment begin.
        m_sec = _RE_SECTION.search(source, pos)
        m_env = _RE_BEGIN_ENV.search(source, pos)
        next_events = [
            (m_sec.start(), "section", m_sec) if m_sec else None,
            (m_env.start(), "env", m_env) if m_env else None,
        ]
        next_events = [e for e in next_events if e is not None]
        if not next_events:
            # Trailing paragraphs in the last section (if any).
            tail_text = source[pos:].strip()
            if tail_text and section_stack:
                container = _current_section()
                if container is None:
                    break
                for chunk in _split_paragraphs(tail_text):
                    para_id = f"para:{paper_id}:{para_idx}"
                    para_idx += 1
                    counts["n_paragraphs"] += 1
                    nodes.append(ParsedNode(
                        node_id=para_id, subtype="paragraph",
                        attrs={
                            "paragraph_id": para_id,
                            "parent_section_ref": container.split(":", 1)[-1],
                            "ordinal": para_idx - 1,
                            "text": chunk,
                        },
                    ))
                    edges.append(ParsedEdge(
                        src=container, dst=para_id, edge_type="contains",
                    ))
                    _emit_citations_in(text=chunk, container_id=para_id,
                                       container_subtype="paragraph")
            break

        next_events.sort(key=lambda x: x[0])
        ev_pos, ev_kind, ev_m = next_events[0]

        # Inter-event text: paragraphs in the current section.
        inter = source[pos:ev_pos].strip()
        if inter and section_stack:
            container = _current_section()
            if container is not None:
                for chunk in _split_paragraphs(inter):
                    para_id = f"para:{paper_id}:{para_idx}"
                    para_idx += 1
                    counts["n_paragraphs"] += 1
                    nodes.append(ParsedNode(
                        node_id=para_id, subtype="paragraph",
                        attrs={
                            "paragraph_id": para_id,
                            "parent_section_ref": container.split(":", 1)[-1],
                            "ordinal": para_idx - 1,
                            "text": chunk,
                        },
                    ))
                    edges.append(ParsedEdge(
                        src=container, dst=para_id, edge_type="contains",
                    ))
                    _emit_citations_in(text=chunk, container_id=para_id,
                                       container_subtype="paragraph")

        if ev_kind == "section":
            kind = ev_m.group(1)
            title = ev_m.group(2).strip()
            depth = _SECTION_DEPTH[kind]
            # Pop section_stack to depth-1.
            while section_stack and section_stack[-1][0] >= depth:
                section_stack.pop()
            sec_id = f"section:{paper_id}:{section_idx}"
            section_idx += 1
            counts["n_sections"] += 1
            # Compute path: parent_path + "." + ordinal.
            if section_stack:
                parent_node = section_stack[-1][1]
                parent_path = next(
                    (n_.attrs["path"] for n_ in reversed(nodes)
                     if n_.node_id == parent_node), "§"
                )
                # Count siblings at this depth already.
                siblings_at_depth = sum(
                    1 for n_ in nodes
                    if n_.subtype == "paper_section"
                    and n_.attrs.get("parent_section_ref")
                        == parent_node.split(":", 1)[-1]
                )
                path = f"{parent_path}.{siblings_at_depth + 1}"
                parent_ref = parent_node.split(":", 1)[-1]
                edges.append(ParsedEdge(
                    src=parent_node, dst=sec_id, edge_type="contains",
                ))
            else:
                top_count = sum(
                    1 for n_ in nodes
                    if n_.subtype == "paper_section"
                    and not n_.attrs.get("parent_section_ref")
                )
                path = f"§{top_count + 1}"
                parent_ref = None
                edges.append(ParsedEdge(
                    src=f"paper:{paper_id}", dst=sec_id, edge_type="contains",
                ))
            # Per dogfood-canary follow-up (2026-05-18): classify
            # the section_kind from the title prefix so downstream
            # extractors (hypothesis_extractor scopes to intro /
            # discussion / conclusion) can find their inputs without
            # a second pass.
            sec_attrs = {
                "paper_section_id": sec_id,
                "paper_ref": f"paper:{paper_id}",
                "path": path,
                "ordinal": section_idx - 1,
                "title": title,
                "section_kind": _classify_section_kind(title),
            }
            if parent_ref:
                sec_attrs["parent_section_ref"] = parent_ref
            nodes.append(ParsedNode(
                node_id=sec_id, subtype="paper_section", attrs=sec_attrs,
            ))
            section_stack.append((depth, sec_id))
            pos = ev_m.end()
            continue

        # Environment.
        env_name = ev_m.group(1)
        body_start = ev_m.end()
        next_pos, body = _consume_until_end_env(body_start, env_name)

        sec_container = _current_section()
        if sec_container is None:
            # Environment before any section — attach to Paper directly
            # via a synthetic "preface" section.
            sec_id = f"section:{paper_id}:{section_idx}"
            section_idx += 1
            counts["n_sections"] += 1
            nodes.append(ParsedNode(
                node_id=sec_id, subtype="paper_section",
                attrs={
                    "paper_section_id": sec_id,
                    "paper_ref": f"paper:{paper_id}",
                    "path": "§0", "ordinal": section_idx - 1,
                    "title": "Front matter",
                    "section_kind": "front_matter",
                },
            ))
            edges.append(ParsedEdge(
                src=f"paper:{paper_id}", dst=sec_id, edge_type="contains",
            ))
            section_stack.append((1, sec_id))
            sec_container = sec_id

        label_m = _RE_LABEL.search(body)
        label = label_m.group(1) if label_m else None
        caption_m = _RE_CAPTION.search(body)
        caption = caption_m.group(1).strip() if caption_m else None
        sec_ref = sec_container.split(":", 1)[-1]

        if env_name == "figure":
            fig_id = f"fig:{paper_id}:{fig_idx}"
            fig_idx += 1
            counts["n_figures"] += 1
            nodes.append(ParsedNode(
                node_id=fig_id, subtype="figure",
                attrs={
                    "figure_id": fig_id,
                    "parent_section_ref": sec_ref,
                    "label": label or f"fig-{fig_idx}",
                    "ordinal": fig_idx - 1,
                    **({"caption": caption} if caption else {}),
                },
            ))
            edges.append(ParsedEdge(
                src=sec_container, dst=fig_id, edge_type="contains",
            ))
            _emit_citations_in(text=body, container_id=fig_id,
                               container_subtype="figure")
        elif env_name == "table":
            tbl_id = f"tbl:{paper_id}:{tbl_idx}"
            tbl_idx += 1
            counts["n_tables"] += 1
            nodes.append(ParsedNode(
                node_id=tbl_id, subtype="table",
                attrs={
                    "table_id": tbl_id,
                    "parent_section_ref": sec_ref,
                    "label": label or f"tbl-{tbl_idx}",
                    "ordinal": tbl_idx - 1,
                    **({"caption": caption} if caption else {}),
                },
            ))
            edges.append(ParsedEdge(
                src=sec_container, dst=tbl_id, edge_type="contains",
            ))
            _emit_citations_in(text=body, container_id=tbl_id,
                               container_subtype="table")
        elif env_name == "algorithm":
            alg_id = f"alg:{paper_id}:{alg_idx}"
            alg_idx += 1
            counts["n_algorithms"] += 1
            nodes.append(ParsedNode(
                node_id=alg_id, subtype="algorithm",
                attrs={
                    "algorithm_id": alg_id,
                    "parent_section_ref": sec_ref,
                    "label": label or f"alg-{alg_idx}",
                    "ordinal": alg_idx - 1,
                    **({"caption": caption} if caption else {}),
                    "pseudocode": body.strip(),
                },
            ))
            edges.append(ParsedEdge(
                src=sec_container, dst=alg_id, edge_type="contains",
            ))
        elif env_name in ("equation", "align"):
            eqn_id = f"eqn:{paper_id}:{eqn_idx}"
            eqn_idx += 1
            counts["n_equations"] += 1
            nodes.append(ParsedNode(
                node_id=eqn_id, subtype="equation",
                attrs={
                    "equation_id": eqn_id,
                    "parent_section_ref": sec_ref,
                    "label": label or f"eqn-{eqn_idx}",
                    "ordinal": eqn_idx - 1,
                    "latex_source": body.strip(),
                },
            ))
            edges.append(ParsedEdge(
                src=sec_container, dst=eqn_id, edge_type="contains",
            ))
        elif env_name in (
            "theorem", "lemma", "proposition", "definition", "corollary",
        ):
            thm_id = f"thm:{paper_id}:{thm_idx}"
            thm_idx += 1
            counts["n_theorems"] += 1
            nodes.append(ParsedNode(
                node_id=thm_id, subtype="theorem",
                attrs={
                    "theorem_id": thm_id,
                    "parent_section_ref": sec_ref,
                    "label": label or f"thm-{thm_idx}",
                    "kind": env_name,
                    "statement": body.strip(),
                },
            ))
            edges.append(ParsedEdge(
                src=sec_container, dst=thm_id, edge_type="contains",
            ))
            last_theorem_id = thm_id
            _emit_citations_in(text=body, container_id=thm_id,
                               container_subtype="theorem")
        elif env_name == "proof":
            prf_id = f"proof:{paper_id}:{prf_idx}"
            prf_idx += 1
            counts["n_proofs"] += 1
            if last_theorem_id is None:
                raise LatexParseError(
                    "proof_without_preceding_theorem"
                )
            proves_ref = last_theorem_id.split(":", 1)[-1]
            nodes.append(ParsedNode(
                node_id=prf_id, subtype="proof",
                attrs={
                    "proof_id": prf_id,
                    "proves_theorem_ref": proves_ref,
                    "text": body.strip(),
                },
            ))
            # theorem_proved_by_proof uses `contains` (per the finding
            # `2026-05-18-theorem-proved-by-archetype.md`).
            edges.append(ParsedEdge(
                src=last_theorem_id, dst=prf_id, edge_type="contains",
            ))
        pos = next_pos

    # Per dogfood-canary follow-up gap #6 (2026-05-18): downstream
    # A2 extractors (evidence_extractor, hypothesis_extractor) scope
    # their queries by `paper_ref` on the child node. Without this,
    # they have to JOIN through parent_section_ref → paper_section
    # → paper_ref every time. Propagate paper_ref onto every
    # extracted child so the cheap scope works.
    paper_ref_value = f"paper:{paper_id}"
    _PROPAGATE_PAPER_REF_TO = {
        "paper_section", "paragraph", "figure", "table",
        "algorithm", "equation", "citation", "theorem", "proof",
    }
    for n in nodes:
        if (
            n.subtype in _PROPAGATE_PAPER_REF_TO
            and "paper_ref" not in n.attrs
        ):
            n.attrs["paper_ref"] = paper_ref_value

    return ParsedAnatomy(
        nodes=nodes,
        edges=edges,
        **counts,
    )


def _split_paragraphs(text: str) -> list[str]:
    """Split a text block into paragraph chunks at blank-line boundaries."""
    raw = re.split(r"\n\s*\n", text)
    return [chunk.strip() for chunk in raw if chunk.strip()]


# ---------------------------------------------------------------- #
# Enricher class.                                                  #
# ---------------------------------------------------------------- #


class LatexStructuralEnricher:
    """Substrate enricher: turns Papers with `.tex` source into
    typed paper-anatomy subgraphs.

    Registered in a deployment's `enrichers:` block:

        enrichers:
          - class: okg.substrate.library.enrichers.scholarly.latex_structural.LatexStructuralEnricher

    Reads `okg.graph_nodes WHERE subtype='paper'` (via a Chronos
    checkout at the pinned generation) with `attrs.source_tex_path`
    set; skips Papers that already have at least one `contains`
    child (already decomposed).
    """

    name: str = "latex_structural"
    reads_subtypes: tuple[str, ...] = ("paper",)
    emits_edge_types: tuple[str, ...] = ("contains",)
    # PACT admit-derived-node-emission: this enricher writes the parsed
    # paper anatomy as nodes whose subtypes come from the document itself,
    # so the node surface is declared UNBOUNDED rather than enumerated.
    # PACT bind-derived-node-provenance: every emitted node declares the
    # paper node it was parsed out of.
    unbounded_write_dimensions: tuple[str, ...] = ("nodes",)
    declares_node_provenance: bool = True
    # Every parsed node, and the extraction_error observation, is derived
    # from exactly one paper node and stays live only while that paper does.
    support_policy: DerivedSupportPolicy = "all_members"
    requires_narrowings: tuple[tuple[str, str, str], ...] = (
        ("paper", "contains", "paper_section"),
        ("paper_section", "contains", "paper_section"),
        ("paper_section", "contains", "paragraph"),
        ("paper_section", "contains", "figure"),
        ("paper_section", "contains", "table"),
        ("paper_section", "contains", "algorithm"),
        ("paper_section", "contains", "equation"),
        ("paper_section", "contains", "theorem"),
        ("theorem", "contains", "proof"),
        ("paragraph", "contains", "citation"),
        ("theorem", "contains", "citation"),
        ("figure", "contains", "citation"),
        ("table", "contains", "citation"),
    )

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        **_publisher_kwargs: object,
    ) -> EnrichResult:
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)

        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        rows = [
            (row["node_id"], row["tex_path"])
            for row in graph.query(
                """
                SELECT n.node_id, n.attrs->>'source_tex_path' AS tex_path
                  FROM okg.graph_nodes n
                 WHERE n.subtype = 'paper'
                   AND coalesce(n.attrs->>'source_tex_path', '') <> ''
                   AND NOT EXISTS (
                       SELECT 1 FROM okg.graph_edges el
                        WHERE el.src = n.node_id
                          AND el.edge_type = 'contains'
                   )
                """
            )
        ]

        if not rows:
            logger.info(
                "%s: zero papers needing decomposition at gen %d",
                self.name, generation_id,
            )
            return EnrichResult(enricher_name=self.name, n_edges_emitted=0)

        n_edges = 0
        n_skipped = 0
        with conn.cursor() as cur:
            for paper_node_id, tex_path in rows:
                paper_id = paper_node_id.split(":", 1)[-1]
                try:
                    src = Path(tex_path).expanduser().read_text("utf-8")
                except OSError as e:
                    _emit_extraction_error(
                        cur,
                        paper_node_id=paper_node_id,
                        extractor=EXTRACTOR_NAME,
                        version=EXTRACTOR_VERSION,
                        confidence=None,
                        reason=f"file_unreadable: {e}",
                        run_id=run_id,
                        generation_id=generation_id,
                        observed_at=observed_at,
                        support_policy=resolve_declared_support_policy(self),
                    )
                    n_skipped += 1
                    continue
                except UnicodeDecodeError as e:
                    _emit_extraction_error(
                        cur,
                        paper_node_id=paper_node_id,
                        extractor=EXTRACTOR_NAME,
                        version=EXTRACTOR_VERSION,
                        confidence=None,
                        reason=f"not_utf8: {e}",
                        run_id=run_id,
                        generation_id=generation_id,
                        observed_at=observed_at,
                        support_policy=resolve_declared_support_policy(self),
                    )
                    n_skipped += 1
                    continue

                try:
                    anatomy = parse_latex(paper_id=paper_id, source=src)
                except LatexParseError as e:
                    _emit_extraction_error(
                        cur,
                        paper_node_id=paper_node_id,
                        extractor=EXTRACTOR_NAME,
                        version=EXTRACTOR_VERSION,
                        confidence=None,
                        reason=e.reason,
                        run_id=run_id,
                        generation_id=generation_id,
                        observed_at=observed_at,
                        support_policy=resolve_declared_support_policy(self),
                    )
                    n_skipped += 1
                    continue

                n_edges += _commit_anatomy(
                    cur,
                    anatomy=anatomy,
                    extractor=EXTRACTOR_NAME,
                    extractor_version=EXTRACTOR_VERSION,
                    run_id=run_id,
                    generation_id=generation_id,
                    observed_at=observed_at,
                    paper_node_id=paper_node_id,
                    support_policy=resolve_declared_support_policy(self),
                )

        logger.info(
            "%s: emitted %d edges across %d papers (%d skipped) at gen %d",
            self.name, n_edges, len(rows) - n_skipped, n_skipped,
            generation_id,
        )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=n_edges,
            n_edges_skipped=n_skipped,
        )


# ---------------------------------------------------------------- #
# Shared commit helpers (used by both latex_structural and          #
# pdf_structural).                                                  #
# ---------------------------------------------------------------- #


def _commit_anatomy(
    cur: psycopg.Cursor,
    *,
    anatomy: ParsedAnatomy,
    extractor: str,
    extractor_version: str,
    run_id: str,
    generation_id: int,
    observed_at: datetime,
    paper_node_id: str,
    support_policy: DerivedSupportPolicy,
) -> int:
    """Insert ParsedAnatomy nodes + edges into okg.{node,edge}_facts.

    Returns the number of edges inserted (after dedupe). Provenance
    discipline (S-A1-R6) is applied here: every node attr gets
    `provenance = derived_deterministic`, `confidence = 1.0`,
    `extractor` and `extractor_version`; every edge gets the same
    via its dedicated provenance/confidence fields plus
    `attrs.extractor` for audit.
    """
    # Migrated to the substrate _fact_emitters helpers. The
    # caller passes a `cur`, but the helpers want a connection.
    # We re-derive the connection via `cur.connection` so we can
    # use the shared helpers without changing the call sites.
    from okg.substrate.library.enrichers._fact_emitters import (
        emit_node_fact, emit_edge_fact,
    )
    conn = cur.connection

    base_attrs = {
        "provenance": "derived_deterministic",
        "confidence": 1.0,
        "extractor": extractor,
        "extractor_version": extractor_version,
        "enricher_run_id": run_id,
    }
    source_record_id = {
        "enricher": extractor,
        "paper_node_id": paper_node_id,
    }
    source_revision = {
        "run_id": run_id,
        "generation_id": generation_id,
        "extractor_version": extractor_version,
    }
    for n in anatomy.nodes:
        node_dedupe_key = hashlib.sha256(
            f"{extractor}:{n.node_id}:{n.subtype}".encode("utf-8"),
        ).hexdigest()
        emit_node_fact(
            conn,
            enricher_name=extractor,
            node_id=n.node_id,
            subtype=n.subtype,
            attrs={**n.attrs, **base_attrs},
            dedupe_key=node_dedupe_key,
            source_record_id=source_record_id,
            source_revision=source_revision,
            run_id=run_id,
            observed_at=observed_at,
            # PACT bind-derived-node-provenance: the whole anatomy is parsed
            # out of one paper node.
            derived_from=[paper_node_id],
            support_policy=support_policy,
        )
    n_edges = 0
    for e in anatomy.edges:
        dedupe_key = hashlib.sha256(
            f"{extractor}:{e.src}->{e.dst}:{e.edge_type}".encode("utf-8"),
        ).hexdigest()
        emit_edge_fact(
            conn,
            enricher_name=extractor,
            src=e.src,
            dst=e.dst,
            edge_type=e.edge_type,
            attrs={**e.attrs, **base_attrs},
            provenance="derived_deterministic",
            confidence=1.0,
            dedupe_key=dedupe_key,
            source_record_id=source_record_id,
            source_revision=source_revision,
            run_id=run_id,
            observed_at=observed_at,
        )
        # emit_edge_fact doesn't return rowcount; we count
        # unconditionally — the projector's dedupe at edges_live
        # is the source of truth for landed-edge count.
        n_edges += 1
    return n_edges


def _emit_extraction_error(
    cur: psycopg.Cursor,
    *,
    paper_node_id: str,
    extractor: str,
    version: str,
    confidence: Optional[float],
    reason: str,
    run_id: str,
    generation_id: int,
    observed_at: datetime,
    support_policy: DerivedSupportPolicy,
) -> None:
    """Emit exactly one `extraction_error` Observation per S-A1-R6.

    The Paper node itself is never modified — this is additive.
    The observation_id is deterministic per (extractor, paper, run)
    so a re-run produces a fresh observation; prior errors stay in
    fact-stream for audit per S-A1-R3.
    """
    from okg.substrate.library.enrichers._fact_emitters import (
        emit_node_fact,
    )
    obs_seed = f"{extractor}:{paper_node_id}:{run_id}".encode("utf-8")
    obs_id = f"obs:extract-err:{hashlib.sha256(obs_seed).hexdigest()[:16]}"
    paper_ref = paper_node_id.split(":", 1)[-1]
    statement = (
        f"Structural extraction failed for {paper_node_id} "
        f"via {extractor}: {reason}"
    )
    err_dedupe_key = hashlib.sha256(
        f"{extractor}:err:{paper_node_id}:{run_id}".encode("utf-8"),
    ).hexdigest()
    emit_node_fact(
        cur.connection,
        enricher_name=extractor,
        node_id=obs_id,
        subtype="observation",
        attrs={
            "observation_id": obs_id,
            "statement": statement,
            "kind": "extraction_error",
            "extractor": extractor,
            "extractor_version": version,
            "source_paper_ref": paper_ref,
            "reason": reason,
            "attempted_at": observed_at.isoformat(),
            "confidence": confidence,
            # Required Observation attrs from reasoning schema.
            "source_kind": "paper",
            "source_ref": paper_ref,
            "source_subtype": "agent_derived",
            "source_path": paper_node_id,
            "frontmatter_schema_version": 1,
            "created_at": observed_at.isoformat(),
            "last_modified_at": observed_at.isoformat(),
            "provenance": "derived_deterministic",
        },
        dedupe_key=err_dedupe_key,
        source_record_id={
            "enricher": extractor,
            "paper_node_id": paper_node_id,
        },
        source_revision={
            "run_id": run_id,
            "generation_id": generation_id,
            "extractor_version": version,
        },
        run_id=run_id,
        observed_at=observed_at,
        # PACT bind-derived-node-provenance: the error observation is about
        # the paper whose extraction failed, and derives from it.
        derived_from=[paper_node_id],
        support_policy=support_policy,
    )
