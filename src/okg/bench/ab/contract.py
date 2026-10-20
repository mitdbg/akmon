"""Corpus contracts: what makes a benchmark OKG usable, checked at build time.

The build gate this replaces was `expected_families published nonzero rows`
— a NODE-COUNT gate. It cannot observe zero edges, a dropped hierarchy, or
an answer unit the graph cannot express, so it went green on corpora an
agent cannot use: literature published 5,183 paper nodes and ZERO edges,
legal 60 statute nodes and ZERO edges, and both passed.

A contract binds the properties that actually decide whether an agent can
reach the benchmark's answer:

- required node subtypes (each must publish rows),
- required EDGE TRIPLES (src_subtype, edge_type, dst_subtype) with minimum
  counts — a corpus with no edges cannot satisfy a triple clause,
- an isolated-node ceiling, plus subtypes where ANY isolated node fails,
- ANSWER-UNIT PROBES: queries shaped like the benchmark's own question that
  must return rows. A negative probe (`expect_empty`) proves the opposite —
  ops uses one to show gold is absent.

Every clause reports its observed value whether it passes or fails, so a
build log states what the corpus IS, not merely that it was allowed.

PACT okg-corpus-design / req.design.contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import CorpusBuildError


@dataclass(frozen=True)
class TripleClause:
    """(src_subtype, edge_type, dst_subtype) must appear at least `minimum`
    times. `minimum` is a floor measured against the source; the first real
    build records the observed count so the floor can be tightened."""

    src: str
    edge_type: str
    dst: str
    minimum: int = 1
    note: str = ""

    @property
    def label(self) -> str:
        return f"{self.src} -{self.edge_type}-> {self.dst}"


@dataclass(frozen=True)
class Probe:
    """An answer-unit reachability check: SQL over the published generation
    that must return rows (or, when `expect_empty`, must return none —
    the shape that proves gold exclusion)."""

    name: str
    sql: str
    expect_empty: bool = False
    note: str = ""


@dataclass(frozen=True)
class CorpusContract:
    subtypes: tuple[str, ...] = ()
    triples: tuple[TripleClause, ...] = ()
    #: fraction of nodes with no edge at all that is still acceptable
    max_isolated_fraction: float = 1.0
    #: subtypes where ANY isolated node is a failure
    zero_isolated_subtypes: tuple[str, ...] = ()
    probes: tuple[Probe, ...] = ()
    notes: str = ""

    def is_binding(self) -> bool:
        """A contract that an edgeless node bag could satisfy is itself a
        defect (umbrella req.design.contract). Binding means it constrains
        connectivity: at least one triple clause, or a zero-isolated rule."""
        return bool(self.triples) or bool(self.zero_isolated_subtypes)

    def unbacked_isolation_rules(self) -> tuple[str, ...]:
        """Zero-isolated subtypes that nothing requires to EXIST.

        "Zero isolated X" is VACUOUSLY TRUE when the corpus holds no X at all,
        so a zero-isolated rule over a subtype absent from `subtypes` can be
        satisfied by deleting the subtype outright. Found in cti: dropping all
        56 attack_campaign nodes and their 1,344 incident edges satisfied every
        clause, because campaign was required-CONNECTED but never
        required-PRESENT.

        Same family as `all()` over an empty collection. A connectivity rule
        must stand on an existence rule or it constrains nothing.
        """
        return tuple(
            s for s in self.zero_isolated_subtypes if s not in self.subtypes
        )


@dataclass
class ClauseResult:
    kind: str
    clause: str
    ok: bool
    observed: Any
    required: Any
    note: str = ""

    def line(self) -> str:
        mark = "ok  " if self.ok else "FAIL"
        return (
            f"  [{mark}] {self.kind}: {self.clause} "
            f"observed={self.observed} required={self.required}"
            + (f"  ({self.note})" if self.note else "")
        )


@dataclass
class ContractReport:
    results: list[ClauseResult] = field(default_factory=list)
    node_counts: dict[str, int] = field(default_factory=dict)
    triple_counts: dict[str, int] = field(default_factory=dict)
    isolated_by_subtype: dict[str, int] = field(default_factory=dict)
    total_nodes: int = 0
    total_edges: int = 0
    connected_nodes: int = 0
    #: 'real' or 'stand-in' — a stand-in build is NEVER reported as
    #: satisfying the contract (umbrella integration_acceptance).
    provenance: str = "real"

    @property
    def ok(self) -> bool:
        return all(r.ok for r in self.results)

    @property
    def failures(self) -> list[ClauseResult]:
        return [r for r in self.results if not r.ok]

    def connected_fraction(self) -> float:
        return (self.connected_nodes / self.total_nodes) if self.total_nodes else 0.0

    def render(self) -> str:
        lines = [
            f"nodes={self.total_nodes} edges={self.total_edges} "
            f"connected={self.connected_nodes} "
            f"({100.0 * self.connected_fraction():.1f}%) "
            f"provenance={self.provenance}",
        ]
        lines += [r.line() for r in self.results]
        return "\n".join(lines)


def evaluate(
    *,
    dsn: str,
    generation_id: str,
    contract: CorpusContract,
    provenance: str = "real",
) -> ContractReport:
    """Measure a published generation against its contract. Never raises on
    a failing clause — it reports; the caller decides. Raises only when the
    generation cannot be read at all."""
    from okg.substrate.graph_store import ChronosGraphStore

    report = ContractReport(provenance=provenance)
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        graph = store.checkout_generation(generation_id)
        for row in graph.query(
            "SELECT subtype, count(*) AS n FROM okg.graph_nodes GROUP BY subtype"
        ):
            report.node_counts[row["subtype"]] = int(row["n"])
        for row in graph.query(
            "SELECT e.edge_type, s.subtype AS src_t, d.subtype AS dst_t, "
            "count(*) AS n FROM okg.graph_edges e "
            "JOIN okg.graph_nodes s ON s.node_id = e.src "
            "JOIN okg.graph_nodes d ON d.node_id = e.dst "
            "GROUP BY 1,2,3"
        ):
            key = f"{row['src_t']} -{row['edge_type']}-> {row['dst_t']}"
            report.triple_counts[key] = int(row["n"])
        for row in graph.query(
            "SELECT n.subtype, count(*) AS n FROM okg.graph_nodes n "
            "WHERE NOT EXISTS (SELECT 1 FROM okg.graph_edges e "
            "  WHERE e.src = n.node_id OR e.dst = n.node_id) GROUP BY 1"
        ):
            report.isolated_by_subtype[row["subtype"]] = int(row["n"])
        report.total_nodes = sum(report.node_counts.values())
        report.total_edges = sum(report.triple_counts.values())
        connected = graph.query(
            "SELECT count(DISTINCT nid) AS n FROM ("
            "  SELECT src AS nid FROM okg.graph_edges "
            "  UNION SELECT dst FROM okg.graph_edges) u"
        )
        report.connected_nodes = int(connected[0]["n"]) if connected else 0
        _score(report, contract)
        for probe in contract.probes:
            rows = graph.query(probe.sql)
            hit = len(rows)
            ok = (hit == 0) if probe.expect_empty else (hit > 0)
            report.results.append(
                ClauseResult(
                    kind="probe" + ("(negative)" if probe.expect_empty else ""),
                    clause=probe.name,
                    ok=ok,
                    observed=f"{hit} rows",
                    required="0 rows" if probe.expect_empty else ">0 rows",
                    note=probe.note,
                )
            )
    finally:
        store.close()
    return report


def _score(report: ContractReport, contract: CorpusContract) -> None:
    if not contract.is_binding():
        report.results.append(
            ClauseResult(
                kind="contract", clause="binds connectivity", ok=False,
                observed="no triple or zero-isolated clause",
                required="at least one",
                note="a contract an edgeless node bag could satisfy is a defect",
            )
        )
    unbacked = contract.unbacked_isolation_rules()
    if unbacked:
        report.results.append(
            ClauseResult(
                kind="contract", clause="isolation rules are backed by existence",
                ok=False, observed=f"{list(unbacked)} required connected but "
                "never required present", required="every zero-isolated subtype "
                "also declared in subtypes",
                note="'zero isolated X' is VACUOUSLY TRUE when no X exists, so "
                "the rule is satisfied by deleting the subtype outright",
            )
        )
    for subtype in contract.subtypes:
        n = report.node_counts.get(subtype, 0)
        report.results.append(
            ClauseResult("subtype", subtype, n > 0, n, ">0")
        )
    for clause in contract.triples:
        n = report.triple_counts.get(clause.label, 0)
        report.results.append(
            ClauseResult(
                "triple", clause.label, n >= clause.minimum, n,
                f">={clause.minimum}", clause.note,
            )
        )
    isolated = sum(report.isolated_by_subtype.values())
    frac = (isolated / report.total_nodes) if report.total_nodes else 0.0
    report.results.append(
        ClauseResult(
            "isolation", "isolated fraction",
            frac <= contract.max_isolated_fraction,
            f"{frac:.3f} ({isolated} nodes)",
            f"<={contract.max_isolated_fraction}",
        )
    )
    for subtype in contract.zero_isolated_subtypes:
        n = report.isolated_by_subtype.get(subtype, 0)
        report.results.append(
            ClauseResult("isolation", f"zero isolated {subtype}", n == 0, n, "0")
        )


#: The provenance verdicts under which a corpus counts as satisfying its
#: contract. ONE definition, because there are two callers -- the build gate
#: (`enforce`, below) and the `corpus audit-health` roll-up -- and they must
#: agree. They did not: audit-health open-coded `!= "real"` and so counted a
#: fully passing `generated` corpus as an unmet contract, silently, with no
#: line saying why. The operator saw a total that exceeded the FAIL lines.
#:
#: `generated` satisfies alongside `real`: a corpus produced by the release's
#: OWN generator from a recorded seed substitutes for nothing, so refusing it
#: would permanently block a benchmark whose corpus is meant to be generated.
#: It stays a DISTINCT verdict rather than being folded into `real`, because
#: "downloaded the release" and "ran the release's generator" are different
#: claims and the audit reports which.
SATISFYING_PROVENANCE = ("real", "generated")


def enforce(report: ContractReport, *, vertical: str) -> None:
    """Fail the build when any clause is unmet, naming clause and observed
    value. A stand-in build never counts as satisfying its contract.
    """
    if report.provenance not in SATISFYING_PROVENANCE:
        raise CorpusBuildError(
            f"{vertical} corpus was built over {report.provenance} inputs; "
            "a stand-in build is never reported as satisfying its contract. "
            f"Observed:\n{report.render()}"
        )
    if not report.ok:
        detail = "\n".join(r.line() for r in report.failures)
        raise CorpusBuildError(
            f"{vertical} corpus does not meet its contract "
            f"({len(report.failures)} unmet clause(s)):\n{detail}\n"
            f"full report:\n{report.render()}"
        )
