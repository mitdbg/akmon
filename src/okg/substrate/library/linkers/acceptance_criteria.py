"""AcceptanceCriteriaLinker — deterministic OpenSpec test coverage links."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import psycopg

from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
)
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers._incremental import incremental_touches


__all__ = ["AcceptanceCriteriaDiagnostics", "AcceptanceCriteriaLinker"]


@dataclass(frozen=True)
class AcceptanceCriteriaDiagnostics:
    eligible_criteria: int
    matched_criteria: int
    matched_tests: int
    unmatched_criteria: list[dict[str, str]]

    @property
    def unmatched_criterion_count(self) -> int:
        return len(self.unmatched_criteria)

    def as_dict(self) -> dict[str, Any]:
        return {
            "eligible_criteria": self.eligible_criteria,
            "matched_criteria": self.matched_criteria,
            "matched_tests": self.matched_tests,
            "unmatched_criterion_count": self.unmatched_criterion_count,
            "unmatched_criteria": list(self.unmatched_criteria),
        }


class AcceptanceCriteriaLinker:
    """Link OpenSpec acceptance criteria to test evidence.

    The linker is intentionally conservative. It only links when a
    test case reports a stable OpenSpec id, either through JUnit
    testcase properties/markers captured as attrs, or through the
    classname/name/message text containing an exact criterion or
    scenario id. Scenario ids resolve to their parent criterion.
    """

    name = "acceptance_criteria"
    publisher_skip_when_clean = True
    reads_subtypes = (
        "acceptance_criterion",
        "acceptance_scenario",
        "test_case",
        "test_report",
    )
    reads_edge_types = ("contains",)
    emits_edge_types = ("verified_by",)
    requires_narrowings = (
        ("acceptance_criterion", "verified_by", "test_case"),
        ("acceptance_criterion", "verified_by", "test_report"),
    )

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        with conn.cursor() as cur:
            if not incremental_touches(
                cur,
                incremental,
                node_subtypes=self.reads_subtypes,
                edge_endpoint_subtypes=("test_report", "test_case"),
                edge_types=self.reads_edge_types,
            ):
                return EnrichResult(enricher_name=self.name, n_edges_emitted=0)
            pairs = self._match_pairs(cur)
            emitted, skipped = self._emit_edges(
                cur,
                pairs=pairs,
                generation_id=generation_id,
            )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=emitted,
            n_edges_skipped=skipped,
        )

    def diagnostics(
        self,
        conn: psycopg.Connection,
    ) -> AcceptanceCriteriaDiagnostics:
        with conn.cursor() as cur:
            criteria = _fetch_nodes(cur, "acceptance_criterion")
            pairs = self._match_pairs(cur)
        matched_criteria = {src for src, _ in pairs}
        matched_tests = {dst for _, dst in pairs if dst.startswith("test-case:")}
        unmatched = [
            {
                "node_id": node_id,
                "criterion_id": str(attrs.get("criterion_id") or node_id),
                "title": str(attrs.get("title") or attrs.get("label") or ""),
            }
            for node_id, attrs in sorted(criteria.items())
            if node_id not in matched_criteria
        ]
        return AcceptanceCriteriaDiagnostics(
            eligible_criteria=len(criteria),
            matched_criteria=len(matched_criteria),
            matched_tests=len(matched_tests),
            unmatched_criteria=unmatched,
        )

    def _match_pairs(self, cur) -> set[tuple[str, str]]:
        criteria = _fetch_nodes(cur, "acceptance_criterion")
        scenarios = _fetch_nodes(cur, "acceptance_scenario")
        tests = _fetch_nodes(cur, "test_case")
        if not criteria or not tests:
            return set()

        criterion_ids: dict[str, str] = {}
        for node_id, attrs in criteria.items():
            attr_id = attrs.get("criterion_id")
            for alias in _criterion_aliases(node_id, attr_id):
                criterion_ids[alias] = node_id

        scenario_to_criterion: dict[str, str] = {}
        for node_id, attrs in scenarios.items():
            raw_criterion = attrs.get("criterion_id")
            if isinstance(raw_criterion, str):
                criterion = criterion_ids.get(raw_criterion)
                if criterion:
                    scenario_to_criterion[node_id] = criterion
            raw_scenario = attrs.get("scenario_id")
            if isinstance(raw_scenario, str) and node_id in scenario_to_criterion:
                for alias in _scenario_aliases(node_id, raw_scenario):
                    scenario_to_criterion[alias] = scenario_to_criterion[node_id]

        pairs: set[tuple[str, str]] = set()
        for test_id, attrs in tests.items():
            refs = _explicit_refs(attrs)
            text = _search_text(attrs)
            for ref in refs:
                criterion = criterion_ids.get(ref) or scenario_to_criterion.get(ref)
                if criterion:
                    pairs.add((criterion, test_id))
            for ref, criterion in criterion_ids.items():
                if ref and ref in text:
                    pairs.add((criterion, test_id))
            for ref, criterion in scenario_to_criterion.items():
                if ref and ref in text:
                    pairs.add((criterion, test_id))

        if pairs:
            for criterion_id, test_id in list(pairs):
                for report_id in _reports_for_test(cur, test_id):
                    pairs.add((criterion_id, report_id))
        return pairs

    def _emit_edges(
        self,
        cur,
        *,
        pairs: set[tuple[str, str]],
        generation_id: int,
    ) -> tuple[int, int]:
        if not pairs:
            return 0, 0
        result = insert_deterministic_edges(
            cur,
            source=f"_enricher:{self.name}",
            candidates=[
                DerivedEdgeCandidate(
                    src=src,
                    edge_type="verified_by",
                    dst=dst,
                    source_record_id={
                        "src_node_id": src,
                        "dst_node_id": dst,
                        "linker": self.name,
                    },
                    source_revision={"generation_id": generation_id},
                )
                for src, dst in sorted(pairs)
            ],
        )
        return result.inserted, result.skipped


def _fetch_nodes(cur, subtype: str) -> dict[str, dict[str, Any]]:
    rows = _chronos.nodes(cur, (subtype,))
    return {
        str(row["node_id"]): dict(row["attrs"] or {})
        for row in rows
    }


def _reports_for_test(cur, test_id: str) -> list[str]:
    rows = _chronos.query(
        cur,
        """
        SELECT e.src
          FROM okg.graph_edges e
          JOIN okg.graph_nodes r
            ON r.node_id = e.src
           AND r.subtype = 'test_report'
         WHERE e.edge_type = 'contains'
           AND e.dst = :test_id
        """,
        {"test_id": test_id},
    )
    return [str(row["src"]) for row in rows]


def _criterion_aliases(node_id: str, attr_id: Any) -> set[str]:
    aliases = {node_id}
    if isinstance(attr_id, str) and attr_id:
        aliases.add(attr_id)
    for raw in list(aliases):
        parts = raw.split(":")
        if len(parts) >= 5 and parts[0] == "crit":
            aliases.add(f"crit:{parts[1]}:{parts[-1]}")
    return aliases


def _scenario_aliases(node_id: str, attr_id: Any) -> set[str]:
    aliases = {node_id}
    if isinstance(attr_id, str) and attr_id:
        aliases.add(attr_id)
    for raw in list(aliases):
        parts = raw.split(":")
        if len(parts) >= 6 and parts[0] == "scn":
            aliases.add(f"scn:{parts[1]}:{parts[-2]}:{parts[-1]}")
    return aliases


def _explicit_refs(attrs: dict[str, Any]) -> set[str]:
    refs: set[str] = set()
    for key in (
        "criteria_refs",
        "criterion_id",
        "acceptance_criterion_id",
        "acceptance_criteria",
        "acceptance_criterion_ids",
        "openspec_criterion",
        "openspec_criteria",
        "scenario_id",
        "acceptance_scenario_id",
        "openspec_scenario",
        "openspec_scenarios",
    ):
        refs.update(_refs_from_value(attrs.get(key)))
    refs.update(_refs_from_value(attrs.get("properties")))
    return refs


def _refs_from_value(value: Any) -> set[str]:
    refs: set[str] = set()
    if value is None:
        return refs
    if isinstance(value, dict):
        for key, nested in value.items():
            refs.update(_refs_from_value(key))
            refs.update(_refs_from_value(nested))
        return refs
    if isinstance(value, (list, tuple, set)):
        for nested in value:
            refs.update(_refs_from_value(nested))
        return refs
    text = str(value)
    refs.update(re.findall(r"\b(?:crit|scn):[A-Za-z0-9_.:/-]+", text))
    refs.update(re.findall(
        r"\b[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+",
        text,
    ))
    return refs


def _search_text(attrs: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("classname", "name", "message", "text"):
        value = attrs.get(key)
        if value:
            parts.append(str(value))
    props = attrs.get("properties")
    if isinstance(props, dict):
        for key, value in props.items():
            parts.append(str(key))
            parts.append(str(value))
    refs = attrs.get("criteria_refs")
    if refs:
        parts.append(json.dumps(refs, sort_keys=True))
    return "\n".join(parts)
