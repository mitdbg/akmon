"""Shared fixtures for the invariant-spec slice witnesses (concord-spec-views-contract).

Program builders in the repo's Transform-IR test idiom, a program registry
that stands in for the ``ir://`` resolver, and one well-formed base spec the
refusal witnesses mutate one field at a time.
"""
from __future__ import annotations

import copy
from typing import Any

from okg.substrate.transform import (
    Block,
    Binding,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
)
from okg.substrate.transform.expr import col, eq, lit


def subject_local_program(pid: str) -> TransformProgram:
    """FROM nodes_live only -> instance_certifiable."""
    return TransformProgram(
        program_id=pid,
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "spec_probe",
            "normalized_predicate": "spec_flag",
            "participant_roles": {"page": "node_id"},
            "severity": "low",
            "normalized_value_columns": ["node_id"],
        }},
        blocks=(Block(
            block_id="entry",
            bindings=(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n"),
                Binding(Op(OpKind.WHERE,
                           {"expr": eq(col("n.subtype"), lit("page"))}),
                        output="f", inputs=("n",)),
                Binding(Op(OpKind.SELECT,
                           {"project": {"node_id": col("n.node_id")}}),
                        output="offending", inputs=("f",)),
                Binding(Op(OpKind.EMIT, {"target": "offending"}),
                        inputs=("offending",)),
            ),
            terminator=Terminator(TerminatorKind.RETURN,
                                  {"values": ["offending"]}),
        ),),
    )


def annotation_reader_program(pid: str, *kinds: str) -> TransformProgram:
    """FROM annotation:<kind> surfaces -> kind_level_only, annotation reads."""
    if not kinds:
        raise ValueError("annotation_reader_program needs at least one kind")
    bindings = []
    for i, kind in enumerate(kinds):
        bindings.append(Binding(
            Op(OpKind.FROM, {"surface": f"annotation:{kind}",
                             "alias": f"a{i}"}),
            output=f"a{i}",
        ))
    bindings.extend((
        Binding(Op(OpKind.SELECT,
                   {"project": {"annotation_id": col("a0.annotation_id"),
                                "document": col("a0.document")}}),
                output="offending", inputs=("a0",)),
        Binding(Op(OpKind.EMIT, {"target": "offending"}),
                inputs=("offending",)),
    ))
    return TransformProgram(
        program_id=pid,
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "spec_probe",
            "normalized_predicate": "spec_annotation",
            "participant_roles": {"annotation": "annotation_id"},
            "severity": "low",
            "normalized_value_columns": ["annotation_id"],
        }},
        blocks=(Block(
            block_id="entry",
            bindings=tuple(bindings),
            terminator=Terminator(TerminatorKind.RETURN,
                                  {"values": ["offending"]}),
        ),),
    )


def aggregate_program(pid: str) -> TransformProgram:
    """FROM nodes_live with GROUP -> whole_graph (aggregation reads all rows)."""
    return TransformProgram(
        program_id=pid,
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "spec_probe",
            "normalized_predicate": "spec_aggregate",
            "participant_roles": {"page": "node_id"},
            "severity": "low",
            "normalized_value_columns": ["node_id"],
        }},
        blocks=(Block(
            block_id="entry",
            bindings=(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n"),
                Binding(Op(OpKind.GROUP,
                           {"key": {"subtype": col("n.subtype")},
                            "agg": {"cnt": {"fn": "count"}}}),
                        output="g", inputs=("n",)),
                Binding(Op(OpKind.SELECT,
                           {"project": {"subtype": col("g.subtype"),
                                        "cnt": col("g.cnt")}}),
                        output="offending", inputs=("g",)),
                Binding(Op(OpKind.EMIT, {"target": "offending"}),
                        inputs=("offending",)),
            ),
            terminator=Terminator(TerminatorKind.RETURN,
                                  {"values": ["offending"]}),
        ),),
    )


def incident_edges_program(pid: str) -> TransformProgram:
    """FROM nodes_live + edges_live -> the frontier's node_with_incident_edges
    mode, which classification DEMOTES to kind_level_only (EWD-SVC-4)."""
    from okg.substrate.transform.expr import and_

    return TransformProgram(
        program_id=pid,
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "spec_probe",
            "normalized_predicate": "spec_incident",
            "participant_roles": {"page": "node_id"},
            "severity": "low",
            "normalized_value_columns": ["node_id"],
        }},
        blocks=(Block(
            block_id="entry",
            bindings=(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n"),
                Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}),
                        output="e"),
                Binding(Op(OpKind.JOIN, {"how": "left", "on": and_(
                    eq(col("e.dst"), col("n.node_id")),
                    eq(col("e.edge_type"), lit("links_to")))}),
                        output="j", inputs=("n", "e")),
                Binding(Op(OpKind.SELECT,
                           {"project": {"node_id": col("n.node_id")}}),
                        output="offending", inputs=("j",)),
                Binding(Op(OpKind.EMIT, {"target": "offending"}),
                        inputs=("offending",)),
            ),
            terminator=Terminator(TerminatorKind.RETURN,
                                  {"values": ["offending"]}),
        ),),
    )


def projectionless_program(pid: str) -> TransformProgram:
    """A program with no SELECT projection: witness-key validation must
    refuse rather than pass vacuously (EWD-SVC-6)."""
    return TransformProgram(
        program_id=pid,
        effect=Effect.READ_ONLY,
        outputs=("n",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "spec_probe",
            "normalized_predicate": "spec_raw",
            "participant_roles": {"page": "node_id"},
            "severity": "low",
            "normalized_value_columns": ["node_id"],
        }},
        blocks=(Block(
            block_id="entry",
            bindings=(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n"),
                Binding(Op(OpKind.EMIT, {"target": "n"}), inputs=("n",)),
            ),
            terminator=Terminator(TerminatorKind.RETURN, {"values": ["n"]}),
        ),),
    )


class ProgramRegistry:
    """The test stand-in for the ``ir://`` resolver."""

    def __init__(self) -> None:
        self.programs: dict[str, TransformProgram] = {}

    def add(self, ref: str, program: TransformProgram) -> str:
        """Register and return the authored ``query_digest`` for the spec."""
        self.programs[ref] = program
        return f"sha256:{program.checksum()}"

    def resolve(self, ref: str) -> TransformProgram:
        return self.programs[ref]


def build_registry() -> tuple[ProgramRegistry, dict[str, str]]:
    registry = ProgramRegistry()
    digests = {
        "ir://checks/day_count_match@v5": registry.add(
            "ir://checks/day_count_match@v5",
            subject_local_program("spec.day_count_match.v5")),
        "ir://checks/handbook_vacation_view@v2": registry.add(
            "ir://checks/handbook_vacation_view@v2",
            subject_local_program("spec.handbook_vacation_view.v2")),
        "ir://checks/ctx_sections@v1": registry.add(
            "ir://checks/ctx_sections@v1",
            annotation_reader_program("spec.ctx_sections.v1",
                                      "policy-citation")),
        "ir://checks/day_count_reader@v1": registry.add(
            "ir://checks/day_count_reader@v1",
            annotation_reader_program("spec.day_count_reader.v1",
                                      "day-count")),
        "ir://checks/corpus_census@v1": registry.add(
            "ir://checks/corpus_census@v1",
            aggregate_program("spec.corpus_census.v1")),
    }
    return registry, digests


def base_spec(digests: dict[str, str]) -> dict[str, Any]:
    """One well-formed spec exercising both tiers, three authority classes,
    generation + judged-context views, and all four policy modes' shapes."""
    return {
        "spec_version": 1,
        "deployment": "spec-fixture",
        "defaults": {
            "bounded_k": 3,
            "bounded_time_bound_days": 7,
            "eventual_slice_basis_points": 1000,
            "repair_attempts_r": 2,
            "repair_created_subjects_per_pass": 50,
            "corpus_authority": "advisory",
            "lazy_escalation": "eventual",
        },
        "corpora": [
            {
                "id": "hr-wiki", "version": 3, "kind": "documents",
                "authority": "owned", "digest_algorithm": "sha256",
                "extractors": [
                    {"id": "hr-annotations", "version": 7,
                     "output_envelope": ["day-count", "policy-citation"]},
                ],
            },
            {
                "id": "reg-lib", "version": 1, "kind": "documents",
                "authority": "authoritative", "digest_algorithm": "sha256",
                "extractors": [
                    {"id": "reg-extract", "version": 2,
                     "output_envelope": ["obligation"]},
                ],
            },
            {
                # authority omitted -> the defaults block's advisory; never
                # inferred stronger.
                "id": "partner-wiki", "version": 1, "kind": "documents",
                "digest_algorithm": "sha256",
                "extractors": [
                    {"id": "partner-annotations", "version": 1,
                     "output_envelope": ["day-count"]},
                ],
            },
        ],
        "views": [
            {
                "id": "handbook-vacation", "version": 2,
                "query": "ir://checks/handbook_vacation_view@v2",
                "query_digest": digests["ir://checks/handbook_vacation_view@v2"],
                "role": "generation",
                "renders": "doc://hr-wiki/handbook#vacation",
                "attaches": ["vacation-summary"],
            },
            {
                "id": "ctx-sections", "version": 1,
                "query": "ir://checks/ctx_sections@v1",
                "query_digest": digests["ir://checks/ctx_sections@v1"],
                "role": "judged-context",
            },
        ],
        "invariants": [
            {
                "id": "day-counts-match", "version": 5, "tier": "formal",
                "scope": "corpus://hr-wiki",
                "policy": {"mode": "bounded", "k": 3, "time_bound_days": 7},
                "property": {
                    "query": "ir://checks/day_count_match@v5",
                    "query_digest": digests["ir://checks/day_count_match@v5"],
                },
                "witness_key": ["node_id"],
                "repairs": {
                    "envelope": ["annotation://hr-wiki/*/day-count"],
                    "hints": ["generator://copy-surviving-value@v1", "model"],
                    "attempts_r": 2,
                },
            },
            {
                "id": "no-contradiction", "version": 1, "tier": "judged",
                "scope": "corpus://hr-wiki",
                "policy": {"mode": "eventual"},
                "property": {
                    "text": "this runbook must not contradict the handbook",
                    "context_view": "ctx-sections",
                    "judge": {"model": "judge-model@pin",
                              "prompt_template": "tmpl://contradiction@v3",
                              "settings": "settings://judge-default@v1"},
                    "witness_parser": "parser://section-header@v1",
                },
                "repairs": {"envelope": []},
            },
        ],
    }


def spec_with(digests: dict[str, str], **overrides: Any) -> dict[str, Any]:
    spec = copy.deepcopy(base_spec(digests))
    spec.update(overrides)
    return spec
