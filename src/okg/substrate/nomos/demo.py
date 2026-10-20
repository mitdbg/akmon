"""End-to-end local Nomos demo artifacts."""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from okg.substrate.nomos.core import (
    DecisionInput,
    compile_policy_graph,
    evaluate,
    write_json,
)
from okg.substrate.nomos.refinement import (
    build_refinement_state,
    check_noninterference_trace,
)


def demo_policy_graph() -> dict[str, Any]:
    return {
        "policy_bundle_id": "nomos:demo:baseline",
        "policy_version": "v0",
        "source_generation_id": 0,
        "scope": {"deployment": "nomos-demo"},
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_public_source_emit",
                "effect": "allow",
                "priority": 100,
                "subjects": ["source:demo"],
                "actions": ["source.emit"],
                "match": {
                    "classification": "public",
                    "node_subtype": "public_note",
                },
                "reason_code": "nomos.demo.public_source_emit_allowed",
            },
            {
                "rule_id": "allow_public_poros_context",
                "effect": "allow",
                "priority": 90,
                "subjects": ["okg:lab-a"],
                "actions": ["poros.graph.submit"],
                "match": {
                    "classification": "public",
                    "root_pattern": "context",
                    "graph_profile": "context",
                    "node_subtypes": ["context_packet"],
                    "requested_incorporation_mode": ["fast_track", "review"],
                },
                "reason_code": "nomos.demo.public_poros_context_allowed",
            },
            {
                "rule_id": "quarantine_proposal_shaped_poros",
                "effect": "quarantine",
                "priority": 80,
                "subjects": ["okg:lab-a"],
                "actions": ["poros.graph.submit"],
                "match": {"root_pattern": "proposal"},
                "reason_code": "nomos.demo.profile_hint_cannot_broaden",
            },
            {
                "rule_id": "redact_restricted_export",
                "effect": "redact",
                "priority": 70,
                "subjects": ["agent:curator"],
                "actions": ["graph.export"],
                "match": {
                    "classification": "restricted",
                    "redaction_evidence": True,
                },
                "reason_code": "nomos.demo.restricted_export_redacted",
                "obligations": [
                    {
                        "type": "redaction",
                        "fields": ["body.text", "object_locator"],
                    }
                ],
            },
            {
                "rule_id": "review_agent_self_expansion",
                "effect": "review",
                "priority": 60,
                "subjects": ["agent:curator"],
                "actions": ["policy.activate"],
                "match": {"self_expansion": True},
                "reason_code": "nomos.demo.agent_self_expansion_requires_review",
            },
        ],
    }


def demo_inputs() -> list[DecisionInput]:
    return [
        DecisionInput(
            subject="source:demo",
            action="source.emit",
            classification="public",
            resource={
                "resource_id": "node:public-note-1",
                "node_subtype": "public_note",
            },
            context={"enforcement_point": "source_admission"},
        ),
        DecisionInput(
            subject="okg:lab-a",
            action="poros.graph.submit",
            classification="public",
            resource={
                "graph_id": "poros:graph:demo-context",
                "root_pattern": "context",
                "graph_profile": "context",
                "node_subtypes": ["context_packet"],
                "root_subtypes": ["context_packet"],
                "requested_incorporation_mode": "fast_track",
                "recipient": "okg:mit-demo",
            },
            context={"enforcement_point": "poros_incorporation"},
        ),
        DecisionInput(
            subject="okg:lab-a",
            action="poros.graph.submit",
            classification="public",
            resource={
                "graph_id": "poros:graph:misleading-profile",
                "root_pattern": "proposal",
                "graph_profile": "fact_bundle",
                "node_subtypes": ["proposal"],
                "root_subtypes": ["proposal"],
                "requested_incorporation_mode": "fast_track",
                "recipient": "okg:mit-demo",
            },
            context={"enforcement_point": "poros_incorporation"},
        ),
        DecisionInput(
            subject="okg:unknown",
            action="poros.graph.submit",
            classification="public",
            resource={
                "graph_id": "poros:graph:untrusted-context",
                "root_pattern": "context",
                "graph_profile": "context",
                "node_subtypes": ["context_packet"],
                "root_subtypes": ["context_packet"],
                "requested_incorporation_mode": "fast_track",
                "recipient": "okg:mit-demo",
            },
            context={"enforcement_point": "poros_incorporation"},
        ),
        DecisionInput(
            subject="agent:curator",
            action="graph.export",
            classification="restricted",
            resource={
                "graph_id": "graph:restricted-finding",
                "destination": "okg:public-demo",
                "redaction_evidence": True,
            },
            context={"enforcement_point": "export_egress"},
        ),
        DecisionInput(
            subject="agent:curator",
            action="policy.activate",
            classification="internal",
            resource={
                "policy_bundle_id": "nomos:demo:expanded-agent-rights",
                "self_expansion": True,
            },
            context={"enforcement_point": "agent_action"},
        ),
    ]


def run_demo(out_dir: Path, *, overwrite: bool = False) -> dict[str, Any]:
    out_dir = out_dir.expanduser().resolve()
    if out_dir.exists() and any(out_dir.iterdir()):
        if not overwrite:
            raise FileExistsError(
                f"{out_dir} already contains files; pass --overwrite"
            )
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    policy_graph = demo_policy_graph()
    bundle = compile_policy_graph(policy_graph)
    decisions = [
        evaluate(
            bundle,
            decision_input,
            enforcement_point=str(
                decision_input.context.get("enforcement_point", "nomos.demo")
            ),
        )
        for decision_input in demo_inputs()
    ]

    visible = [
        decision.decision_id
        for decision in decisions
        if decision.action in {"source.emit", "poros.graph.submit"}
        and decision.effect == "allow"
    ]
    quarantine = [
        decision.decision_id
        for decision in decisions
        if decision.effect in {"review", "quarantine"}
    ]
    public_outputs = [
        decision.decision_id
        for decision in decisions
        if decision.action in {"graph.export"}
        and decision.effect in {"allow", "redact", "declassify"}
    ]
    state = build_refinement_state(
        decisions,
        policy_graphs=[policy_graph["policy_bundle_id"]],
        compiled_bundles=[bundle.policy_fingerprint],
        active_bundle=bundle.policy_fingerprint,
        visible_state=visible,
        quarantine_state=quarantine,
        public_outputs=public_outputs,
        generation_policy_pins={
            "demo-generation-1": bundle.policy_fingerprint,
        },
        unsupported=[],
    )
    check_noninterference_trace(state)

    artifacts = {
        "policy_graph": out_dir / "policy.graph.json",
        "compiled_bundle": out_dir / "compiled-policy.json",
        "decision_inputs": out_dir / "decision-inputs.json",
        "decisions": out_dir / "decisions.json",
        "refinement_state": out_dir / "refinement-state.json",
        "readme": out_dir / "README.md",
    }
    write_json(artifacts["policy_graph"], policy_graph)
    write_json(artifacts["compiled_bundle"], bundle.to_dict())
    write_json(
        artifacts["decision_inputs"],
        [decision_input.to_dict() for decision_input in demo_inputs()],
    )
    write_json(artifacts["decisions"], [decision.to_dict() for decision in decisions])
    write_json(artifacts["refinement_state"], state)
    artifacts["readme"].write_text(_readme(bundle.policy_fingerprint), encoding="utf-8")

    return {
        "status": "ok",
        "policy_fingerprint": bundle.policy_fingerprint,
        "decision_counts": _decision_counts(decisions),
        "visible_decisions": visible,
        "quarantine_decisions": quarantine,
        "public_output_decisions": public_outputs,
        "artifacts": {key: str(path) for key, path in artifacts.items()},
    }


def _decision_counts(decisions) -> dict[str, int]:
    counts: dict[str, int] = {}
    for decision in decisions:
        counts[decision.effect] = counts.get(decision.effect, 0) + 1
    return dict(sorted(counts.items()))


def _readme(policy_fingerprint: str) -> str:
    return f"""# Nomos Demo Artifacts

This directory was created by `okg nomos demo`.

The demo compiles a graph-native policy into a deterministic bundle,
evaluates source, Poros, export, and agent-action decisions, emits
durable decision records, and writes a refinement-state JSON artifact
that maps the runtime slice back to `formal/nomos/Nomos.tla`.

Policy fingerprint: `{policy_fingerprint}`

Important files:

- `policy.graph.json`: graph-native policy source.
- `compiled-policy.json`: deterministic enforcement bundle.
- `decision-inputs.json`: bounded `(subject, action, resource, context)`
  records.
- `decisions.json`: durable Nomos decision evidence.
- `refinement-state.json`: abstract state consumed by refinement checks.
"""
