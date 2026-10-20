from __future__ import annotations

from okg.substrate.nomos import (
    ACTIONS,
    AGENT_RUNTIME_LOCAL_EFFECT_SURFACES,
    DATA_CLASSIFICATIONS,
    PRINCIPAL_KINDS,
    RESOURCE_KINDS,
    compile_policy_graph,
    runtime_surface_mapping_for,
)


EXPECTED_AGENT_RUNTIME_EFFECTS = {
    "agent.launch": ("agent_runtime", "agent", "internal"),
    "agent.bypass": ("agent_run", "agent", "restricted"),
    "secret.use": ("secret", "okg_service", "secret"),
    "ssh.use": ("ssh_identity", "agent", "restricted"),
    "docker.run": ("container", "agent", "restricted"),
    "network.egress": ("network_destination", "agent", "restricted"),
    "file.read": ("file", "agent", "restricted"),
    "file.write": ("file", "agent", "restricted"),
    "transcript.persist": ("transcript", "agent", "restricted"),
    "service.persist": ("local_service", "agent", "restricted"),
}


def test_agent_runtime_local_effects_are_canonical_vocabulary() -> None:
    assert set(AGENT_RUNTIME_LOCAL_EFFECT_SURFACES) == set(
        EXPECTED_AGENT_RUNTIME_EFFECTS
    )

    for surface, (
        resource_kind,
        principal_kind,
        classification,
    ) in EXPECTED_AGENT_RUNTIME_EFFECTS.items():
        mapping = runtime_surface_mapping_for(surface)

        assert mapping.surface == surface
        assert mapping.action == surface
        assert mapping.action in ACTIONS
        assert mapping.resource_kind == resource_kind
        assert mapping.resource_kind in RESOURCE_KINDS
        assert mapping.principal_kind == principal_kind
        assert mapping.principal_kind in PRINCIPAL_KINDS
        assert mapping.classification == classification
        assert mapping.classification in DATA_CLASSIFICATIONS


def test_strict_policy_graph_accepts_agent_runtime_actions() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:agent-runtime-vocabulary",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": f"allow_{action.replace('.', '_')}",
                "effect": "allow",
                "actions": [action],
                "subjects": ["subject:agent:1"],
                "reason_code": "nomos.test.agent_runtime_allowed",
            }
            for action in sorted(EXPECTED_AGENT_RUNTIME_EFFECTS)
        ],
    })

    assert {rule.actions[0] for rule in bundle.rules} == set(
        EXPECTED_AGENT_RUNTIME_EFFECTS
    )
