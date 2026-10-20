"""The runtime boundary owns deployment, scope and the surface-derived facts.

``evaluate_runtime_boundary_with_active_policy`` used to build the policy
resource as ``{"deployment": ..., "scope": ..., **request.resource}``, so a
caller resource carrying ``deployment`` or ``scope`` replaced the trusted value
in the recorded decision, and a caller ``subject``/``action``/``classification``
key replaced the fact rules match on (issue #2115's class). These tests use an
in-memory storage adapter, so no database is needed.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos.core import NomosPolicyError, compile_policy_graph
from okg.substrate.nomos.runtime_enforcement import NomosRuntimeRequest
from okg.substrate.nomos import store as nomos_store
from okg.substrate.nomos.store import (
    RESERVED_BOUNDARY_FACTS,
    NomosBoundaryRequestMalformed,
    NomosReservedFactConflict,
    evaluate_runtime_boundary_with_active_policy,
)


DEPLOYMENT = "deploy-y"
SCOPE = f"deployment:{DEPLOYMENT}"
SURFACE = "graph.read.cli_search"


class _Adapter:
    """Same-transaction storage seam: serves one bundle, records decisions."""

    def __init__(self, bundle: Any) -> None:
        self.bundle = bundle
        self.loads: list[tuple[str, str]] = []
        self.decisions: list[Any] = []

    def load_active_policy_bundle(self, conn, *, deployment_name, scope):
        self.loads.append((deployment_name, scope))
        return self.bundle

    def persist_nomos_decision(self, conn, *, deployment_name, decision, target):
        self.decisions.append(decision)


def _policy_allowing(match: dict[str, Any]) -> Any:
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:reserved-facts",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_matching",
                "effect": "allow",
                "actions": ["*"],
                "subjects": ["*"],
                "match": match,
                "priority": 10,
                "reason_code": "test.allow",
                "obligations": [{"type": "audit"}],
            },
        ],
    })


def _evaluate(
    adapter: _Adapter,
    *,
    resource: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    deployment: str = DEPLOYMENT,
    scope: str = SCOPE,
    surface: str = SURFACE,
):
    return evaluate_runtime_boundary_with_active_policy(
        None,
        deployment_name=deployment,
        scope=scope,
        request=NomosRuntimeRequest(
            surface=surface,
            subject="human_operator:alice",
            resource={"resource_kind": "cli_search_boundary", **(resource or {})},
            context=context or {},
        ),
        performed_obligations=("audit",),
        enforcement_mode="enforce",
        storage_adapter=adapter,
    )


def test_reserved_fact_list_names_every_boundary_owned_fact() -> None:
    assert RESERVED_BOUNDARY_FACTS == (
        "deployment",
        "scope",
        "subject",
        "action",
        "classification",
        "principal_kind",
        "nomos_surface",
        "nomos_boundary",
    )
    # resource_kind is a caller-refinable default, not a boundary fact.
    assert "resource_kind" not in RESERVED_BOUNDARY_FACTS


@pytest.mark.parametrize(
    ("key", "forged"),
    [
        ("deployment", "deploy-x"),
        ("scope", "deployment:deploy-x"),
        ("subject", "human_operator:root"),
        ("action", "graph.read"),
        ("classification", "public"),
        ("principal_kind", "okg_service"),
        ("nomos_surface", "graph.read.public_demo"),
        ("nomos_boundary", "graph.read.public_demo"),
    ],
)
@pytest.mark.parametrize("location", ["resource", "context"])
def test_forged_reserved_fact_is_refused_before_evaluation(
    key: str, forged: str, location: str
) -> None:
    adapter = _Adapter(_policy_allowing({}))
    with pytest.raises(NomosReservedFactConflict) as raised:
        _evaluate(adapter, **{location: {key: forged}})
    err = raised.value
    assert err.code == "nomos_reserved_fact_conflict"
    assert err.data == {
        "key": key,
        "location": location,
        "surface": SURFACE,
        "deployment": DEPLOYMENT,
        "scope": SCOPE,
    }
    # Callers that already catch NomosPolicyError / SubstrateError still do.
    assert isinstance(err, NomosPolicyError)
    assert isinstance(err, SubstrateError)
    # Refused before any policy load or decision write: no verdict exists.
    assert adapter.loads == []
    assert adapter.decisions == []


def test_policy_for_deployment_x_cannot_be_passed_from_deployment_y() -> None:
    """A caller in Y claiming X is refused, not evaluated as X."""
    adapter = _Adapter(_policy_allowing({"deployment": "deploy-x"}))
    with pytest.raises(NomosReservedFactConflict):
        _evaluate(adapter, resource={"deployment": "deploy-x"})
    with pytest.raises(NomosReservedFactConflict):
        _evaluate(adapter, context={"deployment": "deploy-x"})
    # Without the claim the honest caller in Y is denied by the X-only policy.
    outcome = _evaluate(adapter)
    assert outcome.allowed is False
    assert outcome.decision.resource["deployment"] == DEPLOYMENT


def test_forged_classification_could_not_satisfy_a_classification_rule() -> None:
    """On dev a resource ``classification`` replaced the matched fact."""
    adapter = _Adapter(_policy_allowing({"classification": "public"}))
    with pytest.raises(NomosReservedFactConflict):
        _evaluate(adapter, resource={"classification": "public"})
    assert adapter.decisions == []


def test_honest_request_is_evaluated_unchanged() -> None:
    adapter = _Adapter(_policy_allowing({"deployment": DEPLOYMENT, "scope": SCOPE}))
    outcome = _evaluate(adapter, resource={"query": "q"}, context={"os_user": "a"})
    assert outcome.allowed is True
    assert adapter.loads == [(DEPLOYMENT, SCOPE)]
    decision = adapter.decisions[-1]
    assert decision.resource["deployment"] == DEPLOYMENT
    assert decision.resource["scope"] == SCOPE
    assert decision.resource["query"] == "q"
    assert decision.context["deployment"] == DEPLOYMENT
    assert decision.context["os_user"] == "a"


def test_equal_reserved_values_are_accepted_and_change_nothing() -> None:
    """Callers such as the ingest runner echo deployment and scope."""
    policy = _policy_allowing({"deployment": DEPLOYMENT})
    plain = _Adapter(policy)
    echoed = _Adapter(policy)
    first = _evaluate(plain)
    second = _evaluate(
        echoed,
        resource={
            "deployment": DEPLOYMENT,
            "scope": SCOPE,
            "subject": "human_operator:alice",
            "classification": "internal",
        },
        context={"deployment": DEPLOYMENT, "scope": SCOPE},
    )
    assert first.allowed is second.allowed is True
    assert second.decision.resource["deployment"] == DEPLOYMENT


def test_unknown_surface_has_no_trusted_action_so_any_claim_is_refused() -> None:
    adapter = _Adapter(_policy_allowing({}))
    with pytest.raises(NomosReservedFactConflict):
        _evaluate(adapter, surface="not.a.surface", resource={"action": "x"})


def test_activation_boundary_refuses_a_forged_deployment() -> None:
    with pytest.raises(NomosReservedFactConflict) as raised:
        nomos_store._authorize_activation_boundary(
            None,
            deployment_name=DEPLOYMENT,
            scope=SCOPE,
            surface="policy.activate",
            subject="human_operator:alice",
            resource={"deployment": "deploy-x"},
            target={"kind": "policy_activation"},
            bootstrap=True,
        )
    assert raised.value.data["location"] == "resource"


@pytest.mark.parametrize(
    ("location", "pairs", "match"),
    [
        ("resource", [("classification", "public")], {"classification": "public"}),
        ("context", [("action", "graph.export")], {"action": "graph.export"}),
        ("context", [("deployment", "deploy-x")], {"deployment": "deploy-x"}),
    ],
)
def test_list_of_pairs_request_is_refused(location, pairs, match) -> None:
    """A list of pairs once skipped the check and flipped a deny to allow."""
    adapter = _Adapter(_policy_allowing(match))
    request = NomosRuntimeRequest(
        surface=SURFACE,
        subject="human_operator:alice",
        resource=(
            pairs if location == "resource"
            else {"resource_kind": "cli_search_boundary"}
        ),
        context=pairs if location == "context" else {},
    )
    with pytest.raises(NomosBoundaryRequestMalformed) as raised:
        evaluate_runtime_boundary_with_active_policy(
            None,
            deployment_name=DEPLOYMENT,
            scope=SCOPE,
            request=request,
            performed_obligations=("audit",),
            enforcement_mode="enforce",
            storage_adapter=adapter,
        )
    err = raised.value
    assert err.code == "nomos_boundary_request_malformed"
    assert err.data["location"] == location
    assert err.data["value_type"] == "list"
    assert isinstance(err, NomosPolicyError)
    assert adapter.loads == []
    assert adapter.decisions == []


class _LyingStr(str):
    """A str subclass that dict lookup cannot match but sanitizing flattens."""

    def __eq__(self, other: object) -> bool:
        return False

    def __hash__(self) -> int:
        return id(self)


@pytest.mark.parametrize(
    ("location", "key", "forged", "match"),
    [
        ("resource", "action", "graph.export", {"action": "graph.export"}),
        ("context", "subject", "human_operator:root", {"subject": "human_operator:root"}),
    ],
)
def test_non_str_key_is_refused(location, key, forged, match) -> None:
    adapter = _Adapter(_policy_allowing(match))
    with pytest.raises(NomosBoundaryRequestMalformed) as raised:
        _evaluate(adapter, **{location: {_LyingStr(key): forged}})
    err = raised.value
    assert err.code == "nomos_boundary_request_malformed"
    assert err.data["location"] == location
    assert err.data["key_type"] == "_LyingStr"
    assert adapter.loads == []
    assert adapter.decisions == []


def test_trusted_facts_win_even_with_the_conflict_check_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the second line of defence: trusted values are written last."""
    monkeypatch.setattr(
        nomos_store, "_refuse_reserved_fact_conflicts", lambda **_: None
    )
    adapter = _Adapter(_policy_allowing({"classification": "public"}))
    outcome = _evaluate(
        adapter,
        resource={
            "deployment": "deploy-x",
            "scope": "deployment:deploy-x",
            "classification": "public",
        },
        context={"deployment": "deploy-x", "action": "graph.export"},
    )
    assert outcome.allowed is False
    decision = adapter.decisions[-1]
    assert decision is outcome.decision
    assert decision.resource["deployment"] == DEPLOYMENT
    assert decision.resource["scope"] == SCOPE
    # graph.read.cli_search's canonical classification and action.
    assert decision.resource["classification"] == "internal"
    assert decision.context["deployment"] == DEPLOYMENT
    assert decision.context["action"] == "graph.read.cli_search"
    assert decision.classification == "internal"
