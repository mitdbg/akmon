"""The OpenSearch reference deployment's two live relations, up to the adapter.

PACT ``opensearch-reference-integration`` (issue #2014), req.ori.relations.
Every test here is hermetic: no container, no database, no network. What it
proves is everything on THIS side of the adapter boundary --

* the ``search_systems`` module declares ``documents`` and ``histogram`` in the
  shape issue #2013 defines, and every argument carries a bound;
* the deployment binds both, and the binding's body uses exactly the arguments
  the ontology declares -- both directions;
* the shipped Nomos bundle allows ``external.opensearch.search`` for the
  runtime service principal on ``search_index`` and denies everything else,
  including the absent-bundle case;
* one dispatch through ``dispatch_live_relation`` with a FAKE adapter returns
  the ``external_live`` envelope, and a denied call never enters the adapter.

What it deliberately does NOT prove is the call itself. The HTTP adapter is
issue #2100 / PACT ``live-relation-http-adapter``, owned by another worker, and
this change writes no HTTP transport of its own. Until that lands this
deployment does not start at all -- ``assert_live_bindings_are_declared``
refuses a binding whose adapter kind has no registered runner -- and
``test_the_deployment_refuses_to_start_without_the_http_runner`` asserts that
refusal rather than papering over it with a stub runner in production wiring.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.external_backed.live_dispatch import (
    LiveAdapterUnavailable,
    LiveArgumentsInvalid,
    assert_live_bindings_are_declared,
    build_live_gate,
    dispatch_live_relation,
)
from okg.substrate.external_backed.live_relations import (
    BoundLiveRelation,
    LiveRelation,
    bind_live_relations,
    compile_args_model,
    parse_live_bindings,
    parse_live_relations,
)
from okg.substrate.mcp.effective_surface import parse_agent_surface

DEPLOYMENT = "opensearch-reference"
SUBTYPE = "search_index"
ACTION = "external.opensearch.search"
SERVICE_PRINCIPAL = "service:okg-runtime"

_REPO = Path(__file__).resolve().parents[4]
DEPLOYMENTS_ROOT = _REPO / "deployments"
MANIFEST = DEPLOYMENTS_ROOT / DEPLOYMENT / "deployment.yaml"
BUNDLE = DEPLOYMENTS_ROOT / DEPLOYMENT / "nomos_external_probe.json"
SCHEMA = (
    _REPO
    / "src/okg/substrate/library/ontologies/search_systems/schema.yaml"
)

def _is_constrained(published: dict[str, Any], type_name: str) -> bool:
    """#2100's rule, restated here so drift breaks a test, not a deployment.

    A string needs an ``enum``, or a ``pattern`` TOGETHER WITH a
    ``maxLength`` -- a length alone is not a constraint on the value, and a
    pattern alone is a regular expression over an unbounded string. A number
    needs an ``enum`` or a range bound. Verified against
    ``validate_live_binding_configs`` on ``claude/s20-http-adapter`` at
    ``948e02c6f``: both bindings in this deployment pass it.
    """
    if "enum" in published:
        return True
    if type_name == "string":
        return "pattern" in published and "maxLength" in published
    if type_name == "boolean":
        return True
    return "minimum" in published or "maximum" in published


def declared_relations() -> dict[str, LiveRelation]:
    doc = yaml.safe_load(SCHEMA.read_text(encoding="utf-8"))
    return parse_live_relations(
        doc["classes"]["SearchIndex"]["annotations"], subtype=SUBTYPE,
    )


def declared_bindings() -> dict[str, Any]:
    raw = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    surface = parse_agent_surface(raw, manifest_path=MANIFEST)
    assert surface is not None, "the manifest declares an agent_surface block"
    return dict(surface.live_bindings)


def bound_relation(name: str) -> BoundLiveRelation:
    return bind_live_relations(declared_relations(), declared_bindings())[name]


# --------------------------------------------------------------------------
# Declaration
# --------------------------------------------------------------------------


def test_search_index_declares_both_relations_with_the_declared_policy():
    relations = declared_relations()
    assert sorted(relations) == ["documents", "histogram"]
    for relation in relations.values():
        assert relation.action == ACTION
        # Service is the ONLY mode served this sprint. `caller` would be
        # refused at load, and a downgrade to service would run the call
        # under the runtime's principal while the deployment believed
        # otherwise.
        assert relation.principal_mode == "service"
        assert relation.sample_policy == "inline"
        assert relation.retention_class == "warm"
        assert 0 < relation.deadline_seconds <= 120

    assert sorted(a.name for a in relations["documents"].args) == [
        "index", "limit", "query", "time_window",
    ]
    assert sorted(a.name for a in relations["histogram"].args) == [
        "field", "index", "interval", "query", "time_window",
    ]


def test_every_declared_argument_carries_a_bound():
    """No unconstrained slot reaches the adapter.

    A `string` with no `maxLength` is an unbounded fragment of somebody's
    query DSL and an `integer` with no `maximum` is an unbounded page size.
    """
    unbounded = [
        f"{relation.name}.{arg.name}"
        for relation in declared_relations().values()
        for arg in relation.args
        if not _is_constrained(arg.published(), arg.type_name)
    ]
    assert unbounded == []


# --------------------------------------------------------------------------
# Binding
# --------------------------------------------------------------------------


def _slots(node: Any) -> list[str]:
    """Every `{"$arg": name}` slot in a binding body, in the #2100 grammar."""
    if isinstance(node, dict):
        if set(node) == {"$arg"} and isinstance(node["$arg"], str):
            return [node["$arg"]]
        return [name for value in node.values() for name in _slots(value)]
    if isinstance(node, list):
        return [name for item in node for name in _slots(item)]
    return []


def test_the_deployment_binds_both_relations_to_the_local_container():
    bindings = declared_bindings()
    assert sorted(bindings) == ["documents", "histogram"]
    for binding in bindings.values():
        assert binding.kind == "http"
        # A local, unauthenticated, disposable container: no credential
        # enters this repository, so there is nothing to resolve.
        assert binding.secret_refs == ()
        config = binding.config
        assert config["base_url"] == "http://127.0.0.1:9209"
        assert config["allowed_hosts"] == ["127.0.0.1:9209"]
        assert config["allow_non_global"] is True
        assert config["method"] == "POST"
        # No slot may appear in a path, and a query string is not supported.
        assert config["path"].startswith("/")
        assert "?" not in config["path"] and "#" not in config["path"]
        assert "$arg" not in config["path"]
        assert 0 < config["max_rows"] <= 10_000


def test_every_declared_argument_is_slotted_and_every_slot_is_declared():
    """Both directions, because each silently breaks a different way.

    A declared argument with no slot is published by `inspect` and does
    nothing: an agent varies it and gets the same answer. A slot naming no
    declared argument is a request body with a hole in it.
    """
    relations = declared_relations()
    for name, binding in declared_bindings().items():
        slotted = set(_slots(binding.config["body"]))
        declared = {arg.name for arg in relations[name].args}
        assert slotted == declared, name


def test_bind_pairs_every_declaration_with_its_binding():
    bound = bind_live_relations(declared_relations(), declared_bindings())
    assert sorted(bound) == ["documents", "histogram"]
    assert bound["documents"].binding.kind == "http"
    assert bound["documents"].relation.action == ACTION


# --------------------------------------------------------------------------
# The gate
# --------------------------------------------------------------------------


def test_the_shipped_bundle_allows_the_declared_action_for_the_service():
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    outcome = gate(
        declared_relations()["documents"], SERVICE_PRINCIPAL, "os:idx:1",
    )
    assert outcome.allowed is True
    assert outcome.action == ACTION
    assert outcome.reason == "opensearch_reference.live_search_allowed"
    assert outcome.raised is False


@pytest.mark.parametrize(
    "principal",
    ["service:some-other-runtime", "user:alice", "service:okg-runtime-2"],
)
def test_any_other_principal_is_denied(principal: str):
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    outcome = gate(declared_relations()["documents"], principal, "os:idx:1")
    assert outcome.allowed is False
    assert outcome.reason == "nomos.default_deny"


def test_a_missing_bundle_denies_rather_than_defaulting_open(tmp_path: Path):
    """The failure case for the bundle itself.

    A deployment directory with no `nomos_external_probe.json` gets a gate
    that denies every call, with the reason naming the absent bundle. Nothing
    about a missing file is allowed to read as permission.
    """
    (tmp_path / DEPLOYMENT).mkdir()
    gate = build_live_gate(DEPLOYMENT, deployments_root=tmp_path)
    outcome = gate(
        declared_relations()["documents"], SERVICE_PRINCIPAL, "os:idx:1",
    )
    assert outcome.allowed is False
    assert outcome.reason == "nomos.default_deny.no_active_bundle"


def test_the_bundle_denies_by_default_and_names_one_allow_rule():
    graph = json.loads(BUNDLE.read_text(encoding="utf-8"))
    assert graph["default_effect"] == "deny"
    assert [rule["effect"] for rule in graph["rules"]] == ["allow"]
    assert graph["rules"][0]["actions"] == [ACTION]
    assert graph["rules"][0]["subjects"] == [SERVICE_PRINCIPAL]
    # The rule names the two relations it authorizes. Matching on action
    # plus subtype alone would authorize any FUTURE relation that reused
    # `external.opensearch.search` on `search_index` -- an `_msearch` or
    # `_sql` binding would arrive pre-approved with no bundle edit, which
    # is the opposite of what a deny-by-default bundle is for.
    assert graph["rules"][0]["match"]["relation"] == [
        "documents", "histogram",
    ], graph["rules"][0]["match"]


def test_a_new_relation_reusing_the_action_is_not_authorized_by_default():
    """The failure case for the bundle's `relation` match.

    A third relation on the same subtype, declaring the same action, must
    be DENIED until someone edits the bundle. Built by hand rather than
    added to the ontology, because the point is what the POLICY does with a
    relation the operator never reviewed.
    """
    from okg.substrate.external_backed.live_relations import LiveRelation

    exfiltrate = LiveRelation(
        name="exfiltrate",
        subtype=SUBTYPE,
        action=ACTION,
        principal_mode="service",
        deadline_seconds=10.0,
    )
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    outcome = gate(exfiltrate, SERVICE_PRINCIPAL, "os:idx:1")
    assert outcome.allowed is False, outcome
    assert outcome.reason == "nomos.default_deny"

    # The control: the same gate, the same principal, the same action, and
    # a relation the bundle DOES name. Without this the assertion above
    # would pass on a gate that denied everything.
    allowed = gate(
        declared_relations()["documents"], SERVICE_PRINCIPAL, "os:idx:1",
    )
    assert allowed.allowed is True, allowed


def test_both_bodies_bound_the_cost_on_the_cluster_side():
    """`deadline_seconds` bounds this side of the wire only.

    It abandons the client's thread; the cluster keeps executing. A
    `query_string` accepts `*`, a leading wildcard and a regex, so without
    a shard-level cap one agent's typo is a full scan the graph cannot
    call off. Both literals are operator config, in every body, and this
    deployment is the reference others copy.
    """
    for name, binding in declared_bindings().items():
        body = binding.config["body"]
        assert body.get("terminate_after") == 10_000, name
        assert body.get("timeout") == "5s", name
        # Literals, not slots: a caller must not be able to raise its own
        # cost bound.
        assert _slots(body.get("terminate_after")) == [], name
        assert _slots(body.get("timeout")) == [], name


# --------------------------------------------------------------------------
# Dispatch, against a FAKE adapter
# --------------------------------------------------------------------------


class _FakeAdapter:
    """Stands in for #2100. Records what it was handed; never calls out."""

    def __init__(self, rows: Any = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.rows = rows if rows is not None else [{"message": "disk full"}]

    def __call__(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return self.rows


class _RefusingAdapter:
    """Fails the test if it is ever entered."""

    def __call__(self, **kwargs: Any) -> Any:  # pragma: no cover
        raise AssertionError("a denied live call reached the adapter")


def _dispatch(relation: str, args: dict[str, Any], *, adapter, gate):
    bound = bound_relation(relation)
    samples: list[dict[str, Any]] = []
    envelope = dispatch_live_relation(
        bound=bound,
        args_model=compile_args_model(bound.relation),
        args=args,
        node_id="os:search_index:abc",
        gate=gate,
        adapter=adapter,
        write_sample=lambda **kw: samples.append(kw),
        generation_id=42,
        # No response budget: a direct call, outside MCP.
        room=None,
    )
    return envelope, samples


def test_an_allowed_call_returns_the_external_live_envelope():
    adapter = _FakeAdapter()
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    envelope, samples = _dispatch(
        "documents",
        {"index": "app-logs-2026.09", "query": "disk"},
        adapter=adapter,
        gate=gate,
    )

    assert envelope["state"] == "ok"
    assert envelope["relation"] == "documents"
    # Beside the graph rows, never merged into them, and labelled.
    assert envelope["trust"] == "untrusted"
    assert envelope["source"] == "http"
    assert envelope["pinned_generation_id"] == 42

    # The DEFAULTS were applied before the adapter saw the arguments, so the
    # adapter is handed the validated model dump rather than the caller's
    # partial object.
    assert len(adapter.calls) == 1
    assert adapter.calls[0]["args"] == {
        "index": "app-logs-2026.09",
        "query": "disk",
        "time_window": "now-7d",
        "limit": 10,
    }
    assert adapter.calls[0]["deadline_seconds"] == 10.0
    assert adapter.calls[0]["config"]["base_url"] == "http://127.0.0.1:9209"

    assert len(samples) == 1
    assert samples[0]["decision"] == "allow"
    assert samples[0]["status"] == "ok"
    assert samples[0]["attrs"]["adapter_ran"] is True
    assert samples[0]["attrs"]["action"] == ACTION


def test_a_denied_call_never_enters_the_adapter():
    """Counted at the adapter, not read off the returned state.

    A state check cannot tell "denied" from "called, then discarded".
    """
    real_gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)

    def denying_gate(relation, principal, node_id):
        return real_gate(relation, "service:not-the-runtime", node_id)

    envelope, samples = _dispatch(
        "histogram",
        {"index": "app-logs-2026.09", "query": "disk"},
        adapter=_RefusingAdapter(),
        gate=denying_gate,
    )
    assert envelope["state"] == "denied"
    assert envelope["error"]["reason"] == "nomos.default_deny"
    assert samples[0]["decision"] == "deny"
    assert samples[0]["attrs"]["adapter_ran"] is False


def test_an_unknown_argument_is_refused_by_name_before_the_gate():
    """A malformed argument is a client error, never a policy question."""
    with pytest.raises(LiveArgumentsInvalid) as excinfo:
        _dispatch(
            "documents",
            {"index": "app-logs-2026.09", "query": "disk", "siz": 5},
            adapter=_RefusingAdapter(),
            gate=build_live_gate(
                DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT,
            ),
        )
    assert "siz" in str(excinfo.value)


def test_an_out_of_range_argument_is_refused_rather_than_clamped():
    with pytest.raises(LiveArgumentsInvalid) as excinfo:
        _dispatch(
            "documents",
            {"index": "app-logs-2026.09", "query": "disk", "limit": 5_000},
            adapter=_RefusingAdapter(),
            gate=build_live_gate(
                DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT,
            ),
        )
    assert "limit" in str(excinfo.value)


# --------------------------------------------------------------------------
# The adapter boundary, stated as a test
# --------------------------------------------------------------------------


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *args, **kwargs):
        return None

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cursor(self._rows)


class _Session:
    catalog_version_id = 1


def _catalog_conn() -> _Conn:
    metadata = {
        name: relation.to_metadata()
        for name, relation in declared_relations().items()
    }
    return _Conn([(SUBTYPE, {"live_relations": metadata})])


def test_the_deployment_refuses_to_start_without_the_http_runner():
    """The honest state of this change until issue #2100 lands.

    The substrate ships no `http` live adapter, so `build_server` refuses
    this deployment by name at LOAD. Asserted rather than worked around: a
    stub runner registered in production wiring would turn a refusal nobody
    can miss into a live call that silently returns nothing.
    """
    with pytest.raises(LiveAdapterUnavailable) as excinfo:
        assert_live_bindings_are_declared(
            _catalog_conn(), _Session(), declared_bindings(),
            adapter_runners={},
        )
    assert "http" in str(excinfo.value)


def test_the_cross_check_passes_once_an_http_runner_is_registered():
    """The same check with a runner present.

    So the refusal above is the only thing missing -- not the declaration,
    not the binding, not the catalog.
    """
    declared = assert_live_bindings_are_declared(
        _catalog_conn(),
        _Session(),
        declared_bindings(),
        adapter_runners={"http": _FakeAdapter()},
    )
    assert declared == {"documents": SUBTYPE, "histogram": SUBTYPE}


def test_a_stored_declaration_round_trips_through_the_catalog():
    """What the catalog stores is what the dispatcher rebuilds.

    `to_metadata` withholds nothing the dispatcher needs; an earlier shape
    stored only the published subset and lost the deadline and the principal
    mode, so the call ran unbounded under an unstated principal.
    """
    for name, relation in declared_relations().items():
        rebuilt = LiveRelation.from_metadata(
            name, SUBTYPE, relation.to_metadata(),
        )
        # Compared field by field with `args` SORTED on both sides.
        # `parse_live_relations` keeps the operator's declaration order and
        # `from_metadata` sorts, so a plain `==` on the two objects is
        # false for any relation whose arguments are not already in
        # alphabetical order. That asymmetry belongs to #2013 and is
        # reported there rather than worked around in its own module; what
        # matters to this change is that no VALUE is lost, which is what is
        # asserted here.
        assert sorted(rebuilt.args, key=lambda a: a.name) == sorted(
            relation.args, key=lambda a: a.name,
        )
        for field in (
            "action", "principal_mode", "sample_policy", "retention_class",
            "deadline_seconds", "subtype",
        ):
            assert getattr(rebuilt, field) == getattr(relation, field), field


def test_the_binding_config_is_not_validated_here_and_says_so():
    """`parse_live_bindings` checks the binding's SHAPE, not its config.

    Config validation -- the egress allowlist, the slot grammar, the
    unconstrained-slot refusal -- is `validate_live_binding_configs` in
    #2100. Pinned so that a later reader does not mistake this parser's
    silence for approval of the body below.
    """
    bindings = parse_live_bindings(
        {"documents": {"kind": "http", "config": {"nonsense": True}}},
    )
    assert bindings["documents"].config == {"nonsense": True}
