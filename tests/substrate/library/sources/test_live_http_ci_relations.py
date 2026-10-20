"""The live-http-ci fixture's two live relations, up to the adapter.

PACT ``laptop-monitor-live-example`` (issue #2164). Every test here is
hermetic: no Glances, no database, no network. Its companion,
``tests/substrate/e2e/test_live_relation_ci_walk.py``, starts a real Glances
and drives the same bindings through a real server; this file is what runs in
the fast lane on every pull request. What it proves is everything on
THIS side of the adapter boundary --

* ``compute_env``'s ``Machine`` declares ``cpu_per_core`` and ``top_processes``
  in the shape issue #2013 defines, with ZERO arguments;
* the deployment binds both as ``GET`` with no body, no slot in the path, and
  a bound a caller cannot raise;
* ``inspect`` lists both relations with their schemas;
* the shipped Nomos bundle allows ``external.host.read_telemetry`` for the
  runtime service principal on ``machine`` and denies everything else,
  including a third relation reusing the action and the absent-bundle case;
* one dispatch through ``dispatch_live_relation`` with a FAKE adapter returns
  typed rows in the ``external_live`` envelope, and a denied call never enters
  the adapter.

This is the SECOND live system, after the OpenSearch reference (PR #2167).
The point of it is the difference: OpenSearch is POST with a slotted JSON body
and an object-rooted response, this is GET with no body at all and a top-level
array. The two share no code path beyond the adapter itself, which is the only
way to tell a generic adapter from one shaped around its first caller.

The deployment under test is a FIXTURE, under ``tests/substrate/e2e/fixtures/``
rather than in the shipped ``deployments/`` tree: it exists so CI can exercise
this path, and nothing about it is user-facing.
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
    parse_live_relations,
)
from okg.substrate.mcp.effective_surface import parse_agent_surface
from okg.substrate.mcp.subtype_summary import catalog_subtype_shape
from tests.mirror_scope import private_path

DEPLOYMENT = "live-http-ci"
SUBTYPE = "machine"
ACTION = "external.host.read_telemetry"
SERVICE_PRINCIPAL = "service:okg-runtime"
RELATIONS = ["cpu_per_core", "top_processes"]
BASE_URL = "http://127.0.0.1:61208"

_REPO = Path(__file__).resolve().parents[4]
#: The fixture root, not `deployments/`. `build_live_gate` takes the root as
#: a parameter for exactly this reason.
DEPLOYMENTS_ROOT = _REPO / "tests/substrate/e2e/fixtures"
MANIFEST = DEPLOYMENTS_ROOT / DEPLOYMENT / "deployment.yaml"
BUNDLE = DEPLOYMENTS_ROOT / DEPLOYMENT / "nomos_external_probe.json"
REGISTRY = DEPLOYMENTS_ROOT / DEPLOYMENT / "source_registry.yaml"
#: The declaration lives in THIS DEPLOYMENT's schemas/, not in the shared
#: `compute_env` module. See `test_the_shared_compute_env_module_declares_no_
#: live_relations` below for why that distinction is load-bearing.
SCHEMA = DEPLOYMENTS_ROOT / DEPLOYMENT / "schemas/machine_live_relations.yaml"
LIBRARY_SCHEMA = (
    _REPO / "src/okg/substrate/library/ontologies/compute_env/schema.yaml"
)

#: One `percpu` row, as Glances 4.5.6 actually returns it. Copied from a live
#: response rather than invented, so the hermetic tests and the CI walk assert
#: the same field names. The version is pinned in `pyproject.toml`, so this
#: shape and the running service's cannot drift apart silently.
PERCPU_ROW = {
    "key": "cpu_number", "cpu_number": 0, "total": 31.2, "user": 12.5,
    "system": 8.3, "idle": 68.8, "nice": 0.0, "iowait": None, "irq": None,
    "softirq": None, "steal": None, "guest": None, "guest_nice": None,
    "dpc": None, "interrupt": None,
}
PROCESS_ROW = {
    "pid": 96504, "name": "python3.12", "username": "riley", "status": "S",
    "cpu_percent": 99.1, "memory_percent": 1.2, "num_threads": 17,
    "nice": 0, "key": "pid", "cmdline": ["python3.12", "-m", "pytest"],
}


def machine_annotations() -> dict[str, Any]:
    doc = yaml.safe_load(SCHEMA.read_text(encoding="utf-8"))
    return doc["classes"]["Machine"]["annotations"]


def declared_relations() -> dict[str, LiveRelation]:
    return parse_live_relations(machine_annotations(), subtype=SUBTYPE)


def declared_bindings() -> dict[str, Any]:
    raw = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    surface = parse_agent_surface(raw, manifest_path=MANIFEST)
    assert surface is not None, "the manifest declares an agent_surface block"
    return dict(surface.live_bindings)


def bound_relation(name: str) -> BoundLiveRelation:
    return bind_live_relations(declared_relations(), declared_bindings())[name]


# --------------------------------------------------------------------------
# The node: reused, not rebuilt
# --------------------------------------------------------------------------


def test_the_deployment_mints_its_machine_node_from_the_existing_source():
    """req.lmle.node. No new source adapter, and none needed.

    `compute_env`'s LocalEnvSource already probes the local host. A second
    live system that had to ship its own source adapter would not be testing
    the HTTP adapter's genericity, it would be testing a new adapter.
    """
    registry = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))
    source = registry["sources"]["local-env"]
    assert source["module"] == "okg.substrate.library.sources.compute_env_local"
    assert source["class"] == "LocalEnvSource"

    admitted = {
        node["subtype"]
        for node in source["admission_policy"]["output_signature"]["nodes"]
    }
    assert SUBTYPE in admitted
    # The source ALSO emits `mcp_server` nodes from the operator's
    # settings.json. Left out on purpose: an inventory of somebody's own
    # tooling has no part in "what is my machine doing", and an emission with
    # no clause is refused by name at ingest rather than stored quietly.
    assert "mcp_server" not in admitted


def test_the_shared_compute_env_module_declares_no_live_relations(tmp_path):
    """req.lmle.relations. The declaration must not leak to other deployments.

    `Machine` is a shared library subtype: okg-workspace, okg-ci and
    okg-ci-dev all enrol it and none of them binds a host-telemetry
    adapter. A declaration on the library class is published by `inspect` in
    every one of them, so an agent there would be told it can ask any machine
    node for its process table and be refused on the first call.

    Asserted against the COMPOSED catalogs, not just the YAML, because the
    composer is what decides: it reads a deployment's own `schemas/*.yaml`
    through the same `_extract_subtypes` path it reads a module's, which is
    the whole reason the declaration can live here at all.
    """
    import yaml as _yaml

    from okg.substrate.catalog.modules_compose import compose_catalog

    library = _yaml.safe_load(LIBRARY_SCHEMA.read_text(encoding="utf-8"))
    assert "okg_live_relations" not in (
        library["classes"]["Machine"].get("annotations") or {}
    ), "compute_env's Machine must stay free of deployment-specific relations"

    mine = compose_catalog(DEPLOYMENTS_ROOT / DEPLOYMENT)
    assert sorted(
        mine.subtype_metadata["machine"]["live_relations"],
    ) == RELATIONS

    # The control, and the assertion that actually protects the other
    # deployments: a catalog that enrols `compute_env` WITHOUT this
    # deployment's schemas carries no live relation on `machine`. This is the
    # same fixture with its schemas directory emptied.
    control = tmp_path / DEPLOYMENT
    (control / "schemas").mkdir(parents=True)
    for name in ("deployment.yaml", "source_registry.yaml"):
        (control / name).write_bytes((DEPLOYMENTS_ROOT / DEPLOYMENT / name).read_bytes())
    theirs = compose_catalog(control)
    assert "machine" in theirs.subtype_metadata, "the control must still enrol compute_env"
    assert "live_relations" not in theirs.subtype_metadata["machine"]


def test_the_workspace_deployment_carries_no_live_relation_on_machine():
    """The same control against this repository's own okg-workspace deployment.

    It is private; the public copy skips this test (tests/mirror_scope.py).
    """
    from okg.substrate.catalog.modules_compose import compose_catalog

    theirs = compose_catalog(private_path("deployments/okg-workspace"))
    assert "live_relations" not in theirs.subtype_metadata.get("machine", {})


def test_a_machine_is_findable_by_name_and_not_only_by_id():
    """req.lmle.node. The walk STARTS with a search.

    A node reachable only by id is no use to an agent told "my machine": it
    has no id to start from. Without these annotations the whole walk has no
    first step.
    """
    annotations = machine_annotations()
    fields = annotations["okg_searchable_text_fields"].split(",")
    assert "hostname" in fields
    assert annotations["okg_searchable_text_path_fields"] == "hostname"


# --------------------------------------------------------------------------
# Declaration
# --------------------------------------------------------------------------


def test_machine_declares_both_relations_with_the_declared_policy():
    relations = declared_relations()
    assert sorted(relations) == RELATIONS
    for relation in relations.values():
        # ONE action for BOTH relations. That is what makes the bundle's
        # `relation` match below the only thing separating these two from a
        # third that reused the action.
        assert relation.action == ACTION
        # Service is the only mode served this sprint; `caller` is refused at
        # deployment load.
        assert relation.principal_mode == "service"
        # The answer to "what is my machine doing" has to be citable, so the
        # rows stay with the sample.
        assert relation.sample_policy == "inline"
        # `hot` (7 days), not `warm` (90). These rows carry process names,
        # usernames and full command lines off the operator's own machine;
        # `inline` keeps them because the sample is the proof, and the
        # retention class is the one dial that bounds how long.
        assert relation.retention_class == "hot"
        assert 0 < relation.deadline_seconds <= 120


def test_neither_relation_declares_an_argument():
    """req.lmle.relations, and it is a CONSTRAINT, not an omission.

    The HTTP adapter refuses a `GET` that declares any argument: a GET has no
    body and query-string arguments are not supported yet, so `inspect` would
    otherwise advertise a parameter that does nothing. Glances is GET-only --
    its OpenAPI lists POST on four mutating routes and `POST /api/4/all`
    answers 405 -- so an argument here is not expressible at all.

    Asserted rather than left implicit because an argument added to the
    ontology later would be refused at DEPLOYMENT LOAD, and the operator
    would meet it as a deployment that stopped starting.
    """
    for name, relation in declared_relations().items():
        assert relation.args == (), name


def test_inspect_lists_both_relations_with_their_schemas():
    """req.lmle.hermetic, through the real `inspect` code path.

    `catalog_subtype_shape` is what `inspect` calls. Driven here off a fake
    cursor holding exactly what the catalog stamps, so the published shape is
    rendered by the shipped code rather than restated by the test.
    """
    metadata = {
        "live_relations": {
            name: relation.to_metadata()
            for name, relation in declared_relations().items()
        },
    }
    shape = catalog_subtype_shape(
        _Conn([(metadata,), []]), _Session(), SUBTYPE,
    )
    published = shape["live_relations"]
    assert sorted(published) == RELATIONS
    for name in RELATIONS:
        assert published[name]["action"] == ACTION
        assert published[name]["args"] == {}
        # `principal_mode` and `deadline_seconds` are operator facts a caller
        # cannot act on, so `inspect` withholds them. Pinned so a later widening
        # of the published subset is a decision somebody makes on purpose.
        assert "principal_mode" not in published[name]
        assert "deadline_seconds" not in published[name]


# --------------------------------------------------------------------------
# Binding
# --------------------------------------------------------------------------


def test_the_deployment_binds_both_relations_to_the_local_glances():
    bindings = declared_bindings()
    assert sorted(bindings) == RELATIONS
    for name, binding in bindings.items():
        assert binding.kind == "http"
        # Glances is loopback-bound and unauthenticated: no credential enters
        # this repository, so there is nothing to resolve.
        assert binding.secret_refs == ()
        config = binding.config
        assert config["base_url"] == BASE_URL
        # The port is EXACT. A bare `127.0.0.1` in the allowlist would
        # authorise every other service on this laptop.
        assert config["allowed_hosts"] == ["127.0.0.1:61208"]
        assert config["allow_non_global"] is True
        assert config["method"] == "GET", name
        # A GET carries no body, and a binding that set one would be refused
        # at load. Asserted here so the refusal is never the first time
        # anybody finds out.
        assert "body" not in config, name
        assert config["path"].startswith("/")
        assert "?" not in config["path"] and "#" not in config["path"]
        assert "$arg" not in json.dumps(config), name
        assert 0 < config["max_rows"] <= 10_000, name


def test_the_only_bound_a_caller_cannot_raise_is_the_one_in_the_manifest():
    """req.lmle.relations. Two literals, both operator config.

    `deadline_seconds` bounds this side of the wire only: it abandons the
    client's thread while the server keeps working. With no argument to carry
    a limit, the bound on the ANSWER is the literal in the path -- Glances
    cuts the list server-side -- and `max_rows`, which cuts again here. A
    server that stopped honouring the path is still bounded.
    """
    bindings = declared_bindings()
    assert bindings["top_processes"].config["path"].endswith("/top/5")
    assert bindings["top_processes"].config["max_rows"] == 8
    assert bindings["cpu_per_core"].config["path"] == "/api/4/percpu"


def test_bind_pairs_every_declaration_with_its_binding():
    bound = bind_live_relations(declared_relations(), declared_bindings())
    assert sorted(bound) == RELATIONS
    assert bound["cpu_per_core"].binding.kind == "http"
    assert bound["cpu_per_core"].relation.action == ACTION


# --------------------------------------------------------------------------
# The gate
# --------------------------------------------------------------------------


@pytest.mark.parametrize("relation", RELATIONS)
def test_the_shipped_bundle_allows_both_relations_for_the_service(relation):
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    outcome = gate(
        declared_relations()[relation], SERVICE_PRINCIPAL, "machine:ci-host",
    )
    assert outcome.allowed is True
    assert outcome.action == ACTION
    assert outcome.reason == "live_http_ci.host_telemetry_allowed"
    assert outcome.raised is False


@pytest.mark.parametrize(
    "principal",
    ["service:some-other-runtime", "user:riley", "service:okg-runtime-2"],
)
def test_any_other_principal_is_denied(principal: str):
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    outcome = gate(
        declared_relations()["cpu_per_core"], principal, "machine:ci-host",
    )
    assert outcome.allowed is False
    assert outcome.reason == "nomos.default_deny"


def test_a_missing_bundle_denies_rather_than_defaulting_open(tmp_path: Path):
    """The failure case for the bundle itself.

    Nothing about an absent file is allowed to read as permission.
    """
    (tmp_path / DEPLOYMENT).mkdir()
    gate = build_live_gate(DEPLOYMENT, deployments_root=tmp_path)
    outcome = gate(
        declared_relations()["cpu_per_core"], SERVICE_PRINCIPAL,
        "machine:ci-host",
    )
    assert outcome.allowed is False
    assert outcome.reason == "nomos.default_deny.no_active_bundle"


def test_the_bundle_denies_by_default_and_names_both_relations():
    graph = json.loads(BUNDLE.read_text(encoding="utf-8"))
    assert graph["default_effect"] == "deny"
    assert [rule["effect"] for rule in graph["rules"]] == ["allow"]
    assert graph["rules"][0]["actions"] == [ACTION]
    assert graph["rules"][0]["subjects"] == [SERVICE_PRINCIPAL]
    # Matching on action plus subtype ALONE would authorise any future
    # relation that reused `external.host.read_telemetry` on `machine` -- a
    # `read_env` or a `list_open_files` would arrive pre-approved with no
    # bundle edit, which is the opposite of what a deny-by-default bundle is
    # for. Both relations here share one action, so this match is the only
    # thing standing between the two that were reviewed and a third.
    assert graph["rules"][0]["match"]["relation"] == RELATIONS


def test_a_third_relation_reusing_the_action_is_dispatched_and_denied():
    """req.lmle.hermetic, through `dispatch_live_relation` itself.

    `test_a_third_relation_reusing_the_action_is_denied` below asks the GATE
    what it thinks. This asks the DISPATCHER, which is the thing an agent
    actually reaches, and counts the adapter calls: `_RefusingAdapter` raises
    if it is entered at all, so "zero adapter calls" is a fact about the run
    rather than a reading of the returned state.

    The relation is built by hand and bound to `cpu_per_core`'s own binding,
    so the only difference from a call that works is its NAME -- which is
    exactly what the bundle's `relation` match is there to catch.
    """
    list_open_files = LiveRelation(
        name="list_open_files",
        subtype=SUBTYPE,
        action=ACTION,
        principal_mode="service",
        sample_policy="inline",
        deadline_seconds=10.0,
    )
    bound = BoundLiveRelation(
        relation=list_open_files,
        binding=declared_bindings()["cpu_per_core"],
    )
    samples: list[dict[str, Any]] = []
    envelope = dispatch_live_relation(
        bound=bound,
        args_model=compile_args_model(list_open_files),
        args={},
        node_id="machine:ci-host",
        gate=build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT),
        adapter=_RefusingAdapter(),
        write_sample=lambda **kw: samples.append(kw),
        generation_id=42,
        # No response budget: a direct call, outside MCP.
        room=None,
    )
    assert envelope["state"] == "denied", envelope
    assert envelope["error"]["reason"] == "nomos.default_deny"
    assert samples[0]["decision"] == "deny"
    assert samples[0]["attrs"]["adapter_ran"] is False

    # The control: the SAME dispatcher, the same principal, the same action
    # and a relation the bundle does name, reaching a counting adapter.
    allowed = _FakeAdapter()
    ok = _dispatch(
        "cpu_per_core", {}, adapter=allowed,
        gate=build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT),
    )[0]
    assert ok["state"] == "ok", ok
    assert len(allowed.calls) == 1


def test_a_third_relation_reusing_the_action_is_denied():
    """req.lmle.gate. Built by hand, not added to the ontology.

    The point is what the POLICY does with a relation the operator never
    reviewed, so it must not be one the ontology blesses.
    """
    list_open_files = LiveRelation(
        name="list_open_files",
        subtype=SUBTYPE,
        action=ACTION,
        principal_mode="service",
        deadline_seconds=10.0,
    )
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    outcome = gate(list_open_files, SERVICE_PRINCIPAL, "machine:ci-host")
    assert outcome.allowed is False, outcome
    assert outcome.reason == "nomos.default_deny"

    # The control: same gate, same principal, same action, a relation the
    # bundle DOES name. Without it the assertion above would pass on a gate
    # that denied everything.
    allowed = gate(
        declared_relations()["cpu_per_core"], SERVICE_PRINCIPAL,
        "machine:ci-host",
    )
    assert allowed.allowed is True, allowed


# --------------------------------------------------------------------------
# Dispatch, against a FAKE adapter
# --------------------------------------------------------------------------


class _FakeAdapter:
    """Stands in for the HTTP adapter. Records what it was handed."""

    def __init__(self, rows: Any = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.rows = rows if rows is not None else [PERCPU_ROW]

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
        node_id="machine:ci-host",
        gate=gate,
        adapter=adapter,
        write_sample=lambda **kw: samples.append(kw),
        generation_id=42,
        # No response budget: a direct call, outside MCP.
        room=None,
    )
    return envelope, samples


def test_an_allowed_call_returns_typed_rows_in_the_live_envelope():
    adapter = _FakeAdapter([PERCPU_ROW, {**PERCPU_ROW, "cpu_number": 1}])
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    envelope, samples = _dispatch(
        "cpu_per_core", {}, adapter=adapter, gate=gate,
    )

    assert envelope["state"] == "ok"
    assert envelope["relation"] == "cpu_per_core"
    # Beside the graph rows, never merged into them, and labelled: a laptop's
    # process table is text this graph does not control.
    assert envelope["trust"] == "untrusted"
    assert envelope["source"] == "http"
    assert envelope["pinned_generation_id"] == 42

    assert len(adapter.calls) == 1
    # NO arguments reach the adapter, because none are declared. The relation
    # is entirely operator-shaped.
    assert adapter.calls[0]["args"] == {}
    assert adapter.calls[0]["deadline_seconds"] == 10.0
    assert adapter.calls[0]["config"]["base_url"] == BASE_URL

    assert len(samples) == 1
    assert samples[0]["decision"] == "allow"
    assert samples[0]["status"] == "ok"
    assert samples[0]["attrs"]["adapter_ran"] is True
    assert samples[0]["attrs"]["action"] == ACTION


def test_the_rows_keep_the_fields_the_upstream_sent():
    """The rows are typed, not stringified, on the way back out.

    A per-core row whose numbers arrived as text would read fine in a chat
    answer and be useless to anything that compared them.
    """
    gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)
    envelope, _ = _dispatch(
        "cpu_per_core", {}, adapter=_FakeAdapter([PERCPU_ROW]), gate=gate,
    )
    row = envelope["rows"][0]
    assert row["cpu_number"] == 0
    assert isinstance(row["total"], float)
    assert isinstance(row["idle"], float)


def test_a_denied_call_never_enters_the_adapter():
    """req.lmle.hermetic. Counted AT the adapter, not read off the state.

    A state check cannot tell "denied" from "called, then discarded", and the
    difference is the whole claim: a refused relation must not reach the
    laptop at all.
    """
    real_gate = build_live_gate(DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT)

    def denying_gate(relation, principal, node_id):
        return real_gate(relation, "service:not-the-runtime", node_id)

    envelope, samples = _dispatch(
        "top_processes", {}, adapter=_RefusingAdapter(), gate=denying_gate,
    )
    assert envelope["state"] == "denied"
    assert envelope["error"]["reason"] == "nomos.default_deny"
    assert samples[0]["decision"] == "deny"
    assert samples[0]["attrs"]["adapter_ran"] is False


def test_an_argument_to_an_argumentless_relation_is_refused_by_name():
    """A malformed call is a client error, never a policy question.

    These relations take nothing, so ANY keyword is unknown. It is refused
    before the gate is consulted and before the adapter is reached.
    """
    with pytest.raises(LiveArgumentsInvalid) as excinfo:
        _dispatch(
            "top_processes", {"limit": 5}, adapter=_RefusingAdapter(),
            gate=build_live_gate(
                DEPLOYMENT, deployments_root=DEPLOYMENTS_ROOT,
            ),
        )
    assert "limit" in str(excinfo.value)


# --------------------------------------------------------------------------
# The adapter boundary
# --------------------------------------------------------------------------


class _Cursor:
    def __init__(self, results):
        self._results = list(results)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *args, **kwargs):
        return None

    def fetchone(self):
        return self._results.pop(0) if self._results else None

    def fetchall(self):
        return self._results.pop(0) if self._results else []


class _Conn:
    def __init__(self, results):
        self._results = results

    def cursor(self):
        return _Cursor(self._results)


class _Session:
    catalog_version_id = 1


def _catalog_conn() -> _Conn:
    metadata = {
        name: relation.to_metadata()
        for name, relation in declared_relations().items()
    }
    return _Conn([[(SUBTYPE, {"live_relations": metadata})]])


def test_the_cross_check_pairs_both_relations_with_the_machine_subtype():
    declared = assert_live_bindings_are_declared(
        _catalog_conn(),
        _Session(),
        declared_bindings(),
        adapter_runners={"http": _FakeAdapter()},
    )
    assert declared == {
        "cpu_per_core": SUBTYPE, "top_processes": SUBTYPE,
    }


def test_the_deployment_refuses_to_start_without_an_http_runner():
    """An unregistered adapter kind is a refusal at LOAD, not at call time.

    Asserted rather than assumed: a deployment that started and then failed
    every live call would look like a broken laptop rather than a broken
    manifest.
    """
    with pytest.raises(LiveAdapterUnavailable) as excinfo:
        assert_live_bindings_are_declared(
            _catalog_conn(), _Session(), declared_bindings(),
            adapter_runners={},
        )
    assert "http" in str(excinfo.value)


def test_the_http_adapter_accepts_both_bindings():
    """req.lmle.relations, against the REAL config validator.

    Everything above checks the binding against this change's own reading of
    the grammar. This one checks it against the grammar itself: the egress
    allowlist, the GET rules, and `rows_path`. It is the test that fails if
    the adapter and this deployment ever disagree about what a legal GET
    binding is.
    """
    from okg.substrate.external_backed.http_adapter import parse_http_binding

    relations = declared_relations()
    for name, binding in declared_bindings().items():
        config = parse_http_binding(name, binding, relations[name])
        assert config.method == "GET"
        assert config.body is None
        assert config.url.startswith(BASE_URL)
