from pathlib import Path

import pytest

from okg.substrate.deployments import (
    DeploymentConfigError, PublisherConfig, _publisher_config_from_manifest,
)
from okg.substrate.publisher.publisher import Publisher
from okg.substrate.cli.deployment_readiness import _ready_publisher_lock_headroom_status


@pytest.mark.parametrize("raw", [
    {"publisher": None},
    {"publisher": {"first_publish_warning_budget": None}},
    {"publisher": {"unknown": 1}},
    {"publisher": {"hard_statement_timeout_seconds": True}},
    {"publisher": {"hard_statement_timeout_seconds": float("inf")}},
    {"publisher": {"first_publish_warning_budget": {"node_count": 1}}},
])
def test_strict_publisher_rejects_invalid_objects(raw):
    with pytest.raises(DeploymentConfigError):
        _publisher_config_from_manifest(raw, manifest_path=Path("deployment.yaml"))


@pytest.mark.parametrize("nodes,edges,first,triggered", [
    (2330000, 0, True, True),
    (0, 3650000, True, True),
    (2329999, 3649999, True, False),
    (2330000, 3650000, False, False),
])
def test_first_publish_thresholds(nodes, edges, first, triggered):
    publisher = object.__new__(Publisher)
    publisher.first_publish_warning_budget = {"node_count": 2330000, "edge_count": 3650000}
    publisher.first_publish_warning_budget_source = "deployment_manifest"
    result = publisher._initial_generation_budget_warning(
        first_publish=first, node_count=nodes, edge_count=edges,
    )
    assert result["triggered"] is triggered
    assert result["source"] == "deployment_manifest"
    assert result["node_count"] == nodes
    assert result["edge_count"] == edges


def test_readiness_environment_cannot_weaken_manifest_policy(monkeypatch):
    ledger = {"rows": [{"row_kind": "source", "readiness_required": True,
                       "latest_full_pipeline_lock_hold_p95_ms": 600000}]}
    before = _ready_publisher_lock_headroom_status(ledger, publisher_config=PublisherConfig())
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR", "1")
    monkeypatch.setenv("OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS", "9999")
    after = _ready_publisher_lock_headroom_status(ledger, publisher_config=PublisherConfig())
    assert before == after
    assert after["hard_timeout_source"] == "compatibility_default"
    assert after["policy"]["required_factor"] == 2.0


def test_direct_publisher_defaults_ignore_environment(monkeypatch):
    monkeypatch.setenv("OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS", "9999")
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR", "1")
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HEADROOM_ALLOW_RAISED_TIMEOUT", "true")
    publisher = Publisher("", "unit-only", history_retention_policy=None)
    receipt = publisher._publisher_configuration_receipt()
    assert receipt["hard_statement_timeout_ms"] == 900000
    assert receipt["hard_statement_timeout_source"] == "direct_default"
    assert receipt["lock_hold_headroom_factor"] == 2.0
    assert receipt["allow_raised_statement_timeout"] is False


@pytest.mark.parametrize("block,valid", [
    ({}, True),
    ({"hard_statement_timeout_seconds": 1200, "lock_hold_headroom_factor": 3,
      "allow_raised_statement_timeout": True, "kickoff_interval": "2m",
      "first_publish_warning_budget": {"node_count": 2330000, "edge_count": 3650000}}, True),
    (None, False),
    ({"first_publish_warning_budget": None}, False),
    ({"first_publish_warning_budget": {"node_count": 0, "edge_count": 1}}, False),
    ({"hard_statement_timeout_seconds": "900"}, False),
    ({"lock_hold_headroom_factor": 0.9}, False),
    ({"allow_raised_statement_timeout": 1}, False),
    ({"kickoff_interval": "1day"}, False),
    ({"unknown": 1}, False),
])
def test_schema_and_loader_accept_the_same_publisher_blocks(block, valid):
    import json
    from jsonschema import Draft202012Validator

    schema = json.loads((Path(__file__).parents[3] / "schemas/deployment.schema.json").read_text())
    assert Draft202012Validator(schema["properties"]["publisher"]).is_valid(block) is valid
    if valid:
        _publisher_config_from_manifest({"publisher": block}, manifest_path=Path("deployment.yaml"))
    else:
        with pytest.raises(DeploymentConfigError):
            _publisher_config_from_manifest({"publisher": block}, manifest_path=Path("deployment.yaml"))


def _authored_stack(tmp_path, monkeypatch):
    from okg.substrate.publisher import deployment_stack

    _stub_repository_provenance(monkeypatch)

    (tmp_path / "deployment.yaml").write_text('''name: budget-unit
publisher:
  hard_statement_timeout_seconds: 1200
  lock_hold_headroom_factor: 3
  allow_raised_statement_timeout: true
  first_publish_warning_budget:
    node_count: 2330000
    edge_count: 3650000
''')
    deployment_stack._clear_deployment_stack_cache()
    monkeypatch.setattr(deployment_stack, "build_runtime_catalog_identity", lambda **kwargs: None)
    return deployment_stack.build_deployment_publisher_stack(
        dsn="", deployment_name="budget-unit", deployment_dir=tmp_path,
    )


def _stub_repository_provenance(monkeypatch):
    from okg.substrate.publisher.repository_provenance import RepositoryProvenance

    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.tracking_repository_provenance",
        lambda _path: RepositoryProvenance.exact(
            core_revision="1" * 40,
            deployment_revision="2" * 40,
            deployment_tree_sha="3" * 40,
        ),
    )


def test_factory_preserves_manifest_authority_under_hostile_environment(tmp_path, monkeypatch):
    from okg.substrate.publisher.factory import build_deployment_publisher

    stack = _authored_stack(tmp_path, monkeypatch)
    monkeypatch.setenv("OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR", "1")
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HEADROOM_ALLOW_RAISED_TIMEOUT", "false")
    publisher = build_deployment_publisher(
        dsn="", deployment_name="budget-unit", command="unit",
        deployment_dir=tmp_path, stack=stack,
    )
    receipt = publisher._publisher_configuration_receipt()
    assert receipt == stack.publisher_config.authority_receipt()
    assert receipt["hard_statement_timeout_ms"] == 1200000
    assert receipt["hard_statement_timeout_source"] == "deployment_manifest"
    assert receipt["first_publish_warning_budget"] == {"node_count": 2330000, "edge_count": 3650000}


@pytest.mark.parametrize("key", [
    "hard_statement_timeout_ms", "hard_statement_timeout_source",
    "lock_hold_headroom_factor", "lock_hold_headroom_factor_source",
    "allow_raised_statement_timeout", "allow_raised_statement_timeout_source",
    "first_publish_warning_budget", "first_publish_warning_budget_source",
])
def test_factory_rejects_authority_override(tmp_path, monkeypatch, key):
    from okg.substrate.publisher.factory import build_deployment_publisher

    stack = _authored_stack(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="cannot replace deployment authority"):
        build_deployment_publisher(
            dsn="", deployment_name="budget-unit", command="unit",
            deployment_dir=tmp_path, stack=stack, publisher_overrides={key: None},
        )


def test_factory_tolerates_a_stack_that_declares_no_publisher_authority(
    tmp_path, monkeypatch,
):
    """A duck-typed stack must not crash the factory.

    `build_deployment_publisher` takes any object with a `publisher_kwargs()`,
    and several callers pass a minimal stand-in. Indexing the issue #1479
    authority keys unconditionally raised KeyError for every one of them
    (observed on tests/substrate/runtime/test_issue_979_source_sync_probe_permission.py).
    A key the stack never declared carries no deployment authority to protect,
    so it is simply absent and the publisher applies its documented default.
    """
    from okg.substrate.publisher import deployment_stack as stack_mod
    from okg.substrate.publisher.factory import build_deployment_publisher

    class _BareStack:
        deployment_name = "bare"
        resolution_aware_reuse_enabled = False

        def publisher_kwargs(self):
            return {}

    captured = {}

    class _Publisher:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(stack_mod, "publisher_kwargs_from_env", lambda: {})
    monkeypatch.setattr(
        stack_mod, "build_runtime_catalog_identity", lambda **_kw: None,
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.factory.Publisher", _Publisher, raising=False,
    )
    _stub_repository_provenance(monkeypatch)
    build_deployment_publisher(
        dsn="", deployment_name="bare", command="unit",
        deployment_dir=tmp_path, stack=_BareStack(),
    )
    for key in (
        "hard_statement_timeout_ms",
        "lock_hold_headroom_factor",
        "first_publish_warning_budget",
    ):
        assert key not in captured


def test_a_hostile_env_cannot_relax_the_recorded_headroom_policy(monkeypatch):
    """The environment may tighten the recorded bar. It may never loosen it.

    The receipt declares a strict 3x factor. An environment asking for 1x must
    not win, or an operator export would erase the deployment's own stricter
    policy. Withholding the raised-timeout authorization pulls the bound back
    to the 900s default, which only makes the verdict harder to pass: the
    receipt is the artifact under judgement, so it never gets to widen its own
    bar (see test_a_receipt_cannot_author_its_own_passing_headroom_verdict).
    """
    import json
    from okg.substrate.publish_lock_headroom import evaluate_lock_headroom, lock_headroom_from_payload

    recorded = evaluate_lock_headroom(
        hold_ms=500000, hard_timeout_ms=1200000,
        hard_timeout_source="deployment_manifest", required_factor=3,
        factor_source="deployment_manifest", allow_raised_timeout=True,
        allow_raised_timeout_source="deployment_manifest",
    )
    payload = json.loads(json.dumps({"timing_details": {"lock": {
        "hold_ms": 500000, "hard_timeout_ms": 1200000, "headroom": recorded,
    }}}))
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR", "1")
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HEADROOM_ALLOW_RAISED_TIMEOUT", "false")
    restored = lock_headroom_from_payload(payload)
    # The stricter recorded factor and its provenance survive the weaker env.
    assert restored["policy"]["required_factor"] == 3
    assert restored["policy"]["factor_source"] == "deployment_manifest"
    assert restored["hard_timeout_ms"] == recorded["hard_timeout_ms"]
    # The reader withheld the raise, so the bound clamps DOWN to the default.
    assert restored["policy"]["allow_raised_timeout"] is False
    assert restored["policy"]["allow_raised_timeout_source"] == (
        "reader_withheld_raised_timeout"
    )
    assert restored["policy"]["policy_timeout_source"] == "clamped_to_default"
    assert restored["policy"]["max_hold_ms"] <= recorded["policy"]["max_hold_ms"]
    assert restored["status"] == "blocked"
    assert restored["hard_timeout_source"] == "deployment_manifest"

    # With the reader's own authorization the recorded raise IS honored, and
    # the verdict says the manifest is where it came from.
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HEADROOM_ALLOW_RAISED_TIMEOUT", "1")
    widened = lock_headroom_from_payload(payload)
    assert widened["policy"]["allow_raised_timeout"] is True
    assert widened["policy"]["allow_raised_timeout_source"] == (
        "deployment_manifest"
    )
    assert widened["policy"]["max_hold_ms"] == recorded["policy"]["max_hold_ms"]


@pytest.mark.postgres
def test_cycle_warns_after_count_before_cold_stages(clean_runtime, monkeypatch):
    from contextlib import contextmanager
    import psycopg
    from okg.substrate.publisher import publisher as module
    from tests.substrate.publisher.test_invariant_gate_pinned_read import _gen_metadata, _seed_graph
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
    from tests.substrate.test_publisher_chronos_extractor_skip import _extractor_config

    timeline = []
    completed_stages = set()
    monkeypatch.setenv("OKG_IVM_CONSOLE_COUNT_ROLLUP", "1")
    original_stage = module._publisher_timed_stage
    original_warning = Publisher._initial_generation_budget_warning

    @contextmanager
    def record_stage(timings, name):
        timeline.append(name)
        with original_stage(timings, name):
            yield
        completed_stages.add(name)

    def record_warning(self, **kwargs):
        timeline.append("warning_evaluated")
        return original_warning(self, **kwargs)

    monkeypatch.setattr(module, "_publisher_timed_stage", record_stage)
    monkeypatch.setattr(
        Publisher, "_initial_generation_budget_warning", record_warning,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn, ["n:a", "n:b"])
    result = Publisher(
        clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        **_extractor_config(),
        first_publish_warning_budget={"node_count": 2, "edge_count": 1},
        first_publish_warning_budget_source="deployment_manifest",
    ).cycle(force_native_view_refresh=True)
    assert result["status"] == "published"
    assert result["first_publish_budget_warning"]["triggered"] is True
    # Console rollup is non-gating: a failed maintenance call can exit its
    # timing context normally. Require the successful persisted checkpoint
    # receipt, which the publisher omits on an unavailable rollup.
    metadata = _gen_metadata(clean_runtime, result["generation_id"])
    rollup = metadata["console_count_rollup"]
    assert rollup["mode"] == "full_recount"
    assert rollup["rows_written"] > 0
    assert timeline.index("current_graph_counts_initial") < timeline.index("warning_evaluated")
    assert timeline.index("warning_evaluated") < timeline.index("phase_event.phase_b_complete")
    # Require successful execution before comparing order: an absent or failed
    # stage must not vacuously satisfy this assertion. Extraction uses the real
    # regex stage; the fixture deliberately enables the real console rollup and
    # requests the overlay materialization, without stubbing publish work.
    required_stages = {
        "extractor", "gates", "attribute_effective_attrs_materialize",
        "console_count_rollup",
    }
    # `timeline` records stage ENTRY, so the ordering assertion above only
    # proves the warning followed the count stage starting. Require the count
    # stage to have COMPLETED as well, or a count that raised inside its
    # context would satisfy the order while proving nothing.
    assert "current_graph_counts_initial" in completed_stages
    assert required_stages <= completed_stages
    for stage in required_stages:
        assert timeline.index("warning_evaluated") < timeline.index(stage)
    # This fixture does not claim admitted derived-fact application, productive
    # enrichment, or native-view output coverage. Those require their own
    # catalog/provenance fixture and a real PostgreSQL run.


def test_generation_readiness_preserves_observed_warning(monkeypatch):
    from types import SimpleNamespace
    from okg.substrate import generation_resolver
    from okg.substrate.cli.deployment_readiness import _ready_generation_status

    warning = {"triggered": True, "node_count": 2330000, "edge_count": 3650000,
               "thresholds": {"node_count": 2330000, "edge_count": 3650000},
               "source": "deployment_manifest"}
    monkeypatch.setattr(generation_resolver, "resolve_okg_generation", lambda *a, **k: SimpleNamespace(
        generation_id="test-published-version", deployment_name="unit", status="published",
        catalog_version_id=1, target_generation_id=None, created_at=None,
        metadata={"first_publish_budget_warning": warning},
    ))
    result = _ready_generation_status(None, "unit")
    assert result["first_publish_budget_warning"] == warning
    assert result["generation_id"] == "test-published-version"
