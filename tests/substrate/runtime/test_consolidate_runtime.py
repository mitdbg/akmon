"""Tests for `consolidate-deployment-runtime`.

Covers:

  1. `RuntimeConfig.from_manifest_block` accepts and round-trips a
     `runtime.source_sync` block.
  2. `resolve_source_sync` fills in defaults for partial blocks.
  3. `should_register_embed` / `should_register_lens_putback` /
     `should_register_escalation_sweep` return the expected gate
     decisions on various manifest / DSN states.
  4. `check_runtime_services_health` returns the documented
     verdicts: `not_observed` outside the worker; `healthy` /
     `down` inside.
"""
from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import patch

import pytest

from okg.substrate.db import concurrency_budget as budget
from okg.substrate.runtime.config import (
    DBOS_SYSTEM_SCHEMA,
    RuntimeConfig,
    SOURCE_SYNC_BLOCK_DEFAULTS,
    resolve_source_sync,
)
from okg.substrate.runtime.publisher_health import (
    check_runtime_services_health,
)
from okg.substrate.runtime.services import (
    _reset_state,
    get_services_state,
    should_register_embed,
)
from okg.substrate.runtime.engine import build_dbos_config
from okg.substrate.runtime.source_sync_constants import (
    SOURCE_SYNC_APPLICATION_VERSION,
)


# ---------------------------------------------------------------------------
# RuntimeConfig / resolve_source_sync
# ---------------------------------------------------------------------------


def test_runtime_config_from_empty_manifest_block() -> None:
    cfg = RuntimeConfig.from_manifest_block(None)
    assert cfg.source_sync == {}
    # Round-trip through as_dict / resolve.
    d = cfg.as_dict()
    assert d["source_sync"] == {}


def test_runtime_config_round_trips_source_sync_block() -> None:
    raw = {
        "enabled": True,
        "source_sync": {
            "watch": {"enabled": False},
            "webhook": {"enabled": True, "port": 9999},
        },
    }
    cfg = RuntimeConfig.from_manifest_block(raw)
    assert cfg.source_sync["watch"] == {"enabled": False}
    assert cfg.source_sync["webhook"]["port"] == 9999
    assert cfg.as_dict()["source_sync"] == cfg.source_sync


def test_runtime_config_round_trips_python_extras() -> None:
    cfg = RuntimeConfig.from_manifest_block(
        {
            "python_extras": ["quality", "code", "quality"],
            "smoke_sources": ["pact", "pact"],
            "smoke_reset_cursor": True,
        }
    )
    assert cfg.python_extras == ("quality", "code")
    assert cfg.as_dict()["python_extras"] == ["quality", "code"]
    assert cfg.smoke_sources == ("pact",)
    assert cfg.as_dict()["smoke_sources"] == ["pact"]
    assert cfg.smoke_reset_cursor is True
    assert cfg.as_dict()["smoke_reset_cursor"] is True


def test_runtime_config_rejects_invalid_python_extras() -> None:
    with pytest.raises(ValueError, match="python_extras"):
        RuntimeConfig.from_manifest_block({"python_extras": "quality"})

    with pytest.raises(ValueError, match="smoke_sources"):
        RuntimeConfig.from_manifest_block({"smoke_sources": "pact"})

    with pytest.raises(ValueError, match="smoke_reset_cursor"):
        RuntimeConfig.from_manifest_block({"smoke_reset_cursor": "yes"})


def test_runtime_config_rejects_non_dict_source_sync() -> None:
    with pytest.raises(ValueError, match="source_sync"):
        RuntimeConfig.from_manifest_block(
            {"source_sync": "not a mapping"}
        )


def _runtime_approver_block() -> dict:
    return {
        "approver": {
            "enabled": True,
            "actor": "runtime.approver",
            "rules": [{
                "id": "bounded.observations",
                "producer_ids": ["agent:trusted"],
                "producer_kinds": ["agent"],
                "intents": ["record_observation"],
                "node_subtypes": ["alpha"],
                "edge_types": [],
                "max_nodes": 2,
                "max_edges": 1,
            }],
        },
    }


def test_runtime_config_round_trips_closed_admission_approver() -> None:
    admission = _runtime_approver_block()
    cfg = RuntimeConfig.from_manifest_block({"admission": admission})
    assert cfg.admission == admission
    assert cfg.as_dict()["admission"] == admission


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update({"extra": True}), "unknown key"),
        (
            lambda value: value["approver"].pop("actor"),
            "contain exactly",
        ),
        (
            lambda value: value["approver"]["rules"][0].update(
                {"extra": True}
            ),
            "closed rule schema",
        ),
        (
            lambda value: value["approver"].update({"rules": []}),
            "1..64",
        ),
        (
            lambda value: value["approver"]["rules"][0].update(
                {"max_nodes": 0}
            ),
            "positive int",
        ),
        (
            lambda value: value["approver"]["rules"][0].update(
                {"intents": ["not an intent"]}
            ),
            "intent must match",
        ),
    ],
)
def test_runtime_config_rejects_invalid_admission_approver(
    mutate,
    message: str,
) -> None:
    admission = _runtime_approver_block()
    mutate(admission)
    with pytest.raises(ValueError, match=message):
        RuntimeConfig.from_manifest_block({"admission": admission})


@pytest.mark.parametrize("value", [False, [], "approver"])
def test_runtime_config_rejects_non_mapping_admission(value) -> None:
    with pytest.raises(ValueError, match="admission must be a mapping"):
        RuntimeConfig.from_manifest_block({"admission": value})


# ---------------------------------------------------------------------------
# admin_port / run_admin_server (per-deployment DBOS admin server)
# ---------------------------------------------------------------------------


def test_runtime_config_admin_port_defaults_to_none_server_on() -> None:
    # Absent block: no explicit admin port (DBOS uses its default), and
    # the admin server is on — preserving prior single-deployment behavior.
    cfg = RuntimeConfig.from_manifest_block(None)
    assert cfg.admin_port is None
    assert cfg.run_admin_server is True
    d = cfg.as_dict()
    assert d["admin_port"] is None
    assert d["run_admin_server"] is True


def test_runtime_config_round_trips_admin_port_and_server_flag() -> None:
    raw = {"enabled": True, "admin_port": 3011, "run_admin_server": False}
    cfg = RuntimeConfig.from_manifest_block(raw)
    assert cfg.admin_port == 3011
    assert cfg.run_admin_server is False
    assert cfg.as_dict()["admin_port"] == 3011
    assert cfg.as_dict()["run_admin_server"] is False


def test_runtime_config_rejects_non_int_admin_port() -> None:
    with pytest.raises(ValueError, match="admin_port"):
        RuntimeConfig.from_manifest_block({"admin_port": "3011"})


def test_runtime_config_rejects_bool_admin_port() -> None:
    # bool is a subclass of int — must be rejected, not silently read as
    # port 1 / port 0.
    with pytest.raises(ValueError, match="admin_port"):
        RuntimeConfig.from_manifest_block({"admin_port": True})


# ---------------------------------------------------------------------------
# runtime.concurrency → concurrency registry
# ---------------------------------------------------------------------------


@pytest.fixture()
def _clean_budget_overrides():
    budget.clear_overrides()
    yield
    budget.clear_overrides()


def test_runtime_concurrency_override_takes_effect(
    _clean_budget_overrides,
) -> None:
    # The whole point of req.coherent-parallelism.manifest-expressed: a
    # width declared in the manifest resolves through width(), not just
    # round-trips through as_dict().
    assert budget.width("enrichment.model_calls") == 4  # registry default
    cfg = RuntimeConfig.from_manifest_block(
        {"concurrency": {"enrichment.model_calls": 8}}
    )
    cfg.apply_to_concurrency_budget()
    assert budget.width("enrichment.model_calls") == 8


@pytest.mark.parametrize(
    "term",
    [
        "gate.invariants",
        "enrichment.model_calls",
        "embeddings.batch",
        "views.recheck_judge",
    ],
)
def test_each_registry_only_term_is_manifest_settable(
    term: str, _clean_budget_overrides
) -> None:
    # Width 5 differs from every one of the four defaults (2/4/4/8), so a
    # pass proves the override applied rather than the default surviving.
    # gate.invariants borrows from db.shared_pool, so the pool is widened
    # to keep the combination feasible — this test is about expressibility,
    # not feasibility.
    cfg = RuntimeConfig.from_manifest_block(
        {"okg_read_pool_size": 20, "concurrency": {term: 5}}
    )
    cfg.apply_to_concurrency_budget()
    assert budget.width(term) == 5


def test_runtime_concurrency_unknown_term_refuses_naming_it(
    _clean_budget_overrides,
) -> None:
    cfg = RuntimeConfig.from_manifest_block(
        {"concurrency": {"enrichment.model_cals": 8}}
    )
    with pytest.raises(ValueError) as exc:
        cfg.apply_to_concurrency_budget()
    # Names the typo'd term AND lists the settable set.
    assert "enrichment.model_cals" in str(exc.value)
    assert "enrichment.model_calls" in str(exc.value)


@pytest.mark.parametrize("bad", [0, 65])
def test_runtime_concurrency_out_of_range_width_refuses(
    bad: int, _clean_budget_overrides
) -> None:
    # 0 must NOT clamp to 1, and 65 exceeds _MAX_DECLARABLE_WIDTH (64).
    cfg = RuntimeConfig.from_manifest_block(
        {"concurrency": {"embeddings.batch": bad}}
    )
    with pytest.raises(ValueError, match="embeddings.batch"):
        cfg.apply_to_concurrency_budget()
    assert budget.width("embeddings.batch") == 4  # untouched default


def test_runtime_concurrency_refuses_terms_with_dedicated_keys(
    _clean_budget_overrides,
) -> None:
    # db.shared_pool has okg_read_pool_size; accepting both would make two
    # manifest spellings for one width.
    cfg = RuntimeConfig.from_manifest_block(
        {"concurrency": {"db.shared_pool": 10}}
    )
    with pytest.raises(ValueError, match="okg_read_pool_size"):
        cfg.apply_to_concurrency_budget()
    cfg = RuntimeConfig.from_manifest_block(
        {"concurrency": {"runtime.dbos_pool": 4}}
    )
    with pytest.raises(ValueError, match="dbos_pool_size"):
        cfg.apply_to_concurrency_budget()


def test_runtime_concurrency_rejects_non_mapping_and_bad_value_types() -> None:
    with pytest.raises(ValueError, match="concurrency"):
        RuntimeConfig.from_manifest_block({"concurrency": "8"})
    with pytest.raises(ValueError, match="concurrency"):
        RuntimeConfig.from_manifest_block(
            {"concurrency": {"enrichment.model_calls": "8"}}
        )
    # bool is a subclass of int — `gate.invariants: true` is a config
    # error, not width 1.
    with pytest.raises(ValueError, match="concurrency"):
        RuntimeConfig.from_manifest_block(
            {"concurrency": {"gate.invariants": True}}
        )


def test_runtime_concurrency_round_trips_through_as_dict() -> None:
    cfg = RuntimeConfig.from_manifest_block(
        {"concurrency": {"views.recheck_judge": 4}}
    )
    assert cfg.as_dict()["concurrency"] == {"views.recheck_judge": 4}
    assert RuntimeConfig.from_manifest_block(None).as_dict()["concurrency"] == {}


def test_build_dbos_config_omits_admin_port_when_unset() -> None:
    cfg = build_dbos_config(
        deployment="alpha",
        dsn="postgresql://x/y",
        runtime=RuntimeConfig.from_manifest_block(None),
    )
    assert cfg["name"] == "okg-alpha"
    # No explicit admin port → key absent → DBOS falls back to its default.
    assert "admin_port" not in cfg
    assert cfg["run_admin_server"] is True


def test_build_dbos_config_splits_combined_budget_across_dbos_engines() -> None:
    cfg = build_dbos_config(
        deployment="alpha",
        dsn="postgresql://x/y",
        runtime=RuntimeConfig.from_manifest_block({"dbos_pool_size": 7}),
    )

    assert cfg["sys_db_pool_size"] == 3
    assert cfg["db_engine_kwargs"] == {
        "pool_size": 4,
        "max_overflow": 0,
    }
    assert (
        cfg["sys_db_pool_size"] + cfg["db_engine_kwargs"]["pool_size"]
        == 7
    )
    assert "app_db_pool_size" not in cfg


def test_dbos_processes_runtime_pool_budget_for_both_engines() -> None:
    from dbos._dbos_config import (
        process_config,
        translate_dbos_config_to_config_file,
    )

    cfg = build_dbos_config(
        deployment="alpha",
        dsn="postgresql://x/y",
        runtime=RuntimeConfig.from_manifest_block({"dbos_pool_size": 6}),
    )
    processed = process_config(
        data=translate_dbos_config_to_config_file(cfg),
        silent=True,
    )

    database = processed["database"]
    assert database["db_engine_kwargs"]["pool_size"] == 3
    assert database["sys_db_engine_kwargs"]["pool_size"] == 3
    assert (
        database["db_engine_kwargs"]["pool_size"]
        + database["sys_db_engine_kwargs"]["pool_size"]
        == 6
    )
    assert database["db_engine_kwargs"]["max_overflow"] == 0
    assert database["sys_db_engine_kwargs"]["max_overflow"] == 0


def test_build_dbos_config_rejects_budget_too_small_for_two_engines() -> None:
    with pytest.raises(ValueError, match="combined DBOS connection budget"):
        build_dbos_config(
            deployment="alpha",
            dsn="postgresql://x/y",
            runtime=RuntimeConfig.from_manifest_block({"dbos_pool_size": 1}),
        )


def test_build_dbos_config_sets_distinct_admin_port_per_deployment() -> None:
    # The fix: two co-located deployments get distinct admin ports, so
    # both runtime workers can bind without `Errno 48 Address in use`.
    cfg_a = build_dbos_config(
        deployment="alpha",
        dsn="postgresql://x/y",
        runtime=RuntimeConfig.from_manifest_block({"admin_port": 3011}),
    )
    cfg_b = build_dbos_config(
        deployment="beta",
        dsn="postgresql://x/z",
        runtime=RuntimeConfig.from_manifest_block({"admin_port": 3012}),
    )
    assert cfg_a["admin_port"] == 3011
    assert cfg_b["admin_port"] == 3012
    assert cfg_a["admin_port"] != cfg_b["admin_port"]


def test_build_dbos_config_can_disable_admin_server() -> None:
    cfg = build_dbos_config(
        deployment="alpha",
        dsn="postgresql://x/y",
        runtime=RuntimeConfig.from_manifest_block(
            {"run_admin_server": False}
        ),
    )
    assert cfg["run_admin_server"] is False


def test_build_dbos_config_sets_source_sync_replay_boundary() -> None:
    cfg = build_dbos_config(
        deployment="alpha",
        dsn="postgresql://x/y",
        runtime=RuntimeConfig.from_manifest_block(None),
    )
    assert cfg["application_version"] == SOURCE_SYNC_APPLICATION_VERSION
    assert cfg["application_version"] != "DEFAULT_VERSION"


def test_resolve_source_sync_returns_defaults_for_missing_block() -> None:
    cfg = RuntimeConfig.from_manifest_block(None)
    out = resolve_source_sync(cfg)
    # Defaults populated.
    assert out == SOURCE_SYNC_BLOCK_DEFAULTS
    # But it's a copy, not the shared dict.
    assert out is not SOURCE_SYNC_BLOCK_DEFAULTS


def test_resolve_source_sync_merges_partial_override() -> None:
    cfg = RuntimeConfig.from_manifest_block(
        {"source_sync": {"webhook": {"enabled": True}}}
    )
    out = resolve_source_sync(cfg)
    # The override took.
    assert out["webhook"]["enabled"] is True
    # The other webhook fields kept defaults.
    assert out["webhook"]["host"] == "127.0.0.1"
    assert out["webhook"]["port"] == 8088
    # Other sections (watch/poll) untouched.
    assert out["watch"]["enabled"] is True
    assert out["poll"]["enabled"] is True


def test_resolve_source_sync_tolerates_non_dict_override() -> None:
    """If an operator hand-writes garbage, we fall back to defaults
    rather than crashing the worker boot."""

    class _Fake:
        source_sync = "string, not dict"  # type: ignore[assignment]

    out = resolve_source_sync(_Fake())
    # Falls back to defaults.
    assert out["watch"]["enabled"] is True


# ---------------------------------------------------------------------------
# should_register_embed
# ---------------------------------------------------------------------------


def test_should_register_embed_returns_true_when_enabled() -> None:
    from okg.substrate.embeddings import EmbeddingConfig

    class _M:
        embedding_config = EmbeddingConfig(enabled=True, backend="hash")

    assert should_register_embed(_M()) is True


def test_should_register_embed_returns_false_when_disabled() -> None:
    from okg.substrate.embeddings import EmbeddingConfig

    class _M:
        embedding_config = EmbeddingConfig(enabled=False)

    assert should_register_embed(_M()) is False


def test_should_register_embed_refuses_missing_typed_contract() -> None:
    class _M:
        embedding_config = None

    with pytest.raises(TypeError, match="embedding_config"):
        should_register_embed(_M())

    class _N:
        pass  # no embedding attr at all

    with pytest.raises(TypeError, match="embedding_config"):
        should_register_embed(_N())


# ---------------------------------------------------------------------------
# check_runtime_services_health
# ---------------------------------------------------------------------------


def test_runtime_services_health_not_observed_outside_worker() -> None:
    """No worker boot in this process — state is empty."""
    # _reset_state(None) leaves state.deployment = None; but the
    # module-level _STATE may have been touched by another test, so
    # patch its `deployment` field explicitly.
    with patch.object(
        get_services_state(), "deployment", None
    ):
        verdict = check_runtime_services_health(deployment="any")
    assert verdict.ok is True
    assert verdict.code == "runtime_services_not_observed"


def test_runtime_services_health_other_deployment() -> None:
    """When a different deployment booted in this process."""
    _reset_state("other-dep")
    try:
        verdict = check_runtime_services_health(
            deployment="this-dep"
        )
        assert verdict.ok is True
        assert verdict.code == "runtime_services_other_deployment"
    finally:
        _reset_state("")
        get_services_state().deployment = None


def test_runtime_services_health_none_enabled() -> None:
    """Worker booted but neither service is enabled."""
    _reset_state("dep-x")
    try:
        verdict = check_runtime_services_health(deployment="dep-x")
        assert verdict.ok is True
        assert verdict.code == "runtime_services_none_enabled"
    finally:
        _reset_state("")
        get_services_state().deployment = None


def test_runtime_services_health_down_reports_failures() -> None:
    """Worker booted, watcher enabled, but the thread is dead."""
    _reset_state("dep-x")
    state = get_services_state()
    state.watcher.enabled = True
    state.watcher.instance = None  # alive() returns False
    state.watcher.last_error = "boom"
    try:
        verdict = check_runtime_services_health(deployment="dep-x")
        assert verdict.ok is False
        assert verdict.code == "runtime_services_down"
        assert "boom" in verdict.message
    finally:
        _reset_state("")
        get_services_state().deployment = None


# ---------------------------------------------------------------------------
# runtime.dbos_schema (#1585): one DBOS system schema, refused otherwise
# ---------------------------------------------------------------------------

#: DBOS tables okg reads or writes by a schema-qualified SQL name.
_DBOS_TABLE_REFERENCE = re.compile(
    # The system tables DBOS 2.26.0 creates (dbos/_schemas/system_database.py).
    r"\b([A-Za-z_][A-Za-z0-9_]*)\.(workflow_status|operation_outputs|"
    r"application_versions|workflow_schedules|workflow_events_history|"
    r"workflow_events|notifications|streams|queues)\b"
)


@pytest.mark.parametrize("block", [None, {}, {"dbos_schema": "dbos"}])
def test_dbos_config_names_the_one_dbos_system_schema(block) -> None:
    runtime = RuntimeConfig.from_manifest_block(block)
    cfg = build_dbos_config(
        deployment="alpha", dsn="postgresql://x/y", runtime=runtime,
    )
    assert runtime.dbos_schema == DBOS_SYSTEM_SCHEMA
    assert cfg["dbos_system_schema"] == DBOS_SYSTEM_SCHEMA


@pytest.mark.parametrize("schema", ["okg_alt", "DBOS", "public", ""])
def test_runtime_config_refuses_an_unsupported_dbos_schema(schema) -> None:
    with pytest.raises(ValueError, match=r"runtime\.dbos_schema .* not supported"):
        RuntimeConfig.from_manifest_block({"dbos_schema": schema})


def test_runtime_config_still_type_checks_dbos_schema() -> None:
    with pytest.raises(ValueError, match="runtime.dbos_schema must be str"):
        RuntimeConfig.from_manifest_block({"dbos_schema": 7})


def test_upgrade_context_refuses_an_unsupported_dbos_schema() -> None:
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.errors import SubstrateError

    assert du._declared_dbos_schema(None) == DBOS_SYSTEM_SCHEMA
    assert du._declared_dbos_schema({"enabled": True}) == DBOS_SYSTEM_SCHEMA
    assert du._declared_dbos_schema({"dbos_schema": "dbos"}) == DBOS_SYSTEM_SCHEMA
    with pytest.raises(SubstrateError, match="not supported"):
        du._declared_dbos_schema({"dbos_schema": "okg_alt"})


def test_upgrade_bootstrap_probe_never_steers_to_another_schema() -> None:
    from types import SimpleNamespace

    from okg.substrate import deployment_upgrade as du
    from okg.substrate.errors import SubstrateError

    assert du._runtime_dbos_schema(SimpleNamespace(data={})) == DBOS_SYSTEM_SCHEMA
    with pytest.raises(SubstrateError, match="unsupported dbos schema"):
        du._runtime_dbos_schema(
            SimpleNamespace(data={"runtime_dbos_schema": "okg_alt"})
        )


def test_upgrade_bootstrap_probe_refuses_before_it_can_fail_closed(
    monkeypatch,
) -> None:
    """An unsupported schema is a refusal, not a database fault. If the
    schema lookup moved inside the probe's fail-closed ``try``, the refusal
    would be swallowed and the probe would report the runtime bootstrapped.
    """
    from types import SimpleNamespace

    import okg.substrate.db.connection as connection
    from okg.substrate import deployment_upgrade as du
    from okg.substrate.errors import SubstrateError

    def _no_database(*_args, **_kwargs):
        raise AssertionError("the probe must refuse before it connects")

    monkeypatch.setattr(connection, "connect", _no_database)
    ctx = SimpleNamespace(
        dsn="postgresql://unused/none", data={"runtime_dbos_schema": "okg_alt"}
    )
    with pytest.raises(SubstrateError, match="unsupported dbos schema") as info:
        du._probe_runtime_bootstrapped(ctx)
    assert info.value.fix_hint == "remove runtime.dbos_schema from deployment.yaml"


def test_hardcoded_dbos_table_readers_match_the_configured_schema() -> None:
    """Class guard: the hardcoded readers under ``src/okg`` use
    :data:`DBOS_SYSTEM_SCHEMA`, no DBOS table is qualified with another
    ``dbos``-prefixed schema, and the constant is the only schema a manifest
    may declare. Changing the constant, or admitting a second schema, fails
    here until each reader takes the schema as a parameter.

    Scope: this does not flag a DBOS table under an unrelated schema name
    (``okg_alt.workflow_status``) or a schema passed as a string argument;
    table aliases such as ``aw.workflow_status`` make a broad scan noisy.
    """
    src = Path(__file__).resolve().parents[3] / "src" / "okg"
    readers: dict[str, int] = {}
    other: list[str] = []
    for path in sorted(src.rglob("*")):
        if path.suffix not in {".py", ".sql"} or not path.is_file():
            continue
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            for match in _DBOS_TABLE_REFERENCE.finditer(line):
                schema = match.group(1)
                rel = f"{path.relative_to(src)}:{lineno}"
                if schema == DBOS_SYSTEM_SCHEMA:
                    readers[rel] = readers.get(rel, 0) + 1
                elif schema in {"dbos", "DBOS"} or schema.startswith("dbos"):
                    other.append(f"{rel}: {match.group(0)}")
    assert readers, "expected hardcoded DBOS table readers under src/okg"
    assert other == [], other
    # The manifest cannot select a schema those readers do not use.
    for candidate in {"okg_alt", "runtime_dbos"}:
        with pytest.raises(ValueError):
            RuntimeConfig.from_manifest_block({"dbos_schema": candidate})
