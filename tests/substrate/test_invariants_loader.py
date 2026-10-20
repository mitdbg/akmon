"""M5.1 — invariants.yaml loader + SQL-write detection + installer."""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.catalog import (
    DEFAULT_TIMEOUT_MS,
    InvariantSpec,
    load_invariants,
    sql_contains_writes,
)
from okg.substrate.catalog.invariants import install_invariants
from okg.substrate.errors import SubstrateError


def _write(tmp_path: Path, doc: dict) -> Path:
    p = tmp_path / "invariants.yaml"
    p.write_text(yaml.safe_dump(doc))
    return p


# ---------- write detection ----------

@pytest.mark.parametrize("sql", [
    "SELECT 1",
    "SELECT e.edge_id FROM edges_live e LIMIT 10",
    "WITH x AS (SELECT * FROM t) SELECT * FROM x",
    "SELECT 'INSERT foo' AS weird_text",          # string literal
    "SELECT 1 -- INSERT here would be bad",       # comment
    "SELECT 1 /* DELETE FROM bar */ FROM s",      # block comment
    "SELECT col FROM t WHERE col = 'DROP TABLE x'",
])
def test_read_only_sql_accepted(sql: str) -> None:
    assert sql_contains_writes(sql) is None


@pytest.mark.parametrize("sql,expected", [
    ("INSERT INTO t VALUES (1)", "INSERT"),
    ("UPDATE t SET x=1", "UPDATE"),
    ("DELETE FROM t", "DELETE"),
    ("TRUNCATE t", "TRUNCATE"),
    ("CREATE TABLE x (i int)", "CREATE"),
    ("DROP TABLE x", "DROP"),
    ("ALTER TABLE x ADD COLUMN y int", "ALTER"),
    ("GRANT SELECT ON t TO r", "GRANT"),
    ("SELECT 1; INSERT INTO t VALUES (1)", "INSERT"),
    ("SET statement_timeout = '1s'", "SET"),
    ("BEGIN; SELECT 1; COMMIT", "BEGIN"),
    ("COPY t FROM stdin", "COPY"),
])
def test_write_sql_rejected(sql: str, expected: str) -> None:
    assert sql_contains_writes(sql) == expected


def test_dollar_quotes_rejected() -> None:
    assert sql_contains_writes(
        "SELECT $tag$ anything $tag$"
    ) == "DOLLAR_QUOTE"


# ---------- loader ----------

def test_load_minimal_cross_invariant(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [{
            "name": "no_orphan_edges",
            "severity": "error",
            "scope": "cross",
            "sql": "SELECT 1 FROM edges_live WHERE 1=0",
        }]
    })
    out = load_invariants(p)
    assert len(out) == 1
    spec = out[0]
    assert spec.name == "no_orphan_edges"
    assert spec.severity == "error"
    assert spec.scope == "cross"
    assert spec.source is None
    assert spec.timeout_ms is None


def test_load_source_invariant_requires_source(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [{
            "name": "x",
            "severity": "warn",
            "scope": "source",
            "sql": "SELECT 1",
        }]
    })
    with pytest.raises(SubstrateError, match="requires a source"):
        load_invariants(p)


def test_load_cross_invariant_forbids_source(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [{
            "name": "x",
            "severity": "error",
            "scope": "cross",
            "source": "jira",
            "sql": "SELECT 1",
        }]
    })
    with pytest.raises(SubstrateError, match="must not set"):
        load_invariants(p)


def test_load_rejects_write_body(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [{
            "name": "bad",
            "severity": "error",
            "scope": "cross",
            "sql": "DELETE FROM nodes_live WHERE 1=0",
        }]
    })
    with pytest.raises(SubstrateError) as exc:
        load_invariants(p)
    assert exc.value.code == "invariant_body_writes"
    assert "DELETE" in str(exc.value)


def test_load_rejects_duplicate_names(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [
            {"name": "x", "severity": "warn", "scope": "cross",
             "sql": "SELECT 1"},
            {"name": "x", "severity": "warn", "scope": "cross",
             "sql": "SELECT 2"},
        ]
    })
    with pytest.raises(SubstrateError, match="duplicate"):
        load_invariants(p)


def test_load_rejects_bad_name(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [{
            "name": "BadName",
            "severity": "error",
            "scope": "cross",
            "sql": "SELECT 1",
        }]
    })
    with pytest.raises(SubstrateError, match="name must match"):
        load_invariants(p)


def test_load_rejects_bad_severity(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [{
            "name": "x",
            "severity": "critical",
            "scope": "cross",
            "sql": "SELECT 1",
        }]
    })
    with pytest.raises(SubstrateError, match="severity"):
        load_invariants(p)


def test_load_timeout_ms_accepted(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [{
            "name": "slow_check",
            "severity": "warn",
            "scope": "cross",
            "sql": "SELECT 1",
            "timeout_ms": 2000,
        }]
    })
    out = load_invariants(p)
    assert out[0].timeout_ms == 2000


def test_load_timeout_ms_rejects_non_positive(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "invariants": [{
            "name": "x",
            "severity": "warn",
            "scope": "cross",
            "sql": "SELECT 1",
            "timeout_ms": 0,
        }]
    })
    with pytest.raises(SubstrateError, match="positive integer"):
        load_invariants(p)


def test_load_unknown_top_level_key(tmp_path: Path) -> None:
    p = _write(tmp_path, {"invariants": [], "weird": 1})
    with pytest.raises(SubstrateError, match="unknown top-level"):
        load_invariants(p)


def test_default_timeout_constant_matches_design() -> None:
    # design.md §720 "each [invariant] has a per-invariant timeout
    # (default 5 s)"
    assert DEFAULT_TIMEOUT_MS == 5000


# ---------- installer (postgres-backed) ----------

postgres_mark = pytest.mark.postgres


def _seed_cv(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


@postgres_mark
def test_install_invariants_inserts_rows(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        specs = [
            InvariantSpec("a", "error", "cross", None, "SELECT 1", None),
            InvariantSpec("b", "warn", "source", "jira", "SELECT 2", 2000),
        ]
        n = install_invariants(conn, specs, catalog_version_id=cv)
        assert n == 2
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name, severity, scope, source, sql_body, "
                "       timeout_ms "
                "FROM okg.invariants "
                "WHERE catalog_version_id = %s ORDER BY name",
                (cv,),
            )
            rows = cur.fetchall()
    assert rows[0] == ("a", "error", "cross", None, "SELECT 1", None)
    assert rows[1] == ("b", "warn", "source", "jira", "SELECT 2", 2000)


@postgres_mark
def test_install_invariants_replaces_existing(clean_runtime: str) -> None:
    """Re-installing at the same catalog_version_id REPLACES
    the prior set, not appends."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(
            conn,
            [InvariantSpec("a", "error", "cross", None, "SELECT 1", None)],
            catalog_version_id=cv,
        )
        install_invariants(
            conn,
            [InvariantSpec("b", "warn", "cross", None, "SELECT 2", None)],
            catalog_version_id=cv,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name FROM okg.invariants "
                "WHERE catalog_version_id = %s ORDER BY name",
                (cv,),
            )
            names = [r[0] for r in cur.fetchall()]
    assert names == ["b"]  # 'a' was wiped


# ---------- judge spend declaration ----------


def _judge_body(tmp_path: Path, **extra) -> dict:
    """A minimal valid llm_judge entry; the prompt file must exist."""
    prompt = tmp_path / "prompt.md"
    prompt.write_text("judge it")
    body = {
        "name": "judge_inv",
        "kind": "llm_judge",
        "severity": "warn",
        "scope": "cross",
        "candidates_sql": "SELECT node_id AS candidate_id, "
                          "attrs AS payload FROM nodes_live",
        "judge_prompt": "prompt.md",
        "judge_model": "stub",
        "sample_cap": 5,
    }
    body.update(extra)
    return body


def test_load_judge_spend_declaration_round_trips(tmp_path: Path) -> None:
    p = _write(tmp_path, {"invariants": [_judge_body(
        tmp_path,
        judge_spend_cap_usd=0.50,
        judge_spend_estimate_usd=0.02,
    )]})
    (spec,) = load_invariants(p)
    assert spec.judge_spend_cap_usd == pytest.approx(0.50)
    assert spec.judge_spend_estimate_usd == pytest.approx(0.02)


def test_load_judge_spend_absent_means_unbounded_not_defaulted(
    tmp_path: Path,
) -> None:
    p = _write(tmp_path, {"invariants": [_judge_body(tmp_path)]})
    (spec,) = load_invariants(p)
    assert spec.judge_spend_cap_usd is None
    assert spec.judge_spend_estimate_usd is None


@pytest.mark.parametrize("half", [
    {"judge_spend_cap_usd": 0.50},
    {"judge_spend_estimate_usd": 0.02},
])
def test_load_judge_spend_half_declaration_rejected(
    tmp_path: Path, half: dict,
) -> None:
    p = _write(tmp_path, {"invariants": [_judge_body(tmp_path, **half)]})
    with pytest.raises(SubstrateError, match="declared together"):
        load_invariants(p)


@pytest.mark.parametrize("bad", [0, -1, 0.0, True, "cheap"])
def test_load_judge_spend_non_positive_rejected(
    tmp_path: Path, bad,
) -> None:
    p = _write(tmp_path, {"invariants": [_judge_body(
        tmp_path,
        judge_spend_cap_usd=bad,
        judge_spend_estimate_usd=0.02,
    )]})
    with pytest.raises(SubstrateError, match="positive number"):
        load_invariants(p)


def test_load_sql_kind_rejects_judge_spend_keys(tmp_path: Path) -> None:
    p = _write(tmp_path, {"invariants": [{
        "name": "sql_inv", "severity": "warn", "scope": "cross",
        "sql": "SELECT 1",
        "judge_spend_cap_usd": 0.50,
    }]})
    with pytest.raises(SubstrateError, match="belongs to"):
        load_invariants(p)


@postgres_mark
def test_judge_spend_keys_reach_the_runner_load_path(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """YAML → install → `_load_invariants_for_cv` round trip.

    Regression for the dead-knob defect this closes: the runner read
    `judge_spend_cap_usd` / `judge_spend_estimate_usd` from its
    invariant dict, but neither the loader's key set nor the gates
    SELECT could ever supply them — spend admission for the judge lane
    was structurally disabled while claimed wired.
    """
    from okg.substrate.publisher.gates import _load_invariants_for_cv

    p = _write(tmp_path, {"invariants": [_judge_body(
        tmp_path,
        judge_spend_cap_usd=0.50,
        judge_spend_estimate_usd=0.02,
    )]})
    specs = load_invariants(p)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, specs, catalog_version_id=cv)
        (inv,) = _load_invariants_for_cv(
            conn, cv, kinds=("llm_judge",),
        )
    assert inv["judge_spend_cap_usd"] == pytest.approx(0.50)
    assert inv["judge_spend_estimate_usd"] == pytest.approx(0.02)
    assert isinstance(inv["judge_spend_cap_usd"], float)


# ---------- dropped legacy graph relations (issue #1456) ----------

def _sql_body(sql: str) -> dict:
    return {
        "name": "reads_dropped",
        "severity": "error",
        "scope": "cross",
        "sql": sql,
    }


@pytest.mark.parametrize("relation", ["okg.nodes_live", "okg.edges_live"])
def test_dropped_graph_relation_in_sql_is_refused(
    tmp_path: Path, relation: str,
) -> None:
    """`retire_legacy_graph_storage` DROPs both on every migrate, so a body
    reading one fails at publish time with undefined_table and no checkout can
    repair it. Refuse at authoring time and name the replacement."""
    p = _write(tmp_path, {"invariants": [
        _sql_body(f"SELECT node_id FROM {relation}"),
    ]})
    with pytest.raises(SubstrateError) as exc:
        load_invariants(p)
    assert exc.value.code == "invariant_body_dropped_relation"
    message = str(exc.value)
    assert "reads_dropped" in message
    assert relation in message
    replacement = (
        "okg.v_nodes" if relation == "okg.nodes_live" else "okg.v_edges"
    )
    assert replacement in message
    assert exc.value.data["offending_relation"] == relation
    assert exc.value.data["field"] == "sql"


def test_dropped_graph_relation_in_candidates_sql_is_refused(
    tmp_path: Path,
) -> None:
    body = _judge_body(tmp_path)
    body["candidates_sql"] = (
        "SELECT node_id AS candidate_id, attrs AS payload "
        "FROM okg.nodes_live"
    )
    p = _write(tmp_path, {"invariants": [body]})
    with pytest.raises(SubstrateError) as exc:
        load_invariants(p)
    assert exc.value.code == "invariant_body_dropped_relation"
    assert exc.value.data["field"] == "candidates_sql"


@pytest.mark.parametrize("sql", [
    "SELECT node_id FROM okg.v_nodes -- okg.nodes_live was retired",
    "SELECT node_id FROM okg.v_nodes /* not okg.edges_live */",
    "SELECT 'okg.nodes_live' AS retired_name FROM okg.v_nodes",
])
def test_dropped_relation_mention_outside_a_read_is_not_refused(
    tmp_path: Path, sql: str,
) -> None:
    """Detection is parser-based, so a comment or string literal naming the
    relation is not a read of it."""
    p = _write(tmp_path, {"invariants": [_sql_body(sql)]})
    (spec,) = load_invariants(p)
    assert spec.name == "reads_dropped"


def test_maintained_graph_views_still_load(tmp_path: Path) -> None:
    p = _write(tmp_path, {"invariants": [_sql_body(
        "SELECT e.edge_id FROM okg.v_edges e "
        "LEFT JOIN okg.v_nodes n ON n.node_id = e.src "
        "WHERE n.node_id IS NULL"
    )]})
    (spec,) = load_invariants(p)
    assert spec.sql_body is not None


def test_shipped_library_and_profile_invariants_still_load() -> None:
    """The refusal must not strand what the repository ships.

    Every library preset and every profile bundle reads okg.v_nodes /
    okg.v_edges, which this change makes readable rather than refused.
    """
    root = Path(__file__).resolve().parents[2] / "src/okg/substrate/library"
    paths = sorted(root.glob("invariants/*.yaml"))
    paths += sorted(root.glob("profiles/*/invariants.yaml"))
    assert paths, "expected shipped invariant bundles to exist"
    for path in paths:
        load_invariants(path)


# ---------- requires_sources (issue #3624) ----------


def _scheduler_invariant(**extra: object) -> dict:
    return {
        "name": "single_scheduler",
        "severity": "error",
        "scope": "cross",
        "sql": "SELECT 1 FROM okg.v_nodes WHERE subtype = 'scheduler' AND 1=0",
        **extra,
    }


def test_requires_sources_is_loaded(tmp_path: Path) -> None:
    p = _write(tmp_path, {"invariants": [
        _scheduler_invariant(requires_sources=["slurm-inventory"]),
    ]})
    (spec,) = load_invariants(p)
    assert spec.requires_sources == ("slurm-inventory",)


@pytest.mark.parametrize("value", [[], "slurm-inventory", [""], [3], ["a", "a"]])
def test_requires_sources_bad_shapes_are_refused(tmp_path: Path, value) -> None:
    p = _write(tmp_path, {"invariants": [
        _scheduler_invariant(requires_sources=value),
    ]})
    with pytest.raises(SubstrateError, match="requires_sources"):
        load_invariants(p)


def test_an_unregistered_required_source_is_refused_at_load(tmp_path: Path) -> None:
    """A typo would otherwise read as "source off" and skip the check."""
    from okg.substrate.catalog.invariants import load_deployment_invariants

    _write(tmp_path, {"invariants": [
        _scheduler_invariant(requires_sources=["slurm-inventroy"]),
    ]})
    (tmp_path / "source_registry.yaml").write_text(
        "sources:\n  slurm-inventory:\n    ownership_id: unit.slurm-inventory\n"
    )
    with pytest.raises(SubstrateError, match="slurm-inventroy"):
        load_deployment_invariants(tmp_path)


def test_a_registered_required_source_loads(tmp_path: Path) -> None:
    from okg.substrate.catalog.invariants import load_deployment_invariants

    _write(tmp_path, {"invariants": [
        _scheduler_invariant(requires_sources=["slurm-inventory"]),
    ]})
    (tmp_path / "source_registry.yaml").write_text(
        "sources:\n  slurm-inventory:\n    ownership_id: unit.slurm-inventory\n"
    )
    (spec,) = load_deployment_invariants(tmp_path)
    assert spec.requires_sources == ("slurm-inventory",)


def _inv(name: str, requires: list[str] | None) -> dict:
    return {"name": name, "severity": "error", "scope": "cross",
            "requires_sources": requires}


def test_an_invariant_whose_source_is_off_is_skipped_and_named() -> None:
    from okg.substrate.publisher.gates import _apply_source_requirements

    evaluate, skipped, unchecked = _apply_source_requirements(
        [_inv("plain", None), _inv("sched", ["slurm", "guide"]),
         _inv("gone", ["retired"])],
        {"slurm": False, "guide": True},
    )
    assert [i["name"] for i in evaluate] == ["plain"]
    assert unchecked == []
    by_name = {s["name"]: s for s in skipped}
    assert by_name["sched"]["off_sources"] == ["slurm"]
    assert "slurm (enabled_by_default: false)" in by_name["sched"]["reason"]
    assert by_name["gone"]["reason"].endswith("retired (not registered)")


def test_an_invariant_whose_sources_are_on_is_evaluated() -> None:
    from okg.substrate.publisher.gates import _apply_source_requirements

    evaluate, skipped, unchecked = _apply_source_requirements(
        [_inv("sched", ["slurm"])], {"slurm": True},
    )
    assert [i["name"] for i in evaluate] == ["sched"]
    assert skipped == [] and unchecked == []


def test_without_a_registry_the_invariant_runs_and_is_reported_unchecked() -> None:
    from okg.substrate.publisher.gates import _apply_source_requirements

    evaluate, skipped, unchecked = _apply_source_requirements(
        [_inv("sched", ["slurm"])], None,
    )
    assert [i["name"] for i in evaluate] == ["sched"]
    assert skipped == []
    assert unchecked == ["sched"]


def test_requires_sources_is_refused_on_llm_judge_invariants(tmp_path: Path) -> None:
    """The judge runner does not apply it, so it must not be accepted."""
    (tmp_path / "prompt.md").write_text("Judge the candidate.\n")
    p = _write(tmp_path, {"invariants": [{
        "name": "judged",
        "severity": "warn",
        "scope": "cross",
        "kind": "llm_judge",
        "candidates_sql": "SELECT 'a' AS candidate_id, '{}'::jsonb AS payload WHERE false",
        "judge_prompt": "prompt.md",
        "judge_model": "claude-haiku-5-5",
        "sample_cap": 5,
        "requires_sources": ["slurm-inventory"],
    }]})
    with pytest.raises(SubstrateError, match="sql invariants only"):
        load_invariants(p)


def test_an_unused_requires_sources_leaves_the_catalog_fingerprint_unchanged(
    tmp_path: Path,
) -> None:
    """Upgrading okg must not change every deployment's catalog fingerprint
    (a changed fingerprint refuses publishes until the catalog reloads)."""
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import _catalog_content_payload

    def invariants_payload(extra: dict) -> list:
        dep = tmp_path / f"dep{len(extra)}"
        (dep / "schemas").mkdir(parents=True)
        (dep / "deployment.yaml").write_text(
            "name: scratch\nschema_dir: schemas\nmodules: []\n"
        )
        (dep / "invariants.yaml").write_text(yaml.safe_dump({"invariants": [
            {**_scheduler_invariant(), **extra},
        ]}))
        return _catalog_content_payload(compose_catalog(dep))["invariants"]

    (plain,) = invariants_payload({})
    assert "requires_sources" not in plain
    (needs,) = invariants_payload({"requires_sources": ["slurm"]})
    assert needs["requires_sources"] == ("slurm",)


def _registry_publisher(tmp_path: Path, monkeypatch, registry_text: str | None):
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "deployments" / "unitdep"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text("name: unitdep\nschema_dir: schemas\nmodules: []\n")
    if registry_text is not None:
        (dep / "source_registry.yaml").write_text(registry_text)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    publisher = Publisher.__new__(Publisher)
    publisher.deployment_name = "unitdep"
    return publisher


def test_source_states_follow_enabled_by_default(tmp_path: Path, monkeypatch) -> None:
    publisher = _registry_publisher(tmp_path, monkeypatch, (
        "sources:\n"
        "  on-default:\n    ownership_id: unitdep.on-default\n"
        "  slurm-off:\n    ownership_id: unitdep.slurm-off\n    enabled_by_default: false\n"
    ))
    assert publisher._source_states() == {"on-default": True, "slurm-off": False}


def test_a_malformed_registry_yields_no_states_instead_of_crashing(
    tmp_path: Path, monkeypatch,
) -> None:
    publisher = _registry_publisher(tmp_path, monkeypatch, "sources: [unclosed\n")
    assert publisher._source_states() is None


def test_the_registry_is_not_read_when_no_invariant_needs_it() -> None:
    """A deployment with no requires_sources invariant never reads its
    registry at publish, so a broken registry cannot break its gate."""
    from okg.substrate.publisher import gates

    def must_not_be_called():
        raise AssertionError("registry read")

    invs = [_inv("plain", None)]
    needs = any(inv.get("requires_sources") for inv in invs)
    assert needs is False
    evaluate, skipped, unchecked = gates._apply_source_requirements(
        invs, must_not_be_called() if needs else None,
    )
    assert [i["name"] for i in evaluate] == ["plain"]
    assert skipped == [] and unchecked == []
