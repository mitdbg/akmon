"""Config-axes generator tests (bench-okg-internals task.config-axes).

Asserts the deployment generator lifts the previously-hardcoded OKG-internals
knobs (enrichers, views, chronos interval-continuation, embedding) into
first-class, sweepable matrix axes: each axis value renders a *distinct*,
schema-valid deployment.yaml that differs from its siblings ONLY in that axis,
and a `sweep:` block fans out into one experiment (= one deployment) per cell.

These are pure-filesystem render tests — `GENERATED_DIR` is redirected to a
tmp dir, no Postgres is touched. The live mini-sweep that provisions + publishes
these configs is the orchestrator-driven proof attached separately.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

import okg.bench.orchestrator as orch


def _render(tmp: Path, *, source_set="files+code", config=None, exp_id="t"):
    """Render one experiment's deployment dir into `tmp` and return
    (raw deployment.yaml text, parsed dict)."""
    orch.GENERATED_DIR = tmp
    exp = orch.Experiment(
        id=exp_id,
        repo=orch.Repo(name="tinydb", url="https://example/tinydb.git"),
        source_set=source_set,
        replay=orch.Replay(strategy="first_n", n=5),
        config=config or orch.BenchConfig(),
    )
    orch._write_deployment_files(exp)
    text = (tmp / exp_id / "deployment.yaml").read_text()
    return text, yaml.safe_load(text)


def _enricher_classes(doc) -> list[str]:
    return [e["class"].rsplit(".", 1)[-1] for e in (doc.get("enrichers") or [])]


# --- enricher-subset axis --------------------------------------------------

def test_enricher_axis_renders_n_distinct_valid_and_differs_only_in_enrichers(tmp_path):
    presets = ["none", "declarative", "all"]
    texts, docs = {}, {}
    for p in presets:
        cfg = orch.BenchConfig(enrichers=p)
        t, d = _render(tmp_path / p, config=cfg, exp_id="enr")
        texts[p], docs[p] = t, d

    # N distinct rendered files
    assert len({texts[p] for p in presets}) == len(presets)

    # Each is schema-valid YAML with the expected enricher set
    assert _enricher_classes(docs["none"]) == []
    assert _enricher_classes(docs["declarative"]) == [
        "DeclarativeLinker", "DeclarativeLinker"]
    assert _enricher_classes(docs["all"]) == [
        "DeclarativeLinker", "DeclarativeLinker", "CodeReferenceResolver"]

    # Loading the code_reference enricher also ranks it above code-structure
    # for the edges both write (item 92a); without it nothing is ranked.
    assert "edge_precedence" not in docs["none"]
    assert "edge_precedence" not in docs["declarative"]
    assert docs["all"]["edge_precedence"] == {
        "calls": ["_enricher:code_reference", "code-structure"],
        "references": ["_enricher:code_reference", "code-structure"],
    }

    # They differ ONLY in the enrichers axis (and the ranking that follows
    # from it): drop both, rest identical
    stripped = []
    for p in presets:
        d = dict(docs[p])
        d.pop("enrichers", None)
        d.pop("edge_precedence", None)
        stripped.append(d)
    assert stripped[0] == stripped[1] == stripped[2]


# --- chronos interval-continuation axis ------------------------------------

def test_chronos_axis_differs_only_in_interval_continuation(tmp_path):
    _, d50 = _render(tmp_path / "a",
                     config=orch.BenchConfig(chronos_continuation_pct=50),
                     exp_id="chr")
    _, d95 = _render(tmp_path / "b",
                     config=orch.BenchConfig(chronos_continuation_pct=95),
                     exp_id="chr")
    assert d50["chronos"]["interval_continuation_percent"] == 50
    assert d95["chronos"]["interval_continuation_percent"] == 95
    d50.pop("chronos")
    d95.pop("chronos")
    assert d50 == d95


def test_native_view_refresh_axes_render_policy_only_when_enabled(tmp_path):
    _, off = _render(
        tmp_path / "a",
        config=orch.BenchConfig(
            native_view_min_refresh_interval_ms=0,
            native_view_materialization_delay_ms=0,
        ),
        exp_id="nvd",
    )
    _, on = _render(
        tmp_path / "b",
        config=orch.BenchConfig(
            native_view_min_refresh_interval_ms=1_000,
            native_view_materialization_delay_ms=60_000,
        ),
        exp_id="nvd",
    )

    assert "native_views" not in off
    assert on["native_views"]["min_refresh_interval_ms"] == 1_000
    assert on["native_views"]["materialization_delay_ms"] == 60_000
    on.pop("native_views")
    assert off == on


def test_attribute_view_empty_overlay_axis_renders_policy_only_when_enabled(tmp_path):
    _, off = _render(
        tmp_path / "a",
        config=orch.BenchConfig(
            attribute_effective_view_empty_overlay_fast_path=False,
        ),
        exp_id="ave",
    )
    _, on = _render(
        tmp_path / "b",
        config=orch.BenchConfig(
            attribute_effective_view_empty_overlay_fast_path=True,
        ),
        exp_id="ave",
    )

    assert "native_views" not in off
    assert (
        on["native_views"]["attribute_effective_view_empty_overlay_fast_path"]
        is True
    )
    on.pop("native_views")
    assert off == on


def test_source_sync_compaction_axis_renders_watcher_policy(tmp_path):
    off_dir = tmp_path / "a"
    on_dir = tmp_path / "b"
    _render(
        off_dir,
        config=orch.BenchConfig(source_sync_compaction_window_seconds=0),
        exp_id="ssc",
    )
    _render(
        on_dir,
        config=orch.BenchConfig(source_sync_compaction_window_seconds=1),
        exp_id="ssc",
    )

    off = yaml.safe_load((off_dir / "ssc" / "source_registry.yaml").read_text())
    on = yaml.safe_load((on_dir / "ssc" / "source_registry.yaml").read_text())

    assert off["sources"]["git-files"]["sync"]["compaction"]["window_seconds"] == 0
    assert on["sources"]["git-files"]["sync"]["compaction"]["window_seconds"] == 1
    assert off["sources"]["code-structure"]["sync"].get("compaction") is None


def test_source_sync_watch_debounce_axis_renders_runtime_policy(tmp_path):
    _, default = _render(
        tmp_path / "a",
        config=orch.BenchConfig(source_sync_watch_debounce_seconds=0.01),
        exp_id="ssd",
    )
    _, coalesced = _render(
        tmp_path / "b",
        config=orch.BenchConfig(source_sync_watch_debounce_seconds=0.1),
        exp_id="ssd",
    )

    assert (
        default["runtime"]["source_sync"]["watch"]["debounce_seconds"]
        == 0.01
    )
    assert (
        coalesced["runtime"]["source_sync"]["watch"]["debounce_seconds"]
        == 0.1
    )


def test_source_sync_head_poll_axis_renders_runtime_policy(tmp_path):
    _, default = _render(
        tmp_path / "a",
        config=orch.BenchConfig(source_sync_head_poll_interval_seconds=0.25),
        exp_id="hpi",
    )
    _, fast = _render(
        tmp_path / "b",
        config=orch.BenchConfig(source_sync_head_poll_interval_seconds=0.05),
        exp_id="hpi",
    )

    assert (
        default["runtime"]["source_sync"]["watch"][
            "head_poll_interval_seconds"
        ]
        == 0.25
    )
    assert (
        fast["runtime"]["source_sync"]["watch"][
            "head_poll_interval_seconds"
        ]
        == 0.05
    )


def test_source_sync_reconcile_watermark_axis_maps_to_runtime_env():
    default = orch.Experiment(
        id="default",
        repo=orch.Repo(name="tinydb", url="https://example/tinydb.git"),
        source_set="files+code+lsp",
        replay=orch.Replay(strategy="first_n", n=5),
    )
    tuned = orch.Experiment(
        id="tuned",
        repo=orch.Repo(name="tinydb", url="https://example/tinydb.git"),
        source_set="files+code+lsp",
        replay=orch.Replay(strategy="first_n", n=5),
        config=orch.BenchConfig(
            source_sync_reconcile_watermark_min_refresh_ms=60_000,
            source_sync_progress_min_refresh_ms=60_000,
            source_sync_queue_polling_interval_seconds=0.01,
        ),
    )

    assert orch._experiment_runtime_env(default) == {}
    assert orch._experiment_runtime_env(tuned) == {
        "OKG_SOURCE_SYNC_RECONCILE_WATERMARK_MIN_REFRESH_MS": "60000",
        "OKG_SOURCE_SYNC_PROGRESS_MIN_REFRESH_MS": "60000",
        "OKG_DBOS_SOURCE_SYNC_QUEUE_POLLING_INTERVAL_SEC": "0.01",
    }


def test_lsp_hot_path_budget_renders_source_param(tmp_path):
    source_set = "files+code+lsp"
    _render(
        tmp_path,
        source_set=source_set,
        config=orch.BenchConfig(lsp_hot_path_max_requests=512),
        exp_id="lsp",
    )

    registry = yaml.safe_load(
        (tmp_path / "lsp" / "source_registry.yaml").read_text(),
    )
    assert registry["sources"]["code-lsp-resolution"]["params"][
        "hot_path_max_lsp_requests"
    ] == 512


# --- embedding axis --------------------------------------------------------

def test_embedding_axis_differs_only_in_embedding_enabled(tmp_path):
    _, off = _render(tmp_path / "a",
                     config=orch.BenchConfig(embedding=False), exp_id="emb")
    _, on = _render(tmp_path / "b",
                    config=orch.BenchConfig(embedding=True), exp_id="emb")
    assert off["embeddings"]["enabled"] is False
    assert on["embeddings"]["enabled"] is True
    off.pop("embeddings")
    on.pop("embeddings")
    assert off == on


# --- views axis ------------------------------------------------------------

def test_views_axis_toggles_view_module_and_view_dir(tmp_path):
    on_dir = tmp_path / "on"
    off_dir = tmp_path / "off"
    _, on = _render(on_dir, config=orch.BenchConfig(views=True), exp_id="vw")
    _, off = _render(off_dir, config=orch.BenchConfig(views=False), exp_id="vw")
    assert "views" in on["modules"]
    assert "views" not in off["modules"]
    # the view package + registry only land when the view loads
    assert (on_dir / "vw" / "views").is_dir()
    assert not (off_dir / "vw" / "views").exists()
    assert (on_dir / "vw" / "views_registry.yaml").exists()
    assert not (off_dir / "vw" / "views_registry.yaml").exists()


# --- defaults reproduce pre-change behavior --------------------------------

def test_default_config_preserves_legacy_behavior(tmp_path):
    # files+code with no config: no enrichers, chronos 95, embedding off, views on
    _, d = _render(tmp_path / "fc", source_set="files+code", exp_id="def")
    assert _enricher_classes(d) == []
    assert d["chronos"]["interval_continuation_percent"] == 95
    assert d["embeddings"]["enabled"] is False
    assert "views" in d["modules"]
    assert "native_views" not in d
    source_registry = yaml.safe_load(
        (tmp_path / "fc" / "def" / "source_registry.yaml").read_text()
    )
    assert (
        source_registry["sources"]["git-files"]["sync"]["compaction"][
            "window_seconds"
        ]
        == 0
    )
    assert d["runtime"]["source_sync"]["watch"]["debounce_seconds"] == 0.01
    assert (
        d["runtime"]["source_sync"]["watch"]["head_poll_interval_seconds"]
        == 0.25
    )
    # tier1-enriched default still gets both linkers (the old include_enrichers)
    _, te = _render(tmp_path / "te", source_set="tier1-enriched", exp_id="te")
    assert _enricher_classes(te) == [
        "DeclarativeLinker", "DeclarativeLinker", "CodeReferenceResolver"]


def test_enrichers_force_empty_on_non_code_source_set(tmp_path):
    # asking for enrichers on a history-only set yields none (no code graph)
    _, d = _render(tmp_path / "h", source_set="history-only",
                   config=orch.BenchConfig(enrichers="all"), exp_id="ho")
    assert _enricher_classes(d) == []


# --- sweep expansion -------------------------------------------------------

def test_sweep_expands_single_axis_to_distinct_cells():
    entry = {
        "id": "tinydb-enr",
        "repo": {"name": "tinydb", "url": "https://example/tinydb.git"},
        "source_set": "files+code",
        "replay": {"strategy": "first_n", "n": 5},
        "sweep": {"enrichers": ["none", "declarative", "all"]},
    }
    cells = orch._expand_sweep(entry)
    assert [c["id"] for c in cells] == [
        "tinydb-enr-en0", "tinydb-enr-endecl", "tinydb-enr-enall"]
    assert [c["config"]["enrichers"] for c in cells] == [
        "none", "declarative", "all"]
    # each cell parses into a valid Experiment with the swept config
    exps = [orch._parse_experiment(c) for c in cells]
    assert {e.config.enrichers for e in exps} == {"none", "declarative", "all"}


def test_sweep_cartesian_over_two_axes():
    entry = {
        "id": "tdb",
        "repo": {"name": "tinydb", "url": "https://example/tinydb.git"},
        "source_set": "files+code",
        "replay": {"strategy": "first_n", "n": 5},
        "sweep": {"enrichers": ["none", "all"], "embedding": [False, True]},
    }
    cells = orch._expand_sweep(entry)
    assert len(cells) == 4
    assert len({c["id"] for c in cells}) == 4


def test_sweep_preserves_base_config_for_non_swept_axes():
    entry = {
        "id": "tdb",
        "repo": {"name": "tinydb", "url": "https://example/tinydb.git"},
        "source_set": "files+code",
        "replay": {"strategy": "first_n", "n": 5},
        "config": {"chronos_continuation_pct": 50},
        "sweep": {"enrichers": ["none", "all"]},
    }
    cells = orch._expand_sweep(entry)
    assert all(c["config"]["chronos_continuation_pct"] == 50 for c in cells)


def test_sweep_rejects_unknown_axis():
    entry = {
        "id": "x", "repo": {"name": "r", "url": "u"},
        "source_set": "files+code", "replay": {"strategy": "first_n", "n": 5},
        "sweep": {"bogus": [1, 2]},
    }
    with pytest.raises(ValueError, match="sweep axis"):
        orch._expand_sweep(entry)


# --- config validation + id cap --------------------------------------------

def test_invalid_enricher_preset_rejected():
    with pytest.raises(ValueError, match="config.enrichers"):
        orch._parse_bench_config("x", {"enrichers": "nope"})


def test_unknown_config_key_rejected():
    with pytest.raises(ValueError, match="unknown config keys"):
        orch._parse_bench_config("x", {"enrichers_typo": "all"})


def test_chronos_pct_out_of_range_rejected():
    with pytest.raises(ValueError, match="chronos_continuation_pct"):
        orch._parse_bench_config("x", {"chronos_continuation_pct": 0})


def test_id_over_dbos_cap_rejected():
    entry = {
        "id": "x" * 27, "repo": {"name": "r", "url": "u"},
        "source_set": "files+code", "replay": {"strategy": "first_n", "n": 5},
    }
    with pytest.raises(ValueError, match="DBOS app-name cap"):
        orch._parse_experiment(entry)


def test_shipped_manifest_still_loads():
    # regression: the 12 pre-existing config-less experiments still parse
    exps = orch._load_manifest()
    assert len(exps) >= 1
    assert all(isinstance(e.config, orch.BenchConfig) for e in exps)
