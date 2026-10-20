from pathlib import Path

import pytest
import yaml

import okg.bench.orchestrator as orchestrator
from okg.substrate.deployment_lint import lint_deployment
from okg.substrate.ingest.pii_posture import (
    resolve_deployment_source_pii_posture,
)
from okg.substrate.nomos.runtime_enforcement import manifest_nomos_enforcement_mode

Experiment = orchestrator.Experiment
Replay = orchestrator.Replay
Repo = orchestrator.Repo
_commits_for = orchestrator._commits_for
_record_from_index = orchestrator._record_from_index
_write_deployment_files = orchestrator._write_deployment_files


def _experiment(
    monkeypatch,
    tmp_path: Path,
    *,
    experiment_id: str,
    source_set: str,
) -> Experiment:
    monkeypatch.setattr(orchestrator, "BENCH_REPOS_DIR", tmp_path / "repos")
    monkeypatch.setattr(orchestrator, "GENERATED_DIR", tmp_path / "generated")
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", tmp_path / "results")
    return Experiment(
        id=experiment_id,
        repo=Repo(name="tinydb", url="https://example.invalid/tinydb.git"),
        source_set=source_set,
        replay=Replay(strategy="first_n", n=10),
    )


def test_files_only_deployment_does_not_copy_doc_corpus_bridge(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exp = _experiment(
        monkeypatch,
        tmp_path,
        experiment_id="tinydb-files-only-10",
        source_set="files-only",
    )

    stale_bridge = exp.deployment_dir / "schemas" / "bridges"
    stale_bridge.mkdir(parents=True)
    (stale_bridge / "extraction_to_git_files.yaml").write_text("stale")

    _write_deployment_files(exp)

    deployment = (exp.deployment_dir / "deployment.yaml").read_text()
    assert "git_graph" in deployment
    assert "interval_continuation_percent: 95" in deployment
    # 8cf9f864d (2026-06-23) made an EMPTY schemas/ dir unconditional (the
    # typed manifest requires schema_dir to exist), which silently broke the
    # older not-exists assert here. The intent stands: files-only must not
    # ship the doc-corpus bridge — the stale one seeded above is cleared and
    # nothing replaces it.
    assert (exp.deployment_dir / "schemas").is_dir()
    assert not (exp.deployment_dir / "schemas" / "bridges").exists()
    assert not any((exp.deployment_dir / "schemas").iterdir())


@pytest.mark.parametrize("source_set", sorted(orchestrator.VALID_SOURCE_SETS))
def test_generated_deployment_explicitly_authorizes_benchmark_raw_storage(
    monkeypatch,
    tmp_path: Path,
    source_set: str,
) -> None:
    exp = _experiment(
        monkeypatch,
        tmp_path,
        experiment_id=f"tinydb-{source_set.replace('+', '-')}-10",
        source_set=source_set,
    )

    _write_deployment_files(exp)

    manifest = yaml.safe_load((exp.deployment_dir / "deployment.yaml").read_text())
    registry = yaml.safe_load(
        (exp.deployment_dir / "source_registry.yaml").read_text(),
    )["sources"]
    assert manifest["nomos"]["runtime_enforcement"] == "off"
    assert manifest_nomos_enforcement_mode(manifest) == "off"
    assert registry
    for source_name, source_entry in registry.items():
        posture = resolve_deployment_source_pii_posture(
            manifest=manifest,
            source_name=source_name,
            source_entry=source_entry,
            build_detector=False,
            enforce_dsn_keys={},
        )
        assert posture.store_raw is True, source_name


def test_tier1_deployment_keeps_doc_corpus_bridge(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exp = _experiment(
        monkeypatch,
        tmp_path,
        experiment_id="palimpzest-tier1-10",
        source_set="tier1",
    )

    _write_deployment_files(exp)

    bridge = exp.deployment_dir / "schemas" / "bridges" / "extraction_to_git_files.yaml"
    assert bridge.exists()
    assert "document_derived_from_source_file" in bridge.read_text()


def test_history_only_deployment_backfills_source_file_stubs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exp = _experiment(
        monkeypatch,
        tmp_path,
        experiment_id="tinydb-history-only-10",
        source_set="history-only",
    )

    _write_deployment_files(exp)

    registry = (exp.deployment_dir / "source_registry.yaml").read_text()
    assert "git-history:" in registry
    assert "git-files:" not in registry
    assert "source_file_stub_globs:" in registry
    assert '- "*"' in registry
    assert '- "**/*"' in registry


def test_files_history_deployment_lets_git_files_own_source_files(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exp = _experiment(
        monkeypatch,
        tmp_path,
        experiment_id="tinydb-files-history-10",
        source_set="files+history",
    )

    _write_deployment_files(exp)

    registry = (exp.deployment_dir / "source_registry.yaml").read_text()
    assert "git-files:" in registry
    assert "git-history:" in registry
    assert "source_file_stub_globs:" not in registry


def test_files_code_lsp_deployment_registers_lsp_source(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exp = _experiment(
        monkeypatch,
        tmp_path,
        experiment_id="tinydb-code-lsp-head10",
        source_set="files+code+lsp",
    )

    _write_deployment_files(exp)

    registry = yaml.safe_load(
        (exp.deployment_dir / "source_registry.yaml").read_text(),
    )["sources"]
    source = registry["code-lsp-resolution"]
    assert source["class"] == "CodeLspResolutionSource"
    assert source["predecessor_source"] == [
        "tinydb-code-lsp-head10.git-files",
        "tinydb-code-lsp-head10.code-structure",
    ]
    assert source["params"]["server_command"] == [
        "uv",
        "run",
        "--with",
        "pyright==1.1.410",
        "pyright-langserver",
        "--stdio",
    ]
    assert source["sync"]["default_event_mode"] == "scope_complete"
    assert source["sync"]["triggers"] == ["manual", "reconcile"]
    assert "watch" not in source["sync"]
    assert registry["code-structure"]["sync"]["triggers"] == [
        "manual",
        "reconcile",
    ]
    assert "watch" not in registry["code-structure"]["sync"]
    edge_prefixes = {
        (edge["src_id_prefix"], edge["edge_type"], edge["dst_id_prefix"])
        for edge in source["admission_policy"]["output_signature"]["edges"]
    }
    assert edge_prefixes == {
        ("reference:", "references", "symbol:tinydb:"),
        ("symbol:tinydb:", "calls", "symbol:tinydb:"),
    }


def test_generated_git_history_watch_is_ref_only(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exp = _experiment(
        monkeypatch,
        tmp_path,
        experiment_id="palimpzest-tier1-10",
        source_set="tier1",
    )

    _write_deployment_files(exp)

    registry = yaml.safe_load(
        (exp.deployment_dir / "source_registry.yaml").read_text(),
    )["sources"]
    history_sync = registry["git-history"]["sync"]
    files_sync = registry["git-files"]["sync"]

    assert "**/*" not in history_sync["watch"]["include_globs"]
    assert history_sync["watch"]["include_globs"] == [
        ".git/HEAD",
        ".git/packed-refs",
        ".git/refs/heads/**",
    ]
    assert history_sync["triggers"] == [
        "git_ref_change",
        "manual",
        "reconcile",
    ]
    assert history_sync["default_event_mode"] == "cursor"
    assert history_sync["cheap_probe"] == "git_refs"
    assert files_sync["scoped_delete_mode"] == "scope_complete"
    assert files_sync["scoped_delete_dependent_fanout"] == "direct"


def test_generated_git_sources_poll_with_exact_adapter_change_probes(
    monkeypatch,
    tmp_path: Path,
) -> None:
    exp = _experiment(
        monkeypatch,
        tmp_path,
        experiment_id="palimpzest-tier1-10",
        source_set="tier1",
    )

    _write_deployment_files(exp)

    registry = yaml.safe_load(
        (exp.deployment_dir / "source_registry.yaml").read_text(),
    )["sources"]
    assert registry["git-files"]["sync"]["poll"]["probe"] == "change_probe"
    assert registry["git-history"]["sync"]["poll"]["probe"] == "change_probe"

    lint_report = lint_deployment(exp.deployment_dir, env={})
    poll_findings = [
        finding for finding in lint_report["findings"]
        if finding["code"].startswith("deployment.source_registry.poll_probe_")
    ]
    assert poll_findings == []


def test_tail_window_replays_prefix_but_records_tail(
    monkeypatch,
) -> None:
    exp = Experiment(
        id="palimpzest-tier1-tail-2",
        repo=Repo(name="palimpzest", url="https://example.invalid/repo.git"),
        source_set="tier1",
        replay=Replay(strategy="tail_window", last_n=2),
    )
    monkeypatch.setattr(
        orchestrator,
        "_all_commits_topo",
        lambda _repo_root: ["root", "c1", "c2", "c3", "c4"],
    )

    commits = _commits_for(exp)

    assert commits == ["c1", "c2", "c3", "c4"]
    assert _record_from_index(exp, commits) == 3


def test_tail_window_rejects_oversized_window(
    monkeypatch,
) -> None:
    exp = Experiment(
        id="palimpzest-tier1-tail-too-large",
        repo=Repo(name="palimpzest", url="https://example.invalid/repo.git"),
        source_set="tier1",
        replay=Replay(strategy="tail_window", last_n=5),
    )
    monkeypatch.setattr(
        orchestrator,
        "_all_commits_topo",
        lambda _repo_root: ["root", "c1", "c2"],
    )

    try:
        _commits_for(exp)
    except ValueError as exc:
        assert "exceeds replayable commit count" in str(exc)
    else:
        raise AssertionError("tail_window should reject windows past history")
