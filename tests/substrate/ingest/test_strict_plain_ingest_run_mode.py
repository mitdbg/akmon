"""Plain `okg ingest` runs a strict source in its own whole-scope mode (#2476).

`okg deployment add-source --apply` rewrites the whole source registry as
`admission_contract: strict_v1`. A strict source runs under its frozen
executable profile, and the runner refuses a (profile, mode) pair with no
retraction policy. Plain `okg ingest` and `okg provision` ask every source for
`scope_complete`, which only `discovery_crawl` understands, so on a fresh
workspace-graph install the `reference_catalog` source `compute_env_local`
failed every plain ingest after the first add-source.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
from pathlib import Path

import pytest
import yaml

import okg.substrate

from okg.substrate.cli import runtime as cli_runtime
from okg.substrate.cli.main import _build_parser
from okg.substrate.ingest import incomplete_recovery as ir
from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.runner import _POLICY_MODE
from okg.substrate.ingest.service import IngestOptions, _run_mode_for_source
from okg.substrate.sources.registry import admit_source_registry_document


_SHIPPED_DEFAULT = (
    Path(okg.substrate.__file__).parent
    / "library/profiles/workspace-graph/source-defaults/compute_env_local.yaml"
)


def _shipped_compute_env_local() -> dict:
    """The workspace-graph profile's shipped default, as the scaffolder fills it."""
    text = _SHIPPED_DEFAULT.read_text(encoding="utf-8")
    body = yaml.safe_load(text.replace("${deployment_name}", "ws"))
    return body["compute_env_local"]


def _entry(*, strict: bool, body: dict | None = None, name: str = "compute_env_local"):
    document: dict = {"sources": {name: dict(body or _shipped_compute_env_local())}}
    if strict:
        document = {"admission_contract": "strict_v1", **document}
    admission = admit_source_registry_document(
        document, registry_path=Path("/nonexistent/source_registry.yaml"),
    )
    assert admission.contract == ("strict_v1" if strict else "legacy_v1")
    return admission.entries[name]


def _options(**overrides) -> IngestOptions:
    base = dict(
        dsn="postgresql://fake/strict-mode",
        deployment="ws",
        deployment_dir=Path("/nonexistent"),
        run_mode="scope_complete",
    )
    base.update(overrides)
    return IngestOptions(**base)


def _mode(options: IngestOptions, entry) -> str:
    return _run_mode_for_source(options, entry, entry.name, (), None)


def test_plain_ingest_runs_a_strict_reference_catalog_in_release_new():
    entry = _entry(strict=True)
    mode = _mode(_options(), entry)
    assert mode == "release_new"
    # The runner's own gate accepts what the service now hands it.
    assert (entry.source_class, mode) in _POLICY_MODE


@pytest.mark.parametrize(
    ("source_class", "expected"),
    [
        ("discovery_crawl", "scope_complete"),
        ("reference_catalog", "release_new"),
        ("mutable_api", "reconcile"),
        ("append_feed", "append"),
    ],
)
def test_every_strict_profile_gets_a_mode_the_runner_accepts(source_class, expected):
    # The registry schema ties identity and deletion fields to the profile,
    # so vary only the profile on the admitted shipped entry.
    entry = dataclasses.replace(_entry(strict=True), source_class=source_class)
    mode = _mode(_options(), entry)
    assert mode == expected
    assert (source_class, mode) in _POLICY_MODE


def test_an_older_contract_source_with_a_profile_gets_a_mode_it_accepts():
    # #2501: `compute_env_local` on the older registry contract still declares
    # `reference_catalog`. Handed `scope_complete`, the runner's retraction
    # guard refused the first run that had to delete a record.
    entry = _entry(strict=False)
    assert entry.source_class == "reference_catalog"
    mode = _mode(_options(), entry)
    assert mode == "release_new"
    assert (entry.source_class, mode) in _POLICY_MODE


def test_a_source_with_no_profile_keeps_the_requested_mode():
    entry = dataclasses.replace(_entry(strict=False), source_class=None)
    assert _mode(_options(), entry) == "scope_complete"


def test_the_shipped_default_is_the_reference_catalog_under_test():
    assert _entry(strict=True).source_class == "reference_catalog"


def test_an_explicit_mode_is_never_rewritten():
    entry = _entry(strict=True)
    explicit = dict(run_mode_explicit=True)
    # `okg ingest --mode scope_complete` typed by the operator stays as typed
    # and reaches the runner's refusal, instead of silently becoming another
    # mode.
    assert _mode(_options(**explicit), entry) == "scope_complete"
    assert _mode(_options(run_mode="release_unchanged", **explicit), entry) == "release_unchanged"
    assert _mode(_options(run_mode="release_unchanged"), entry) == "release_unchanged"
    assert _mode(_options(run_mode="cursor"), entry) == "cursor"


def test_ingest_cli_mode_defaults_to_unset():
    parser = _build_parser()
    assert parser.parse_args(["ingest", "--deployment", "ws"]).mode is None
    typed = parser.parse_args(["ingest", "--deployment", "ws", "--mode", "scope_complete"])
    assert typed.mode == "scope_complete"


def test_cmd_ingest_marks_only_a_typed_mode_explicit(monkeypatch, tmp_path):
    from okg.substrate.cli import main as cli_main
    from okg.substrate.ingest import service

    captured: list[IngestOptions] = []

    class _Stop(Exception):
        pass

    def _fake_run_ingest(options):
        captured.append(options)
        raise _Stop

    registry = tmp_path / "source_registry.yaml"
    registry.write_text(yaml.safe_dump(
        {"sources": {"compute_env_local": _shipped_compute_env_local()}},
    ))
    monkeypatch.setattr(service, "run_ingest", _fake_run_ingest)
    monkeypatch.setattr(
        "okg.substrate.deployments.admission.require_active_deployment",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(cli_main, "_resolve_dsn", lambda args: "postgresql://fake/x")
    monkeypatch.setattr(cli_main, "_deployment_runtime_enabled", lambda d: False)
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda *a, **k: {"allowed": True},
    )
    monkeypatch.setattr(
        cli_main, "_guard_inline_publish_against_active_source_sync",
        lambda **k: None,
    )
    for argv in (["ingest", "--deployment", str(tmp_path)],
                 ["ingest", "--deployment", str(tmp_path), "--mode", "scope_complete"]):
        args = _build_parser().parse_args(argv)
        with pytest.raises(_Stop):
            cli_main._cmd_ingest(args, as_json=True)
    assert [(o.run_mode, o.run_mode_explicit) for o in captured] == [
        ("scope_complete", False),
        ("scope_complete", True),
    ]


def test_the_runner_refusal_names_the_profile_mode():
    from okg.substrate.ingest.runner import _profile_mode_fix_hint

    hint = _profile_mode_fix_hint("reference_catalog")
    assert "--mode release_new" in hint
    assert "release_unchanged" in hint
    assert "omit --mode" in hint


def test_a_contract_mismatch_names_the_way_out():
    with pytest.raises(SubstrateError) as caught:
        ir.require_exact_repair_contract(
            expected={"source_contract": [{"source": "compute_env_local", "run_mode": "scope_complete"}]},
            actual_source_contract=[{"source": "compute_env_local", "run_mode": "release_new"}],
            deployment="ws",
            checkpoint_id="cp",
        )
    assert "okg runtime supersede-checkpoint --deployment ws" in caught.value.fix_hint


def test_explicit_per_source_modes_win():
    entry = _entry(strict=True)
    options = _options(run_modes_by_source={entry.name: "release_unchanged"})
    assert _mode(options, entry) == "release_unchanged"


def test_the_recovery_contract_records_the_mode_that_ran():
    entry = _entry(strict=True)
    options = _options()
    contract = ir.build_source_contract(
        ordered=(entry.name,),
        runnable={entry.name: entry},
        deps={},
        source_events={},
        source_binding_plans={},
        run_mode_for_source=lambda e, n, ev, sc: _run_mode_for_source(
            options, e, n, ev, sc,
        ),
    )
    assert contract[0]["run_mode"] == "release_new"


def _no_runtime_deployment(monkeypatch):
    import okg.substrate.deployments as deployments

    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda *a, **k: argparse.Namespace(
            runtime=argparse.Namespace(enabled=False),
        ),
    )


def test_supersede_checkpoint_runs_without_an_enabled_runtime(monkeypatch, capsys):
    """A plain inline ingest can block a checkpoint on an install that runs
    no background worker; the recorded way out must not demand one."""
    args = _build_parser().parse_args([
        "runtime", "supersede-checkpoint",
        "--deployment", "ws",
        "--reason", "blocked by #2476",
        "--json",
    ])
    seen: dict = {}

    def _fake_supersede(**kwargs):
        seen.update(kwargs)
        return {
            "deployment": "ws",
            "checkpoint_id": "cp",
            "prior_state": ir.REPAIR_REQUIRED,
            "required_sources": ["compute_env_local"],
            "reason": kwargs["reason"],
            "actor": kwargs["actor"],
            "applied": False,
            "next_command": "okg runtime supersede-checkpoint --apply",
        }

    monkeypatch.setattr(ir, "supersede_incomplete_checkpoint", _fake_supersede)
    _no_runtime_deployment(monkeypatch)
    args.dsn = "postgresql://fake/strict-mode"
    rc = cli_runtime._cmd_runtime(args, as_json=True)
    out = capsys.readouterr().out
    assert rc == 0, out
    assert seen["deployment"] == "ws"
    assert json.loads(out)["checkpoint_id"] == "cp"


def test_other_runtime_verbs_still_need_an_enabled_runtime(monkeypatch, capsys):
    args = _build_parser().parse_args([
        "runtime", "list-schedules", "--deployment", "ws", "--json",
    ])
    _no_runtime_deployment(monkeypatch)
    args.dsn = "postgresql://fake/strict-mode"
    rc = cli_runtime._cmd_runtime(args, as_json=True)
    assert rc != 0
    assert "runtime is not enabled" in capsys.readouterr().err


def _zero_output(run_mode: str, next_cursor):
    from okg.substrate.ingest.zero_output_admission import (
        _zero_output_admission_verdict,
    )

    return _zero_output_admission_verdict(
        source="ws.compute_env_local",
        run_id="run-1",
        admission_mode="fast_track",
        completed_scope=True,
        source_health_status="ok",
        run_mode=run_mode,
        next_cursor=next_cursor,
    )["producer_policy"]


def test_an_all_deduped_release_new_replay_is_an_authoritative_replay():
    """A forced replay of an unchanged catalog, in the mode plain ingest now
    picks, publishes the way the same replay in scope_complete always did."""
    cursor = {"record_set": {"records": {"machine:host": {"descendants": []}}}}
    policy = _zero_output("release_new", cursor)
    assert policy["status"] == "matched"
    assert policy["reason"] == "authoritative_replay"
    assert _zero_output("scope_complete", cursor)["reason"] == "authoritative_replay"


def test_a_release_new_run_without_a_record_set_is_still_refused():
    policy = _zero_output("release_new", {"probe": "x"})
    assert policy["status"] == "rejected"
    assert policy["reason"] == "unclassified_empty"

