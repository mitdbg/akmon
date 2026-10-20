"""One run-mode policy per source profile (#2569 item 3, #2367, #2486).

`okg.substrate.source_run_modes` is the only table of which run modes a
source profile accepts and which one reads its whole scope. These tests pin
every former copy to it, forbid a new copy, and check the three callers that
used to hand a non-crawl source `scope_complete`: plain ingest for a source on
the older registry contract, bootstrap fixture verification, and queued
ingest with a typed `--mode`.
"""
from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

import okg.substrate
from okg.substrate import source_run_modes as policy
from okg.substrate.deployment_bootstrap.workflow import fixture_run_mode
from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.runner import (
    _AUTHORITATIVE_REPLAY_MODE,
    _POLICY_MODE,
    profile_native_mode,
)
from okg.substrate.ingest.service import (
    IngestOptions,
    _run_mode_for_source,
    _validate_run_modes_by_source,
)
from okg.substrate.policy.retraction import RunMode, retraction_permitted
from okg.substrate.source_sync import SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
from okg.substrate.source_sync_convergence import COMPLETED_SCOPE_RUN_MODES
from okg.substrate.sources.guided_add import complete_scan_mode


_PROFILES = sorted(policy.PROFILE_RUN_MODES)
_WHOLE_SCOPE = {
    "mutable_api": "reconcile",
    "discovery_crawl": "scope_complete",
    "reference_catalog": "release_new",
    "append_feed": "append",
}


# --- one table -------------------------------------------------------------


def test_the_policy_covers_every_runnable_profile():
    assert set(policy.PROFILE_RUN_MODES) == set(_WHOLE_SCOPE)
    for profile, whole in _WHOLE_SCOPE.items():
        assert policy.whole_scope_mode(profile) == whole


def test_the_runner_tables_are_derived_from_the_policy():
    assert set(_POLICY_MODE) == {
        (profile, mode)
        for profile, modes in policy.PROFILE_RUN_MODES.items()
        for mode in modes.accepted
    }
    assert all(value == RunMode(mode) for (_p, mode), value in _POLICY_MODE.items())
    assert _AUTHORITATIVE_REPLAY_MODE == _WHOLE_SCOPE


def test_both_completed_scope_sets_are_the_policy_set():
    assert policy.COMPLETED_SCOPE_RUN_MODES == {
        "reconcile", "scope_complete", "release_new", "release_unchanged",
    }
    assert SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES is policy.COMPLETED_SCOPE_RUN_MODES
    assert COMPLETED_SCOPE_RUN_MODES is policy.COMPLETED_SCOPE_RUN_MODES


@pytest.mark.parametrize("profile", _PROFILES)
def test_guided_add_scans_in_the_policy_whole_scope_mode(profile):
    assert complete_scan_mode(profile) == _WHOLE_SCOPE[profile]


def test_guided_add_keeps_the_request_for_an_unknown_profile():
    assert complete_scan_mode("unknown") == "scope_complete"


@pytest.mark.parametrize("profile", _PROFILES)
def test_the_per_source_mode_check_reads_the_policy(profile):
    plan = SimpleNamespace(
        ordered=("s",), runnable={"s": SimpleNamespace(source_class=profile)},
    )
    for mode in sorted(policy.accepted_modes(profile)):
        options = IngestOptions("unused", "t", Path("."), run_modes_by_source={"s": mode})
        _validate_run_modes_by_source(options, plan)
    refused = {"cursor", "scope_complete", "release_new", "append"} - policy.accepted_modes(profile)
    for mode in sorted(refused):
        options = IngestOptions("unused", "t", Path("."), run_modes_by_source={"s": mode})
        with pytest.raises(SubstrateError):
            _validate_run_modes_by_source(options, plan)


@pytest.mark.parametrize("profile", ["mutable_api", "discovery_crawl", "reference_catalog"])
def test_the_whole_scope_mode_of_a_retracting_profile_may_retract(profile):
    # Translating a whole-scope request is only useful if the mode it
    # lands on may delete a record that went missing.
    permitted, reason = retraction_permitted(
        source_class=profile, run_mode=RunMode(policy.whole_scope_mode(profile)),
    )
    assert permitted, reason


_PROFILE_NAMES = "|".join(sorted(_WHOLE_SCOPE))
_MODE_NAMES = "cursor|reconcile|scope_complete|scope_incomplete|release_new|release_unchanged|append"
# A profile name used as a table key whose value (or tuple key partner) is a
# run mode: `"mutable_api": "reconcile"`, `"mutable_api": {"cursor", ...}`,
# `("mutable_api", "cursor"):`.
_PROFILE_MODE_TABLE = re.compile(
    rf"""["']({_PROFILE_NAMES})["']\s*
        (?: :\s*(?:frozenset\(|set\()?\s*[\[{{(]?\s*
          | ,\s*
        )
        ["']({_MODE_NAMES})["']""",
    re.VERBOSE,
)


def test_no_other_module_defines_a_profile_to_mode_table():
    src = Path(okg.substrate.__file__).parent
    owner = Path(policy.__file__).resolve()
    offenders = []
    for path in sorted(src.rglob("*.py")):
        if path.resolve() == owner:
            continue
        text = path.read_text(encoding="utf-8")
        for match in _PROFILE_MODE_TABLE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(src)}:{line}: {match.group(0)}")
    assert offenders == [], (
        "declare profile run modes in okg.substrate.source_run_modes only:\n"
        + "\n".join(offenders)
    )


def test_the_table_scan_recognises_the_shapes_it_forbids():
    for text in (
        '"mutable_api": "reconcile",',
        '"discovery_crawl": {"scope_complete", "scope_incomplete"},',
        "(\"reference_catalog\", \"release_new\"): PolicyRunMode.RELEASE_NEW,",
        '"append_feed": frozenset({"append"}),',
    ):
        assert _PROFILE_MODE_TABLE.search(text), text
    assert not _PROFILE_MODE_TABLE.search('source_class="mutable_api", mode="reconcile"')


# --- whole-scope requests ---------------------------------------------------


@pytest.mark.parametrize("profile", _PROFILES)
@pytest.mark.parametrize("contract", ["strict_v1", "legacy_v1"])
def test_a_whole_scope_request_gets_a_mode_the_profile_accepts(profile, contract):
    entry = SimpleNamespace(source_class=profile, admission_contract=contract)
    options = IngestOptions("unused", "t", Path("."), run_mode="scope_complete")
    mode = _run_mode_for_source(options, entry, "s", ())
    assert mode == _WHOLE_SCOPE[profile]
    assert (profile, mode) in _POLICY_MODE


@pytest.mark.parametrize("profile", _PROFILES)
def test_a_mode_the_profile_accepts_is_never_changed(profile):
    entry = SimpleNamespace(source_class=profile)
    for mode in sorted(policy.accepted_modes(profile)):
        assert profile_native_mode(entry, mode) == mode


def test_other_modes_and_profile_less_sources_are_never_changed():
    assert profile_native_mode(SimpleNamespace(source_class=None), "scope_complete") == "scope_complete"
    assert profile_native_mode(SimpleNamespace(), "scope_complete") == "scope_complete"
    assert profile_native_mode(SimpleNamespace(source_class="registry_only"), "scope_complete") == "scope_complete"
    # Only the whole-scope request is translated; a wrong specific mode is
    # left for the runner's refusal and its fix hint.
    assert profile_native_mode(SimpleNamespace(source_class="mutable_api"), "release_new") == "release_new"


def test_a_typed_mode_is_never_rewritten():
    entry = SimpleNamespace(source_class="mutable_api", admission_contract="legacy_v1")
    options = IngestOptions(
        "unused", "t", Path("."), run_mode="scope_complete", run_mode_explicit=True,
    )
    assert _run_mode_for_source(options, entry, "s", ()) == "scope_complete"


def test_the_refusal_hint_names_the_profile_whole_scope_mode():
    assert "--mode reconcile" in policy.profile_mode_fix_hint("mutable_api")
    assert "cursor, reconcile" in policy.profile_mode_fix_hint("mutable_api")
    assert policy.profile_mode_fix_hint("live_overlay") == "use the profile-native source run mode"


# --- bootstrap fixture verification (#2486 part 1) ---------------------------


@pytest.mark.parametrize("profile", _PROFILES)
def test_bootstrap_verifies_a_strict_source_in_its_profile_whole_scope_mode(profile):
    binding = SimpleNamespace(executable_profile=profile)
    entry = SimpleNamespace(source_class=profile)
    mode = fixture_run_mode(entry, binding)
    assert mode == _WHOLE_SCOPE[profile]
    assert (profile, mode) in _POLICY_MODE


def test_bootstrap_resolves_a_non_strict_source_by_its_declared_profile():
    assert fixture_run_mode(SimpleNamespace(source_class="reference_catalog")) == "release_new"
    assert fixture_run_mode(SimpleNamespace(source_class=None)) == "scope_complete"


def test_bootstrap_hands_the_resolved_mode_to_the_runner():
    source = Path(
        okg.substrate.deployment_bootstrap.workflow.__file__
    ).read_text(encoding="utf-8")
    assert ".run(adapter, mode=fixture_run_mode(entry, strict_binding))" in source
    assert 'mode="scope_complete"' not in source


# --- checkpoints recorded before the upgrade ---------------------------------


def _contract(**modes):
    return [
        {"source": name, "ownership_id": f"ws.{name}", "run_mode": mode, "sync_scope": {}}
        for name, mode in modes.items()
    ]


def test_a_repair_recorded_as_a_whole_scope_request_matches_the_resolved_mode():
    from okg.substrate.ingest.incomplete_recovery import require_exact_repair_contract

    # Recorded before the upgrade: an older-contract reference_catalog source
    # beside a strict one already recorded in its own mode.
    recorded = {"source_contract": _contract(env="scope_complete", cat="release_new", docs="scope_complete")}
    profiles = {"env": "reference_catalog", "cat": "reference_catalog", "docs": "discovery_crawl"}
    require_exact_repair_contract(
        expected=recorded,
        actual_source_contract=_contract(env="release_new", cat="release_new", docs="scope_complete"),
        deployment="ws",
        checkpoint_id="cp-1",
        source_profiles=profiles,
    )


@pytest.mark.parametrize(
    ("actual", "profiles"),
    [
        # Not what the request resolves to for this profile.
        ({"env": "release_unchanged"}, {"env": "reference_catalog"}),
        ({"env": "reconcile"}, {"env": "reference_catalog"}),
        # No profile passed, or a profile-less source: no tolerance.
        ({"env": "release_new"}, None),
        ({"env": "release_new"}, {"env": None}),
    ],
)
def test_any_other_repair_difference_is_still_refused(actual, profiles):
    from okg.substrate.ingest.incomplete_recovery import require_exact_repair_contract

    with pytest.raises(SubstrateError, match="recovery scope or binding changed"):
        require_exact_repair_contract(
            expected={"source_contract": _contract(env="scope_complete")},
            actual_source_contract=_contract(**actual),
            deployment="ws",
            checkpoint_id="cp-1",
            source_profiles=profiles,
        )


def test_a_repair_with_another_changed_field_is_still_refused():
    from okg.substrate.ingest.incomplete_recovery import require_exact_repair_contract

    actual = _contract(env="release_new")
    actual[0]["ownership_id"] = "ws.other"
    with pytest.raises(SubstrateError, match="recovery scope or binding changed"):
        require_exact_repair_contract(
            expected={"source_contract": _contract(env="scope_complete")},
            actual_source_contract=actual,
            deployment="ws",
            checkpoint_id="cp-1",
            source_profiles={"env": "reference_catalog"},
        )


# --- queued okg ingest (#2486 part 2) ----------------------------------------


_MIXED_REGISTRY = """\
sources:
  crawl:
    module: no_such_module
    class: NoSuchSource
    ownership_id: test_dep.crawl
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [path]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    sync:
      triggers: [manual, reconcile]
      default_event_mode: scope_complete
      reconcile_mode: scope_complete
  api:
    module: no_such_module
    class: NoSuchSource
    ownership_id: test_dep.api
    source_class: mutable_api
    record_identity_kind: remote_id
    record_identity_fields: [id]
    source_revision_kind: updated_at
    deletion_semantics: explicit_delete
    publication_mode: published_generation
    predecessor_source: test_dep.crawl
    sync:
      triggers: [manual, reconcile]
      default_event_mode: cursor
      reconcile_mode: reconcile
"""


def _queued_ingest_calls(tmp_path, monkeypatch, *mode_args):
    import okg.substrate.cli.main as cli
    import okg.substrate.runtime.admin as runtime_admin

    dep = tmp_path / "deployments" / "test_dep"
    (dep / "schemas").mkdir(parents=True)
    (dep / "source_registry.yaml").write_text(_MIXED_REGISTRY)
    (dep / "deployment.yaml").write_text(
        "name: test_dep\nschema_dir: schemas\n"
        "source_registry: source_registry.yaml\nmodules: []\n"
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda deployment: True)
    calls: list[dict] = []

    def fake_enqueue_source_sync(**kwargs):
        calls.append(dict(kwargs))
        return SimpleNamespace(
            workflow_id=f"wf-{len(calls)}", status="SUCCESS", any_failed=False,
            elapsed_seconds=0.1, report={"results": []}, error=None,
        )

    monkeypatch.setattr(runtime_admin, "client_enqueue_source_sync", fake_enqueue_source_sync)
    rc = cli.main([
        "--json", "ingest", "--deployment", "test_dep",
        "--dsn", "postgresql://unused@127.0.0.1:1/unused",
        *mode_args, "--include", "crawl,api", "--no-publish",
    ])
    assert rc == 0
    return [(call["run_mode"], call["run_mode_explicit"]) for call in calls]


def test_queued_ingest_marks_only_a_batch_in_the_typed_mode_as_typed(tmp_path, monkeypatch, capsys):
    # `--mode reconcile` is split per source into its registry reconcile
    # mode. The crawl batch's `scope_complete` is the CLI's own rewrite, not
    # the operator's typed mode, so the workflow still resolves it per profile.
    assert _queued_ingest_calls(tmp_path, monkeypatch, "--mode", "reconcile") == [
        ("scope_complete", False),
        ("reconcile", True),
    ]
    capsys.readouterr()


def test_queued_ingest_marks_an_omitted_mode_as_not_typed(tmp_path, monkeypatch, capsys):
    assert _queued_ingest_calls(tmp_path, monkeypatch) == [("scope_complete", False)]
    capsys.readouterr()
