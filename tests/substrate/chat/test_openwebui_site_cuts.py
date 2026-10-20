"""The Open WebUI 0.11.4 cut list, driven by ``okg chat sync`` (PACT
openwebui-0114-pin-and-cuts).

The rule: cut a vendor feature only when a SETTING cuts it. Each test
drives the real :func:`okg.substrate.chat.sync.sync` against
:class:`tests.substrate.chat.fake_openwebui.FakeInstance` and asserts on the
instance's stored state (the wire), then proves the readback catches the
setting drifting back.

The vendor facts each lever rests on are cited in ``chat/sync.py`` against
the v0.11.4 source (revision 8bd8b4fa).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.instance import (
    CONTAINER_FIXED_ENV,
    AppDatabase,
    PortPlan,
    build_instance_spec,
    normalize_dsn,
    settings_fingerprint,
)
from okg.substrate.chat.plan_apply import chat_readback, resolve_chat_plan
from okg.substrate.chat.sync import (
    CHANGELOG_ADMIN_KEY,
    CHANGELOG_DEFAULT_KEY,
    ENV_EASTER_EGGS_KEY,
    ENV_UPDATE_CHECK_KEY,
    GOVERNED_TOGGLES,
    SURFACE_INSTANCE_UI,
    SURFACE_PRESET,
    SUGGESTIONS_I18N_KEY,
    SUGGESTIONS_KEY,
    ChatSyncError,
)
from tests.substrate.test_chat_sync_projection import (
    _client,
    _instance,
    _make_inputs,
    _raw_manifest,
    _run,
)

#: Every config key the cut list switches off, and the vendor default the
#: fake holds before the run.
CUT_CONFIG_KEYS = (
    "task.follow_up.enable",
    "task.tags.enable",
    "task.query.retrieval.enable",
    "task.autocomplete.enable",
    "ui.enable_community_sharing",
    "users.enable_status",
    "calendar.enable",
    "automations.enable",
)


def _instance_with_env_off() -> Any:
    """An instance started by this change's `chat-instance up`."""
    instance = _instance(webui_name="Fixture Site")
    instance.env_features = {
        "enable_version_update_check": False,
        "enable_easter_eggs": False,
    }
    return instance


def _ui_diff(report: Any) -> dict[str, dict[str, Any]]:
    return {entry["key"]: entry for entry in report.surfaces[SURFACE_INSTANCE_UI].diff}


def _manifest_without(*features: str) -> dict[str, Any]:
    raw = _raw_manifest()
    for name in features:
        raw["chat"]["ui"]["features"].pop(name)
    return raw


# -- cuts 6-9, 11-14: config keys ------------------------------------------


@pytest.mark.parametrize("key", CUT_CONFIG_KEYS)
def test_each_cut_config_key_is_driven_off_and_read_back(tmp_path: Path, key: str) -> None:
    instance = _instance_with_env_off()

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok, report.to_dict()
    assert instance.config[key] is False
    entry = _ui_diff(report)[f"config:{key}"]
    assert entry["verified"] is True and entry["expected"] is False


def test_autocomplete_is_written_even_when_absent_from_the_export(tmp_path: Path) -> None:
    """Default OFF at the vendor, but a key the site never stored is absent
    from the v0.11.4 export: absent is unreadable, so the run writes it."""
    instance = _instance_with_env_off()
    del instance.config["task.autocomplete.enable"]

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    assert instance.config["task.autocomplete.enable"] is False
    assert _ui_diff(report)["config:task.autocomplete.enable"]["changed"] is True


def test_notes_are_never_touched(tmp_path: Path) -> None:
    instance = _instance_with_env_off()

    _run(_make_inputs(tmp_path), instance)

    assert instance.config["notes.enable"] is True
    imported = [
        payload for method, path, payload in instance.request_log
        if path == "/api/v1/configs/import"
    ]
    assert imported, "the run imported no config at all"
    assert all("notes.enable" not in (p.get("config") or {}) for p in imported)
    assert all(t.config_keys != ("notes.enable",) for t in GOVERNED_TOGGLES)


def test_a_cut_config_key_that_drifts_back_on_is_restored(tmp_path: Path) -> None:
    instance = _instance_with_env_off()
    inputs = _make_inputs(tmp_path)
    _run(inputs, instance)
    instance.config["calendar.enable"] = True  # an admin flips it back

    report = _run(inputs, instance)

    assert report.ok
    entry = _ui_diff(report)["config:calendar.enable"]
    assert entry["changed"] is True and entry["verified"] is True
    assert instance.config["calendar.enable"] is False


def test_a_cut_that_does_not_read_back_refuses(tmp_path: Path) -> None:
    """Failure case: the instance drops the write. Never counted verified."""
    instance = _instance_with_env_off()
    instance.omit_export_keys.add("users.enable_status")

    with pytest.raises(ChatSyncError) as caught:
        _run(_make_inputs(tmp_path), instance)

    assert caught.value.data["key"] == "config:users.enable_status"
    assert caught.value.data["reason"] == "readback_unreadable"


# -- cuts 3 and 4: arena and channels default off --------------------------


def test_arena_and_channels_default_off_when_undeclared(tmp_path: Path) -> None:
    """Fail-before: an undeclared toggle was not driven, and the vendor
    default for the arena is ON."""
    instance = _instance_with_env_off()
    assert instance.config["evaluation.arena.enable"] is True

    report = _run(
        _make_inputs(tmp_path, manifest=_manifest_without("arena", "channels")),
        instance,
    )

    assert report.ok, report.to_dict()
    assert instance.config["evaluation.arena.enable"] is False
    assert instance.config["channels.enable"] is False
    assert instance.permissions["features"]["channels"] is False


def test_a_manifest_can_still_turn_the_arena_on(tmp_path: Path) -> None:
    raw = _raw_manifest()
    raw["chat"]["ui"]["features"]["arena"] = True
    instance = _instance_with_env_off()

    report = _run(_make_inputs(tmp_path, manifest=raw), instance)

    assert report.ok
    assert instance.config["evaluation.arena.enable"] is True


# -- cut 17: voice mode (users only) and cut 16: workspace knowledge -------


def test_voice_mode_and_workspace_knowledge_permissions_are_off(tmp_path: Path) -> None:
    instance = _instance_with_env_off()
    assert instance.permissions["chat"]["call"] is True

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    assert instance.permissions["chat"]["call"] is False
    # The other chat permissions were carried forward, not replaced.
    assert instance.permissions["chat"]["controls"] is True
    assert instance.permissions["workspace"]["knowledge"] is False
    diff = _ui_diff(report)
    assert diff["permission:chat.call"]["verified"] is True
    assert diff["permission:workspace.knowledge"]["verified"] is True


# -- cut 5: vendor suggested prompts ---------------------------------------


def test_vendor_suggested_prompts_are_emptied_through_their_route(tmp_path: Path) -> None:
    """A null list is the browser's own localized starters; an i18n entry
    would override the list. Both are driven, through /configs/suggestions."""
    instance = _instance_with_env_off()
    instance.config["ui.prompt_suggestions_i18n"] = {
        "de-DE": {"suggestion_prompts": [{"content": "Hallo"}]},
    }

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    assert instance.config["ui.prompt_suggestions"] == []
    assert instance.config["ui.prompt_suggestions_i18n"] == {}
    posted = [p for m, path, p in instance.request_log if path == "/api/v1/configs/suggestions"]
    assert posted == [{"suggestions": [], "i18n": {}}]
    diff = _ui_diff(report)
    assert diff[SUGGESTIONS_KEY]["verified"] and diff[SUGGESTIONS_I18N_KEY]["verified"]
    # The preset's OWN suggestions are a separate, manifest-owned surface.
    preset = instance.model_row("okg-fixture")
    assert preset["meta"]["suggestion_prompts"] == [{"content": "What changed last week?"}]


# -- cut 1: What's New -----------------------------------------------------


def test_whats_new_is_off_site_wide_and_for_the_admin(tmp_path: Path) -> None:
    instance = _instance_with_env_off()
    instance.config["ui.default_interface_settings"] = {"textScale": 1.1}

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    # The site default was merged, not replaced.
    assert instance.config["ui.default_interface_settings"] == {
        "textScale": 1.1, "showChangelog": False,
    }
    diff = _ui_diff(report)
    assert diff[CHANGELOG_DEFAULT_KEY]["verified"] is True
    assert diff[CHANGELOG_ADMIN_KEY]["verified"] is True


def test_an_admin_who_turned_whats_new_back_on_is_reset(tmp_path: Path) -> None:
    """The admin's own setting wins over the site default, so the merged
    value is what the readback checks and what the run corrects."""
    instance = _instance_with_env_off()
    instance.user_ui_settings[""] = {"showChangelog": True, "chatBubble": False}

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    assert instance.user_ui_settings[""] == {"showChangelog": False, "chatBubble": False}
    assert _ui_diff(report)[CHANGELOG_ADMIN_KEY]["changed"] is True


# -- cuts 2 and 15: environment-owned --------------------------------------


def test_container_env_turns_off_update_check_and_easter_eggs() -> None:
    app_dsn = "postgresql://app@127.0.0.1:5999/app"
    graph_dsn = "postgresql://graph@127.0.0.1:5998/graph"
    spec = build_instance_spec(
        "fixture",
        app_database=AppDatabase(
            env_var="APP",
            target=normalize_dsn(app_dsn, label="app"),
            dsn=app_dsn,
            graph_target=normalize_dsn(graph_dsn, label="graph"),
        ),
        secret_env="SECRET",
        environ={"SECRET": "fixture-key"},
        port_plan=PortPlan(instance_port=18080, source="test"),
    )

    assert spec.env["ENABLE_VERSION_UPDATE_CHECK"] == "false"
    assert spec.env["ENABLE_EASTER_EGGS"] == "false"
    assert dict(CONTAINER_FIXED_ENV) == {
        "ENABLE_VERSION_UPDATE_CHECK": "false", "ENABLE_EASTER_EGGS": "false",
    }
    # Part of the fingerprint: a container started without them is a
    # different setting, so `up` refuses `instance_settings_changed`.
    without = {k: v for k, v in spec.env.items() if k not in CONTAINER_FIXED_ENV}
    assert settings_fingerprint(without) != settings_fingerprint(spec.env)


def test_env_owned_cuts_read_back_from_the_running_site(tmp_path: Path) -> None:
    instance = _instance_with_env_off()

    report = _run(_make_inputs(tmp_path), instance)

    diff = _ui_diff(report)
    assert diff[ENV_UPDATE_CHECK_KEY]["verified"] is True
    assert diff[ENV_EASTER_EGGS_KEY]["verified"] is True


def test_an_old_container_reports_env_drift_without_blocking_other_cuts(tmp_path: Path) -> None:
    """Failure case: a container started before this change still shows the
    update toast. The deviation is reported with the recreate hint, and the
    API-owned cuts still land."""
    instance = _instance(webui_name="Fixture Site")
    instance.env_features = {  # the vendor default: started some other way
        "enable_version_update_check": True,
        "enable_easter_eggs": True,
    }

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    diff = _ui_diff(report)
    assert diff[ENV_UPDATE_CHECK_KEY]["verified"] is False
    assert diff[ENV_UPDATE_CHECK_KEY]["non_blocking"] is True
    assert "chat-instance down" in diff[ENV_UPDATE_CHECK_KEY]["fix_hint"]
    notes = report.surfaces[SURFACE_INSTANCE_UI].notes
    assert any(ENV_EASTER_EGGS_KEY in note for note in notes)
    assert instance.config["calendar.enable"] is False


# -- cut 10: built-in knowledge tools only ---------------------------------


def test_preset_turns_off_builtin_knowledge_tools_only(tmp_path: Path) -> None:
    instance = _instance_with_env_off()

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    meta = instance.model_row("okg-fixture")["meta"]
    assert meta["builtinTools"] == {"knowledge": False}
    # Never the whole built-in tool capability: notes, time and chats stay.
    assert "capabilities" not in meta
    entry = {e["key"]: e for e in report.surfaces[SURFACE_PRESET].diff}[
        "preset:meta.builtinTools.knowledge"
    ]
    assert entry["verified"] is True


def test_site_default_builtin_tool_choices_are_carried_onto_the_preset(tmp_path: Path) -> None:
    """Fail-before (review of #2957): the preset got `{"knowledge": false}`
    whole, and because v0.11.4 fills a model's builtinTools from the site
    default only when the model has none, the operator's site-wide
    `chats: false` was switched back on for the okg preset."""
    instance = _instance_with_env_off()
    instance.config["models.default_metadata"] = {
        "builtinTools": {"chats": False, "web_search": False},
        "capabilities": {"vision": False},
    }

    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok, report.to_dict()
    meta = instance.model_row("okg-fixture")["meta"]
    assert meta["builtinTools"] == {"chats": False, "web_search": False, "knowledge": False}


def test_preset_level_builtin_tool_choices_are_kept(tmp_path: Path) -> None:
    instance = _instance_with_env_off()
    inputs = _make_inputs(tmp_path)
    _run(inputs, instance)
    instance.models["okg-fixture"]["meta"]["builtinTools"] = {
        "time": False, "knowledge": True,
    }

    report = _run(inputs, instance)

    assert report.ok
    assert instance.model_row("okg-fixture")["meta"]["builtinTools"] == {
        "time": False, "knowledge": False,
    }


def test_unreadable_model_defaults_refuse_rather_than_guess(tmp_path: Path) -> None:
    """Failure case: a response without DEFAULT_MODEL_METADATA is not read
    as "no site defaults"."""
    instance = _instance_with_env_off()
    instance.omit_model_defaults = True

    with pytest.raises(ChatSyncError) as caught:
        _run(_make_inputs(tmp_path), instance)

    assert caught.value.data["reason"] == "model_defaults_unreadable"


def test_a_preset_whose_knowledge_tools_were_turned_back_on_is_rewritten(tmp_path: Path) -> None:
    instance = _instance_with_env_off()
    inputs = _make_inputs(tmp_path)
    _run(inputs, instance)
    instance.models["okg-fixture"]["meta"]["builtinTools"] = {"knowledge": True}

    report = _run(inputs, instance)

    assert report.ok
    assert instance.model_row("okg-fixture")["meta"]["builtinTools"] == {"knowledge": False}


# -- the readback verb sees every cut --------------------------------------


def test_chat_readback_reports_a_cut_that_drifted(tmp_path: Path) -> None:
    instance = _instance_with_env_off()
    inputs = _make_inputs(tmp_path)
    _run(inputs, instance)
    plan = resolve_chat_plan(inputs)

    clean = chat_readback(inputs, _client(instance), plan, verify=False)
    assert clean["ok"] is True, clean

    instance.config["task.follow_up.enable"] = True
    instance.config["ui.prompt_suggestions"] = None
    instance.models["okg-fixture"]["meta"].pop("builtinTools")

    drifted = chat_readback(inputs, _client(instance), plan, verify=False)

    assert drifted["ok"] is False
    keys = {d["key"] for d in drifted["deviations"]}
    assert {
        "config:task.follow_up.enable",
        SUGGESTIONS_KEY,
        "preset:meta.builtinTools.knowledge",
    } <= keys
