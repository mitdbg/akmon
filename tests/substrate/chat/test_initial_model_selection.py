"""What the browser opens on: the preset, not its raw base model (#1183).

PACT ``chat-initial-model-selection``. A reviewer's finding 1 on
internal issue 1183.

The vendor behavior these tests are written against was MEASURED at
``ghcr.io/open-webui/open-webui:v0.11.0``: the built frontend selects the
initial model with
``selectedModelId = $config?.default_models.split(',')[0]``
(``_app/immutable/chunks/BLLL3FN7.js``), fed by ``ui.default_models``
(``backend/open_webui/config.py:3047``). ``okg chat sync`` wrote
``chat.models.default`` into that field -- and that same value is the
preset's ``base_model_id``, so it has to name a RAW provider model. The
site therefore opened on the one configuration guaranteed to carry no MCP
tools and no system prompt, and a first domain question was answered from
general knowledge with nothing saying the graph was never consulted.

Every assertion below is on the WIRE: the config the fake instance holds
after a real ``sync`` run, and the model rows it holds, never on a value
the projection computed internally.

**What these tests do NOT prove.** Selecting the preset is not the same as
the preset's tools being ATTACHED. Reached through the vendor's API rather
than the browser, v0.11.0 deliberately does not attach a preset's tools
("API callers don't expect hidden tools; they can explicitly request tools
via ``tool_ids``", ``middleware.py:2861``) while still applying the system
prompt -- so the model is told it has graph operators it was not given.
That path is owned by Sprint 14 (#1792) and is not in scope here.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.sync import (
    SURFACE_MODELS,
    ChatSyncError,
    InstanceClient,
    _verify_initial_selection,
    sync,
)
from okg.substrate.deployments.chat_block import (
    ChatManifestError,
    parse_chat_block,
    ui_default_model,
)

from .fake_openwebui import FakeInstance
from .test_write_connection_projection import (
    AUTH_TOKEN_ENV,
    AUTH_TOKEN_VALUE,
    BASE_URL,
    READ_ONLY_TOOLS,
    TOKEN,
    _instance,
    _make_inputs,
    _pin_endpoint,
    _raw_manifest,
)

#: What ``_make_inputs`` names the preset row. Spelt out rather than
#: imported so a change to the derivation is visible here as a failure.
PRESET_ID = "okg-fixture"

#: The raw provider model the fixture manifest declares. It stays the
#: preset's ``base_model_id``; what it stops being is the browser's
#: pre-selection.
BASE_MODEL = "base/model-a"


def _manifest(**models: Any) -> dict[str, Any]:
    """The fixture manifest with ``chat.models`` overridden."""
    manifest = copy.deepcopy(_raw_manifest(sso=False))
    manifest["chat"]["models"].update(models)
    return manifest


def _run(inputs: Any, instance: FakeInstance) -> Any:
    return sync(
        inputs,
        InstanceClient(BASE_URL, TOKEN, opener=instance.opener),
        probe_tools=lambda _endpoint, _env: READ_ONLY_TOOLS,
    )


@pytest.fixture()
def endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    _pin_endpoint(monkeypatch, advertised=READ_ONLY_TOOLS, enforces_auth=True)
    monkeypatch.setenv(AUTH_TOKEN_ENV, AUTH_TOKEN_VALUE)


# ---------------------------------------------------------------------------
# req.opens-on-the-preset
# ---------------------------------------------------------------------------

def test_the_written_field_selects_the_preset_not_the_base_model(
    tmp_path: Path, endpoint: None,
) -> None:
    """The defect, asserted by the vendor's own selection rule.

    Not "the field changed": the assertion applies ``split(',')[0]`` -- what
    the browser does -- to the value the instance actually holds.

    The fixture manifest declares no ``chat.models.ui_default``, so this is
    the UNEDITED deployment: the hazard closes for a manifest nobody
    touched, which is what makes it a fix rather than an opt-in.
    """
    instance = _instance()
    _run(_make_inputs(tmp_path, manifest=_raw_manifest(sso=False)), instance)

    written = instance.config["ui.default_models"]
    assert written.split(",")[0] == PRESET_ID
    assert written.split(",")[0] != BASE_MODEL


def test_the_selected_id_is_a_model_row_that_carries_the_graph_tools(
    tmp_path: Path, endpoint: None,
) -> None:
    """Selecting an id proves nothing unless the id resolves to the preset.

    The preset row is the ONLY configuration on the instance carrying an
    MCP tool id and a system prompt, so this is the assertion that says the
    hazard is closed rather than moved.
    """
    instance = _instance()
    _run(_make_inputs(tmp_path, manifest=_raw_manifest(sso=False)), instance)

    selected = instance.config["ui.default_models"].split(",")[0]
    row = instance.model_row(selected)
    assert row is not None, f"{selected!r} is not a model row on this instance"
    assert row["meta"]["toolIds"] == ["server:mcp:okg-fixture"]
    assert "Use the OKG tools" in row["params"]["system"]


def test_the_preset_still_runs_on_chat_models_default(
    tmp_path: Path, endpoint: None,
) -> None:
    """The two fields stay separate: the fix must not repoint the base model.

    If ``base_model_id`` were changed to the preset id the preset would name
    itself and answer nothing, which would trade a silent wrong answer for a
    broken site.
    """
    instance = _instance()
    _run(_make_inputs(tmp_path, manifest=_raw_manifest(sso=False)), instance)

    assert instance.model_row(PRESET_ID)["base_model_id"] == BASE_MODEL
    # And the raw model is still a curated, granted row of its own.
    assert instance.config["ui.model_order_list"] == [BASE_MODEL]
    assert instance.model_row(BASE_MODEL) is not None


# ---------------------------------------------------------------------------
# req.explicit-opt-out
# ---------------------------------------------------------------------------

def test_ui_default_opts_out_to_a_declared_builtin(
    tmp_path: Path, endpoint: None,
) -> None:
    """A shared instance may open on a raw model -- but only on purpose."""
    instance = _instance()
    inputs = _make_inputs(
        tmp_path, manifest=_manifest(ui_default=BASE_MODEL),
    )

    report = _run(inputs, instance)

    assert instance.config["ui.default_models"] == BASE_MODEL
    assert report.ok


def test_the_opt_out_says_out_loud_what_it_costs(
    tmp_path: Path, endpoint: None,
) -> None:
    """A silent opt-out would re-create the defect as a manifest key."""
    instance = _instance()
    inputs = _make_inputs(
        tmp_path, manifest=_manifest(ui_default=BASE_MODEL),
    )

    report = _run(inputs, instance)

    notes = " ".join(report.surfaces[SURFACE_MODELS].notes)
    assert BASE_MODEL in notes
    assert "no MCP tools" in notes and "no system prompt" in notes


def test_ui_default_naming_an_undeclared_model_refuses_at_load(
    tmp_path: Path,
) -> None:
    """A typo must not open the site on nothing."""
    with pytest.raises(ChatManifestError) as caught:
        parse_chat_block(
            _manifest(ui_default="base/typo"), manifest_path=tmp_path / "deployment.yaml",
        )

    assert caught.value.data["path"] == "chat.models.ui_default"
    assert caught.value.data["requested"] == "base/typo"
    assert caught.value.data["builtin"] == [BASE_MODEL]


def test_the_resolver_answers_the_preset_when_nothing_is_declared() -> None:
    """The one place plan and apply both read, checked directly."""
    assert ui_default_model(None, preset_id=PRESET_ID) == PRESET_ID
    chat = parse_chat_block(_raw_manifest(sso=False), manifest_path=None)
    assert ui_default_model(chat.models, preset_id=PRESET_ID) == PRESET_ID
    opted = parse_chat_block(_manifest(ui_default=BASE_MODEL), manifest_path=None)
    assert ui_default_model(opted.models, preset_id=PRESET_ID) == BASE_MODEL


# ---------------------------------------------------------------------------
# req.selection-is-verified
# ---------------------------------------------------------------------------

def _verify_after(
    inputs: Any, instance: FakeInstance, report: Any,
) -> InstanceClient:
    """Run the end-of-sync check alone against a perturbed instance.

    Driven directly rather than through a second ``sync``: the drift this
    check exists to catch is one that lands AFTER the models surface has
    read its field back and AFTER the preset surface has verified its row,
    and a second full run would simply correct it before the check saw it.
    That is the point -- the two surfaces cannot see this between them.
    ``test_the_verified_run_records_what_it_checked`` proves the check is
    wired into ``sync`` itself.
    """
    client = InstanceClient(BASE_URL, TOKEN, opener=instance.opener)
    _verify_initial_selection(inputs, client, report)
    return client


def test_a_selection_edited_out_of_band_is_reported_not_silently_green(
    tmp_path: Path, endpoint: None,
) -> None:
    """An admin prepends a raw model; the vendor then selects THAT."""
    instance = _instance()
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))
    report = _run(inputs, instance)
    assert instance.config["ui.default_models"] == PRESET_ID

    instance.config["ui.default_models"] = f"{BASE_MODEL},{PRESET_ID}"

    with pytest.raises(ChatSyncError) as caught:
        _verify_after(inputs, instance, report)

    data = caught.value.data
    assert data["reason"] == "initial_selection_not_intended"
    assert data["surface"] == SURFACE_MODELS
    assert data["expected"] == PRESET_ID
    # The vendor takes the HEAD of the list, so a prepended id is the drift
    # even though the preset is still present in the field. A whole-string
    # comparison would call this "changed"; only the vendor's own rule calls
    # it "the browser now opens on base/model-a".
    assert data["observed"] == BASE_MODEL
    assert data["default_models"] == f"{BASE_MODEL},{PRESET_ID}"


def test_a_selection_that_names_no_model_row_is_reported(
    tmp_path: Path, endpoint: None,
) -> None:
    """Reading back correctly and resolving to nothing is still a failure."""
    instance = _instance()
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))
    report = _run(inputs, instance)

    instance.models.pop(PRESET_ID)

    with pytest.raises(ChatSyncError) as caught:
        _verify_after(inputs, instance, report)

    data = caught.value.data
    assert data["reason"] == "initial_selection_row_missing"
    assert data["observed"] == PRESET_ID


def test_a_whitespace_padded_selection_is_not_read_as_a_match(
    tmp_path: Path, endpoint: None,
) -> None:
    """The check may not be more forgiving than the vendor it quotes.

    MEASURED in ``ghcr.io/open-webui/open-webui:v0.11.0``: nothing trims the
    entry. The frontend splits and indexes bare (``default_models.split(",")``
    at every call site), the write route stores the posted string verbatim
    (``backend/open_webui/routers/configs.py:751``) and the read route
    returns it verbatim (``:764``). So the leading space below is part of the
    id the browser looks up, it matches no model row, and the site does NOT
    open on the preset -- while a check that trimmed before comparing would
    report the run green.
    """
    instance = _instance()
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))
    report = _run(inputs, instance)

    instance.config["ui.default_models"] = f" {PRESET_ID}"

    with pytest.raises(ChatSyncError) as caught:
        _verify_after(inputs, instance, report)

    data = caught.value.data
    assert data["reason"] == "initial_selection_not_intended"
    assert data["observed"] == f" {PRESET_ID}"
    assert data["expected"] == PRESET_ID


def test_an_unperturbed_instance_passes_the_same_check(
    tmp_path: Path, endpoint: None,
) -> None:
    """The negative arm: the check must not refuse a healthy site."""
    instance = _instance()
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(sso=False))
    report = _run(inputs, instance)

    _verify_after(inputs, instance, report)


def test_the_verified_run_records_what_it_checked(
    tmp_path: Path, endpoint: None,
) -> None:
    """A check nobody can see the result of is not evidence."""
    instance = _instance()
    report = _run(_make_inputs(tmp_path, manifest=_raw_manifest(sso=False)), instance)

    notes = " ".join(report.surfaces[SURFACE_MODELS].notes)
    assert "initial selection verified" in notes
    assert PRESET_ID in notes
