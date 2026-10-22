"""``okg chat sync`` — the declarative projection (PACT chat-sync-projection).

Every test drives the real :func:`okg.substrate.chat.sync.sync` against
:class:`tests.substrate.chat.fake_openwebui.FakeInstance`, which reproduces
the MEASURED v0.11.0 semantics rather than a convenient mock: the
``WEBUI_NAME`` suffix transform, the silently-broken ``/api/v1/models/sync``,
the silent drop of unknown valve fields, and grant replacement.

The assertions are on the WIRE — the request bodies the instance received and
the state it holds afterwards — not on internal variables, so a projection
that computed the right thing and sent the wrong thing fails here.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat import sync as sync_module
from okg.substrate.chat.sync import (
    ENV_ADMIN_TOKEN,
    ENV_INSTANCE_URL,
    SURFACE_DEEP_LINKS,
    SURFACE_INSTANCE_UI,
    SURFACE_MCP,
    SURFACE_MODELS,
    SURFACE_PRESET,
    SURFACE_PROMPTS,
    WRITTEN_SURFACES,
    ChatSyncError,
    InstanceClient,
    Posture,
    SyncInputs,
    compose_system_prompt,
    driven_knob_set,
    effective_tool_binding,
    expected_site_name,
    load_inputs,
    redact,
    resolve_chat_prompt_path,
    resolve_operator_environment,
    resolve_posture,
    sync,
)
from okg.substrate.chat.data import chat_data_dir
from okg.substrate.deployments.chat_block import (
    UI_KNOB_CENSUS_FILE,
    ChatUiCensusUnavailable,
    load_ui_knob_census,
    parse_chat_block,
    ui_knob_census_path,
)
from tests.substrate.chat.fake_openwebui import FakeInstance
from tests.substrate._fake_credentials import fake_credential

REPO_ROOT = Path(__file__).resolve().parents[2]
CENSUS = ui_knob_census_path()

TOKEN = "fake-admin-token-value-0123456789"
BASE_URL = "http://instance.test"

#: The seven operators a real ``okg mcp-serve`` registers, measured live.
LIVE_TOOLS = ("aggregate", "expand", "filter", "inspect", "map", "query", "search")


# ---------------------------------------------------------------------------
# Fixture deployment
# ---------------------------------------------------------------------------

def _raw_manifest(**overrides: Any) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "name": "fixture",
        "nomos": {"runtime_enforcement": "enforce"},
        "search": {"profiles": {"analyst": {"subtypes": ["document"]}}},
        "chat": {
            "enabled": True,
            "ui": {
                "site_name": "Fixture Site",
                "banner": "Answers are pinned to one published generation.",
                "features": {
                    "web_search": False,
                    "image_generation": False,
                    "code_interpreter": False,
                    "arena": False,
                    "channels": False,
                    "byok": False,
                    "context_compaction": True,
                },
            },
            "models": {"builtin": ["base/model-a"], "default": "base/model-a"},
            "preset": {
                "display_name": "Fixture Preset",
                "description": "Ask the fixture graph.",
                "system_prompt_ref": "chat/system_prompt.md",
                "suggestions": ["What changed last week?"],
            },
            "search_profile": "analyst",
            "mcp": {
                "port": 8100,
                "tools": [
                    "search", "inspect", "expand", "filter", "map",
                    "aggregate", "query",
                ],
            },
            "access": {"groups": ["okg-users"]},
            "deep_links": {
                "operator_ui_base": "http://console.test",
                "chat_site_base": "http://chat.test",
            },
        },
    }
    for key, value in overrides.items():
        if value is None:
            manifest.pop(key, None)
        else:
            manifest[key] = value
    return manifest


def _unmasked_catalog() -> Any:
    """A catalog that COMPOSES and declares no ``pii_classes``.

    This is the stated negation of the masked predicate, not an invented
    "unmasked" manifest field — the schema has none, and a fixture that
    invented one would test a predicate nobody implements.
    """

    class _Catalog:
        subtype_metadata: dict[str, dict[str, Any]] = {"document": {"other": 1}}

    return _Catalog()


def _make_inputs(
    tmp_path: Path,
    *,
    manifest: dict[str, Any] | None = None,
    mcp_instructions: str | None = None,
    system_prompt: str | None = "Use the OKG tools. Quote node ids.",
    catalog_loader: Any = None,
    state_home: Path | None = None,
) -> SyncInputs:
    raw = manifest if manifest is not None else _raw_manifest()
    dep_dir = tmp_path / "deployment"
    (dep_dir / "chat").mkdir(parents=True, exist_ok=True)
    if system_prompt is not None:
        (dep_dir / "chat" / "system_prompt.md").write_text(system_prompt, encoding="utf-8")
    instructions_path = None
    if mcp_instructions is not None:
        instructions_path = dep_dir / "mcp-instructions.md"
        instructions_path.write_text(mcp_instructions, encoding="utf-8")
    chat = parse_chat_block(raw, manifest_path=dep_dir / "deployment.yaml")
    return SyncInputs(
        deployment="fixture",
        deployment_dir=dep_dir,
        raw_manifest=raw,
        chat=chat,
        mcp_instructions_path=instructions_path,
        # None: the packaged directory, resolved only when a run reads it.
        chat_data_dir=None,
        mcp_probe_url="http://127.0.0.1:8100/mcp",
        mcp_instance_url="http://host.docker.internal:8100/mcp",
        preset_id="okg-fixture",
        mcp_server_id="okg-fixture",
        state_home=state_home or (tmp_path / "state"),
        catalog_loader=catalog_loader,
    )


def _instance(**kwargs: Any) -> FakeInstance:
    instance = FakeInstance(**kwargs)
    instance.add_group("okg-users", "group-users")
    instance.install_function(
        "okg_review_queue",
        valve_fields=("deployment", "openwebui_api_base", "admin_group"),
    )
    return instance


def _client(instance: FakeInstance) -> InstanceClient:
    return InstanceClient(BASE_URL, TOKEN, opener=instance.opener)


def _probe(_endpoint: str, _token_env: str | None) -> tuple[str, ...]:
    return LIVE_TOOLS


def _run(inputs: SyncInputs, instance: FakeInstance, **kwargs: Any) -> Any:
    return sync(inputs, _client(instance), probe_tools=kwargs.pop("probe_tools", _probe))


def _preset_row(instance: FakeInstance) -> dict[str, Any]:
    return instance.model_row("okg-fixture")


def _connection(instance: FakeInstance) -> dict[str, Any]:
    for entry in instance.tool_servers:
        if (entry.get("info") or {}).get("id") == "okg-fixture":
            return entry
    raise AssertionError("no okg-fixture MCP connection was registered")


# ---------------------------------------------------------------------------
# Surface 1-6: one run renders the whole site
# ---------------------------------------------------------------------------

def test_sync_renders_instance_ui_from_manifest(tmp_path: Path) -> None:
    """The census-admitted ui knobs reach the instance and read back."""
    instance = _instance(webui_name="Fixture Site")
    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok, report.to_dict()
    assert instance.config["ui.banners"][0]["content"].startswith("Answers are pinned")
    # The api-mechanism toggles were driven on BOTH switches the census
    # measured, not just the global one.
    assert instance.config["web.search.enable"] is False
    assert instance.permissions["features"]["web_search"] is False
    assert instance.config["evaluation.arena.enable"] is False
    assert instance.connections["ENABLE_DIRECT_CONNECTIONS"] is False
    ui = report.surfaces[SURFACE_INSTANCE_UI]
    assert ui.ok and ui.status == "verified"


def test_sync_renders_curated_model_list_and_default(tmp_path: Path) -> None:
    """Curation is a GRANT SET, not a list: each built-in gets a granted row."""
    instance = _instance(webui_name="Fixture Site")
    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    # The field the BROWSER opens on names the preset, not the raw provider
    # model (#1183 finding 1). `base/model-a` is still `chat.models.default`
    # and still the preset's base_model_id -- asserted in
    # tests/substrate/chat/test_initial_model_selection.py -- but it is the
    # one configuration with no tools and no system prompt, so it is not what
    # a reader's first question reaches.
    assert instance.config["ui.default_models"] == "okg-fixture"
    assert instance.config["ui.model_order_list"] == ["base/model-a"]
    row = instance.model_row("base/model-a")
    assert [
        (g["principal_type"], g["principal_id"], g["permission"])
        for g in row["access_grants"]
    ] == [("group", "group-users", "read")]


class TestModelReconciliation:
    """The model inventory is authoritative at the InstanceClient boundary."""

    def test_authoritative_listing_reads_all_pages(self, tmp_path: Path) -> None:
        instance = _instance(webui_name="Fixture Site")
        for index in range(31):
            model_id = f"other/model-{index}"
            instance.models[model_id] = {
                "id": model_id,
                "name": model_id,
                "base_model_id": None,
                "meta": {"description": None},
                "params": {},
                "is_active": True,
            }
        report = _run(_make_inputs(tmp_path), instance)
        assert report.ok, report.to_dict()
        assert instance.requests.count(("GET", "/api/v1/models/list")) >= 2

    def test_empty_export_uses_existing_authoritative_rows_for_idempotence(
        self, tmp_path: Path,
    ) -> None:
        instance = _instance(webui_name="Fixture Site")
        inputs = _make_inputs(tmp_path)
        first = _run(inputs, instance)
        assert first.ok

        # Reproduce #1215: export is empty for this API-key caller, while the
        # workspace listing still contains every model. A duplicate create is
        # the vendor's HTTP 401 response, so the test proves we never reach it.
        instance.empty_export = True
        instance.duplicate_create_401 = True
        instance.requests.clear()
        second = _run(inputs, instance)
        assert second.ok and second.changes == 0
        assert ("GET", "/api/v1/models/list") in instance.requests
        assert not any(
            method == "POST" and path == "/api/v1/models/create"
            for method, path in instance.requests
        )

        # A changed model field on an existing row must still use the update
        # arm, even though the export route remains empty.
        instance.models["base/model-a"]["name"] = "edited by hand"
        instance.requests.clear()
        third = _run(inputs, instance)
        assert third.ok
        assert ("POST", "/api/v1/models/model/update") in instance.requests
        assert not any(
            method == "POST" and path == "/api/v1/models/create"
            for method, path in instance.requests
        )

    def test_malformed_authoritative_listing_refuses_before_model_write(
        self, tmp_path: Path,
    ) -> None:
        listings = [
            [],
            {"data": [{"id": "wrong-route"}]},
            {"items": [{"name": "missing-id"}], "total": 1},
            {"items": [{"id": "missing-total"}]},
            {"items": [{"id": "duplicate"}, {"id": "duplicate"}], "total": 2},
            {"items": [{"id": "truncated"}], "total": 2},
        ]
        for listing in listings:
            instance = _instance(webui_name="Fixture Site")
            instance.model_list_override = listing
            with pytest.raises(ChatSyncError) as excinfo:
                _run(_make_inputs(tmp_path), instance)
            assert excinfo.value.data["reason"] in {
                "models_listing_malformed", "models_listing_truncated",
                "models_listing_duplicate_ids",
            }
            assert not any(
                method == "POST" and path in {
                    "/api/v1/models/create", "/api/v1/models/model/update",
                }
                for method, path in instance.requests
            )

    def test_arbitrary_duplicate_create_401_is_not_recovered_as_update(
        self, tmp_path: Path,
    ) -> None:
        instance = _instance(webui_name="Fixture Site")
        inputs = _make_inputs(tmp_path)
        assert _run(inputs, instance).ok
        instance.empty_export = True
        instance.model_list_override = {"items": [], "total": 0}
        instance.duplicate_create_401 = True
        instance.requests.clear()

        with pytest.raises(ChatSyncError) as excinfo:
            _run(inputs, instance)
        assert excinfo.value.data["path"] == "/api/v1/models/create"
        assert excinfo.value.data["http_status"] == 401
        assert "already registered" in excinfo.value.data["detail"]
        assert ("POST", "/api/v1/models/create") in instance.requests
        assert ("POST", "/api/v1/models/model/update") not in instance.requests


def test_sync_reconciles_preset_via_models_update(tmp_path: Path) -> None:
    """The preset row carries BOTH halves the chat agent needs.

    Named for the endpoint actually used. The PACT's verification names
    ``test_sync_reconciles_preset_via_models_sync``; that endpoint cannot
    update an existing preset at the pinned version and fails silently with
    HTTP 200, so building on it would make this test pass while the site
    stayed unconfigured. ``test_models_sync_endpoint_is_never_called`` is the
    other half of that decision.
    """
    instance = _instance(webui_name="Fixture Site")
    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    row = _preset_row(instance)
    assert row["name"] == "Fixture Preset"
    # F5: params.system AND meta.toolIds. Neither alone is a working preset.
    assert "Use the OKG tools" in row["params"]["system"]
    assert row["meta"]["toolIds"] == ["server:mcp:okg-fixture"]
    assert row["meta"]["description"] == "Ask the fixture graph."
    assert row["meta"]["suggestion_prompts"] == [{"content": "What changed last week?"}]
    assert row["base_model_id"] == "base/model-a"
    # F2: without info.id the registration is `server:mcp:None`.
    assert _connection(instance)["info"]["id"] == "okg-fixture"


def test_models_sync_endpoint_is_never_called(tmp_path: Path) -> None:
    """F7 asserted at the wire: /api/v1/models/sync is not in the call log."""
    instance = _instance(webui_name="Fixture Site")
    _run(_make_inputs(tmp_path), instance)
    assert not any(path == "/api/v1/models/sync" for _m, path in instance.requests)
    assert any(
        path == "/api/v1/models/model/update" or path == "/api/v1/models/create"
        for _m, path in instance.requests
    )


def test_sync_updates_an_existing_preset_row(tmp_path: Path) -> None:
    """The UPDATE arm lands, which is exactly what /models/sync cannot do."""
    instance = _instance(webui_name="Fixture Site")
    inputs = _make_inputs(tmp_path)
    _run(inputs, instance)
    instance.models["okg-fixture"]["name"] = "Renamed By Hand"
    report = _run(inputs, instance)
    assert report.ok
    assert _preset_row(instance)["name"] == "Fixture Preset"


def test_prompts_surface_refuses_with_named_reason(tmp_path: Path) -> None:
    """No recorded readback proof -> the surface is DROPPED, never written."""
    instance = _instance(webui_name="Fixture Site")
    report = _run(_make_inputs(tmp_path), instance)

    prompts = report.surfaces[SURFACE_PROMPTS]
    assert prompts.status == "dropped"
    assert "no write-plus-readback proof" in (prompts.refusal or "")
    assert "prompts-api-readback.json" in (prompts.refusal or "")
    assert instance.prompts == {}
    assert not any(path.startswith("/api/v1/prompts/") for _m, path in instance.requests)


def _data_dir_with_prompts_proof(tmp_path: Path, *, version: str = "0.11.4") -> Path:
    """A chat data directory carrying the REAL census plus a prompts proof.

    The census is copied verbatim rather than synthesized, so activating the
    prompts surface cannot accidentally weaken the census gate this run still
    has to pass.
    """
    import shutil

    root = tmp_path / "chat-data"
    root.mkdir()
    shutil.copyfile(CENSUS, root / UI_KNOB_CENSUS_FILE)
    (root / sync_module.PROMPTS_PROOF_FILE).write_text(
        json.dumps({
            "schema_version": "1.0.0",
            "openwebui_version": version,
            "proved": True,
            "endpoints": [
                "POST /api/v1/prompts/create",
                "POST /api/v1/prompts/id/{id}/update",
                "GET /api/v1/prompts/",
            ],
        }),
        encoding="utf-8",
    )
    return root


def test_sync_renders_prompt_library_entries(tmp_path: Path) -> None:
    """With the proof recorded at the RUNNING version, the surface is driven."""
    inputs = _make_inputs(tmp_path)
    inputs = SyncInputs(
        **{**inputs.__dict__, "chat_data_dir": _data_dir_with_prompts_proof(tmp_path)},
    )
    instance = _instance(webui_name="Fixture Site")
    report = _run(inputs, instance)

    assert report.ok, report.to_dict()
    prompts = report.surfaces[SURFACE_PROMPTS]
    assert prompts.status == "verified"
    stored = list(instance.prompts.values())
    assert len(stored) == 1
    assert stored[0]["command"] == "/okg-fixture"
    assert "Use the OKG tools" in stored[0]["content"]


def test_prompts_proof_at_another_version_drops_the_surface(tmp_path: Path) -> None:
    """A proof recorded at another version proves nothing about this one."""
    inputs = _make_inputs(tmp_path)
    inputs = SyncInputs(**{
        **inputs.__dict__,
        "chat_data_dir": _data_dir_with_prompts_proof(tmp_path, version="0.10.0"),
    })
    report = _run(inputs, _instance(webui_name="Fixture Site"))
    prompts = report.surfaces[SURFACE_PROMPTS]
    assert prompts.status == "dropped"
    assert "recorded at Open WebUI '0.10.0'" in (prompts.refusal or "")


def test_sync_carries_deep_link_bases_into_valves(tmp_path: Path) -> None:
    """chat_site_base reaches its consuming valve; operator_ui_base is NAMED.

    Both declared bases are accounted for. ``chat_site_base`` lands in the
    review-queue Pipe's ``openwebui_api_base`` valve — the second source of
    truth this removes. ``operator_ui_base`` has no installed component that
    reads it at this version, and the run SAYS SO rather than writing it into
    a component that would silently drop it.
    """
    instance = _instance(webui_name="Fixture Site")
    report = _run(_make_inputs(tmp_path), instance)

    assert report.ok
    assert instance.functions["okg_review_queue"]["valves"] == {
        "openwebui_api_base": "http://chat.test",
    }
    links = report.surfaces[SURFACE_DEEP_LINKS]
    assert links.status == "verified"
    assert any("operator_ui_base is declared but was carried NOWHERE" in n for n in links.notes)


def test_deep_link_valve_absent_from_spec_refuses(tmp_path: Path) -> None:
    """An installed consumer with no such valve is a refusal, not a blind write.

    v0.11.0 validates posted valves against the function's own model and drops
    unknown fields silently, so writing anyway would read back as a knob that
    worked.
    """
    instance = _instance(webui_name="Fixture Site")
    instance.install_function("okg_review_queue", valve_fields=("deployment",))
    with pytest.raises(ChatSyncError) as excinfo:
        _run(_make_inputs(tmp_path), instance)
    assert excinfo.value.data["reason"] == "valve_not_declared"
    assert excinfo.value.data["valve"] == "openwebui_api_base"


def test_sync_confirms_mcp_endpoint_answers(tmp_path: Path) -> None:
    """The probe is initialize + tools/list covering the preset's bound tools."""
    instance = _instance(webui_name="Fixture Site")
    report = _run(_make_inputs(tmp_path), instance)

    assert report.mcp["reachable"] is True
    assert report.mcp["checked"] == "initialize + tools/list"
    assert set(report.mcp["tools"]) == set(LIVE_TOOLS)
    assert report.mcp["covers_bound_tools"] is True
    # A probe reports reachable/unreachable, never changed/unchanged.
    assert SURFACE_MCP not in WRITTEN_SURFACES
    assert SURFACE_MCP not in report.surfaces


@pytest.mark.parametrize(
    ("probe", "reason"),
    [
        pytest.param(
            lambda _e, _t: (_ for _ in ()).throw(RuntimeError("HTTP 401 Unauthorized")),
            "mcp_unreachable",
            id="401",
        ),
        pytest.param(lambda _e, _t: (), "mcp_empty_listing", id="empty-listing"),
        pytest.param(
            lambda _e, _t: (_ for _ in ()).throw(ConnectionError("connect only")),
            "mcp_unreachable",
            id="connect-only",
        ),
    ],
)
def test_mcp_non_answers_fail_the_run(tmp_path: Path, probe: Any, reason: str) -> None:
    """A 401, an empty listing and a bare connect are FAILURES, not answers."""
    instance = _instance(webui_name="Fixture Site")
    with pytest.raises(ChatSyncError) as excinfo:
        sync(_make_inputs(tmp_path), _client(instance), probe_tools=probe)
    assert excinfo.value.data["reason"] == reason


def test_mcp_listing_missing_a_bound_tool_fails(tmp_path: Path) -> None:
    """The live listing must be a SUPERSET of what the preset binds."""
    instance = _instance(webui_name="Fixture Site")
    with pytest.raises(ChatSyncError) as excinfo:
        sync(
            _make_inputs(tmp_path),
            _client(instance),
            probe_tools=lambda _e, _t: ("search", "inspect"),
        )
    assert excinfo.value.data["reason"] == "mcp_missing_bound_tools"
    assert "expand" in excinfo.value.data["missing"]


def test_system_prompt_composed_from_mcp_instructions_and_deployment_file(
    tmp_path: Path,
) -> None:
    """Both sources contribute, and the verb contains no prompt text of its own."""
    inputs = _make_inputs(
        tmp_path,
        mcp_instructions="MCP INSTRUCTIONS: the OKG read algebra.",
        system_prompt="DEPLOYMENT PROMPT: quote node ids.",
    )
    instance = _instance(webui_name="Fixture Site")
    report = _run(inputs, instance)

    system = _preset_row(instance)["params"]["system"]
    assert "MCP INSTRUCTIONS: the OKG read algebra." in system
    assert "DEPLOYMENT PROMPT: quote node ids." in system
    assert set(report.prompt.contributing) == {"mcp_instructions", "system_prompt_file"}
    assert report.prompt.absent == ()
    # No inline prompt text in the verb: every line of the composed prompt is
    # traceable to a source file.
    assert system == (
        "MCP INSTRUCTIONS: the OKG read algebra.\n\n"
        "DEPLOYMENT PROMPT: quote node ids."
    )


def test_absent_mcp_instructions_is_an_honest_case(tmp_path: Path) -> None:
    """okg-workspace declares none; that composes and is REPORTED, not hidden."""
    inputs = _make_inputs(tmp_path, mcp_instructions=None)
    instance = _instance(webui_name="Fixture Site")
    report = _run(inputs, instance)

    assert report.prompt.contributing == ("system_prompt_file",)
    assert report.prompt.absent == ("mcp_instructions",)
    assert _preset_row(instance)["params"]["system"]


def test_both_prompt_sources_absent_refuses(tmp_path: Path) -> None:
    """An empty system prompt that looks configured is never emitted."""
    manifest = _raw_manifest()
    manifest["chat"]["preset"].pop("system_prompt_ref")
    inputs = _make_inputs(tmp_path, manifest=manifest, system_prompt=None, mcp_instructions=None)
    with pytest.raises(ChatSyncError) as excinfo:
        _run(inputs, _instance(webui_name="Fixture Site"))
    assert excinfo.value.data["reason"] == "no_prompt_source"


def test_compose_refuses_a_declared_but_empty_source(tmp_path: Path) -> None:
    empty = tmp_path / "empty.md"
    empty.write_text("   \n", encoding="utf-8")
    with pytest.raises(ChatSyncError) as excinfo:
        compose_system_prompt(
            "fixture", mcp_instructions_path=empty, system_prompt_path=None,
        )
    assert excinfo.value.data["reason"] == "mcp_instructions_empty"


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def test_credentials_resolve_only_from_named_operator_env(tmp_path: Path) -> None:
    """POSITIVE assertion: the one credential comes from OKG_CHAT_ADMIN_TOKEN.

    Asserted positively because a grep for credential-looking manifest values
    would pass VACUOUSLY — the schema forbids such a key existing at all. What
    is checked instead is that the manifest key set read while rendering is
    disjoint from the credential set, and that the token in flight is the
    environment's.
    """
    base, token = resolve_operator_environment(
        {ENV_INSTANCE_URL: BASE_URL, ENV_ADMIN_TOKEN: TOKEN},
    )
    assert (base, token) == (BASE_URL, TOKEN)

    from okg.substrate.deployments.chat_block import schema_field_census

    manifest_keys = set().union(*schema_field_census().values())
    assert manifest_keys.isdisjoint({ENV_ADMIN_TOKEN, ENV_INSTANCE_URL, "key", "api_key"})

    seen: list[str] = []

    def _capture(request, _timeout):
        seen.append(request.get_header("Authorization") or "")
        return 200, json.dumps({"version": "0.11.0"})

    InstanceClient(BASE_URL, TOKEN, opener=_capture).version()
    assert seen == [f"Bearer {TOKEN}"]


@pytest.mark.parametrize("missing", [ENV_INSTANCE_URL, ENV_ADMIN_TOKEN])
def test_missing_operator_env_refuses_by_name(missing: str) -> None:
    env = {ENV_INSTANCE_URL: BASE_URL, ENV_ADMIN_TOKEN: TOKEN}
    env[missing] = ""
    with pytest.raises(ChatSyncError) as excinfo:
        resolve_operator_environment(env)
    assert excinfo.value.data["variable"] == missing


def test_http_error_bodies_never_echo_the_token() -> None:
    """A 422 echoing the submitted payload must not carry the token to stderr."""
    def _echo(request, _timeout):
        import urllib.error

        raise urllib.error.HTTPError(
            request.full_url, 422,
            f"invalid payload: Authorization=Bearer {TOKEN}",
            {},  # type: ignore[arg-type]
            None,
        )

    client = InstanceClient(BASE_URL, TOKEN, opener=_echo)
    with pytest.raises(ChatSyncError) as excinfo:
        client.call("POST", "/api/v1/models/create", {"id": "x"})
    assert TOKEN not in json.dumps(excinfo.value.to_json())
    assert TOKEN not in str(excinfo.value)


def test_redact_scrubs_unknown_token_shapes() -> None:
    """Shape, not only identity: a token this process never held is scrubbed too."""
    assert "sk-" not in redact("leaked " + fake_credential("sk-", "abcdefghijklmnopqrstuvwxyz012345"))
    assert redact("short ok") == "short ok"


# ---------------------------------------------------------------------------
class TestCensus:
    """req.chat-sync-projection.knob-census-bound"""

    def test_driven_knob_set_equals_census(self, tmp_path: Path) -> None:
        """The knobs a REAL RUN drives are exactly the census-admitted set.

        Adversarial-review finding: the previous form of this test asserted
        ``driven_knob_set(...) == load_ui_knob_census(...)``, which is the
        same expression on both sides — deleting the banner branch from
        ``_sync_instance_ui`` left it green. The binding now reads the knob
        each written value was tagged with off a real run's diff, so a knob
        the verb stops driving fails here.
        """
        assert driven_knob_set() == frozenset(
            {"site_name", "logo_ref", "banner", "features"}
        )

        manifest = _raw_manifest()
        manifest["chat"]["ui"]["logo_ref"] = "chat/logo.png"
        inputs = _make_inputs(tmp_path, manifest=manifest)
        logo = inputs.deployment_dir / "chat" / "logo.png"
        logo.write_bytes(b"PNG-ish deployment logo bytes")
        instance = _instance(webui_name="Fixture Site")
        # The `file` mechanism is operator-provisioned: the verb asserts the
        # served bytes, it does not upload them.
        instance.static["/static/logo.png"] = b"PNG-ish deployment logo bytes"

        report = _run(inputs, instance)
        assert report.ok, report.to_dict()
        observed_knobs = {
            entry["knob"]
            for entry in report.surfaces[SURFACE_INSTANCE_UI].diff
            if entry["knob"]
        }
        assert observed_knobs == load_ui_knob_census()

    def test_logo_bytes_mismatch_names_the_file_mechanism(self, tmp_path: Path) -> None:
        """A logo that does not match the served bytes fails, naming STATIC_DIR."""
        manifest = _raw_manifest()
        manifest["chat"]["ui"]["logo_ref"] = "chat/logo.png"
        inputs = _make_inputs(tmp_path, manifest=manifest)
        (inputs.deployment_dir / "chat" / "logo.png").write_bytes(b"declared bytes")
        instance = _instance(webui_name="Fixture Site")
        instance.static["/static/logo.png"] = b"something else entirely"

        with pytest.raises(ChatSyncError) as excinfo:
            _run(inputs, instance)
        assert excinfo.value.data["reason"] == "unwritable_mechanism_mismatch"
        assert excinfo.value.data["key"] == "logo_bytes_sha256"
        assert excinfo.value.data["mechanism"] == "file"
        assert "STATIC_DIR" in (excinfo.value.fix_hint or "")

    def test_unserved_logo_is_unreadable_not_absent(self, tmp_path: Path) -> None:
        """A 404 on the asset is could-not-read, never agreement."""
        manifest = _raw_manifest()
        manifest["chat"]["ui"]["logo_ref"] = "chat/logo.png"
        inputs = _make_inputs(tmp_path, manifest=manifest)
        (inputs.deployment_dir / "chat" / "logo.png").write_bytes(b"declared bytes")
        with pytest.raises(ChatSyncError) as excinfo:
            _run(inputs, _instance(webui_name="Fixture Site"))
        assert excinfo.value.data["observed_readable"] is False

    def test_missing_declared_logo_file_refuses(self, tmp_path: Path) -> None:
        manifest = _raw_manifest()
        manifest["chat"]["ui"]["logo_ref"] = "chat/absent.png"
        with pytest.raises(ChatSyncError) as excinfo:
            _run(_make_inputs(tmp_path, manifest=manifest), _instance(webui_name="Fixture Site"))
        assert excinfo.value.data["reason"] == "logo_unreadable"

    def test_uncensused_knob_refuses_run(self, tmp_path: Path) -> None:
        """A manifest asking for a knob outside the census refuses the run.

        The census is read from the ARTIFACT at run time, so a re-recorded
        census that no longer admits a feature refuses even though the
        parser's compiled-in constant still accepts it. That is the point: a
        version bump that shrinks the drivable set must show up as a refusal,
        not as silent no-ops.
        """
        shrunk = tmp_path / "chat-data"
        shrunk.mkdir()
        (shrunk / UI_KNOB_CENSUS_FILE).write_text(
            json.dumps({
                "openwebui_version": "0.11.4",
                "knobs": [
                    {"manifest_field": "chat.ui.banner", "mechanism": "api", "drivable": True},
                    {
                        "manifest_field": "chat.ui.features.web_search",
                        "mechanism": "api",
                        "drivable": True,
                    },
                ],
            }),
            encoding="utf-8",
        )
        inputs = _make_inputs(tmp_path)
        inputs = SyncInputs(**{**inputs.__dict__, "chat_data_dir": shrunk})
        with pytest.raises(ChatUiCensusUnavailable) as excinfo:
            _run(inputs, _instance(webui_name="Fixture Site"))
        assert excinfo.value.data["reason"] == "knob_outside_census"
        assert "site_name" in excinfo.value.data["requested"]

    def test_version_mismatched_census_refuses(self, tmp_path: Path) -> None:
        """A census taken at another version is refused, never reused."""
        instance = _instance(version="0.12.0", webui_name="Fixture Site")
        with pytest.raises(ChatUiCensusUnavailable) as excinfo:
            _run(_make_inputs(tmp_path), instance)
        assert excinfo.value.data["reason"] == "census_version_mismatch"
        assert excinfo.value.data["running_version"] == "0.12.0"

    def test_uncensused_feature_leaf_refuses_run(self, tmp_path: Path) -> None:
        """The census bounds the ROOM, not only the door.

        ``features`` is admitted as a container, so a census that admits it
        while measuring only one leaf drivable must still refuse a manifest
        declaring the others — otherwise the container would hold an
        unaudited vocabulary.
        """
        shrunk = tmp_path / "chat-data"
        shrunk.mkdir()
        (shrunk / UI_KNOB_CENSUS_FILE).write_text(
            json.dumps({
                "openwebui_version": "0.11.4",
                "knobs": [
                    {"manifest_field": f, "mechanism": m, "drivable": True}
                    for f, m in (
                        ("chat.ui.site_name", "env"),
                        ("chat.ui.logo_ref", "file"),
                        ("chat.ui.banner", "api"),
                        ("chat.ui.features.web_search", "api"),
                    )
                ],
            }),
            encoding="utf-8",
        )
        inputs = _make_inputs(tmp_path)
        inputs = SyncInputs(**{**inputs.__dict__, "chat_data_dir": shrunk})
        with pytest.raises(ChatUiCensusUnavailable) as excinfo:
            _run(inputs, _instance(webui_name="Fixture Site"))
        assert excinfo.value.data["reason"] == "knob_outside_census"
        assert "arena" in excinfo.value.data["requested"]
        assert excinfo.value.data["admitted"] == ["web_search"]

    def test_missing_census_refuses(self, tmp_path: Path) -> None:
        inputs = _make_inputs(tmp_path)
        inputs = SyncInputs(**{**inputs.__dict__, "chat_data_dir": tmp_path / "nowhere"})
        with pytest.raises(ChatUiCensusUnavailable) as excinfo:
            _run(inputs, _instance(webui_name="Fixture Site"))
        assert excinfo.value.data["reason"] == "missing_or_unreadable"


# ---------------------------------------------------------------------------
class TestReadback:
    """req.readback-no-partial-apply and req.idempotent"""

    def test_readback_diff_covers_every_written_surface(self, tmp_path: Path) -> None:
        report = _run(_make_inputs(tmp_path), _instance(webui_name="Fixture Site"))
        assert report.ok
        assert set(report.surfaces) == set(WRITTEN_SURFACES)
        diffed = []
        for name in (SURFACE_INSTANCE_UI, SURFACE_MODELS, SURFACE_PRESET, SURFACE_DEEP_LINKS):
            entries = report.surfaces[name].diff
            assert entries, f"{name} produced no expected-vs-observed diff"
            diffed.append(name)
            for entry in entries:
                assert {"key", "expected", "observed", "readable", "verified"} <= set(entry)
                assert entry["verified"] is True
        # The dropped surface says why instead of producing a diff.
        assert report.surfaces[SURFACE_PROMPTS].refusal
        # Surface 6 is a probe, reported separately from the written surfaces.
        assert report.mcp["reachable"] is True

        # The COUNT is printed, not asserted: 4 here because prompts is
        # dropped, and it must equal the number of surfaces that produced a
        # diff. Without this, a run that quietly verified one surface would
        # render identically to one that verified four.
        payload = report.to_dict()
        assert payload["written_surfaces_verified"] == len(diffed) == 4
        assert payload["written_surfaces_total"] == 5
        # A dropped surface is NAMED with its refusal, never omitted, and
        # never counted among the verified.
        assert [d["surface"] for d in payload["written_surfaces_dropped"]] == [
            SURFACE_PROMPTS
        ]
        assert payload["written_surfaces_dropped"][0]["refusal"]
        # The accounting is TOTAL. Verified plus dropped must equal the whole
        # written set: without this, a surface that is neither verified nor
        # dropped disappears from the output and the run still exits 0 — which
        # is what a live run against an instance with no deep-link consumer
        # actually did before this assertion existed.
        assert (
            payload["written_surfaces_verified"]
            + len(payload["written_surfaces_dropped"])
            == payload["written_surfaces_total"]
        )
        assert payload["written_surfaces_accounted"] == 5
        # All six surfaces are named in the output.
        assert set(payload["written_surfaces"]) | set(payload["probe_surfaces"]) == {
            SURFACE_INSTANCE_UI,
            SURFACE_MODELS,
            SURFACE_PRESET,
            SURFACE_PROMPTS,
            SURFACE_DEEP_LINKS,
            SURFACE_MCP,
        }

    def test_unconsumed_written_surface_is_accounted_with_a_named_refusal(
        self, tmp_path: Path,
    ) -> None:
        """A written surface with no consumer is DROPPED, not invisible.

        The live shape this test pins: the real okg-workspace instance has no
        ``okg_review_queue`` function installed, so ``deep_links`` reports
        ``no_consumer``. Before this test, that status was in neither the
        verified count nor the dropped list, and the run printed "written
        surfaces verified: 3 of 5 (dropped: prompts)" while exiting 0 — the
        fifth surface simply was not in the output. Every other fixture in
        this file installs the consuming function, so none of them could see
        it. req.readback-no-partial-apply requires the accounting to be TOTAL.
        """
        instance = FakeInstance(webui_name="Fixture Site")
        instance.add_group("okg-users", "group-users")
        # Deliberately NOT installing okg_review_queue: this is the live shape.
        report = _run(_make_inputs(tmp_path), instance)
        assert report.ok

        deep_links = report.surfaces[SURFACE_DEEP_LINKS]
        assert deep_links.status == "no_consumer"
        assert deep_links.refusal, "an unwritten surface must name its refusal"
        assert "NO CONSUMER" in deep_links.refusal
        assert "okg_review_queue" in deep_links.refusal

        payload = report.to_dict()
        # Two surfaces unwritten now: prompts (dropped) and deep_links
        # (no_consumer). Both are named, both carry a refusal, and the
        # arithmetic closes.
        assert sorted(d["surface"] for d in payload["written_surfaces_dropped"]) == [
            SURFACE_DEEP_LINKS,
            SURFACE_PROMPTS,
        ]
        for entry in payload["written_surfaces_dropped"]:
            assert entry["refusal"], f"{entry['surface']} was unwritten with no refusal"
            assert entry["status"] in {"dropped", "no_consumer", "not_declared"}
        assert payload["written_surfaces_verified"] == 3
        assert payload["written_surfaces_accounted"] == 5
        assert (
            payload["written_surfaces_verified"]
            + len(payload["written_surfaces_dropped"])
            == payload["written_surfaces_total"]
        )

    def test_run_prints_the_driven_knob_count_and_census_exclusions(
        self, tmp_path: Path,
    ) -> None:
        """A shrinking driven set is VISIBLE, and each exclusion says why.

        req.readback-no-partial-apply requires every census exclusion printed
        with its knob name and census reason, alongside the count of admitted
        knobs actually driven. The fixture declares three of the four admitted
        knobs (no logo_ref), so the count differs from the admitted count —
        which is exactly the case a `driven == admitted` assertion could not
        see.
        """
        report = _run(_make_inputs(tmp_path), _instance(webui_name="Fixture Site"))
        payload = report.to_dict()
        assert payload["driven_knobs"] == ["banner", "features", "site_name"]
        assert payload["driven_knob_count"] == 3
        assert payload["census_admitted_knob_count"] == 4
        assert "logo_ref" in payload["census_admitted_knobs"]

        excluded = {e["knob"]: e for e in payload["census_excluded_knobs"]}
        # Read off the artifact, so a re-recorded census moves this by itself.
        assert "chat.ui.theme.accent" in excluded
        assert "chat.ui.theme.base" in excluded
        for entry in excluded.values():
            assert entry["reason"], f"{entry['knob']} was excluded with no reason"
        # #3965: the preset exclusion states what sync really does. Sync
        # updates the preset through /models/model/update; the census text
        # must not claim an existing preset cannot be updated.
        preset_reason = excluded["chat.preset"]["reason"]
        assert "cannot update an existing preset" not in preset_reason.lower()
        assert "BROKEN" not in preset_reason
        assert "/api/v1/models/model/update" in preset_reason
        # And nothing excluded was driven.
        assert not (set(excluded) & set(payload["driven_knobs"]))

    def test_first_run_diff_is_non_empty(self, tmp_path: Path) -> None:
        report = _run(_make_inputs(tmp_path), _instance(webui_name="Fixture Site"))
        assert report.changes > 0

    def test_partial_apply_exits_non_zero(self, tmp_path: Path) -> None:
        """A rejected preset reconciliation fails the WHOLE run.

        And nothing reports success: the surfaces written before it keep their
        diff, because the operator needs to know what is already applied, but
        lose their `ok`.
        """
        instance = _instance(webui_name="Fixture Site")
        instance.fail_paths.add("/api/v1/models/create")
        with pytest.raises(ChatSyncError) as excinfo:
            _run(_make_inputs(tmp_path), instance)
        err = excinfo.value
        assert err.data["path"] == "/api/v1/models/create"
        assert err.data["http_status"] == 422
        # The failure NAMES the surface, what it was reconciling toward, and
        # that there is no observed value because the write was rejected.
        assert err.data["surface"] == SURFACE_MODELS
        assert err.data["observed"] == "(the write was rejected)"
        # And it names WHAT it was reconciling toward. Reading this off the
        # diff would report `expected: null`, because the diff is only
        # assigned after the second readback that never happened.
        assert err.data["expected"]["ui.default_models"] == "okg-fixture"
        assert err.data["expected"]["model:base/model-a:present"] is True
        # And the row it was reconciling toward never landed.
        assert "base/model-a" not in instance.models

    def test_partial_apply_demotes_earlier_surfaces(self, tmp_path: Path) -> None:
        """No surface reports ok once another surface could not be applied."""
        instance = _instance(webui_name="Fixture Site")
        instance.fail_paths.add("/api/v1/configs/tool_servers")
        inputs = _make_inputs(tmp_path)
        report = _sync_to_report(inputs, instance)
        assert report.ok is False
        assert all(not o.ok for o in report.surfaces.values())
        assert report.surfaces[SURFACE_INSTANCE_UI].status == "applied_but_run_failed"
        assert report.surfaces[SURFACE_INSTANCE_UI].diff, (
            "the operator still needs to see what was already written"
        )

    def test_unreadable_value_is_not_a_match(self, tmp_path: Path) -> None:
        """A response that OMITS a field fails rather than counting as equal."""
        instance = _instance(webui_name="Fixture Site")
        instance.omit_export_keys.add("evaluation.arena.enable")
        with pytest.raises(ChatSyncError) as excinfo:
            _run(_make_inputs(tmp_path), instance)
        assert excinfo.value.data["reason"] == "readback_unreadable"
        assert excinfo.value.data["key"] == "config:evaluation.arena.enable"
        assert excinfo.value.data["observed_readable"] is False

    def test_readback_compares_effective_desired_state(self, tmp_path: Path) -> None:
        """A policy-gated value passes against its GATED value, with the cause.

        Compaction is declared ``true`` in the fixture manifest and the
        verdict is absent, so ``false`` IS the effective desired value.
        Comparing the raw manifest here would fail the correct day-one run by
        construction.
        """
        instance = _instance(webui_name="Fixture Site")
        report = _run(_make_inputs(tmp_path), instance)
        assert report.ok
        assert instance.config["chat.context_compaction.enable"] is False
        causes = [d for d in report.deviations if "context_compaction" in d["key"]]
        assert causes and causes[0]["manifest_value"] is True
        assert causes[0]["effective_value"] is False
        assert "verdict" in causes[0]["cause"]

    def test_readback_normalizes_the_site_name_transform(self, tmp_path: Path) -> None:
        """F8: the expected value carries the vendor suffix, or it drifts forever."""
        assert expected_site_name("Fixture Site") == "Fixture Site (Open WebUI)"
        assert expected_site_name("Open WebUI") == "Open WebUI"
        instance = _instance(webui_name="Fixture Site")
        report = _run(_make_inputs(tmp_path), instance)
        entry = next(
            e for e in report.surfaces[SURFACE_INSTANCE_UI].diff if e["key"] == "site_name"
        )
        assert entry["expected"] == "Fixture Site (Open WebUI)"
        assert entry["verified"] is True
        assert entry["changed"] is False

    def test_site_name_drift_is_non_blocking(
        self, tmp_path: Path,
    ) -> None:
        """site_name drift is reported, but does not stop later surfaces."""
        instance = _instance(webui_name="Something Else")
        inputs = _make_inputs(tmp_path)
        inputs = SyncInputs(
            **{**inputs.__dict__, "chat_data_dir": _data_dir_with_prompts_proof(tmp_path)},
        )
        report = _run(inputs, instance)
        assert report.ok, report.to_dict()
        entry = next(
            e for e in report.surfaces[SURFACE_INSTANCE_UI].diff
            if e["key"] == "site_name"
        )
        assert entry["expected"] == "Fixture Site (Open WebUI)"
        assert entry["observed"] == "Something Else (Open WebUI)"
        assert entry["verified"] is False
        assert entry["non_blocking"] is True
        assert entry["mechanism"] == "env"
        assert "WEBUI_NAME" in entry["fix_hint"]
        assert "restart" in entry["fix_hint"]
        assert any(
            "non-blocking deviation" in note and "WEBUI_NAME" in note
            for note in report.surfaces[SURFACE_INSTANCE_UI].notes
        )
        assert report.surfaces[SURFACE_MODELS].ok
        assert report.surfaces[SURFACE_PRESET].ok
        assert report.surfaces[SURFACE_PROMPTS].status == "verified"
        assert report.surfaces[SURFACE_DEEP_LINKS].ok

    def test_second_run_reports_zero_changes(self, tmp_path: Path) -> None:
        instance = _instance(webui_name="Fixture Site")
        inputs = _make_inputs(tmp_path)
        first = _run(inputs, instance)
        second = _run(inputs, instance)
        assert first.changes > 0
        assert second.ok and second.changes == 0
        for outcome in second.surfaces.values():
            assert outcome.changes == 0

    def test_second_run_is_zero_change_with_local_state_removed(
        self, tmp_path: Path,
    ) -> None:
        """The count comes from readback, so there is no cache to invalidate.

        Proven two ways: the whole state directory is deleted between runs,
        and the run is asserted to have written no file under it. A count
        computed from a cache would either survive the deletion or be
        recomputed as non-zero.
        """
        instance = _instance(webui_name="Fixture Site")
        state_home = tmp_path / "state"
        inputs = _make_inputs(tmp_path, state_home=state_home)
        _run(inputs, instance)

        import shutil

        if state_home.exists():
            shutil.rmtree(state_home)
        (tmp_path / "corrupt.json").write_text("{ not json", encoding="utf-8")
        second = _run(inputs, instance)
        assert second.ok and second.changes == 0
        assert not state_home.exists(), (
            "the verb wrote local state; the zero-change report would then be "
            "a property of that file rather than of the instance"
        )

    def test_hand_edited_value_is_detected_and_corrected(self, tmp_path: Path) -> None:
        """One value perturbed on EVERY readback-able surface is corrected."""
        instance = _instance(webui_name="Fixture Site")
        inputs = _make_inputs(tmp_path)
        _run(inputs, instance)

        # instance_ui
        instance.config["evaluation.arena.enable"] = True
        # models
        instance.config["ui.default_models"] = "somebody/else"
        # preset (both halves)
        instance.models["okg-fixture"]["params"] = {"system": "edited by hand"}
        instance.tool_servers = []
        # deep_links
        instance.functions["okg_review_queue"]["valves"]["openwebui_api_base"] = "http://wrong"

        third = _run(inputs, instance)
        assert third.ok
        assert third.changes >= 4
        for name in (SURFACE_INSTANCE_UI, SURFACE_MODELS, SURFACE_PRESET, SURFACE_DEEP_LINKS):
            assert third.surfaces[name].changes > 0, f"{name} drift was not detected"
        # The run PRINTS how many surfaces it perturbed and detected, and that
        # number equals the written-surface count it reports — so a drift
        # proof that covered one easy API knob while the rest were cached
        # would be visible as a smaller number rather than reading identically.
        payload = third.to_dict()
        assert payload["surfaces_with_detected_changes"] == 4
        assert (
            payload["surfaces_with_detected_changes"]
            == payload["written_surfaces_verified"]
        ), "every written surface this run verified must also be one it detected drift on"
        # prompts is the one written surface not perturbed, and it is reported
        # DROPPED with its named refusal rather than silently omitted.
        assert [d["surface"] for d in payload["written_surfaces_dropped"]] == [
            SURFACE_PROMPTS
        ]
        assert payload["written_surfaces_dropped"][0]["refusal"]
        assert instance.config["evaluation.arena.enable"] is False
        assert instance.config["ui.default_models"] == "okg-fixture"
        assert "Use the OKG tools" in _preset_row(instance)["params"]["system"]
        assert _connection(instance)["info"]["id"] == "okg-fixture"
        assert (
            instance.functions["okg_review_queue"]["valves"]["openwebui_api_base"]
            == "http://chat.test"
        )
        fourth = _run(inputs, instance)
        assert fourth.changes == 0

    def test_removing_access_groups_actually_revokes(self, tmp_path: Path) -> None:
        """Grants are compared and asserted in BOTH directions.

        Adversarial-review finding: a grant comparison that vanishes when the
        declared set is empty means removing ``chat.access.groups`` never
        revokes, while the code prints an ACL claim it no longer enforces.
        """
        instance = _instance(webui_name="Fixture Site")
        _run(_make_inputs(tmp_path), instance)
        assert instance.grants["okg-fixture"]

        stripped = _raw_manifest()
        stripped["chat"].pop("access")
        report = _run(_make_inputs(tmp_path, manifest=stripped), instance)
        assert report.ok
        assert instance.grants["okg-fixture"] == []
        assert instance.grants["base/model-a"] == []

    def test_unknown_access_group_refuses(self, tmp_path: Path) -> None:
        instance = FakeInstance(webui_name="Fixture Site")
        instance.install_function(
            "okg_review_queue", valve_fields=("openwebui_api_base",),
        )
        with pytest.raises(ChatSyncError) as excinfo:
            _run(_make_inputs(tmp_path), instance)
        assert excinfo.value.data["reason"] == "unknown_access_group"
        assert excinfo.value.data["missing"] == ["okg-users"]


def _sync_to_report(inputs: SyncInputs, instance: FakeInstance) -> Any:
    """Run a sync that is expected to fail, and return the report anyway."""
    from okg.substrate.chat import sync as module

    captured: dict[str, Any] = {}
    original = module._demote_on_failure

    def _spy(report):
        captured["report"] = report
        original(report)

    module._demote_on_failure = _spy
    try:
        with pytest.raises(ChatSyncError):
            _run(inputs, instance)
    finally:
        module._demote_on_failure = original
    return captured["report"]


# ---------------------------------------------------------------------------
class TestToggle:
    """req.chat-sync-projection.toggle-verification"""

    def test_governed_toggles_are_read_back(self, tmp_path: Path) -> None:
        """Every governed toggle is asserted from the RESPONSE, not the request."""
        instance = _instance(webui_name="Fixture Site")
        report = _run(_make_inputs(tmp_path), instance)
        assert report.ok

        verified = {
            entry["key"]: entry
            for entry in report.surfaces[SURFACE_INSTANCE_UI].diff
        }
        for key in (
            "config:image_generation.enable",
            "config:code_interpreter.enable",
            "config:code_execution.enable",
            "permission:features.memories",
            "permission:features.image_generation",
            "permission:features.code_interpreter",
            "permission:features.web_search",
            "permission:features.channels",
            # Control (b) of the knowledge contract. On the SAME measured
            # permissions surface as the features.* leaves, and fully
            # qualified with its own `workspace.` section: a hard-coded
            # `features.` prefix would have written `features.knowledge`, a
            # key the application never reads and that round-trips perfectly.
            "permission:workspace.knowledge",
            "config:memories.system_context.enable",
            "config:chat.context_compaction.enable",
            "config:web.search.enable",
            "config:evaluation.arena.enable",
            "config:channels.enable",
            "connections:ENABLE_DIRECT_CONNECTIONS",
        ):
            assert key in verified, f"{key} was never read back"
            assert verified[key]["readable"] is True
            assert verified[key]["verified"] is True
        # Compaction equals the VERDICT-GATE result, never asserted as a
        # constant on.
        assert verified["config:chat.context_compaction.enable"]["expected"] is False
        for key in (
            "config:image_generation.enable",
            "config:code_interpreter.enable",
            "permission:features.memories",
            "permission:workspace.knowledge",
        ):
            assert verified[key]["expected"] is False

    def test_memory_is_driven_on_the_master_gate_not_only_the_permission(
        self, tmp_path: Path,
    ) -> None:
        """`memories.enable` is the gate that governs ADMINS.

        Adversarial-review finding, confirmed first-hand in the running
        container: the recorded census rejected `memory.enable` (singular) as
        "not a knob" and concluded the per-user permission is the whole memory
        surface. The vendor's key is `memories.enable` (plural,
        `config.py:2809`), and `routers/memories.py:41` skips the permission
        check entirely for admins. Driving only the permission would report
        "memory verified off" while every admin kept memory.
        """
        instance = _instance(webui_name="Fixture Site")
        instance.config["memories.enable"] = True
        report = _run(_make_inputs(tmp_path), instance)
        assert report.ok

        assert instance.config["memories.enable"] is False
        assert instance.config["memories.system_context.enable"] is False
        assert instance.permissions["features"]["memories"] is False
        verified = {e["key"]: e for e in report.surfaces[SURFACE_INSTANCE_UI].diff}
        assert verified["config:memories.enable"]["verified"] is True
        assert verified["config:memories.enable"]["readable"] is True

    def test_omitted_master_memory_gate_refuses(self, tmp_path: Path) -> None:
        """The new switch obeys the same unreadable-is-not-a-match rule."""
        instance = _instance(webui_name="Fixture Site")
        instance.config["memories.enable"] = True
        instance.omit_export_keys.add("memories.enable")
        with pytest.raises(ChatSyncError) as excinfo:
            _run(_make_inputs(tmp_path), instance)
        assert excinfo.value.data["key"] == "config:memories.enable"
        assert excinfo.value.data["observed_readable"] is False

    def test_knowledge_bases_clause_is_reported_unsatisfiable(
        self, tmp_path: Path,
    ) -> None:
        """Control (a) is NAMED unsatisfiable; control (b) is driven anyway.

        The knowledge contract is THREE controls, and only one of them is
        blocked by the census. Control (a),
        ``rag.bypass_embedding_and_retrieval``, has no drive-plus-readback
        measurement at v0.11.0 and req.knob-census-bound forbids driving it
        best-effort, so the run names it — the KEY, not just "knowledge bases"
        — cites the census artifact, and counts it neither verified nor
        dropped. Control (b), the ``workspace.knowledge`` permission, is on
        the measured permissions surface and IS driven and read back.

        An unsatisfiable clause withdraws ONE control, not the whole governed
        toggle: the earlier form of this test asserted that no diff entry
        mentioned knowledge at all, which would have kept passing on a verb
        that silently stopped driving control (b) as well.
        """
        instance = _instance(webui_name="Fixture Site")
        # POSITIVE CONTROL for (b): start from the WRONG value, so a verb that
        # never wrote the permission cannot pass on the vendor default.
        instance.permissions["workspace"]["knowledge"] = True
        report = _run(_make_inputs(tmp_path), instance)
        assert report.ok

        # -- control (a): named, with the key, and not counted verified ------
        clauses = [c for c in report.unsatisfiable if "knowledge_bases" in c["clause"]]
        assert len(clauses) == 1
        assert clauses[0]["key"] == "rag.bypass_embedding_and_retrieval"
        assert "rag.bypass_embedding_and_retrieval" in clauses[0]["clause"]
        assert "no drive-plus-readback measurement" in clauses[0]["reason"]
        assert clauses[0]["artifact"] == "okg/substrate/chat/data/ui-knob-census.json"
        # The positive control the requirement demands is unrun, and the run
        # says so rather than letting a zero-row result read as evidence.
        assert "POSITIVE CONTROL" in clauses[0]["reason"]
        # The residual limits are stated, not a total-guarantee claim.
        assert "routers/retrieval.py:3011" in clauses[0]["residual_limits"]
        assert report.status == "verified_with_unsatisfiable_clauses"
        # And it is genuinely NOT driven: no rag.* key was written.
        assert not any(key.startswith("rag.") for key in instance.config)
        assert not any(
            entry["key"].startswith("config:rag.")
            for entry in report.surfaces[SURFACE_INSTANCE_UI].diff
        )

        # -- control (b): driven on the master permission and read back ------
        assert instance.permissions["workspace"]["knowledge"] is False
        entry = next(
            e
            for e in report.surfaces[SURFACE_INSTANCE_UI].diff
            if e["key"] == "permission:workspace.knowledge"
        )
        assert entry["expected"] is False
        assert entry["readable"] is True
        assert entry["verified"] is True
        # It was actually CHANGED, so the assertion is not passing on a
        # default that happened to already be false.
        assert entry["changed"] is True

    def test_workspace_knowledge_permission_omission_refuses(
        self, tmp_path: Path,
    ) -> None:
        """Control (b) obeys the unreadable-is-not-a-match rule too."""
        instance = _instance(webui_name="Fixture Site")
        instance.omit_permission_keys.add("workspace.knowledge")
        with pytest.raises(ChatSyncError) as excinfo:
            _run(_make_inputs(tmp_path), instance)
        assert excinfo.value.data["key"] == "permission:workspace.knowledge"
        assert excinfo.value.data["observed_readable"] is False

    def test_preset_readback_binds_no_knowledge_base(self, tmp_path: Path) -> None:
        """Control (c): the preset's knowledge binding, read back INSTANCE-SIDE.

        Asserted from the ``knowledge`` field of the preset row returned by
        ``GET /api/v1/models/list`` — never from the request body this verb
        composed. The verb never emits a knowledge binding, so a check on its
        own outbound payload is true by construction: it would report "no
        knowledge base bound" with identical confidence on an instance where
        an admin bound one by hand, which is the exact case the control
        exists to catch.

        Both arms are here, so the check can fail. The clean arm proves the
        readback verifies empty; the hand-bound arm proves the SAME assertion
        detects a binding this verb did not write and corrects it.
        """
        instance = _instance(webui_name="Fixture Site")
        inputs = _make_inputs(tmp_path)

        # -- clean arm: the binding reads back empty, from the instance ------
        first = _run(inputs, instance)
        assert first.ok
        entry = next(
            e
            for e in first.surfaces[SURFACE_PRESET].diff
            if e["key"] == "preset:meta.knowledge"
        )
        assert entry["expected"] == []
        assert entry["readable"] is True
        assert entry["verified"] is True
        # The observation came off the authoritative workspace listing, not
        # off the request.
        assert _preset_row(instance)["meta"]["knowledge"] == []
        assert ("GET", "/api/v1/models/list") in instance.requests

        # -- POSITIVE CONTROL: an admin binds one by hand -------------------
        instance.models["okg-fixture"]["meta"]["knowledge"] = [
            {"id": "kb-hand-added", "name": "Hand-added knowledge base"},
        ]
        second = _run(inputs, instance)
        assert second.ok
        detected = next(
            e
            for e in second.surfaces[SURFACE_PRESET].diff
            if e["key"] == "preset:meta.knowledge"
        )
        # Detected as drift, corrected, and re-read as empty.
        assert detected["changed"] is True
        assert detected["observed"] == []
        assert _preset_row(instance)["meta"]["knowledge"] == []
        assert second.surfaces[SURFACE_PRESET].changes > 0

    def test_omitted_toggle_field_refuses_sync(self, tmp_path: Path) -> None:
        """A configuration readback that omits a governed toggle exits non-zero."""
        instance = _instance(webui_name="Fixture Site")
        instance.omit_permission_keys.add("features.memories")
        with pytest.raises(ChatSyncError) as excinfo:
            _run(_make_inputs(tmp_path), instance)
        assert excinfo.value.data["reason"] == "readback_unreadable"
        assert excinfo.value.data["key"] == "permission:features.memories"
        assert excinfo.value.data["observed_readable"] is False

    def test_uninterpretable_toggle_value_refuses_sync(self, tmp_path: Path) -> None:
        """A value that is not the boolean requested is a mismatch, not agreement."""
        instance = _instance(webui_name="Fixture Site")
        instance.corrupt_export["image_generation.enable"] = "off-ish"
        with pytest.raises(ChatSyncError) as excinfo:
            _run(_make_inputs(tmp_path), instance)
        assert excinfo.value.data["key"] == "config:image_generation.enable"
        assert excinfo.value.data["observed"] == "off-ish"

    def test_unverified_observability_verdict_disables_compaction(
        self, tmp_path: Path,
    ) -> None:
        """A verdict proving only the OTHER configuration is unverified here."""
        from okg.substrate.chat.observability_verdict import (
            Configuration,
            VerdictKey,
            resolve_verdict_path,
            write_verdict,
        )

        state_home = tmp_path / "state"
        path = resolve_verdict_path("fixture", state_home=state_home)
        write_verdict(
            path,
            VerdictKey("fixture", "okg-fixture", "0.11.4"),
            Configuration.COMPACTION_OFF,
        )
        instance = _instance(webui_name="Fixture Site")
        report = _run(_make_inputs(tmp_path, state_home=state_home), instance)
        assert instance.config["chat.context_compaction.enable"] is False
        assert any("verdict" in d["cause"] for d in report.deviations)

    def test_verified_verdict_enables_compaction(self, tmp_path: Path) -> None:
        """The gate is a gate, not a constant off: a proved key turns it on."""
        from okg.substrate.chat.observability_verdict import (
            Configuration,
            VerdictKey,
            resolve_verdict_path,
            write_verdict,
        )

        state_home = tmp_path / "state"
        write_verdict(
            resolve_verdict_path("fixture", state_home=state_home),
            VerdictKey("fixture", "okg-fixture", "0.11.4"),
            Configuration.COMPACTION_ON,
        )
        instance = _instance(webui_name="Fixture Site")
        report = _run(_make_inputs(tmp_path, state_home=state_home), instance)
        assert report.ok
        assert instance.config["chat.context_compaction.enable"] is True
        assert not [d for d in report.deviations if "compaction" in d["key"]]

    def test_absent_verdict_treated_as_unverified(self, tmp_path: Path) -> None:
        """No record for the key -> compaction off, and the run says which verdict."""
        instance = _instance(webui_name="Fixture Site")
        report = _run(_make_inputs(tmp_path, state_home=tmp_path / "empty"), instance)
        assert instance.config["chat.context_compaction.enable"] is False
        cause = next(d["cause"] for d in report.deviations if "compaction" in d["key"])
        assert "absent verdict is treated as unverified" in cause

    def test_verdict_for_another_version_does_not_carry(self, tmp_path: Path) -> None:
        """A new version is a new key, so an old proof stops applying by itself."""
        from okg.substrate.chat.observability_verdict import (
            Configuration,
            VerdictKey,
            resolve_verdict_path,
            write_verdict,
        )

        state_home = tmp_path / "state"
        write_verdict(
            resolve_verdict_path("fixture", state_home=state_home),
            VerdictKey("fixture", "okg-fixture", "0.10.0"),
            Configuration.COMPACTION_ON,
        )
        instance = _instance(webui_name="Fixture Site")
        _run(_make_inputs(tmp_path, state_home=state_home), instance)
        assert instance.config["chat.context_compaction.enable"] is False


# ---------------------------------------------------------------------------
class Test_query_exclusion:  # noqa: N801 - the underscore makes `-k query_exclusion` select this class
    """req.query-exclusion-on-masked-deployments

    Named with an underscore on purpose: the PACT's verification runs
    ``pytest -q -k query_exclusion``, and ``-k`` matches a substring of the
    node id. ``TestQueryExclusion`` would select NOTHING and the task would
    pass vacuously on an empty run.
    """

    def test_masking_posture_preset_omits_query(self, tmp_path: Path) -> None:
        """Proven AT THE WIRE: the registered filter list blocks query."""
        instance = _instance(webui_name="Fixture Site")
        report = _run(_make_inputs(tmp_path), instance)
        assert report.ok

        filter_list = _connection(instance)["config"]["function_name_filter_list"]
        assert "!query" in filter_list.split(",")
        assert "query" not in effective_tool_binding(LIVE_TOOLS, filter_list.split(","))
        entry = next(
            e for e in report.surfaces[SURFACE_PRESET].diff if e["key"] == "bound_tools"
        )
        assert "query" not in entry["observed"]
        assert entry["verified"] is True

    def test_indeterminate_posture_is_treated_as_masked(self, tmp_path: Path) -> None:
        """A catalog that cannot be composed resolves MASKED, never unmasked."""
        def _broken() -> Any:
            raise RuntimeError("the catalog could not be composed")

        manifest = _raw_manifest(nomos={"runtime_enforcement": "off"})
        inputs = _make_inputs(tmp_path, manifest=manifest, catalog_loader=_broken)
        instance = _instance(webui_name="Fixture Site")
        report = _run(inputs, instance)

        assert report.posture.masked is True
        assert report.posture.indeterminate is True
        filter_list = _connection(instance)["config"]["function_name_filter_list"]
        assert "!query" in filter_list.split(",")

    def test_unreadable_nomos_block_is_indeterminate(self) -> None:
        posture = resolve_posture("fixture", {"nomos": "not-a-mapping"})
        assert posture.masked and posture.indeterminate

    def test_exclusion_reason_present_in_run_output(self, tmp_path: Path) -> None:
        report = _run(_make_inputs(tmp_path), _instance(webui_name="Fixture Site"))
        assert len(report.exclusions) == 1
        reason = report.exclusions[0]["reason"]
        assert report.exclusions[0]["tool"] == "query"
        assert "MASKED" in reason
        assert "runtime_enforcement" in reason
        # This PACT must NOT claim masking coverage; it says whose proof it is.
        assert "mcp-http-auth" in reason
        assert "does not claim masking coverage" in reason
        assert json.dumps(report.to_dict())

    def test_unmasked_posture_keeps_query(self, tmp_path: Path) -> None:
        """The STATED negation: enforcement off AND no subtype declares pii_classes."""
        manifest = _raw_manifest(nomos={"runtime_enforcement": "off"})
        inputs = _make_inputs(
            tmp_path, manifest=manifest, catalog_loader=_unmasked_catalog,
        )
        instance = _instance(webui_name="Fixture Site")
        report = _run(inputs, instance)

        assert report.posture.masked is False
        assert report.exclusions == []
        filter_list = _connection(instance)["config"]["function_name_filter_list"]
        assert "!query" not in filter_list.split(",")
        assert "query" in effective_tool_binding(LIVE_TOOLS, filter_list.split(","))

    def test_pii_classes_alone_resolves_masked(self) -> None:
        class _Catalog:
            subtype_metadata = {"person": {"pii_classes": {"email": "direct"}}}

        posture = resolve_posture(
            "fixture",
            {"nomos": {"runtime_enforcement": "off"}},
            catalog_loader=lambda: _Catalog(),
        )
        assert posture.masked and not posture.indeterminate
        assert "pii_classes" in posture.reason

    def test_okg_workspace_resolves_masked(self) -> None:
        """The real manifest pins the live proof's expectation."""
        from okg.substrate import fastyaml

        manifest_path = REPO_ROOT / "deployments/okg-workspace/deployment.yaml"
        raw = fastyaml.safe_load(manifest_path.read_text())
        posture = resolve_posture(
            "okg-workspace", raw, deployment_dir=manifest_path.parent,
        )
        assert posture.masked is True
        assert posture.indeterminate is False
        assert "runtime_enforcement" in posture.reason

    def test_okg_workspace_inputs_load_and_exclude_query(self) -> None:
        """load_inputs resolves the real deployment end to end.

        The exclusion moved (PACT effective-surface-declaration, issue
        #2012). `query` used to be declared in `chat.mcp.tools` and removed
        at sync time under the masked posture, so the manifest stated a set
        that was true nowhere. Now `agent_surface.served` keeps `query` for
        stdio callers and the chat projection simply does not name it, so
        there is nothing left to exclude and the manifest says what the
        client is given.
        """
        from okg.substrate.mcp.effective_surface import parse_agent_surface

        inputs = load_inputs("okg-workspace")
        assert inputs.chat.enabled
        assert inputs.preset_id == "okg-okg-workspace"
        assert inputs.mcp_probe_url == "http://127.0.0.1:8100/mcp"

        surface = parse_agent_surface(inputs.raw_manifest)
        assert surface is not None
        assert "query" in surface.served, (
            "stdio callers keep the bounded SQL escape hatch"
        )
        assert "query" not in surface.projection("chat").tools, (
            "Nomos masking does not cover query output, so chat never gets it"
        )
        # The legacy spelling is gone from this manifest, not merely unused:
        # declaring it beside the block is refused at load.
        assert inputs.chat.mcp.tools == ()

        # F9: okg-workspace declares no mcp_instructions — the honest case.
        assert inputs.mcp_instructions_path is None
        prompt = compose_system_prompt(
            "okg-workspace",
            mcp_instructions_path=inputs.mcp_instructions_path,
            system_prompt_path=resolve_chat_prompt_path(inputs),
        )
        assert prompt.absent == ("mcp_instructions",)
        assert prompt.contributing == ("system_prompt_file",)


def test_posture_defaults_to_masked_when_the_catalog_is_absent(tmp_path: Path) -> None:
    """No deployment dir -> compose fails -> indeterminate -> masked."""
    posture = resolve_posture(
        "fixture",
        {"nomos": {"runtime_enforcement": "off"}},
        deployment_dir=tmp_path / "nowhere",
    )
    assert posture.masked is True
    assert posture.indeterminate is True


# ---------------------------------------------------------------------------
# The CLI surface: exit codes and what an operator actually sees
# ---------------------------------------------------------------------------

def _cli(tmp_path: Path, instance: FakeInstance, monkeypatch, **kwargs: Any) -> int:
    """Drive `_cmd_sync` with the fake instance behind it."""
    import argparse

    from okg.substrate.cli import chat as cli_chat

    inputs = _make_inputs(tmp_path, **kwargs)
    monkeypatch.setenv(ENV_INSTANCE_URL, BASE_URL)
    monkeypatch.setenv(ENV_ADMIN_TOKEN, TOKEN)
    monkeypatch.setattr(sync_module, "load_inputs", lambda *a, **k: inputs)
    monkeypatch.setattr(
        sync_module, "InstanceClient",
        lambda base, token, **kw: InstanceClient(base, token, opener=instance.opener),
    )
    monkeypatch.setattr(
        sync_module, "sync",
        lambda i, c, **kw: sync(i, c, probe_tools=_probe),
    )
    args = argparse.Namespace(
        deployment="fixture",
        mcp_probe_url=None,
        mcp_instance_url=None,
        mcp_host="127.0.0.1",
    )
    return cli_chat._cmd_sync(args, as_json=kwargs.pop("as_json", False))


def test_cli_exits_zero_on_a_verified_run(tmp_path: Path, monkeypatch, capsys) -> None:
    code = _cli(tmp_path, _instance(webui_name="Fixture Site"), monkeypatch)
    out = capsys.readouterr().out
    assert code == 0
    # The qualification is on the TOP LINE, not buried in a JSON field.
    assert "VERIFIED_WITH_UNSATISFIABLE_CLAUSES" in out
    assert "UNSATISFIABLE" in out
    assert "excluded 'query'" in out
    assert "deviation: chat.ui.features.context_compaction" in out
    # The counts req.readback-no-partial-apply and req.idempotent require
    # PRINTED, in the operator-readable output and not only in the JSON.
    assert "written surfaces verified: 4 of 5 (dropped: prompts [dropped])" in out
    # The arithmetic is printed too. Without this line a surface that is
    # neither verified nor dropped is invisible in the human output.
    assert "written surfaces accounted (verified + dropped): 5 of 5" in out
    assert "dropped prompts [dropped]:" in out
    assert "surfaces with changes detected and applied: 4" in out
    assert "census-admitted knobs driven: 3 of 4" in out
    # Every census exclusion is named with its reason, so a shrinking driven
    # set is legible instead of silently satisfying a self-scoped quantifier.
    assert "census-excluded knob chat.ui.theme.accent" in out
    # #3965: the printed census lines are labelled as notes, and none says an
    # existing preset cannot be updated (sync updates it via model/update).
    assert "census-excluded knob chat.preset" in out
    assert "census note, not a write failure" in out
    assert "cannot update an existing preset" not in out.lower()
    # All six surfaces are named in the human rendering: the five written
    # lines plus the mcp probe line.
    for surface in (*WRITTEN_SURFACES, SURFACE_MCP):
        assert f"  {surface:<13}" in out
    # And the unsatisfiable clause names the KEY, not just "knowledge bases".
    assert "rag.bypass_embedding_and_retrieval" in out


def test_cli_exits_non_zero_and_still_prints_the_partial_state(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """A failed run must still show the operator what was already written.

    Adversarial-review finding: `_demote_on_failure` carefully preserves each
    surface's diff and appends a "PARTIALLY applied" note, and the CLI used to
    print only the error — so nobody ever saw either.
    """
    instance = _instance(webui_name="Fixture Site")
    instance.fail_paths.add("/api/v1/configs/tool_servers")
    code = _cli(tmp_path, instance, monkeypatch)
    captured = capsys.readouterr()
    assert code == 1
    assert "PARTIALLY applied" in captured.out
    assert "instance_ui" in captured.out
    assert "applied_but_run_failed" in captured.out
    assert "chat_sync_failed" in captured.err
    assert TOKEN not in captured.out and TOKEN not in captured.err


def test_cli_refuses_when_the_operator_env_is_unset(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    import argparse

    from okg.substrate.cli import chat as cli_chat

    monkeypatch.delenv(ENV_INSTANCE_URL, raising=False)
    monkeypatch.delenv(ENV_ADMIN_TOKEN, raising=False)
    args = argparse.Namespace(
        deployment="fixture", mcp_probe_url=None, mcp_instance_url=None,
        mcp_host="127.0.0.1",
    )
    assert cli_chat._cmd_sync(args, as_json=True) == 1
    assert ENV_INSTANCE_URL in capsys.readouterr().err


def test_posture_dict_is_serializable() -> None:
    assert Posture(True, False, "why").to_dict() == {
        "masked": True, "indeterminate": False, "reason": "why",
    }


# ---------------------------------------------------------------------------
# The packaged measurements: reports name them, and a zip install refuses
# ---------------------------------------------------------------------------
def test_a_real_run_report_names_no_absolute_install_path(tmp_path: Path) -> None:
    """A report names the packaged census by its package-relative path.

    The run consults the census and drops the prompts surface (no proof is
    packaged), so both files are named somewhere in the report. Neither may
    appear as this machine's install location: a report is shared, and the
    absolute path belongs only in error data.
    """
    instance = _instance(webui_name="Fixture Site")
    report = _run(_make_inputs(tmp_path), instance)
    assert report.ok, report.to_dict()
    serialized = json.dumps(report.to_dict(), default=str)

    install_dir = str(chat_data_dir())
    assert install_dir not in serialized
    assert str(Path(install_dir).parent) not in serialized
    assert "okg/substrate/chat/data/ui-knob-census.json" in serialized
    assert "okg/substrate/chat/data/prompts-api-readback.json" in serialized
    # Control: the packaged directory really is absolute, so the check above
    # could have failed.
    assert Path(install_dir).is_absolute()


def test_a_zip_install_refuses_sync_with_the_typed_census_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No on-disk data directory is a census refusal, not a traceback."""
    import okg.substrate.chat.data as chat_data

    class _NotADirectory:
        def __str__(self) -> str:
            return str(tmp_path / "okg.zip" / "okg" / "substrate" / "chat" / "data")

    monkeypatch.setattr(chat_data.resources, "files", lambda _name: _NotADirectory())
    with pytest.raises(ChatUiCensusUnavailable) as excinfo:
        chat_data.chat_data_dir()
    assert excinfo.value.data["reason"] == "missing_or_unreadable"

    with pytest.raises(ChatUiCensusUnavailable) as excinfo:
        _run(_make_inputs(tmp_path), _instance(webui_name="Fixture Site"))
    assert excinfo.value.data["reason"] == "missing_or_unreadable"


def test_building_inputs_and_a_plan_never_resolves_the_data_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``okg chat plan`` reads no measurement, so a zip install must not break it."""
    import okg.substrate.chat.data as chat_data
    from okg.substrate.chat.plan_apply import resolve_chat_plan

    def _refuse() -> Path:
        raise AssertionError("the chat data directory was resolved")

    monkeypatch.setattr(chat_data, "chat_data_dir", _refuse)
    inputs = _make_inputs(tmp_path)
    assert inputs.chat_data_dir is None
    plan = resolve_chat_plan(inputs, require_credential=False)
    assert plan["plan_digest"]
