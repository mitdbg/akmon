"""A bundle's chat contribution, composed with the deployment's chat: block.

Slice S3 of #1183. Every test builds a REAL package-v2 archive carrying a
chat contribution, resolves it through #1179's install verb, composes the
result with the S2 fixture manifest, and asserts on the plan's bytes and on
the fake instance's request log. No database, no container, no network.

The fixture names are neutral on purpose: the composition is a substrate
contract, and a fixture carrying one deployment's domain vocabulary would
read as if the contract were that deployment's.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, cast

import pytest

import okg.deployment as deployment
from okg.substrate.chat.bundle_composition import (
    CONTRIBUTION_SCHEMA,
    ChatBundleAssetUnresolved,
    ChatBundleConnectionUnresolved,
    ChatBundleContributionInvalid,
    ChatBundleLiveToolConflict,
    ChatBundleManifestWidens,
    bundle_chat_selection,
    compose_bundle_chat,
)
from okg.substrate.chat.plan_apply import (
    chat_readback,
    resolve_chat_plan,
    write_chat_plan,
)
from okg.substrate.chat.sync import (
    load_bundle_chat,
    plan_tool_curation,
    resolve_posture,
)
from okg.substrate.contracts.deployment_product import (
    CompatibilityMetadata,
    DistributionRelease,
)
from okg.substrate.mcp.operators import CHAT_DEFAULT_TOOLS
from okg.substrate.distributions import (
    AssetLocator,
    DistributionObject,
    DistributionPackageManifest,
    DistributionPackageProvenance,
    DistributionPackageSource,
    ModuleDeclaration,
    OkgCompatibility,
    PackagedAsset,
    build_distribution_package,
    canonical_release_digest,
    verify_distribution_package,
)
from okg.substrate.distributions import package as package_impl
from okg.substrate.distributions.install_plan import (
    resolve_install_plan,
    write_install_plan,
)

from ..catalog.test_module_providers import _empty_cross_links, _write_module
from ..catalog.test_package_module_provider import (
    _MODULE_ID,
    _OWNER,
    _SCHEMA_YAML,
    _asset,
    _ref,
)
from ..test_chat_plan_apply_readback import (
    LIVE_TOOLS,
    _client,
    _instance,
    _make_inputs,
    _probe,
    _raw_manifest,
)

_ZERO_DIGEST = "sha256:" + "0" * 64
_REVISION = "0" * 40
_OKG_VERSION = "1.0.0"
_BUNDLE = "lab-alpha"
_INSTANCE: dict[str, Any] = {"name": "demo"}
#: A live tool the bundle NAMES but does not wire. S4 owns the slot.
_LIVE_TOOL = "notebook_render"
#: The variable the S2 fixture manifest names for the MCP bearer credential.
MCP_TOKEN_ENV = "OKG_TEST_CHAT_MCP_TOKEN"


@pytest.fixture
def mcp_token(monkeypatch: pytest.MonkeyPatch) -> str:
    """A credential built at RUN TIME, never a literal (CodeQL, and honesty).

    Redeclared here rather than imported: a fixture is resolved by the
    module under collection, so importing the S2 helpers does not bring
    its fixtures along.
    """
    value = "mcp-" + hashlib.sha256(b"bundle-composition").hexdigest()[:24]
    monkeypatch.setenv(MCP_TOKEN_ENV, value)
    return value


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _contribution(**overrides: Any) -> bytes:
    document: dict[str, Any] = {
        "schema": CONTRIBUTION_SCHEMA,
        "branding": {
            "name": "Bundle Site",
            "description": "Ask the packaged graph.",
            "logo": f"{_OWNER}:site-logo",
        },
        "instructions": [f"{_OWNER}:review-playbook"],
        "connections": [f"{_OWNER}:readers", f"{_OWNER}:writers"],
        "graph_tools": list(LIVE_TOOLS),
        "live_tools": [_LIVE_TOOL],
    }
    document.update(overrides)
    return json.dumps(document, sort_keys=True).encode("utf-8")


def _connector_asset(connector_type: str, class_name: str) -> bytes:
    """One connector asset body, as a real declared registration.

    #1181's install verb reads every selected `connector` asset back
    through `ConnectorRegistration.from_asset_payload`, so a fixture
    connection has to be an actual declaration rather than a stand-in
    string. Nothing here is imported or run: the body is data the plan
    validates. `required_modules` is empty on purpose -- these two
    connections exist to be NAMED by the chat contribution, and a module
    requirement would test the install verb, not the composition.
    """
    registration = deployment.ConnectorRegistration(
        connector_type=connector_type,
        version="1.0.0",
        module="lab_units.connections",
        class_name=class_name,
        ownership_id=f"lab_units.{connector_type}",
        source_class="discovery_crawl",
        trust_label="archi_operated",
        sdk_range=deployment.SdkVersionRange("1.0.0", "9.0.0"),
    )
    return json.dumps(
        registration.as_asset_payload(), sort_keys=True,
    ).encode("utf-8")


def _package(tmp_path: Path, *, contribution: bytes | None = None) -> Path:
    """A package-v2 archive whose single bundle carries a chat contribution."""
    root = tmp_path / "authoring"
    root.mkdir(parents=True, exist_ok=True)
    output = tmp_path / "packages"
    output.mkdir(exist_ok=True)

    payloads: dict[str, bytes] = {
        "lab-units-schema": _SCHEMA_YAML,
        "chat-contribution": _contribution() if contribution is None else contribution,
        "review-playbook": b"# Review\nQuote node ids.\n",
        "site-logo": b"<svg role='img'></svg>",
        "readers": _connector_asset("lab_units_read", "LabUnitsReader"),
        "writers": _connector_asset("lab_units_write", "LabUnitsWriter"),
    }
    kinds = {
        "lab-units-schema": "ontology_schema",
        "chat-contribution": "chat-policy",
        "review-playbook": "playbook",
        "site-logo": "interface",
        "readers": "connector",
        "writers": "connector",
    }
    assets = [_asset(name, payloads[name], kind=kinds[name]) for name in payloads]
    refs = tuple(_ref(item) for item in assets)
    packaged = tuple(
        PackagedAsset(
            asset=item,
            locator=AssetLocator(object_digest=_digest(payloads[item.asset_id])),
            uncompressed_size=len(payloads[item.asset_id]),
        )
        for item in assets
    )
    bundle = package_impl.ProductBundle(
        bundle_id=_BUNDLE,
        version="v1",
        distribution_id=_OWNER,
        distribution_version="1.0.0",
        bundle_digest=_ZERO_DIGEST,
        assets=refs,
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("public",),
        ),
    )
    bundle = bundle.model_copy(
        update={"bundle_digest": package_impl.canonical_bundle_digest(bundle)},
    )
    release = DistributionRelease(
        distribution_id=_OWNER,
        version="1.0.0",
        origin="external",
        release_digest=_ZERO_DIGEST,
        assets=refs,
        bundle_ids=(_BUNDLE,),
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("public",),
        ),
    )
    provisional = DistributionPackageManifest(
        schema_version=package_impl.SCHEMA_V2,
        distribution_release=release,
        bundles=(bundle,),
        assets=packaged,
        objects=tuple(
            DistributionObject(
                digest=_digest(payload),
                size=len(payload),
                media_type="application/octet-stream",
            )
            for payload in payloads.values()
        ),
        provenance=DistributionPackageProvenance(
            repository_url="https://github.com/example/lab-units",
            revision=_REVISION,
            builder="okg-hermetic-test",
        ),
        okg_compatibility=OkgCompatibility(
            min_version="0.1.0", max_version_exclusive="9.0.0",
        ),
        module_declarations=(
            ModuleDeclaration(module_id=_MODULE_ID, asset_refs=(refs[0],)),
        ),
    )
    manifest = cast(
        DistributionPackageManifest,
        provisional.model_copy(
            update={
                "distribution_release": release.model_copy(
                    update={"release_digest": canonical_release_digest(provisional)},
                ),
            },
        ),
    )
    sources = {}
    for name, payload in payloads.items():
        member = root / f"{name}.bin"
        member.write_bytes(payload)
        sources[_digest(payload)] = DistributionPackageSource(root=root, path=member)
    return build_distribution_package(manifest, sources, output)


def _builtin(tmp_path: Path) -> Path:
    root = tmp_path / "builtin"
    _write_module(root, "reasoning", {"Hypothesis": "entity"})
    return root


def _install_plan(tmp_path: Path, package_path: Path) -> dict[str, Any]:
    plan, _lock = resolve_install_plan(
        package_path,
        _BUNDLE,
        _INSTANCE,
        okg_version=_OKG_VERSION,
        environ={},
        builtin_modules_root=_builtin(tmp_path),
        cross_links_root=_empty_cross_links(tmp_path),
    )
    return plan


def _selection(tmp_path: Path, *, contribution: bytes | None = None) -> Any:
    package_path = _package(tmp_path, contribution=contribution)
    plan = _install_plan(tmp_path, package_path)
    package = verify_distribution_package(package_path)
    try:
        return bundle_chat_selection(package, plan)
    finally:
        package.close_runtime_authority()


def _without_mcp(chat: Any) -> Any:
    """The parsed block with its ``mcp`` dropped — built, not parsed.

    No MANIFEST can express this: `chat.mcp.tools` is mandatory and
    non-empty wherever the block exists, `chat.mcp` itself is mandatory
    wherever `chat.enabled` is true, and `load_inputs` refuses a chat that
    is not enabled. `compose_bundle_chat` is exported all the same, and
    the tool ceiling it enforces must not depend on a check two modules
    away, so the shape is constructed here and the function is held to it
    directly.
    """
    return dataclasses.replace(chat, mcp=None)


def _composed_inputs(
    tmp_path: Path,
    *,
    manifest: dict[str, Any] | None = None,
    contribution: bytes | None = None,
) -> Any:
    """The S2 inputs, with the bundle composed in exactly as ``load_inputs`` does."""
    inputs = _make_inputs(tmp_path / "dep", manifest)
    selection = _selection(tmp_path, contribution=contribution)
    chat, record = compose_bundle_chat(inputs.chat, selection)
    return dataclasses.replace(inputs, chat=chat, bundle_chat=record)


# ---------------------------------------------------------------------------
# (a) the contribution is a typed asset the install plan resolves
# ---------------------------------------------------------------------------

def test_the_contribution_is_a_typed_asset_resolved_by_the_install_plan(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-bundle-composition.contribution."""
    selection = _selection(tmp_path)
    assert selection.bundle_id == _BUNDLE
    assert selection.release_digest.startswith("sha256:")
    assert selection.bundle_digest.startswith("sha256:")
    document = selection.document
    assert document["contribution"]["qualified_id"] == f"{_OWNER}:chat-contribution"
    assert document["contribution"]["asset_digest"].startswith("sha256:")
    # every reference resolved to a packaged asset, with ITS digest
    assert document["branding"]["logo"]["qualified_id"] == f"{_OWNER}:site-logo"
    assert [item["qualified_id"] for item in document["instructions"]] == [
        f"{_OWNER}:review-playbook",
    ]
    assert [item["qualified_id"] for item in document["connections"]] == [
        f"{_OWNER}:readers", f"{_OWNER}:writers",
    ]
    assert all(
        item["asset_digest"].startswith("sha256:")
        for item in document["instructions"] + document["connections"]
    )
    assert document["live_tools"] == [_LIVE_TOOL]


def test_a_package_with_no_contribution_composes_nothing(tmp_path: Path) -> None:
    """A package that says nothing about chat is not an error."""
    package_path = _package(tmp_path)
    plan = _install_plan(tmp_path, package_path)
    stripped = dict(plan)
    stripped["assets"] = [
        item for item in plan["assets"] if item["kind"] != "chat-policy"
    ]
    package = verify_distribution_package(package_path)
    try:
        assert bundle_chat_selection(package, stripped) is None
    finally:
        package.close_runtime_authority()


def test_load_bundle_chat_reads_the_written_install_plan(tmp_path: Path) -> None:
    """The verb-level path: plan.json plus the package it names, verified."""
    package_path = _package(tmp_path)
    out = tmp_path / "install"
    write_install_plan(
        package_path,
        _BUNDLE,
        _INSTANCE,
        out,
        okg_version=_OKG_VERSION,
        environ={},
        builtin_modules_root=_builtin(tmp_path),
        cross_links_root=_empty_cross_links(tmp_path),
    )
    shutil.copy2(package_path, out / package_path.name)
    selection = load_bundle_chat(out)
    assert selection is not None
    assert selection.bundle_id == _BUNDLE


# ---------------------------------------------------------------------------
# (b) precedence: bundle default, manifest override, manifest narrowing
# ---------------------------------------------------------------------------

def test_the_bundle_supplies_branding_where_the_manifest_is_silent(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-bundle-composition.precedence — the default arm."""
    raw = _raw_manifest(
        ui={"features": {"web_search": False}},
        preset={"suggestions": ["What changed?"]},
    )
    inputs = _composed_inputs(tmp_path, manifest=raw)
    plan = resolve_chat_plan(inputs)
    assert plan["surfaces"]["instance_ui"]["desired"]["site_name"].startswith(
        "Bundle Site",
    )
    preset = plan["surfaces"]["preset"]["desired"]
    assert preset["preset:name"] == "Bundle Site"
    assert preset["preset:meta.description"] == "Ask the packaged graph."
    record = plan["bundle"]["branding"]
    assert record["site_name"] == "bundle"
    assert record["preset_name"] == "bundle"
    assert record["preset_description"] == "bundle"
    # instructions: the manifest declares no system_prompt_ref here, and
    # nothing in this slice writes a packaged playbook onto the site, so the
    # source is UNDETERMINED rather than a claim the bundle supplied it
    assert plan["bundle"]["instructions"]["source"] == "undetermined"
    assert plan["bundle"]["instructions"]["selected"] == []
    assert [
        item["qualified_id"] for item in plan["bundle"]["instructions"]["declared"]
    ] == [f"{_OWNER}:review-playbook"]


def _written_install_plan(tmp_path: Path) -> Path:
    package_path = _package(tmp_path)
    out = tmp_path / "install"
    write_install_plan(
        package_path,
        _BUNDLE,
        _INSTANCE,
        out,
        okg_version=_OKG_VERSION,
        environ={},
        builtin_modules_root=_builtin(tmp_path),
        cross_links_root=_empty_cross_links(tmp_path),
    )
    shutil.copy2(package_path, out / package_path.name)
    return out


def _setup_webui_name(tmp_path: Path, raw: dict[str, Any], install_plan: Path) -> Any:
    """Run `okg chat setup --install-plan` through its instance step with the
    real `stand_up` on a process runtime; return the container's WEBUI_NAME."""
    import urllib.error

    from okg.substrate.chat.instance import stand_up as real_stand_up
    from okg.substrate.chat.setup import SetupOptions, run_setup
    from okg.substrate.deployments.chat_block import parse_chat_block

    from ..chat.instance_harness import ProcessRuntime, free_port, reachable_probe

    chat = parse_chat_block(raw, manifest_path=None)
    runtime = ProcessRuntime()

    def stand_up(deployment: str, **kwargs: Any) -> Any:
        kwargs.update(
            runtime=runtime, reachability_probe=reachable_probe,
            instance_port_override=free_port(), ready_timeout=20.0,
        )
        return real_stand_up(deployment, **kwargs)

    def unreachable(_request: Any, _timeout: float) -> Any:
        raise urllib.error.URLError("the test stops after the instance step")

    try:
        report = run_setup(
            "fixture",
            options=SetupOptions(install_plan=str(install_plan)),
            environ={
                "OKG_CHAT_APP_DATABASE_URL": "postgresql://okg:pw@127.0.0.1:5432/fixture_app",
                "OKG_CHAT_WEBUI_SECRET_KEY": "a-long-random-fixture-key",
            },
            state_root=tmp_path / "state",
            load_manifest=lambda: (
                chat, "postgresql://okg:pw@127.0.0.1:5432/fixture_graph",
            ),
            stand_up=stand_up,
            opener=unreachable,
        )
        assert report.steps["instance"].status == "ok", report.to_dict()
        (spec,) = runtime.run_calls
        return spec.env.get("WEBUI_NAME")
    finally:
        runtime.shutdown()


def test_setup_starts_the_container_with_the_bundles_site_name(
    tmp_path: Path, mcp_token: str,
) -> None:
    """Fail-before (at 6a9b62dcbd): a chat block with no ui.site_name
    started the container with no WEBUI_NAME, while plan and readback expect
    the bundle's name, so readback failed on site_name (#2875)."""
    raw = _raw_manifest(ui={"features": {"web_search": False}})

    assert _setup_webui_name(tmp_path, raw, _written_install_plan(tmp_path)) == "Bundle Site"


def test_setup_keeps_the_manifests_site_name_over_the_bundles(
    tmp_path: Path, mcp_token: str,
) -> None:
    """Control: the manifest's name wins, as it does in the plan."""
    raw = _raw_manifest()

    assert _setup_webui_name(tmp_path, raw, _written_install_plan(tmp_path)) == "Fixture Site"


_APP_DSN = "postgresql://okg:pw@127.0.0.1:5432/fixture_app"
_GRAPH_DSN = "postgresql://okg:pw@127.0.0.1:5432/fixture_graph"
_SECRET = "a-long-random-fixture-key"


class _SetupWithAPlan:
    """A setup run with --install-plan through the real `stand_up` on a
    process runtime; it stops after the instance step (no instance HTTP)."""

    def __init__(self, tmp_path: Path) -> None:
        from okg.substrate.chat.instance import stand_up
        from okg.substrate.deployments.chat_block import parse_chat_block

        from ..chat.instance_harness import ProcessRuntime, free_port, reachable_probe

        self.tmp_path = tmp_path
        self.chat = parse_chat_block(
            _raw_manifest(ui={"features": {"web_search": False}}), manifest_path=None,
        )
        self.runtime = ProcessRuntime()
        self.plan = _written_install_plan(tmp_path)
        self.dialled: list[str] = []
        self.common: dict[str, Any] = {
            "runtime": self.runtime, "reachability_probe": reachable_probe,
            "instance_port_override": free_port(), "ready_timeout": 20.0,
            "state_root": tmp_path / "state",
        }
        self.stand_up = stand_up

    def seam(self, deployment: str, **kwargs: Any) -> Any:
        return self.stand_up(deployment, **{**kwargs, **self.common})

    def opener(self, request: Any, _timeout: float) -> Any:
        import urllib.error

        self.dialled.append(request.full_url)
        raise urllib.error.URLError("the test stops after the instance step")

    def setup(self, install_plan: Path | None) -> Any:
        from okg.substrate.chat.setup import SetupOptions, run_setup

        return run_setup(
            "fixture",
            options=SetupOptions(install_plan=str(install_plan) if install_plan else None),
            environ={"OKG_CHAT_APP_DATABASE_URL": _APP_DSN, "OKG_CHAT_WEBUI_SECRET_KEY": _SECRET},
            state_root=self.tmp_path / "state",
            load_manifest=lambda: (self.chat, _GRAPH_DSN),
            stand_up=self.seam,
            opener=self.opener,
        )


def test_a_plain_up_after_setup_with_a_plan_refuses_naming_the_recorded_plan(
    tmp_path: Path, mcp_token: str,
) -> None:
    """The recorded plan is never reused implicitly (it may be gone, and
    setup's sync would not compose it); the refusal names it so the operator
    can pass it again. Fail-before (at eb03327893): the plan was reused."""
    from okg.substrate.chat.instance import ChatInstanceError

    run = _SetupWithAPlan(tmp_path)
    try:
        run.setup(run.plan)

        with pytest.raises(ChatInstanceError) as caught:
            run.stand_up(
                "fixture", chat_block=run.chat, graph_dsn=_GRAPH_DSN,
                environ={"OKG_CHAT_APP_DATABASE_URL": _APP_DSN, "OKG_CHAT_WEBUI_SECRET_KEY": _SECRET},
                **run.common,
            )

        recorded = str(run.plan.resolve())
        assert caught.value.data["reason"] == "instance_settings_changed"
        assert caught.value.data["recorded_install_plan"] == recorded
        assert f"--install-plan {recorded}" in (caught.value.fix_hint or "")
        (spec,) = run.runtime.run_calls
        assert spec.env["WEBUI_NAME"] == "Bundle Site"
    finally:
        run.runtime.shutdown()


def test_setup_without_the_plan_after_setup_with_it_refuses_before_writing(
    tmp_path: Path, mcp_token: str,
) -> None:
    """Fail-before (at eb03327893): the second setup reused the plan for the
    container, then reached the admin, provider and sync steps with a block
    that did not carry the bundle. Now it stops at the instance step and
    never dials the instance."""
    run = _SetupWithAPlan(tmp_path)
    try:
        run.setup(run.plan)
        run.dialled.clear()

        report = run.setup(None)

        assert report.failed_step == "instance", report.to_dict()
        assert report.error["data"]["reason"] == "instance_settings_changed"
        assert str(run.plan.resolve()) in report.error["fix_hint"]
        assert run.dialled == []
        assert len(run.runtime.run_calls) == 1
    finally:
        run.runtime.shutdown()


def test_chat_instance_up_takes_install_plan_and_records_it(
    tmp_path: Path, mcp_token: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg chat-instance up --install-plan` starts the container with the
    bundle's site name and records the plan; `up` with the same plan answers
    already_running, and a plain `up` refuses naming the recorded plan
    (fail-before at fc76534710: no plan was recorded)."""
    import argparse

    from okg.substrate.chat import instance as instance_module
    from okg.substrate.chat import instance_cli
    from okg.substrate.chat.instance import read_instance
    from okg.substrate.deployments.chat_block import parse_chat_block

    from ..chat.instance_harness import ProcessRuntime, free_port

    chat = parse_chat_block(
        _raw_manifest(ui={"features": {"web_search": False}}), manifest_path=None,
    )
    runtime = ProcessRuntime()
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("OKG_CHAT_APP_DATABASE_URL", _APP_DSN)
    monkeypatch.setenv("OKG_CHAT_WEBUI_SECRET_KEY", _SECRET)
    monkeypatch.setattr(instance_cli, "_load_manifest", lambda *_a, **_k: (chat, _GRAPH_DSN))
    monkeypatch.setattr(instance_cli, "_runtime", lambda _args: runtime)
    monkeypatch.setattr(
        instance_module, "require_app_database_reachable", lambda *_a, **_k: None,
    )
    plan = _written_install_plan(tmp_path)

    def args(**extra: Any) -> argparse.Namespace:
        values: dict[str, Any] = {
            "deployment": "fixture", "app_database_url_env": "OKG_CHAT_APP_DATABASE_URL",
            "admin_secret_env": "OKG_CHAT_WEBUI_SECRET_KEY", "instance_port": port,
            "image": None, "host": "127.0.0.1", "host_gateway_alias": "",
            "ready_timeout": 20.0, "install_plan": None, "container_network": "auto",
        }
        values.update(extra)
        return argparse.Namespace(**values)

    port = free_port()
    try:
        assert instance_cli._cmd_up(args(install_plan=str(plan)), as_json=True) == 0
        (spec,) = runtime.run_calls
        assert spec.env["WEBUI_NAME"] == "Bundle Site"
        record = read_instance("fixture")
        assert record is not None and record.install_plan == str(plan.resolve())

        # The same explicit plan again: already running, no second container.
        assert instance_cli._cmd_up(args(install_plan=str(plan)), as_json=True) == 0
        assert len(runtime.run_calls) == 1
        # Without it, the recorded plan is named, not reused.
        with pytest.raises(instance_module.ChatInstanceError) as caught:
            instance_cli._cmd_up(args(), as_json=True)
        assert caught.value.data["recorded_install_plan"] == str(plan.resolve())
        assert len(runtime.run_calls) == 1
    finally:
        runtime.shutdown()


def test_the_manifest_overrides_the_bundles_branding(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-bundle-composition.precedence — the override arm."""
    inputs = _composed_inputs(tmp_path)
    plan = resolve_chat_plan(inputs)
    assert plan["surfaces"]["instance_ui"]["desired"]["site_name"].startswith(
        "Fixture Site",
    )
    assert plan["surfaces"]["preset"]["desired"]["preset:meta.description"] == (
        "Ask the fixture graph."
    )
    assert plan["bundle"]["branding"]["site_name"] == "deployment"
    # the fixture manifest declares chat.preset.system_prompt_ref, which
    # supersedes the bundle's playbooks WHOLESALE rather than being merged
    # with them into a text neither side reviewed
    assert plan["bundle"]["instructions"]["source"] == "deployment_override"


def test_the_manifest_narrows_the_bundles_tools_and_connections(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-bundle-composition.precedence — the narrowing arm."""
    raw = _raw_manifest(
        mcp={
            "port": 8100,
            "tools": ["query", "search"],
            "auth_token_env": MCP_TOKEN_ENV,
            "audiences": {"readers": {"tools": ["query", "search"]}},
        },
    )
    inputs = _composed_inputs(tmp_path, manifest=raw)
    plan = resolve_chat_plan(inputs)
    record = plan["bundle"]
    assert record["graph_tools"]["declared"] == sorted(LIVE_TOOLS)
    assert record["graph_tools"]["selected"] == ["query", "search"]
    assert record["graph_tools"]["source"] == "deployment"
    assert [item["qualified_id"] for item in record["connections"]["selected"]] == [
        f"{_OWNER}:readers",
    ]
    # the narrowing is what the apply will read, not a note beside it
    assert plan["surfaces"]["mcp"]["desired"]["mcp:tools"] == ["query", "search"]


def test_a_manifest_with_no_mcp_block_gets_the_substrate_default_tools(
    tmp_path: Path,
) -> None:
    """req.chat-bundle-composition.precedence — the no-declaration arm.

    A block with no `mcp` does not ask for NO tools: the curation reads an
    absent block as "the deployment named nothing" and serves
    `CHAT_DEFAULT_TOOLS`. The record has to say that, in the curation's
    words, or the plan would describe a tool set the apply does not produce.
    """
    base = _make_inputs(tmp_path / "dep")
    chat, record_in = compose_bundle_chat(
        _without_mcp(base.chat), _selection(tmp_path),
    )
    assert chat.mcp is None
    inputs = dataclasses.replace(base, chat=chat, bundle_chat=record_in)
    plan = resolve_chat_plan(inputs)
    record = plan["bundle"]["graph_tools"]
    assert record["declared"] == sorted(LIVE_TOOLS)
    assert record["selected"] == sorted(CHAT_DEFAULT_TOOLS)
    assert record["source"] == "substrate_default"
    # the record and the curation are one reading, asserted against the
    # curation itself rather than against a second copy of the constant
    curation, _ = plan_tool_curation(
        inputs.chat,
        deployment="fixture",
        posture=resolve_posture(
            "fixture", {"nomos": {"runtime_enforcement": "enforce"}},
        ),
    )
    assert sorted(curation.declared) == record["selected"]
    assert curation.declaration_source == record["source"]


def test_a_manifest_with_no_mcp_block_cannot_widen_past_the_bundle(
    tmp_path: Path,
) -> None:
    """req.chat-bundle-composition.refusals — the default-set widening arm.

    The bundle declares one operator. With no `mcp` on the block the site
    would be served all six defaults, five of them outside what the bundle
    was reviewed for. A default is a widening like any other.
    """
    contribution = _contribution(graph_tools=["search"], live_tools=[_LIVE_TOOL])
    chat = _without_mcp(_make_inputs(tmp_path / "dep").chat)
    with pytest.raises(ChatBundleManifestWidens) as caught:
        compose_bundle_chat(chat, _selection(tmp_path, contribution=contribution))
    assert caught.value.code == "chat_bundle_manifest_widens"
    assert caught.value.data["reason"] == "manifest_widens_tools"
    assert caught.value.data["surface"] == "mcp:tools"
    # the first default the bundle does not declare, named
    assert caught.value.data["undeclared"] == "aggregate"
    assert caught.value.data["declared"] == ["search"]


def test_the_live_tool_names_are_declarations_not_grants(
    tmp_path: Path, mcp_token: str,
) -> None:
    """S4 wires the slot; the plan says so rather than implying a grant."""
    plan = resolve_chat_plan(_composed_inputs(tmp_path))
    assert plan["bundle"]["live_tools"] == {
        "declared": [_LIVE_TOOL], "selected": [],
    }
    # S4 gives the live-tool slot its OWN surface rather than a corner of
    # `mcp`, because it is a different connection carrying a different
    # authority. With no slot declared here the entry is still undetermined
    # for S3's reason -- it just lives on the surface that would carry it.
    assert any(
        item["key"] == "bundle.live_tools.selected"
        for item in plan["surfaces"]["live_tools"]["undetermined"]
    )
    assert not any(
        item["key"] == "bundle.live_tools.selected"
        for item in plan["surfaces"]["mcp"]["undetermined"]
    )
    # the playbooks and the logo are resolved and digested and applied by
    # nothing, so each is recorded under the surface that would consume it
    assert any(
        item["key"] == "bundle.instructions.selected"
        for item in plan["surfaces"]["preset"]["undetermined"]
    )
    assert any(
        item["key"] == "bundle.branding.logo"
        for item in plan["surfaces"]["instance_ui"]["undetermined"]
    )


# ---------------------------------------------------------------------------
# (c) the refusals
# ---------------------------------------------------------------------------

def test_a_playbook_the_package_does_not_carry_refuses(tmp_path: Path) -> None:
    """req.chat-bundle-composition.refusals."""
    with pytest.raises(ChatBundleAssetUnresolved) as caught:
        _selection(tmp_path, contribution=_contribution(
            instructions=[f"{_OWNER}:absent-playbook"],
        ))
    assert caught.value.code == "chat_bundle_asset_unresolved"
    assert caught.value.data["reason"] == "playbook_absent"
    assert caught.value.data["qualified_id"] == f"{_OWNER}:absent-playbook"


def test_an_instruction_asset_of_the_wrong_kind_refuses(tmp_path: Path) -> None:
    """A packaged asset is not a playbook merely by being referenced as one."""
    with pytest.raises(ChatBundleAssetUnresolved) as caught:
        _selection(tmp_path, contribution=_contribution(
            instructions=[f"{_OWNER}:site-logo"],
        ))
    assert caught.value.data["reason"] == "playbook_wrong_kind"


def test_a_connection_the_plan_does_not_resolve_refuses(tmp_path: Path) -> None:
    """req.chat-bundle-composition.refusals — never a raw URL, never a guess."""
    with pytest.raises(ChatBundleConnectionUnresolved) as caught:
        _selection(tmp_path, contribution=_contribution(
            connections=[f"{_OWNER}:absent-endpoint"],
        ))
    assert caught.value.code == "chat_bundle_connection_unresolved"
    assert caught.value.data["reason"] == "connection_absent"


def test_a_manifest_that_widens_the_tools_refuses(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-bundle-composition.refusals — the widening arm.

    The bundle declares two of the seven operators. The manifest's own
    parse already refuses a name outside the operator universe, so the
    only widening the manifest can express — and the only one worth a
    refusal — is asking for an operator this BUNDLE was not reviewed for.
    """
    contribution = _contribution(graph_tools=["query", "search"])
    raw = _raw_manifest(mcp={
        "port": 8100,
        "tools": ["query", "expand"],
        "auth_token_env": MCP_TOKEN_ENV,
    })
    with pytest.raises(ChatBundleManifestWidens) as caught:
        _composed_inputs(tmp_path, manifest=raw, contribution=contribution)
    assert caught.value.code == "chat_bundle_manifest_widens"
    assert caught.value.data["surface"] == "mcp:tools"
    assert caught.value.data["undeclared"] == "expand"
    assert caught.value.data["declared"] == ["query", "search"]


def test_a_manifest_that_widens_the_connections_refuses(
    tmp_path: Path, mcp_token: str,
) -> None:
    raw = _raw_manifest(mcp={
        "port": 8100,
        "tools": list(LIVE_TOOLS),
        "auth_token_env": MCP_TOKEN_ENV,
        "audiences": {"auditors": {"tools": list(LIVE_TOOLS)}},
    })
    with pytest.raises(ChatBundleManifestWidens) as caught:
        _composed_inputs(tmp_path, manifest=raw)
    assert caught.value.data["surface"] == "mcp:connections"
    assert caught.value.data["undeclared"] == "auditors"


def test_a_live_tool_name_that_shadows_a_graph_operator_refuses(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-bundle-composition.refusals — the live-tool arm.

    The design note assumed the manifest could ask for a live tool and be
    refused. It cannot: `chat.mcp.tools` is closed to the OKG operator
    universe at parse time, so that arm is unreachable. The reachable one
    is a bundle declaring a name under BOTH transports, which S4's slot
    could not bind without choosing one silently.
    """
    contribution = _contribution(live_tools=["search"])
    with pytest.raises(ChatBundleLiveToolConflict) as caught:
        _composed_inputs(tmp_path, contribution=contribution)
    assert caught.value.code == "chat_bundle_live_tool_conflict"
    assert caught.value.data["reason"] == "live_tool_shadows_graph_tool"
    assert caught.value.data["tool"] == "search"


def test_a_contribution_carrying_an_unrecognized_key_refuses(
    tmp_path: Path,
) -> None:
    """The key set is closed, so a host or a credential cannot ride along."""
    with pytest.raises(ChatBundleContributionInvalid) as caught:
        _selection(tmp_path, contribution=_contribution(mcp_url="http://elsewhere"))
    assert caught.value.data["reason"] == "contribution_unknown_keys"


def test_a_contribution_that_is_not_the_schema_refuses(tmp_path: Path) -> None:
    with pytest.raises(ChatBundleContributionInvalid) as caught:
        _selection(tmp_path, contribution=b'{"schema": "something/v9"}')
    assert caught.value.data["reason"] == "contribution_schema_unknown"


# ---------------------------------------------------------------------------
# (d) the composed plan is a plan: deterministic, and S2 consumes it unchanged
# ---------------------------------------------------------------------------

def test_the_composed_plan_is_byte_identical_on_rerun(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-bundle-composition.deterministic."""
    inputs = _composed_inputs(tmp_path)
    first = write_chat_plan(inputs, tmp_path / "a")["plan_path"]
    second = write_chat_plan(inputs, tmp_path / "b")["plan_path"]
    assert Path(first).read_bytes() == Path(second).read_bytes()
    plan = json.loads(Path(first).read_text())
    # the digest covers the COMPOSED state: dropping the bundle changes it
    manifest_only = resolve_chat_plan(
        dataclasses.replace(inputs, bundle_chat=None),
    )
    assert plan["plan_digest"] != manifest_only["plan_digest"]


def test_apply_and_readback_consume_the_composed_plan_unchanged(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-bundle-composition.consumed-by-s2.

    S2's apply gate recomputes the plan from the inputs and refuses drift.
    Passing a COMPOSED plan must therefore apply, not refuse, and the
    readback of the resulting instance must report no deviation.
    """
    inputs = _composed_inputs(tmp_path)
    plan = resolve_chat_plan(inputs)
    instance = _instance()
    client = _client(instance)
    from okg.substrate.chat.sync import sync

    report = sync(inputs, client, probe_tools=_probe, expected_plan=plan)
    assert report.plan_digest == plan["plan_digest"]
    document = chat_readback(inputs, _client(instance), plan)
    assert document["plan_digest"] == plan["plan_digest"]
    assert document["ok"], document.get("surfaces")
