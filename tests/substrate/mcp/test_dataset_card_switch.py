"""PACT dataset-card-switch: a deployment can turn the dataset card off.

`mcp: {dataset_card: false}` in deployment.yaml. Absent or `true` leaves the
card on and the served text unchanged. Off means no card is gathered (no
database read, no path check) and none is served; everything else in the
served instructions stays as it was. The readbacks say which.

Requirements proved here:

- req.dataset-card-switch.off -- off serves no card and runs no gather; the
  rest of the served text equals the on server's text minus the card.
- req.dataset-card-switch.default-on -- absent and `true` serve the card.
- req.dataset-card-switch.readback -- the surface report, the
  `inspect(session_start)` block and the fleet doctor's summary state on/off;
  the doctor's expected authored hash still matches.
- req.dataset-card-switch.manifest -- the key is boolean only, in the loader
  and in the generated schema alike.

No database: every server here is the `OKG_MCP_SURFACE_PROBE=1` build.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("mcp")

from jsonschema import Draft202012Validator  # noqa: E402

from okg.substrate.deployments import DeploymentManifest  # noqa: E402
from okg.substrate.mcp.dataset_card import (  # noqa: E402
    CARD_HEADER,
    DEPLOYMENT_GUIDANCE_HEADING,
    split_dataset_card,
)
from okg.substrate.mcp.provenance import UNTRUSTED_DATA_CONTRACT  # noqa: E402
from okg.substrate.mcp.tool_profiles import (  # noqa: E402
    ToolProfileConfigInvalid,
    dataset_card_enabled,
    parse_tool_profile_config,
)
from okg.substrate.mcp.tool_prose import EXAMPLE_SUBTYPE_PLACEHOLDER  # noqa: E402

DEPLOYMENT = "cardswitch"
ROOT = Path(__file__).resolve().parents[3]
FILE_TEXT = "Answer from the tickets first.\n"
OFF_REASON = "deployment config mcp.dataset_card: false"


def _write_deployment(root: Path, mcp_block: str) -> Path:
    dep = root / DEPLOYMENT
    (dep / "schemas").mkdir(parents=True)
    (dep / "docs").mkdir()
    (dep / "source_registry.yaml").write_text("", encoding="utf-8")
    (dep / "docs" / "mcp-instructions.md").write_text(FILE_TEXT, encoding="utf-8")
    (dep / "deployment.yaml").write_text(
        f"name: {DEPLOYMENT}\ndescription: dataset-card switch fixture\n"
        "schema_dir: schemas\nsource_registry: source_registry.yaml\n"
        "mcp_instructions: docs/mcp-instructions.md\n"
        "not_ingested:\n  - TWiki pages\n" + mcp_block,
        encoding="utf-8",
    )
    return dep / "deployment.yaml"


# ── req.dataset-card-switch.manifest ──────────────────────────────────


def test_the_key_parses_and_defaults_on() -> None:
    assert dataset_card_enabled(None) is True
    assert dataset_card_enabled(parse_tool_profile_config({})) is True
    assert dataset_card_enabled(
        parse_tool_profile_config({"dataset_card": True})
    ) is True
    assert dataset_card_enabled(
        parse_tool_profile_config({"dataset_card": False})
    ) is False


@pytest.mark.parametrize("value", ["false", "no", 0, 1, None, [], {}])
def test_a_non_boolean_is_refused(value: Any) -> None:
    with pytest.raises(ToolProfileConfigInvalid, match="dataset_card"):
        parse_tool_profile_config({"dataset_card": value})


@pytest.mark.parametrize(("block", "expected"), [
    ("", True),
    ("mcp:\n  dataset_card: true\n", True),
    ("mcp:\n  dataset_card: false\n", False),
])
def test_the_manifest_loads_the_key(
    tmp_path: Path, block: str, expected: bool,
) -> None:
    manifest = DeploymentManifest.from_yaml(_write_deployment(tmp_path, block))
    assert dataset_card_enabled(parse_tool_profile_config(manifest.mcp)) is expected


@pytest.mark.parametrize("value", ["'no'", "0", "null", "[]"])
def test_the_manifest_refuses_a_non_boolean(tmp_path: Path, value: str) -> None:
    with pytest.raises(Exception, match="dataset_card"):
        DeploymentManifest.from_yaml(
            _write_deployment(tmp_path, f"mcp:\n  dataset_card: {value}\n"),
        )


@pytest.mark.parametrize("value", [True, False, "false", 0, None])
def test_the_schema_agrees_with_the_loader(value: Any) -> None:
    schema = json.loads(
        (ROOT / "schemas" / "deployment.schema.json").read_text(encoding="utf-8")
    )
    fragment = {"$defs": schema["$defs"], **schema["properties"]["mcp"]}
    errors = list(Draft202012Validator(fragment).iter_errors({"dataset_card": value}))
    try:
        parse_tool_profile_config({"dataset_card": value})
    except ToolProfileConfigInvalid:
        parsed = False
    else:
        parsed = True
    assert parsed == (not errors), value


# ── servers built without a database ──────────────────────────────────


def _build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, block: str,
    *, forbid_gather: bool = False, allow_writes: bool = True,
) -> Any:
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {"dsn": PROBE_DSN, "authority": {"source": "stub"}},
    )
    if forbid_gather:
        def _refuse(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("the dataset card was gathered while off")

        # Every way the build compiles card facts: the live gather, its
        # no-pin form and the no-database form.
        monkeypatch.setattr(server_mod, "_gather_dataset_facts", _refuse)
        monkeypatch.setattr(server_mod, "_gather_dataset_facts_for_start", _refuse)
        monkeypatch.setattr(server_mod, "_dataset_facts_without_database", _refuse)
    _write_deployment(tmp_path, block)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    # A write launch by default, so `inspect(target={"kind": "surface"})`
    # (the kind the fleet doctor calls) is served.
    return server_mod.build_server(
        dsn=PROBE_DSN, deployment_name=DEPLOYMENT, allow_writes=allow_writes,
    )


def _surface(mcp: Any) -> dict[str, Any]:
    return mcp._tool_manager._tools["inspect"].fn(target={"kind": "surface"})


@pytest.mark.parametrize("block", ["", "mcp:\n  dataset_card: true\n"])
def test_absent_or_true_serves_the_card(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, block: str,
) -> None:
    mcp = _build(tmp_path, monkeypatch, block)
    try:
        text = mcp.instructions or ""
        card, rest = split_dataset_card(text)
        assert card.startswith(CARD_HEADER)
        assert "- TWiki pages" in card
        state = mcp._okg_dataset_card_state
        assert state["state"] == "on"
        assert state["text"] == card
        assert mcp._okg_surface_freshness["dataset_card_state"] == "on"
        assert _surface(mcp)["dataset_card_state"] == "on"
    finally:
        mcp._okg_pool.close()


def test_off_serves_no_card_and_gathers_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    on = _build(tmp_path / "on", monkeypatch, "")
    try:
        on_text = on.instructions or ""
        on_report = dict(on._okg_surface_freshness)
    finally:
        on._okg_pool.close()
    off = _build(
        tmp_path / "off", monkeypatch, "mcp:\n  dataset_card: false\n",
        forbid_gather=True,
    )
    try:
        text = off.instructions or ""
        assert CARD_HEADER not in text
        assert "TWiki pages" not in text
        # Everything but the card is served exactly as with the card on:
        # okg's guidance, the trust contract, the deployment file under its
        # heading and the unavailable lines.
        _card, on_rest = split_dataset_card(on_text)
        assert text == on_rest
        assert UNTRUSTED_DATA_CONTRACT in text
        assert "\n\n" + DEPLOYMENT_GUIDANCE_HEADING + "\n\n" + FILE_TEXT.strip() in text
        report = off._okg_surface_freshness
        assert report["authored_instructions_hash"] == on_report[
            "authored_instructions_hash"
        ]
        assert report["dataset_card_bytes"] == 0
        assert report["dataset_card_state"] == "off"
        # The aggregate example names no subtype it was not told about.
        aggregate = off._tool_manager._tools["aggregate"].description
        assert EXAMPLE_SUBTYPE_PLACEHOLDER in aggregate
    finally:
        off._okg_pool.close()


def test_off_is_read_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp.surface_fleet import _surface_summary, expected_surface

    mcp = _build(tmp_path, monkeypatch, "mcp:\n  dataset_card: false\n")
    try:
        # `inspect(session_start)` serves this dict as its `dataset_card`.
        state = mcp._okg_dataset_card_state
        assert state["state"] == "off"
        assert state["reason"] == OFF_REASON
        assert "text" not in state
        assert state["deployment_guidance"]["heading"] == DEPLOYMENT_GUIDANCE_HEADING
        surface = _surface(mcp)
        assert surface["dataset_card_state"] == "off"
        assert _surface_summary(surface)["dataset_card_state"] == "off"
    finally:
        mcp._okg_pool.close()
    # The fleet doctor recomputes the authored part of a read registration
    # without a database; turning the card off does not make a healthy server
    # look stale.
    read_only = _build(
        tmp_path / "ro", monkeypatch, "mcp:\n  dataset_card: false\n",
        allow_writes=False,
    )
    try:
        expected = expected_surface(DEPLOYMENT)
        assert expected["instructions_hash"] == (
            read_only._okg_surface_freshness["authored_instructions_hash"]
        )
        assert read_only._okg_surface_freshness["dataset_card_state"] == "off"
    finally:
        read_only._okg_pool.close()
