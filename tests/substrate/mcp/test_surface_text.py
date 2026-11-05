"""Served text as per-deployment, branch-able config (issue #4280).

Two halves. The pure half pins the file's strictness and the branch merge. The
server half builds the REAL server in surface-probe mode (no database) from a
copy of the live-http-ci fixture, once for the default branch and once for a
branch with its own layer, and reads what an agent is served: the tool list,
a refusal's fix_hint, the instructions, and the digest a replay records.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.mcp.surface_text import (
    NO_SURFACE_TEXT,
    EffectiveSurfaceText,
    SurfaceTextInvalid,
    apply_fix_hint,
    apply_tool_text,
    check_fix_hint_codes,
    effective_surface_text,
    known_error_codes,
    parse_surface_text,
    surface_text_report,
)
from tests.substrate.mcp._result_payload import (
    call_json,
    json_client_sessions,
    payload as _json_payload,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "tests/substrate/e2e/fixtures/live-http-ci"
WHERE = "surface_text.yaml"


# ── the file ──────────────────────────────────────────────────────────


def test_an_empty_file_is_a_declared_surface_with_nothing_overridden() -> None:
    effective = effective_surface_text(parse_surface_text({}, where=WHERE), "default")
    assert effective.declared and effective.branch_layer is None
    assert effective.overridden() == {"tools": [], "params": [], "fix_hints": [], "instructions": []}
    assert not NO_SURFACE_TEXT.declared


@pytest.mark.parametrize("raw, fragment", [
    ({"tool": {"search": "x"}}, "unknown key"),
    ({"branches": {"b": {"branches": {}}}}, "unknown key"),
    ({"branches": {"default": {}}}, "branches.default"),
    ({"fix_hints": {"node_not_found": "{code} again"}}, "placeholder"),
    ({"fix_hints": {"node_not_found": "{default"}}, "not a valid template"),
    ({"fix_hints": {"node_not_found": 3}}, "must be text"),
    ({"fix_hints": {"node_not_found": "x" * 1025}}, "limit is 1024"),
    ({"params": {"search": "no argument named"}}, "<tool>.<argument>"),
    ({"tools": ["search"]}, "must be a mapping"),
    ({"instructions": ""}, "file path"),
])
def test_a_file_that_cannot_change_what_it_says_is_refused(raw: Any, fragment: str) -> None:
    with pytest.raises(SurfaceTextInvalid) as info:
        parse_surface_text(raw, where=WHERE)
    assert fragment in info.value.message
    assert info.value.code == "surface_text_invalid"


def test_a_branch_layer_wins_per_id_and_other_branches_serve_the_base() -> None:
    parsed = parse_surface_text({
        "tools": {"search": "base search", "inspect": "base inspect"},
        "fix_hints": {"node_not_found": "base hint"},
        "instructions": "base.md",
        "branches": {"trial": {
            "tools": {"search": "trial search"},
            "fix_hints": {"search_completeness_refused": "{default}; trial"},
        }},
    }, where=WHERE)
    trial = effective_surface_text(parsed, "trial")
    assert trial.branch_layer == "trial"
    assert trial.tools == {"search": "trial search", "inspect": "base inspect"}
    assert trial.fix_hints == {
        "node_not_found": "base hint", "search_completeness_refused": "{default}; trial",
    }
    assert trial.instructions == "base.md"
    for branch in ("default", "other", ""):
        base = effective_surface_text(parsed, branch)
        assert base.branch_layer is None and base.tools["search"] == "base search"
    assert effective_surface_text(None, "trial") is NO_SURFACE_TEXT


# ── fix_hints ─────────────────────────────────────────────────────────


def _refusal(code: str, hint: str | None = "the code's own hint") -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": "m"}
    if hint is not None:
        error["fix_hint"] = hint
    return {"status": "refused", "error": error}


def test_a_fix_hint_is_replaced_by_code_with_the_default_available() -> None:
    effective = EffectiveSurfaceText(fix_hints={"node_not_found": "copy the id exactly; {default}"})
    payload = _refusal("node_not_found")
    served = apply_fix_hint(payload, effective)
    assert served["error"]["fix_hint"] == "copy the id exactly; the code's own hint"
    assert payload["error"]["fix_hint"] == "the code's own hint", "the caller's payload is not mutated"
    assert apply_fix_hint(_refusal("other_code"), effective) == _refusal("other_code")
    assert apply_fix_hint({"status": "ok"}, effective) == {"status": "ok"}
    assert apply_fix_hint(payload, None) is payload


def test_an_empty_override_drops_the_hint_and_a_missing_default_renders_empty() -> None:
    dropped = apply_fix_hint(_refusal("node_not_found"), EffectiveSurfaceText(fix_hints={"node_not_found": ""}))
    assert "fix_hint" not in dropped["error"]
    added = apply_fix_hint(
        _refusal("node_not_found", hint=None),
        EffectiveSurfaceText(fix_hints={"node_not_found": "new hint{default}"}),
    )
    assert added["error"]["fix_hint"] == "new hint"


def test_an_unknown_error_code_is_refused_and_real_codes_are_known() -> None:
    import okg.substrate.mcp.search  # noqa: F401 - declares search_completeness_refused

    codes = known_error_codes()
    assert "search_completeness_refused" in codes and "surface_text_invalid" in codes
    check_fix_hint_codes(EffectiveSurfaceText(fix_hints={"search_completeness_refused": "x"}))
    with pytest.raises(SurfaceTextInvalid) as info:
        check_fix_hint_codes(EffectiveSurfaceText(fix_hints={"search_completness_refused": "typo"}))
    assert info.value.data["unknown_codes"] == ["search_completness_refused"]


# ── tool text ─────────────────────────────────────────────────────────


def _fake_mcp() -> Any:
    tool = SimpleNamespace(
        description="code default",
        parameters={"type": "object", "properties": {"query": {"type": "string", "description": "q"}}},
    )
    return SimpleNamespace(_tool_manager=SimpleNamespace(_tools={"search": tool}))


def test_tool_and_argument_text_replace_the_published_copy() -> None:
    mcp = _fake_mcp()
    before = surface_text_report(mcp, instructions="i", effective=NO_SURFACE_TEXT)
    effective = EffectiveSurfaceText(
        tools={"search": "deployment text"}, params={"search.query": "what to look for"}, declared=True,
    )
    apply_tool_text(mcp, effective)
    tool = mcp._tool_manager._tools["search"]
    assert tool.description == "deployment text"
    assert tool.parameters["properties"]["query"]["description"] == "what to look for"
    after = surface_text_report(mcp, instructions="i", effective=effective)
    assert before["digest"] != after["digest"]
    assert after["overridden"]["tools"] == ["search"] and after["declared"] is True


@pytest.mark.parametrize("effective, unpublished", [
    (EffectiveSurfaceText(tools={"serch": "typo"}), ["serch"]),
    (EffectiveSurfaceText(params={"search.limit": "no such argument"}), ["search.limit"]),
    (EffectiveSurfaceText(params={"expand.query": "no such tool"}), ["expand.query"]),
])
def test_an_unpublished_tool_or_argument_is_refused(effective: Any, unpublished: list[str]) -> None:
    with pytest.raises(SurfaceTextInvalid) as info:
        apply_tool_text(_fake_mcp(), effective)
    assert info.value.data["unpublished"] == unpublished


def test_the_digest_covers_instructions_and_fix_hints() -> None:
    mcp = _fake_mcp()
    one = surface_text_report(mcp, instructions="a", effective=NO_SURFACE_TEXT)["digest"]
    assert one == surface_text_report(mcp, instructions="a", effective=NO_SURFACE_TEXT)["digest"]
    assert one != surface_text_report(mcp, instructions="b", effective=NO_SURFACE_TEXT)["digest"]
    hinted = EffectiveSurfaceText(fix_hints={"node_not_found": "x"})
    assert one != surface_text_report(mcp, instructions="a", effective=hinted)["digest"]


# ── the real server, default branch vs a branch with a layer ─────────


SURFACE_TEXT_YAML = """\
tools:
  inspect: "Read one record (surface_text base)."
branches:
  trial:
    fix_hints:
      invalid_operator_arguments: "TRIAL: {default}"
    params:
      search.query: "Words to look for (surface_text trial)."
    instructions: trial-instructions.md
"""


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    json_client_sessions(monkeypatch)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _stage(tmp_path: Path, surface_text: str | None) -> Path:
    root = tmp_path / "deployments"
    shutil.copytree(FIXTURE, root / "live-http-ci")
    manifest = root / "live-http-ci" / "deployment.yaml"
    text = manifest.read_text(encoding="utf-8")
    declared = "  response_bytes:\n    ceiling: 32768\n"
    assert declared in text, "the fixture's response_bytes block moved"
    if surface_text is not None:
        manifest.write_text(
            text.replace(declared, declared + "  surface_text: surface_text.yaml\n"), encoding="utf-8",
        )
        (root / "live-http-ci" / "surface_text.yaml").write_text(surface_text, encoding="utf-8")
        (root / "live-http-ci" / "trial-instructions.md").write_text(
            "Trial branch guidance: search before you inspect.\n", encoding="utf-8",
        )
    return root


def _build(monkeypatch: pytest.MonkeyPatch, root: Path, branch: str) -> Any:
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {"dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"}},
    )
    return server_mod.build_server(dsn=PROBE_DSN, deployment_name="live-http-ci", branch_name=branch)


async def _served(mcp: Any) -> dict[str, Any]:
    tools = {tool.name: tool for tool in await mcp.list_tools()}
    result = await call_json(mcp, "search", {"query": "x", "max_bytes": "8192"})
    body = _json_payload(result)
    assert result.isError and body["error"]["code"] == "invalid_operator_arguments", body
    return {
        "inspect": tools["inspect"].description,
        "search.query": tools["search"].inputSchema["properties"]["query"].get("description"),
        "fix_hint": body["error"].get("fix_hint"),
        "instructions": mcp._mcp_server.instructions,
        "report": mcp._okg_surface_freshness["surface_text"],
    }


@pytest.mark.anyio
async def test_a_branch_serves_its_layer_and_default_serves_the_base(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """The #4280 done-when, in process: one changed hint (plus an argument
    description and the instructions) on branch `trial`; default unchanged;
    the two digests differ."""
    plain_root = _stage(tmp_path / "plain", None)
    root = _stage(tmp_path / "declared", SURFACE_TEXT_YAML)
    servers = {
        "plain": _build(monkeypatch, plain_root, "default"),
        "default": _build(monkeypatch, root, "default"),
        "trial": _build(monkeypatch, root, "trial"),
    }
    try:
        plain, default, trial = [await _served(servers[k]) for k in ("plain", "default", "trial")]
    finally:
        for mcp in servers.values():
            mcp._okg_pool.close()

    assert plain["report"]["declared"] is False
    assert default["inspect"] == trial["inspect"] == "Read one record (surface_text base)."
    assert plain["inspect"] != default["inspect"]
    # One changed hint: the branch serves the new text with the code's own hint kept.
    assert default["fix_hint"] == plain["fix_hint"] and default["fix_hint"]
    assert trial["fix_hint"] == f"TRIAL: {default['fix_hint']}"
    assert trial["search.query"] == "Words to look for (surface_text trial)."
    assert default["search.query"] == plain["search.query"]
    assert "Trial branch guidance" in trial["instructions"]
    assert "Trial branch guidance" not in default["instructions"]
    assert default["report"]["branch_layer"] is None and trial["report"]["branch_layer"] == "trial"
    assert trial["report"]["overridden"]["fix_hints"] == ["invalid_operator_arguments"]
    digests = {k: v["report"]["digest"] for k, v in (("plain", plain), ("default", default), ("trial", trial))}
    assert len(set(digests.values())) == 3, digests


@pytest.mark.anyio
@pytest.mark.parametrize("bad, fragment", [
    ("fix_hints:\n  invalid_operator_argument: x\n", "invalid_operator_argument"),
    ("tools:\n  map: x\n", "map"),
    ("branches:\n  trial:\n    instructions: missing.md\n", "missing.md"),
])
async def test_a_layer_naming_something_this_server_lacks_refuses_the_launch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, bad: str, fragment: str,
) -> None:
    root = _stage(tmp_path, bad)
    with pytest.raises(SurfaceTextInvalid) as info:
        _build(monkeypatch, root, "trial")
    assert fragment in info.value.message


# ── review fixes (S1-S6, N3, N4) ─────────────────────────────────────


@pytest.mark.anyio
@pytest.mark.parametrize("bad, fragment", [
    ("instructions: ../../outside.md\n", "outside the deployment directory"),
    ("instructions: /etc/hostname\n", "outside the deployment directory"),
    ("branches:\n  other:\n    instructions: ../escape.md\n", "outside the deployment directory"),
    # A mistake in ANOTHER branch's layer refuses the default launch too.
    ("branches:\n  other:\n    fix_hints:\n      no_such_code: x\n", "no_such_code"),
    ("branches:\n  other:\n    params:\n      search.nope: x\n", "search.nope"),
])
async def test_paths_stay_inside_and_every_layer_is_checked_on_every_launch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, bad: str, fragment: str,
) -> None:
    (tmp_path / "outside.md").write_text("secret", encoding="utf-8")
    root = _stage(tmp_path, bad)
    with pytest.raises(SurfaceTextInvalid) as info:
        _build(monkeypatch, root, "default")
    assert fragment in info.value.message


@pytest.mark.anyio
async def test_a_layer_instructions_file_naming_an_unserved_tool_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from okg.substrate.mcp.effective_surface import SurfaceTextNamesUnservedOperator

    root = _stage(tmp_path, "instructions: bad.md\n")
    (root / "live-http-ci" / "bad.md").write_text("Tools:\n- `map`: map a set\n", encoding="utf-8")
    with pytest.raises(SurfaceTextNamesUnservedOperator):
        _build(monkeypatch, root, "default")


def test_a_dict_built_read_refusal_code_may_be_overridden() -> None:
    check_fix_hint_codes(EffectiveSurfaceText(fix_hints={
        "response_byte_budget_exceeded": "ask for fewer rows", "operator_execution_failed": "x",
    }))


@pytest.mark.anyio
async def test_an_override_never_pushes_a_refusal_over_its_byte_budget(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """S1: the override is applied before the last fit, so the budget holds."""
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES

    long_hint = "L" * 1_000 + " {default}"
    root = _stage(tmp_path, f'fix_hints:\n  invalid_operator_arguments: "{long_hint}"\n')
    mcp = _build(monkeypatch, root, "default")
    try:
        result = await call_json(
            mcp, "search", {"query": "x" * 3_000, "max_bytes": "4096"},
        )
        body = _json_payload(result)
        served = "".join(getattr(block, "text", "") for block in result.content)
    finally:
        mcp._okg_pool.close()
    assert result.isError and body["error"]["code"] == "invalid_operator_arguments", body
    assert len(served.encode("utf-8")) <= max(MIN_RESPONSE_BYTES, body["error"].get("max_bytes") or 0), (
        len(served.encode("utf-8")), body["error"].get("max_bytes"),
    )


@pytest.mark.anyio
async def test_an_empty_override_drops_the_hint_on_the_wire(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    root = _stage(tmp_path, 'fix_hints:\n  invalid_operator_arguments: ""\n')
    mcp = _build(monkeypatch, root, "default")
    try:
        body = _json_payload(await call_json(mcp, "search", {"query": "x", "max_bytes": "8192"}))
    finally:
        mcp._okg_pool.close()
    assert body["error"]["code"] == "invalid_operator_arguments"
    assert "fix_hint" not in body["error"], body


@pytest.mark.anyio
async def test_run_refusals_get_the_override_too(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """S4: `run` builds its own refusals outside the serving exit."""
    from okg.substrate.mcp import surface_text as st
    from okg.substrate.mcp.run_tool import _refusal

    token = st.CURRENT.set(EffectiveSurfaceText(fix_hints={"invalid_operator_arguments": "RUN: {default}"}))
    try:
        served = _refusal("invalid_operator_arguments", "bad steps", "fix the steps")
    finally:
        st.CURRENT.reset(token)
    text = "".join(getattr(block, "text", "") for block in served.content)
    assert "RUN: fix the steps" in text
    plain = _refusal("invalid_operator_arguments", "bad steps", "fix the steps")
    assert "RUN:" not in "".join(getattr(block, "text", "") for block in plain.content)


@pytest.mark.anyio
async def test_the_doctor_expects_the_layer_instructions_and_caller_text_wins(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """S2: the fleet doctor's expectation applies the default branch's layer,
    so a healthy server using surface_text instructions is not reported stale.
    N3: a caller's own instructions win, and the report says the layer's were
    not applied."""
    from okg.substrate.mcp.surface_fleet import expected_surface

    root = _stage(tmp_path, "instructions: base-instructions.md\n")
    (root / "live-http-ci" / "base-instructions.md").write_text(
        "Base layer guidance for every session.\n", encoding="utf-8",
    )
    mcp = _build(monkeypatch, root, "default")
    try:
        assert "Base layer guidance" in mcp._mcp_server.instructions
        report = mcp._okg_surface_freshness["surface_text"]
        assert report["instructions_applied"] is True
        assert report["overridden"]["instructions"] == ["base-instructions.md"]
        assert expected_surface("live-http-ci")["instructions_hash"] == (
            mcp._okg_surface_freshness["authored_instructions_hash"]
        )
    finally:
        mcp._okg_pool.close()

    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    own = server_mod.build_server(
        dsn=PROBE_DSN, deployment_name="live-http-ci", instructions="Caller text only.",
    )
    try:
        report = own._okg_surface_freshness["surface_text"]
        assert report["instructions_applied"] is False and report["overridden"]["instructions"] == []
    finally:
        own._okg_pool.close()
