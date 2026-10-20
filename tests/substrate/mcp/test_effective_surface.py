"""The one declared agent surface, and the refusals that keep it true.

PACT `effective-surface-declaration` (issue #2012). Each test here pins a
refusal that used to be a silent narrowing, a warning, or nothing at all.

The two real files are in the suite on purpose. A scan rule that refuses a
correct instructions file is worse than no scan, and
`deployments/okg-ci/mcp-instructions.md` is the file that catches it: it says
"do not use `query`" — exactly what a good instructions file says about a
tool its launch withholds — and an earlier draft of this rule refused it for
saying so.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.external_backed.live_relations import (
    LiveRelationAdapterKindUnknown,
)
from okg.substrate.mcp.effective_surface import (
    AgentSurfaceConflictsWithMcpBlock,
    AgentSurfaceInvalid,
    AgentSurfaceLegacyKeyPresent,
    AgentSurfaceProjectionWiderThanServed,
    AgentSurfaceResponseBytesOutOfRange,
    InspectKindNotServed,
    LiveBindingsNotImplemented,
    READ_INSPECT_KINDS,
    ServedSetMismatch,
    SurfaceTextNamesUnservedOperator,
    assert_inspect_kind_served,
    assert_served_sets_equal,
    assert_surface_files_agree,
    assert_text_serves_only,
    parse_agent_surface,
    scan_tool_mentions,
    served_inspect_kinds,
)
from tests.mirror_scope import private_path
from tests.private_deployments import names
from okg.substrate.mcp.operator_models import INSPECT_TARGET_KINDS
from okg.substrate.mcp.operators import (
    AKMON_READ_TOOLS,
    OPERATOR_TOOLS,
    WRITE_ADMIN_TOOLS,
)
from tests.substrate.mcp._result_payload import (
    call_json,
    json_client_sessions,
    payload as _json_payload,
)


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)


REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
READ_SIX = ["inspect", "search", "expand", "filter", "map", "aggregate"]


def _block(**over):
    body = {"served": list(READ_SIX)}
    body.update(over)
    return {"agent_surface": body}


# ── the block ──────────────────────────────────────────────────────────

def test_absent_block_is_none_not_an_empty_surface() -> None:
    """The block is OPTIONAL this change (issue #2067 makes it mandatory).

    `None` and "declared, empty" must never collapse: the first keeps the
    frozen legacy path, the second would serve nothing.
    """
    assert parse_agent_surface({"name": "x"}) is None


def test_the_block_parses_and_carries_what_derives_from_it() -> None:
    surface = parse_agent_surface(_block(
        response_bytes={"default": 32768, "ceiling": 131072},
        projections={"chat": {"tools": ["inspect", "search"]}},
    ))
    assert surface is not None
    assert surface.served == tuple(READ_SIX)
    assert surface.projection("chat").tools == ("inspect", "search")
    assert surface.effective_response_default == 32768
    assert surface.effective_response_ceiling == 131072


def test_a_projection_wider_than_served_is_refused_naming_the_extra() -> None:
    """A projection only NARROWS.

    Widening would bind a client to tools the server never registers, so
    every call would be refused at the wire with nothing in the manifest to
    explain it.
    """
    with pytest.raises(AgentSurfaceProjectionWiderThanServed) as excinfo:
        parse_agent_surface(_block(
            projections={"chat": {"tools": [*READ_SIX, "query"]}},
        ))
    assert excinfo.value.data["wider_than_served"] == ["query"]
    assert "query" in str(excinfo.value)


def test_an_unknown_nested_key_is_refused() -> None:
    """The top-level census matches only top-level NAMES.

    Non-vacuity partner for the block: without this, `serverd:` would load
    clean and bound nothing.
    """
    with pytest.raises(AgentSurfaceInvalid) as excinfo:
        parse_agent_surface({"agent_surface": {"served": READ_SIX, "serverd": []}})
    assert "serverd" in str(excinfo.value)


@pytest.mark.parametrize("legacy,code", [
    ("mcp_instructions", "agent_surface_legacy_key_present"),
    ("tool_description_overrides", "agent_surface_legacy_key_present"),
    ("mcp", "agent_surface_conflicts_with_mcp_block"),
])
def test_a_legacy_key_beside_the_block_is_refused(legacy: str, code: str) -> None:
    """The frozen path and the block cannot coexist in ONE manifest.

    This is what bounds option B: "declares no block" is the whole of the
    legacy population, so issue #2067 has a finite thing to delete.

    The `mcp:` case needs a synthetic manifest — no in-repo deployment
    declares a top-level `mcp:` block, so nothing real exercises it.
    """
    raw = _block()
    raw[legacy] = "something" if legacy != "mcp" else {"tool_profiles": {}}
    with pytest.raises(AgentSurfaceLegacyKeyPresent) as excinfo:
        parse_agent_surface(raw)
    assert excinfo.value.code == code
    if legacy == "mcp":
        assert isinstance(excinfo.value, AgentSurfaceConflictsWithMcpBlock)


def test_a_well_formed_live_binding_now_parses() -> None:
    """#2075 refused EVERY non-empty binding; #2013 dispatches them.

    The refusal was narrowed, not deleted -- see the test below. What this
    one pins is that the narrowing really happened: a binding naming an
    adapter kind this substrate runs is carried on the declaration.
    """
    surface = parse_agent_surface(_block(live_bindings={
        "hits": {"kind": "http", "config": {"url": "http://localhost"}},
    }))
    assert surface is not None
    assert set(surface.live_bindings) == {"hits"}
    assert surface.live_bindings["hits"].kind == "http"


def test_a_binding_with_an_unknown_adapter_kind_is_still_refused() -> None:
    """The #2075 posture survives: nothing is accepted-but-inert.

    `opensearch` is not an adapter kind this substrate runs, so the binding
    could never dispatch. Parsing and ignoring it would leave the deployment
    believing it had bound a live relation, which is exactly what
    `live_bindings_not_implemented` was protecting against.
    """
    with pytest.raises(LiveRelationAdapterKindUnknown):
        parse_agent_surface(_block(live_bindings={"hits": {"kind": "opensearch"}}))


def test_an_empty_live_bindings_slot_is_accepted() -> None:
    """Non-vacuity partner: the slot exists, it is just empty."""
    surface = parse_agent_surface(_block(live_bindings={}))
    assert surface is not None
    assert surface.live_bindings == {}


def test_the_retired_reservation_error_still_resolves_to_its_code() -> None:
    """#2075's `live_bindings_not_implemented` is kept, unraised.

    Nothing raises it any more -- #2013 implements the dispatch the error
    said was missing. It is not deleted because that code string may sit in
    an operator's notes or in a logged refusal from a #2075-era deployment,
    and a code that resolves to nothing is worse than one that resolves to an
    explanation of what replaced it.

    This test is also what keeps the import above honest: an unreferenced
    retirement is indistinguishable from a forgotten one.
    """
    assert LiveBindingsNotImplemented.code == "live_bindings_not_implemented"
    assert issubclass(LiveBindingsNotImplemented, AgentSurfaceInvalid)
    assert "#2013" in (LiveBindingsNotImplemented.__doc__ or "")


@pytest.mark.parametrize("body,why", [
    ({"ceiling": 1024}, "below the substrate minimum"),
    ({"ceiling": 512 * 1024}, "above the substrate maximum"),
    ({"default": 1024}, "default below the minimum"),
    ({"default": 200_000, "ceiling": 131072}, "default above the ceiling"),
])
def test_a_response_budget_outside_the_substrate_bounds_is_refused(
    body: dict, why: str,
) -> None:
    """Refused at LOAD, not clamped at call time.

    A deployment ceiling may only lower the substrate maximum. Accepting a
    higher one would let a manifest grant a budget the substrate does not
    honor, and the operator would read the manifest and believe it.
    """
    with pytest.raises(AgentSurfaceResponseBytesOutOfRange):
        parse_agent_surface(_block(response_bytes=body))


def test_a_budget_inside_the_bounds_is_accepted() -> None:
    surface = parse_agent_surface(_block(
        response_bytes={"default": 32768, "ceiling": 131072},
    ))
    assert surface.effective_response_ceiling == 131072


# ── inspect kinds ──────────────────────────────────────────────────────

def test_every_inspect_kind_is_classified() -> None:
    """An unclassified kind would be routed and published nowhere."""
    from okg.substrate.mcp.effective_surface import (
        CEILING_INSPECT_KINDS,
        IMAGE_INSPECT_KINDS,
        RECORD_INSPECT_KINDS,
        WRITE_ADMIN_INSPECT_KINDS,
    )

    classified = (
        READ_INSPECT_KINDS | WRITE_ADMIN_INSPECT_KINDS
        | RECORD_INSPECT_KINDS | CEILING_INSPECT_KINDS
        | IMAGE_INSPECT_KINDS
    )
    assert classified == set(INSPECT_TARGET_KINDS)


def test_a_read_surface_publishes_read_kinds_only() -> None:
    kinds = served_inspect_kinds(AKMON_READ_TOOLS, allow_writes=False)
    # `surface` is served read-only on every launch (#3666, M10).
    assert kinds == READ_INSPECT_KINDS | {"surface"}
    for withheld in ("admission", "record", "alias_suggestions",
                     "record_prefixes", "record_templates"):
        assert withheld not in kinds


def test_the_record_kinds_follow_the_record_TOOL_not_a_profile_name() -> None:
    """Derived condition, not a profile name.

    `served` is a deployment-declared list now, so a deployment can serve
    `record` without being "the write profile". Keying on the profile name
    would answer the wrong question.
    """
    without = served_inspect_kinds(
        [*AKMON_READ_TOOLS, "propose"], allow_writes=True,
    )
    with_record = served_inspect_kinds(
        [*AKMON_READ_TOOLS, "record"], allow_writes=True,
    )
    assert "record_templates" not in without
    assert "record_prefixes" not in without
    assert {"record_templates", "record_prefixes"} <= with_record


def test_the_surface_kind_is_served_on_every_launch() -> None:
    """`surface` lists, read-only, the forms a launch accepts and does not
    publish (#3666, M10), so every launch serves it. Expanding a session
    (`action="expand"`) still needs a ceiling and is refused without one
    (test_lean_read_surface.py)."""
    assert "surface" in served_inspect_kinds(
        AKMON_READ_TOOLS, allow_writes=False,
    )
    assert "surface" in served_inspect_kinds(
        AKMON_READ_TOOLS, allow_writes=False, has_expansion_ceiling=True,
    )
    assert "surface" in served_inspect_kinds(
        [*AKMON_READ_TOOLS, *WRITE_ADMIN_TOOLS], allow_writes=True,
    )


def test_a_write_launch_serves_every_kind() -> None:
    """The control for the byte work: the write surface must not move.

    Every kind but `image`, which follows a declared image source rather
    than the launch (#3046); with one, a write launch serves every kind."""
    from okg.substrate.mcp.effective_surface import IMAGE_INSPECT_KINDS

    tools = [*AKMON_READ_TOOLS, *WRITE_ADMIN_TOOLS]
    kinds = served_inspect_kinds(tools, allow_writes=True)
    assert kinds == set(INSPECT_TARGET_KINDS) - IMAGE_INSPECT_KINDS
    assert served_inspect_kinds(
        tools, allow_writes=True, has_image_source=True,
    ) == set(INSPECT_TARGET_KINDS)


def test_the_image_kind_follows_the_image_source_not_the_launch() -> None:
    """#3046: no image source, no image kind -- on any launch."""
    read = served_inspect_kinds(AKMON_READ_TOOLS, allow_writes=False)
    assert "image" not in read
    with_images = served_inspect_kinds(
        AKMON_READ_TOOLS, allow_writes=False, has_image_source=True,
    )
    assert with_images == read | {"image"}
    with pytest.raises(InspectKindNotServed) as excinfo:
        assert_inspect_kind_served("image", served_kinds=read)
    assert excinfo.value.data["known_kind"] is True
    assert "image source" in excinfo.value.data["serves_it"]


def test_an_unserved_kind_is_refused_by_NAME_with_what_would_serve_it() -> None:
    """Unit level. The REGISTERED call path is pinned separately, below,
    because this one passes with the router guard deleted.

    Not a schema rejection.

    A caller has to tell a kind that does not exist from one this launch
    withholds, because the fix differs: the first is a typo, the second is a
    relaunch.
    """
    served = served_inspect_kinds(AKMON_READ_TOOLS, allow_writes=False)
    with pytest.raises(InspectKindNotServed) as excinfo:
        assert_inspect_kind_served("admission", served_kinds=served)
    assert excinfo.value.code == "agent_surface_inspect_kind_unserved"
    assert "--allow-writes" in str(excinfo.value)
    assert excinfo.value.data["known_kind"] is True


def test_an_unknown_kind_is_told_apart_from_a_withheld_one() -> None:
    served = served_inspect_kinds(AKMON_READ_TOOLS, allow_writes=False)
    with pytest.raises(InspectKindNotServed) as excinfo:
        assert_inspect_kind_served("addmission", served_kinds=served)
    assert excinfo.value.data["known_kind"] is False


def test_an_attr_alias_is_served_wherever_attr_is() -> None:
    """`attribute` and `attribute_provenance` are spellings, not kinds."""
    served = served_inspect_kinds(AKMON_READ_TOOLS, allow_writes=False)
    for spelling in ("attr", "attribute", "attribute_provenance"):
        assert_inspect_kind_served(spelling, served_kinds=served)


# ── the affirmative text scan ──────────────────────────────────────────

def test_the_two_real_instruction_files_pass() -> None:
    """The rule has to hold against what is actually written today.

    okg-ci runs a write/admin launch and serves no `query`; its instructions
    name `query` exactly once, in a do-not-use line.
    """
    ci_served = set(AKMON_READ_TOOLS) | set(WRITE_ADMIN_TOOLS)
    assert_text_serves_only(
        (REPO_ROOT / "deployments/okg-ci/mcp-instructions.md").read_text(),
        served=ci_served, source="mcp-instructions.md", deployment="okg-ci",
    )
    assert_text_serves_only(
        (REPO_ROOT
         / "deployments/okg-workspace/chat/system_prompt.md").read_text(),
        served=READ_SIX, source="system_prompt.md",
        deployment="okg-workspace",
    )


def test_the_okg_ci_do_not_use_line_is_seen_and_not_refused() -> None:
    """Non-vacuity partner for the test above.

    A rule that never noticed `query` at all would pass it too.
    """
    text = private_path("deployments/okg-ci/mcp-instructions.md").read_text()
    query = [m for m in scan_tool_mentions(text) if m.tool == "query"]
    assert query, "the scan must SEE the name it does not refuse"
    assert not any(m.refusable for m in query)


def test_a_do_not_use_line_is_seen_and_not_refused() -> None:
    """The same non-vacuity check on a synthetic line, which the public copy runs."""
    text = "Under Nomos enforcement, do not use `query` or graph-backed `run_sql`: arbitrary\n"
    query = [m for m in scan_tool_mentions(text) if m.tool == "query"]
    assert query, "the scan must SEE the name it does not refuse"
    assert not any(m.refusable for m in query)


def test_a_served_tool_is_never_refused_however_it_is_written() -> None:
    assert_text_serves_only(
        "- `search` — find nodes.\nCall `inspect(target={...})` next.",
        served=READ_SIX, source="prompt.md", deployment="d",
    )


def test_the_workspace_deployment_files_agree_with_its_block() -> None:
    """End to end on the real manifest, with no server running."""
    from okg.substrate.deployments import DeploymentManifest

    manifest = DeploymentManifest.from_yaml(
        REPO_ROOT / "deployments/okg-workspace/deployment.yaml",
    )
    assert manifest.agent_surface is not None
    assert_surface_files_agree(
        manifest.agent_surface,
        deployment="okg-workspace",
        deployment_dir=REPO_ROOT / "deployments/okg-workspace",
    )


def test_the_workspace_block_keeps_query_on_stdio_and_narrows_only_chat() -> None:
    """Operator decision, 2026-09-20.

    Coding agents reach this deployment over stdio and use the bounded SQL
    escape hatch; the chat client may not, because Nomos masking does not
    cover query output. One block states both.
    """
    from okg.substrate.deployments import DeploymentManifest

    surface = DeploymentManifest.from_yaml(
        REPO_ROOT / "deployments/okg-workspace/deployment.yaml",
    ).agent_surface
    assert "query" in surface.served
    assert "query" not in surface.projection("chat").tools


# ── served-set equality ────────────────────────────────────────────────

def test_a_superset_listing_is_refused_not_accepted() -> None:
    """THE defect this PACT exists for.

    The pre-existing check in chat/sync.py asserts the live listing is a
    SUPERSET of the bound tools, so a server serving seven while the
    deployment declares six passes it — which was okg-workspace's measured
    state.
    """
    with pytest.raises(ServedSetMismatch) as excinfo:
        assert_served_sets_equal(
            declared=READ_SIX,
            observed=[*READ_SIX, "query"],
            deployment="okg-workspace",
            endpoint="http://127.0.0.1:8100/mcp",
            allow_writes=False,
        )
    assert excinfo.value.data["listed_not_served"] == ["query"]
    assert excinfo.value.data["served_not_listed"] == []


def test_a_missing_tool_is_refused_and_named_on_the_other_side() -> None:
    with pytest.raises(ServedSetMismatch) as excinfo:
        assert_served_sets_equal(
            declared=[*READ_SIX, "query"], observed=READ_SIX,
            deployment="d", endpoint="e", allow_writes=False,
        )
    assert excinfo.value.data["served_not_listed"] == ["query"]


def test_equal_sets_pass() -> None:
    assert_served_sets_equal(
        declared=READ_SIX, observed=list(reversed(READ_SIX)),
        deployment="d", endpoint="e", allow_writes=False,
    )


# ── the response byte budget at runtime ────────────────────────────────

def _budgeted(default=None, ceiling=None):
    from okg.substrate.mcp.response_bounds import bound_read_tool

    seen: dict[str, object] = {}

    def search(query: str = "x", limit: int = 10, max_bytes: int | None = None):
        seen["max_bytes"] = max_bytes
        return {"operator_name": "search", "items": [], "limit": limit}

    return bound_read_tool(
        "search", search,
        deployment_default_bytes=default,
        deployment_ceiling_bytes=ceiling,
    ), seen


def test_the_deployment_default_applies_when_the_caller_omits_max_bytes() -> None:
    """Applied at CALL time, never written into the signature.

    The signature is what publishes the `max_bytes` default in `tools/list`,
    so a per-deployment value there would make every byte baseline depend on
    which deployment was loaded.
    """
    from okg.substrate.mcp.response_bounds import DEFAULT_RESPONSE_BYTES

    tool, seen = _budgeted(default=32768)
    out = tool(query="x")
    assert out["operator_name"] == "search"
    # The operator receives the RESOLVED budget, not the raw argument, so
    # anything inside it that sizes work against `max_bytes` uses the same
    # number the wrapper enforces (issue #2156).
    assert seen["max_bytes"] == 32768
    assert DEFAULT_RESPONSE_BYTES == 64 * 1024


def test_a_caller_over_the_deployment_ceiling_is_refused_not_clamped() -> None:
    """Refused by name, with the manifest key that set the ceiling.

    A clamp would answer a question the caller did not ask, and give no
    signal that the budget it reasoned about was not the budget applied.
    """
    tool, _ = _budgeted(ceiling=131072)
    out = tool(query="x", max_bytes=200_000)
    assert out["error"]["code"] == "invalid_response_byte_budget"
    assert "agent_surface.response_bytes.ceiling" in out["error"]["message"]


def test_a_caller_inside_the_deployment_ceiling_is_served() -> None:
    """Non-vacuity partner: the ceiling refuses a value, not every value."""
    tool, _ = _budgeted(ceiling=131072)
    assert "error" not in tool(query="x", max_bytes=100_000)


def test_an_explicit_max_bytes_is_never_overridden_by_the_default() -> None:
    """The deployment default fills an ABSENCE, it does not outrank a caller."""
    tool, _ = _budgeted(default=8192)
    assert "error" not in tool(query="x", max_bytes=64 * 1024)


# ── served is the whole surface (operator decision (a), 2026-09-20) ────

def test_a_write_launch_against_a_read_only_declaration_is_REFUSED() -> None:
    """A named launch-time refusal, not a silently narrower start.

    Chosen over the alternative — start read-only and say nothing — because
    it is what this area already does one level up: a `--tools`
    naming an unserved tool raises rather than narrowing, and an unreachable
    expansion ceiling raises rather than advertising something it cannot
    honor. The operator asked for a write surface and would have got a read
    one with no signal that the flag did nothing.
    """
    from okg.substrate.mcp.effective_surface import (
        WriteLaunchUnservedByDeclaration,
        assert_write_launch_is_served,
    )

    surface = parse_agent_surface(_block())
    with pytest.raises(WriteLaunchUnservedByDeclaration) as excinfo:
        assert_write_launch_is_served(
            surface, allow_writes=True, deployment="read-only-fixture",
        )
    assert excinfo.value.code == "agent_surface_write_launch_unserved"
    # The fix names the tools that would satisfy the flag, so the operator
    # does not have to go find the write/admin set themselves.
    assert "record" in excinfo.value.fix_hint
    assert "record" in excinfo.value.data["write_admin_tools"]


def test_a_read_launch_against_a_read_only_declaration_is_fine() -> None:
    """Non-vacuity partner. `served` without write tools is a normal
    deployment; it is the COMBINATION with the flag that cannot mean
    anything, which is why this is a launch refusal and not a load one."""
    from okg.substrate.mcp.effective_surface import assert_write_launch_is_served

    assert_write_launch_is_served(
        parse_agent_surface(_block()), allow_writes=False, deployment="d",
    )


def test_a_write_launch_against_a_declaration_that_serves_writes_is_fine() -> None:
    from okg.substrate.mcp.effective_surface import assert_write_launch_is_served

    surface = parse_agent_surface(_block(served=[*READ_SIX, "record"]))
    assert_write_launch_is_served(
        surface, allow_writes=True, deployment="d",
    )


def test_a_block_less_deployment_is_untouched_by_the_write_check() -> None:
    """Option B: no block means the frozen legacy path, unchanged."""
    from okg.substrate.mcp.effective_surface import assert_write_launch_is_served

    assert_write_launch_is_served(None, allow_writes=True, deployment="d")


def test_the_workspace_block_declares_every_tool_it_serves() -> None:
    """Decision (a): `served` bounds every launch mode.

    A tool missing here is a tool no launch of this deployment can ever
    serve, so the block has to name all eighteen — exactly what
    `--allow-writes` registered before the block existed.
    """
    from okg.substrate.deployments import DeploymentManifest
    from okg.substrate.mcp.operators import OPERATOR_TOOLS, WRITE_ADMIN_TOOLS

    surface = DeploymentManifest.from_yaml(
        REPO_ROOT / "deployments/okg-workspace/deployment.yaml",
    ).agent_surface
    assert set(surface.served) == set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS)
    assert len(surface.served) == 18


# ── the guard through the REAL call path ──────────────────────────────

@pytest.fixture
def _probe(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    # `build_server` admits the deployment's source registry through a
    # process-wide cache keyed by file and mtime only. An admission made here
    # (with this process's environment) would be served to a later test that
    # loads the same registry under a different environment, e.g.
    # tests/okg-ci/test_sealed_environment_topology.py. Give each probe its
    # own cache and restore the original afterwards.
    from okg.substrate.sources import registry as registry_mod

    monkeypatch.setattr(registry_mod, "_REGISTRY_CACHE", {})
    return PROBE_DSN


async def _call_inspect(mcp, kind: str):
    result = await call_json(mcp, "inspect", {"target": {"kind": kind}})
    return _json_payload(result)


@pytest.mark.anyio
async def test_the_named_refusal_reaches_the_CLIENT(_probe) -> None:
    """The regression this test exists for, and it is not hypothetical.

    The first implementation put the narrowed union on the tool SIGNATURE, so
    pydantic rejected an unserved kind before the body ran and the client got
    `invalid_operator_arguments` with `union_tag_invalid` — a schema-shaped
    message that cannot tell a kind that does not exist from one this launch
    withholds. `assert_inspect_kind_served` was unreachable dead code, and
    every unit test of it passed anyway because they call the helper
    directly.

    So this goes through `call_tool`, the path a real client takes.
    """
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=_probe)
    try:
        payload = await _call_inspect(mcp, "admission")
    finally:
        mcp._okg_pool.close()
    assert payload["error"]["code"] == "agent_surface_inspect_kind_unserved"
    assert "--allow-writes" in payload["error"]["message"]


@pytest.mark.anyio
async def test_an_unknown_kind_gets_the_named_refusal_at_the_CLIENT(
    _probe,
) -> None:
    """A kind that is in no inspect union gets the same named refusal as a
    withheld one, over `call_tool` (Jason 179a).

    It never reaches the router guard: the full-union validator refuses it
    first. Before 179a that refusal was `invalid_operator_arguments` with
    `union_tag_invalid`; the validation seam now raises the guard's own
    refusal instead, with the same served set and `known_kind` false.
    """
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=_probe)
    try:
        payload = await _call_inspect(mcp, "nope")
        served = sorted(mcp._okg_inspect_served_kinds)
    finally:
        mcp._okg_pool.close()
    error = payload["error"]
    assert error["code"] == "agent_surface_inspect_kind_unserved"
    assert error["data"]["kind"] == "nope"
    assert error["data"]["known_kind"] is False
    assert error["data"]["serves_it"] is None
    assert error["data"]["served_kinds"] == served
    assert "admission" not in served
    assert "not an inspect target kind at all" in error["message"]


@pytest.mark.anyio
async def test_a_write_launch_routes_the_same_kind(_probe) -> None:
    """Non-vacuity partner: the guard refuses because the kind is withheld
    HERE, not because it is refused everywhere."""
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=_probe, allow_writes=True)
    try:
        payload = await _call_inspect(mcp, "admission")
    finally:
        mcp._okg_pool.close()
        pool = getattr(mcp, "_okg_write_pool", None)
        if pool is not None:
            pool.close()
    # It reaches the body and fails on the unopened probe pool, which is
    # exactly what "served here" looks like without a database.
    assert payload.get("error", {}).get("code") != (
        "agent_surface_inspect_kind_unserved"
    )


@pytest.mark.anyio
async def test_the_published_schema_and_the_guard_agree(_probe) -> None:
    """The published schema is stricter than the validator. Every kind it
    lists is admitted by the guard; a kind the guard admits and the schema
    does not list is one the lean surface hides on purpose (#3666, M10:
    accepted, not shown), never one the launch withholds.

    Neither may be changed without the other, so this compares the kinds the
    schema lists against the kinds the guard admits, on one built server.
    """
    from okg.substrate.mcp.lean_surface import SHOWN_INSPECT_KINDS

    import json

    from okg.substrate.mcp.operator_models import INSPECT_KIND_ALIASES
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=_probe)
    try:
        listed: set[str] = set()
        for tool in await mcp.list_tools():
            if tool.name != "inspect":
                continue
            blob = json.dumps(tool.inputSchema)
            for kind in INSPECT_TARGET_KINDS:
                if f'"{kind}"' in blob:
                    listed.add(kind)
        for kind in INSPECT_TARGET_KINDS:
            folded = INSPECT_KIND_ALIASES.get(kind, kind)
            served = folded in listed
            try:
                assert_inspect_kind_served(
                    kind, served_kinds=served_inspect_kinds(
                        AKMON_READ_TOOLS, allow_writes=False,
                    ),
                )
                guarded = True
            except InspectKindNotServed:
                guarded = False
            if served:
                assert guarded, f"{kind!r}: published but the guard refuses it"
            else:
                assert not guarded or folded not in SHOWN_INSPECT_KINDS, (
                    f"{kind!r}: a shown kind the guard admits is not published"
                )
        assert listed and listed <= set(SHOWN_INSPECT_KINDS) | {
            k for k, v in INSPECT_KIND_ALIASES.items() if v in SHOWN_INSPECT_KINDS
        }
    finally:
        mcp._okg_pool.close()


# ── the response budget, ceiling-only ─────────────────────────────────

def test_a_ceiling_only_manifest_lowers_the_default_rather_than_refusing() -> None:
    """A manifest declaring only a ceiling must not refuse every default call.

    The signature default is 65 536 B. Passing the RAW declaration through
    left the default unset, so a 32 KiB ceiling refused every call that
    omitted `max_bytes` — the deployment would have looked broken rather than
    bounded.
    """
    surface = parse_agent_surface(_block(response_bytes={"ceiling": 32768}))
    assert surface.response_default_bytes is None
    assert surface.effective_response_default == 32768
    assert surface.effective_response_ceiling == 32768


def test_a_ceiling_only_surface_serves_a_defaulted_call() -> None:
    from okg.substrate.mcp.response_bounds import bound_read_tool

    surface = parse_agent_surface(_block(response_bytes={"ceiling": 32768}))

    def search(query: str = "x", limit: int = 10, max_bytes: int | None = None):
        return {"operator_name": "search", "items": [], "limit": limit}

    tool = bound_read_tool(
        "search", search,
        deployment_default_bytes=surface.effective_response_default,
        deployment_ceiling_bytes=surface.effective_response_ceiling,
    )
    assert "error" not in tool(query="x")


# ── refusal positions, and the negation grammar inside them ──────────
#
# Six sentences from the verification review. The first four are PROSE: a
# backticked name in the middle of a sentence is a warning, never a refusal,
# because refusing it means refusing ordinary English about the surface. The
# last two sit in refusal positions and carry a word that looks like a
# negation but governs something else.

@pytest.mark.parametrize("text", [
    "This surface does not serve `query`.",
    "`query` is not served.",
    "There is no `query` tool.",
    "Use `search` instead of `query`.",
])
def test_prose_naming_an_unserved_tool_is_a_warning_not_a_refusal(
    text: str,
) -> None:
    """All four refused under the previous rule. None should.

    Refusing prose teaches an author to write around the checker, which
    leaves a check that is both annoying and untrue.
    """
    assert_text_serves_only(
        text, served=READ_SIX, source="prompt.md", deployment="d",
    )
    assert [m.position for m in scan_tool_mentions(text) if m.tool == "query"] \
        == ["mention"]


@pytest.mark.parametrize("text", [
    "Do not hesitate to use `query(sql=...)`.",
    "You cannot avoid using `query(sql=...)`.",
])
def test_a_near_miss_negation_in_a_refusal_position_still_refuses(
    text: str,
) -> None:
    """Both were exempt under the previous rule, and both say to call it.

    "do not" governs "hesitate", not "use"; "cannot" governs "avoid". The
    grammar requires the negator to sit directly in front of the calling
    verb, which is what a real prohibition looks like.
    """
    with pytest.raises(SurfaceTextNamesUnservedOperator):
        assert_text_serves_only(
            text, served=READ_SIX, source="prompt.md", deployment="d",
        )


@pytest.mark.parametrize("text", [
    "Never call `query(sql=...)`.",
    "- Do not use `query(sql=...)` on this deployment.",
])
def test_a_real_prohibition_in_a_refusal_position_is_exempt(text: str) -> None:
    """The prohibition must PRECEDE the name, in the same clause.

    A bulleted item whose name comes first still refuses, and should: the
    name heads a tool listing, which is what a model reads as "these are your
    tools", whatever the sentence goes on to say afterwards.
    """
    assert_text_serves_only(
        text, served=READ_SIX, source="prompt.md", deployment="d",
    )


@pytest.mark.parametrize("text,position", [
    ("- `query` — run bounded SQL.", "list_head"),
    ("Escape with `query(sql=...)` when the algebra runs out.", "call"),
])
def test_a_refusal_position_naming_an_unserved_tool_refuses(
    text: str, position: str,
) -> None:
    with pytest.raises(SurfaceTextNamesUnservedOperator) as excinfo:
        assert_text_serves_only(
            text, served=READ_SIX, source="prompt.md", deployment="d",
        )
    assert excinfo.value.data["mentions"][0]["position"] == position


# ── the lint path actually scans both files ───────────────────────────

def _lint_deployment_dir(tmp_path, *, prompt: str, instructions: str | None = None):
    """A minimal on-disk deployment that declares the block."""
    import yaml

    root = tmp_path / "deployments"
    dep = root / "fixture"
    (dep / "chat").mkdir(parents=True, exist_ok=True)
    (dep / "schemas").mkdir(parents=True, exist_ok=True)
    (dep / "chat" / "system_prompt.md").write_text(prompt, encoding="utf-8")
    surface = {
        "served": list(READ_SIX),
        "projections": {
            "chat": {
                "tools": list(READ_SIX),
                "system_prompt_ref": "chat/system_prompt.md",
            },
        },
    }
    if instructions is not None:
        (dep / "mcp-instructions.md").write_text(instructions, encoding="utf-8")
        surface["instructions"] = "mcp-instructions.md"
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "agent_surface": surface,
    }), encoding="utf-8")
    (dep / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    return root


def _surface_findings(report):
    return [
        f for f in report["findings"]
        if f["code"] == "deployment.agent_surface.text_names_unserved_operator"
    ]


def test_okg_deployments_lint_REFUSES_a_prompt_naming_an_unserved_tool(
    tmp_path,
) -> None:
    """The blocking finding of the verification review.

    `build_server` scans only the instructions file, deliberately — a chat
    prose edit must not refuse an unrelated stdio launch. That left the chat
    system prompt scanned by NOTHING, and `req.esd.drift-refused` unenforced
    for the only instruction channel a chat assistant has.
    """
    from okg.substrate.deployment_lint import lint_deployment

    root = _lint_deployment_dir(
        tmp_path, prompt="- `query` — run bounded SQL.",
    )
    findings = _surface_findings(lint_deployment("fixture", deployments_root=root))
    assert findings, "lint must refuse a prompt naming an unserved operator"
    assert findings[0]["severity"] == "blocker"
    assert findings[0]["details"]["unserved"] == ["query"]


def test_okg_deployments_lint_REFUSES_instructions_naming_an_unserved_tool(
    tmp_path,
) -> None:
    from okg.substrate.deployment_lint import lint_deployment

    root = _lint_deployment_dir(
        tmp_path,
        prompt="- `search` — find nodes.",
        instructions="- `query(sql=...)` — the escape hatch.",
    )
    findings = _surface_findings(lint_deployment("fixture", deployments_root=root))
    assert findings and findings[0]["details"]["unserved"] == ["query"]


def test_okg_deployments_lint_passes_text_naming_only_served_tools(
    tmp_path,
) -> None:
    """Non-vacuity partner: the check refuses a name, not every deployment."""
    from okg.substrate.deployment_lint import lint_deployment

    root = _lint_deployment_dir(
        tmp_path,
        prompt="- `search` — find nodes.\n- `inspect` — read one node.",
        instructions="Do not use `query` here.",
    )
    assert not _surface_findings(
        lint_deployment("fixture", deployments_root=root)
    )


# ── issue #2085: every offender, and no error read as clean ───────────

def test_okg_deployments_lint_reports_EVERY_offending_file_in_one_run(
    tmp_path,
) -> None:
    """Issue #2085. The check stopped at the first offending file, so a
    deployment whose instructions AND chat prompt both named an unserved
    tool needed two lint runs to see both."""
    from okg.substrate.deployment_lint import lint_deployment

    root = _lint_deployment_dir(
        tmp_path,
        prompt="- `query` — run bounded SQL.",
        instructions="- `query(sql=...)` — the escape hatch.",
    )
    findings = _surface_findings(lint_deployment("fixture", deployments_root=root))
    sources = sorted(f["details"]["source"] for f in findings)
    assert sources == [
        "the MCP instructions file mcp-instructions.md",
        "the chat system prompt chat/system_prompt.md",
    ], findings
    assert all(f["severity"] == "blocker" for f in findings)


def _rewrite_surface(root, **over) -> None:
    import yaml

    path = root / "fixture" / "deployment.yaml"
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    manifest["agent_surface"].update(over)
    path.write_text(yaml.safe_dump(manifest), encoding="utf-8")


def _blockers(report):
    return [f for f in report["findings"] if f["severity"] == "blocker"]


@pytest.mark.parametrize("over,code", [
    ({"bogus": 1}, "agent_surface_invalid"),
    ({"served": ["no_such_tool"]}, "agent_surface_invalid"),
    (
        {"live_bindings": {"hits": {"kind": "opensearch"}}},
        "live_relation_adapter_kind_unknown",
    ),
])
def test_okg_deployments_lint_names_a_malformed_block_ONCE(
    tmp_path, over, code,
) -> None:
    """Issue #2085. A block that does not parse used to return no finding
    from the agent-surface check, on the unverified assumption that another
    check reported it. The only other report was the manifest-model check's
    catch-all, which called it an unexpected error. Now the agent-surface
    check names it, by its refusal code, exactly once."""
    from okg.substrate.deployment_lint import lint_deployment

    root = _lint_deployment_dir(tmp_path, prompt="- `search` — find nodes.")
    _rewrite_surface(root, **over)
    blockers = _blockers(lint_deployment("fixture", deployments_root=root))
    invalid = [
        f for f in blockers if f["code"] == "deployment.agent_surface.invalid"
    ]
    assert len(invalid) == 1, blockers
    assert invalid[0]["details"]["error_code"] == code
    assert not [
        f for f in blockers
        if f["code"] == "deployment.manifest.model_load_error"
    ], "the same refusal must not be reported twice"


def _check_errors(report):
    return [
        f for f in report["findings"]
        if f["code"] == "deployment.agent_surface.check_error"
    ]


def test_an_unexpected_error_in_the_text_check_is_a_blocker_not_clean(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2085: an error that is not a surface refusal is reported as a
    blocker. Not a crash -- that would lose every other finding, and every
    other deployment's in a portfolio audit -- and not an empty list."""
    from okg.substrate import deployment_lint
    from okg.substrate.mcp import effective_surface

    def boom(*_a, **_k):
        raise RuntimeError("scanner broke")

    root = _lint_deployment_dir(
        tmp_path,
        prompt="- `search` — find nodes.",
        instructions="- `search` — find nodes.",
    )
    monkeypatch.setattr(effective_surface, "assert_text_serves_only", boom)
    errors = _check_errors(
        deployment_lint.lint_deployment("fixture", deployments_root=root)
    )
    # One per file: an error scanning one file does not skip the other.
    assert [e["severity"] for e in errors] == ["blocker", "blocker"], errors
    assert all("RuntimeError: scanner broke" in e["observed"] for e in errors)


def test_an_unexpected_error_parsing_the_block_is_a_blocker_not_clean(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_lint
    from okg.substrate.mcp import effective_surface

    def boom(*_a, **_k):
        raise RuntimeError("parser broke")

    root = _lint_deployment_dir(tmp_path, prompt="- `search` — find nodes.")
    monkeypatch.setattr(effective_surface, "parse_agent_surface", boom)
    errors = _check_errors(
        deployment_lint.lint_deployment("fixture", deployments_root=root)
    )
    assert len(errors) == 1 and errors[0]["severity"] == "blocker", errors
    assert "RuntimeError: parser broke" in errors[0]["observed"]


def _write_mixed_key_block(dep) -> None:
    """A real manifest the parser cannot handle: an integer key beside
    string keys makes the strict-key check's `sorted()` raise TypeError."""
    (dep / "deployment.yaml").write_text(
        "name: {name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "agent_surface: {{served: [search], 1: a, bogus: b}}\n".format(
            name=dep.name,
        ),
        encoding="utf-8",
    )


def test_a_block_the_parser_cannot_handle_is_a_blocker_not_a_crash(
    tmp_path,
) -> None:
    """Real input, no monkeypatch (review of #2326)."""
    from okg.substrate.deployment_lint import lint_deployment

    root = _lint_deployment_dir(tmp_path, prompt="- `search` — find nodes.")
    _write_mixed_key_block(root / "fixture")
    errors = _check_errors(lint_deployment("fixture", deployments_root=root))
    assert len(errors) == 1 and errors[0]["severity"] == "blocker", errors
    assert errors[0]["observed"].startswith("TypeError: ")


def test_a_chat_prompt_that_is_not_utf8_is_a_blocker_not_a_crash(
    tmp_path,
) -> None:
    """Real input, no monkeypatch (review of #2326). The instructions file
    still gets its own scan and its own result."""
    from okg.substrate.deployment_lint import lint_deployment

    root = _lint_deployment_dir(
        tmp_path,
        prompt="placeholder",
        instructions="- `query(sql=...)` — the escape hatch.",
    )
    (root / "fixture" / "chat" / "system_prompt.md").write_bytes(
        b"- `search` \xff\xfe not utf-8\n"
    )
    report = lint_deployment("fixture", deployments_root=root)
    errors = _check_errors(report)
    assert len(errors) == 1 and errors[0]["severity"] == "blocker", errors
    assert errors[0]["observed"].startswith("UnicodeDecodeError: ")
    assert [f["details"]["source"] for f in _surface_findings(report)] == [
        "the MCP instructions file mcp-instructions.md",
    ]


def test_a_portfolio_audit_reports_every_deployment_when_one_is_broken(
    tmp_path,
) -> None:
    """Real input (review of #2326). `okg deployments audit` lints each
    deployment in one loop with no per-deployment guard, so a crash in one
    deployment's lint lost every other deployment's findings."""
    import shutil

    from okg.substrate.deployment_contract import audit_deployment_portfolio

    root = _lint_deployment_dir(tmp_path, prompt="- `query` — run SQL.")
    shutil.copytree(root / "fixture", root / "broken")
    _write_mixed_key_block(root / "broken")
    report = audit_deployment_portfolio(root)
    by_name = {d["name"]: d for d in report["deployments"]}
    assert set(by_name) == {"fixture", "broken"}, sorted(by_name)
    codes = {
        name: {f["code"] for f in d["findings"]}
        for name, d in by_name.items()
    }
    assert any("text_names_unserved_operator" in c for c in codes["fixture"])
    assert any("agent_surface.check_error" in c for c in codes["broken"])


def test_the_real_workspace_deployment_lints_clean(tmp_path) -> None:
    from okg.substrate.deployment_lint import lint_deployment

    assert not _surface_findings(
        lint_deployment("okg-workspace", deployments_root=REPO_ROOT / "deployments")
    )


# ── registration really uses the shared function ──────────────────────

@pytest.mark.anyio
async def test_registration_matches_registered_tools_for_launch(_probe) -> None:
    """The post-condition the docstring claimed and the code did not make.

    `okg chat sync` compares a live tools/list against
    `registered_tools_for_launch`. If registration does not actually produce
    that set, sync is checking against a number nothing guarantees, and the
    two would agree only by coincidence.
    """
    from okg.substrate.deployments import DeploymentManifest
    from okg.substrate.mcp.effective_surface import registered_tools_for_launch
    from okg.substrate.mcp.server import build_server

    # kubernetes-smoke ships and declares the full served block (reads and
    # writes); okg-workspace, this repository's own deployment, does not ship.
    surface = DeploymentManifest.from_yaml(
        REPO_ROOT / "deployments/kubernetes-smoke/deployment.yaml",
    ).agent_surface
    assert surface is not None and "record" in surface.served

    for allow_writes in (False, True):
        mcp = build_server(
            dsn=_probe, deployment_name="kubernetes-smoke",
            allow_writes=allow_writes,
        )
        try:
            registered = set(mcp._tool_manager._tools)
        finally:
            mcp._okg_pool.close()
            pool = getattr(mcp, "_okg_write_pool", None)
            if pool is not None:
                pool.close()
        assert registered == set(registered_tools_for_launch(
            surface.served, allow_writes=allow_writes,
        ))


def test_an_unreachable_inspect_schema_RAISES_rather_than_no_ops() -> None:
    """Three silent returns became one named refusal.

    Every step reaches into MCP SDK internals. A rename in a dependency
    upgrade would otherwise make the narrowing a no-op: the server would
    publish every withheld kind while the router refused them, and the byte
    baselines would move with no explanation.
    """
    from okg.substrate.mcp.operator_models import (
        InspectSchemaUnreachable,
        publish_narrowed_inspect_target,
    )

    class _Renamed:
        pass

    with pytest.raises(InspectSchemaUnreachable) as excinfo:
        publish_narrowed_inspect_target(_Renamed(), frozenset({"graph"}))
    assert excinfo.value.code == "inspect_schema_unreachable"


# ── the in-repo migration (issue #2067, PACT agent-surface-migrate) ────

#: Every in-repo deployment that ran the frozen legacy surface before the
#: migration. `okg-workspace` and `opensearch-reference` already declared
#: the block and are not in this list.
MIGRATED_DEPLOYMENTS = (
    "beaver-okg",
    "consistency-fixture",
    "consistency-injection-fixture",
    *names("team_deployment"),
    "kubernetes-smoke",
    "mcp-two-principal-fixture",
    *names("retired_prototype"),
    "okg-ci",
    "okg-ci-dev",
    "synth-bench",
    "synthetic-corpus-smoke",
)

#: What a block-less launch registered: the seven read operators on a read
#: launch, and those plus every write and source-admin tool with
#: `--allow-writes`. No migrated deployment registers the conditional upload
#: tool. `served` must equal the write set, so no launch loses a tool.
_LEGACY_READ_LAUNCH = frozenset(OPERATOR_TOOLS)
_LEGACY_WRITE_LAUNCH = _LEGACY_READ_LAUNCH | frozenset(WRITE_ADMIN_TOOLS)


@pytest.mark.parametrize("name", MIGRATED_DEPLOYMENTS)
def test_every_in_repo_deployment_declares_the_block_serving_what_it_did(
    name: str,
) -> None:
    from okg.substrate.deployments import DeploymentManifest

    manifest = DeploymentManifest.from_yaml(
        REPO_ROOT / "deployments" / name / "deployment.yaml",
    )
    assert manifest.agent_surface is not None, name
    assert set(manifest.agent_surface.served) == _LEGACY_WRITE_LAUNCH
    # No budget or projection was declared: those would change behaviour.
    assert manifest.agent_surface.response_default_bytes is None
    assert manifest.agent_surface.response_ceiling_bytes is None
    assert dict(manifest.agent_surface.projections) == {}


@pytest.mark.parametrize("name", MIGRATED_DEPLOYMENTS)
def test_every_migrated_launch_registers_exactly_what_it_did(
    _probe, name: str,
) -> None:
    """The before/after proof, per deployment and per launch mode.

    One tool differs by decision, not drift (Jason, via orchestrator chat
    2026-10-13, query-by-default; issue #3666): a deployment whose access
    rules are on (Nomos `audit` or `enforce`) registers no `query`, since
    SQL cannot be filtered by them. The mode is read from the manifest here,
    so a deployment with the rules off still proves `query` is served.
    """
    from okg.substrate.mcp.server import build_server
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
        deployment_nomos_enforcement_mode,
    )

    # Read the real manifest, not a directory an earlier test memoized.
    _clear_nomos_enforcement_mode_cache()
    withheld = (
        frozenset() if deployment_nomos_enforcement_mode(name) == "off"
        else frozenset({"query"})
    )
    for allow_writes, expected in (
        (False, _LEGACY_READ_LAUNCH - withheld),
        (True, _LEGACY_WRITE_LAUNCH - withheld),
    ):
        mcp = build_server(
            dsn=_probe, deployment_name=name, allow_writes=allow_writes,
        )
        try:
            registered = set(mcp._tool_manager._tools)
        finally:
            mcp._okg_pool.close()
            pool = getattr(mcp, "_okg_write_pool", None)
            if pool is not None:
                pool.close()
        assert registered == expected, (name, allow_writes)


def test_an_unreadable_manifest_withholds_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed (issue #3666): a deployment.yaml that is present but
    does not parse gives no access-rule mode, so `query` is withheld from
    the launch, not served past rules that may apply. A readable manifest
    with no rules is the control: `query` is served."""
    from okg.substrate.mcp.effective_surface import (
        query_withheld_by_access_rules,
        registered_tools_for_launch,
    )
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
    )

    root = tmp_path / "deployments"
    (root / "qbroken").mkdir(parents=True)
    (root / "qbroken" / "deployment.yaml").write_text(
        "name: qbroken\nnomos: [unclosed\n", encoding="utf-8",
    )
    (root / "qopen").mkdir()
    (root / "qopen" / "deployment.yaml").write_text(
        "name: qopen\n", encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    _clear_nomos_enforcement_mode_cache()
    try:
        assert query_withheld_by_access_rules("qbroken") == "unreadable"
        assert "query" not in registered_tools_for_launch(
            OPERATOR_TOOLS, allow_writes=False, deployment="qbroken",
        )
        assert query_withheld_by_access_rules("qopen") is None
        assert "query" in registered_tools_for_launch(
            OPERATOR_TOOLS, allow_writes=False, deployment="qopen",
        )
    finally:
        _clear_nomos_enforcement_mode_cache()


def test_the_migrated_launches_cover_query_on_and_off() -> None:
    """Both arms of the query-by-default rule run above: some migrated
    deployments have their access rules off and serve `query`, some have
    them on and withhold it."""
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
        deployment_nomos_enforcement_mode,
    )

    _clear_nomos_enforcement_mode_cache()
    modes = {deployment_nomos_enforcement_mode(n) for n in MIGRATED_DEPLOYMENTS}
    assert "off" in modes
    assert modes & {"audit", "enforce"}


@pytest.mark.parametrize("name", ["okg-ci", "okg-ci-dev"])
def test_the_ci_instructions_are_still_served_after_the_move(
    _probe, name: str,
) -> None:
    """`mcp_instructions` moved to `agent_surface.instructions`. The server
    read its instructions only from the old key, so the move alone would
    have served none, with nothing said."""
    from okg.substrate.mcp.server import build_server

    expected = (
        REPO_ROOT / "deployments/okg-ci/mcp-instructions.md"
    ).read_text(encoding="utf-8").splitlines()[0]
    mcp = build_server(dsn=_probe, deployment_name=name)
    try:
        assert expected in (mcp.instructions or ""), name
    finally:
        mcp._okg_pool.close()


def _instruction_manifest(tmp_path: Path, surface: dict) -> Path:
    import yaml

    (tmp_path / "schemas").mkdir(exist_ok=True)
    (tmp_path / "source_registry.yaml").write_text(
        "sources: []\n", encoding="utf-8",
    )
    path = tmp_path / "deployment.yaml"
    path.write_text(yaml.safe_dump({
        "name": "fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "agent_surface": surface,
    }), encoding="utf-8")
    return path


def test_declared_surface_instructions_are_the_served_instructions(
    tmp_path,
) -> None:
    from okg.substrate.deployments import DeploymentManifest

    (tmp_path / "instructions.md").write_text("hi\n", encoding="utf-8")
    manifest = DeploymentManifest.from_yaml(_instruction_manifest(
        tmp_path, {"served": ["search"], "instructions": "instructions.md"},
    ))
    assert manifest.mcp_instructions == tmp_path / "instructions.md"


def test_declared_surface_instructions_that_do_not_exist_are_refused(
    tmp_path,
) -> None:
    """The same strictness the top-level key had: a typo must not silently
    serve no instructions."""
    from okg.substrate.deployments import (
        DeploymentConfigError,
        DeploymentManifest,
    )

    with pytest.raises(DeploymentConfigError, match="agent_surface.instructions"):
        DeploymentManifest.from_yaml(_instruction_manifest(
            tmp_path, {"served": ["search"], "instructions": "missing.md"},
        ))


def test_the_portfolio_audit_scans_the_declared_surface_instructions(
    tmp_path,
) -> None:
    """The audit's instruction scan read only the old key, so moving a
    deployment's instructions into the block would drop them from it."""
    from okg.substrate.deployment_contract import _instruction_paths

    (tmp_path / "docs").mkdir()
    target = tmp_path / "docs" / "agent.md"
    target.write_text("hi\n", encoding="utf-8")
    paths = _instruction_paths(
        {"agent_surface": {"served": ["search"], "instructions": "docs/agent.md"}},
        tmp_path,
    )
    assert target in paths


@pytest.mark.parametrize("name", ["okg-ci", "okg-ci-dev", *names("team_deployment")])
def test_the_manifest_and_the_definition_declare_the_same_surface(
    name: str,
) -> None:
    """These deployments carry the block twice: once in deployment.yaml, read
    by a flat launch, and once in definition.yaml, read by an environment.
    Nothing else keeps the two copies equal."""
    import yaml

    from okg.substrate.agent_surface_paths import agent_surface_path_values

    directory = REPO_ROOT / "deployments" / name
    manifest = yaml.safe_load((directory / "deployment.yaml").read_text())
    definition = yaml.safe_load((directory / "definition.yaml").read_text())
    assert parse_agent_surface(manifest) == parse_agent_surface(definition)
    assert {
        field: (directory / value).resolve()
        for field, value in agent_surface_path_values(manifest)
    } == {
        field: (directory / value).resolve()
        for field, value in agent_surface_path_values(definition)
    }
