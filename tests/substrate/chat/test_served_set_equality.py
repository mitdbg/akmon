"""The live tool listing must equal what that launch has to register.

PACT `effective-surface-declaration`, req.esd.drift-refused,
task.esd.served-set-equality. Hermetic: `_probe_mcp` is driven through its
injected `probe_tools` seam, so nothing dials a server or a database.

The defect these pin is specific. `okg chat sync` already asserted the live
listing is a SUPERSET of the preset's bound tools, which answers a different
question — it catches a preset pointing at a tool that does not exist. It
could never catch this one: okg-workspace declared seven chat tools, sync
removed `query` under the masked posture, the server registered all seven,
and a superset check passed every time.

The second trap is the one an earlier draft of this change walked into. The
comparison must be against what THAT LAUNCH registers, not against
`agent_surface.served`: `served` is the whole surface across every launch
mode, so a read-only endpoint of a deployment that also declares write tools
legitimately lists seven of eighteen. Comparing to `served` refused every
real launch, including okg-workspace's own.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.effective_surface import (
    ServedSetMismatch,
    assert_served_sets_equal,
    launch_mode_of,
    parse_agent_surface,
    registered_tools_for_launch,
)
from okg.substrate.mcp.operators import COMPOSITION_TOOLS, OPERATOR_TOOLS, WRITE_ADMIN_TOOLS

# `run` (read-sets-run-3666) is served where a deployment names it; the
# workspace manifest these tests read does not yet.
READ_SEVEN = sorted(set(OPERATOR_TOOLS) - set(COMPOSITION_TOOLS))
ALL_EIGHTEEN = sorted(set(READ_SEVEN) | set(WRITE_ADMIN_TOOLS))
CHAT_SIX = ["inspect", "search", "expand", "filter", "map", "aggregate"]
READ_SIX_NO_QUERY = [t for t in READ_SEVEN if t != "query"]


@pytest.fixture(autouse=True)
def _access_rules_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """The launch-mode tests run with access rules off, where a read launch
    registers all seven operators, `query` included (Jason, via orchestrator
    chat 2026-10-13, query-by-default; issue #3666). The tests under the
    rules set the mode themselves."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")


def _surface(served=None, projection=None):
    return parse_agent_surface({
        "agent_surface": {
            "served": served or ALL_EIGHTEEN,
            "projections": {"chat": {"tools": projection or CHAT_SIX}},
        },
    })


def _check(surface, observed, *, allow_writes=False):
    projection = surface.projection("chat")
    assert_served_sets_equal(
        declared=surface.served,
        observed=observed,
        deployment="okg-workspace",
        endpoint="http://127.0.0.1:8100/mcp",
        allow_writes=allow_writes,
        projected=projection.tools if projection else None,
    )


# ── the launch-mode split ──────────────────────────────────────────────

def test_a_read_endpoint_of_a_write_declaring_deployment_is_ACCEPTED() -> None:
    """The regression that made `okg chat sync` refuse every real launch.

    This is the okg-workspace shape exactly: eighteen declared, seven listed.
    On the first implementation this raised.
    """
    _check(_surface(), READ_SEVEN)


def test_a_write_endpoint_of_the_same_deployment_is_accepted() -> None:
    _check(_surface(), ALL_EIGHTEEN, allow_writes=True)


def test_a_READ_BOUND_endpoint_listing_the_write_set_is_REFUSED() -> None:
    """The mode comes from the binding, never from the listing.

    Inferring it made this case pass: an endpoint bound read-only but
    launched `--allow-writes` lists eighteen tools, the inference concluded
    "so it is a write launch", and the check approved the very drift it
    exists to catch. A chat endpoint is a read binding, so eighteen tools on
    it is a misconfigured server.
    """
    with pytest.raises(ServedSetMismatch) as excinfo:
        _check(_surface(), ALL_EIGHTEEN, allow_writes=False)
    assert set(excinfo.value.data["listed_not_served"]) == set(
        WRITE_ADMIN_TOOLS
    )


def test_the_expected_set_is_the_shared_function_registration_uses() -> None:
    """One function answers "what does this launch register", or the two
    answers drift — which is the whole subject of this change."""
    assert registered_tools_for_launch(
        ALL_EIGHTEEN, allow_writes=False,
    ) == frozenset(READ_SEVEN)
    assert registered_tools_for_launch(
        ALL_EIGHTEEN, allow_writes=True,
    ) == frozenset(ALL_EIGHTEEN)
    # `launch_mode_of` is REPORTING only now; it must not pick the set.
    assert launch_mode_of(READ_SEVEN) is False
    assert launch_mode_of(ALL_EIGHTEEN) is True


# ── what it still refuses ──────────────────────────────────────────────

def test_an_endpoint_serving_MORE_than_its_launch_should_is_refused() -> None:
    """THE defect. A superset check passes this; equality does not."""
    surface = _surface(served=CHAT_SIX, projection=CHAT_SIX)
    with pytest.raises(ServedSetMismatch) as excinfo:
        _check(surface, [*CHAT_SIX, "query"])
    assert excinfo.value.data["listed_not_served"] == ["query"]


def test_an_endpoint_serving_FEWER_tools_than_declared_is_refused() -> None:
    """A tool the launch must register is missing from the listing.

    `query` is dropped rather than a projected tool, so the served-set
    assertion is what answers rather than the projection check in front of
    it: the two refusals name different data and a test must not pass on the
    wrong one.
    """
    with pytest.raises(ServedSetMismatch) as excinfo:
        _check(_surface(), [t for t in READ_SEVEN if t != "query"])
    assert excinfo.value.data["served_not_listed"] == ["query"]


def test_a_half_write_listing_is_neither_legal_set_and_is_refused() -> None:
    """A listing that is neither the read set nor the write set.

    Deriving the launch mode from the listing could have made this vacuous —
    any listing would "be" whatever mode it looked like. It does not: the
    mode selects which of the two legal sets to require, and a listing that
    matches neither still fails.
    """
    with pytest.raises(ServedSetMismatch):
        _check(_surface(), [*READ_SEVEN, "record"], allow_writes=False)


def test_a_projection_naming_a_tool_the_endpoint_does_not_list_is_refused() -> None:
    """The projection is checked against the LIVE listing, not only against
    the manifest: the listing is the set the client will really meet."""
    surface = _surface(projection=[*CHAT_SIX, "query"])
    with pytest.raises(ServedSetMismatch) as excinfo:
        _check(surface, [t for t in READ_SEVEN if t != "query"])
    assert excinfo.value.data["projected_not_listed"] == ["query"]


# ── through the sync probe seam ────────────────────────────────────────

def _probe_inputs(tmp_path, *, served, projection, prompt: str):
    """Minimal real `SyncInputs` for a deployment that declares the block."""
    from okg.substrate.chat.data import chat_data_dir
    from okg.substrate.chat.sync import SyncInputs
    from okg.substrate.deployments.chat_block import parse_chat_block

    dep = tmp_path / "deployment"
    (dep / "chat").mkdir(parents=True, exist_ok=True)
    (dep / "chat" / "system_prompt.md").write_text(prompt, encoding="utf-8")
    raw = {
        "name": "fixture",
        "agent_surface": {
            "served": list(served),
            "projections": {
                "chat": {
                    "tools": list(projection),
                    "system_prompt_ref": "chat/system_prompt.md",
                },
            },
        },
        "search": {"profiles": {"analyst": {"subtypes": ["pact_change"]}}},
        "chat": {
            "enabled": True,
            "ui": {"site_name": "Fixture"},
            "models": {"builtin": ["openai/gpt-5.6-luna"],
                       "default": "openai/gpt-5.6-luna"},
            "preset": {"display_name": "Fixture", "description": "d"},
            "search_profile": "analyst",
            "mcp": {"port": 8100},
        },
    }
    return SyncInputs(
        deployment="fixture",
        deployment_dir=dep,
        raw_manifest=raw,
        chat=parse_chat_block(raw, manifest_path=dep / "deployment.yaml"),
        mcp_instructions_path=None,
        chat_data_dir=chat_data_dir(),
        mcp_probe_url="http://127.0.0.1:8100/mcp",
        mcp_instance_url="http://host.docker.internal:8100/mcp",
        preset_id="okg-fixture",
        mcp_server_id="okg-fixture",
        state_home=tmp_path / "state",
    )


def _run_probe(inputs, listing):
    """Call the REAL `_probe_mcp` with its probe seam injected."""
    from okg.substrate.chat.sync import SyncReport, _probe_mcp

    return _probe_mcp(
        inputs,
        SyncReport(deployment=inputs.deployment),
        bound_tools=tuple(CHAT_SIX),
        probe_tools=lambda url, token: tuple(listing),
    )


def test_the_REAL_probe_path_runs_the_equality_check(tmp_path) -> None:
    """Drive `_probe_mcp` itself, not a local stand-in.

    The earlier version of this test called its own fake and then called the
    assertion directly, so deleting the wiring in `sync.py` reddened nothing.
    This calls the real function through the seam `okg chat sync` uses, so
    removing the equality call makes it fail.
    """
    inputs = _probe_inputs(
        tmp_path, served=ALL_EIGHTEEN, projection=CHAT_SIX,
        prompt="- `search` — find nodes.",
    )
    assert set(_run_probe(inputs, READ_SEVEN)) == set(READ_SEVEN)

    with pytest.raises(ServedSetMismatch):
        _run_probe(inputs, ALL_EIGHTEEN)


def test_the_REAL_probe_path_scans_the_chat_PROMPT(tmp_path) -> None:
    """req.esd.drift-refused, on the channel it was written for.

    Open WebUI never delivers the MCP server's initialize instructions to a
    model, so this prompt is the only instruction channel a chat assistant
    has — and until this call existed it was scanned by nothing at all.
    """
    from okg.substrate.mcp.effective_surface import (
        SurfaceTextNamesUnservedOperator,
    )

    inputs = _probe_inputs(
        tmp_path, served=ALL_EIGHTEEN, projection=CHAT_SIX,
        prompt="- `query` — run bounded SQL when the algebra runs out.",
    )
    with pytest.raises(SurfaceTextNamesUnservedOperator) as excinfo:
        _run_probe(inputs, READ_SEVEN)
    assert excinfo.value.data["unserved"] == ["query"]


def test_the_REAL_probe_path_accepts_a_prompt_naming_only_projected_tools(
    tmp_path,
) -> None:
    """Non-vacuity partner: the scan refuses a name, not every prompt."""
    inputs = _probe_inputs(
        tmp_path, served=ALL_EIGHTEEN, projection=CHAT_SIX,
        prompt="- `search` — find nodes.\n- `inspect` — read one node.",
    )
    assert _run_probe(inputs, READ_SEVEN)


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_under_access_rules_a_launch_lists_no_query(
    mode: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under `audit` or `enforce` a launch registers no `query` (issue
    #3666): the listing without it is accepted on read and write
    endpoints, and a listing that still serves it is refused by name."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    assert registered_tools_for_launch(
        ALL_EIGHTEEN, allow_writes=False, deployment="okg-workspace",
    ) == frozenset(READ_SIX_NO_QUERY)
    _check(_surface(), READ_SIX_NO_QUERY)
    _check(
        _surface(), [t for t in ALL_EIGHTEEN if t != "query"],
        allow_writes=True,
    )
    with pytest.raises(ServedSetMismatch) as excinfo:
        _check(_surface(), READ_SEVEN)
    assert excinfo.value.data["listed_not_served"] == ["query"]


def _workspace_surface():
    from pathlib import Path

    from okg.substrate.deployments import DeploymentManifest

    root = Path(__file__).resolve().parents[3]
    surface = DeploymentManifest.from_yaml(
        root / "deployments/okg-workspace/deployment.yaml",
    ).agent_surface
    assert surface is not None
    return surface


def test_the_real_workspace_manifest_agrees_with_a_read_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deployment this PACT migrates, against the listing its own
    read-only mcp-serve produces. Its manifest sets access rules to
    `enforce`, so that listing has no `query`, and one with it is refused."""
    from okg.substrate.nomos.runtime_enforcement import (
        _clear_nomos_enforcement_mode_cache,
    )

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT")
    # Read the real manifest, not a directory an earlier test memoized.
    _clear_nomos_enforcement_mode_cache()
    surface = _workspace_surface()
    projection = surface.projection("chat")
    kwargs = {
        "declared": surface.served,
        "deployment": "okg-workspace",
        "endpoint": "http://127.0.0.1:8100/mcp",
        "allow_writes": False,
        "projected": projection.tools,
    }
    assert_served_sets_equal(observed=READ_SIX_NO_QUERY, **kwargs)
    with pytest.raises(ServedSetMismatch):
        assert_served_sets_equal(observed=READ_SEVEN, **kwargs)
