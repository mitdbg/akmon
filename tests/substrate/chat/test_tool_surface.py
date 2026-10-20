"""The tool surface a chat assistant is given (PACT chat-tool-surface).

Every assertion here is on an OBSERVABLE END STATE: what the instance holds
after the run, read back through its own API, and what the vendor's own
matcher would admit from that held state against the advertised tool names.
Nothing asserts on a request body the projection just constructed — a
projection that computed the right thing and sent the wrong thing has to fail
here, and one that sent the right thing to a vendor that reads it backwards
has to fail here too.

The two vendor traps this suite exists to pin, re-verified against the running
``ghcr.io/open-webui/open-webui:v0.11.0`` container on 2026-08-11:

* TRAP 1 — ``is_string_allowed`` matches with ``endswith``, not equality:
  ``utils/misc.py:71`` for the allow arm, ``:75`` for the block arm. A filter
  entry is therefore NOT a tool name.
* TRAP 2 — an EMPTY filter list means ALLOW ALL: ``utils/misc.py:63-64``
  returns True immediately on a falsy list, and ``utils/middleware.py:2242``
  skips filtering when the list is empty. The string form ``''`` reaches the
  same place by a different road: ``:2239`` splits it into the truthy
  ``['']``, whose entries all strip to empty, which composes to an empty allow
  list and an empty block list — admitting everything.

And the reason a preset cannot substitute for the connection filter:
``middleware.py:2740`` matches the ``server:mcp:`` prefix, ``:2742`` takes the
WHOLE remainder as a server id, and ``:2757`` iterates every advertised spec
and registers each one.

Every refusal arm below has a positive control in this same file, and the
empty-declared-set arm is guarded by an explicit demonstration that the trap
is real — otherwise "zero tools served" would be indistinguishable from a
check that never ran.
"""

from __future__ import annotations

import dataclasses
import io
import json
import urllib.error
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.data import chat_data_dir
from okg.substrate.chat import sync as sync_module
from okg.substrate.chat.sync import (
    CHAT_DEFAULT_TOOLS,
    CONNECTION_SERVING_DECLARED,
    CONNECTION_SERVING_NOTHING,
    DENY_ALL_SENTINEL,
    GUIDANCE_KEYS,
    RESOLVED_AGENT_SURFACE_KEYS,
    ChatSyncError,
    InstanceClient,
    SyncInputs,
    bind_tool_curation,
    compose_standing_guidance,
    deny_all_filter_list,
    effective_tool_binding,
    is_string_allowed,
    load_inputs,
    payload_secrets,
    plan_tool_curation,
    resolve_posture,
    sync,
)
from okg.substrate.deployments.chat_block import ChatManifestError, parse_chat_block
from okg.substrate.mcp.operators import (
    AKMON_READ_TOOLS,
    OPERATOR_METADATA,
    ToolDescriptionError,
    compose_tool_descriptions,
    load_tool_description_overrides,
)

from .fake_openwebui import FakeInstance

REPO_ROOT = Path(__file__).resolve().parents[3]

TOKEN = "fake-admin-token-value-0123456789"
BASE_URL = "http://instance.test"

#: What a real ``okg mcp-serve`` advertises today.
LIVE_TOOLS = ("aggregate", "expand", "filter", "inspect", "map", "query", "search")

#: The same server plus a name that ENDS IN a declared entry. Not invented for
#: convenience: ``run_sql`` was renamed to ``query`` by
#: ``shrink-mcp-to-operator-surface``, so a future ``raw_query`` /
#: ``graph_query`` is the ordinary case, not the exotic one.
COLLIDING_TOOLS = (*LIVE_TOOLS, "raw_query")

ALL_TOOLS = ("search", "inspect", "expand", "filter", "map", "aggregate", "query")


# ---------------------------------------------------------------------------
# Fixture deployment
# ---------------------------------------------------------------------------

def _raw_manifest(
    *,
    tools: list[str] | None = None,
    audiences: dict[str, Any] | None = None,
    enforcement: str = "enforce",
    agent_tools: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mcp: dict[str, Any] = {"port": 8100, "tools": list(tools or ALL_TOOLS)}
    if audiences is not None:
        mcp["audiences"] = audiences
    manifest: dict[str, Any] = {
        "name": "fixture",
        "nomos": {"runtime_enforcement": enforcement},
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
            "mcp": mcp,
            "access": {"groups": ["okg-users"]},
            "deep_links": {
                "operator_ui_base": "http://console.test",
                "chat_site_base": "http://chat.test",
            },
        },
    }
    if agent_tools is not None:
        manifest["agent_tools"] = agent_tools
    return manifest


def _unmasked_catalog() -> Any:
    class _Catalog:
        subtype_metadata: dict[str, dict[str, Any]] = {"document": {"other": 1}}

    return _Catalog()


SYSTEM_PROMPT = "Use the OKG tools. Quote node ids."


def _make_inputs(
    tmp_path: Path,
    *,
    manifest: dict[str, Any] | None = None,
    drop_mcp_block: bool = False,
    overrides: dict[str, str] | None = None,
    skills: dict[str, str] | None = None,
    skill_triggers: dict[str, Any] | None = None,
) -> SyncInputs:
    raw = manifest if manifest is not None else _raw_manifest()
    dep_dir = tmp_path / "deployment"
    (dep_dir / "chat").mkdir(parents=True, exist_ok=True)
    (dep_dir / "chat" / "system_prompt.md").write_text(SYSTEM_PROMPT, encoding="utf-8")

    overrides_path: Path | None = None
    if overrides is not None:
        overrides_path = dep_dir / "tool_description_overrides.yaml"
        overrides_path.write_text(
            "\n".join(f"{name}: {text!r}" for name, text in overrides.items()),
            encoding="utf-8",
        )
    skills_dir: Path | None = None
    if skills is not None:
        skills_dir = dep_dir / "skills"
        skills_dir.mkdir(parents=True, exist_ok=True)
        for stem, body in skills.items():
            (skills_dir / f"{stem}.md").write_text(body, encoding="utf-8")
    triggers_path: Path | None = None
    if skill_triggers is not None:
        triggers_path = dep_dir / "skill_triggers.yaml"
        triggers_path.write_text(json.dumps(skill_triggers), encoding="utf-8")

    chat = parse_chat_block(raw, manifest_path=dep_dir / "deployment.yaml")
    if drop_mcp_block:
        chat = dataclasses.replace(chat, mcp=None)
    return SyncInputs(
        deployment="fixture",
        deployment_dir=dep_dir,
        raw_manifest=raw,
        chat=chat,
        mcp_instructions_path=None,
        chat_data_dir=chat_data_dir(),
        mcp_probe_url="http://127.0.0.1:8100/mcp",
        mcp_instance_url="http://host.docker.internal:8100/mcp",
        preset_id="okg-fixture",
        mcp_server_id="okg-fixture",
        state_home=tmp_path / "state",
        catalog_loader=_unmasked_catalog,
        tool_description_overrides_path=overrides_path,
        skills_dir=skills_dir,
        skill_triggers_path=triggers_path,
        agent_tools=raw.get("agent_tools") or {},
    )


class _StubMcp:
    """The shape :func:`plan_tool_curation` reads off a parsed chat block.

    Used only where the arm under test needs a tool name the manifest schema
    refuses at LOAD (``raw_query`` is not in the admitted universe). Load-time
    validation is chat-manifest-block's and is not re-litigated here; what is
    under test is the wire behaviour AFTER a set is declared.
    """

    def __init__(self, tools: tuple[str, ...], audiences: tuple[Any, ...]) -> None:
        self.tools = tools
        self.audiences = audiences
        self.auth_token_env: str | None = None


class _StubChat:
    def __init__(
        self, *, tools: tuple[str, ...] = (), audiences: tuple[Any, ...] = (),
    ) -> None:
        self.mcp = _StubMcp(tools=tools, audiences=audiences)


class _NoMcpChat:
    """A chat block with NO mcp declaration at all."""

    mcp = None


def _instance() -> FakeInstance:
    instance = FakeInstance(webui_name="Fixture Site")
    instance.add_group("okg-users", "group-users")
    instance.install_function(
        "okg_review_queue",
        valve_fields=("deployment", "openwebui_api_base", "admin_group"),
    )
    return instance


def _run(
    inputs: SyncInputs,
    instance: FakeInstance,
    *,
    advertised: tuple[str, ...] = LIVE_TOOLS,
) -> Any:
    return sync(
        inputs,
        InstanceClient(BASE_URL, TOKEN, opener=instance.opener),
        probe_tools=lambda _endpoint, _env: advertised,
    )


def _connection(instance: FakeInstance) -> dict[str, Any] | None:
    for entry in instance.tool_servers:
        if (entry.get("info") or {}).get("id") == "okg-fixture":
            return entry
    return None


def _served_tools(
    instance: FakeInstance, *, advertised: tuple[str, ...] = LIVE_TOOLS,
) -> tuple[str, ...]:
    """The tools/list READBACK: what this connection will actually serve.

    Both halves come off the instance — the stored filter string is read back
    from ``GET /api/v1/configs/tool_servers`` and run through the vendor's own
    predicate against the advertised names. Nothing here reads the value the
    projection intended to write.
    """
    connection = _connection(instance)
    if connection is None:
        raise AssertionError(
            "no okg-fixture connection is registered; zero served tools read "
            "off an ABSENT connection would prove nothing"
        )
    stored = (connection.get("config") or {}).get("function_name_filter_list")
    assert isinstance(stored, str), stored
    return effective_tool_binding(advertised, stored.split(","))


# ---------------------------------------------------------------------------
# The vendor predicate, replicated — and the traps it carries
# ---------------------------------------------------------------------------

class TestVendorMatcher:
    """The replica must agree with the MEASURED v0.11.0 semantics.

    These are the facts every refusal below depends on. If the replica stops
    matching the vendor, the refusals are computed against a predicate nobody
    runs, and they would pass while the instance did something else.
    """

    def test_empty_filter_list_admits_everything(self) -> None:
        """TRAP 2, arm one: utils/misc.py:63-64."""
        assert effective_tool_binding(LIVE_TOOLS, []) == tuple(sorted(LIVE_TOOLS))

    def test_the_empty_string_form_also_admits_everything(self) -> None:
        """TRAP 2, arm two: '' splits to [''] (truthy) and strips to nothing.

        This is why "write an empty filter list to mean no tools" is not a
        near miss but the exact opposite of the intent.
        """
        assert effective_tool_binding(LIVE_TOOLS, "".split(",")) == tuple(
            sorted(LIVE_TOOLS)
        )

    def test_an_allow_entry_matches_by_suffix(self) -> None:
        """TRAP 1, allow arm: utils/misc.py:71."""
        assert is_string_allowed("raw_query", ["query"]) is True
        assert is_string_allowed("graph_query", ["query"]) is True
        assert is_string_allowed("search", ["query"]) is False

    def test_a_block_entry_matches_by_suffix(self) -> None:
        """TRAP 1, block arm: utils/misc.py:75."""
        assert is_string_allowed("raw_query", ["!query"]) is False
        # ... and a bare block list admits every NON-matching name, which is
        # why `!query` alone is a widening, not a narrowing.
        assert effective_tool_binding(LIVE_TOOLS, ["!query"]) == (
            "aggregate", "expand", "filter", "inspect", "map", "search",
        )

    def test_the_deny_all_list_is_non_empty_and_admits_nothing(self) -> None:
        deny = deny_all_filter_list(COLLIDING_TOOLS)
        assert deny, "an empty deny list is the ALLOW ALL trap"
        assert ",".join(deny) != ""
        assert effective_tool_binding(COLLIDING_TOOLS, deny) == ()

    def test_the_sentinel_admits_nothing_ON_ITS_OWN(self) -> None:
        """The property the sentinel exists for, not the fact that it is there.

        An adversarial review changed ``DENY_ALL_SENTINEL`` to ``search`` — a
        REAL advertised tool name — and the suite stayed green, because the
        only assertion on it was ``DENY_ALL_SENTINEL in deny`` and the block
        arm was doing all the work. Presence is not the property. What the
        sentinel must satisfy is that NO advertised name ends with it, so the
        allow arm of ``is_string_allowed`` (utils/misc.py:71) rejects every
        one of them by itself.
        """
        allow_arm = [e for e in deny_all_filter_list(COLLIDING_TOOLS) if e[:1] != "!"]
        assert allow_arm == [DENY_ALL_SENTINEL], allow_arm
        assert effective_tool_binding(COLLIDING_TOOLS, allow_arm) == ()
        for name in (*COLLIDING_TOOLS, *ALL_TOOLS, *CHAT_DEFAULT_TOOLS):
            assert not name.endswith(DENY_ALL_SENTINEL), name
            assert is_string_allowed(name, [DENY_ALL_SENTINEL]) is False

    def test_the_block_arm_admits_nothing_ON_ITS_OWN(self) -> None:
        """The other arm, pinned the same way.

        The same review DELETED the ``!name`` block arm and the suite stayed
        green, because the sentinel was doing all the work. The code comment
        claimed "neither alone would survive the other being wrong"; as
        tested, each was masking the other's absence. Asserting each arm in
        isolation is what makes that comment true.
        """
        blocks = [e for e in deny_all_filter_list(COLLIDING_TOOLS) if e[:1] == "!"]
        assert blocks == [f"!{name}" for name in sorted(COLLIDING_TOOLS)]
        assert effective_tool_binding(COLLIDING_TOOLS, blocks) == ()

    def test_an_arm_that_leaks_on_its_own_is_refused_at_composition(self) -> None:
        """... and the same property is enforced in the code, not only here.

        A test can only pin what it is run against. ``deny_all_filter_list``
        re-derives both arms from the list it is about to return and refuses
        if either admits anything alone, so the independence survives a
        future edit made without reading this file.
        """
        with pytest.raises(ChatSyncError) as raised:
            deny_all_filter_list(("search", f"tool{DENY_ALL_SENTINEL}"))
        assert raised.value.data["reason"] == "deny_all_arm_leaks"
        assert raised.value.data["arm"] == "allow"
        assert raised.value.data["leaked"] == [f"tool{DENY_ALL_SENTINEL}"]

    def test_the_positive_control_for_that_refusal(self) -> None:
        """The refusal above is not unconditional: the live listing composes."""
        deny = deny_all_filter_list(LIVE_TOOLS)
        assert effective_tool_binding(LIVE_TOOLS, deny) == ()
        assert len(deny) == 1 + len(LIVE_TOOLS)


# ---------------------------------------------------------------------------
# req.chat-tool-surface.curation-reaches-the-wire
# ---------------------------------------------------------------------------

class TestDeclaredSubsetReachesTheWire:

    def test_the_declared_subset_is_what_the_connection_serves(
        self, tmp_path: Path,
    ) -> None:
        """scn.declared-subset. The positive control for every refusal below."""
        inputs = _make_inputs(
            tmp_path, manifest=_raw_manifest(tools=["search", "inspect", "expand"]),
        )
        instance = _instance()
        report = _run(inputs, instance)

        assert report.ok
        assert _served_tools(instance) == ("expand", "inspect", "search")
        # The readback the requirement names, off the reconciled diff rather
        # than off the value the run intended.
        entry = next(
            e for e in report.surfaces["preset"].diff if e["key"] == "bound_tools"
        )
        assert entry["verified"] is True
        assert entry["observed"] == ["expand", "inspect", "search"]
        assert report.curation.connection_state == CONNECTION_SERVING_DECLARED

    def test_the_preset_binds_the_whole_server_never_a_subset(
        self, tmp_path: Path,
    ) -> None:
        """A preset CANNOT narrow an MCP server (middleware.py:2742, :2757).

        So the tool id must stay the bare ``server:mcp:<id>``. A projection
        that tried to curate through toolIds would either drop the server
        entirely or bind a per-tool id the vendor never resolves.
        """
        inputs = _make_inputs(
            tmp_path, manifest=_raw_manifest(tools=["search", "inspect"]),
        )
        instance = _instance()
        _run(inputs, instance)

        tool_ids = instance.model_row("okg-fixture")["meta"]["toolIds"]
        assert tool_ids == ["server:mcp:okg-fixture"]
        for tool_id in tool_ids:
            assert tool_id.count(":") == 2, "a per-tool id is never resolved"
            assert "|" not in tool_id, (
                "the server_id|function_names grammar (utils/tools.py:382-385) "
                "is parsed on the OpenAPI path only and is dead even there"
            )

    def test_the_no_declaration_default_is_the_named_literal_six(
        self, tmp_path: Path,
    ) -> None:
        """The literal default set, asserted BY NAME rather than derived."""
        assert CHAT_DEFAULT_TOOLS == (
            "inspect", "search", "expand", "filter", "map", "aggregate",
        )
        assert "query" not in CHAT_DEFAULT_TOOLS
        assert set(CHAT_DEFAULT_TOOLS) == set(AKMON_READ_TOOLS)

        inputs = _make_inputs(tmp_path, drop_mcp_block=True)
        instance = _instance()
        report = _run(inputs, instance)

        assert report.curation.plan.declaration_source == "substrate_default"
        assert report.curation.declared == CHAT_DEFAULT_TOOLS
        assert _served_tools(instance) == tuple(sorted(CHAT_DEFAULT_TOOLS))
        assert "query" not in _served_tools(instance)

    def test_the_default_set_is_reported_on_every_run_declared_or_not(
        self, tmp_path: Path,
    ) -> None:
        """Even when the manifest declares its own set, the default is stated.

        An unstated default is default-value-as-fallback: a reader of a run
        that DID declare a set still has to be told what the unstated case
        would have been, or the fallback is invisible until it fires.
        """
        inputs = _make_inputs(
            tmp_path, manifest=_raw_manifest(tools=["search", "inspect"]),
        )
        instance = _instance()
        report = _run(inputs, instance)

        payload = report.to_dict()["tool_curation"]
        assert payload["substrate_default_set"] == list(CHAT_DEFAULT_TOOLS)
        assert payload["declaration_source"] == "manifest"
        assert payload["declared"] == ["search", "inspect"]
        # And an operator who never asks for --json still sees it.
        notes = " ".join(report.surfaces["preset"].notes)
        assert "substrate default set is" in notes
        assert "inspect" in notes and "aggregate" in notes
        assert json.dumps(report.to_dict(), default=str)


class TestEndswithOverreach:
    """TRAP 1: a filter entry is not a tool name."""

    def test_an_entry_that_would_admit_more_than_declared_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """scn.endswith-overreach-refused."""
        inputs = _make_inputs(
            tmp_path,
            manifest=_raw_manifest(tools=["search", "query"], enforcement="off"),
        )
        instance = _instance()
        with pytest.raises(ChatSyncError) as raised:
            _run(inputs, instance, advertised=COLLIDING_TOOLS)

        data = raised.value.data
        assert data["reason"] == "filter_admits_a_different_set"
        # All three sets the scenario demands, by name.
        assert data["declared"] == ["query", "search"]
        assert data["admitted"] == ["query", "raw_query", "search"]
        assert data["extra"] == ["raw_query"]
        assert "endswith" in str(raised.value)

    def test_a_non_colliding_declaration_over_the_same_server_is_served(
        self, tmp_path: Path,
    ) -> None:
        """The positive control: same advertised set, no collision, no refusal.

        Without this the refusal above could be an unconditional raise on any
        server advertising ``raw_query``.
        """
        inputs = _make_inputs(
            tmp_path, manifest=_raw_manifest(tools=["search", "inspect"]),
        )
        instance = _instance()
        report = _run(inputs, instance, advertised=COLLIDING_TOOLS)

        assert report.ok
        served = _served_tools(instance, advertised=COLLIDING_TOOLS)
        assert served == ("inspect", "search")
        assert "raw_query" not in served

    def test_a_block_entry_that_would_drop_a_declared_tool_is_refused(self) -> None:
        """The other direction: the ``!query`` exclusion also blocks a suffix.

        A masked deployment declaring ``query`` alongside a future
        ``raw_query`` gets the exclusion entry ``!query`` — which the endswith
        BLOCK arm (utils/misc.py:75) also applies to ``raw_query``, silently
        losing a tool the operator declared. Refused with MISSING named, not
        shipped: a narrowing that nobody asked for is as wrong as a widening.
        """
        plan, exclusions = plan_tool_curation(
            _StubChat(tools=("search", "query", "raw_query")),
            deployment="fixture",
            posture=resolve_posture(
                "fixture", {"nomos": {"runtime_enforcement": "enforce"}},
            ),
        )
        assert [e["tool"] for e in exclusions] == ["query"]
        assert plan.intended == ("search", "raw_query")

        with pytest.raises(ChatSyncError) as raised:
            bind_tool_curation(
                plan, advertised=COLLIDING_TOOLS, deployment="fixture",
            )
        data = raised.value.data
        assert data["reason"] == "filter_admits_a_different_set"
        assert data["missing"] == ["raw_query"]
        assert data["admitted"] == ["search"]

    def test_the_same_plan_without_the_colliding_name_binds_cleanly(self) -> None:
        """The positive control for the block-side arm."""
        plan, _ = plan_tool_curation(
            _StubChat(tools=("search", "query")),
            deployment="fixture",
            posture=resolve_posture(
                "fixture", {"nomos": {"runtime_enforcement": "enforce"}},
            ),
        )
        curation = bind_tool_curation(
            plan, advertised=LIVE_TOOLS, deployment="fixture",
        )
        assert curation.admitted == ("search",)
        assert curation.connection_state == CONNECTION_SERVING_DECLARED


class TestEmptyDeclaredSet:
    """TRAP 2: an empty filter list means ALLOW ALL."""

    def _empty_case(self, tmp_path: Path) -> tuple[Any, FakeInstance]:
        """A MASKED deployment whose only declared tool is ``query``.

        Not a contrived fixture: this is the live path. The posture exclusion
        removes ``query``, leaving an empty intended set — and the pre-PACT
        projection wrote the filter ``!query`` for it, which admits every
        OTHER advertised tool. The declared set was one tool; the served set
        was six.
        """
        inputs = _make_inputs(
            tmp_path, manifest=_raw_manifest(tools=["query"], enforcement="enforce"),
        )
        instance = _instance()
        return _run(inputs, instance), instance

    def test_no_empty_filter_list_is_written(self, tmp_path: Path) -> None:
        """scn.empty-set-is-not-empty-list, half one."""
        _, instance = self._empty_case(tmp_path)
        connection = _connection(instance)
        assert connection is not None
        stored = connection["config"]["function_name_filter_list"]
        assert stored != "", "the vendor reads an empty filter list as ALLOW ALL"
        assert [e for e in stored.split(",") if e.strip()], stored

    def test_the_connection_serves_zero_tools(self, tmp_path: Path) -> None:
        """scn.empty-set-is-not-empty-list, half two — by readback."""
        _, instance = self._empty_case(tmp_path)
        assert _served_tools(instance) == ()

    def test_the_run_states_which_state_it_produced(self, tmp_path: Path) -> None:
        """Zero tools must never be read off an absence.

        The connection is PRESENT — every connection field reads back — and it
        serves nothing. That is a different fact from "no connection", and the
        run says which one it produced.
        """
        report, instance = self._empty_case(tmp_path)
        assert report.curation.connection_state == CONNECTION_SERVING_NOTHING
        assert report.to_dict()["tool_curation"]["connection_state"] == (
            CONNECTION_SERVING_NOTHING
        )
        assert report.mcp["connection_state"] == CONNECTION_SERVING_NOTHING
        # The distinguishing evidence: the connection really is there.
        connection = _connection(instance)
        assert connection is not None
        assert connection["info"]["id"] == "okg-fixture"
        assert connection["config"]["enable"] is True
        assert "serving_nothing" in " ".join(report.surfaces["preset"].notes)

    def test_the_trap_this_arm_neutralises_is_real(self, tmp_path: Path) -> None:
        """The control that makes "zero tools" non-vacuous.

        Had the run written the pre-PACT filter for this case — ``!query``,
        the only entry an exclusion-driven builder produces from an empty
        intended set — the SAME connection would have served six tools. This
        asserts the counterfactual against the vendor predicate, so the
        zero-tool readback above cannot be satisfied by a filter that simply
        never mattered.
        """
        report, _ = self._empty_case(tmp_path)
        assert report.curation.intended == ()
        naive = effective_tool_binding(LIVE_TOOLS, ["!query"])
        assert len(naive) == 6, naive
        assert effective_tool_binding(LIVE_TOOLS, report.curation.filter_list) == ()

    def test_a_non_empty_declaration_still_serves_its_tools(
        self, tmp_path: Path,
    ) -> None:
        """The positive control: the deny path is not unconditional."""
        inputs = _make_inputs(
            tmp_path, manifest=_raw_manifest(tools=["search", "aggregate"]),
        )
        instance = _instance()
        report = _run(inputs, instance)
        assert report.curation.connection_state == CONNECTION_SERVING_DECLARED
        assert _served_tools(instance) == ("aggregate", "search")

    def test_an_empty_declaration_is_not_read_as_no_declaration(self) -> None:
        """The two empty tuples that must never be confused.

        "the deployment declares no mcp block" and "the deployment declares an
        EMPTY tool set" both arrive as ``()``. Reading the second as the first
        would substitute the six-tool default for a deployment that asked for
        none — an empty set turning into six tools, which is the same defect
        shape as TRAP 2 arriving by a different road. Keyed on the presence of
        the block, so the two cases diverge structurally.
        """
        unmasked = resolve_posture(
            "fixture",
            {"nomos": {"runtime_enforcement": "off"}},
            catalog_loader=_unmasked_catalog,
        )
        absent, _ = plan_tool_curation(
            _StubChat(), deployment="fixture", posture=unmasked,
        )
        no_block, _ = plan_tool_curation(
            _NoMcpChat(), deployment="fixture", posture=unmasked,
        )
        assert no_block.declared == CHAT_DEFAULT_TOOLS
        assert no_block.declaration_source == "substrate_default"
        assert absent.declared == ()
        assert absent.declaration_source == "manifest"
        assert absent.intended == ()
        assert bind_tool_curation(
            absent, advertised=LIVE_TOOLS, deployment="fixture",
        ).admitted == ()

    def test_a_deny_all_filter_never_composes_to_empty(self) -> None:
        """The structural guard, independent of any listing."""
        plan, _ = plan_tool_curation(
            _StubChat(tools=("query",)),
            deployment="fixture",
            posture=resolve_posture(
                "fixture", {"nomos": {"runtime_enforcement": "enforce"}},
            ),
        )
        assert plan.intended == ()
        curation = bind_tool_curation(
            plan, advertised=LIVE_TOOLS, deployment="fixture",
        )
        assert ",".join(curation.filter_list) != ""
        assert curation.admitted == ()

    def test_the_empty_filter_guard_FIRES_when_its_precondition_is_forced(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The positive control the test above cannot be.

        ``test_a_deny_all_filter_never_composes_to_empty`` asserts the
        composed OUTPUT is non-empty. That is a property of
        ``deny_all_filter_list``, not of the guard in
        ``bind_tool_curation`` that catches an empty list -- so the guard is
        never reached, and a mutation run deleting the guard outright left
        the whole suite GREEN. Presence is not the property: an arm that
        nothing exercises is an arm that is not pinned, which is the same
        shape as the deny-all arms covering for each other.

        This forces the guard's precondition -- a composer that returns
        nothing -- and asserts the NAMED refusal, so the guard cannot be
        deleted behind the composer that currently makes it unreachable.
        """
        monkeypatch.setattr(
            sync_module, "deny_all_filter_list", lambda _advertised: (),
        )
        plan, _ = plan_tool_curation(
            _StubChat(tools=("query",)),
            deployment="fixture",
            posture=resolve_posture(
                "fixture", {"nomos": {"runtime_enforcement": "enforce"}},
            ),
        )
        assert plan.intended == ()
        with pytest.raises(ChatSyncError) as raised:
            bind_tool_curation(plan, advertised=LIVE_TOOLS, deployment="fixture")
        assert raised.value.data["reason"] == "empty_filter_list_means_allow_all"
        # The refusal states the stakes rather than only the fact.
        assert "ALLOW ALL" in str(raised.value)

        # Non-vacuity: with the real composer restored, the SAME call passes.
        monkeypatch.undo()
        assert bind_tool_curation(
            plan, advertised=LIVE_TOOLS, deployment="fixture",
        ).admitted == ()


class TestTwoAudiences:
    """scn.two-audiences-need-two-connections."""

    def test_two_different_tool_sets_over_one_connection_are_refused(
        self, tmp_path: Path,
    ) -> None:
        manifest = _raw_manifest(
            tools=["search", "inspect", "expand"],
            audiences={
                "admin": {"tools": ["search", "inspect", "expand"]},
                "user": {"tools": ["search"]},
            },
        )
        inputs = _make_inputs(tmp_path, manifest=manifest)
        instance = _instance()
        with pytest.raises(ChatSyncError) as raised:
            _run(inputs, instance)

        message = str(raised.value)
        data = raised.value.data
        assert data["reason"] == "two_audiences_one_connection"
        assert data["audiences"] == {
            "admin": ["expand", "inspect", "search"], "user": ["search"],
        }
        # The reason and the remedy, both named.
        assert "function_name_filter_list" in message
        assert "CONNECTION" in message
        assert "two MCP connections" in (raised.value.fix_hint or "")
        # No partial projection: the refusal fires before any surface runs.
        assert instance.tool_servers == []
        assert instance.models == {}

    def test_two_audiences_agreeing_on_one_set_project_fine(
        self, tmp_path: Path,
    ) -> None:
        """The positive control: audiences are not refused for existing."""
        manifest = _raw_manifest(
            tools=["search", "inspect"],
            audiences={
                "admin": {"tools": ["search", "inspect"]},
                "user": {"tools": ["inspect", "search"]},
            },
        )
        inputs = _make_inputs(tmp_path, manifest=manifest)
        instance = _instance()
        report = _run(inputs, instance)

        assert report.ok
        assert _served_tools(instance) == ("inspect", "search")
        assert report.to_dict()["tool_curation"]["audiences"] == [
            {"audience": "admin", "tools": ["search", "inspect"]},
            {"audience": "user", "tools": ["inspect", "search"]},
        ]

    def test_an_audience_disagreeing_with_chat_mcp_tools_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """One audience is still two sets when it differs from the main one."""
        manifest = _raw_manifest(
            tools=["search", "inspect"], audiences={"user": {"tools": ["search"]}},
        )
        inputs = _make_inputs(tmp_path, manifest=manifest)
        with pytest.raises(ChatSyncError) as raised:
            _run(inputs, _instance())
        assert raised.value.data["reason"] == "two_audiences_one_connection"

    def test_an_audience_tool_outside_the_admitted_surface_is_refused_at_load(
        self, tmp_path: Path,
    ) -> None:
        """The audience tool list is validated exactly like chat.mcp.tools."""
        manifest = _raw_manifest(
            tools=["search"], audiences={"user": {"tools": ["record"]}},
        )
        with pytest.raises(ChatManifestError) as raised:
            _make_inputs(tmp_path, manifest=manifest)
        assert raised.value.data["path"] == "chat.mcp.audiences.user.tools"
        assert raised.value.data["unknown"] == ["record"]

    def test_an_empty_audience_map_is_refused(self, tmp_path: Path) -> None:
        manifest = _raw_manifest(tools=["search"], audiences={})
        with pytest.raises(ChatManifestError) as raised:
            _make_inputs(tmp_path, manifest=manifest)
        assert raised.value.data["path"] == "chat.mcp.audiences"


# ---------------------------------------------------------------------------
# req.chat-tool-surface.descriptions-are-deployment-owned
# ---------------------------------------------------------------------------

DOMAIN_SNIPPET = (
    "In this deployment `search` means: look up a published paper by title "
    "or DOI. It never searches the web."
)


class TestDeploymentOwnedDescriptions:

    def test_a_declared_override_reaches_the_served_surface(
        self, tmp_path: Path,
    ) -> None:
        """scn.override-projected, verified by readback of the served row.

        The channel is forced. Open WebUI's MCP client keeps only name,
        description and inputSchema per tool (utils/mcp/client.py:88-107) and
        drops the server's `instructions` entirely; it never reads MCP
        resources on the chat path. So the preset's `params.system` — read
        back here from GET /api/v1/models/export, which is where a hand-edited
        row would also show up — is the surface the model sees.
        """
        inputs = _make_inputs(
            tmp_path,
            manifest=_raw_manifest(tools=["search", "inspect"]),
            overrides={"search": DOMAIN_SNIPPET},
        )
        instance = _instance()
        report = _run(inputs, instance)

        assert report.ok
        served_system = instance.model_row("okg-fixture")["params"]["system"]
        assert DOMAIN_SNIPPET in served_system
        assert "### `search`" in served_system
        # The substrate description is carried WITH it, not replaced by it.
        assert OPERATOR_METADATA["search"].description in served_system
        # And the readback verified, rather than the run asserting its own body.
        entry = next(
            e for e in report.surfaces["preset"].diff
            if e["key"] == "preset:params.system"
        )
        assert entry["verified"] is True
        assert DOMAIN_SNIPPET in entry["observed"]

    def test_a_tool_with_no_override_reports_the_substrate_default(
        self, tmp_path: Path,
    ) -> None:
        """An absent override is a STATED default, never silence."""
        inputs = _make_inputs(
            tmp_path,
            manifest=_raw_manifest(tools=["search", "inspect"]),
            overrides={"search": DOMAIN_SNIPPET},
        )
        report = _run(inputs, _instance())

        rows = {
            d["name"]: d
            for d in report.to_dict()["standing_guidance"]["tool_descriptions"]
        }
        assert rows["search"]["source"] == "deployment_override"
        assert rows["inspect"]["source"] == "substrate_default"
        assert rows["inspect"]["override_chars"] == 0
        notes = " ".join(report.surfaces["preset"].notes)
        assert "substrate default(s) ['inspect']" in notes

    def test_an_override_naming_an_unprojected_tool_raises(
        self, tmp_path: Path,
    ) -> None:
        """The raise arm: `query` is excluded, so an override for it refuses."""
        inputs = _make_inputs(
            tmp_path,
            manifest=_raw_manifest(tools=["search", "query"], enforcement="enforce"),
            overrides={"query": "Ask SQL of the pinned views."},
        )
        instance = _instance()
        with pytest.raises(ToolDescriptionError) as raised:
            _run(inputs, instance)

        assert raised.value.data["reason"] == "override_for_unprojected_tool"
        assert raised.value.data["unprojected"] == ["query"]
        assert raised.value.data["projected"] == ["search"]

    def test_an_override_is_refused_rather_than_dropped_when_nothing_projects(
        self,
    ) -> None:
        """An empty projected set admits nothing; it does not admit everything."""
        with pytest.raises(ToolDescriptionError) as raised:
            compose_tool_descriptions((), {"search": "x"})
        assert raised.value.data["reason"] == "override_for_unprojected_tool"
        assert raised.value.data["projected"] == []

    def test_an_over_budget_override_refuses_and_names_every_offender(self) -> None:
        with pytest.raises(ToolDescriptionError) as raised:
            compose_tool_descriptions(
                ("search", "inspect"),
                {"search": "x" * 2100, "inspect": "y" * 2100},
            )
        offenders = [row["tool"] for row in raised.value.data["offenders"]]
        assert offenders == ["search", "inspect"]

    def test_a_declared_but_missing_overrides_file_refuses(
        self, tmp_path: Path,
    ) -> None:
        with pytest.raises(ToolDescriptionError) as raised:
            load_tool_description_overrides(tmp_path / "nowhere.yaml")
        assert raised.value.data["reason"] == "overrides_unreadable"

    def test_an_undeclared_overrides_file_is_the_honest_empty_case(self) -> None:
        assert load_tool_description_overrides(None) == {}

    def test_a_non_string_snippet_refuses(self, tmp_path: Path) -> None:
        path = tmp_path / "overrides.yaml"
        path.write_text("search:\n  - a list\n", encoding="utf-8")
        with pytest.raises(ToolDescriptionError) as raised:
            load_tool_description_overrides(path)
        assert raised.value.data["reason"] == "override_not_a_string"


# ---------------------------------------------------------------------------
# req.chat-tool-surface.standing-guidance
# ---------------------------------------------------------------------------

#: NOTE the declared tool set every deployment fixture below pairs with this:
#: ``recent_publication_changes`` names ``aggregate``, so ``aggregate`` MUST be
#: in that deployment's ``chat.mcp.tools``. Until this suite pinned it, the
#: fixture declared only ``search`` and ``inspect`` while shipping a macro
#: telling the model to use ``aggregate`` — the exact defect
#: :func:`_render_guidance_agent_tools` now refuses, sitting unnoticed in the
#: test data that was supposed to prove the feature.
AGENT_TOOLS = {
    "recent_publication_changes": {
        "kind": "operator_macro",
        "operator": "aggregate",
        "description": "count publications by venue for the pinned generation",
    },
    "live_orcid_lookup": {
        "kind": "operator_macro",
        "operator": "search",
        "boundary": "external_live",
        "requires_env": "ORCID_TOKEN",
        "description": "hit the ORCID API",
    },
    "reindex": {"kind": "maintenance", "operator": "record"},
}


class TestStandingGuidance:

    def test_a_deployment_declaring_everything_records_a_decision_per_key(
        self, tmp_path: Path,
    ) -> None:
        inputs = _make_inputs(
            tmp_path,
            manifest=_raw_manifest(
                # `aggregate` is declared because the macro below NAMES it.
                tools=["search", "inspect", "aggregate"], agent_tools=AGENT_TOOLS,
            ),
            overrides={"search": DOMAIN_SNIPPET},
            skills=({"triage": "# Triage\n" + "x" * 5000}),
            skill_triggers={"intents": {"triage": {"skill": "triage"}}},
        )
        instance = _instance()
        report = _run(inputs, instance)

        # The macro's operator is one the connection actually serves. Asserted
        # against the READBACK, not against the manifest, because the served
        # set is what the model can call.
        assert "aggregate" in _served_tools(instance)

        guidance = report.to_dict()["standing_guidance"]
        assert guidance["keys"] == list(GUIDANCE_KEYS)
        decisions = {d["key"]: d for d in guidance["decisions"]}
        assert set(decisions) == set(GUIDANCE_KEYS), "every key gets a row"

        # CARRIED, with how.
        assert decisions["tool_description_overrides"]["carried"] is True
        assert decisions["agent_tools"]["carried"] is True
        assert decisions["agent_tools"]["detail"]["carried"] == [
            "recent_publication_changes"
        ]

        # NOT CARRIED, with why — declared, so the reason is recorded.
        for key in ("skills_dir", "skill_triggers"):
            assert decisions[key]["declared"] is True
            assert decisions[key]["carried"] is False
            assert len(decisions[key]["reason"]) > 80, decisions[key]["reason"]
        assert "unbounded" in decisions["skills_dir"]["reason"]
        assert "skills_dir" in decisions["skill_triggers"]["reason"]

        # The excluded agent tools are named with their reasons, not dropped.
        not_carried = {
            row["tool"]: row["reason"]
            for row in decisions["agent_tools"]["detail"]["not_carried"]
        }
        assert set(not_carried) == {"live_orcid_lookup", "reindex"}
        assert "external_live" in not_carried["live_orcid_lookup"]

        # And what was carried is in the surface the model sees.
        served_system = instance.model_row("okg-fixture")["params"]["system"]
        assert "recent_publication_changes" in served_system
        assert "live_orcid_lookup" not in served_system
        # The skills bundle is NOT inlined.
        assert "x" * 500 not in served_system

    def test_a_deployment_declaring_nothing_says_so_and_adds_no_placeholder(
        self, tmp_path: Path,
    ) -> None:
        """scn.absent-sources-named."""
        inputs = _make_inputs(
            tmp_path, manifest=_raw_manifest(tools=["search", "inspect"]),
        )
        instance = _instance()
        report = _run(inputs, instance)

        guidance = report.to_dict()["standing_guidance"]
        assert guidance["absent"] == list(GUIDANCE_KEYS)
        assert guidance["carried"] == []
        assert guidance["declared_not_carried"] == []
        for decision in guidance["decisions"]:
            assert decision["declared"] is False
            assert "declares no" in decision["reason"]

        # No placeholder implying guidance exists: the served prompt is
        # BYTE-IDENTICAL to the composed operator prompt.
        served_system = instance.model_row("okg-fixture")["params"]["system"]
        assert served_system == SYSTEM_PROMPT
        assert guidance["chars"] == 0
        # ... and the substrate default is still stated per projected tool.
        assert [d["source"] for d in guidance["tool_descriptions"]] == [
            "substrate_default", "substrate_default",
        ]

    def test_a_declared_but_EMPTY_overrides_file_is_not_reported_as_undeclared(
        self, tmp_path: Path,
    ) -> None:
        """An empty declared file is a MISTAKE IN THE FILE, not an absent key.

        ``load_tool_description_overrides`` returns ``{}`` without raising for
        a file that parses to nothing (operators.py:543-544), so the
        declared-but-empty case and the undeclared case reach
        :func:`compose_standing_guidance` as the same falsy mapping. Branching
        the reason on the mapping rather than on the PATH reported "the
        deployment declares no tool_description_overrides" to an operator who
        had in fact declared one — sending them to add a declaration that
        already existed instead of to the empty file. ``declared`` and
        ``carried`` are two booleans precisely so this pair stays
        distinguishable; this test is what stops them collapsing back.
        """
        path = tmp_path / "overrides.yaml"
        path.write_text("{}\n", encoding="utf-8")
        guidance = compose_standing_guidance(
            "fixture",
            projected_tools=("search",),
            tool_description_overrides_path=path,
            skills_dir=None,
            skill_triggers_path=None,
            agent_tools={},
        )
        row = next(
            k for k in guidance.keys if k.name == "tool_description_overrides"
        )
        assert row.declared is True
        assert row.carried is False
        assert "EMPTY" in row.reason
        assert "declares no tool_description_overrides" not in row.reason
        # The operator is pointed at the file to fix.
        assert row.detail["path"] == str(path)
        assert "tool_description_overrides" in guidance.declared_not_carried
        assert "tool_description_overrides" not in guidance.absent

        # Positive control: the UNDECLARED case still reports the honest
        # absence, so the arm above is not an unconditional rename.
        undeclared = compose_standing_guidance(
            "fixture",
            projected_tools=("search",),
            tool_description_overrides_path=None,
            skills_dir=None,
            skill_triggers_path=None,
            agent_tools={},
        )
        absent_row = next(
            k for k in undeclared.keys if k.name == "tool_description_overrides"
        )
        assert absent_row.declared is False
        assert "declares no tool_description_overrides" in absent_row.reason
        assert "path" not in absent_row.detail
        assert "tool_description_overrides" in undeclared.absent

    def test_declared_and_carried_are_different_facts(self, tmp_path: Path) -> None:
        """A single flag would render "absent" and "refused" identically."""
        declared = compose_standing_guidance(
            "fixture",
            projected_tools=("search",),
            tool_description_overrides_path=None,
            skills_dir=tmp_path / "skills",
            skill_triggers_path=None,
            agent_tools={},
        )
        assert declared.declared_not_carried == ("skills_dir",)
        assert "skill_triggers" in declared.absent
        assert declared.carried == ()

    def test_a_macro_naming_an_unprojected_operator_is_refused(self) -> None:
        """The rendered line is an INSTRUCTION, so its operator is checked.

        ``- `X` — use the `aggregate` operator`` goes into the preset's
        system prompt, which Open WebUI hands the model on every turn. If
        ``aggregate`` is not on the connection, the model is being told to
        call a tool it does not have; it cannot detect that, it can only fail
        the turn or invent. Until this arm existed, mutating the operator to
        a name that does not exist changed nothing the suite could see.
        """
        with pytest.raises(ChatSyncError) as raised:
            compose_standing_guidance(
                "fixture",
                projected_tools=("search", "inspect"),
                tool_description_overrides_path=None,
                skills_dir=None,
                skill_triggers_path=None,
                agent_tools=AGENT_TOOLS,
            )
        data = raised.value.data
        assert data["reason"] == "macro_names_unprojected_operator"
        assert data["agent_tool"] == "recent_publication_changes"
        assert data["operator"] == "aggregate"
        assert data["projected"] == ["inspect", "search"]

    def test_the_same_macro_over_a_projected_operator_renders(self) -> None:
        """The positive control: the refusal is not unconditional.

        Same macro, same code path — the only difference is that the operator
        it names is bound. Without this, the arm above could be an
        unconditional raise on any deployment declaring agent_tools.
        """
        guidance = compose_standing_guidance(
            "fixture",
            projected_tools=("search", "inspect", "aggregate"),
            tool_description_overrides_path=None,
            skills_dir=None,
            skill_triggers_path=None,
            agent_tools=AGENT_TOOLS,
        )
        assert "use the `aggregate` operator" in guidance.text
        assert "recent_publication_changes" in guidance.text

    @pytest.mark.parametrize(
        "operator", [None, 42, "", "AGGREGATE", "aggregate "],
        ids=["missing", "not-a-string", "empty", "wrong-case", "trailing-space"],
    )
    def test_a_macro_whose_operator_is_not_a_projected_NAME_is_refused(
        self, operator: Any,
    ) -> None:
        """No coercion, no normalisation, no near-miss acceptance.

        ``AGGREGATE`` and ``aggregate `` are not tool names Open WebUI will
        resolve, and a missing key is not "the default operator". Each is
        undetermined, and undetermined refuses.
        """
        body: dict[str, Any] = {"kind": "operator_macro"}
        if operator is not None:
            body["operator"] = operator
        with pytest.raises(ChatSyncError) as raised:
            compose_standing_guidance(
                "fixture",
                projected_tools=("search", "aggregate"),
                tool_description_overrides_path=None,
                skills_dir=None,
                skill_triggers_path=None,
                agent_tools={"macro": body},
            )
        assert raised.value.data["reason"] == "macro_names_unprojected_operator"

    def test_a_macro_naming_a_POSTURE_EXCLUDED_operator_is_refused(
        self, tmp_path: Path,
    ) -> None:
        """The live shape: declared in the manifest, removed by posture.

        A masked deployment declares ``query`` in ``chat.mcp.tools`` and a
        macro over it. The posture exclusion strips ``query`` from the served
        set, so the macro's operator is declared-but-unbound — and validating
        the macro against ``chat.mcp.tools`` instead of against the PROJECTED
        set would have missed exactly this case.
        """
        inputs = _make_inputs(
            tmp_path,
            manifest=_raw_manifest(
                tools=["search", "query"],
                enforcement="enforce",
                agent_tools={
                    "raw_lookup": {"kind": "operator_macro", "operator": "query"},
                },
            ),
        )
        with pytest.raises(ChatSyncError) as raised:
            _run(inputs, _instance())
        data = raised.value.data
        assert data["reason"] == "macro_names_unprojected_operator"
        assert data["operator"] == "query"
        assert data["projected"] == ["search"], "the posture-excluded set"

    def test_the_same_deployment_without_the_excluded_macro_projects(
        self, tmp_path: Path,
    ) -> None:
        """The positive control for the posture arm."""
        inputs = _make_inputs(
            tmp_path,
            manifest=_raw_manifest(
                tools=["search", "query"],
                enforcement="enforce",
                agent_tools={
                    "title_lookup": {"kind": "operator_macro", "operator": "search"},
                },
            ),
        )
        instance = _instance()
        report = _run(inputs, instance)
        assert report.ok
        assert _served_tools(instance) == ("search",)
        assert "use the `search` operator" in (
            instance.model_row("okg-fixture")["params"]["system"]
        )

    def test_an_over_limit_macro_is_still_validated(self) -> None:
        """A bad operator must not hide behind the rendering limit.

        The limit is a budget on how much reaches the prompt, not a licence
        to stop checking. Validating after the limit check would let entry
        41 carry an unresolvable operator until someone raised the cap.
        """
        macros: dict[str, Any] = {
            f"macro_{i:03d}": {"kind": "operator_macro", "operator": "search"}
            for i in range(45)
        }
        macros["zzz_last"] = {"kind": "operator_macro", "operator": "not_an_operator"}
        with pytest.raises(ChatSyncError) as raised:
            compose_standing_guidance(
                "fixture",
                projected_tools=("search",),
                tool_description_overrides_path=None,
                skills_dir=None,
                skill_triggers_path=None,
                agent_tools=macros,
            )
        assert raised.value.data["reason"] == "macro_names_unprojected_operator"
        assert raised.value.data["agent_tool"] == "zzz_last"

    def test_the_guidance_block_is_bounded(self, tmp_path: Path) -> None:
        """Unbounded prompt growth refuses rather than shipping."""
        big = {
            f"macro_{i}": {
                "kind": "operator_macro",
                "operator": "search",
                "description": "d" * 400,
            }
            for i in range(60)
        }
        with pytest.raises(ChatSyncError) as raised:
            compose_standing_guidance(
                "fixture",
                projected_tools=("search",),
                tool_description_overrides_path=None,
                skills_dir=None,
                skill_triggers_path=None,
                agent_tools=big,
            )
        assert raised.value.data["reason"] == "guidance_over_budget"


# ---------------------------------------------------------------------------
# A DECLARED path that cannot be read is never rendered as "declares none"
# (req.chat-tool-surface.standing-guidance -- "an absent override is a stated
# default, never silence" only holds if ABSENT is a fact rather than a fallback)
# ---------------------------------------------------------------------------

class TestUnresolvableManifestRefuses:
    """``load_inputs`` reads four agent-surface paths off the resolved manifest.

    If resolution fails, all four arrive as ``None`` -- and ``None`` means
    "the deployment declares this nowhere", which the guidance report states
    as a fact and ``load_tool_description_overrides`` accepts as the honest
    empty case. Coercing an unreadable manifest into that shape is the same
    defect this program refuses everywhere else: a MISSING signal read as a
    SATISFIED one. ``load_tool_description_overrides`` exists precisely to
    refuse a declared path it cannot read; swallowing the resolution failure
    means the refusal never receives a path to refuse.
    """

    def _prepare(self, tmp_path: Path) -> Path:
        dep_dir = tmp_path / "fixture"
        dep_dir.mkdir(parents=True, exist_ok=True)
        # JSON is valid YAML, so this is the fixture manifest on disk.
        (dep_dir / "deployment.yaml").write_text(
            json.dumps(_raw_manifest()), encoding="utf-8",
        )
        return dep_dir

    def _patch(
        self, monkeypatch: pytest.MonkeyPatch, dep_dir: Path, resolver: Any,
    ) -> None:
        from okg.substrate import deployments

        monkeypatch.setattr(deployments, "find_deployment_dir", lambda *_a, **_k: dep_dir)
        monkeypatch.setattr(deployments, "resolve_deployment", resolver)

    def test_an_unresolvable_manifest_refuses_rather_than_declaring_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        dep_dir = self._prepare(tmp_path)

        def boom(*_a: Any, **_k: Any) -> Any:
            raise RuntimeError("deployment module import failed")

        self._patch(monkeypatch, dep_dir, boom)
        with pytest.raises(ChatSyncError) as raised:
            load_inputs("fixture")

        data = raised.value.data
        assert data["reason"] == "deployment_unresolvable"
        # Every key the failure left UNDETERMINED is named, so no reader has
        # to infer which facts went missing.
        assert data["undetermined_keys"] == list(RESOLVED_AGENT_SURFACE_KEYS)
        assert set(RESOLVED_AGENT_SURFACE_KEYS) == {
            "mcp_instructions",
            "tool_description_overrides",
            "skills_dir",
            "skill_triggers",
        }
        assert "deployment module import failed" in data["error"]
        assert "UNDECLARED" in str(raised.value)

    def test_a_resolver_returning_nothing_is_the_same_refusal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``None`` back from the resolver is undetermined, not empty."""
        dep_dir = self._prepare(tmp_path)
        self._patch(monkeypatch, dep_dir, lambda *_a, **_k: None)
        with pytest.raises(ChatSyncError) as raised:
            load_inputs("fixture")
        assert raised.value.data["reason"] == "deployment_unresolvable"
        assert raised.value.data["error"] == "resolve_deployment returned None"

    def test_a_resolvable_manifest_carries_its_declared_paths(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The positive control: the refusal is not unconditional.

        The same call, the same manifest, a resolver that works -- and all
        four declared paths reach ``SyncInputs``. Without this, the refusal
        above could be an unconditional raise and nothing here would notice.
        """
        dep_dir = self._prepare(tmp_path)
        overrides = dep_dir / "tool_description_overrides.yaml"

        class _Resolved:
            mcp_instructions = dep_dir / "mcp_instructions.md"
            tool_description_overrides = overrides
            skills_dir = dep_dir / "skills"
            skill_triggers = dep_dir / "skill_triggers.yaml"

        self._patch(monkeypatch, dep_dir, lambda *_a, **_k: _Resolved())
        inputs = load_inputs("fixture")

        assert inputs.tool_description_overrides_path == overrides
        assert inputs.mcp_instructions_path == _Resolved.mcp_instructions
        assert inputs.skills_dir == _Resolved.skills_dir
        assert inputs.skill_triggers_path == _Resolved.skill_triggers

    def test_a_genuinely_undeclared_key_is_still_the_honest_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The other positive control: DECLARES-NONE survives as a real answer.

        Refusing an unresolvable manifest must not collapse into refusing
        every deployment that simply declares no overrides -- that would trade
        one wrong answer for another.
        """
        dep_dir = self._prepare(tmp_path)

        class _Resolved:
            mcp_instructions = None
            tool_description_overrides = None
            skills_dir = None
            skill_triggers = None

        self._patch(monkeypatch, dep_dir, lambda *_a, **_k: _Resolved())
        inputs = load_inputs("fixture")

        assert inputs.tool_description_overrides_path is None
        assert inputs.skills_dir is None
        # ... and the honest empty case still composes, rather than raising.
        assert load_tool_description_overrides(None) == {}


# ---------------------------------------------------------------------------
# A credential must never reach an error envelope
# ---------------------------------------------------------------------------

#: An operator-chosen loader credential that BOTH shape passes miss: 7
#: characters (under the 20-character floor of the token-shaped regex) and
#: punctuated (outside its `[A-Za-z0-9_.-]` class). Fabricated for this test.
SHORT_LOADER_TOKEN = "l0ad!er"

#: A second fabricated value, filed under a key whose NAME says nothing about
#: credentials -- the residual case the explicit `secrets` argument covers.
UNNAMED_CREDENTIAL = "x y!z"

LOADER_KEY = "EXTERNAL_DOCUMENT_LOADER_API_KEY"
LOADER_ROUTE = "/api/v1/retrieval/config/update"


def _echoing_422(body_payload: Any) -> Any:
    """An opener answering the measured FastAPI 422: `detail[].input` is the body.

    This is the real leak path, not a hypothetical. The document-loader
    projection POSTs the loader token in the request BODY
    (document_loader_projection.py:193); a drifted instance answers 422 with
    the submitted body echoed under `detail[].input`; and the client carried
    that body into `ChatSyncError.data['detail']`, which the CLI prints.
    """
    raw = json.dumps({
        "detail": [{
            "type": "missing",
            "loc": ["body", "engine"],
            "msg": "field required",
            "input": body_payload,
        }],
    })

    def opener(request: Any, _timeout: float) -> Any:
        raise urllib.error.HTTPError(
            request.full_url, 422, "Unprocessable Entity", {},  # type: ignore[arg-type]
            io.BytesIO(raw.encode("utf-8")),
        )

    return opener


def _envelope(error: ChatSyncError) -> str:
    """Everything an operator or an agent can see: message, data, fix hint."""
    return json.dumps(
        {
            "message": str(error),
            "data": error.data,
            "fix_hint": error.fix_hint,
        },
        default=str,
    )


class TestErrorEnvelopeRedaction:
    """The client is the only place that can scrub what it put on the wire."""

    def test_a_422_echoing_a_submitted_loader_token_does_not_leak_it(self) -> None:
        """The floor: no call-site cooperation required.

        `secrets` is deliberately NOT passed here. The value is scrubbed
        because it was filed under a key whose NAME says it is a credential,
        which is what makes the redaction non-optional rather than a thing a
        call site can forget.
        """
        payload = {
            "ENGINE": "external",
            LOADER_KEY: SHORT_LOADER_TOKEN,
            "EXTERNAL_DOCUMENT_LOADER_URL": "http://loader.test",
        }
        client = InstanceClient(BASE_URL, TOKEN, opener=_echoing_422(payload))
        with pytest.raises(ChatSyncError) as raised:
            client.call("POST", LOADER_ROUTE, payload)

        assert SHORT_LOADER_TOKEN not in _envelope(raised.value)
        assert "<redacted>" in raised.value.data["detail"]
        # ... and the envelope is still diagnosable: the redaction removes the
        # credential, not the reason.
        assert "field required" in raised.value.data["detail"]
        assert "http://loader.test" in raised.value.data["detail"]

    def test_a_declared_secret_is_redacted_whatever_its_length_or_charset(
        self,
    ) -> None:
        """The ceiling: a credential the vendor files under a neutral key.

        Nothing about the key `blob` says credential, and the value is five
        characters with a space and a `!` in it -- invisible to both the
        key-name sweep and the token-shaped regex. Declaring it is the only
        thing that can scrub it, which is why `call` takes `secrets`.
        """
        payload = {"blob": UNNAMED_CREDENTIAL, "site_name": "Fixture Site"}
        client = InstanceClient(BASE_URL, TOKEN, opener=_echoing_422(payload))
        with pytest.raises(ChatSyncError) as raised:
            client.call("POST", LOADER_ROUTE, payload, secrets=(UNNAMED_CREDENTIAL,))

        assert UNNAMED_CREDENTIAL not in _envelope(raised.value)
        assert "Fixture Site" in raised.value.data["detail"], (
            "a non-credential value must survive, or the assertion above "
            "would pass on an empty detail"
        )

    def test_the_same_value_undeclared_and_unnamed_is_NOT_redacted(self) -> None:
        """The non-vacuity control, and an honest statement of the residual.

        Same body, same route, same client -- with neither lever engaged.
        The value survives. So the two assertions above are detecting the
        redaction rather than an empty or truncated detail, and the residual
        exposure is stated rather than implied: a credential under a
        neutral key with no `secrets` declaration is NOT covered, and a call
        site putting one there must declare it.
        """
        payload = {"blob": UNNAMED_CREDENTIAL}
        client = InstanceClient(BASE_URL, TOKEN, opener=_echoing_422(payload))
        with pytest.raises(ChatSyncError) as raised:
            client.call("POST", LOADER_ROUTE, payload)
        assert UNNAMED_CREDENTIAL in raised.value.data["detail"]

    def test_the_admin_token_is_still_redacted(self) -> None:
        """Regression: the pre-existing arm did not weaken."""
        payload = {"note": TOKEN}
        client = InstanceClient(BASE_URL, TOKEN, opener=_echoing_422(payload))
        with pytest.raises(ChatSyncError) as raised:
            client.call("POST", LOADER_ROUTE, payload)
        assert TOKEN not in _envelope(raised.value)

    def test_a_400_ANSWER_is_scrubbed_on_the_same_terms_as_a_raised_one(
        self,
    ) -> None:
        """Two arms return an error body; both must scrub.

        An opener that ANSWERS 4xx (rather than raising `HTTPError`) reaches a
        different branch of `call`. Scrubbing one and not the other would
        leave a leak that only shows up against instances whose client stack
        does not raise.
        """
        payload = {LOADER_KEY: SHORT_LOADER_TOKEN}
        body = json.dumps({"detail": [{"input": payload}]})

        def opener(_request: Any, _timeout: float) -> tuple[int, str]:
            return 422, body

        client = InstanceClient(BASE_URL, TOKEN, opener=opener)
        with pytest.raises(ChatSyncError) as raised:
            client.call("POST", LOADER_ROUTE, payload)
        assert raised.value.data["reason"] == "http_error"
        assert SHORT_LOADER_TOKEN not in _envelope(raised.value)

    def test_an_unreachable_instance_message_is_scrubbed_too(self) -> None:
        """The third arm: a transport error whose message quotes the request."""
        payload = {LOADER_KEY: SHORT_LOADER_TOKEN}

        def opener(_request: Any, _timeout: float) -> tuple[int, str]:
            raise urllib.error.URLError(f"refused while sending {SHORT_LOADER_TOKEN}")

        client = InstanceClient(BASE_URL, TOKEN, opener=opener)
        with pytest.raises(ChatSyncError) as raised:
            client.call("POST", LOADER_ROUTE, payload)
        assert raised.value.data["reason"] == "unreachable"
        assert SHORT_LOADER_TOKEN not in _envelope(raised.value)

    def test_a_secret_that_contains_another_is_fully_removed(self) -> None:
        """Longest-first, or the containing secret is left half-rewritten.

        Replacing `ab` first turns `abcd` into `<redacted>cd`, and `cd` is
        then unmatchable -- a partial credential printed is still a leak.
        """
        outer, inner = "ab!cd", "ab"
        payload = {"blob": outer}
        client = InstanceClient(BASE_URL, TOKEN, opener=_echoing_422(payload))
        with pytest.raises(ChatSyncError) as raised:
            client.call("POST", LOADER_ROUTE, payload, secrets=(inner, outer))
        assert "cd" not in raised.value.data["detail"]


class TestProbeCredentialRedaction:
    """The OTHER error path that carries a second credential.

    ``_probe_mcp`` sends the value NAMED by ``chat.mcp.auth_token_env``, and
    its refusal quotes the transport error. The code comment there already
    said this was "the ONE error path carrying a second credential" -- while
    redacting by SHAPE only, which is precisely the filter an operator-chosen
    token walks past. Naming a risk is not covering it.
    """

    PROBE_ENV = "OKG_FIXTURE_MCP_TOKEN"
    PROBE_TOKEN = "mcp!tok"

    def _inputs(self, tmp_path: Path) -> SyncInputs:
        manifest = _raw_manifest(tools=["search", "inspect"])
        manifest["chat"]["mcp"]["auth_token_env"] = self.PROBE_ENV
        return _make_inputs(tmp_path, manifest=manifest)

    def test_a_probe_failure_quoting_the_token_does_not_print_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(self.PROBE_ENV, self.PROBE_TOKEN)

        def boom(_endpoint: str, _env: str | None) -> Any:
            raise ConnectionError(
                f"401 Unauthorized for Bearer {self.PROBE_TOKEN} at /mcp"
            )

        with pytest.raises(ChatSyncError) as raised:
            sync(
                self._inputs(tmp_path),
                InstanceClient(BASE_URL, TOKEN, opener=_instance().opener),
                probe_tools=boom,
            )
        assert raised.value.data["reason"] == "mcp_unreachable"
        assert self.PROBE_TOKEN not in _envelope(raised.value)
        # ... and the refusal still says what happened. Both the message and
        # the report's `mcp.reason` are rendered from the same scrubbed
        # string, so one assertion covers both surfaces.
        assert "401 Unauthorized" in str(raised.value)
        assert "<redacted>" in str(raised.value)

    def test_the_same_failure_with_no_declared_env_still_refuses(
        self, tmp_path: Path,
    ) -> None:
        """The positive control: the arm is not an unconditional raise.

        No ``auth_token_env``, no resolved value, and the refusal still
        carries its diagnosis -- so the assertion above is detecting the
        redaction and not a probe path that stopped reporting.
        """
        def boom(_endpoint: str, _env: str | None) -> Any:
            raise ConnectionError("connection refused at /mcp")

        with pytest.raises(ChatSyncError) as raised:
            sync(
                _make_inputs(tmp_path, manifest=_raw_manifest(tools=["search"])),
                InstanceClient(BASE_URL, TOKEN, opener=_instance().opener),
                probe_tools=boom,
            )
        assert "connection refused" in str(raised.value)


class TestPayloadSecretSweep:
    """What the automatic floor does and does not claim to cover."""

    def test_it_finds_credential_named_keys_at_every_depth(self) -> None:
        found = payload_secrets({
            LOADER_KEY: "a",
            "config": {"key": "b", "headers": {"Authorization": "c"}},
            "connections": [{"api_token": "d"}, {"password": "e"}],
            "nested": {"deep": {"client_secret": "f"}},
        })
        assert sorted(found) == ["a", "b", "c", "d", "e", "f"]

    def test_the_over_wide_match_is_deliberate_and_stated(self) -> None:
        """`auth_type: bearer` is swept, and that is the accepted trade.

        The fragment list matches a SUBSTRING of the key, so a discriminator
        like `auth_type` is swept alongside real credentials. The cost is one
        scheme word replaced in an error message; the alternative -- carving
        out key names that "obviously" cannot be credentials -- is a hole
        maintained by judgement. Pinned here so the behaviour is a stated
        consequence rather than a surprise in a support transcript.
        """
        assert payload_secrets({"auth_type": "bearer"}) == ("bearer",)

    def test_it_leaves_non_credential_values_alone(self) -> None:
        """Or every error message would come back as `<redacted>`."""
        assert payload_secrets({
            "site_name": "Fixture Site",
            "url": "http://loader.test",
            "enable": True,
            "toolIds": ["server:mcp:okg-fixture"],
            "description": "Ask the fixture graph.",
        }) == ()

    def test_it_ignores_non_string_and_empty_values(self) -> None:
        assert payload_secrets({"key": "", "token": None, "secret": 7}) == ()

    def test_it_survives_a_payload_that_is_not_a_mapping(self) -> None:
        assert payload_secrets(None) == ()
        assert payload_secrets("just a string") == ()
        assert payload_secrets([{"key": "z"}]) == ("z",)
