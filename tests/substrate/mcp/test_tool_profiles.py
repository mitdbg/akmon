"""Profile resolution, manifest parsing, and selector precedence.

PACT `mcp-tool-surface-profiles` (issue #1457), req.mcp.named-tool-profiles.

Pure argument/manifest validation — no server, no database — so every arm is
provable without Postgres.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.mcp.operators import (
    ALL_ADMITTED_TOOLS,
    OPERATOR_TOOLS,
    WRITE_ADMIN_TOOLS,
)
from okg.substrate.mcp.tool_profiles import (
    BUILTIN_TOOL_PROFILES,
    RESERVED_FULL,
    ToolProfileConfigInvalid,
    ToolProfileToolAbsent,
    ToolSurfaceSelectionConflict,
    UnknownToolProfile,
    assert_narrowing_is_servable,
    parse_tool_profile_config,
    render_tool_list,
    resolve_tool_surface,
    tools_named_in,
)


# ---------- built-in profiles ----------

def test_discovery_profile_is_exactly_the_minimal_read_set() -> None:
    """Pinned by name, because the measured 34% saving in issue #1457 is a
    claim about THIS set. A member added here changes what every operator
    who typed `--tools discovery` gets."""
    assert BUILTIN_TOOL_PROFILES["discovery"] == ("aggregate", "inspect", "search")


def test_reader_profile_is_the_full_read_algebra() -> None:
    assert set(BUILTIN_TOOL_PROFILES["reader"]) == set(OPERATOR_TOOLS)


@pytest.mark.parametrize("name", sorted(BUILTIN_TOOL_PROFILES))
def test_every_builtin_profile_narrows_within_the_read_surface(name: str) -> None:
    """A profile may only NARROW. One that named a write tool would look
    like a way to widen past the --allow-writes boundary."""
    assert set(BUILTIN_TOOL_PROFILES[name]) <= set(OPERATOR_TOOLS)


@pytest.mark.parametrize("name", sorted(BUILTIN_TOOL_PROFILES))
def test_builtin_profiles_resolve_to_their_declared_tools(name: str) -> None:
    resolved = resolve_tool_surface(
        requested_profile=name, env_allowlist=None,
    )
    assert resolved.profile == name
    assert resolved.source == "cli"
    assert resolved.tools == frozenset(BUILTIN_TOOL_PROFILES[name])


def test_reserved_full_means_no_narrowing_at_all() -> None:
    resolved = resolve_tool_surface(
        requested_profile=RESERVED_FULL, env_allowlist=None,
    )
    assert resolved.profile == RESERVED_FULL
    assert resolved.tools is None
    assert resolved.is_narrowed is False


def test_an_unknown_profile_refuses_and_names_what_exists() -> None:
    """Never degrades to the unnarrowed surface: a typo'd profile silently
    serving the full 18,823-byte surface is the cost this feature avoids,
    and the operator would have no signal it happened."""
    with pytest.raises(UnknownToolProfile) as excinfo:
        resolve_tool_surface(requested_profile="discovry", env_allowlist=None)
    message = str(excinfo.value)
    assert "discovry" in message
    assert "discovery" in message
    assert RESERVED_FULL in message


# ---------- manifest block ----------

def test_absent_block_parses_to_none() -> None:
    assert parse_tool_profile_config(None) is None


def test_declared_profile_resolves() -> None:
    config = parse_tool_profile_config(
        {"tool_profiles": {"analyst": {"tools": ["inspect", "search", "expand"]}}}
    )
    assert config is not None
    assert config.declared_names == ("analyst",)
    resolved = resolve_tool_surface(
        requested_profile="analyst", env_allowlist=None, config=config,
    )
    assert resolved.tools == frozenset({"inspect", "search", "expand"})


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param({"tool_profles": {}}, id="misspelled-block-key"),
        pytest.param(
            {"tool_profiles": {"a": {"tols": ["inspect"]}}}, id="misspelled-profile-key"
        ),
        pytest.param({"tool_profiles": {"a": {"tools": []}}}, id="empty-tool-list"),
        pytest.param({"tool_profiles": {"a": {"tools": ["inspect", ""]}}}, id="blank-name"),
        pytest.param({"tool_profiles": {"a": {"tools": ["serach"]}}}, id="unknown-tool"),
        pytest.param({"tool_profiles": {"a": ["inspect"]}}, id="profile-not-a-mapping"),
        pytest.param({"tool_profiles": ["a"]}, id="profiles-not-a-mapping"),
        pytest.param(["mcp"], id="block-not-a-mapping"),
    ],
)
def test_a_malformed_block_raises_at_load(raw: object) -> None:
    """Strict rather than tolerant: a silently-ignored `default_tool_profle:`
    leaves a deployment believing it bounded its MCP surface when it did
    not."""
    with pytest.raises(ToolProfileConfigInvalid):
        parse_tool_profile_config(raw)


@pytest.mark.parametrize("name", [*sorted(BUILTIN_TOOL_PROFILES), RESERVED_FULL])
def test_a_reserved_or_builtin_name_cannot_be_redeclared(name: str) -> None:
    """`--tools discovery` must mean the same thing on every
    deployment, or the launch command stops being readable."""
    with pytest.raises(ToolProfileConfigInvalid) as excinfo:
        parse_tool_profile_config(
            {"tool_profiles": {name: {"tools": ["inspect"]}}}
        )
    assert name in str(excinfo.value)


def test_a_default_naming_an_undeclared_profile_raises() -> None:
    with pytest.raises(UnknownToolProfile):
        parse_tool_profile_config({"default_tool_profile": "nope"})


def test_a_default_may_name_a_builtin_or_the_reserved_name() -> None:
    for name in ("discovery", "reader", RESERVED_FULL):
        config = parse_tool_profile_config({"default_tool_profile": name})
        assert config is not None
        assert config.default_tool_profile == name


# ---------- selector precedence ----------

def test_no_selector_and_no_default_is_unnarrowed() -> None:
    resolved = resolve_tool_surface(requested_profile=None, env_allowlist=None)
    assert resolved.tools is None
    assert resolved.source == "unnarrowed"


def test_a_manifest_default_applies_when_nothing_is_explicit() -> None:
    config = parse_tool_profile_config({"default_tool_profile": "discovery"})
    resolved = resolve_tool_surface(
        requested_profile=None, env_allowlist=None, config=config,
    )
    assert resolved.profile == "discovery"
    assert resolved.source == "manifest_default"


def test_an_explicit_flag_overrides_the_manifest_default() -> None:
    """The surface a reader infers from the launch command is the surface
    served."""
    config = parse_tool_profile_config({"default_tool_profile": "discovery"})
    resolved = resolve_tool_surface(
        requested_profile="reader", env_allowlist=None, config=config,
    )
    assert resolved.profile == "reader"
    assert resolved.source == "cli"


def test_an_explicit_env_allowlist_overrides_the_manifest_default() -> None:
    config = parse_tool_profile_config({"default_tool_profile": "discovery"})
    resolved = resolve_tool_surface(
        requested_profile=None, env_allowlist={"inspect"}, config=config,
    )
    assert resolved.source == "env_allowlist"
    assert resolved.tools == frozenset({"inspect"})
    assert resolved.profile is None


def test_two_explicit_selectors_at_once_is_refused() -> None:
    """Refused, not merged. An intersection would serve a set matching
    neither reading of the launch command, and preferring one would
    silently discard the other."""
    with pytest.raises(ToolSurfaceSelectionConflict) as excinfo:
        resolve_tool_surface(
            requested_profile="discovery", env_allowlist={"inspect"},
        )
    message = str(excinfo.value)
    assert "--tools" in message
    assert "OKG_MCP_TOOL_ALLOWLIST" in message


def test_an_unresolvable_manifest_flags_the_unhonored_default() -> None:
    """A manifest this checkout cannot read may declare a bound the launch
    then fails to honor. Reading "unreadable" as "declares nothing" would
    serve the FULL surface with no signal — the widening this module's
    contract forbids."""
    resolved = resolve_tool_surface(
        requested_profile=None, env_allowlist=None,
        config=None, manifest_unresolved=True,
    )
    assert resolved.tools is None
    assert resolved.unhonored_manifest_default is True
    assert resolved.source == "unnarrowed_manifest_unresolved"


def test_a_resolvable_manifest_declaring_nothing_flags_nothing() -> None:
    """Non-vacuity partner: the flag means UNREADABLE, not merely absent.
    Firing on every deployment without an `mcp:` block would make it noise."""
    resolved = resolve_tool_surface(
        requested_profile=None, env_allowlist=None,
        config=None, manifest_unresolved=False,
    )
    assert resolved.unhonored_manifest_default is False
    assert resolved.source == "unnarrowed"


@pytest.mark.parametrize(
    "selector",
    [
        pytest.param({"requested_profile": "discovery", "env_allowlist": None},
                     id="cli"),
        pytest.param({"requested_profile": None, "env_allowlist": {"inspect"}},
                     id="env"),
    ],
)
def test_an_explicit_selector_clears_the_unhonored_default_flag(
    selector: dict,
) -> None:
    """The operator stated the surface, so there is no unread default to
    warn about; warning anyway would be crying wolf."""
    resolved = resolve_tool_surface(manifest_unresolved=True, **selector)
    assert resolved.unhonored_manifest_default is False
    assert resolved.is_narrowed


def test_an_empty_env_allowlist_is_refused_not_served() -> None:
    """A server with no tools cannot be told apart from one whose narrowing
    was dropped on the floor."""
    with pytest.raises(ToolProfileConfigInvalid):
        resolve_tool_surface(requested_profile=None, env_allowlist=set())


# ---------- fail-closed narrowing ----------

def test_a_servable_narrowing_passes() -> None:
    assert_narrowing_is_servable(
        {"inspect", "search"}, registered=set(OPERATOR_TOOLS),
    ) is None


def test_a_misspelled_tool_refuses_and_names_it() -> None:
    """Before this change `inspect,serach` served only `inspect` with no
    refusal and no warning: absence dressed as agreement."""
    with pytest.raises(ToolProfileToolAbsent) as excinfo:
        assert_narrowing_is_servable(
            {"inspect", "serach"}, registered=set(OPERATOR_TOOLS),
        )
    message = str(excinfo.value)
    assert "serach" in message
    assert "not OKG MCP tools at all" in message


def test_a_write_tool_on_a_read_only_launch_refuses_with_a_distinct_reason() -> None:
    """A real tool this launch withholds is not a typo, and the operator fix
    is --allow-writes rather than a spelling correction."""
    with pytest.raises(ToolProfileToolAbsent) as excinfo:
        assert_narrowing_is_servable(
            {"inspect", "record"}, registered=set(OPERATOR_TOOLS),
        )
    message = str(excinfo.value)
    assert "record" in message
    assert "--allow-writes" in message
    assert "not OKG MCP tools at all" not in message
    assert excinfo.value.data["withheld_by_launch"] == ["record"]
    assert excinfo.value.data["unrecognized"] == []


def test_the_refusal_separates_typos_from_withheld_tools() -> None:
    with pytest.raises(ToolProfileToolAbsent) as excinfo:
        assert_narrowing_is_servable(
            {"serach", "record"}, registered=set(OPERATOR_TOOLS),
        )
    assert excinfo.value.data["unrecognized"] == ["serach"]
    assert excinfo.value.data["withheld_by_launch"] == ["record"]


def test_every_known_tool_is_narrowable_on_a_write_launch() -> None:
    """Non-vacuity partner: the refusal above is about ABSENCE, not about
    the write tools being unnameable."""
    assert_narrowing_is_servable(
        set(WRITE_ADMIN_TOOLS),
        registered=set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS),
    )


# ---------- rendering helpers ----------

def test_tool_lists_read_in_canonical_surface_order() -> None:
    assert render_tool_list(OPERATOR_TOOLS) == (
        "inspect, search, expand, filter, map, aggregate, and query"
    )
    assert render_tool_list(["search", "inspect"]) == "inspect and search"
    assert render_tool_list(["search"]) == "search"


def test_tool_name_detection_is_word_bounded() -> None:
    """`map` must not be reported for `mapping`, or the instruction warning
    would fire on prose that names no tool."""
    assert tools_named_in("use `map` then `filter`") == ("filter", "map")
    assert tools_named_in("a mapping of searches") == ()
    assert set(tools_named_in("call inspect")) <= set(ALL_ADMITTED_TOOLS)


# ---------- the manifest door reaches a real server ----------
#
# Hermetic: OKG_MCP_SURFACE_PROBE builds both pools UNOPENED, so a real
# `build_server` registers the real surface against an unreachable loopback
# DSN with no database.

def _write_deployment(root: Path, *, name: str, body: str) -> None:
    directory = root / name
    (directory / "schemas").mkdir(parents=True)
    (directory / "source_registry.yaml").write_text("", encoding="utf-8")
    (directory / "deployment.yaml").write_text(
        f"name: {name}\n"
        "description: test\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n" + body,
        encoding="utf-8",
    )


@pytest.fixture
def probe(monkeypatch: pytest.MonkeyPatch):
    """Build servers without a database, and without ambient narrowing."""
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )

    built: list[object] = []

    def _build(**kwargs):
        from okg.substrate.mcp.server import build_server

        mcp = build_server(dsn=PROBE_DSN, **kwargs)
        built.append(mcp)
        return mcp

    try:
        yield _build
    finally:
        for mcp in built:
            mcp._okg_pool.close()


def test_a_manifest_default_profile_narrows_a_real_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
) -> None:
    root = tmp_path / "deployments"
    _write_deployment(
        root, name="narrow", body="mcp:\n  default_tool_profile: discovery\n",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    mcp = probe(deployment_name="narrow")
    assert mcp._okg_served_tools == ("aggregate", "inspect", "search")
    assert mcp._okg_tool_profile == "discovery"
    assert mcp._okg_tool_profile_source == "manifest_default"


def test_a_manifest_declared_profile_is_selectable_by_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
) -> None:
    root = tmp_path / "deployments"
    _write_deployment(
        root,
        name="declared",
        body=(
            "mcp:\n"
            "  tool_profiles:\n"
            "    analyst:\n"
            "      tools: [inspect, search, expand]\n"
        ),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    mcp = probe(deployment_name="declared", tool_profile="analyst")
    assert mcp._okg_served_tools == ("expand", "inspect", "search")
    assert mcp._okg_tool_profile_source == "cli"


def test_a_misspelled_key_under_mcp_fails_at_manifest_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The top-level census matches only top-level NAMES, so a nested typo
    would otherwise leave the deployment serving the full surface while
    believing it had bounded it."""
    from okg.substrate.deployments import resolve_deployment

    root = tmp_path / "deployments"
    _write_deployment(
        root, name="typo", body="mcp:\n  default_tool_profle: discovery\n",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    with pytest.raises(ToolProfileConfigInvalid) as excinfo:
        resolve_deployment("typo")
    assert "default_tool_profle" in str(excinfo.value)


def test_a_deployment_instruction_file_naming_a_pruned_tool_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
) -> None:
    """The deployment's own text stays authoritative — rewriting an
    operator's prose would be worse than the inconsistency — but the
    readback says which tools it names that this launch does not serve."""
    root = tmp_path / "deployments"
    _write_deployment(
        root,
        name="authored",
        body=(
            "mcp_instructions: mcp-instructions.md\n"
            "mcp:\n  default_tool_profile: discovery\n"
        ),
    )
    (root / "authored" / "mcp-instructions.md").write_text(
        "Call `expand` to traverse, then `query` for SQL, then `search`.",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    mcp = probe(deployment_name="authored")
    # Warned, not refused: the server still starts and still serves the
    # deployment's own text.
    assert mcp._okg_instruction_pruned_tools == ("expand", "query")
    assert mcp._okg_instruction_source == "deployment_file"
    assert mcp._okg_served_tools == ("aggregate", "inspect", "search")
    assert "Call `expand` to traverse" in (mcp.instructions or "")


def test_a_narrowed_server_without_a_deployment_file_renders_its_own(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
) -> None:
    root = tmp_path / "deployments"
    _write_deployment(
        root, name="rendered", body="mcp:\n  default_tool_profile: discovery\n",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    mcp = probe(deployment_name="rendered")
    assert mcp._okg_instruction_source == "rendered"
    assert mcp._okg_instruction_pruned_tools == ()
    for pruned in ("expand", "filter", "map", "query"):
        assert f"`{pruned}`" not in (mcp.instructions or "")


def test_a_deployment_instruction_file_within_the_surface_warns_of_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
) -> None:
    """Non-vacuity partner: the warning above is about the NARROWING, not
    about deployment instruction files in general."""
    root = tmp_path / "deployments"
    _write_deployment(
        root,
        name="consistent",
        body=(
            "mcp_instructions: mcp-instructions.md\n"
            "mcp:\n  default_tool_profile: discovery\n"
        ),
    )
    (root / "consistent" / "mcp-instructions.md").write_text(
        "Use `search` and `inspect`.", encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    mcp = probe(deployment_name="consistent")
    assert mcp._okg_instruction_pruned_tools == ()


def test_the_instruction_warning_is_emitted_not_merely_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A readback nobody reads is not a warning. This pins the log line."""
    root = tmp_path / "deployments"
    _write_deployment(
        root,
        name="loud",
        body=(
            "mcp_instructions: mcp-instructions.md\n"
            "mcp:\n  default_tool_profile: discovery\n"
        ),
    )
    (root / "loud" / "mcp-instructions.md").write_text(
        "Call `expand` to traverse.", encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    with caplog.at_level("WARNING", logger="okg.substrate.mcp.server"):
        mcp = probe(deployment_name="loud")
    assert any("expand" in record.getMessage()
               for record in caplog.records), caplog.text
    assert mcp._okg_surface_freshness["tool_surface"][
        "instructions_name_unserved_tools"
    ] == ["expand"]


# ---------- a declared bound that cannot be read is not agreement ----------

def test_an_unresolvable_named_deployment_says_the_bound_was_not_honored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A named deployment this checkout cannot resolve may declare a tool
    bound. Serving the full surface silently is the widening the module's
    contract forbids, so it is served WITH a loud statement."""
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "empty"))
    with caplog.at_level("WARNING", logger="okg.substrate.mcp.server"):
        mcp = probe(deployment_name="absent-elsewhere")
    assert mcp._okg_unhonored_manifest_default is True
    assert mcp._okg_surface_freshness["tool_surface"][
        "unhonored_manifest_default"
    ] is True
    assert any("could NOT be honored" in record.getMessage()
               for record in caplog.records), caplog.text


def test_the_adhoc_default_deployment_does_not_cry_wolf(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
) -> None:
    """`default` has no manifest BY DESIGN — it is the ad-hoc `--dsn` path —
    so flagging it would fire on every ordinary launch and train operators
    to ignore the line."""
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "empty"))
    mcp = probe(deployment_name="default")
    assert mcp._okg_unhonored_manifest_default is False


def test_an_explicit_profile_silences_the_unresolved_manifest_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, probe,
) -> None:
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "empty"))
    mcp = probe(deployment_name="absent-elsewhere", tool_profile="discovery")
    assert mcp._okg_unhonored_manifest_default is False
    assert mcp._okg_served_tools == ("aggregate", "inspect", "search")
