"""The start-time capability probe and what it publishes (#3151 slice 1).

PACT `prompt-compiler-3151-slice1`. Hermetic: the probe's reads run against a
scripted fake connection, and the server-level tests build under
`OKG_MCP_SURFACE_PROBE=1` with a named capability fixture, so nothing here
needs Postgres. The real start against a disposable database is recorded as
evidence in the PACT directory.
"""
from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Any

import psycopg
import pytest

from okg.substrate.mcp import capability_probe as cp
from okg.substrate.mcp.capability_probe import (
    AVAILABLE,
    CAPABILITY_FIXTURES,
    GENERATED_INSTRUCTIONS_BUDGET_BYTES,
    NOT_AVAILABLE_HEADER,
    UNAVAILABLE,
    UNKNOWN,
    Capability,
    CapabilityFixtureInvalid,
    CapabilityProbeFailed,
    SearchMethodUnavailable,
    SearchRankByUnavailable,
    SearchSchemaUnreachable,
    assert_search_capability,
    capabilities_for_surface_probe,
    compile_tool_surface,
    compose_instructions,
    probe_capabilities,
    probe_capabilities_from_pool,
    render_unavailable_lines,
    split_generated_instructions,
)
from tests.substrate.mcp._result_payload import call_json, payload


# ── a scripted connection ─────────────────────────────────────────────


class _Cursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self._conn.statements.append(sql)
        if "set_config('statement_timeout'" in sql:
            self._rows = [(params["ms"],)]
            return
        if sql == "RESET statement_timeout":
            self._rows = []
            return
        for needle, outcome in self._conn.script.items():
            if needle in sql:
                if isinstance(outcome, BaseException):
                    raise outcome
                self._rows = list(outcome)
                return
        raise AssertionError(f"unscripted probe statement: {sql}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _FakeConn:
    """Answers each probe read by a substring of its SQL."""

    def __init__(self, script: dict[str, Any]) -> None:
        self.script = script
        self.statements: list[str] = []

    @contextmanager
    def transaction(self):
        yield self

    def cursor(self) -> _Cursor:
        return _Cursor(self)


ALIASES = "FROM okg.aliases"
ROLLUPS = "FROM okg.subtypes"


def _pin(monkeypatch: pytest.MonkeyPatch, outcome: Any) -> None:
    """Script the pin read (`resolve_okg_generation`). A tuple is
    (generation_id, catalog_version_id); None is "no published generation"."""
    from types import SimpleNamespace

    from okg.substrate import generation_resolver

    def _resolve(conn, **_kwargs):
        if isinstance(outcome, BaseException):
            raise outcome
        if outcome is None:
            return None
        return SimpleNamespace(
            generation_id=outcome[0], catalog_version_id=outcome[1],
        )

    monkeypatch.setattr(generation_resolver, "resolve_okg_generation", _resolve)


def _probe(conn: _FakeConn, *, embedder: bool = False):
    return probe_capabilities(
        conn, deployment_name="d", embedder_configured=embedder,
    )


# ── every probe outcome ───────────────────────────────────────────────


def test_aliases_and_rollups_present_publish_alias_and_passages(monkeypatch) -> None:
    _pin(monkeypatch, ("g7", 41))
    conn = _FakeConn({
        ALIASES: [(True,)],
        ROLLUPS: [("document",), ("ticket",)],
    })
    caps = _probe(conn, embedder=True)
    assert caps.mode == "live"
    assert caps.available_methods() == (
        "lexical", "semantic", "alias", "identifier_mentions", "attrs", "grep",
    )
    assert caps.available_rank_by() == ("records", "self", "passages")
    assert caps.rollup_subtypes == ("document", "ticket")
    assert caps.pinned == {
        "basis": "latest_published_generation",
        "generation_id": "g7",
        "catalog_version_id": 41,
    }
    assert caps.reads == ("pin", "aliases", "passage_rollups")
    assert caps.probe_ms is not None and caps.probe_ms >= 0
    assert caps.method("semantic").source == "config"
    assert caps.method("alias").source == "catalog"
    # Every read carried a statement timeout: the pin sets one for the
    # session and resets it, the two catalog reads set their own.
    timeouts = [s for s in conn.statements if "statement_timeout" in s]
    assert len(timeouts) == 4
    assert "RESET statement_timeout" in conn.statements


def test_a_bare_server_publishes_neither(monkeypatch) -> None:
    _pin(monkeypatch, ("g7", 41))
    caps = _probe(_FakeConn({ALIASES: [(False,)], ROLLUPS: []}))
    assert caps.available_methods() == ("lexical", "identifier_mentions", "attrs", "grep")
    assert caps.method("semantic") == Capability(
        UNAVAILABLE, "no embedder is configured", "config",
    )
    assert caps.method("alias").status == UNAVAILABLE
    assert "catalog version 41" in caps.method("alias").reason
    assert caps.rank_by_value("passages").status == UNAVAILABLE
    assert "catalog version 41" in caps.rank_by_value("passages").reason


def test_a_timed_out_read_is_unknown_never_available(monkeypatch) -> None:
    """Decision 2 (b): a timeout publishes the capability as unavailable,
    with a "not verified" reason, and the next read still runs."""
    _pin(monkeypatch, ("g7", 41))
    caps = _probe(_FakeConn({
        ALIASES: psycopg.errors.QueryCanceled("canceling statement due to statement timeout"),
        ROLLUPS: [("document",)],
    }))
    alias = caps.method("alias")
    assert alias.status == UNKNOWN
    assert alias.reason.startswith("not verified at start")
    assert "alias" not in caps.available_methods()
    assert caps.rank_by_value("passages").status == AVAILABLE


def test_a_timed_out_rollup_read_is_unknown(monkeypatch) -> None:
    _pin(monkeypatch, ("g7", 41))
    caps = _probe(_FakeConn({
        ALIASES: [(True,)],
        ROLLUPS: psycopg.errors.QueryCanceled("timeout"),
    }))
    assert caps.rank_by_value("passages").status == UNKNOWN
    assert "passages" not in caps.available_rank_by()


def test_a_failed_read_refuses_start_naming_the_read(monkeypatch) -> None:
    """Decision 2 (a): an ERROR is not a timeout. It refuses the start."""
    _pin(monkeypatch, ("g7", 41))
    with pytest.raises(CapabilityProbeFailed) as excinfo:
        _probe(_FakeConn({
            ALIASES: psycopg.errors.InsufficientPrivilege("permission denied"),
            ROLLUPS: [],
        }))
    assert excinfo.value.code == "agent_surface_probe_failed"
    assert excinfo.value.data["read"] == "aliases"
    assert excinfo.value.data["error_type"] == "InsufficientPrivilege"
    assert "'aliases'" in str(excinfo.value)


def test_a_failed_rollup_read_refuses_start(monkeypatch) -> None:
    _pin(monkeypatch, ("g7", 41))
    with pytest.raises(CapabilityProbeFailed) as excinfo:
        _probe(_FakeConn({
            ALIASES: [(False,)],
            ROLLUPS: psycopg.errors.UndefinedTable("relation does not exist"),
        }))
    assert excinfo.value.data["read"] == "passage_rollups"


def test_a_failed_pin_read_refuses_start(monkeypatch) -> None:
    _pin(monkeypatch, psycopg.OperationalError("server closed the connection"))
    with pytest.raises(CapabilityProbeFailed) as excinfo:
        _probe(_FakeConn({}))
    assert excinfo.value.data["read"] == "pin"


class _MidCommandConn(_FakeConn):
    """A connection a stop signal interrupted mid-query: any further command
    fails the way libpq does while the first is still in flight."""

    def __init__(self) -> None:
        super().__init__({})
        self.reset_attempted = False

    def cursor(self):
        cur = _Cursor(self)
        real_execute = cur.execute

        def execute(sql, params=None):
            if sql == "RESET statement_timeout":
                self.reset_attempted = True
                raise psycopg.OperationalError(
                    "another command is already in progress"
                )
            real_execute(sql, params)

        cur.execute = execute  # type: ignore[method-assign]
        return cur


@pytest.mark.parametrize("interrupt", [SystemExit(143), KeyboardInterrupt()])
def test_a_stop_signal_during_the_pin_read_propagates_unchanged(
    monkeypatch, interrupt,
) -> None:
    """The stop signal's own exception reaches the caller (exit 143), not the
    RESET failure the half-finished connection would otherwise raise."""
    _pin(monkeypatch, interrupt)
    conn = _MidCommandConn()
    with pytest.raises(type(interrupt)) as excinfo:
        _probe(conn)
    assert excinfo.value is interrupt
    assert not conn.reset_attempted


def test_an_ordinary_pin_error_still_resets_the_timeout(monkeypatch) -> None:
    _pin(monkeypatch, psycopg.OperationalError("server closed the connection"))
    conn = _FakeConn({})
    with pytest.raises(CapabilityProbeFailed):
        _probe(conn)
    assert "RESET statement_timeout" in conn.statements


def test_a_timed_out_pin_makes_every_catalog_fact_unknown(monkeypatch) -> None:
    _pin(monkeypatch, psycopg.errors.QueryCanceled("timeout"))
    caps = _probe(_FakeConn({}))
    assert caps.method("alias").status == UNKNOWN
    assert caps.rank_by_value("passages").status == UNKNOWN
    assert caps.pinned is None
    assert caps.reads == ("pin",)


def test_before_any_publish_the_catalog_facts_are_unknown(monkeypatch) -> None:
    """No published generation is a state, not an error. The probe does not
    substitute some other catalog version for the one a session would pin."""
    _pin(monkeypatch, None)
    conn = _FakeConn({})
    caps = _probe(conn)
    assert caps.pinned is None
    assert caps.reads == ("pin",)
    assert caps.method("alias").status == UNKNOWN
    assert caps.method("alias").reason == (
        "not verified at start: no published generation existed"
    )
    assert caps.rank_by_value("passages").status == UNKNOWN
    assert not any("okg.aliases" in s for s in conn.statements)


def test_a_broken_pin_refuses_start_rather_than_substituting(monkeypatch) -> None:
    """An invalid or compacted checkpoint is not "no generation yet"."""
    from okg.substrate.errors import GenerationPinInvalid

    _pin(monkeypatch, GenerationPinInvalid("checkpoint has no catalog_version_id"))
    with pytest.raises(CapabilityProbeFailed) as excinfo:
        _probe(_FakeConn({}))
    assert excinfo.value.data["read"] == "pin"
    assert excinfo.value.data["error_type"] == "GenerationPinInvalid"


def test_a_connection_the_pool_cannot_give_refuses_start() -> None:
    from psycopg_pool import PoolTimeout

    class _Pool:
        @contextmanager
        def connection(self):
            raise PoolTimeout("couldn't get a connection after 5.00 sec")
            yield  # pragma: no cover

    with pytest.raises(CapabilityProbeFailed) as excinfo:
        probe_capabilities_from_pool(
            _Pool(), deployment_name="d", embedder_configured=False,
        )
    assert excinfo.value.data["read"] == "connection"


# ── hermetic records ──────────────────────────────────────────────────


def test_the_named_fixtures() -> None:
    none = capabilities_for_surface_probe(embedder_configured=True, fixture="none")
    full = capabilities_for_surface_probe(embedder_configured=False, fixture="full")
    assert none.mode == "fixture:none"
    assert none.available_methods() == ("lexical", "identifier_mentions", "attrs", "grep")
    assert full.available_methods() == (
        "lexical", "semantic", "alias", "identifier_mentions", "attrs", "grep",
    )
    assert set(CAPABILITY_FIXTURES) == {"none", "full"}


def test_an_unnamed_hermetic_build_does_not_guess() -> None:
    """No database and no fixture: config facts only, database facts
    unknown, and unknown publishes as unavailable."""
    caps = capabilities_for_surface_probe(embedder_configured=True, fixture=None)
    assert caps.mode == "surface_probe_unprobed"
    assert caps.method("semantic").status == AVAILABLE
    assert caps.method("alias").status == UNKNOWN
    assert caps.method("alias").source == "not_probed"
    assert "alias" not in caps.available_methods()
    assert "passages" not in caps.available_rank_by()


def test_an_unknown_fixture_name_refuses() -> None:
    with pytest.raises(CapabilityFixtureInvalid, match="no capability fixture"):
        capabilities_for_surface_probe(embedder_configured=False, fixture="nope")


# ── the guard ─────────────────────────────────────────────────────────


def test_the_guard_refuses_an_unavailable_method_by_name() -> None:
    caps = CAPABILITY_FIXTURES["none"]()
    with pytest.raises(SearchMethodUnavailable) as excinfo:
        assert_search_capability(caps, method="alias", rank_by=None)
    assert excinfo.value.code == "search_method_unavailable"
    assert excinfo.value.data["available_methods"] == [
        "lexical", "identifier_mentions", "attrs", "grep",
    ]
    assert excinfo.value.data["reason"] == "no aliases are registered"


def test_the_guard_leaves_semantic_to_its_own_refusal_code() -> None:
    caps = CAPABILITY_FIXTURES["none"]()
    assert_search_capability(caps, method="semantic", rank_by=None)


def test_the_guard_refuses_unavailable_passages() -> None:
    caps = CAPABILITY_FIXTURES["none"]()
    with pytest.raises(SearchRankByUnavailable) as excinfo:
        assert_search_capability(caps, method="lexical", rank_by="passages")
    assert excinfo.value.code == "search_rank_by_unavailable"
    assert excinfo.value.data["available_rank_by"] == ["records", "self"]


def test_the_guard_refuses_unknown_like_unavailable() -> None:
    caps = capabilities_for_surface_probe(embedder_configured=False, fixture=None)
    with pytest.raises(SearchMethodUnavailable):
        assert_search_capability(caps, method="alias", rank_by=None)


def test_the_guard_passes_what_is_available() -> None:
    caps = CAPABILITY_FIXTURES["full"]()
    for method in ("lexical", "semantic", "alias", "identifier_mentions", "attrs", "grep"):
        assert_search_capability(caps, method=method, rank_by="passages")


# ── generated text ────────────────────────────────────────────────────


def test_the_generated_lines_name_each_unavailable_capability() -> None:
    text = render_unavailable_lines(
        CAPABILITY_FIXTURES["none"](), served_tools=("inspect", "search"),
    )
    # Prompt redesign (#3666): "semantic" and "alias" are methods the
    # published schema hides, so the block no longer names them.
    assert text == "\n".join([
        NOT_AVAILABLE_HEADER,
        '- search rank_by "passages": no subtype declares a passage rollup.',
    ])
    assert "semantic" not in text and "alias" not in text


def test_a_method_the_schema_hides_is_never_named() -> None:
    """Prompt redesign (#3666): the LB3 server's block said only
    'search method "semantic": no embedder is configured.', a mode the
    published schema never showed. Failure case: a hidden method that is
    unavailable produces no block at all when it is the only gap."""
    from okg.substrate.mcp.lean_surface import SHOWN_SEARCH_METHODS

    only_semantic = cp._record(
        semantic=Capability(UNAVAILABLE, "no embedder is configured", "config"),
        alias=Capability(AVAILABLE, "aliases are registered", "catalog"),
        passages=Capability(AVAILABLE, "a subtype declares a passage rollup", "catalog"),
        mode="live",
    )
    assert "semantic" not in SHOWN_SEARCH_METHODS
    assert render_unavailable_lines(only_semantic, served_tools=("search",)) == ""


def test_nothing_is_generated_when_everything_runs_or_search_is_not_served() -> None:
    assert render_unavailable_lines(
        CAPABILITY_FIXTURES["full"](), served_tools=("search",),
    ) == ""
    assert render_unavailable_lines(
        CAPABILITY_FIXTURES["none"](), served_tools=("inspect", "expand"),
    ) == ""


def test_an_unknown_capability_reads_not_verified() -> None:
    text = render_unavailable_lines(
        capabilities_for_surface_probe(embedder_configured=False, fixture=None),
        served_tools=("search",),
    )
    assert '- search rank_by "passages": not verified at start (not probed' in text


#: The benchmark fairness rule: generated text states what the tools do and
#: do not do on this server, never which method to call or when.
_STRATEGY_WORDS = re.compile(
    r"\b(try|prefer|first|instead|should|best|fails?|fall ?back|use)\b",
    re.IGNORECASE,
)


@pytest.mark.parametrize("record", [
    CAPABILITY_FIXTURES["none"](),
    capabilities_for_surface_probe(embedder_configured=False, fixture=None),
], ids=["none", "unprobed"])
def test_generated_text_states_facts_not_strategy(record) -> None:
    text = render_unavailable_lines(record, served_tools=("search",))
    assert text
    assert not _STRATEGY_WORDS.search(text), _STRATEGY_WORDS.search(text)


def test_the_worst_case_generated_block_fits_its_budget() -> None:
    """Decision 12 (b): every optional capability unknown, with the longest
    reason the probe can produce."""
    long_reason = (
        "not verified at start: the passage-rollup read timed out after "
        "2000000000 ms"
    )
    worst = cp._record(
        semantic=Capability(UNKNOWN, long_reason, "catalog"),
        alias=Capability(UNKNOWN, long_reason, "catalog"),
        passages=Capability(UNKNOWN, long_reason, "catalog"),
        mode="live",
    )
    text = render_unavailable_lines(worst, served_tools=("search",))
    assert len(text.encode("utf-8")) <= GENERATED_INSTRUCTIONS_BUDGET_BYTES


def test_compose_and_split_round_trip() -> None:
    generated = render_unavailable_lines(
        CAPABILITY_FIXTURES["none"](), served_tools=("search",),
    )
    # Exact inverses, so the authored part recomputed from served text hashes
    # the same as the file it came from, trailing newline included.
    served = compose_instructions("okg text\n", generated)
    assert served == "okg text\n\n\n" + generated
    assert split_generated_instructions(served) == ("okg text\n", generated)
    assert split_generated_instructions("okg text") == ("okg text", "")
    assert compose_instructions("okg text", "") == "okg text"


# ── the published schema ──────────────────────────────────────────────


class _Tool:
    def __init__(self, parameters: dict[str, Any]) -> None:
        self.parameters = parameters


class _Mcp:
    def __init__(self, tools: dict[str, Any]) -> None:
        self._tool_manager = type("M", (), {"_tools": tools})()


def test_compile_refuses_rather_than_no_op_on_an_unexpected_schema() -> None:
    mcp = _Mcp({"search": _Tool({"properties": {"method": {"type": "string"}}})})
    with pytest.raises(SearchSchemaUnreachable):
        compile_tool_surface(mcp, CAPABILITY_FIXTURES["none"]())


def test_compile_leaves_a_surface_without_search_alone() -> None:
    mcp = _Mcp({"inspect": _Tool({"properties": {}})})
    compile_tool_surface(mcp, CAPABILITY_FIXTURES["none"]())
    assert mcp._tool_manager._tools["inspect"].parameters == {"properties": {}}


# ── through the server, the way a client calls it ─────────────────────


@pytest.fixture
def bare_server(monkeypatch: pytest.MonkeyPatch):
    """A hermetic server under the `none` fixture. Its pools are unopened,
    so any call that reached the database would fail on the pool instead
    of returning the named refusal."""
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "none")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = build_server(dsn=PROBE_DSN, allow_writes=True)
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()
        write_pool = getattr(mcp, "_okg_write_pool", None)
        if write_pool is not None:
            write_pool.close()


@pytest.mark.anyio
async def test_an_unavailable_method_is_refused_by_name_over_the_wire(
    bare_server,
) -> None:
    result = await call_json(bare_server, "search", {"query": "x", "method": "alias"})
    body = payload(result)
    assert result.isError is True
    assert body["error"]["code"] == "search_method_unavailable"
    assert body["error"]["data"]["available_methods"] == [
        "lexical", "identifier_mentions", "attrs", "grep",
    ]


@pytest.mark.anyio
async def test_semantic_keeps_its_code_and_names_the_probe_reason(bare_server) -> None:
    result = await call_json(
        bare_server, "search", {"query": "x", "method": "semantic"},
    )
    body = payload(result)
    assert body["error"]["code"] == "semantic_search_unavailable"
    assert body["error"]["capability"]["reason"] == "no embedder is configured"
    assert body["error"]["capability"]["available_methods"] == [
        "lexical", "identifier_mentions", "attrs", "grep",
    ]


@pytest.mark.anyio
async def test_unavailable_passages_is_refused_by_name_over_the_wire(
    bare_server,
) -> None:
    result = await call_json(
        bare_server, "search",
        {"query": "x", "rank_by": "passages", "subtype_filter": "document"},
    )
    body = payload(result)
    assert body["error"]["code"] == "search_rank_by_unavailable"


@pytest.mark.anyio
async def test_the_surface_readback_carries_the_probe(bare_server) -> None:
    result = await call_json(bare_server, "inspect", {"target": {"kind": "surface"}})
    body = payload(result)
    caps = body["capabilities"]
    assert caps["mode"] == "fixture:none"
    assert caps["fixed_for_process"] is True
    assert caps["search"]["published_methods"] == [
        "lexical", "identifier_mentions", "attrs", "grep",
    ]
    assert caps["search"]["methods"]["semantic"]["status"] == "unavailable"
    assert caps["search"]["published_rank_by"] == ["records", "self"]
    assert "source_lineage" in caps["search"]["published_select_fields"]
    assert body["generated_instructions_bytes"] > 0


def test_a_capability_fixture_on_a_live_server_refuses(monkeypatch) -> None:
    """Fixtures are for hermetic builds. On a server that opens a database
    the probe runs; a leftover fixture variable is refused, not ignored."""
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.delenv("OKG_MCP_SURFACE_PROBE", raising=False)
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    opened = []
    real_build_pool = server_mod._build_pool
    monkeypatch.setattr(
        server_mod, "_build_pool",
        lambda **kwargs: opened.append(kwargs) or real_build_pool(**kwargs),
    )
    with pytest.raises(CapabilityFixtureInvalid, match="hermetic"):
        build_server(dsn=PROBE_DSN)
    # Refused before any pool exists, so nothing is left retrying the DSN.
    assert opened == []


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"



# ── the deployment-file paths (decision 5b) ───────────────────────────


def _hermetic_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "none")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )


def _sha(text: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _assert_appended(mcp, file_text: str, ids) -> None:
    """PACT dataset-card: the served text is the dataset card, okg's own
    guidance, the deployment's file under its fixed heading, then the
    generated block. The file is appended, never a replacement."""
    from okg.substrate.mcp.dataset_card import (
        DEPLOYMENT_GUIDANCE_HEADING,
        split_dataset_card,
    )
    from okg.substrate.mcp.server import authored_instructions_for

    expected_block = render_unavailable_lines(
        CAPABILITY_FIXTURES["none"](), served_tools=("search",),
    )
    served_tools = tuple(mcp._okg_surface_freshness["tool_surface"]["served_tools"])
    authored = authored_instructions_for(
        served_tools,
        deployment_text=file_text,
        # Fixed per fixture (#4052, #4064), not read back from the server:
        # the caller says which id families its deployment declares.
        ids=ids,
    )
    assert authored.endswith(
        DEPLOYMENT_GUIDANCE_HEADING + "\n\n" + file_text.strip()
    )
    card, rest = split_dataset_card(mcp.instructions)
    assert card
    assert rest == authored + "\n\n" + expected_block
    report = mcp._okg_surface_freshness
    assert report["tool_surface"]["instruction_source"] == "deployment_file"
    assert report["instructions_hash"] == _sha(mcp.instructions)
    assert report["authored_instructions_hash"] == _sha(authored)
    assert split_generated_instructions(rest) == (authored, expected_block)


def test_a_legacy_mcp_instructions_file_gets_the_lines(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The legacy `mcp_instructions:` key, with no `agent_surface` block --
    a private deployment's shape."""
    from okg.substrate.mcp.server import IdFamilies, build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    _hermetic_none(monkeypatch)
    directory = tmp_path / "legacy-probe"
    (directory / "schemas").mkdir(parents=True)
    (directory / "docs").mkdir()
    (directory / "source_registry.yaml").write_text("", encoding="utf-8")
    authored = "# Legacy deployment\n\nAnswers questions about tickets.\n"
    (directory / "docs" / "mcp-instructions.md").write_text(authored, encoding="utf-8")
    (directory / "deployment.yaml").write_text(
        "name: legacy-probe\n"
        "description: decision 5b legacy path\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "mcp_instructions: docs/mcp-instructions.md\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    mcp = build_server(dsn=PROBE_DSN, deployment_name="legacy-probe")
    try:
        assert mcp._okg_agent_surface is None
        # An empty source registry declares no id family.
        _assert_appended(
            mcp, authored,
            IdFamilies(doc_text=False, file_nodes=False, commit_nodes=False),
        )
    finally:
        mcp._okg_pool.close()


def test_an_agent_surface_instructions_file_gets_the_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deployment that declares `agent_surface.instructions` (okg-ci)."""
    from pathlib import Path

    from okg.substrate.mcp.server import IdFamilies, build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    _hermetic_none(monkeypatch)
    mcp = build_server(dsn=PROBE_DSN, deployment_name="okg-ci")
    try:
        assert mcp._okg_agent_surface is not None
        authored_file = (
            Path(__file__).resolve().parents[3]
            / "deployments" / "okg-ci" / "mcp-instructions.md"
        )
        # okg-ci declares git files and git history, and no doc source.
        _assert_appended(
            mcp, authored_file.read_text(encoding="utf-8"),
            IdFamilies(doc_text=False, file_nodes=True, commit_nodes=True),
        )
    finally:
        mcp._okg_pool.close()


# ── search views (PACT read-model-s4-list) ─────────────────────────────


def _without_records(caps):
    from dataclasses import replace

    return replace(caps, rank_by=tuple(
        (value, Capability(UNAVAILABLE, "test: records withheld", "fixture"))
        if value == "records" else (value, cap)
        for value, cap in caps.rank_by
    ))


def test_the_view_matrix_follows_the_methods_and_rank_modes_that_run() -> None:
    caps = CAPABILITY_FIXTURES["none"]()
    report = caps.to_report()["search"]
    assert report["views"] == {
        # #3222 slice 3: `metadata` on lexical records (and passages, below).
        "lexical": {"records": ["cards", "list", "metadata"], "self": ["cards"]},
        # #3666 step 2b: a where with no query lists the records it selects.
        "attrs": {"self": ["cards", "list"]},
        "identifier_mentions": {"self": ["cards"]},
        # Merged with read-record-windows: grep serves record windows.
        "grep": {"records": ["cards", "list", "windows"]},
    }
    assert report["published_views"] == ["cards", "list", "windows", "metadata"]
    full = CAPABILITY_FIXTURES["full"]().to_report()["search"]["views"]
    assert full["lexical"]["passages"] == ["cards", "metadata"]
    assert full["semantic"] == full["alias"] == {"self": ["cards"]}
    # Failure case: with no records rank mode (and no passages here),
    # nothing can serve a list or a metadata search.
    withheld = _without_records(caps)
    assert withheld.published_views() == ("cards",)
    assert withheld.to_report()["search"]["views"]["lexical"] == {"self": ["cards"]}
    with pytest.raises(SearchRankByUnavailable):
        assert_search_capability(withheld, method="lexical", rank_by="records")


def _search_schema(*, with_view: bool) -> dict:
    props: dict[str, Any] = {
        "method": {"type": "string", "enum": list(cp.SEARCH_METHODS), "default": "lexical"},
        "select": {"anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}]},
        "rank_by": {"anyOf": [{"type": "string", "enum": ["records", "self", "passages"]},
                              {"type": "null"}]},
    }
    if with_view:
        props["view"] = {"anyOf": [{"type": "string", "enum": ["cards", "list"]},
                                   {"type": "null"}]}
    return {"properties": props}


def test_compile_publishes_the_view_enum_and_refuses_a_schema_without_one() -> None:
    mcp = _Mcp({"search": _Tool(_search_schema(with_view=True))})
    compile_tool_surface(mcp, CAPABILITY_FIXTURES["none"]())
    view = mcp._tool_manager._tools["search"].parameters["properties"]["view"]["anyOf"][0]
    assert view["enum"] == ["cards", "list", "windows", "metadata"]
    withheld = _Mcp({"search": _Tool(_search_schema(with_view=True))})
    compile_tool_surface(withheld, _without_records(CAPABILITY_FIXTURES["none"]()))
    props = withheld._tool_manager._tools["search"].parameters["properties"]
    assert props["view"]["anyOf"][0]["enum"] == ["cards"]
    with pytest.raises(SearchSchemaUnreachable) as excinfo:
        compile_tool_surface(
            _Mcp({"search": _Tool(_search_schema(with_view=False))}),
            CAPABILITY_FIXTURES["none"](),
        )
    assert excinfo.value.data["has_view_enum"] is False


@pytest.mark.anyio
async def test_a_list_the_server_cannot_serve_is_refused_before_the_pool(bare_server) -> None:
    """The pools are unopened: a refusal here never reached the database."""
    for arguments in (
        {"query": "x", "view": "list", "method": "alias"},
        {"query": "x", "view": "list", "method": "semantic"},
        {"query": "x", "view": "list", "rank_by": "passages", "subtype_filter": "document"},
        {"query": "x", "view": "list", "select": ["excerpt"]},
        {"query": "x", "view": "list", "limit": 101},
        # A where with no query and view="list" is served by the lookup now
        # (issue #3666), so it reaches the pool; its limit is checked first.
        {"where": {"op": "eq", "field": "type", "value": "doc"}, "view": "list", "limit": 101},
    ):
        result = await call_json(bare_server, "search", arguments)
        body = payload(result)
        assert result.isError is True, arguments
        assert body["error"]["code"] == "search_view_unsupported", (arguments, body)


@pytest.mark.anyio
async def test_a_cached_list_request_is_refused_when_records_cannot_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A client holding an older schema that still lists `view="list"`."""
    import json

    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    none = CAPABILITY_FIXTURES["none"]
    monkeypatch.setitem(CAPABILITY_FIXTURES, "none", lambda: _without_records(none()))
    _hermetic_none(monkeypatch)
    mcp = build_server(dsn=PROBE_DSN, allow_writes=True)
    try:
        # `view` is published for the short rows (read-passage-rows-3666,
        # D1) on every server, so the published surface does not depend on
        # the rank modes a graph runs (a bound live relation must leave it
        # as it is). Where no list runs, the call is refused by name.
        view = mcp._tool_manager._tools["search"].parameters["properties"].get("view", {})
        assert "list" in json.dumps(view)
        result = await call_json(mcp, "search", {"query": "x", "view": "list"})
        body = payload(result)
        assert result.isError is True
        assert body["error"]["code"] == "search_rank_by_unavailable"
        assert body["error"]["data"]["rank_by"] == "records"
    finally:
        mcp._okg_pool.close()
        write_pool = getattr(mcp, "_okg_write_pool", None)
        if write_pool is not None:
            write_pool.close()
