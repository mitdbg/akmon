"""Hermetic fail-closed contracts for the REAL-graph grounded OpenWebUI proof.

These run in the ordinary fast lane. The live driver is never imported: it
needs Docker, a container, and a wheel-installed okg, and importing it would
turn any defect in it into a collection error for unrelated tests. What is
imported is the pure contract module the driver, the model fixture, and this
test share, plus the driver source text for the fabrication guard.

Every value below is synthetic. The proof reads its real values out of the
graph at run time, so a real generation, node, source, or deployment name has
no place in this file.
"""

from __future__ import annotations

import ast
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tests.live.chat.grounded_api_chain import ChainError, Refusal, check_row_provenance
from tests.live.chat.real_graph_chain import (
    MODEL_FIXTURE_ENV_KEYS,
    MODEL_ID,
    POSTURE_DISCLOSURES,
    assert_tool_served,
    TEXT_BLOCK_TYPES,
    content_block_text,
    PERSISTED_TOOL_NAME,
    SEARCH_LIMIT,
    SERVER_ID,
    TOOL_NAME,
    GRAPH_TOOL_NAME,
    GraphValues,
    call_id_for,
    driver_value_findings,
    graph_call_id_for,
    graph_generation,
    graph_values,
    turn_values,
    mcp_serve_command,
    node_id_from_prompt,
    okg_child_cwd,
    model_fixture_env,
    persisted_real_chain,
    same_graph_values,
    served_manifest_identity,
    served_tool_names,
    search_arguments,
    user_prompt,
    validate_real_graph_chain,
)
from tests.live.chat.real_graph_model_fixture import answer_text, plan_reply

REPO_ROOT = Path(__file__).resolve().parents[3]
DRIVER = REPO_ROOT / "tests/live/chat/drive_real_graph_turn.py"

NODE = "widget:alpha-one"
GENERATION = "okg:demo:branch:default:gen:20260101T000000000000Z:0123456789ab"
SOURCE = "demo.widget-reader"
VALUES = GraphValues(node_id=NODE, generation_id=GENERATION, source=SOURCE)

SESSION = "0123456789abcdef0123456789abcdef"
WINDOW_START = datetime(2026, 9, 21, 1, 0, tzinfo=timezone.utc)
WINDOW = (WINDOW_START, WINDOW_START + timedelta(minutes=5))
INSIDE = (WINDOW_START + timedelta(seconds=30)).isoformat()


def search_envelope(
    *,
    node_id: str = NODE,
    generation_id: str | None = GENERATION,
    source: str | None = SOURCE,
    extra_first: bool = False,
) -> dict:
    """An envelope shaped like the real search answer, CLI or MCP."""
    results = []
    if extra_first:
        results.append(
            {"node_id": "widget:other", "provenance": {"source": "demo.other"}}
        )
    row: dict = {"node_id": node_id}
    if source is not None:
        row["provenance"] = {"source": source}
    results.append(row)
    envelope: dict = {"query": node_id, "results": results}
    if generation_id is not None:
        envelope["pinned_generation_id"] = generation_id
    return envelope


def tool_content(**kwargs) -> str:
    """What OpenWebUI hands the model: the parsed MCP text, re-serialised."""
    return json.dumps(search_envelope(**kwargs), indent=2)


# --------------------------------------------------------------------------
# graph_values: the one extractor both read paths share
# --------------------------------------------------------------------------


def test_graph_values_reads_generation_and_provenance_source() -> None:
    assert graph_values(tool_content(), node_id=NODE, origin="t") == VALUES


def test_graph_values_accepts_a_decoded_object_as_well_as_text() -> None:
    assert graph_values(search_envelope(), node_id=NODE, origin="t") == VALUES


def test_graph_values_finds_the_requested_node_rather_than_the_first_one() -> None:
    assert (
        graph_values(tool_content(extra_first=True), node_id=NODE, origin="t") == VALUES
    )


def test_structured_content_only_result_degrades_to_an_empty_envelope_and_refuses() -> None:
    """The measured failure mode of a structuredContent-only tool result.

    The unpatched official client reads only the MCP ``content`` list. When
    that list is empty the vendor turns it into ``{"results": []}``, which
    carries no pinned generation -- so this arm is what would catch a server
    that answered with structuredContent alone.
    """
    with pytest.raises(ChainError) as caught:
        graph_values(json.dumps({"results": []}), node_id=NODE, origin="tool result")
    assert "pinned_generation_id" in str(caught.value)


def test_graph_values_refuses_a_missing_pinned_generation() -> None:
    with pytest.raises(ChainError):
        graph_values(tool_content(generation_id=None), node_id=NODE, origin="t")


def test_graph_values_refuses_a_blank_pinned_generation() -> None:
    with pytest.raises(ChainError):
        graph_values(tool_content(generation_id="   "), node_id=NODE, origin="t")


def test_graph_values_refuses_text_that_is_not_json() -> None:
    with pytest.raises(ChainError) as caught:
        graph_values("not json at all", node_id=NODE, origin="tool result")
    assert "tool result" in str(caught.value)


def test_graph_values_refuses_a_non_object_payload() -> None:
    with pytest.raises(ChainError):
        graph_values(json.dumps([1, 2, 3]), node_id=NODE, origin="t")


def test_graph_values_refuses_an_empty_result_list() -> None:
    """Named separately from a wrong-node answer, which is a different defect."""
    with pytest.raises(ChainError) as caught:
        graph_values(
            json.dumps({"pinned_generation_id": GENERATION, "results": []}),
            node_id=NODE,
            origin="t",
        )
    assert "carries no results" in str(caught.value)


def test_graph_values_refuses_when_the_requested_node_is_absent() -> None:
    with pytest.raises(ChainError) as caught:
        graph_values(tool_content(node_id="widget:someone-else"), node_id=NODE, origin="t")
    assert NODE in str(caught.value)


def test_graph_values_refuses_a_result_without_provenance() -> None:
    with pytest.raises(ChainError):
        graph_values(tool_content(source=None), node_id=NODE, origin="t")


def test_graph_values_refuses_a_blank_provenance_source() -> None:
    with pytest.raises(ChainError):
        graph_values(tool_content(source=""), node_id=NODE, origin="t")


def mcp_content_blocks(block_type: str = "text", **kwargs) -> list[dict]:
    """What the vendor persists: the raw MCP content list of the tool result."""
    return [{"type": block_type, "text": tool_content(**kwargs)}]


@pytest.mark.parametrize("block_type", TEXT_BLOCK_TYPES)
def test_both_spellings_of_a_text_block_are_read(block_type: str) -> None:
    """The server sends type text; the vendor persists it as input_text."""
    assert (
        graph_values(mcp_content_blocks(block_type), node_id=NODE, origin="t") == VALUES
    )


def test_the_extractor_reads_the_persisted_mcp_content_list() -> None:
    assert graph_values(mcp_content_blocks(), node_id=NODE, origin="t") == VALUES


def test_an_empty_content_list_names_the_structured_content_failure() -> None:
    with pytest.raises(ChainError) as caught:
        content_block_text([], origin="persisted tool result")
    assert "structuredContent" in str(caught.value)


def test_a_content_list_with_only_a_non_text_block_refuses() -> None:
    with pytest.raises(ChainError) as caught:
        content_block_text([{"type": "image", "data": "AA"}], origin="t")
    assert "no MCP text content block" in str(caught.value)


def test_a_non_text_block_that_carries_a_text_key_is_still_refused() -> None:
    """An MCP resource block has a text field the client does NOT read.

    Without the block-type filter this would read as a graph answer, so the
    filter is what makes the accepted set exactly the field the unpatched
    official client passes on.
    """
    with pytest.raises(ChainError) as caught:
        content_block_text(
            [{"type": "resource", "text": tool_content()}], origin="t"
        )
    assert "no MCP text content block" in str(caught.value)


def test_a_resource_block_is_refused_through_graph_values_too() -> None:
    with pytest.raises(ChainError):
        graph_values(
            [{"type": "resource", "text": tool_content()}],
            node_id=NODE,
            origin="t",
        )


def test_two_text_blocks_refuse_rather_than_pick_one() -> None:
    with pytest.raises(ChainError) as caught:
        content_block_text(mcp_content_blocks() * 2, origin="t")
    assert "2 text content blocks" in str(caught.value)


def test_the_extractor_refuses_an_empty_content_list_through_graph_values() -> None:
    with pytest.raises(ChainError):
        graph_values([], node_id=NODE, origin="t")


# --------------------------------------------------------------------------
# same_graph_values: the graph must not move under the run
# --------------------------------------------------------------------------


def test_two_identical_reads_agree() -> None:
    assert same_graph_values(VALUES, VALUES) is None


def test_a_published_generation_that_changes_mid_run_refuses() -> None:
    after = VALUES._replace(generation_id=GENERATION[:-2] + "ff")
    with pytest.raises(Refusal) as caught:
        same_graph_values(VALUES, after)
    assert "moved during the run" in str(caught.value)


def test_a_provenance_source_that_changes_mid_run_refuses() -> None:
    with pytest.raises(Refusal):
        same_graph_values(VALUES, VALUES._replace(source="demo.other-reader"))


# --------------------------------------------------------------------------
# mcp_serve_command: the recorded launch of the real server
# --------------------------------------------------------------------------


def command() -> list[str]:
    return mcp_serve_command(
        okg_executable="/wheels/venv/bin/okg",
        deployment="demo-instance",
        host="0.0.0.0",
        port=9101,
        token_env="OKG_PROOF_TOKEN",
        allowed_hosts=("host.docker.internal:9101",),
    )


def test_mcp_serve_command_serves_streamable_http_on_the_named_port() -> None:
    built = command()
    assert built[:2] == ["/wheels/venv/bin/okg", "mcp-serve"]
    assert built[built.index("--transport") + 1] == "streamable-http"
    assert built[built.index("--port") + 1] == "9101"
    assert built[built.index("--host") + 1] == "0.0.0.0"
    assert built[built.index("--deployment") + 1] == "demo-instance"


def test_mcp_serve_command_names_the_token_variable_and_never_a_token() -> None:
    built = command()
    assert built[built.index("--auth-token-env") + 1] == "OKG_PROOF_TOKEN"
    assert "--auth-token" not in built


def test_mcp_serve_command_never_enables_writes() -> None:
    assert "--allow-writes" not in command()


def test_mcp_serve_command_passes_every_allowed_host() -> None:
    built = mcp_serve_command(
        okg_executable="okg",
        deployment="demo-instance",
        host="0.0.0.0",
        port=1,
        token_env="OKG_PROOF_TOKEN",
        allowed_hosts=("a:1", "b:1"),
    )
    assert built.count("--allowed-host") == 2
    assert "a:1" in built and "b:1" in built


@pytest.mark.parametrize(
    "token_env",
    ["", "lower_case", "AB", "tok-en-value", "OKG PROOF", "Zx9-secret-looking-value"],
)
def test_mcp_serve_command_refuses_anything_that_is_not_a_variable_name(
    token_env: str,
) -> None:
    with pytest.raises(Refusal):
        mcp_serve_command(
            okg_executable="okg",
            deployment="demo-instance",
            host="0.0.0.0",
            port=1,
            token_env=token_env,
            allowed_hosts=("a:1",),
        )


def test_mcp_serve_command_refuses_an_empty_allowed_host_list() -> None:
    with pytest.raises(Refusal) as caught:
        mcp_serve_command(
            okg_executable="okg",
            deployment="demo-instance",
            host="0.0.0.0",
            port=1,
            token_env="OKG_PROOF_TOKEN",
            allowed_hosts=(),
        )
    assert "allowed-host" in str(caught.value)


def test_the_wheel_okg_runs_from_its_own_virtual_environment() -> None:
    assert okg_child_cwd("/wheels/venv/bin/okg", "/src/okg") == "/wheels/venv"


def test_an_okg_inside_the_checkout_refuses() -> None:
    with pytest.raises(Refusal) as caught:
        okg_child_cwd("/src/okg/.venv/bin/okg", "/src/okg")
    assert "checkout-free" in str(caught.value)


def test_a_sibling_directory_sharing_a_prefix_is_not_the_checkout() -> None:
    assert okg_child_cwd("/src/okg-runs/venv/bin/okg", "/src/okg") == "/src/okg-runs/venv"


def test_the_served_manifest_is_recorded_by_digest(tmp_path) -> None:
    root = tmp_path / "deployments" / "demo-instance"
    root.mkdir(parents=True)
    (root / "deployment.yaml").write_text("name: demo-instance\n", encoding="utf-8")
    record = served_manifest_identity(
        "demo-instance", {"OKG_DEPLOYMENTS_DIR": str(tmp_path / "deployments")}
    )
    assert record["served_deployment_manifest"].endswith("deployment.yaml")
    assert len(record["served_deployment_manifest_sha256"]) == 64


def test_a_missing_served_manifest_refuses(tmp_path) -> None:
    with pytest.raises(Refusal) as caught:
        served_manifest_identity("demo-instance", {"OKG_DEPLOYMENTS_DIR": str(tmp_path)})
    assert "OKG_DEPLOYMENTS_DIR" in str(caught.value)


def test_an_unset_deployments_root_refuses() -> None:
    with pytest.raises(Refusal):
        served_manifest_identity("demo-instance", {})


def test_every_posture_departure_is_disclosed_with_the_run() -> None:
    """None of them may be silent: each is recorded in plain words.

    The manifest-copy disclosure left with live-chat-drivers-0114: an
    installed manifest now carries ``release.channel`` itself, so the served
    manifest is the installed one, recorded by digest.
    """
    assert sorted(POSTURE_DISCLOSURES) == [
        "least_privilege_database_role",
        "model_fixture_network_exposure",
    ]
    exposure = POSTURE_DISCLOSURES["model_fixture_network_exposure"]
    assert "0.0.0.0" in exposure and "no authentication" in exposure


def test_the_role_disclosure_names_the_role_and_why_read_only_stopped_working() -> None:
    """The server now records its read-authority evaluation, so the old
    read-only owner session refuses every search; the disclosure must say so
    and name the least-privilege role the run uses instead."""
    text = POSTURE_DISCLOSURES["least_privilege_database_role"]
    assert "okg_mcp" in text and "NOLOGIN" in text
    assert "authority_evaluation_failed" in text
    assert "okg.mcp_call_log" in text


# --------------------------------------------------------------------------
# what the served endpoint advertises is evidence, not a source constant
# --------------------------------------------------------------------------


def tools_list_reply(*names: str) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {"tools": [{"name": name, "inputSchema": {}} for name in names]},
    }


def test_the_served_tool_names_come_from_the_endpoint_reply() -> None:
    assert served_tool_names(tools_list_reply("inspect", TOOL_NAME)) == [
        "inspect",
        TOOL_NAME,
    ]


def test_a_reply_that_is_not_an_object_refuses() -> None:
    with pytest.raises(ChainError):
        served_tool_names(["inspect"])


def test_a_reply_with_no_result_refuses() -> None:
    with pytest.raises(ChainError) as caught:
        served_tool_names({"jsonrpc": "2.0", "id": 1})
    assert "no result" in str(caught.value)


def test_a_reply_advertising_no_tools_refuses() -> None:
    with pytest.raises(ChainError) as caught:
        served_tool_names({"result": {"tools": []}})
    assert "advertised no tools" in str(caught.value)


def test_an_unnamed_tool_refuses_rather_than_being_dropped() -> None:
    reply = tools_list_reply(TOOL_NAME)
    reply["result"]["tools"].append({"inputSchema": {}})
    with pytest.raises(ChainError) as caught:
        served_tool_names(reply)
    assert "unnamed tool" in str(caught.value)


def test_an_endpoint_that_does_not_serve_the_operator_refuses_before_the_turn() -> None:
    with pytest.raises(Refusal) as caught:
        assert_tool_served(["inspect", "query"], TOOL_NAME)
    assert TOOL_NAME in str(caught.value)


def test_an_endpoint_that_serves_the_operator_is_accepted() -> None:
    assert assert_tool_served(["inspect", TOOL_NAME], TOOL_NAME) is None


# --------------------------------------------------------------------------
# the fixture model holds nothing it could read the graph with
# --------------------------------------------------------------------------


def test_the_fixture_model_environment_is_pruned_to_an_allowlist() -> None:
    env = model_fixture_env(
        {
            "PATH": "/usr/bin",
            "HOME": "/home/x",
            "OKG_MCP_RO_DSN": "postgresql://x@y/z",
            "OKG_DEPLOYMENTS_DIR": "/deployments",
            "OKG_REAL_GRAPH_PROOF_TOKEN": "a-token",
            "AWS_SECRET_ACCESS_KEY": "nope",
        }
    )
    assert env == {
        "PATH": "/usr/bin",
        "HOME": "/home/x",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    for key in env:
        assert not key.startswith("OKG_")


def test_the_allowlist_carries_no_graph_variable() -> None:
    for key in MODEL_FIXTURE_ENV_KEYS:
        assert not key.startswith("OKG_")
        assert not key.endswith("_DSN")
        assert "TOKEN" not in key


def test_an_allowlisted_variable_that_looked_like_a_credential_would_refuse(
    monkeypatch,
) -> None:
    """The guard is live, not a comment: it re-checks the built environment."""
    monkeypatch.setattr(
        "tests.live.chat.real_graph_chain.MODEL_FIXTURE_ENV_KEYS",
        ("PATH", "OKG_MCP_RO_DSN"),
    )
    with pytest.raises(Refusal) as caught:
        model_fixture_env({"PATH": "/usr/bin", "OKG_MCP_RO_DSN": "postgresql://x"})
    assert "OKG_MCP_RO_DSN" in str(caught.value)


# --------------------------------------------------------------------------
# every wait in the driver is bounded by the tested primitive
# --------------------------------------------------------------------------


def _driver_tree() -> ast.Module:
    return ast.parse(DRIVER.read_text("utf-8"))


def test_the_driver_bounds_every_wait_with_the_tested_primitive() -> None:
    """The deadline arms are only proved if the driver actually uses them.

    Three bounded waits: a listener coming up (the one helper serves all of
    them), the model recorder filling, and the assistant answer being
    persisted. A hand-rolled loop would leave a deadline nothing tests.
    """
    tree = _driver_tree()
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "poll_until"
    ]
    assert len(calls) == 3, f"expected three poll_until sites, found {len(calls)}"
    reads = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "_read_stored"
    ]
    assert len(reads) == 1, (
        f"_read_stored is referenced {len(reads)} times; it must be reached only "
        "as the bounded wait's fetch"
    )


def test_the_driver_contains_no_unbounded_loop() -> None:
    for node in ast.walk(_driver_tree()):
        if isinstance(node, ast.While):
            assert not (
                isinstance(node.test, ast.Constant) and node.test.value is True
            ), f"line {node.lineno}: an unbounded loop in the driver"


def test_every_urlopen_in_the_driver_passes_a_timeout() -> None:
    """A socket read with no deadline is an unbounded wait the lint above misses."""
    unbounded: list[int] = []
    for node in ast.walk(_driver_tree()):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "urlopen"
        ):
            if not any(keyword.arg == "timeout" for keyword in node.keywords):
                unbounded.append(node.lineno)
    assert not unbounded, f"urlopen without a timeout at line(s) {unbounded}"


# --------------------------------------------------------------------------
# the node id travels in the prompt, not in the fixture
# --------------------------------------------------------------------------


def test_the_prompt_carries_the_node_id_back_out() -> None:
    assert node_id_from_prompt(user_prompt(NODE)) == NODE


def test_a_prompt_naming_no_node_refuses() -> None:
    with pytest.raises(Refusal):
        node_id_from_prompt("tell me about the graph")


def test_a_prompt_naming_two_nodes_refuses() -> None:
    with pytest.raises(Refusal):
        node_id_from_prompt(f"{user_prompt(NODE)} {user_prompt('widget:beta')}")


def test_a_node_id_containing_the_delimiter_refuses() -> None:
    with pytest.raises(Refusal):
        user_prompt("widget:<<odd>>")


def test_search_arguments_ask_the_real_operator_for_the_node() -> None:
    assert search_arguments(NODE) == {
        "query": NODE,
        "method": "lexical",
        "limit": SEARCH_LIMIT,
    }


def test_the_call_id_is_bound_to_the_run() -> None:
    assert call_id_for(SESSION) != call_id_for("f" * 32)
    assert SESSION in call_id_for(SESSION)


def test_the_persisted_tool_name_is_the_server_id_and_the_operator() -> None:
    assert PERSISTED_TOOL_NAME == f"{SERVER_ID}_{TOOL_NAME}"
    assert TOOL_NAME == "search"


# --------------------------------------------------------------------------
# the model fixture: extractive, and refusing when handed nothing
# --------------------------------------------------------------------------


def first_request() -> dict:
    return {
        "messages": [{"role": "user", "content": user_prompt(NODE)}],
        "tools": [{"function": {"name": PERSISTED_TOOL_NAME}}],
    }


def continuation(content: str) -> dict:
    return {
        "messages": [
            {"role": "user", "content": user_prompt(NODE)},
            {"role": "tool", "content": content},
        ]
    }


def test_the_model_asks_for_the_tool_with_the_node_from_the_prompt() -> None:
    message, finish = plan_reply(first_request(), SESSION)
    assert finish == "tool_calls"
    call = message["tool_calls"][0]
    assert call["id"] == call_id_for(SESSION)
    assert call["function"]["name"] == PERSISTED_TOOL_NAME
    assert json.loads(call["function"]["arguments"]) == search_arguments(NODE)


def test_the_model_falls_back_to_the_operator_name_when_no_tool_is_offered() -> None:
    body = first_request()
    body["tools"] = []
    message, _ = plan_reply(body, SESSION)
    assert message["tool_calls"][0]["function"]["name"] == TOOL_NAME


def test_the_model_quotes_only_what_the_tool_result_carried() -> None:
    message, finish = plan_reply(continuation(tool_content()), SESSION)
    assert finish == "stop"
    assert message["content"] == answer_text(NODE, GENERATION, SOURCE)
    for value in (NODE, GENERATION, SOURCE):
        assert value in message["content"]


# --------------------------------------------------------------------------
# the MCP answer is okg's text result since issue #3059
# --------------------------------------------------------------------------


def rendered_text(**kwargs) -> str:
    """What the real server sends Open WebUI: the search envelope rendered by
    okg's own renderer, with a trusted row as the real graph serves it."""
    from okg.substrate.mcp.result_text import render

    envelope = search_envelope(**kwargs)
    envelope.update(total_count=len(envelope["results"]), method="lexical")
    for row in envelope["results"]:
        row.setdefault("subtype", "widget")
        row.setdefault("excerpt", "a widget")
        if "provenance" in row:
            row["provenance"]["trust"] = "trusted"
    return render(envelope, tool="search")


def graph_read_text(generation: str = GENERATION) -> str:
    """inspect(target={"kind":"graph"}) as the real server renders it: the
    one read reply that names the generation (D8)."""
    from okg.substrate.mcp.result_text import render

    return render({"generation_id": generation, "deployment_name": "demo",
                   "node_count": 3}, tool="inspect", kind="graph")


def test_graph_values_reads_the_text_result() -> None:
    """D8 (PACT read-passage-rows-3666): the text search result names no
    generation; it is read at the one the turn's graph read named."""
    text = rendered_text(extra_first=True)
    assert text.startswith("search ok") and GENERATION not in text
    assert graph_values(text, node_id=NODE, origin="t", generation=GENERATION) == VALUES
    blocks = [{"type": "input_text", "text": text}]
    assert graph_values(blocks, node_id=NODE, origin="t", generation=GENERATION) == VALUES
    assert graph_generation(graph_read_text(), origin="t") == GENERATION
    assert turn_values([text, graph_read_text()], node_id=NODE, origin="t") == [VALUES]


def test_graph_values_reads_an_untrusted_row_from_the_text() -> None:
    from okg.substrate.mcp.result_text import render

    envelope = search_envelope()
    envelope["results"][0].update(
        subtype="widget", excerpt="x", provenance={"source": SOURCE, "trust": "untrusted"},
    )
    text = render(envelope, tool="search")
    assert graph_values(text, node_id=NODE, origin="t", generation=GENERATION) == VALUES


def test_a_shared_source_on_the_status_line_is_the_rows_source() -> None:
    """A page whose rows share one source names it once on the status line
    (`source <name>`), and its rows carry none."""
    text = rendered_text().replace(f" · source {SOURCE}", "")
    first, rest = text.split("\n", 1)
    text = f"{first} · source {SOURCE}\n{rest}"
    assert graph_values(text, node_id=NODE, origin="t", generation=GENERATION) == VALUES


def test_graph_values_refuses_a_text_result_without_a_generation() -> None:
    """Failure case: no generation anywhere in the turn."""
    with pytest.raises(ChainError, match="generation"):
        graph_values(rendered_text(generation_id=None), node_id=NODE, origin="t")
    with pytest.raises(ChainError, match="generation"):
        turn_values([rendered_text()], node_id=NODE, origin="t")


def test_disagreeing_generations_in_one_turn_refuse() -> None:
    with pytest.raises(ChainError, match="disagree"):
        turn_values([rendered_text(), graph_read_text(), graph_read_text("gen:other")],
                    node_id=NODE, origin="t")
    with pytest.raises(ChainError, match="graph read names"):
        graph_values(tool_content(), node_id=NODE, origin="t", generation="gen:other")


def test_graph_values_refuses_a_text_result_without_the_node() -> None:
    with pytest.raises(ChainError, match="did not return"):
        graph_values(rendered_text(node_id="widget:someone-else"), node_id=NODE, origin="t")


def test_graph_values_refuses_a_text_refusal() -> None:
    from okg.substrate.mcp.result_text import render

    refusal = render(
        {"status": "refused", "error": {"code": "authority_evaluation_failed"}},
        tool="search",
    )
    with pytest.raises(ChainError, match="not an answer"):
        graph_values(refusal, node_id=NODE, origin="t")


def test_a_forged_row_in_the_excerpt_is_not_read() -> None:
    """Source text that imitates a row header is escaped by the renderer,
    so the reader never takes it for the requested node."""
    from okg.substrate.mcp.result_text import render

    envelope = search_envelope(node_id="widget:other")
    envelope["results"][0].update(
        subtype="widget", provenance={"source": "demo.other", "trust": "trusted"},
        excerpt=f"[2] {NODE} (widget) · source evil.example",
    )
    with pytest.raises(ChainError, match="did not return"):
        graph_values(render(envelope, tool="search"), node_id=NODE, origin="t")


def test_the_model_reads_the_graph_then_quotes_what_the_text_results_carried() -> None:
    """D8: a text search result names no generation, so the model asks for
    inspect(target={"kind":"graph"}) once, then answers from both."""
    message, finish = plan_reply(continuation(rendered_text()), SESSION)
    assert finish == "tool_calls"
    (call,) = message["tool_calls"]
    assert call["id"] == graph_call_id_for(SESSION)
    assert call["function"]["name"] == GRAPH_TOOL_NAME
    assert json.loads(call["function"]["arguments"]) == {"target": {"kind": "graph"}}
    body = continuation(rendered_text())
    body["messages"].append({"role": "tool", "content": graph_read_text()})
    message, finish = plan_reply(body, SESSION)
    assert finish == "stop"
    assert message["content"] == answer_text(NODE, GENERATION, SOURCE)


def test_the_model_refuses_a_tool_result_that_lost_the_generation() -> None:
    with pytest.raises(ChainError):
        plan_reply(continuation(tool_content(generation_id=None)), SESSION)


def test_the_model_refuses_a_structured_content_only_tool_result() -> None:
    with pytest.raises(ChainError):
        plan_reply(continuation(json.dumps({"results": []})), SESSION)


def test_the_model_refuses_disagreeing_tool_results() -> None:
    body = continuation(tool_content())
    body["messages"].append(
        {"role": "tool", "content": tool_content(source="demo.other-reader")}
    )
    with pytest.raises(ChainError) as caught:
        plan_reply(body, SESSION)
    assert "disagree" in str(caught.value)


def test_the_model_refuses_a_request_with_no_user_turn() -> None:
    """Named separately from an unreadable prompt, which is a different defect."""
    with pytest.raises(Refusal) as caught:
        plan_reply({"messages": [{"role": "tool", "content": tool_content()}]}, SESSION)
    assert "no user turn" in str(caught.value)


def test_the_model_id_is_the_fixture_one() -> None:
    assert MODEL_ID.endswith("-model")


# --------------------------------------------------------------------------
# the vendor record of its own tool turn
# --------------------------------------------------------------------------


def stored_message(
    *,
    name: str = PERSISTED_TOOL_NAME,
    call_id: str | None = None,
    result_call_id: str | None = None,
    arguments: str | None = None,
    output: str | None = None,
    content: str | None = None,
    extra_output: list | None = None,
    role: str = "assistant",
    error: object = None,
    call_status: str | None = "completed",
    result_status: str | None = "completed",
) -> dict:
    """Shaped as the vendor persists a tool turn: v0.11.0 and, measured again
    on 2026-09-30, the 0.11.4 compatibility image both mark each item with a
    ``status``. ``None`` drops the key."""
    call_id = call_id or call_id_for(SESSION)
    message: dict = {
        "role": role,
        "content": content
        if content is not None
        else answer_text(NODE, GENERATION, SOURCE),
        "output": [
            {
                "type": "function_call",
                "name": name,
                "call_id": call_id,
                "arguments": arguments
                if arguments is not None
                else json.dumps(search_arguments(NODE)),
                "status": call_status,
            },
            {
                "type": "function_call_output",
                "call_id": result_call_id or call_id,
                "status": result_status,
                "output": output if output is not None else mcp_content_blocks(),
            },
        ],
    }
    for item in message["output"]:
        if item["status"] is None:
            del item["status"]
    if extra_output:
        message["output"].extend(extra_output)
    if error is not None:
        message["error"] = error
    return message


def test_the_persisted_record_reports_the_call_and_the_graph_values() -> None:
    record = persisted_real_chain(stored_message(), session_id=SESSION, values=VALUES)
    assert record["function_call_name"] == PERSISTED_TOOL_NAME
    assert record["tool_result_values"] == VALUES._asdict()
    assert record["has_sources"] is False


def test_a_persisted_text_search_and_graph_read_report_the_graph_values() -> None:
    """D8: the vendor records the search call and the turn's one inspect of
    the graph; the values are read across both."""
    graph_id = graph_call_id_for(SESSION)
    extra = [
        {"type": "function_call", "name": f"{SERVER_ID}_{GRAPH_TOOL_NAME}",
         "call_id": graph_id, "arguments": '{"target": {"kind": "graph"}}',
         "status": "completed"},
        {"type": "function_call_output", "call_id": graph_id, "status": "completed",
         "output": [{"type": "input_text", "text": graph_read_text()}]},
    ]
    stored = stored_message(output=[{"type": "input_text", "text": rendered_text()}],
                            extra_output=extra)
    record = persisted_real_chain(stored, session_id=SESSION, values=VALUES)
    assert record["tool_result_values"] == VALUES._asdict()
    # Failure case: the graph read under another tool name.
    extra[0]["name"] = PERSISTED_TOOL_NAME
    with pytest.raises(ChainError):
        persisted_real_chain(stored, session_id=SESSION, values=VALUES)


def test_a_missing_output_record_refuses() -> None:
    with pytest.raises(ChainError):
        persisted_real_chain({"role": "assistant"}, session_id=SESSION, values=VALUES)


def test_a_second_function_call_refuses() -> None:
    extra = [{"type": "function_call", "name": PERSISTED_TOOL_NAME, "call_id": "x"}]
    with pytest.raises(ChainError):
        persisted_real_chain(
            stored_message(extra_output=extra), session_id=SESSION, values=VALUES
        )


def test_a_second_tool_result_refuses() -> None:
    extra = [{"type": "function_call_output", "call_id": "x", "output": tool_content()}]
    with pytest.raises(ChainError):
        persisted_real_chain(
            stored_message(extra_output=extra), session_id=SESSION, values=VALUES
        )


def test_an_inexact_tool_name_refuses() -> None:
    with pytest.raises(ChainError) as caught:
        persisted_real_chain(
            stored_message(name=PERSISTED_TOOL_NAME + "x"),
            session_id=SESSION,
            values=VALUES,
        )
    assert PERSISTED_TOOL_NAME in str(caught.value)


def test_arguments_that_do_not_name_the_node_refuse() -> None:
    with pytest.raises(ChainError):
        persisted_real_chain(
            stored_message(arguments=json.dumps({"query": "something else"})),
            session_id=SESSION,
            values=VALUES,
        )


def test_a_call_id_from_another_run_refuses() -> None:
    with pytest.raises(ChainError):
        persisted_real_chain(
            stored_message(call_id=call_id_for("f" * 32)),
            session_id=SESSION,
            values=VALUES,
        )


def test_a_tool_result_belonging_to_another_call_refuses() -> None:
    with pytest.raises(ChainError):
        persisted_real_chain(
            stored_message(result_call_id="unrelated"),
            session_id=SESSION,
            values=VALUES,
        )


#: The 0.11.4 failed-tool form, as the vendor handed it to the model and
#: persisted it on 2026-09-30 when the MCP server refused a search (the
#: server's refusal is a Python dict literal inside the error string).
VENDOR_ERROR_TEXT = json.dumps(
    {
        "error": "{'status': 'refused', 'operator_name': 'search', 'error': "
        "{'code': 'authority_evaluation_failed', 'message': 'read authority "
        "could not be evaluated'}}"
    },
    indent=2,
)


@pytest.mark.parametrize(
    ("call_status", "result_status", "named"),
    [
        ("failed", "failed", "stored function call has status 'failed'"),
        ("completed", "failed", "stored tool result has status 'failed'"),
        (None, "completed", "stored function call has status None"),
        ("completed", None, "stored tool result has status None"),
    ],
)
def test_a_tool_turn_the_vendor_did_not_complete_refuses_by_name(
    call_status, result_status, named
) -> None:
    with pytest.raises(ChainError, match=re.escape(named)):
        persisted_real_chain(
            stored_message(call_status=call_status, result_status=result_status),
            session_id=SESSION,
            values=VALUES,
        )


def test_the_vendor_failed_tool_form_is_named_with_the_server_refusal() -> None:
    """0.11.4 stores {"error": ...}; the refusal code must reach the message."""
    blocks = [{"type": "input_text", "text": VENDOR_ERROR_TEXT}]
    with pytest.raises(ChainError) as caught:
        graph_values(blocks, node_id=NODE, origin="persisted tool result")
    assert "failed-tool form" in str(caught.value)
    assert "authority_evaluation_failed" in str(caught.value)


def test_an_envelope_with_an_error_key_beside_results_is_not_the_failed_form() -> None:
    """Only the exact one-key vendor form is recognised; a real answer that
    also carries an error field is read as an answer."""
    envelope = search_envelope()
    envelope["error"] = None
    assert graph_values(envelope, node_id=NODE, origin="t") == VALUES


def test_the_model_refuses_the_vendor_failed_tool_form() -> None:
    body = {
        "messages": [
            {"role": "user", "content": user_prompt(NODE)},
            {"role": "tool", "content": VENDOR_ERROR_TEXT},
        ]
    }
    with pytest.raises(ChainError, match="failed-tool form"):
        plan_reply(body, SESSION)


def test_a_persisted_tool_result_with_an_empty_content_list_refuses() -> None:
    """The structuredContent-only failure, at the layer the vendor recorded."""
    with pytest.raises(ChainError) as caught:
        persisted_real_chain(
            stored_message(output=[]), session_id=SESSION, values=VALUES
        )
    assert "structuredContent" in str(caught.value)


def test_a_persisted_tool_result_disagreeing_with_the_graph_read_refuses() -> None:
    other = mcp_content_blocks(generation_id=GENERATION[:-2] + "ff")
    with pytest.raises(ChainError) as caught:
        persisted_real_chain(
            stored_message(output=other), session_id=SESSION, values=VALUES
        )
    assert "independent graph read" in str(caught.value)


# --------------------------------------------------------------------------
# the whole chain
# --------------------------------------------------------------------------


def model_rows(count: int = 2, *, content: str | None = None) -> list[dict]:
    rows = [
        {
            "kind": "model_request",
            "session": SESSION,
            "at": INSIDE,
            "body": {"messages": [{"role": "user", "content": user_prompt(NODE)}]},
        }
    ]
    if count >= 2:
        rows.append(
            {
                "kind": "model_request",
                "session": SESSION,
                "at": INSIDE,
                "body": {
                    "messages": [
                        {"role": "user", "content": user_prompt(NODE)},
                        {
                            "role": "tool",
                            "content": content if content is not None else tool_content(),
                        },
                    ]
                },
            }
        )
    return rows


def validate(rows=None, stored=None, values=VALUES):
    return validate_real_graph_chain(
        model_rows=rows if rows is not None else model_rows(),
        stored=stored if stored is not None else stored_message(),
        session_id=SESSION,
        window=WINDOW,
        values=values,
        check_row_provenance=check_row_provenance,
    )


def test_the_whole_chain_passes_and_reports_what_was_recorded() -> None:
    verdict = validate()
    assert verdict["model_requests"] == 2
    assert verdict["tool_messages"] == 1
    assert verdict["independent_graph_read"] == VALUES._asdict()
    assert verdict["handed_back_to_model"] == [VALUES._asdict()]
    assert verdict["persisted"]["function_call_name"] == PERSISTED_TOOL_NAME


def test_a_turn_with_no_continuation_refuses() -> None:
    with pytest.raises(ChainError) as caught:
        validate(rows=model_rows(1))
    assert "continuation never arrived" in str(caught.value)


def test_a_continuation_with_no_tool_message_refuses() -> None:
    rows = model_rows()
    rows[1]["body"]["messages"] = [{"role": "user", "content": user_prompt(NODE)}]
    with pytest.raises(ChainError) as caught:
        validate(rows=rows)
    assert "no tool-result message" in str(caught.value)


def test_a_tool_result_the_vendor_altered_refuses() -> None:
    with pytest.raises(ChainError) as caught:
        validate(rows=model_rows(content=tool_content(source="demo.other-reader")))
    assert "independent graph read" in str(caught.value)


def test_a_row_from_another_run_refuses() -> None:
    rows = model_rows()
    rows[1]["session"] = "f" * 32
    with pytest.raises(ChainError):
        validate(rows=rows)


def test_a_row_outside_the_run_window_refuses() -> None:
    rows = model_rows()
    rows[1]["at"] = (WINDOW_START - timedelta(hours=1)).isoformat()
    with pytest.raises(ChainError):
        validate(rows=rows)


def test_a_stored_refusal_refuses() -> None:
    with pytest.raises(ChainError) as caught:
        validate(stored=stored_message(error={"content": "upstream said no"}))
    assert "stored a refusal" in str(caught.value)


def test_a_stored_message_that_is_not_the_assistant_refuses() -> None:
    with pytest.raises(ChainError):
        validate(stored=stored_message(role="user"))


@pytest.mark.parametrize("content", ["", "   ", None])
def test_a_stored_answer_without_text_refuses(content) -> None:
    stored = stored_message()
    stored["content"] = content
    with pytest.raises(ChainError) as caught:
        validate(stored=stored)
    assert "no text" in str(caught.value)


@pytest.mark.parametrize("dropped", [NODE, GENERATION, SOURCE])
def test_a_stored_answer_missing_any_graph_value_refuses(dropped: str) -> None:
    stored = stored_message(
        content=answer_text(NODE, GENERATION, SOURCE).replace(dropped, "elided")
    )
    with pytest.raises(ChainError) as caught:
        validate(stored=stored)
    assert dropped in str(caught.value)


# --------------------------------------------------------------------------
# the driver cannot author the answer
# --------------------------------------------------------------------------


def test_the_committed_driver_authors_no_graph_value_and_no_finished_turn_write() -> None:
    assert driver_value_findings(DRIVER.read_text("utf-8"), VALUES) == []


def test_the_fabrication_guard_bites_on_a_graph_value() -> None:
    source = f'answer = "generation {GENERATION}"\n'
    findings = driver_value_findings(source, VALUES)
    assert findings and GENERATION in findings[0]


def test_the_fabrication_guard_bites_on_the_finished_turn_write_endpoint() -> None:
    findings = driver_value_findings('path = "/api/chat/completed"\n', VALUES)
    assert findings and "finished-turn write" in findings[0]


def test_the_fabrication_guard_ignores_a_blank_value() -> None:
    blank = GraphValues(node_id=NODE, generation_id="", source="")
    assert driver_value_findings('x = ""\n', blank) == []


def test_every_named_artifact_is_written_by_the_driver() -> None:
    """The manifest promises these files, so the driver must name each one."""
    import re

    source = DRIVER.read_text("utf-8")
    names = re.findall(r'^\s+"([a-z0-9.-]+\.(?:json|jsonl|log|txt))":', source, re.M)
    assert names, "the driver must declare its artifacts"
    for name in names:
        assert source.count(name) >= 2, f"{name} is declared but never written"
