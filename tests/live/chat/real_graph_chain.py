"""The pure, import-safe core of the REAL-graph grounded OpenWebUI proof.

Standard library only, no import side effects, no ``sys.path`` mutation. The
live driver, the model fixture, and the hermetic unit test all import this
module, so a defect in the driver can never become a pytest collection error.

How this differs from ``grounded_api_chain``, which proved the same vendor
mechanics with a FIXTURE MCP tool. Here the tool is a real ``okg mcp-serve``
process started from a wheel-installed virtual environment outside any source
checkout, reading a real published graph. So the evidence strings cannot be
module constants: they are read out of the graph at run time, twice, once
through the MCP server that the container calls and once directly through the
``okg`` command line. Nothing in this module names a generation, a node, a
source, or a deployment.

Vocabulary. The "causal chain" is the sequence this proof demonstrates.
OpenWebUI receives one user turn naming a node id, the model asks for the
graph ``search`` tool, OpenWebUI calls the real MCP server, the server reads
the published graph and answers, OpenWebUI hands that answer back to the
model, the model quotes the two values it finds in it, and OpenWebUI persists
the reply. Each link is checked against what was recorded.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
from collections.abc import Mapping
from typing import Any, NamedTuple

from tests.live.chat.grounded_api_chain import (  # re-exported reuse
    FORBIDDEN_WRITE_ENDPOINT,
    ChainError,
    Refusal,
)

#: The id of the OpenWebUI tool-server connection the driver registers. The
#: vendor prefixes the tool name with it, so the name it persists is exactly
#: ``SERVER_ID_TOOL_NAME``.
SERVER_ID = "okg-real-graph"

#: The MCP operator an operator-facing chat calls to look a node up. The real
#: server registers seven read operators (inspect, search, expand, filter,
#: map, aggregate, query); ``search`` is the one that takes a node id and
#: answers with the node plus its provenance and the pinned generation, and it
#: is used unmodified.
TOOL_NAME = "search"
PERSISTED_TOOL_NAME = f"{SERVER_ID}_{TOOL_NAME}"

#: The operator the model reads the published generation from, once per
#: turn. A read reply's text names no generation (PACT
#: read-passage-rows-3666, D8: optional and off by default); ``inspect`` of
#: the graph is how a reader asks for it.
GRAPH_TOOL_NAME = "inspect"
PERSISTED_GRAPH_TOOL_NAME = f"{SERVER_ID}_{GRAPH_TOOL_NAME}"
GRAPH_ARGUMENTS: dict[str, Any] = {"target": {"kind": "graph"}}
#: Every operator the turn may call, as the connection's function filter.
TURN_TOOL_NAMES = (TOOL_NAME, GRAPH_TOOL_NAME)

#: The fixture model advertised to the container. It holds no provider
#: credential and composes no language of its own.
MODEL_ID = "okg-real-graph-extractive-model"

#: An environment variable name, as ``okg mcp-serve --auth-token-env`` wants
#: one. Used to refuse a caller that passes a token VALUE where a variable
#: name belongs -- mcp-serve itself refuses a literal ``--auth-token``, and
#: this proof must never put a credential on a recorded command line.
ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,63}$")


#: How the node id travels from the driver into the turn. The model must READ
#: the node id from the user message rather than hold it, so the driver wraps
#: it in a delimiter no graph id of this deployment contains and the model
#: extracts exactly one occurrence.
PROMPT_NODE_DELIMITERS = ("<<", ">>")
PROMPT_NODE_PATTERN = re.compile(r"<<([^<>]{1,300})>>")

#: How many results the tool call asks for. More than one so the extractor has
#: to FIND the requested node rather than trust position, few enough to stay
#: inside the MCP response byte budget.
SEARCH_LIMIT = 5


def user_prompt(node_id: str) -> str:
    """The one user turn, with the node id the model must read out of it."""
    opener, closer = PROMPT_NODE_DELIMITERS
    if opener in node_id or closer in node_id:
        raise Refusal(f"node id {node_id!r} contains the prompt delimiter")
    return (
        f"Look up the graph node {opener}{node_id}{closer} and cite its "
        "published generation id and its provenance source."
    )


def node_id_from_prompt(text: str) -> str:
    """The node id the user turn names, or a refusal.

    Exactly one occurrence is required. Zero means the model was asked
    something this fixture cannot ground; more than one means the turn is
    ambiguous and a tool call would silently pick one.
    """
    found = PROMPT_NODE_PATTERN.findall(text or "")
    if len(found) != 1:
        raise Refusal(
            f"the user turn names {len(found)} delimited node id(s), expected 1"
        )
    return found[0]


def search_arguments(node_id: str) -> dict[str, Any]:
    """The arguments the model sends to the real search operator."""
    return {"query": node_id, "method": "lexical", "limit": SEARCH_LIMIT}


def call_id_for(session_id: str) -> str:
    """The tool-call id for one run, so a record from an earlier turn cannot
    pair with this one."""
    return f"real-graph-call-{session_id}"


def graph_call_id_for(session_id: str) -> str:
    """The tool-call id of the run's one ``inspect`` of the graph."""
    return f"{call_id_for(session_id)}-graph"


class GraphValues(NamedTuple):
    """The two values read from the graph, plus the node they belong to.

    ``generation_id`` is the published generation the read was pinned to and
    ``source`` is the provenance source of the node. Both are graph facts, so
    they are compared rather than asserted against a literal.
    """

    node_id: str
    generation_id: str
    source: str


#: What a text content block is called. ``text`` is the MCP protocol name and
#: is what the server sends; ``input_text`` is what OpenWebUI relabels the same
#: field to when it persists the function call output. Measured on v0.11.0,
#: and measured again on the 0.11.4 compatibility image (the product pin) on
#: 2026-09-30: the persisted block is still ``input_text``. Both name the one
#: field the client reads, so both are accepted and nothing else is.
TEXT_BLOCK_TYPES = ("text", "input_text")


def content_block_text(blocks: list[Any], *, origin: str) -> str:
    """The one text block of an MCP tool result, as the official client sees it.

    Measured on the run. OpenWebUI persists the function call output as the RAW
    MCP content list (v0.11.0, and again on the 0.11.4 compatibility image on
    2026-09-30), so the recorded evidence shows which field the payload arrived
    in. The upstream client reads ``content`` and nothing else
    (utils/mcp/client.py returns ``result_dict.get("content")``), so a server
    answering with structuredContent alone would deliver an EMPTY list there;
    the compatibility image exists to change exactly that selection. An empty
    list is the failure this function names, rather than letting an empty
    payload read as a merely disappointing result.
    """
    texts = [
        block["text"]
        for block in blocks
        if isinstance(block, dict)
        and block.get("type") in TEXT_BLOCK_TYPES
        and isinstance(block.get("text"), str)
    ]
    if not texts:
        raise ChainError(
            f"{origin} carries no MCP text content block; the unpatched official "
            "client reads only the content field, so a structuredContent-only "
            f"result arrives empty here (blocks were {blocks!r:.200})"
        )
    if len(texts) > 1:
        raise ChainError(
            f"{origin} carries {len(texts)} text content blocks; one graph "
            "answer is expected and picking one would be a guess"
        )
    return texts[0]


def vendor_tool_error(payload: Any) -> str | None:
    """The message of OpenWebUI's failed-tool form, or None for anything else.

    Measured on the 0.11.4 compatibility image on 2026-09-30: when the MCP
    server answers a call with an error result, the vendor hands the model and
    persists ``{"error": "<the error as a Python dict literal>"}`` and marks
    both the ``function_call`` and its ``function_call_output`` with
    ``status: failed`` (v0.11.0 stored the bare ``str(e)``). The payload is an
    object with exactly one key, so without this check it reads as a graph
    answer that merely lacks ``pinned_generation_id``. Naming it keeps the
    server's own refusal -- for example ``authority_evaluation_failed`` -- in
    the failure message instead of a shape complaint.
    """
    if not isinstance(payload, dict) or set(payload) != {"error"}:
        return None
    error = payload["error"]
    return str(error)[:600]


def _one_generation(said: Any, given: str | None, *, origin: str) -> str:
    """The generation a search result is read at: the one it names, else
    the one the turn's ``inspect`` of the graph named. Both named and
    different refuses: the graph moved inside the turn."""
    said = said if isinstance(said, str) and said.strip() else None
    if said is not None and given is not None and said != given:
        raise ChainError(
            f"{origin} names generation {said}, and the turn's graph read names {given}"
        )
    generation = said or given
    if generation is None:
        raise ChainError(
            f"{origin} carries no generation fact on its status line, and the "
            "turn read no generation from inspect(target={\"kind\":\"graph\"})"
        )
    return generation


def graph_values(
    payload: Any, *, node_id: str, origin: str, generation: str | None = None,
) -> GraphValues:
    """Read the pinned generation and the node provenance out of a search result.

    One extractor for both paths on purpose. The real ``okg search --json``
    command line answers with the JSON envelope; the real MCP ``search``
    operator answers with okg's text result (issue #3059), because Open
    WebUI cannot ask for the JSON format. The text is read with okg's own
    reader (:func:`_result_text`), never a second parser, so both paths
    yield the same three values. ``origin`` only names the path in the
    refusal message.

    Every shape failure refuses rather than returning a partial value: an
    absent pinned generation is exactly what a structuredContent-only tool
    result degrades to (OpenWebUI turns an empty content list into an
    envelope with nothing in it), and that must fail the run.

    ``generation`` is the one the turn's ``inspect`` of the graph named
    (:func:`graph_generation`): okg's text result names none (PACT
    read-passage-rows-3666, D8), so the text path reads it from there.
    """
    if isinstance(payload, list):
        payload = content_block_text(payload, origin=origin)
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            return _graph_values_from_text(
                payload, node_id=node_id, origin=origin, cause=exc, generation=generation,
            )
    if not isinstance(payload, dict):
        raise ChainError(f"{origin} answered with {type(payload).__name__}, not an object")
    failure = vendor_tool_error(payload)
    if failure is not None:
        raise ChainError(f"{origin} is the vendor's failed-tool form: {failure}")
    said = payload.get("pinned_generation_id")
    if (not isinstance(said, str) or not said.strip()) and generation is None:
        raise ChainError(
            f"{origin} carries no pinned_generation_id; keys are {sorted(payload)}"
        )
    generation = _one_generation(said, generation, origin=origin)
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        raise ChainError(f"{origin} carries no results for {node_id}")
    for row in results:
        if not isinstance(row, dict):
            continue
        if row.get("node_id") != node_id:
            continue
        provenance = row.get("provenance")
        if not isinstance(provenance, dict):
            raise ChainError(f"{origin} result for {node_id} carries no provenance")
        source = provenance.get("source")
        if not isinstance(source, str) or not source.strip():
            raise ChainError(
                f"{origin} result for {node_id} carries no provenance source"
            )
        return GraphValues(node_id=node_id, generation_id=generation, source=source)
    found = [row.get("node_id") for row in results if isinstance(row, dict)]
    raise ChainError(f"{origin} did not return {node_id}; it returned {found!r}")


#: okg's own text-result reader. Loaded from the checkout by file path: it
#: imports only the standard library, and importing the ``okg.substrate.mcp``
#: package would start the whole server import, which this module promises
#: not to do.
RESULT_TEXT_PATH = (
    pathlib.Path(__file__).resolve().parents[3] / "src/okg/substrate/mcp/result_text.py"
)
_RESULT_TEXT: list[Any] = []


def _result_text() -> Any:
    """``okg.substrate.mcp.result_text``, loaded once by file path."""
    if not _RESULT_TEXT:
        import importlib.util
        import sys

        name = "_okg_result_text_for_real_graph_chain"
        spec = importlib.util.spec_from_file_location(name, RESULT_TEXT_PATH)
        if spec is None or spec.loader is None:
            raise ChainError(f"cannot load okg's result reader from {RESULT_TEXT_PATH}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        _RESULT_TEXT.append(module)
    return _RESULT_TEXT[0]


def _graph_values_from_text(
    text: str, *, node_id: str, origin: str, cause: Exception,
    generation: str | None = None,
) -> GraphValues:
    """The three values from okg's text result: the status line, the
    generation (its ``generation`` fact, else the turn's graph read), the
    requested node's row header and its ``source`` fact. A page whose rows
    share one source names it once, as ``source <name>`` on the status
    line, and its rows carry none."""
    reader = _result_text()
    line = reader.parse_status_line(text)
    if line is None:
        raise ChainError(
            f"{origin} is neither JSON nor an okg result text: {text[:400]!r}"
        ) from cause
    if line.status != "ok":
        raise ChainError(f"{origin} is not an answer: {text.split(chr(10), 1)[0][:400]!r}")
    cards = reader.result_cards(text)
    if not cards:
        raise ChainError(f"{origin} carries no results for {node_id}")
    page_source = reader.fact_value(reader.status_facts(text) or [], "source")
    for card in cards:
        if card.ident != node_id:
            continue
        source = reader.fact_value(list(card.facts), "source") or page_source
        if not isinstance(source, str) or not source.strip():
            raise ChainError(f"{origin} result for {node_id} carries no provenance source")
        said = reader.result_generation(text)
        return GraphValues(
            node_id=node_id,
            generation_id=_one_generation(said, generation, origin=origin),
            source=source,
        )
    raise ChainError(
        f"{origin} did not return {node_id}; it returned {[card.ident for card in cards]!r}"
    )


def _decoded(payload: Any, *, origin: str) -> Any:
    """A tool result as text or a decoded JSON object."""
    if isinstance(payload, list):
        payload = content_block_text(payload, origin=origin)
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload
    return payload


def is_graph_read(payload: Any, *, origin: str = "tool result") -> bool:
    """Whether a tool result is the turn's ``inspect`` of the graph: an okg
    text result whose status line is ``inspect ok``, or the JSON envelope
    that carries ``generation_id`` and no search ``results``."""
    try:
        decoded = _decoded(payload, origin=origin)
    except ChainError:
        return False
    if isinstance(decoded, dict):
        return "generation_id" in decoded and "results" not in decoded
    if isinstance(decoded, str):
        line = _result_text().parse_status_line(decoded)
        return line is not None and line.tool == GRAPH_TOOL_NAME
    return False


def graph_generation(payload: Any, *, origin: str) -> str:
    """The published generation an ``inspect`` of the graph names, or a
    refusal. In text it is the status line's ``generation`` fact (only
    inspect(graph) prints one, D8); in JSON, ``generation_id``."""
    decoded = _decoded(payload, origin=origin)
    if isinstance(decoded, dict):
        failure = vendor_tool_error(decoded)
        if failure is not None:
            raise ChainError(f"{origin} is the vendor's failed-tool form: {failure}")
        generation = decoded.get("generation_id") or decoded.get("pinned_generation_id")
    elif not isinstance(decoded, str):
        raise ChainError(f"{origin} is not a graph read: {type(decoded).__name__}")
    else:
        reader = _result_text()
        line = reader.parse_status_line(decoded)
        if line is None or line.status != "ok":
            raise ChainError(f"{origin} is not a graph read: {str(decoded)[:400]!r}")
        generation = reader.result_generation(decoded)
    if not isinstance(generation, str) or not generation.strip():
        raise ChainError(f"{origin} names no generation")
    return generation


def needs_graph_read(contents: list[Any], *, origin: str) -> bool:
    """Whether the turn must still read the generation: no tool result so
    far is a graph read, and an okg text search result names none (D8). A
    JSON envelope must carry its own `pinned_generation_id`: one without it
    is the structuredContent-only failure, refused by :func:`graph_values`."""
    if any(is_graph_read(content, origin=origin) for content in contents):
        return False
    for content in contents:
        try:
            decoded = _decoded(content, origin=origin)
        except ChainError:
            continue
        if (
            isinstance(decoded, str)
            and _result_text().parse_status_line(decoded) is not None
            and _result_text().result_generation(decoded) is None
        ):
            return True
    return False


def turn_values(contents: list[Any], *, node_id: str, origin: str) -> list[GraphValues]:
    """The graph values of each search result one turn handed back, read at
    the generation the turn's graph read named (every graph read must name
    the same one). Refuses when the turn holds no search result."""
    graphs = [content for content in contents if is_graph_read(content, origin=origin)]
    named = {graph_generation(content, origin=origin) for content in graphs}
    if len(named) > 1:
        raise ChainError(f"{origin}: the turn's graph reads disagree: {sorted(named)!r}")
    generation = next(iter(named), None)
    searches = [content for content in contents if not is_graph_read(content, origin=origin)]
    if not searches:
        raise ChainError(f"{origin}: the turn holds no search result")
    return [
        graph_values(content, node_id=node_id, origin=origin, generation=generation)
        for content in searches
    ]


def same_graph_values(before: GraphValues, after: GraphValues) -> None:
    """Refuse when the graph moved under the run.

    The published generation is what this proof quotes, and repeat ingests do
    create new ones (issue 2076). If the value changed between the read taken
    before the turn and the read taken after it, the answer may be quoting a
    generation that is no longer the published one, and no honest verdict is
    available -- so the run refuses instead of picking a side.
    """
    if before == after:
        return
    raise Refusal(
        "the graph moved during the run: before the turn the read was "
        f"{before!r} and after it {after!r}"
    )


def mcp_serve_command(
    *,
    okg_executable: str,
    deployment: str,
    host: str,
    port: int,
    token_env: str,
    allowed_hosts: tuple[str, ...],
) -> list[str]:
    """Every flag the real MCP server is started with, in one place.

    This function IS the record: the run identity stores exactly what it
    returns, and a reviewer can check the claim "a real okg mcp-serve, from
    the wheel virtual environment, read-only, token required" against it.

    ``--auth-token-env`` carries a VARIABLE NAME. A token value there would be
    refused by mcp-serve and would also land in the recorded command, so a
    value-shaped argument is refused here. No ``--allow-writes``, so the
    server exposes only the read algebra.
    """
    if not ENV_NAME.match(token_env):
        raise Refusal(
            f"--auth-token-env wants an environment VARIABLE NAME, got "
            f"{token_env[:8]!r}...; a token value must never reach a recorded "
            "command line"
        )
    if not allowed_hosts:
        raise Refusal(
            "at least one --allowed-host is required; the server refuses every "
            "HTTP Host header it was not given, which is how the container is "
            "admitted and everything else is not"
        )
    command = [
        okg_executable,
        "mcp-serve",
        "--deployment",
        deployment,
        "--transport",
        "streamable-http",
        "--host",
        host,
        "--port",
        str(port),
        "--auth-token-env",
        token_env,
    ]
    for allowed in allowed_hosts:
        command += ["--allowed-host", allowed]
    return command


#: The ways this proof departs from a plain `okg mcp-serve` start, stated
#: with the run rather than only in the change manifest. Both are config, not
#: code, and neither writes to the graph.
POSTURE_DISCLOSURES = {
    "least_privilege_database_role": (
        "The MCP read pool connects through OKG_MCP_RO_DSN as okg_mcp, the "
        "least-privilege role the migrations create with NOLOGIN; the proof "
        "database gives that role LOGIN and a password and changes nothing "
        "else. An owner session with default_transaction_read_only on, the "
        "posture of the first runs of this proof, no longer works: the server "
        "now evaluates read authority before a search and records that "
        "evaluation, so a read-only session refuses the search with "
        "authority_evaluation_failed (measured 2026-09-30 on the 0.11.4 "
        "compatibility image). With okg_mcp the server writes its own "
        "per-request record, one okg.mcp_call_log row per tool invocation, "
        "beside the function call and output OpenWebUI persists."
    ),
    "model_fixture_network_exposure": (
        "The fixture model listens on 0.0.0.0 with no authentication for the "
        "length of the run, on a run-time free port, because a container on "
        "this platform cannot reach a loopback-only listener on the host and "
        "OpenWebUI upstream connections carry no host-side allowlist to narrow "
        "it with. It serves two routes, answers only from the tool result it "
        "is handed, holds no credential and no database connection, and its "
        "environment is pruned of every OKG variable before it starts. It exits "
        "with the run."
    ),
}


def served_manifest_identity(
    deployment: str, env: Mapping[str, str]
) -> dict[str, Any]:
    """Record WHICH deployment manifest the MCP server was served from."""
    root = env.get("OKG_DEPLOYMENTS_DIR") or ""
    manifest = pathlib.Path(root) / deployment / "deployment.yaml"
    if not manifest.is_file():
        raise Refusal(
            f"no deployment manifest at {manifest}; OKG_DEPLOYMENTS_DIR must "
            "name the root that holds the deployment this proof reads"
        )
    return {
        "served_deployment_manifest": str(manifest),
        "served_deployment_manifest_sha256": hashlib.sha256(
            manifest.read_bytes()
        ).hexdigest(),
    }


def served_tool_names(payload: Any) -> list[str]:
    """The tool names a served MCP endpoint advertises, from its tools/list reply.

    Recorded with every run so the requirement can speak about what THIS server
    served rather than about a constant in this repository. A malformed reply
    refuses: an empty list would otherwise read as a server with no tools.
    """
    if not isinstance(payload, dict):
        raise ChainError(
            f"tools/list did not answer with an object, got "
            f"{type(payload).__name__}"
        )
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ChainError(f"tools/list reply carries no result: {payload!r:.300}")
    tools = result.get("tools")
    if not isinstance(tools, list) or not tools:
        raise ChainError(f"tools/list advertised no tools: {result!r:.300}")
    names = [
        tool["name"]
        for tool in tools
        if isinstance(tool, dict) and isinstance(tool.get("name"), str)
    ]
    if len(names) != len(tools):
        raise ChainError(f"tools/list advertised an unnamed tool: {tools!r:.300}")
    return names


def assert_tool_served(names: list[str], tool: str) -> None:
    """Refuse before the turn if the operator this proof calls is not served."""
    if tool not in names:
        raise Refusal(
            f"the served MCP endpoint advertises {names!r} and does not serve "
            f"{tool!r}; the turn would fail later for a reason that has nothing "
            "to do with the graph"
        )


#: Variables the fixture model is allowed to inherit. An allowlist rather than a
#: denylist: the model needs an interpreter environment and nothing else, and it
#: must be impossible for it to hold a database connection string or a
#: deployments root, so that a reader can see it could not have read the graph.
MODEL_FIXTURE_ENV_KEYS = ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR")


def model_fixture_env(environ: Mapping[str, str]) -> dict[str, str]:
    """The pruned environment the fixture model starts with."""
    env = {
        key: environ[key] for key in MODEL_FIXTURE_ENV_KEYS if environ.get(key)
    }
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    leaked = sorted(
        key
        for key in env
        if key.startswith("OKG_") or key.endswith("_DSN") or "TOKEN" in key
    )
    if leaked:
        raise Refusal(
            f"the fixture model environment would carry {leaked!r}; it must hold "
            "no graph credential, no deployments root, and no run token"
        )
    return env


def okg_child_cwd(okg_executable: str, repo_root: str) -> str:
    """Where the wheel ``okg`` children must run, which is NOT this checkout.

    Measured, not guessed. A wheel-installed ``okg`` refuses outright when its
    working directory is inside an okg checkout -- it reports that bare okg is
    not running this checkout code and tells the operator to use `uv run okg`
    instead. That guard exists to stop a stale install from answering for a
    working tree, and it is exactly right here: this proof wants the wheel okg,
    so the children run from the virtual environment directory that holds them.
    """
    venv = str(pathlib.PurePath(okg_executable).parent.parent)
    root = str(pathlib.PurePath(repo_root))
    if venv == root or venv.startswith(root.rstrip("/") + "/"):
        raise Refusal(
            f"the wheel okg at {okg_executable} resolves to {venv}, inside the "
            f"checkout at {root}; a checkout-free okg is the point of this proof"
        )
    return venv


def persisted_real_chain(
    stored: dict[str, Any], *, session_id: str, values: GraphValues
) -> dict[str, Any]:
    """Read the vendor record of its own tool turn out of the stored message.

    This is the strongest link available, because it is what OpenWebUI says it
    did rather than what a fixture says. It persists an ``output`` list holding
    the function call it made, that call's output, and the assistant message it
    composed (measured on v0.11.0, and unchanged on the 0.11.4 compatibility
    image on 2026-09-30: ``function_call``, ``function_call_output``,
    ``message``, each with a ``status``). 0.11.4 marks a failed call
    ``status: failed`` on both tool items, so a status other than
    ``completed`` refuses by name.
    """
    output = stored.get("output")
    if not isinstance(output, list) or not output:
        raise ChainError("stored message has no output record of the tool turn")
    calls = [
        item
        for item in output
        if isinstance(item, dict) and item.get("type") == "function_call"
    ]
    results = [
        item
        for item in output
        if isinstance(item, dict) and item.get("type") == "function_call_output"
    ]
    # One search call, and (PACT read-passage-rows-3666, D8) at most one
    # inspect of the graph, which names the generation the text search
    # result does not.
    if len(calls) not in (1, 2):
        raise ChainError(
            f"stored output records {len(calls)} function calls, expected 1 or 2"
        )
    if len(results) != len(calls):
        raise ChainError(
            f"stored output records {len(results)} tool results for {len(calls)} calls"
        )
    expected = {
        call_id_for(session_id): PERSISTED_TOOL_NAME,
        graph_call_id_for(session_id): PERSISTED_GRAPH_TOOL_NAME,
    }
    by_id: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for item in calls:
        call_id = item.get("call_id")
        if call_id not in expected:
            raise ChainError(
                f"stored function call id {call_id!r} is not one of this run's "
                f"{sorted(expected)!r}"
            )
        if str(item.get("name") or "") != expected[call_id]:
            raise ChainError(
                f"stored function call names {item.get('name')!r}, expected exactly "
                f"{expected[call_id]!r}"
            )
        paired = [r for r in results if r.get("call_id") == call_id]
        if len(paired) != 1:
            raise ChainError("stored tool result does not belong to the stored call")
        by_id[call_id] = (item, paired[0])
    if call_id_for(session_id) not in by_id:
        raise ChainError(f"stored output records no {PERSISTED_TOOL_NAME!r} call")
    call, result = by_id[call_id_for(session_id)]
    name = str(call.get("name") or "")
    if values.node_id not in str(call.get("arguments") or ""):
        raise ChainError(
            "stored function call arguments do not name the node: "
            f"{call.get('arguments')!r}"
        )
    for item, item_result in by_id.values():
        for kind, record in (("function call", item), ("tool result", item_result)):
            if record.get("status") != "completed":
                raise ChainError(
                    f"stored {kind} has status {record.get('status')!r}, not "
                    "'completed'; the vendor recorded the tool call as not answered"
                )
    # Read the persisted tool outputs through the SAME extractor the model
    # used, so what the vendor stored is checked to be a real graph answer for
    # this node rather than merely to contain two strings somewhere.
    (delivered,) = turn_values(
        [item_result.get("output") for _item, item_result in by_id.values()],
        node_id=values.node_id, origin="persisted tool result",
    )
    if delivered != values:
        raise ChainError(
            f"persisted tool result reports {delivered!r}, and the independent "
            f"graph read reports {values!r}"
        )
    return {
        "function_call_name": name,
        "function_call_arguments": call.get("arguments"),
        "function_call_id": call.get("call_id"),
        "tool_result_status": result.get("status"),
        "tool_result_values": delivered._asdict(),
        "has_sources": bool(stored.get("sources")),
    }


def validate_real_graph_chain(
    *,
    model_rows: list[dict[str, Any]],
    stored: dict[str, Any],
    session_id: str,
    window: tuple[Any, Any],
    values: GraphValues,
    check_row_provenance: Any,
) -> dict[str, Any]:
    """Check every recorded link against the independent graph read, or raise.

    ``check_row_provenance`` is injected rather than imported so this module
    keeps one definition of the run-binding rule -- the merged proof owns it
    and both proofs must agree on what "a row from this run" means.
    """
    check_row_provenance(
        model_rows, kind="model", session_id=session_id, window=window
    )
    bodies = [
        row.get("body") or {}
        for row in model_rows
        if row.get("kind") == "model_request"
    ]
    if len(bodies) < 2:
        raise ChainError(
            f"model received {len(bodies)} request(s); the tool-result "
            "continuation never arrived"
        )
    tool_messages = [
        message
        for body in bodies
        for message in body.get("messages") or []
        if isinstance(message, dict) and message.get("role") == "tool"
    ]
    if not tool_messages:
        raise ChainError("model requests contain no tool-result message")
    handed_back = turn_values(
        [str(message.get("content") or "") for message in tool_messages],
        node_id=values.node_id,
        origin="tool result handed to the model",
    )
    for delivered in handed_back:
        if delivered != values:
            raise ChainError(
                f"OpenWebUI handed the model {delivered!r}, and the independent "
                f"graph read reports {values!r}"
            )

    if stored.get("error"):
        raise ChainError(f"OpenWebUI stored a refusal as the result: {stored['error']!r}")
    if stored.get("role") != "assistant":
        raise ChainError(
            f"stored result has role {stored.get('role')!r}, expected assistant"
        )
    text = stored.get("content")
    if not isinstance(text, str) or not text.strip():
        raise ChainError("stored assistant answer has no text")
    for value in (values.node_id, values.generation_id, values.source):
        if value not in text:
            raise ChainError(f"stored assistant answer lost {value}")
    persisted = persisted_real_chain(stored, session_id=session_id, values=values)

    return {
        "model_requests": len(bodies),
        "tool_messages": len(tool_messages),
        "independent_graph_read": values._asdict(),
        "handed_back_to_model": [row._asdict() for row in handed_back],
        "stored_answer": text,
        "session_id": session_id,
        "window": [window[0].isoformat(), window[1].isoformat()],
        "persisted": persisted,
    }


def driver_value_findings(source: str, values: GraphValues) -> list[str]:
    """Name every way the driver source could fabricate the answer.

    The merged proof could check its driver against module constants. Here the
    evidence values only exist at run time, so the check is parameterised: any
    string literal in the driver that carries a value read from the graph, or
    that names the endpoint which writes a finished assistant turn, is a
    finding.
    """
    import ast

    findings: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        literal = node.value
        if FORBIDDEN_WRITE_ENDPOINT in literal:
            findings.append(
                f"line {node.lineno}: literal names the finished-turn write "
                f"endpoint {FORBIDDEN_WRITE_ENDPOINT}"
            )
        for value in (values.generation_id, values.source):
            if value and value in literal:
                findings.append(
                    f"line {node.lineno}: literal carries the graph value {value}, "
                    "which the driver must read rather than author"
                )
    return findings
