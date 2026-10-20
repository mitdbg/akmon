"""The deployment proof harness, driven against a stub over real HTTP (#3413).

PACT ``live-proof-3413``. ``tests/live/mcp_deployment/harness.py`` is what an
operator runs against a real deployment. A harness that cannot fail proves
nothing, so this module serves a small stand-in MCP endpoint -- a real
FastMCP streamable-HTTP app behind a real bearer check, on a loopback port,
reached through the SDK client exactly as the harness reaches a deployment --
and shows each check passing on a correct server and FAILING on a server with
the defect it exists to catch: a value leaked to principal B, a value left
unmasked, a retired token still accepted, an answer that changed after a
restart, a request without a principal served as A, a live refusal that
returns rows or the wrong code, a stale or unpinned live answer, a relation
``inspect`` does not declare, and A served the wrong answer under load.

Why a stub and not the two-principal fixture
--------------------------------------------
``deployments/mcp-two-principal-fixture`` needs a migrated Postgres and a
seeded ``okg mcp-serve`` subprocess, and it declares no live relation, so it
could exercise only half the harness and only on a database host. The stub
answers in the shapes the okg server answers in (the JSON result format, the
``okg.external_live.v1`` envelope, ``status: refused`` with a Nomos code,
``[nomos:masked]``), needs no database, and runs in a second. The real-server
arm is the live run this harness exists for.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import socket
import threading
import time
from pathlib import Path
from typing import Any, Iterator

import pytest

pytest.importorskip("mcp")
pytest.importorskip("uvicorn")

import uvicorn  # noqa: E402
from mcp.server.fastmcp import Context, FastMCP  # noqa: E402

from tests.live.mcp_deployment import harness  # noqa: E402

TOKEN = "stub-token-0f3c8ab21d7e4f6a9b5c0d1e2f3a4b5c"
OLD_TOKEN = "stub-token-retired-9a8b7c6d5e4f30291827"
HEADER = "X-OKG-Principal"
ALICE = "alice@example.edu"
BOB = "bob@example.edu"

LIVE_NODE = "batch-queue:submit:all"
PUBLIC = "node:public-docs"
PRIVATE = "node:private-drive"
PERSON = "node:person-record"
PRIVATE_TEXT = "alice private drive contents"
CLEAR_EMAIL = "alice.secret@example.edu"
_PARTITION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")

#: What the stub does wrong, toggled per test and restored after.
#: ``mask``: True (mask token), False (clear), "drop" (attribute removed under
#: the okg marker), "drop_wrong_marker" (removed; the marker names another).
#: ``anonymous_as``: whom a request with no principal header is served as.
#: ``refusal_rows`` / ``refusal_code``: what an invalid live call returns.
#: ``observed_offset``: seconds the live observed_at lags now; ``pin``: whether
#: the live answer names its generation; ``declared``: the relation inspect
#: declares. ``a_wrong_after``: A's filter calls after this many get B's view.
BEHAVIOUR: dict[str, Any] = {}
_DEFAULT = {"tokens": {TOKEN}, "leak": False, "mask": True, "drop_private": True,
            "any_token": False, "anonymous_as": None, "refusal_rows": False,
            "refusal_code": "live_arguments_invalid", "observed_offset": 0, "pin": True,
            "declared": "partition_now", "a_wrong_after": None, "alice_filter_calls": 0}
_COUNT_LOCK = threading.Lock()


def _records(principal: str) -> list[dict[str, Any]]:
    public = {"node_id": PUBLIC, "subtype": "page",
              "attrs": {"classification": "public", "text": "shared public words"}}
    private = {"node_id": PRIVATE, "subtype": "page",
               "attrs": {"classification": "public", "text": PRIVATE_TEXT}}
    person = {"node_id": PERSON, "subtype": "person",
              "attrs": {"classification": "internal", "email": CLEAR_EMAIL}}
    if principal == ALICE:
        return [public, private, person]
    rows = [public]
    if not BEHAVIOUR["drop_private"]:
        rows.append(private)
    if BEHAVIOUR["mask"] is True:
        person = {**person, "attrs": {"classification": "internal",
                                      "email": "[nomos:masked]", "nomos_redacted": True}}
    elif BEHAVIOUR["mask"] == "drop":
        person = {**person, "attrs": {"classification": "internal", "nomos_redacted": True}}
    elif BEHAVIOUR["mask"] == "drop_wrong_marker":
        person = {**person, "attrs": {"classification": "internal",
                                      "nomos_redacted": ["classification"]}}
    rows.append(person)
    return rows


def _principal(ctx: Context) -> str | None:
    request = ctx.request_context.request
    value = request.headers.get(HEADER) if request is not None else None
    if value is None:
        return BEHAVIOUR["anonymous_as"]
    return value if value in (ALICE, BOB) else None


def _unidentified() -> str:
    return json.dumps({"status": "refused", "error": {
        "code": "nomos_principal_unidentified",
        "data": {"reason_code": "nomos.identity.unidentified_principal"}}})


def _stub_server() -> FastMCP:
    mcp = FastMCP("okg-substrate-stub")

    @mcp.tool(structured_output=False)
    def filter(input: dict, ctx: Context, predicate: dict | None = None,
               select: list | None = None, limit: int = 50) -> str:
        who = _principal(ctx)
        if who is None:
            return _unidentified()
        if who == ALICE and BEHAVIOUR["a_wrong_after"] is not None:
            with _COUNT_LOCK:
                BEHAVIOUR["alice_filter_calls"] += 1
                if BEHAVIOUR["alice_filter_calls"] > BEHAVIOUR["a_wrong_after"]:
                    who = BOB  # A served B's view: the answers got crossed
        body: dict[str, Any] = {"items": _records(who), "pinned_generation_id": "gen-1"}
        if who == BOB and BEHAVIOUR["leak"]:
            body["warnings"] = [f"also matched a hidden row: ({PRIVATE_TEXT})"]
        return json.dumps(body)

    @mcp.tool(structured_output=False)
    def inspect(target: dict, ctx: Context) -> str:
        who = _principal(ctx)
        if who is None:
            return _unidentified()
        node = target.get("node_id")
        if node == LIVE_NODE:
            return json.dumps({"node_id": LIVE_NODE, "subtype": "batch_queue",
                               "live_relations": {BEHAVIOUR["declared"]: {
                                   "action": "external.slurm.read_state",
                                   "args": {"partition": {"type": "string"}}}}})
        for record in _records(who):
            if record["node_id"] == node:
                return json.dumps(record)
        # Echoes the id the caller sent, as a real refusal may.
        return json.dumps({"status": "refused", "error": {
            "code": "nomos_read_denied", "message": f"{node} is not readable"}})

    @mcp.tool(structured_output=False)
    def expand(input: dict, ctx: Context, live: dict | None = None,
               direction: str = "out", limit: int = 10) -> str:
        if _principal(ctx) is None:
            return _unidentified()
        live = live or {}
        if live.get("relation") != "partition_now":
            return json.dumps({"status": "refused", "error": {
                "code": "live_relation_undeclared", "data": {"declared": ["partition_now"]}}})
        args = live.get("args") or {}
        rows = [{"partition": "submit", "jobs_running": 3},
                {"partition": "submit-gpu", "jobs_running": 1}]
        if set(args) - {"partition"} or not _PARTITION.match(str(args.get("partition", ""))):
            body: dict[str, Any] = {"status": "refused", "error": {
                "code": BEHAVIOUR["refusal_code"]}}
            if BEHAVIOUR["refusal_rows"]:  # refused, yet the source was still read
                body["external_live"] = {"schema": "okg.external_live.v1",
                                         "relation": "partition_now", "state": "ok",
                                         "rows": rows}
            return json.dumps(body)
        observed = time.strftime("%Y-%m-%dT%H:%M:%S.000000+00:00",
                                 time.gmtime(time.time() - BEHAVIOUR["observed_offset"]))
        envelope: dict[str, Any] = {
            "schema": "okg.external_live.v1", "relation": "partition_now",
            "state": "ok", "trust": "untrusted", "source": "http",
            "observed_at": observed, "query": dict(args), "policy": "inline", "rows": rows}
        if BEHAVIOUR["pin"]:
            envelope["pinned_generation_id"] = "gen-1"
        return json.dumps({"neighbors": [], "external_live": envelope})

    return mcp


def _with_bearer(app: Any) -> Any:
    """401 before the MCP app for any token not currently accepted."""

    async def asgi(scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            headers = dict(scope.get("headers") or [])
            auth = headers.get(b"authorization", b"").decode()
            accepted = auth.startswith("Bearer ") and (
                BEHAVIOUR["any_token"] or auth.removeprefix("Bearer ") in BEHAVIOUR["tokens"]
            )
            if not accepted:
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-type", b"application/json")]})
                await send({"type": "http.response.body",
                            "body": b'{"error":"invalid_token"}'})
                return
        await app(scope, receive, send)

    return asgi


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture(scope="module")
def stub_url() -> Iterator[str]:
    BEHAVIOUR.clear()
    BEHAVIOUR.update(_DEFAULT)
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(
        _with_bearer(_stub_server().streamable_http_app()),
        host="127.0.0.1", port=port, log_level="warning", lifespan="on"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise AssertionError("the stub MCP server did not start")
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.fixture(autouse=True)
def _reset_behaviour() -> Iterator[None]:
    BEHAVIOUR.clear()
    BEHAVIOUR.update({**_DEFAULT, "tokens": set(_DEFAULT["tokens"])})
    yield
    BEHAVIOUR.clear()
    BEHAVIOUR.update({**_DEFAULT, "tokens": set(_DEFAULT["tokens"])})


def _expectations() -> dict[str, Any]:
    return {
        "schema": harness.EXPECTATIONS_SCHEMA,
        "deployment": "stub",
        "live": {
            "max_observed_age_seconds": 120,
            "calls": [{"name": "all", "node_id": LIVE_NODE, "relation": "partition_now",
                       "args": {"partition": "all"}, "min_rows": 1, "max_rows": 16,
                       "required_row_keys": ["partition", "jobs_running"],
                       "b_expect_state": "ok"}],
            "refusals": [
                {"name": "pattern", "node_id": LIVE_NODE, "relation": "partition_now",
                 "args": {"partition": "no spaces"}, "expect_code": "live_arguments_invalid"},
                {"name": "unknown_key", "node_id": LIVE_NODE, "relation": "partition_now",
                 "args": {"partition": "all", "sizee": 1}, "expect_code": "live_arguments_invalid"},
                {"name": "undeclared", "node_id": LIVE_NODE, "relation": "node_now",
                 "args": {"node": "n1"}, "expect_code": "live_relation_undeclared"},
            ],
        },
        "multi_user": {
            "denied_node_ids": [PRIVATE],
            "calls": [{"name": "pages", "tool": "filter",
                       "arguments": {"input": {"kind": "nodes"}, "limit": 50},
                       "a": {"node_ids_include": [PUBLIC]},
                       "b": {"node_ids_include": [PUBLIC]}}],
            "refusal": {"tool": "inspect",
                        "arguments": {"target": {"kind": "node", "node_id": PRIVATE}},
                        "b_expect_code": "nomos_read_denied"},
            "mask": {"tool": "inspect",
                     "arguments": {"target": {"kind": "node", "node_id": PERSON}},
                     "node_id": PERSON, "attribute": "email"},
        },
        "concurrency": {"calls_per_principal": 5, "parallelism": 5},
    }


def _target(url: str, *, old_token: str | None = None) -> harness.Target:
    return harness.Target(url=url, token=TOKEN, token_env="STUB_TOKEN",
                          principal_header=HEADER, principal_a=ALICE, principal_b=BOB,
                          old_token=old_token,
                          old_token_env="STUB_OLD_TOKEN" if old_token else None)


def _run(url: str, phase: str = "baseline", **kwargs: Any) -> harness.RunResult:
    old = kwargs.pop("old_token", None)
    return asyncio.run(harness.run(_target(url, old_token=old), _expectations(),
                                   phase=phase, **kwargs))


def _check(evidence: dict[str, Any], section: str, name: str) -> dict[str, Any]:
    matches = [c for c in evidence["checks"] if c["section"] == section and c["name"] == name]
    assert len(matches) == 1, [f"{c['section']}.{c['name']}" for c in evidence["checks"]]
    return matches[0]


def test_a_correct_deployment_passes_every_check(stub_url: str) -> None:
    result = _run(stub_url)
    evidence = result.evidence

    assert evidence["summary"]["ok"] is True, evidence["summary"]
    live = _check(evidence, "live", "call[all]")
    assert live["detail"]["rows"] == 2
    assert live["detail"]["pinned_generation_id"] == "gen-1"
    assert live["detail"]["observed_age_seconds"] is not None
    assert _check(evidence, "live", "refusal[unknown_key]")["detail"]["observed"][
        "tool_code"] == "live_arguments_invalid"
    scan = _check(evidence, "multi_user", "no_a_only_value_in_any_b_response")["detail"]
    assert scan["a_only_values_scanned"] >= 2  # the denied id and its text, at least
    concurrency = _check(evidence, "concurrency", "interleaved")["detail"]
    assert concurrency["a_ok"] == concurrency["b_ok"] == 5


def test_the_written_evidence_holds_no_token_principal_or_hidden_value(
    stub_url: str, tmp_path: Path
) -> None:
    result = _run(stub_url)
    out = tmp_path / "evidence.json"
    harness.write_evidence(result.evidence, out, _target(stub_url), result.sensitive)
    text = out.read_text(encoding="utf-8")
    for value in (TOKEN, ALICE, BOB, CLEAR_EMAIL, PRIVATE, PRIVATE_TEXT):
        assert value not in text, value
    assert "Bearer" not in text


def test_writing_evidence_that_carries_a_hidden_value_is_refused(
    stub_url: str, tmp_path: Path
) -> None:
    evidence = {"note": f"oops {CLEAR_EMAIL}"}
    with pytest.raises(RuntimeError, match="must only appear hashed"):
        harness.write_evidence(evidence, tmp_path / "e.json", _target(stub_url), {CLEAR_EMAIL})
    assert not (tmp_path / "e.json").exists()


def test_a_value_leaked_into_b_fails_the_scan_and_the_concurrency_arm(stub_url: str) -> None:
    BEHAVIOUR["leak"] = True
    evidence = _run(stub_url).evidence

    assert evidence["summary"]["ok"] is False
    scan = _check(evidence, "multi_user", "no_a_only_value_in_any_b_response")
    assert scan["passed"] is False
    assert scan["detail"]["leaked_value_digests"]
    concurrency = _check(evidence, "concurrency", "interleaved")
    assert concurrency["passed"] is False
    assert concurrency["detail"]["b_leaks"] == 5


def test_a_denied_row_served_to_b_fails_the_differential(stub_url: str) -> None:
    BEHAVIOUR["drop_private"] = False
    evidence = _run(stub_url).evidence

    differential = _check(evidence, "multi_user", "differential[pages]")
    assert differential["passed"] is False
    assert "b_sees_denied_node" in differential["detail"]["problems"]


def test_an_unmasked_value_fails_the_mask_check(stub_url: str) -> None:
    BEHAVIOUR["mask"] = False
    evidence = _run(stub_url).evidence

    assert _check(evidence, "multi_user", "b_masked_where_a_is_clear")["passed"] is False
    # And the clear value is now an A-only string B received.
    assert _check(evidence, "multi_user", "no_a_only_value_in_any_b_response")["passed"] is False


def test_rotation_requires_the_old_token_to_be_refused(stub_url: str) -> None:
    passed = _run(stub_url, phase="after-rotation", old_token=OLD_TOKEN).evidence
    assert _check(passed, "transport", "old_token_401")["passed"] is True

    BEHAVIOUR["tokens"] = {TOKEN, OLD_TOKEN}  # the restart did not drop the old token
    failed = _run(stub_url, phase="after-rotation", old_token=OLD_TOKEN).evidence
    check = _check(failed, "transport", "old_token_401")
    assert check["passed"] is False
    assert check["detail"]["http_status"] == 200


def test_a_server_accepting_any_token_fails_the_transport_checks(stub_url: str) -> None:
    BEHAVIOUR["any_token"] = True
    evidence = _run(stub_url).evidence
    assert _check(evidence, "transport", "wrong_token_401")["passed"] is False
    assert _check(evidence, "transport", "wrong_token_401")["detail"]["http_status"] == 200
    assert _check(evidence, "transport", "missing_token_401")["passed"] is True
    assert evidence["summary"]["ok"] is False

    BEHAVIOUR["any_token"] = False
    BEHAVIOUR["tokens"] = set()  # the configured token itself is refused
    evidence = _run(stub_url).evidence
    assert _check(evidence, "transport", "configured_token_200")["passed"] is False
    assert evidence["summary"]["ok"] is False


def test_a_later_phase_must_give_the_baseline_answer(stub_url: str, tmp_path: Path) -> None:
    baseline = _run(stub_url).evidence
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(baseline), encoding="utf-8")

    restarted = harness._now().isoformat()
    same = _run(stub_url, phase="after-restart", baseline_evidence=path,
                restart_declared_at=restarted).evidence
    assert _check(same, "phase", "same_answer_as_baseline")["passed"] is True
    assert _check(same, "phase", "restart_declared_between_runs")["passed"] is True
    assert same["restart_declared_at"] == {"value": restarted, "source": "operator_declared"}

    BEHAVIOUR["drop_private"] = False  # B now gets a different answer
    changed = _run(stub_url, phase="after-restart", baseline_evidence=path,
                   restart_declared_at=restarted).evidence
    assert _check(changed, "phase", "same_answer_as_baseline")["passed"] is False


def test_after_restart_needs_a_declared_restart_time_between_the_runs(
    stub_url: str, tmp_path: Path
) -> None:
    with pytest.raises(harness.HarnessInputError, match="restart-declared-at"):
        _run(stub_url, phase="after-restart")
    with pytest.raises(harness.HarnessInputError, match="ISO 8601"):
        _run(stub_url, phase="after-restart", restart_declared_at="yesterday")

    before_baseline = harness._now().isoformat()
    baseline = _run(stub_url).evidence
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(baseline), encoding="utf-8")
    early = _run(stub_url, phase="after-restart", baseline_evidence=path,
                 restart_declared_at=before_baseline).evidence
    check = _check(early, "phase", "restart_declared_between_runs")
    assert check["passed"] is False
    assert check["detail"]["problems"] == ["before_the_baseline_finished"]
    future = (harness._now() + harness._dt.timedelta(hours=1)).isoformat()
    late = _run(stub_url, phase="after-restart", restart_declared_at=future).evidence
    assert _check(late, "phase", "restart_declared_between_runs")["detail"]["problems"] == [
        "after_this_run_started"]


def test_expectations_without_a_refusal_are_refused(tmp_path: Path) -> None:
    spec = _expectations()
    spec["live"]["refusals"] = []
    path = tmp_path / "x.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    with pytest.raises(harness.HarnessInputError, match="at least one refusal"):
        harness.load_expectations(path)


def _write(tmp_path: Path, spec: dict[str, Any]) -> Path:
    path = tmp_path / "x.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    return path


def test_a_multi_user_refusal_without_an_explicit_code_is_refused(tmp_path: Path) -> None:
    spec = _expectations()
    spec["multi_user"]["refusal"]["b_expect_code"] = None
    with pytest.raises(harness.HarnessInputError, match="b_expect_code must be an explicit"):
        harness.load_expectations(_write(tmp_path, spec))


def test_a_differential_call_must_name_a_node_b_is_served(tmp_path: Path) -> None:
    spec = _expectations()
    spec["multi_user"]["calls"][0]["b"] = {"node_ids_include": []}
    with pytest.raises(harness.HarnessInputError, match="b.node_ids_include"):
        harness.load_expectations(_write(tmp_path, spec))


def test_b_must_call_at_least_one_live_relation(tmp_path: Path) -> None:
    spec = _expectations()
    del spec["live"]["calls"][0]["b_expect_state"]
    with pytest.raises(harness.HarnessInputError, match="b_expect_state"):
        harness.load_expectations(_write(tmp_path, spec))
    with pytest.raises(harness.HarnessInputError, match="b_expect_state"):
        asyncio.run(harness.run(_target("http://127.0.0.1:9/mcp"), spec, phase="baseline"))


def test_the_example_expectations_pass_validation() -> None:
    example = Path(harness.__file__).with_name("expectations.example.json")
    harness.load_expectations(example)


def test_the_command_line_writes_evidence_and_exits_by_the_verdict(
    stub_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = tmp_path / "expectations.json"
    spec.write_text(json.dumps(_expectations()), encoding="utf-8")
    out = tmp_path / "evidence.json"
    monkeypatch.setenv("STUB_TOKEN", TOKEN)
    argv = ["--url", stub_url, "--token-env", "STUB_TOKEN", "--principal-header", HEADER,
            "--principal-a", ALICE, "--principal-b", BOB, "--expectations", str(spec),
            "--out", str(out), "--concurrency", "3", "--parallelism", "3"]

    assert harness.main(argv) == 0
    assert json.loads(out.read_text(encoding="utf-8"))["summary"]["ok"] is True

    BEHAVIOUR["leak"] = True
    assert harness.main(argv) == 1
    assert json.loads(out.read_text(encoding="utf-8"))["summary"]["ok"] is False

    monkeypatch.delenv("STUB_TOKEN")
    assert harness.main(argv) == 2


def test_a_request_with_no_principal_served_as_a_fails_the_unidentified_check(
    stub_url: str,
) -> None:
    BEHAVIOUR["anonymous_as"] = ALICE
    evidence = _run(stub_url).evidence
    check = _check(evidence, "multi_user", "unidentified[no_principal_header]")
    assert check["passed"] is False
    assert check["detail"]["code"] is None
    assert _check(evidence, "multi_user", "unidentified[unbound_principal]")["passed"] is True
    assert evidence["summary"]["ok"] is False


def test_a_live_refusal_that_returns_rows_fails(stub_url: str) -> None:
    BEHAVIOUR["refusal_rows"] = True
    evidence = _run(stub_url).evidence
    check = _check(evidence, "live", "refusal[pattern]")
    assert check["passed"] is False
    assert check["detail"]["observed"]["tool_code"] == "live_arguments_invalid"
    assert check["detail"]["rows"] == 2
    assert _check(evidence, "live", "refusal[undeclared]")["passed"] is True


def test_a_live_refusal_with_the_wrong_code_fails(stub_url: str) -> None:
    BEHAVIOUR["refusal_code"] = "live_source_unavailable"
    evidence = _run(stub_url).evidence
    for name in ("pattern", "unknown_key"):
        check = _check(evidence, "live", f"refusal[{name}]")
        assert check["passed"] is False
        assert check["detail"]["observed"]["tool_code"] == "live_source_unavailable"


@pytest.mark.parametrize(
    ("switch", "value", "problem"),
    [
        ("observed_offset", 3600, "observed_at"),
        ("pin", False, "pinned_generation_id"),
        ("declared", "other_now", "not_declared_by_inspect"),
    ],
)
def test_a_stale_unpinned_or_undeclared_live_answer_fails_the_call(
    stub_url: str, switch: str, value: Any, problem: str
) -> None:
    BEHAVIOUR[switch] = value
    evidence = _run(stub_url).evidence
    check = _check(evidence, "live", "call[all]")
    assert check["passed"] is False
    assert check["detail"]["problems"] == [problem]
    assert evidence["summary"]["ok"] is False


def test_a_wrong_answer_for_a_under_load_fails_the_concurrency_arm(stub_url: str) -> None:
    BEHAVIOUR["a_wrong_after"] = 1  # the differential call is right; later ones are not
    evidence = _run(stub_url).evidence
    assert _check(evidence, "multi_user", "differential[pages]")["passed"] is True
    concurrency = _check(evidence, "concurrency", "interleaved")
    assert concurrency["passed"] is False
    assert concurrency["detail"]["a_wrong_answer"] == 5
    assert concurrency["detail"]["a_ok"] == 0


def test_a_dropped_attribute_counts_as_masked_only_when_a_has_it(stub_url: str) -> None:
    BEHAVIOUR["mask"] = "drop"
    assert _check(_run(stub_url).evidence, "multi_user", "b_masked_where_a_is_clear")[
        "passed"] is True
    BEHAVIOUR["mask"] = "drop_wrong_marker"
    assert _check(_run(stub_url).evidence, "multi_user", "b_masked_where_a_is_clear")[
        "passed"] is False

    dropped = {"node_id": PERSON, "attrs": {"nomos_redacted": True}}
    assert harness.is_masked(dropped, "email", a_has_attribute=True) is True
    assert harness.is_masked(dropped, "email", a_has_attribute=False) is False
    named = {"node_id": PERSON, "attrs": {"nomos_redacted": ["email"]}}
    assert harness.is_masked(named, "email", a_has_attribute=False) is True
    assert harness.is_masked(named, "phone", a_has_attribute=True) is False


def test_an_unreachable_server_records_failed_checks_and_still_writes_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = tmp_path / "expectations.json"
    spec.write_text(json.dumps(_expectations()), encoding="utf-8")
    out = tmp_path / "evidence.json"
    monkeypatch.setenv("STUB_TOKEN", TOKEN)
    argv = ["--url", f"http://127.0.0.1:{_free_port()}/mcp", "--token-env", "STUB_TOKEN",
            "--principal-header", HEADER, "--principal-a", ALICE, "--principal-b", BOB,
            "--expectations", str(spec), "--out", str(out)]

    assert harness.main(argv) == 1
    evidence = json.loads(out.read_text(encoding="utf-8"))
    assert evidence["summary"]["ok"] is False
    transport = _check(evidence, "transport", "configured_token_200")
    assert transport["passed"] is False
    assert transport["detail"]["http_status"] is None
    assert transport["detail"]["transport_error"]


def test_the_evidence_names_the_harness_that_produced_it(
    stub_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    evidence = _run(stub_url).evidence
    source = Path(harness.__file__).read_bytes()
    assert evidence["harness_sha256"] == hashlib.sha256(source).hexdigest()
    assert evidence["harness_git_head"] is None or re.fullmatch(
        r"[0-9a-f]{40}", evidence["harness_git_head"])
    assert evidence["harness_tree_dirty"] in (True, False, None)

    def no_git(*_args: Any, **_kwargs: Any) -> Any:
        raise FileNotFoundError("git")

    monkeypatch.setattr(harness.subprocess, "run", no_git)
    provenance = harness.harness_provenance()
    assert provenance["harness_sha256"] == hashlib.sha256(source).hexdigest()
    assert provenance["harness_git_head"] is None
    assert provenance["harness_tree_dirty"] is None


def test_principal_labels_are_salted_per_run_but_answers_stay_comparable(
    stub_url: str,
) -> None:
    first, second = _run(stub_url).evidence, _run(stub_url).evidence
    assert first["target"]["principal_a"] != second["target"]["principal_a"]
    assert first["target"]["principal_a"] != harness._digest(ALICE)
    assert first["differential_digest"] == second["differential_digest"]
    assert first["differential_digest"]


def test_an_echoed_id_does_not_excuse_a_separate_leaked_field() -> None:
    """The echo of B's own requested id is excused; the same value elsewhere is not."""
    sent = frozenset({"partition_day:q:2026-09-05"})
    echo_only = harness.CallResult(
        payload={"error": {"message": "node 'partition_day:q:2026-09-05' is not visible"}},
        sent=sent,
    )
    assert harness.leaked_values(echo_only, {"2026-09-05"}) == set()
    echo_and_leak = harness.CallResult(
        payload={"error": {"message": "node 'partition_day:q:2026-09-05' is not visible"},
                 "rows": [{"day": "2026-09-05", "text": PRIVATE_TEXT}]},
        sent=sent,
    )
    assert harness.leaked_values(echo_and_leak, {"2026-09-05", PRIVATE_TEXT}) == {
        "2026-09-05", PRIVATE_TEXT}


def test_a_short_sent_string_does_not_hide_a_longer_value() -> None:
    response = harness.CallResult(
        payload={"items": [{"note": "partition_day:q:2026-09-05"}]},
        sent=frozenset({"partition"}),
    )
    assert harness.leaked_values(response, {"partition_day:q:2026-09-05"}) == {
        "partition_day:q:2026-09-05"}


def test_a_value_escaped_in_the_json_is_still_found() -> None:
    value = 'say "hidden words" here'
    response = harness.CallResult(payload={"snippet": value})
    assert harness.leaked_values(response, {value}) == {value}


def test_a_fragment_of_the_callers_own_request_is_not_a_leak() -> None:
    """A refusal echoing B's requested id holds parts of it (a date, a name).

    Seen live on the SubMIT twin: inspect of a denied ``partition_day:<q>:<day>``
    echoed the id, and the day, an A-only string, was counted as a leak.
    """
    echoed = harness.CallResult(
        payload={"error": {"message": "node 'partition_day:q:2026-09-05' is not visible"}},
        sent=frozenset({"partition_day:q:2026-09-05"}),
    )
    assert harness.leaked_values(echoed, {"2026-09-05"}) == set()
    unrelated = harness.CallResult(
        payload={"rows": [{"day": "2026-09-05"}]}, sent=frozenset({"partition_day"}),
    )
    assert harness.leaked_values(unrelated, {"2026-09-05"}) == {"2026-09-05"}
