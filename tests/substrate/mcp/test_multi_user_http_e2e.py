"""Two principals, one real ``okg mcp-serve`` process, different answers.

Slice S1 of #1180. Everything below drives a SUBPROCESS started from the
command line an operator would type, over a real TCP socket, with the MCP
SDK's own ``ClientSession``. Nothing here builds a server in-process, patches
a request context, or calls a tool function directly.

**What this proves.** Given the committed fixture deployment
``mcp-two-principal-fixture`` (which declares Nomos runtime enforcement, a
read posture admitting ``mcp_client``, and the ``nomos.principal_mapping``
block) and database rows binding two principals to two subjects with different
grants: the same request, made twice over the same running server with only
the declared principal header changed, returns different Nomos-governed
results; the five refusal arms return no rows and their own stable codes
(four assert the ``channel`` that refused, and the caller-supplied identity
claim asserts its ``reason_code``); a request whose bearer token is missing or
wrong is refused with a byte-identical 401 before the tool surface; and the
persisted audit names the principal and records the transport's verifier
rather than the caller's own assertion.

**What this does NOT prove.** It does not prove WHICH fronting application
asserted a principal. The transport issues one client id
(``okg-mcp-static-bearer``) for every accepted caller, so ``verified_by`` is a
constant naming the TOKEN, not the app: any holder of that token may name any
user, and the audit trail cannot distinguish them. That is the delegation this
design accepts (sound behind a controlled proxy, not on a reachable port), and
it is the design note's open blocking finding, not something these tests
close. It also does not prove concurrency (interleaved principals on one
process), continuation-token replay across principals, or per-principal
``tools/list`` -- slices S2 and S3 own those. Finally, the server here
connects with the test superuser through ``OKG_MCP_RO_DSN`` rather than the
least-privilege ``okg_mcp`` role, because a freshly created database carries
none of that role's grants; the identity behavior under test is unaffected,
but this run is not evidence about that role.

The database is created and dropped by this module. The shared ``okg_test``
runtime is deliberately not used: the server is a separate process holding its
own pool, and the fixture must own the published generation it reads.
"""

from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    json_client_sessions,
    payload as _result_payload,
)

import asyncio
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence
from urllib.parse import quote, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

pytest.importorskip("mcp")
pytest.importorskip("httpx")

import httpx  # noqa: E402

from tests.substrate.conftest import (  # noqa: E402
    _ensure_test_db_exists,
    _schema_destructive_maintenance_dsn,
)
from tests.substrate.mcp._two_principal_fixture import (  # noqa: E402
    DEPLOYMENT,
    PRINCIPAL_A,
    PRINCIPAL_B,
    PRINCIPAL_HEADER,
    PRIVATE_NODE,
    PUBLIC_NODE,
    SUBJECT_A,
    SUBJECT_B,
    TRANSPORT_VERIFIER,
    seed_all,
)


#: `postgres` only, deliberately not `serial_db`: this module owns a database
#: of its own (created and dropped below) and a server process of its own, so
#: it shares no state with a parallel worker. The database name folds in the
#: base database, xdist worker id, and a fresh nonce so independent pytest
#: sessions cannot drop the same database even with identical configuration.
pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests assert on the dictionary, so every client call asks for
    the JSON result format (issue #3059: the default is one text block)."""
    json_client_sessions(monkeypatch)

_REPO_ROOT = Path(__file__).resolve().parents[3]

#: Disposable-database prefix. Inside the ``okg_test_`` family the test
#: fixtures own (tests/substrate/conftest.py::_is_safe_test_dbname), so the
#: safety guard admits it and no real database can ever be the target.
_DB_PREFIX = "okg_test_mcp2p_"

_TOKEN_ENV = "OKG_MCP_AUTH_TOKEN"
_TOKEN = "0f3c8ab21d7e4f6a9b5c0d1e2f3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c"
_WRONG_TOKEN = "9a8b7c6d5e4f30291827364554637281900f1e2d3c4b5a69788796a5b4c3d2e1"

_UNIDENTIFIED_CODE = "nomos_principal_unidentified"
_CHANNEL_CONFLICT_CODE = "nomos_identity_channel_conflict"

_SERVER_START_TIMEOUT_SECONDS = 180.0
_SERVER_START_ATTEMPTS = 2


# ---------------------------------------------------------------------------
# The disposable database
# ---------------------------------------------------------------------------


def _database_name(base_dsn: str) -> str:
    """Mint one database per fixture invocation, including concurrent sessions.

    The same base database and worker id can occur in independent pytest
    sessions. A fresh nonce keeps one session's force-drop away from another.
    """
    base = psycopg.conninfo.conninfo_to_dict(base_dsn).get("dbname") or ""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    digest = hashlib.sha256(
        f"{base}\x00{worker}\x00{uuid4().hex}".encode("utf-8")
    ).hexdigest()[:24]
    return f"{_DB_PREFIX}{digest}"


def _database_dsn(base_dsn: str, dbname: str) -> str:
    """Rebuild ``base_dsn`` as a URL naming ``dbname``.

    A URL, not a keyword/value string: Chronos' ``connect_sql_database``
    selects its backend by URL scheme and misroutes a scheme-less string to
    the SQLite adapter.
    """
    info = psycopg.conninfo.conninfo_to_dict(base_dsn)
    user, password = info.get("user"), info.get("password")
    host, port = info.get("host"), info.get("port")
    if host is not None and "," in str(host):
        raise RuntimeError(
            f"refusing to derive a test DSN from a multi-host DSN (host={host!r})"
        )
    netloc = ""
    if user:
        netloc += quote(str(user), safe="")
        if password:
            netloc += ":" + quote(str(password), safe="")
        netloc += "@"
    if host:
        netloc += quote(str(host), safe="")
    if port:
        netloc += f":{port}"
    return urlunsplit(("postgresql", netloc, f"/{dbname}", "", ""))


def _drop_database(dsn: str) -> None:
    """Drop only a database this module minted."""
    dbname = psycopg.conninfo.conninfo_to_dict(dsn).get("dbname") or ""
    if not dbname.startswith(_DB_PREFIX):
        raise RuntimeError(f"refusing to drop unexpected database {dbname!r}")
    with psycopg.connect(
        _schema_destructive_maintenance_dsn(dsn), connect_timeout=10, autocommit=True
    ) as conn:
        # WITH (FORCE) terminates the other backends inside the drop, closing
        # the window in which a pooled client reconnects and the drop fails.
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(dbname)
            )
        )


@pytest.fixture(scope="module")
def two_principal_dsn(okg_dsn: str) -> Iterator[str]:
    """A migrated database of this module's own, seeded and then dropped."""
    dsn = _database_dsn(okg_dsn, _database_name(okg_dsn))
    try:
        with psycopg.connect(
            _schema_destructive_maintenance_dsn(dsn), connect_timeout=10
        ):
            pass
    except Exception as exc:  # noqa: BLE001 - no Postgres means skip, not fail
        pytest.skip(f"Postgres not reachable at {okg_dsn}: {exc}")

    _drop_database(dsn)
    from okg.substrate.db import migrate as migrate_mod

    original = dict(migrate_mod.REQUIRED_EXTENSIONS)
    # pg_cron is single-database-per-instance on the dev compose; the shared
    # migrated fixture drops it for the same reason.
    migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        # Inside the try: a failure after the database exists must still reach
        # the drop below, or an unstamped database leaks on the server.
        _ensure_test_db_exists(dsn)
        migrate_mod.migrate(dsn)
        from okg.substrate.db.management import apply_remove_job_runtime_subsystem

        apply_remove_job_runtime_subsystem(dsn, confirm=True)
        seed_all(dsn)
        yield dsn
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        migrate_mod.REQUIRED_EXTENSIONS.clear()
        migrate_mod.REQUIRED_EXTENSIONS.update(original)
        _drop_database(dsn)


# ---------------------------------------------------------------------------
# The real server process
# ---------------------------------------------------------------------------


def _free_port() -> int:
    """Ask the kernel for a free port by binding one, then release it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _server_env(dsn: str, *, token: str = _TOKEN) -> dict[str, str]:
    env = dict(os.environ)
    env[_TOKEN_ENV] = token
    # The read pool would otherwise connect as the least-privilege `okg_mcp`
    # role, which holds no grants in a database created minutes ago. Named
    # here rather than granted, and disclosed in the module docstring: this
    # run is not evidence about that role.
    env["OKG_MCP_RO_DSN"] = dsn
    env["OKG_MCP_TRACE_ENABLED"] = "0"
    # The DEPLOYMENT MANIFEST must be what turns enforcement on. An inherited
    # override would make this proof pass against a deployment that declares
    # nothing, which is exactly the hole slice S0 closes.
    env.pop("OKG_NOMOS_RUNTIME_ENFORCEMENT", None)
    # A process-wide subject is set on purpose: if any request fell back to
    # it instead of resolving the header, the assertions below would see it.
    env["OKG_NOMOS_SUBJECT"] = "subject:process-wide-operator"
    return env


def _start_server(
    dsn: str, port: int, log_path: Path, *, token: str = _TOKEN
) -> subprocess.Popen:
    argv = [
        sys.executable,
        "-m",
        "okg.substrate.cli.main",
        "mcp-serve",
        "--deployment",
        DEPLOYMENT,
        "--dsn",
        dsn,
        "--transport",
        "streamable-http",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--auth-token-env",
        _TOKEN_ENV,
        "--multi-user-principals",
    ]
    # start_new_session: the child leads its own process group and can never
    # signal the pytest-xdist worker (#703, enforced by
    # tests/substrate/test_xdist_spawn_isolation_lint.py). Output goes to a
    # FILE, never a pipe nobody drains -- a full pipe buffer would block the
    # server and read as a hang.
    with log_path.open("wb") as handle:
        return subprocess.Popen(  # noqa: S603
            argv,
            cwd=str(_REPO_ROOT),
            env=_server_env(dsn, token=token),
            stdout=handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )


def _wait_until_listening(proc: subprocess.Popen, port: int, log_path: Path) -> bool:
    """True once the port accepts a connection; False if the server died."""
    deadline = time.monotonic() + _SERVER_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.25)
    raise AssertionError(
        f"okg mcp-serve did not listen on 127.0.0.1:{port} within "
        f"{_SERVER_START_TIMEOUT_SECONDS:g}s; server log:\n"
        + log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
    )


def _stop_server(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=20)


@pytest.fixture(scope="module")
def two_principal_server(
    two_principal_dsn: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[str]:
    """One running ``okg mcp-serve``; yields its endpoint URL."""
    log_dir = tmp_path_factory.mktemp("mcp-two-principal")
    last_log: Path | None = None
    for attempt in range(_SERVER_START_ATTEMPTS):
        # A port chosen by bind-and-release can be taken in the window before
        # the server binds it, so a failed start is retried on a fresh port
        # rather than reported as a defect in the code under test.
        port = _free_port()
        log_path = log_dir / f"server-{attempt}.log"
        last_log = log_path
        proc = _start_server(two_principal_dsn, port, log_path)
        try:
            if _wait_until_listening(proc, port, log_path):
                yield f"http://127.0.0.1:{port}/mcp"
                return
        finally:
            _stop_server(proc)
    raise AssertionError(
        "okg mcp-serve exited before listening on "
        f"{_SERVER_START_ATTEMPTS} attempts; last server log:\n"
        + (
            last_log.read_text(encoding="utf-8", errors="replace")[-4000:]
            if last_log is not None
            else "<no log>"
        )
    )


# ---------------------------------------------------------------------------
# Driving the server with the SDK's own client
# ---------------------------------------------------------------------------


def _headers(
    principal: str | None = None, *, token: str | None = _TOKEN
) -> dict[str, str]:
    headers: dict[str, str] = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if principal is not None:
        headers[PRINCIPAL_HEADER] = principal
    return headers


_FILTER_ARGUMENTS: dict[str, Any] = {
    "input": {"kind": "nodes"},
    "predicate": {},
    "select": ["node_id", "attrs"],
    "limit": 50,
}


async def _call_tool(
    url: str,
    headers: Mapping[str, str] | Sequence[tuple[str, str]],
    arguments: Mapping[str, Any],
) -> Any:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async with streamablehttp_client(url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await session.call_tool("filter", dict(arguments))


def _filter(
    url: str,
    headers: Mapping[str, str] | Sequence[tuple[str, str]],
    *,
    arguments: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """One ``filter`` call over the wire; returns the decoded payload."""
    result = asyncio.run(_call_tool(url, headers, arguments or _FILTER_ARGUMENTS))
    # Since issue #2010 the payload travels in structuredContent and the
    # content list is empty by design: it used to carry a duplicate JSON
    # copy of the same dictionary. What matters is that an answer arrived.
    assert (
        result.structuredContent is not None or result.content
    ), "the server returned no answer at all"
    return _result_payload(result)


def _node_ids(payload: Mapping[str, Any]) -> set[str]:
    return {str(item["node_id"]) for item in payload.get("items", [])}


def _refusal(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Assert the payload is a refusal that returned no rows, and return it."""
    assert payload.get("status") == "refused", payload
    assert "items" not in payload, (
        "a refused request must return NO rows, not a filtered row set"
    )
    error = payload.get("error")
    assert isinstance(error, dict), payload
    return error


# ---------------------------------------------------------------------------
# Reading back what the server persisted
# ---------------------------------------------------------------------------


def _decision_watermark(dsn: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(decision_row_id), 0) FROM okg.nomos_decisions")
        return int(cur.fetchone()[0])


def _decisions_since(dsn: str, watermark: int) -> list[dict[str, Any]]:
    """Decision rows written after ``watermark``.

    Ordered by the BIGSERIAL key, never by ``decision_id``: that column is a
    content hash, so ordering by it is lexicographic noise. Scoping by a
    watermark rather than by table contents keeps every assertion below
    independent of which other test in this module ran first.
    """
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT decision_row_id, subject, action, effect, reason_code,
                   enforcement_point, context
              FROM okg.nomos_decisions
             WHERE deployment_name = %s AND decision_row_id > %s
             ORDER BY decision_row_id
            """,
            (DEPLOYMENT, watermark),
        )
        return [dict(row) for row in cur.fetchall()]


def _request_subjects(rows: list[dict[str, Any]]) -> set[str]:
    return {
        str(row["subject"])
        for row in rows
        if row["action"] in {"graph.read.mcp", "mcp.tool.call"}
    }


def _reason_codes(rows: list[dict[str, Any]]) -> set[str]:
    return {str(row["reason_code"]) for row in rows if row["reason_code"]}


# ==========================================================================
# 1. The differential: the same request, two principals, different answers
# ==========================================================================


def test_two_principals_get_different_rows_from_one_running_server(
    two_principal_server: str, two_principal_dsn: str
) -> None:
    """Identical calls, only the declared header differs, and answers differ."""
    watermark = _decision_watermark(two_principal_dsn)

    a_payload = _filter(two_principal_server, _headers(PRINCIPAL_A))
    b_payload = _filter(two_principal_server, _headers(PRINCIPAL_B))

    a_ids = _node_ids(a_payload)
    b_ids = _node_ids(b_payload)
    # The fixture must DIFFERENTIATE: two empty or two identical row sets
    # would satisfy a weaker assertion while proving nothing at all.
    assert a_ids != b_ids, "the policy fixture is not differentiating"
    assert a_ids == {PUBLIC_NODE, PRIVATE_NODE}
    # B is missing one row, not erroring and not truncated to nothing.
    assert b_ids == {PUBLIC_NODE}
    assert b_payload.get("status") != "refused"

    rows = _decisions_since(two_principal_dsn, watermark)
    # Equality, not membership: a membership test would still pass if the
    # server had ALSO evaluated something under the process-wide subject
    # alongside the two per-caller ones.
    assert _request_subjects(rows) == {SUBJECT_A, SUBJECT_B}
    assert "subject:process-wide-operator" not in {str(row["subject"]) for row in rows}


def test_the_audit_names_the_principal_and_the_transport_verifier(
    two_principal_server: str, two_principal_dsn: str
) -> None:
    """The stored claim records who asserted the user, not the user's say-so.

    The whole blast-radius argument for this delegation rests on the
    delegator being recoverable from the record, so the assertion is on the
    PERSISTED row: a builder stamping a value proves nothing about what the
    audit trail ends up holding.
    """
    _filter(two_principal_server, _headers(PRINCIPAL_A))
    _filter(two_principal_server, _headers(PRINCIPAL_B))

    with (
        psycopg.connect(two_principal_dsn) as conn,
        conn.cursor(row_factory=dict_row) as cur,
    ):
        cur.execute(
            """
            SELECT external_subject,
                   verification_status,
                   evidence ->> 'verified_by' AS verified_by,
                   evidence ->> 'verified_at' AS verified_at
              FROM okg.nomos_external_claims
             WHERE deployment_name = %s AND external_subject = ANY(%s)
            """,
            (DEPLOYMENT, [PRINCIPAL_A, PRINCIPAL_B]),
        )
        claims = [dict(row) for row in cur.fetchall()]

    assert claims, "expected the accepted claims to be persisted"
    assert {row["external_subject"] for row in claims} == {PRINCIPAL_A, PRINCIPAL_B}
    assert {row["verification_status"] for row in claims} == {"valid"}
    assert all(row["verified_at"] for row in claims)
    # One constant for every caller: it names the TOKEN, never the app and
    # never the user. This is the residual risk, asserted rather than hoped
    # for -- see the module docstring.
    assert {row["verified_by"] for row in claims} == {TRANSPORT_VERIFIER}
    assert PRINCIPAL_A not in {row["verified_by"] for row in claims}


# ==========================================================================
# 2. The refusal arms
# ==========================================================================


def test_a_request_without_the_declared_header_is_refused(
    two_principal_server: str, two_principal_dsn: str
) -> None:
    """No principal, no rows -- and its own reason code, not a policy deny."""
    watermark = _decision_watermark(two_principal_dsn)

    error = _refusal(_filter(two_principal_server, _headers(None)))

    assert error["code"] == _UNIDENTIFIED_CODE
    assert error["data"]["reason_code"] == "nomos.identity.unidentified_principal"
    assert error["data"]["channel"] == "header_absent"
    # The refusal is recorded, and recorded as an identity failure rather
    # than as a policy decision about some subject.
    assert "nomos.identity.unidentified_principal" in _reason_codes(
        _decisions_since(two_principal_dsn, watermark)
    )


def test_an_unknown_principal_is_refused_without_reaching_a_subject(
    two_principal_server: str, two_principal_dsn: str
) -> None:
    """A principal nobody bound reaches no rows and no other user's subject."""
    watermark = _decision_watermark(two_principal_dsn)

    error = _refusal(_filter(two_principal_server, _headers("carol@example.edu")))

    assert error["code"] == _UNIDENTIFIED_CODE
    assert error["data"]["channel"] == "no_subject_binding"
    rows = _decisions_since(two_principal_dsn, watermark)
    assert "nomos.identity.no_subject_binding" in _reason_codes(rows)
    # Never another user's subject, and never the process-wide fallback.
    subjects = {str(row["subject"]) for row in rows}
    assert SUBJECT_A not in subjects
    assert SUBJECT_B not in subjects
    assert "subject:process-wide-operator" not in subjects


@pytest.mark.parametrize(
    ("label", "headers"),
    [
        (
            "repeated",
            [
                ("Authorization", f"Bearer {_TOKEN}"),
                (PRINCIPAL_HEADER, PRINCIPAL_A),
                (PRINCIPAL_HEADER, PRINCIPAL_B),
            ],
        ),
        ("comma-folded", _headers(f"{PRINCIPAL_A},{PRINCIPAL_B}")),
    ],
)
def test_an_ambiguous_principal_header_is_refused_not_resolved(
    two_principal_server: str,
    label: str,
    headers: Mapping[str, str] | Sequence[tuple[str, str]],
) -> None:
    """Two principals in one request name nobody, so nobody is served.

    Two headers and one comma-folded value are the same ambiguity at the
    wire: picking either value would let a caller that can add a header
    choose which user a request is attributed to.
    """
    error = _refusal(_filter(two_principal_server, headers))

    assert error["code"] == _UNIDENTIFIED_CODE, label
    assert error["data"]["channel"] == "header_ambiguous", label
    # The rejected value is never echoed back.
    assert PRINCIPAL_A not in json.dumps(error)
    assert PRINCIPAL_B not in json.dumps(error)


def test_an_empty_principal_header_is_refused(two_principal_server: str) -> None:
    """A present-but-empty header is an absent principal, not a wildcard."""
    error = _refusal(_filter(two_principal_server, _headers("")))

    assert error["code"] == _UNIDENTIFIED_CODE
    assert error["data"]["channel"] == "header_empty"


def test_a_caller_supplied_identity_claim_is_refused_outright(
    two_principal_server: str, two_principal_dsn: str
) -> None:
    """Self-asserted identity is never merged with the header, only refused."""
    watermark = _decision_watermark(two_principal_dsn)

    error = _refusal(
        _filter(
            two_principal_server,
            _headers(PRINCIPAL_B),
            arguments={
                **_FILTER_ARGUMENTS,
                "authority": {
                    "nomos_identity_claim": {
                        "provider_id": "okg-chat-sso",
                        "external_subject": PRINCIPAL_A,
                    }
                },
            },
        )
    )

    assert error["code"] == _CHANNEL_CONFLICT_CODE
    assert error["data"]["reason_code"] == "nomos.identity.conflicting_identity_channel"
    assert "nomos.identity.conflicting_identity_channel" in _reason_codes(
        _decisions_since(two_principal_dsn, watermark)
    )


def test_a_tool_argument_naming_another_subject_does_not_widen_the_answer(
    two_principal_server: str,
) -> None:
    """B naming A's subject in an argument still gets exactly B's rows.

    `authority.subject` is not the identity channel and must not become one
    by being ignored quietly in one place and honored in another.
    """
    payload = _filter(
        two_principal_server,
        _headers(PRINCIPAL_B),
        arguments={**_FILTER_ARGUMENTS, "authority": {"subject": SUBJECT_A}},
    )

    assert payload.get("status") != "refused", payload
    assert _node_ids(payload) == {PUBLIC_NODE}
    assert PRIVATE_NODE not in _node_ids(payload)


# ==========================================================================
# 3. The transport credential, below the tool surface
# ==========================================================================


def test_a_missing_or_wrong_bearer_token_is_refused_identically(
    two_principal_server: str,
    two_principal_dsn: str,
    tmp_path: Path,
    record_property,
) -> None:
    """Both unauthenticated arms get the same 401, byte for byte.

    Asserted on a raw request rather than through the SDK client because the
    point is the response the endpoint returns BEFORE any session exists: a
    401 that differed between "no header" and "wrong token" would tell an
    unauthenticated caller which of the two they got wrong.
    """
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    accept = "application/json, text/event-stream"
    responses = {}
    for label, extra in (
        ("missing-header", {}),
        ("wrong-token", {"Authorization": f"Bearer {_WRONG_TOKEN}"}),
    ):
        response = httpx.post(
            two_principal_server,
            json=body,
            headers={"Accept": accept, "Content-Type": "application/json", **extra},
            timeout=30.0,
        )
        responses[label] = response

    assert {r.status_code for r in responses.values()} == {401}
    assert responses["missing-header"].content == responses["wrong-token"].content
    # Nothing about the graph, the principal or the token leaks into it.
    for response in responses.values():
        text = response.text
        assert PRIVATE_NODE not in text
        assert PRINCIPAL_A not in text
        assert _TOKEN not in text
        assert _WRONG_TOKEN not in text

    # Separate owned processes leave the module-scoped server untouched.
    # Fresh fixture secrets also distinguish these processes from other tests.
    tokens = (uuid4().hex + uuid4().hex, uuid4().hex + uuid4().hex)
    port = _free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    phases = []
    generation = None
    for phase, (accepted, rejected) in enumerate((tokens, tokens[::-1])):
        log_path = tmp_path / f"rotation-{phase}.log"
        proc = _start_server(two_principal_dsn, port, log_path, token=accepted)
        try:
            try:
                listening = _wait_until_listening(proc, port, log_path)
            except AssertionError:
                raise AssertionError(
                    "rotation server did not start listening"
                ) from None
            assert listening, "rotation server exited during startup"
            assert proc.poll() is None
            refusals = []
            for credential in (None, rejected):
                response = httpx.post(
                    url,
                    json=body,
                    headers={
                        "Accept": accept,
                        "Content-Type": "application/json",
                        **_headers(PRINCIPAL_A, token=credential),
                    },
                    timeout=30.0,
                )
                assert response.status_code == 401
                assert response.content == responses["missing-header"].content
                if any(
                    value in response.text
                    for value in (*tokens, PRIVATE_NODE, PRINCIPAL_A)
                ):
                    raise AssertionError("rotation refusal disclosed protected content")
                refusals.append(response.status_code)

            # A new watermark per phase prevents pre-rotation audit evidence
            # from satisfying post-rotation assertions.
            watermark = _decision_watermark(two_principal_dsn)
            states = asyncio.run(_rotation_reads(url, accepted))
            generations = {state["generation_id"] for state in states}
            assert len(generations) == 1 and None not in generations
            phase_generation = str(generations.pop())
            assert phase_generation
            if generation is None:
                generation = phase_generation
            assert phase_generation == generation
            rows = _decisions_since(two_principal_dsn, watermark)
            assert _request_subjects(rows) == {SUBJECT_A, SUBJECT_B}
            identity_rows = [
                row for row in rows if row["context"].get("identity_verified_by")
            ]
            assert {row["subject"] for row in identity_rows} == {SUBJECT_A, SUBJECT_B}
            assert {
                row["context"]["identity_verified_by"] for row in identity_rows
            } == {TRANSPORT_VERIFIER}
            phases.append(
                {
                    "phase": phase,
                    "generation_id": generation,
                    "subjects": sorted(_request_subjects(rows)),
                    "verified_by": TRANSPORT_VERIFIER,
                    "refusal_statuses": refusals,
                    "pid": proc.pid,
                }
            )
        finally:
            _stop_server(proc)
            assert proc.poll() is not None
            log_text = log_path.read_text(encoding="utf-8", errors="replace")
            if any(token in log_text for token in tokens):
                raise AssertionError(
                    "rotation server log disclosed a fixture credential"
                )
    assert len(phases) == 2 and phases[0]["pid"] != phases[1]["pid"]
    record_property(
        "static_token_rotation",
        json.dumps(
            {
                "phases": phases,
                "same_endpoint": True,
                "processes_stopped": True,
                "logs_without_tokens": True,
            },
            sort_keys=True,
        ),
    )


async def _rotation_reads(url: str, token: str) -> list[dict[str, Any]]:
    """Each principal reconnects and inspects the generation it actually reads."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    states = []
    for principal, expected in (
        (PRINCIPAL_A, {PUBLIC_NODE, PRIVATE_NODE}),
        (PRINCIPAL_B, {PUBLIC_NODE}),
    ):
        async with streamablehttp_client(
            url, headers=_headers(principal, token=token)
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("filter", dict(_FILTER_ARGUMENTS))
                payload = _result_payload(result)
                assert _node_ids(payload) == expected
                assert payload.get("status") != "refused"
                state = await session.call_tool(
                    "inspect", {"target": {"kind": "session"}}
                )
                states.append(_result_payload(state))
    return states


def test_the_principal_header_alone_reaches_nothing_without_the_token(
    two_principal_server: str,
) -> None:
    """A header naming a real, bound user is not a credential."""
    response = httpx.post(
        two_principal_server,
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        headers={
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            PRINCIPAL_HEADER: PRINCIPAL_A,
        },
        timeout=30.0,
    )

    assert response.status_code == 401
    assert PRINCIPAL_A not in response.text


# Issue #1743: counts share the existing identity-filtered read context.
def test_audit_identity_counts_match_visible_rows(two_principal_server: str) -> None:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def inspect(principal: str) -> dict:
        async with streamablehttp_client(
            two_principal_server, headers=_headers(principal)
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(
                    "inspect", {"target": {"kind": "graph"}}
                )
                assert not result.isError
                return _result_payload(result)

    for principal, expected in ((PRINCIPAL_A, 2), (PRINCIPAL_B, 1)):
        rows = _filter(two_principal_server, _headers(principal))
        counts = asyncio.run(inspect(principal))
        assert len(rows["items"]) == expected
        assert counts["node_count"] == expected


def _counts_session(conn, principal, *, multi_user=True, claim=True):
    from datetime import UTC, datetime
    from okg.substrate.mcp.session import open_session
    from tests.substrate.mcp._two_principal_fixture import (
        AUDIENCE,
        DEPLOYMENT,
        ISSUER,
        PROVIDER_ID,
        SUBJECT_A,
        SUBJECT_B,
        TRANSPORT_VERIFIER,
    )

    stamp = datetime.now(UTC).isoformat()
    return open_session(
        conn,
        deployment_name=DEPLOYMENT,
        nomos_subject=SUBJECT_A if principal == PRINCIPAL_A else SUBJECT_B,
        nomos_identity_claim={
            "provider_id": PROVIDER_ID,
            "issuer": ISSUER,
            "audiences": [AUDIENCE],
            "external_subject": principal,
            "claims": {"email": principal},
            "verification_status": "valid",
            "verified_by": TRANSPORT_VERIFIER,
            "verified_at": stamp,
            "auth_time": stamp,
        }
        if claim
        else None,
        nomos_multi_user=multi_user,
    )


@pytest.mark.parametrize("mode", ["audit", "enforce"])
@pytest.mark.parametrize("multi_user", [False, True])
@pytest.mark.parametrize("principal,expected", [(PRINCIPAL_A, 2), (PRINCIPAL_B, 1)])
def test_identity_counts_at_published_pin(
    two_principal_dsn,
    monkeypatch,
    mode,
    multi_user,
    principal,
    expected,
):
    import psycopg
    from okg.substrate.mcp import tools

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    with psycopg.connect(two_principal_dsn) as conn:
        session = _counts_session(conn, principal, multi_user=multi_user)
        result = tools.inspect_graph(conn, session)
        assert len(tools._visible_node_rows(conn, session)) == expected
        assert result["node_count"] == expected
        assert result["subtype_counts"] == {"page": expected}
        assert result["embed_lag"] == expected
        assert result["edge_count"] == 1
        assert result["schema"]["subtypes"] == {"page": {"node_count": expected}}
        assert result["visibility"] == "enforced"
        assert result["truncated"] is False
        assert result["generation_id"] == session.generation_id
        assert result["counts_source"] == "graph_scan"


@pytest.mark.parametrize(
    "mode,claim,expected",
    [
        ("off", False, 2),
        ("off", True, 2),
        ("audit", False, 2),
        ("enforce", False, 1),
    ],
)
def test_single_user_count_compatibility(
    two_principal_dsn, monkeypatch, mode, claim, expected
):
    import psycopg
    from okg.substrate.mcp import tools

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    with psycopg.connect(two_principal_dsn) as conn:
        session = _counts_session(conn, PRINCIPAL_B, multi_user=False, claim=claim)
        result = tools.inspect_graph(conn, session)
        assert result["node_count"] == expected
        assert len(tools._visible_node_rows(conn, session)) == expected
        assert result["visibility"] == (
            "enforced" if mode == "enforce" else "unfiltered"
        )


@pytest.mark.parametrize("mode", ["audit", "enforce"])
@pytest.mark.parametrize("principal", [PRINCIPAL_A, PRINCIPAL_B])
def test_over_budget_counts_are_unavailable(
    two_principal_dsn, monkeypatch, mode, principal
):
    import psycopg
    from okg.substrate.mcp import tools

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    # Real graph has 2 nodes + 1 edge. Lower the work ceiling, not its data.
    monkeypatch.setattr(tools, "_INSPECT_GRAPH_EXACT_ROW_BUDGET", 2)

    def forbidden(*args, **kwargs):
        pytest.fail(
            "over-budget filtered counts must neither scan rows nor read unfiltered totals"
        )

    monkeypatch.setattr(tools, "_graph_summary_per_row", forbidden)
    monkeypatch.setattr(tools, "_graph_counts_from_rollup", forbidden)
    monkeypatch.setattr(tools, "_graph_summary_sql", forbidden)
    with psycopg.connect(two_principal_dsn) as conn:
        session = _counts_session(conn, principal)
        result = tools.inspect_graph(conn, session)
        for key in (
            "node_count",
            "edge_count",
            "subtype_count",
            "edge_type_count",
            "embed_lag",
            "unresolved_mentions",
            "schema",
        ):
            assert result[key] is None
        assert "subtype_counts" not in result
        assert "edge_type_counts" not in result
        assert result["truncated"] is True
        assert result["generation_id"] == session.generation_id
        assert "unavailable" in result["visibility"]


def test_empty_identity_claim_does_not_enable_unfiltered_counts(
    two_principal_dsn, monkeypatch
):
    from dataclasses import replace
    from okg.substrate.mcp import tools

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    with psycopg.connect(two_principal_dsn) as conn:
        session = replace(_counts_session(conn, PRINCIPAL_B), nomos_identity_claim={})
        from okg.substrate.nomos.core import NomosPolicyError

        # Match the row filter's malformed-claim refusal, not an empty count
        # or an unfiltered aggregate that would bypass identity evaluation.
        with pytest.raises(NomosPolicyError, match="provider_id is required"):
            tools.inspect_graph(conn, session)
        with pytest.raises(NomosPolicyError, match="provider_id is required"):
            tools._visible_node_rows(conn, session)


def test_counts_are_global_not_the_bounded_page_size(two_principal_server):
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def compare():
        async with streamablehttp_client(
            two_principal_server, headers=_headers(PRINCIPAL_A)
        ) as (read, write, _):
            async with ClientSession(read, write) as client:
                await client.initialize()
                page = await client.call_tool(
                    "filter", {**_FILTER_ARGUMENTS, "limit": 1}
                )
                counts = await client.call_tool(
                    "inspect", {"target": {"kind": "graph"}}
                )
                return _result_payload(page), _result_payload(counts)

    page, counts = asyncio.run(compare())
    assert len(page["items"]) == 1
    assert counts["node_count"] == 2
    assert counts["truncated"] is False
    assert page["pinned_generation_id"] == counts["generation_id"]


def test_identity_counts_remain_at_pin_after_publication(okg_dsn, monkeypatch, record_property):
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.mcp import tools
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    # Own an additional disposable database so test ordering cannot change the
    # two-node fixture used by the other real-process tests in this module.
    fixture = two_principal_dsn.__wrapped__(okg_dsn)
    dsn = next(fixture)
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            old = {p: _counts_session(conn, p) for p in (PRINCIPAL_A, PRINCIPAL_B)}
            store = ChronosGraphStore.from_connection(conn)
            store.apply_facts(
                [
                    NodeFact(
                        "node:public-later",
                        "page",
                        {"text": "later public"},
                        {"id": "later"},
                        {"v": 1},
                    ),
                ],
                source="public_docs",
            )
            store.apply_facts(
                [
                    NodeFact(
                        "node:private-mention",
                        "entity_mention",
                        {"entity_type": "private_only"},
                        {"id": "private-mention"},
                        {"v": 1},
                    ),
                    EdgeFact(
                        "node:private-mention",
                        PRIVATE_NODE,
                        "mentions",
                        source_record_id={"id": "private-link"},
                        source_revision={"v": 1},
                    ),
                ],
                source="google_drive",
            )
        published = Publisher(
            dsn,
            DEPLOYMENT,
            catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
        ).cycle()
        assert published["status"] == "published"
        assert published["generation_id"] != old[PRINCIPAL_A].generation_id
        record_property("before_generation", old[PRINCIPAL_A].generation_id)
        record_property("after_generation", published["generation_id"])
        record_property("visible_counts_before", "alice=2,bob=1")
        record_property("visible_counts_after", "alice=4,bob=2;edges=2/1;mentions=1/0")
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
        with psycopg.connect(dsn) as conn:
            for principal, before in ((PRINCIPAL_A, 2), (PRINCIPAL_B, 1)):
                old_counts = tools.inspect_graph(conn, old[principal])
                new = _counts_session(conn, principal)
                new_counts = tools.inspect_graph(conn, new)
                assert old_counts["node_count"] == before
                after = before + (2 if principal == PRINCIPAL_A else 1)
                assert new_counts["node_count"] == after
                assert new_counts["embed_lag"] == after
                assert new_counts["edge_count"] == (
                    2 if principal == PRINCIPAL_A else 1
                )
                if principal == PRINCIPAL_A:
                    assert new_counts["unresolved_mentions"]["total"] == 1
                    assert new_counts["unresolved_mentions"]["resolved"] == 1
                    assert new_counts["unresolved_mentions"]["by_entity_type"] == {
                        "private_only": {"total": 1, "resolved": 1},
                    }
                    assert (
                        new_counts["schema"]["subtypes"]["entity_mention"]["node_count"]
                        == 1
                    )
                    assert new_counts["edge_type_counts"]["mentions"] == 1
                else:
                    assert new_counts["unresolved_mentions"]["total"] == 0
                    assert new_counts["unresolved_mentions"]["by_entity_type"] == {}
                    assert "entity_mention" not in new_counts["schema"]["subtypes"]
                    assert "mentions" not in new_counts["edge_type_counts"]
                    assert "private_only" not in json.dumps(new_counts)
                assert len(tools._visible_node_rows(conn, old[principal])) == before
                assert len(tools._visible_node_rows(conn, new)) == after
                assert old_counts["generation_id"] == old[principal].generation_id
                assert new_counts["generation_id"] == published["generation_id"]
    finally:
        fixture.close()
