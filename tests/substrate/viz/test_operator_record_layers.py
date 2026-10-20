"""The record layers the console's Record page reads (PACT
operator-console-record-layers).

These cases need no database. They cover the parts of the design that are
decisions rather than queries: which parameters each new route knows, what a
recorded filesystem path is allowed to become on its way to a browser, what a
content address must look like before it reaches a query, the caps, and the
headers the byte routes send.

The defect they guard against is a read that looks helpful and is not: an
artifact route that opens a path a source wrote (every source would then be a
way to read any file the server can read), a total that counts neighbours the
reader may not see, a truncated body presented as a whole one, or a fabricated
chunk ordinal that makes a list position look like the document's order.
"""
from __future__ import annotations

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from okg.substrate.console_views import records
from okg.viz import record_endpoints
from okg.viz.read_cache import CLIENT_ONLY_PARAMS
from okg.viz.record_endpoints import register_operator_record_routes

VERSION = "okg:demo:branch:main:gen:1"
NEW_READS = ("raw", "artifact", "artifact_bytes", "payload")
NEW_ROUTES = {
    "raw": "/viz/api/console/records/raw",
    "artifact": "/viz/api/console/records/artifact",
    "artifact_bytes": "/viz/api/console/records/artifact/bytes",
    "payload": "/viz/api/console/records/payload",
}


class _ReadAttempted(AssertionError):
    """Raised when a request that must be refused reached the read path."""


@pytest.fixture()
def client() -> TestClient:
    def ctx_provider():  # pragma: no cover - reaching this IS the failure
        raise _ReadAttempted(
            "the route opened a read context for a request it had to refuse"
        )

    router = APIRouter()
    register_operator_record_routes(router, ctx_provider=ctx_provider)
    app = FastAPI()
    app.include_router(router, prefix="/viz/api")
    return TestClient(app, raise_server_exceptions=True)


# --- the strict-parameter guard knows every new read ------------------------


@pytest.mark.parametrize("read", NEW_READS)
def test_every_new_read_is_in_the_parameter_guard(read: str) -> None:
    """A read missing from the guard would raise KeyError on every request."""
    assert read in record_endpoints._KNOWN_PARAMS
    assert record_endpoints._KNOWN_PARAMS[read] == frozenset({"version", "id"})


@pytest.mark.parametrize("read", NEW_READS)
def test_unknown_parameter_is_refused_before_any_read(
    client: TestClient, read: str,
) -> None:
    response = client.get(
        NEW_ROUTES[read],
        params={"version": VERSION, "id": "node:1", "page": "2"},
    )
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["reason"] == "unknown_parameter"
    assert detail["parameters"] == ["page"]


@pytest.mark.parametrize("read", NEW_READS)
def test_refresh_is_accepted_on_every_new_route(
    client: TestClient, read: str,
) -> None:
    """The shipped client appends it to every read; refusing would error a click."""
    assert "refresh" in CLIENT_ONLY_PARAMS
    # Reaching the read is the pass: this fixture's provider raises there, so
    # the exception proves the parameter got PAST the guard instead of being
    # refused as one the route does not know.
    with pytest.raises(_ReadAttempted):
        client.get(
            NEW_ROUTES[read],
            params={"version": VERSION, "id": "node:1", "refresh": "1"},
        )


@pytest.mark.parametrize("read", NEW_READS)
def test_repeated_parameter_is_refused(client: TestClient, read: str) -> None:
    response = client.get(
        f"{NEW_ROUTES[read]}?version={VERSION}&id=a&id=b",
    )
    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == "repeated_parameter"


# --- a recorded path is masked, and never opened ----------------------------


@pytest.mark.parametrize(
    "recorded",
    [
        "/Users/example/papers/inbox/attention.pdf",
        "/home/someone/Documents/attention.pdf",
        "../../../../etc/passwd",
        "/var/folders/x/../../etc/shadow",
        "C:\\Users\\example\\Desktop\\attention.pdf",
    ],
)
def test_recorded_path_is_reduced_to_its_basename(recorded: str) -> None:
    """No directory, no username, no traversal survives into the response."""
    masked = records._masked_path(recorded)

    assert masked is not None
    assert "/" not in masked
    assert "\\" not in masked
    assert ".." not in masked
    assert "example" not in masked
    assert "someone" not in masked


def test_recorded_path_of_a_hidden_value_is_dropped() -> None:
    from okg.substrate.nomos.read_masking import MASK_TOKEN

    assert records._masked_path(MASK_TOKEN) is None
    assert records._recorded_original_path({"source_pdf_path": MASK_TOKEN}) is None


def test_recorded_path_reads_the_attributes_sources_actually_write() -> None:
    assert records._recorded_original_path(
        {"source_pdf_path": "/Users/example/inbox/a.pdf"},
    ) == "a.pdf"
    assert records._recorded_original_path({"path": "docs/develop/index.md"}) == "index.md"
    assert records._recorded_original_path({"unrelated": "/tmp/x"}) is None


def test_artifact_composer_never_touches_the_filesystem(monkeypatch) -> None:
    """The whole traversal class is absent because no path is ever opened.

    Every call that could read a file's CONTENT is replaced with one that
    fails, and the composer is run over a record whose recorded path is a
    traversal. `Path.resolve` is deliberately NOT patched: it reads no content,
    and unrelated deployment-config lookups call it.
    """
    import builtins
    import io
    import os
    import pathlib

    class _Opened(AssertionError):
        pass

    def _refuse(*args, **kwargs):  # pragma: no cover - reaching this IS failure
        raise _Opened(f"the artifact read opened something: {args!r}")

    monkeypatch.setattr(builtins, "open", _refuse)
    monkeypatch.setattr(io, "open", _refuse)
    monkeypatch.setattr(os, "open", _refuse)
    monkeypatch.setattr(pathlib.Path, "open", _refuse)
    monkeypatch.setattr(pathlib.Path, "read_bytes", _refuse)
    monkeypatch.setattr(pathlib.Path, "read_text", _refuse)

    row = {
        "node_id": "paper:1",
        "subtype": "paper",
        "attrs": {"source_pdf_path": "../../../../etc/passwd", "format": "pdf"},
        "source": "papers",
        "source_record_id": {},
        "source_revision": {},
    }
    # The subject here is what the composer does with the record, not how the
    # record is fetched and filtered -- the Postgres file covers that against a
    # real database. So the read filter is short-circuited and the composer is
    # handed the visible row directly.
    monkeypatch.setattr(records, "checkout", lambda _conn, _session: object())
    monkeypatch.setattr(
        records, "_visible_record_row", lambda *_args, **_kwargs: row,
    )

    class _Session:
        generation_id = VERSION

    described = records.compose_record_artifact(
        object(), session=_Session(), node_id="paper:1",
    )

    assert described["exists"] is False
    assert described["reason"] == "not_retained"
    assert described["path"] == "passwd"
    assert "etc" not in described["detail"]
    assert ".." not in str(described)


# --- content addresses are checked before they reach a query ----------------


@pytest.mark.parametrize(
    "value",
    [
        "", "not-a-hash", "0" * 63, "0" * 65, "G" * 64, "A" * 64,
        "0" * 32 + "'; DROP TABLE okg.visual_crop_blobs; --",
        None, 17, b"0" * 64,
    ],
)
def test_a_non_digest_is_not_a_content_address(value) -> None:
    assert records._is_content_address(value) is False


def test_a_digest_is_a_content_address() -> None:
    assert records._is_content_address("a" * 64) is True
    assert records._is_content_address("0123456789abcdef" * 4) is True


def test_payload_bytes_refuses_a_non_digest_before_querying() -> None:
    class _Conn:
        def cursor(self):  # pragma: no cover - reaching this IS the failure
            raise AssertionError("a non-digest reached the database")

    with pytest.raises(ValueError, match="content address"):
        records._payload_bytes(_Conn(), "'; DROP TABLE x; --", limit=10)


def test_payload_bytes_stops_reading_at_its_limit() -> None:
    """The accumulation stops at the limit, and it stops on a SERVER-SIDE cursor.

    The laziness of the loop was never the whole story: a plain psycopg cursor
    fetches the entire result set when `execute` returns, so stopping early
    still pulled every chunk across the wire. The cursor this opens must be a
    NAMED one, which fetches in batches, or the bound is only on the copy.
    """
    read: list[int] = []
    opened: dict[str, object] = {}

    class _Cursor:
        itersize = 2000

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, *_args):
            return None

        def __iter__(self):
            for index in range(1000):
                read.append(index)
                yield (b"x" * 100,)

    class _Transaction:
        def __enter__(self):
            opened["transaction"] = True
            return self

        def __exit__(self, *_exc):
            return False

    class _Conn:
        def transaction(self):
            return _Transaction()

        def cursor(self, name=None):
            opened["name"] = name
            return _Cursor()

    out = records._payload_bytes(_Conn(), "a" * 64, limit=250)

    assert len(out) == 250
    # Three 100-byte chunks reach the limit; the remaining 997 stay in the
    # database rather than being read and thrown away.
    assert len(read) == 3
    # A server-side cursor, not the default one that would have fetched all
    # 1000 rows before the loop's first iteration.
    assert opened["name"], "the payload read must use a named (server-side) cursor"
    # ...opened inside an explicit transaction, because the record routes run
    # the connection in autocommit and DECLARE CURSOR is refused there. Without
    # this the named cursor cannot be created at all.
    assert opened.get("transaction"), "the named cursor needs its own transaction"


def test_stored_payload_size_is_measured_before_any_payload_is_selected() -> None:
    """The byte_count column is a claim; the chunk rows are the measurement."""
    asked: list[str] = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, sql, _params=None):
            asked.append(" ".join(sql.split()))

        def fetchone(self):
            return (4096,)

    class _Conn:
        def cursor(self, name=None):
            assert name is None, "the size probe needs no server-side cursor"
            return _Cursor()

    assert records._stored_payload_size(_Conn(), "b" * 64) == 4096
    assert "octet_length(payload)" in asked[0]
    # It must NOT select the payload itself.
    assert "SELECT payload" not in asked[0]


def test_stored_payload_size_refuses_a_non_digest() -> None:
    class _Conn:
        def cursor(self, name=None):  # pragma: no cover - reaching this IS failure
            raise AssertionError("a non-digest reached the database")

    with pytest.raises(ValueError, match="content address"):
        records._stored_payload_size(_Conn(), "not-a-digest")


def test_a_payload_too_deep_to_parse_is_not_called_json() -> None:
    """RecursionError is not a ValueError, so it used to escape as a 500."""
    deep = ("[" * 20000) + ("]" * 20000)

    assert records._as_json_text(deep.encode("utf-8")) is None


def test_crop_metadata_refuses_a_non_digest_before_querying() -> None:
    class _Conn:
        def cursor(self):  # pragma: no cover - reaching this IS the failure
            raise AssertionError("a non-digest reached the database")

    assert records._crop_metadata(_Conn(), "not-a-digest") is None


# --- the caps are real, and the console agrees with two of them -------------


def test_the_console_and_the_server_hold_the_same_two_numbers() -> None:
    """console/model/explore/raw.ts: INLINE_PAYLOAD_LIMIT and RAW_HEAD_LINES.

    The client THROWS when an inline payload arrives past its limit, so a
    server that inlined more would break the tab rather than degrade it.
    """
    assert records.RAW_INLINE_PAYLOAD_MAX_BYTES == 256 * 1024
    assert records.RAW_HEAD_LINES == 200


def test_the_ceilings_are_ordered_so_each_one_can_fire() -> None:
    assert (
        records.RAW_INLINE_PAYLOAD_MAX_BYTES
        < records.RAW_PAYLOAD_MAX_BYTES
    )
    assert records.RAW_EDGE_CAP > 0
    assert records.CHUNK_CAP > 0
    assert records.RELATED_TALLY_CEILING > records.RELATED_CAP


def test_svg_is_not_a_servable_artifact_type() -> None:
    """It is an image to a reader and a scriptable document to a browser."""
    assert "image/svg+xml" not in records.ARTIFACT_MEDIA_TYPES
    assert "text/html" not in records.ARTIFACT_MEDIA_TYPES
    assert records.ARTIFACT_MEDIA_TYPES == frozenset(
        {"image/png", "image/jpeg", "image/gif", "image/webp"},
    )


def test_the_not_found_raw_answer_is_constant() -> None:
    """Denied and absent share one value, so they cannot differ in any field."""
    first = records._not_found_raw()
    second = records._not_found_raw()

    assert first == second
    assert first["row"] is None
    assert first["edges"] == []
    assert first["payload"] is None
    assert first["refusal"]["reason"] == "not_found"


# --- the byte routes serve nothing a browser will run -----------------------


def test_byte_headers_forbid_sniffing_scripting_and_cross_origin_reads() -> None:
    headers = record_endpoints._BYTE_HEADERS

    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["Content-Security-Policy"] == "default-src 'none'; sandbox"
    assert headers["Cross-Origin-Resource-Policy"] == "same-origin"
    # `inline`, not `attachment`: the console DISPLAYS these images, and the
    # safety comes from the allowlist plus these headers, not from a download
    # prompt the reader would have to dismiss for every figure.
    assert headers["Content-Disposition"] == "inline"
    assert "no-store" in headers["Cache-Control"]


# --- a server fault never tells the browser what it was ---------------------


SECRET = "postgresql://okg:hunter2@10.0.0.4:5432/okg_prod"


class _StubPool:
    def connection(self):
        class _Ctx:
            def __enter__(self):
                class _Conn:
                    autocommit = False

                return _Conn()

            def __exit__(self, *_exc):
                return False

        return _Ctx()


class _StubCtx:
    deployment_name = "demo"
    pool = _StubPool()

    class session:  # noqa: N801 - mirrors VizContext's attribute
        branch_name = "main"


def _stub_ctx_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(
        "okg.substrate.operator_read.authorize_local_operator_read",
        lambda *_args, **_kwargs: None,
    )
    router = APIRouter()
    register_operator_record_routes(router, ctx_provider=_StubCtx)
    app = FastAPI()
    app.include_router(router, prefix="/viz/api")
    return TestClient(app, raise_server_exceptions=False)


def test_a_version_open_fault_does_not_print_its_exception(monkeypatch) -> None:
    """A psycopg failure's text carries the DSN; the browser gets the type."""
    def _boom(*_args, **_kwargs):
        raise RuntimeError(f"connection to {SECRET} failed")

    monkeypatch.setattr("okg.substrate.mcp.session.open_session", _boom)
    client = _stub_ctx_client(monkeypatch)

    response = client.get(
        "/viz/api/console/records/raw", params={"version": VERSION, "id": "n:1"},
    )

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["reason"] == "version_unavailable"
    assert detail["error_type"] == "RuntimeError"
    assert detail["error"] == record_endpoints._OPAQUE_FAULT
    assert SECRET not in response.text
    assert "hunter2" not in response.text


def test_a_compose_fault_does_not_print_its_exception(monkeypatch) -> None:
    """Including a SubstrateError's own `message`, built from the same places."""
    from okg.substrate.errors import SubstrateError

    class _Session:
        generation_id = VERSION
        deployment_name = "demo"
        branch_name = "main"
        catalog_version_id = 1

    monkeypatch.setattr(
        "okg.substrate.mcp.session.open_session",
        lambda *_args, **_kwargs: _Session(),
    )

    def _boom(*_args, **_kwargs):
        raise SubstrateError(f"SELECT * FROM okg.graph_nodes -- at {SECRET}")

    monkeypatch.setattr(records, "compose_record_raw", _boom)
    client = _stub_ctx_client(monkeypatch)

    response = client.get(
        "/viz/api/console/records/raw", params={"version": VERSION, "id": "n:1"},
    )

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["reason"] == "record_raw_failed"
    assert detail["error_type"] == "SubstrateError"
    assert detail["error"] == record_endpoints._OPAQUE_FAULT
    assert SECRET not in response.text
    assert "graph_nodes" not in response.text


def test_the_opaque_fault_sentence_names_no_cause() -> None:
    sentence = record_endpoints._OPAQUE_FAULT

    assert "log" in sentence
    for leak in ("postgres", "dsn", "password", "select", "/users/", "host="):
        assert leak not in sentence.lower()


# --- chunk ordinals are stored, never invented ------------------------------


def test_a_heading_path_list_becomes_one_string() -> None:
    assert records._heading_path(["Design", "Reads"]) == "Design / Reads"
    assert records._heading_path([]) is None
    assert records._heading_path("  ") is None
    assert records._heading_path(None) is None


@pytest.mark.parametrize("value", [None, True, False, -1, "3", 3.0])
def test_a_page_number_that_is_not_a_count_is_absent(value) -> None:
    assert records._chunk_page({"page_number": value}) is None


def test_a_real_page_number_is_kept() -> None:
    assert records._chunk_page({"page_number": 7}) == 7


@pytest.mark.parametrize("value", [0, 40, "0", "120"])
def test_a_stored_char_offset_is_read_as_a_number(value) -> None:
    """Both spellings chunkers write, and both read as numbers.

    The string form is not incidental: a document_chunk row carrying
    `"char_offset": "40"` is the ordinary shape, and "120" sorts before "40"
    as text, so a lexical read here would be a wrong order that looked right.
    """
    assert records._char_offset({"char_offset": value}) == int(value)


@pytest.mark.parametrize(
    "value",
    [
        None, True, False, -1, 1.5, "", "  ", "40.0", "-40", "x",
        # A digit to Python, not an integer to int(): the guard exists so
        # this returns None instead of raising inside the read.
        "²",
    ],
)
def test_a_char_offset_that_cannot_place_a_chunk_is_absent(value) -> None:
    assert records._char_offset({"char_offset": value}) is None


def test_a_missing_or_unshaped_attribute_bag_has_no_char_offset() -> None:
    assert records._char_offset({}) is None
    assert records._char_offset(None) is None
    assert records._char_offset("char_offset=3") is None


def test_a_record_id_is_spelled_for_the_authority_both_ways() -> None:
    """Both JSON spellings are tried, not one picked and the miss blamed on the store."""
    assert records._canonical_record_ids("rec:1") == ["rec:1"]
    spellings = records._canonical_record_ids({"b": 2, "a": 1})
    assert '{"a":1,"b":2}' in spellings
    assert '{"a": 1, "b": 2}' in spellings
    assert len(spellings) == 2
    assert records._canonical_record_ids({}) == []
    assert records._canonical_record_ids(None) == []


# --- one masking rule, applied to every read --------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "/Users/example/corpus/guide.md",
        "/home/example/corpus/guide.md",
        "~/corpus/guide.md",
        "~example/corpus/guide.md",
        "C:\\Users\\example\\corpus\\guide.md",
        "\\\\fileserver\\share\\example\\guide.md",
        "file:///Users/example/corpus/guide.md",
        "/Users/example/My Documents/guide.md",
    ],
)
def test_an_absolute_path_value_is_masked_to_its_basename(value: str) -> None:
    masked, keys = records.mask_paths(value, at="attrs.path")

    assert masked == "guide.md"
    assert keys == ["attrs.path"]


@pytest.mark.parametrize(
    "value",
    [
        "https://github.com/example-org/example-repo/blob/main/docs/index.md",
        "http://example.test/a/b",
        # A ONE-LETTER scheme is the case the `://` guard exists for: without
        # it, `x:/` reads as a Windows drive and the URL would be masked to
        # its last segment.
        "x://example.test/a/b",
        "docs/develop/index.md",
        "guide.md",
        "not a path at all",
        "",
    ],
)
def test_a_url_or_a_relative_path_is_left_alone(value: str) -> None:
    """citation_url is the console's "open in source"; masking it would break it."""
    masked, keys = records.mask_paths(value, at="attrs.url")

    assert masked == value
    assert keys == []


def test_masking_reaches_nested_values_and_names_every_key() -> None:
    value = {
        "title": "Guide",
        "path": "/Users/example/corpus/guide.md",
        "history": [
            {"file": "/Users/example/corpus/old.md"},
            {"file": "docs/new.md"},
        ],
    }

    masked, keys = records.mask_paths(value, at="row.attrs")

    assert masked["path"] == "guide.md"
    assert masked["history"][0]["file"] == "old.md"
    assert masked["history"][1]["file"] == "docs/new.md"
    assert masked["title"] == "Guide"
    assert keys == ["row.attrs.path", "row.attrs.history[0].file"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("file:///Users/example/corpus/guide.md", "guide.md"),
        ("file://localhost/Users/example/corpus/guide.md", "guide.md"),
        ("file:/Users/example/corpus/guide.md", "guide.md"),
        ("FILE:///Users/example/corpus/guide.md", "guide.md"),
        ("\\\\?\\C:\\Users\\example\\guide.md", "guide.md"),
        ("\\\\?\\UNC\\host\\share\\guide.md", "guide.md"),
    ],
)
def test_every_file_url_and_extended_prefix_is_masked(
    value: str, expected: str,
) -> None:
    masked, keys = records.mask_paths(value, at="attrs.path")

    assert masked == expected
    assert keys == ["attrs.path"]


@pytest.mark.parametrize("value", ["/", "C:\\", "\\\\?\\", "//"])
def test_a_path_with_no_basename_says_so_rather_than_going_empty(value: str) -> None:
    """An empty string would read as "this field is empty"."""
    masked, keys = records.mask_paths(value, at="attrs.path")

    assert masked == records.PATH_WITHHELD
    assert keys == ["attrs.path"]


def test_a_forward_slash_protocol_relative_value_is_not_a_unc_share() -> None:
    """UNC is a BACKSLASH form; //host/x is a protocol-relative URL.

    It is still masked, because the broad rule shortens anything beginning
    with "/" -- but as a slash-led value, not because it was mistaken for a
    Windows share. The backslash form is the one that is UNC.
    """
    assert records._looks_absolute("\\\\host\\share\\x") is True
    assert records._looks_absolute("//host/x") is True
    masked, _ = records.mask_paths("//host/x", at="a")
    assert masked == "x"


def test_a_slash_led_value_that_is_not_a_file_is_shortened_and_named() -> None:
    """The stated cost of the broad rule, proved rather than promised."""
    value = {"route": "/api/v1/records", "pattern": "/^ab+c$/"}

    masked, keys = records.mask_paths(value, at="row.attrs")

    assert masked["route"] == "records"
    # A slash-delimited regex keeps its last segment, which is the body of the
    # pattern minus its opening slash. That is the cost, stated: the value is
    # rewritten and named, not silently kept and not silently dropped.
    assert masked["pattern"] == "^ab+c$"
    assert sorted(keys) == ["row.attrs.pattern", "row.attrs.route"]
    assert "begins with '/'" in records.PATH_RULE_LIMIT


# --- keys are masked too ----------------------------------------------------


def test_a_path_shaped_key_is_masked_and_reported_under_its_masked_name() -> None:
    """Masking values alone republished the path as the key, then again in
    masked_paths."""
    value = {"/Users/example/corpus/guide.md": {"lines": 12}}

    masked, keys = records.mask_paths(value, at="row.attrs")

    assert list(masked) == ["guide.md"]
    assert keys == ["row.attrs.guide.md"]
    assert "example" not in str(masked)
    assert "example" not in str(keys)


def test_two_keys_masking_to_one_basename_keep_both() -> None:
    """Neither field is dropped and neither overwrites the other."""
    value = {
        "/Users/example/a/notes.md": 1,
        "/Users/example/b/notes.md": 2,
        "/Users/example/c/notes.md": 3,
    }

    masked, keys = records.mask_paths(value, at="row.attrs")

    assert list(masked) == ["notes.md", "notes.md~2", "notes.md~3"]
    assert list(masked.values()) == [1, 2, 3]
    assert keys == [
        "row.attrs.notes.md", "row.attrs.notes.md~2", "row.attrs.notes.md~3",
    ]


def test_a_tuple_is_walked_like_a_list() -> None:
    masked, keys = records.mask_paths(("/Users/example/x.md", "ok"), at="a")

    assert masked == ["x.md", "ok"]
    assert keys == ["a[0]"]


def test_the_rule_states_the_case_it_does_not_catch() -> None:
    """A path inside prose is served as stored, and the payload says so."""
    prose = "could not open /Users/example/corpus/guide.md for reading"

    masked, keys = records.mask_paths(prose, at="attrs.error")

    assert masked == prose
    assert keys == []
    assert "free text" in records.PATH_RULE_LIMIT


# --- the byte route trusts the bytes, not the row ---------------------------


@pytest.mark.parametrize(
    ("media_type", "data", "expected"),
    [
        ("image/png", b"\x89PNG\r\n\x1a\n rest", True),
        ("image/jpeg", b"\xff\xd8\xff\xe0 rest", True),
        ("image/gif", b"GIF89a rest", True),
        ("image/webp", b"RIFF\x00\x00\x00\x00WEBP rest", True),
        # The row says png; the bytes are a script-bearing document.
        ("image/png", b"<html><script>alert(1)</script>", False),
        ("image/png", b"<svg xmlns='http://www.w3.org/2000/svg'/>", False),
        ("image/jpeg", b"\x89PNG\r\n\x1a\n", False),
        ("image/webp", b"RIFFxxxxNOPE", False),
        ("image/png", b"", False),
    ],
)
def test_declared_type_is_checked_against_the_leading_bytes(
    media_type: str, data: bytes, expected: bool,
) -> None:
    assert records._bytes_match_media_type(data, media_type) is expected


def test_payload_bytes_that_are_not_json_are_not_called_json() -> None:
    assert records._as_json_text(b'{"a": 1}') == '{"a": 1}'
    assert records._as_json_text(b"\xff\xfe not utf 8") is None
    assert records._as_json_text(b"plain text, not json") is None
    assert records._as_json_text(b"") is None


# --- sort is only excused where it is known ---------------------------------


@pytest.mark.parametrize("read", NEW_READS)
@pytest.mark.parametrize("value", ["relevance", "newest"])
def test_sort_is_an_unknown_parameter_on_a_read_that_ranks_nothing(
    client: TestClient, read: str, value: str,
) -> None:
    """`- {"sort"}` used to excuse it on every route, silently.

    These reads rank nothing, so a `sort` they accepted would be a parameter
    the caller sent, the route ignored, and nobody mentioned.
    """
    response = client.get(
        NEW_ROUTES[read],
        params={"version": VERSION, "id": "node:1", "sort": value},
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["reason"] == "unknown_parameter"
    assert detail["parameters"] == ["sort"]


def test_the_search_route_still_owes_sort_a_named_cause() -> None:
    assert records is not None
    assert "sort" in record_endpoints._NAMED_CAUSE_PARAMS["search"]
    assert "since" in record_endpoints._NAMED_CAUSE_PARAMS["search"]
    for read in NEW_READS:
        assert read not in record_endpoints._NAMED_CAUSE_PARAMS
