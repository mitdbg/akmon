"""Source manifest phase 2: JSON APIs (PACT no-code-source-manifest-http).

Every test talks to a local server on 127.0.0.1 (`_manifest_http_server`);
nothing reaches the network. The reader is driven through its
`SourceAdapter.run` contract with the cursor each run returns handed to the
next, as the ingest runner does.
"""
from __future__ import annotations

import base64
import json
import textwrap
from pathlib import Path
from typing import Any, Iterator

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.change_probe import CURSOR_TOKEN_KEY
from okg.substrate.library.sources.manifest_records import ManifestRecordSource
from okg.substrate.sources import manifest_http
from okg.substrate.sources.manifest import ManifestError, map_records, parse_manifest
from okg.substrate.sources.manifest_http import HttpReadFailed, fetch_read, read_inputs
from tests.substrate.sources._manifest_http_server import (
    LocalServer,
    Request,
    json_response,
    make_tls_material,
)


@pytest.fixture
def servers() -> Iterator[list[LocalServer]]:
    started: list[LocalServer] = []
    yield started
    for server in started:
        server.close()


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Retries never really wait; the waits asked for are recorded."""
    waits: list[float] = []
    monkeypatch.setattr(manifest_http.time, "sleep", waits.append)
    return waits


def serve(servers: list[LocalServer], routes: dict, **kwargs: Any) -> LocalServer:
    server = LocalServer(routes, **kwargs)
    servers.append(server)
    return server


def manifest_of(text: str, tmp_path: Path):
    return parse_manifest(textwrap.dedent(text), base_dir=tmp_path)


def items(n: int, start: int = 1) -> list[dict]:
    return [{"id": f"i{k}", "name": f"Item {k}", "updated": "2026-01-01"} for k in range(start, start + n)]


class Reader:
    """One manifest-backed source; `run` carries the cursor forward."""

    def __init__(self, tmp_path: Path, text: str, *, env: dict | None = None) -> None:
        self.path = tmp_path / "api.source.yaml"
        self.path.write_text(textwrap.dedent(text))
        self.env = env or {}
        self.cursor: dict | None = None

    def rewrite(self, text: str) -> None:
        self.path.write_text(textwrap.dedent(text))

    def adapter(self) -> ManifestRecordSource:
        return ManifestRecordSource(str(self.path), "api", env=self.env)

    def run(self):
        adapter = self.adapter()
        result = adapter.run("run", mode="scope_complete", cursor=self.cursor)
        self.metrics = adapter.last_run_metrics
        self.cursor = {**result.next_cursor, CURSOR_TOKEN_KEY: result.applied_raw_token}
        return result


def one_read(url: str, extra: str = "", *, records: str = "items", type_extra: str = "") -> str:
    """A one-type manifest with an HTTP read; `extra` goes under read:,
    `type_extra` under the type."""
    lines = [
        "manifest: okg.source/v1", "types:", "  api_item:", "    read:",
        f"      url: {url}", f"      records: {records}",
    ]
    lines += ["      " + line for line in textwrap.dedent(extra).splitlines() if line.strip()]
    lines += ['    key: "{id}"', "    title: name"]
    lines += ["    " + line for line in textwrap.dedent(type_extra).splitlines() if line.strip()]
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------
# Several reads, links across them, "*" records, parents
# --------------------------------------------------------------------------


def test_multi_read_links_resolve_across_reads(tmp_path: Path, servers) -> None:
    sites = {"T1_CH_CERN": {"name": "CERN", "tier": 1}, "T2_US_MIT": {"name": "MIT", "tier": 2}}
    units = {
        "su-a": {"name": "disk A", "site": "T2_US_MIT"},
        "su-b": {"name": "disk B", "site": "T9_NOWHERE"},
    }
    server = serve(servers, {
        "/sites": lambda r: json_response(sites),
        "/units": lambda r: json_response(units),
    })
    text = f"""
    manifest: okg.source/v1
    connections:
      cat:
        base_url: {server.base}/
    types:
      cat_site:
        read: {{ connection: cat, path: sites, records: "*" }}
        key: "{{_key}}"
        title: name
        fields:
          tier: {{ from: tier, type: integer }}
      cat_unit:
        read: {{ connection: cat, path: units, records: "*" }}
        key: "{{_key}}"
        title: name
        links:
          located_at: {{ to: cat_site, key: "{{site}}" }}
    deletion: missing_from_full_read
    """
    reader = Reader(tmp_path, text)
    result = reader.run()
    emissions = result.next_cursor["emissions"]
    assert set(emissions) == {"cat_site:T1_CH_CERN", "cat_site:T2_US_MIT", "cat_unit:su-a", "cat_unit:su-b"}
    unit_a = emissions["cat_unit:su-a"]
    assert [(e["edge_type"], e["dst"]) for e in unit_a["edges"]] == [("located_at", "api:cat_site:T2_US_MIT")]
    assert emissions["cat_unit:su-b"]["edges"] == []  # target not in the read: dropped
    assert reader.metrics["links.dropped"] == 1.0
    assert reader.metrics["reads.http"] == 2.0


def test_object_records_expose_their_name_as_key(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/o": lambda r: json_response({"a": {"name": "A"}, "b": {"name": "B"}})})
    manifest = manifest_of(one_read(f"{server.base}/o", records='"*"').replace('"{id}"', '"{_key}"'), tmp_path)
    inputs = read_inputs(manifest, env={})
    assert inputs.records[0][1] == [{"name": "A", "_key": "a"}, {"name": "B", "_key": "b"}]
    mapped = map_records(manifest, source="s", inputs=inputs.records, input_types=inputs.record_types)
    assert sorted(mapped.records) == ["api_item:a", "api_item:b"]


def test_object_records_refuse_a_list(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/o": lambda r: json_response([{"id": 1}])})
    manifest = manifest_of(one_read(f"{server.base}/o", records='"*"'), tmp_path)
    inputs = read_inputs(manifest, env={})
    assert inputs.failed and "is not a JSON object" in inputs.failed[0].message


def test_parent_is_required_with_several_top_level_types(tmp_path: Path) -> None:
    text = """
    manifest: okg.source/v1
    types:
      api_work:
        read: { url: "https://example.org/works", records: results }
        key: "{id}"
      api_venue:
        read: { url: "https://example.org/venues", records: results }
        key: "{id}"
      api_author:
        from: authorships
        key: "{author.id}"
    """
    with pytest.raises(ManifestError) as caught:
        manifest_of(text, tmp_path)
    assert caught.value.code == "parent_ambiguous"
    fixed = manifest_of(text.replace("from: authorships", "from: authorships\n        parent: api_work"), tmp_path)
    assert [t.name for t in fixed.nested_types_of("api_work")] == ["api_author"]
    assert fixed.nested_types_of("api_venue") == []


def test_parent_nested_records_come_only_from_that_parent(tmp_path: Path, servers) -> None:
    server = serve(servers, {
        "/works": lambda r: json_response({"results": [{"id": "w1", "authorships": [{"author": {"id": "a1"}}]}]}),
        "/venues": lambda r: json_response({"results": [{"id": "v1", "authorships": [{"author": {"id": "zz"}}]}]}),
    })
    text = f"""
    manifest: okg.source/v1
    types:
      api_work:
        read: {{ url: "{server.base}/works", records: results }}
        key: "{{id}}"
        links:
          authored_by: {{ to: api_author, each: authorships, key: "{{author.id}}" }}
      api_venue:
        read: {{ url: "{server.base}/venues", records: results }}
        key: "{{id}}"
      api_author:
        from: authorships
        parent: api_work
        key: "{{author.id}}"
    """
    result = Reader(tmp_path, text).run()
    assert sorted(result.next_cursor["emissions"]) == ["api_author:a1", "api_venue:v1", "api_work:w1"]


# --------------------------------------------------------------------------
# Auth, secrets, redaction
# --------------------------------------------------------------------------


def _guarded(check) -> Any:
    def route(request: Request):
        if not check(request.headers):
            return json_response({"error": "unauthorized"}, status=401)
        return json_response({"items": items(2)})
    return route


@pytest.mark.parametrize(
    ("auth", "env", "check"),
    [
        ("{ bearer: { token: API_TOKEN } }", {"API_TOKEN": "s3cr3t-bearer"},
         lambda h: h.get("Authorization") == "Bearer s3cr3t-bearer"),
        ("{ header: { name: X-Api-Key, value: API_KEY } }", {"API_KEY": "s3cr3t-key"},
         lambda h: h.get("X-Api-Key") == "s3cr3t-key"),
        ("{ basic: { user: API_USER, password: API_PASSWORD } }", {"API_USER": "ann", "API_PASSWORD": "pw"},
         lambda h: h.get("Authorization") == "Basic " + base64.b64encode(b"ann:pw").decode()),
        ("none", {}, lambda h: "Authorization" not in h),
    ],
)
def test_auth_modes_send_the_named_variable(tmp_path: Path, servers, auth: str, env: dict, check) -> None:
    server = serve(servers, {"/items": _guarded(check)})
    manifest = manifest_of(one_read(f"{server.base}/items", f"auth: {auth}"), tmp_path)
    inputs = read_inputs(manifest, env=env)
    assert not inputs.failed, inputs.failed
    assert len(inputs.records[0][1]) == 2


def test_auth_from_a_connection_and_a_missing_variable_fails_the_read(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": _guarded(lambda h: h.get("Authorization") == "Bearer t")})
    text = f"""
    manifest: okg.source/v1
    connections:
      api:
        base_url: {server.base}/
        auth: {{ bearer: {{ token: API_TOKEN }} }}
    types:
      api_item:
        read: {{ connection: api, path: items, records: items }}
        key: "{{id}}"
    """
    manifest = manifest_of(text, tmp_path)
    assert not read_inputs(manifest, env={"API_TOKEN": "t"}).failed
    failed = read_inputs(manifest, env={}).failed
    assert failed and "$API_TOKEN" in failed[0].message and "not set" in failed[0].message
    assert server.requests[-1:] and len(server.requests) == 1  # nothing sent without the token


def test_client_certificate_auth_against_a_server_that_requires_one(tmp_path: Path, servers) -> None:
    tls = make_tls_material(tmp_path / "tls")
    server = serve(servers, {"/items": lambda r: json_response({"items": items(3)})}, tls=tls.server_context)
    auth = "{ client_certificate: { cert: GRID_CERT, key: GRID_KEY, ca_dir: GRID_CA_DIR } }"
    manifest = manifest_of(one_read(f"{server.base}/items", f"auth: {auth}"), tmp_path)
    env = {"GRID_CERT": str(tls.client_cert), "GRID_KEY": str(tls.client_key), "GRID_CA_DIR": str(tls.ca_dir)}
    inputs = read_inputs(manifest, env=env)
    assert not inputs.failed, inputs.failed
    assert len(inputs.records[0][1]) == 3
    # The same server refuses a client that trusts it but has no certificate.
    import ssl
    import urllib.request

    trusting = ssl.create_default_context(cafile=str(tls.ca_dir / "okg-test-ca.pem"))
    with pytest.raises(Exception):
        urllib.request.urlopen(f"{server.base}/items", context=trusting, timeout=10).read()


@pytest.mark.parametrize(
    ("snippet", "where"),
    [
        ("auth: { bearer: { token: abc123secretvalue } }", "types.api_item.read.auth.bearer.token"),
        ("auth: { basic: { user: ann, password: hunter2 } }", "types.api_item.read.auth.basic.user"),
        ("auth: { client_certificate: { cert: /etc/x509/proxy, key: X509_KEY } }",
         "types.api_item.read.auth.client_certificate.cert"),
    ],
)
def test_literal_secret_in_auth_is_refused(tmp_path: Path, snippet: str, where: str) -> None:
    with pytest.raises(ManifestError) as caught:
        manifest_of(one_read("https://example.org/items", snippet), tmp_path)
    assert caught.value.code == "auth_literal_secret"
    assert caught.value.path == where


@pytest.mark.parametrize(
    "url",
    [
        "https://example.org/items?api_key=abc",
        "https://example.org/items?filter=x&access_token=abc",
        "https://ann:pw@example.org/items",
    ],
)
def test_url_that_carries_a_secret_is_refused(tmp_path: Path, url: str) -> None:
    with pytest.raises(ManifestError) as caught:
        manifest_of(one_read(url), tmp_path)
    assert caught.value.code == "auth_literal_secret"


def test_failure_messages_are_redacted(tmp_path: Path, servers) -> None:
    def echo(request: Request):
        return 403, {"Content-Type": "text/plain"}, f"bad token {request.headers.get('Authorization')}".encode()
    server = serve(servers, {"/items": echo})
    manifest = manifest_of(one_read(f"{server.base}/items", "auth: { bearer: { token: API_TOKEN } }"), tmp_path)
    failed = read_inputs(manifest, env={"API_TOKEN": "tok-ABCDEF-123"}).failed
    assert failed and "HTTP 403" in failed[0].message
    assert "tok-ABCDEF-123" not in failed[0].message and "***" in failed[0].message
    read = manifest.reads["api_item"]
    assert "tok-ABCDEF-123" not in read.describe() and "$API_TOKEN" in read.describe()


def test_redirect_to_another_host_drops_credentials(tmp_path: Path, servers) -> None:
    target = serve(servers, {"/items": lambda r: json_response({"items": items(1)})})
    origin = serve(servers, {"/items": lambda r: (302, {"Location": f"{target.base}/items"}, b"")})
    manifest = manifest_of(one_read(f"{origin.base}/items", "auth: { bearer: { token: API_TOKEN } }"), tmp_path)
    inputs = read_inputs(manifest, env={"API_TOKEN": "tok"})
    assert not inputs.failed
    assert origin.requests[0].headers.get("Authorization") == "Bearer tok"
    assert "Authorization" not in target.requests[0].headers


# --------------------------------------------------------------------------
# Paging and failures
# --------------------------------------------------------------------------


ALL = items(7)


def _offset(request: Request):
    start, size = int(request.query["skip"]), int(request.query["take"])
    return json_response({"items": ALL[start:start + size]})


def _page(request: Request):
    page, size = int(request.query["p"]), int(request.query["n"])
    return json_response({"items": ALL[page * size:(page + 1) * size]})


def _cursor(request: Request):
    start = int(request.query.get("cursor", "0").replace("*", "0"))
    nxt = start + 3 if start + 3 < len(ALL) else None
    return json_response({"items": ALL[start:start + 3], "meta": {"next_cursor": None if nxt is None else str(nxt)}})


def _link(request: Request):
    start = int(request.query.get("from", "0"))
    headers = {}
    if start + 3 < len(ALL):
        headers["Link"] = f'</items?from={start + 3}>; rel="next", </items?from=0>; rel="first"'
    return json_response({"items": ALL[start:start + 3]}, headers=headers)


@pytest.mark.parametrize(
    ("route", "paging"),
    [
        (_offset, "paging: { offset: { offset_param: skip, limit_param: take, size: 3 } }"),
        (_page, "paging: { page: { page_param: p, size_param: n, size: 3, first: 0 } }"),
        (_cursor, "paging: { cursor: { param: cursor, next: meta.next_cursor } }"),
        (_link, "paging: link_header"),
    ],
)
def test_paging_reads_every_page(tmp_path: Path, servers, route, paging: str) -> None:
    server = serve(servers, {"/items": route})
    url = f"{server.base}/items" + ("?cursor=*" if "cursor" in paging else "")
    manifest = manifest_of(one_read(url, paging), tmp_path)
    inputs = read_inputs(manifest, env={})
    assert not inputs.failed, inputs.failed
    got = [row["id"] for _label, rows in inputs.records for row in rows]
    assert got == [row["id"] for row in ALL]


def test_max_pages_makes_the_read_incomplete(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": _offset})
    manifest = manifest_of(one_read(
        f"{server.base}/items",
        "paging: { offset: { offset_param: skip, limit_param: take, size: 3 } }\nmax_pages: 2",
    ), tmp_path)
    failed = read_inputs(manifest, env={}).failed
    assert failed and "reached max_pages (2)" in failed[0].message
    assert len(server.requests) == 2


def test_retry_honors_retry_after_then_succeeds(tmp_path: Path, servers, no_sleep: list[float]) -> None:
    calls = {"n": 0}

    def flaky(request: Request):
        calls["n"] += 1
        if calls["n"] == 1:
            return 429, {"Retry-After": "7"}, b"slow down"
        if calls["n"] == 2:
            return 503, {}, b"busy"
        return json_response({"items": items(2)})

    server = serve(servers, {"/items": flaky})
    fetched = fetch_read(manifest_of(one_read(f"{server.base}/items"), tmp_path).reads["api_item"], env={})
    assert fetched.retries == 2 and len(fetched.pages[0][1]) == 2
    assert no_sleep == [7.0, 2.0]


def test_retries_give_up_after_three(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": lambda r: (500, {}, b"down")})
    with pytest.raises(HttpReadFailed) as caught:
        fetch_read(manifest_of(one_read(f"{server.base}/items"), tmp_path).reads["api_item"], env={})
    assert "HTTP 500 after 4 attempts" in str(caught.value)
    assert len(server.requests) == 4


def test_client_error_is_not_retried(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": lambda r: (404, {}, b"gone")})
    with pytest.raises(HttpReadFailed):
        fetch_read(manifest_of(one_read(f"{server.base}/items"), tmp_path).reads["api_item"], env={})
    assert len(server.requests) == 1


def test_non_json_response_is_an_incomplete_read(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": lambda r: (200, {"Content-Type": "text/html"}, b"<html>login</html>")})
    failed = read_inputs(manifest_of(one_read(f"{server.base}/items"), tmp_path), env={}).failed
    assert failed and "not JSON" in failed[0].message and "text/html" in failed[0].message


def test_records_path_missing_is_an_incomplete_read(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": lambda r: json_response({"data": []})})
    failed = read_inputs(manifest_of(one_read(f"{server.base}/items"), tmp_path), env={}).failed
    assert failed and "no list at records 'items'" in failed[0].message


def test_failed_read_applies_the_others_and_removes_nothing(tmp_path: Path, servers) -> None:
    state = {"broken": False}

    def b_route(request: Request):
        if state["broken"]:
            return 502, {}, b"bad gateway"
        return json_response({"items": [{"id": "b1", "name": "B one"}]})

    server = serve(servers, {
        "/a": lambda r: json_response({"items": [{"id": "a1", "name": "A one"}, {"id": "a2", "name": "A two"}]}),
        "/b": b_route,
    })
    text = f"""
    manifest: okg.source/v1
    types:
      api_a:
        read: {{ url: "{server.base}/a", records: items }}
        key: "{{id}}"
        title: name
      api_b:
        read: {{ url: "{server.base}/b", records: items }}
        key: "{{id}}"
        title: name
    """
    reader = Reader(tmp_path, text)
    reader.run()
    state["broken"] = True
    result = reader.run()
    assert result.completed_scope
    # b1 is kept: the failed read withholds every deletion.
    assert set(result.next_cursor["emissions"]) == {"api_a:a1", "api_a:a2", "api_b:b1"}
    assert any("incomplete read types.api_b.read" in p and "HTTP 502" in p for p in result.next_cursor["problems"])
    assert reader.metrics["reads.failed"] == 1.0


# --------------------------------------------------------------------------
# The mass-deletion guard
# --------------------------------------------------------------------------


def _guard_reader(tmp_path: Path, servers, fraction: str = "") -> tuple[Reader, dict]:
    state = {"rows": items(10)}
    server = serve(servers, {"/items": lambda r: json_response({"items": state["rows"]})})
    reader = Reader(tmp_path, one_read(f"{server.base}/items") + fraction)
    reader.server = server
    return reader, state


def test_guard_trips_applies_records_and_removes_nothing(tmp_path: Path, servers) -> None:
    reader, state = _guard_reader(tmp_path, servers)
    reader.run()
    state["rows"] = items(3)
    state["rows"][0]["name"] = "Renamed"
    result = reader.run()
    assert len(result.next_cursor["emissions"]) == 10  # nothing removed
    assert result.facts  # the changed record still applied
    message = result.next_cursor["deletion_guard"]
    assert "would remove 7 of 10 earlier records" in message
    assert "max_deleted_fraction 0.5" in message and "--allow-full-retraction api" in message
    assert reader.metrics["deletion.guard_tripped"] == 1.0
    assert reader.metrics["deletion.guard_withheld"] == 7.0


def test_guard_allows_a_removal_under_the_fraction_and_a_raised_fraction(tmp_path: Path, servers) -> None:
    reader, state = _guard_reader(tmp_path, servers)
    reader.run()
    state["rows"] = items(6)  # 4 of 10 go: under 0.5
    assert len(reader.run().next_cursor["emissions"]) == 6
    state["rows"] = items(1)  # 5 of 6 go: over 0.5
    assert len(reader.run().next_cursor["emissions"]) == 6
    reader.rewrite(one_read(f"{reader.server.base}/items") + "\nmax_deleted_fraction: 0.9\n")
    result = reader.run()
    assert set(result.next_cursor["emissions"]) == {"api_item:i1"}
    assert "deletion_guard" not in result.next_cursor


def test_empty_read_is_left_to_the_runner_full_retraction_guard(tmp_path: Path, servers) -> None:
    reader, state = _guard_reader(tmp_path, servers)
    reader.run()
    state["rows"] = []
    result = reader.run()
    assert result.next_cursor["emissions"] == {}
    assert "deletion_guard" not in result.next_cursor


def test_file_only_sources_have_no_default_guard(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "a.json").write_text(json.dumps(items(10)))
    text = """
    manifest: okg.source/v1
    read: { files: data/*.json }
    types:
      api_item:
        key: "{id}"
    """
    reader = Reader(tmp_path, text)
    assert reader.adapter() and manifest_of(text, tmp_path).max_deleted_fraction is None
    reader.run()
    (data / "a.json").write_text(json.dumps(items(1)))
    assert len(reader.run().next_cursor["emissions"]) == 1


# --------------------------------------------------------------------------
# Change detection
# --------------------------------------------------------------------------


def test_conditional_get_skip_uses_the_exact_token(tmp_path: Path, servers) -> None:
    state = {"etag": '"v1"', "rows": items(4)}

    def route(request: Request):
        if request.headers.get("If-None-Match") == state["etag"]:
            return 304, {"ETag": state["etag"]}, b""
        return json_response({"items": state["rows"]}, headers={"ETag": state["etag"]})

    server = serve(servers, {"/items": route})
    reader = Reader(tmp_path, one_read(f"{server.base}/items"))
    first = reader.run()
    assert reader.cursor["http_pages"]["types.api_item.read"][0]["etag"] == '"v1"'
    verdict = reader.adapter().change_probe.probe(cursor=reader.cursor, sync_scope=None)
    assert not verdict.changed, verdict.reason
    assert verdict.token == first.applied_raw_token
    assert server.requests[-1].headers.get("If-None-Match") == '"v1"'
    state.update(etag='"v2"', rows=items(5))
    assert reader.adapter().change_probe.probe(cursor=reader.cursor, sync_scope=None).changed


def test_without_validators_every_run_fetches_and_writes_nothing_unchanged(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": lambda r: json_response({"items": items(4)})})
    reader = Reader(tmp_path, one_read(f"{server.base}/items"))
    reader.run()
    assert reader.adapter().change_probe.probe(cursor=reader.cursor, sync_scope=None).changed
    again = reader.run()
    assert again.facts == [] and reader.metrics["records.changed"] == 0.0


def test_changes_when_revision_follows_the_marker(tmp_path: Path, servers) -> None:
    state = {"rows": items(2)}
    server = serve(servers, {"/items": lambda r: json_response({"items": state["rows"]})})
    reader = Reader(tmp_path, one_read(f"{server.base}/items", type_extra="changes_when: updated"))
    reader.run()
    state["rows"] = items(2)
    state["rows"][0]["name"] = "Renamed but not marked"
    assert reader.run().facts == []  # the marker did not change
    state["rows"][0]["updated"] = "2026-02-01"
    result = reader.run()
    assert reader.metrics["records.changed"] == 1.0
    assert any(getattr(f, "attrs", {}).get("title") == "Renamed but not marked" for f in result.facts)


def test_changes_when_missing_falls_back_to_content_and_is_reported(tmp_path: Path, servers) -> None:
    rows = items(2)
    del rows[1]["updated"]
    server = serve(servers, {"/items": lambda r: json_response({"items": rows})})
    reader = Reader(tmp_path, one_read(f"{server.base}/items", type_extra="changes_when: updated"))
    reader.run()
    assert reader.metrics["changes_when.missing"] == 1.0


def test_reset_footprint_unknown_refuses_under_never(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": lambda r: json_response({"items": items(2)})})
    reader = Reader(tmp_path, one_read(f"{server.base}/items") + "\ndeletion: never\n")
    adapter = reader.adapter()
    from okg.substrate.library.sources.base import RecordSet

    adapter.prepare_authoritative_replay(record_set=RecordSet(), prior_footprint_known=False)
    with pytest.raises(SubstrateError) as caught:
        adapter.run("run", mode="scope_complete", cursor=None)
    assert caught.value.data["reason_code"] == "prior_footprint_unknown"
    assert "--reset-cursor" in str(caught.value)


def test_reset_footprint_unknown_refuses_a_clean_read_under_the_deletion_guard(
    tmp_path: Path, servers,
) -> None:
    """Review M2: after --reset-cursor the 0.5 guard has no earlier record set
    to compare with, so a clean read is refused instead of letting the
    runner retract whatever it missed."""
    server = serve(servers, {"/items": lambda r: json_response({"items": items(2)})})
    reader = Reader(tmp_path, one_read(f"{server.base}/items"))
    adapter = reader.adapter()
    from okg.substrate.library.sources.base import RecordSet

    adapter.prepare_authoritative_replay(record_set=RecordSet(), prior_footprint_known=False)
    with pytest.raises(SubstrateError) as caught:
        adapter.run("run", mode="scope_complete", cursor=None)
    assert caught.value.data["reason_code"] == "prior_footprint_unknown"
    assert "max_deleted_fraction" in str(caught.value)


def test_replay_state_holds_for_one_run_only(tmp_path: Path, servers) -> None:
    """Review m4: a reused adapter must not carry an old replay record set."""
    server = serve(servers, {"/items": lambda r: json_response({"items": items(2)})})
    reader = Reader(tmp_path, one_read(f"{server.base}/items"))
    adapter = reader.adapter()
    from okg.substrate.library.sources.base import RecordSet

    adapter.prepare_authoritative_replay(record_set=RecordSet(), prior_footprint_known=False)
    with pytest.raises(SubstrateError):
        adapter.run("run", mode="scope_complete", cursor=None)
    assert adapter.run("run2", mode="scope_complete", cursor=None).completed_scope


def test_a_redirect_from_https_to_http_drops_credentials() -> None:
    """Review m3: same host, but the credentials would travel in clear."""
    import urllib.request

    from okg.substrate.sources.manifest_http import _SameHostRedirect

    handler = _SameHostRedirect(["Authorization"])
    request = urllib.request.Request("https://api.example.org/items", headers={"Authorization": "Bearer tok"})
    same = handler.redirect_request(request, None, 302, "Found", {}, "https://api.example.org/next")
    assert same.get_header("Authorization") == "Bearer tok"
    downgraded = handler.redirect_request(request, None, 302, "Found", {}, "http://api.example.org/next")
    assert downgraded.get_header("Authorization") is None


def test_a_space_in_the_url_is_encoded_not_a_crash(tmp_path: Path, servers) -> None:
    server = serve(servers, {"/items": lambda r: json_response({"items": items(1)})})
    manifest = manifest_of(one_read(f'"{server.base}/items?q=knowledge graph"'), tmp_path)
    assert manifest.reads["api_item"].url.endswith("q=knowledge%20graph")
    inputs = read_inputs(manifest, env={})
    assert not inputs.failed and server.requests[0].query == {"q": "knowledge graph"}


def test_add_source_output_shows_the_withheld_deletion_note_and_removals() -> None:
    from okg.substrate.cli.source_add import _render_text

    payload = {
        "schema": "okg.deployment.add_source.outcome/v1", "name": "api", "deployment": "d",
        "status": "published", "generation_id": "g1",
        "counts": {"records_added": 0, "relationships_added": 0, "records_removed": 15,
                   "relationships_removed": 17},
        "steps": [{"step": "import", "status": "ok",
                   "detail": {"note": "this read would remove 15 of 27 earlier records"}}],
    }
    text = _render_text(payload)
    assert "Records removed: 15, relationships removed: 17" in text
    assert "Note: this read would remove 15 of 27 earlier records" in text
