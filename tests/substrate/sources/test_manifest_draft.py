"""`okg source draft` (PACT no-code-source-manifest-http, task draft).

The draft must be a manifest the compiler reads, with the guessed key and
deletion lines marked. URL drafts talk to a local server; no network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.sources.manifest import parse_manifest
from okg.substrate.sources.manifest_draft import SAMPLE_LIMIT, DraftError, draft, parse_auth_option
from tests.substrate.sources._manifest_http_server import LocalServer, json_response


def _write(path: Path, payload) -> str:
    path.write_text(json.dumps(payload))
    return str(path)


def _parsed(text: str, tmp_path: Path):
    return parse_manifest(text, base_dir=tmp_path), yaml.safe_load(text)


def test_clear_key_title_text_dates_and_types(tmp_path: Path) -> None:
    rows = [
        {"id": f"T-{n}", "title": f"Ticket {n}", "votes": n, "ok": n % 2 == 0,
         "description": "A long description of the problem that the ticket reports " * 2,
         "created": f"2026-01-{n:02d}", "updated": f"2026-02-{n:02d}T10:00:00Z", "tags": ["a", "b"]}
        for n in range(1, 6)
    ]
    text = draft([_write(tmp_path / "tickets.json", {"issues": rows})])
    manifest, raw = _parsed(text, tmp_path)
    (type_name, body), = raw["types"].items()
    assert body["read"]["records"] == "issues"
    assert body["key"] == "{id}" and body["title"] == "title" and body["text"] == "description"
    assert body["dates"] == {"created": "created", "modified": "updated"}
    assert body["fields"]["votes"] == {"from": "votes", "type": "integer"}
    assert body["fields"]["ok"] == {"from": "ok", "type": "boolean"}
    assert body["fields"]["tags"] == {"from": "tags", "type": "list"}
    assert "key: \"{id}\"   # GUESSED" in text and "deletion: missing_from_full_read   # GUESSED" in text
    assert manifest.max_deleted_fraction is None


def test_ambiguous_key_is_marked_with_alternatives(tmp_path: Path) -> None:
    rows = [{"code": f"C{n}", "slug": f"s-{n}", "label": "same"} for n in range(4)]
    text = draft([_write(tmp_path / "items.json", rows)])
    _manifest, raw = _parsed(text, tmp_path)
    body = next(iter(raw["types"].values()))
    assert body["key"] == "{code}"
    assert "AMBIGUOUS" in text and "other candidates: slug" in text


def test_no_key_candidate_writes_a_placeholder_to_choose(tmp_path: Path) -> None:
    rows = [{"label": "same"}, {"label": "same"}]
    text = draft([_write(tmp_path / "items.json", rows)])
    assert '"{CHOOSE_A_FIELD}"' in text and "no field is present in every sample and unique" in text


def test_nested_lists_and_objects_become_nested_types_with_links(tmp_path: Path) -> None:
    rows = [
        {"id": f"w{n}", "title": f"Work {n}",
         "authorships": [{"author": {"id": f"a{n}", "display_name": f"Ann {n}"}, "position": "first"}],
         "venue": {"id": "v1", "name": "Journal"}, "meta": {"pages": "1-2"}}
        for n in range(3)
    ]
    text = draft([_write(tmp_path / "works.json", {"results": rows})], type_name="lib_work")
    _manifest, raw = _parsed(text, tmp_path)
    types = raw["types"]
    work = types["lib_work"]
    assert work["links"]["authorships"] == {"to": "lib_authorship", "each": "authorships", "key": "{author.id}"}
    assert work["links"]["venue"] == {"to": "lib_venue", "key": "{venue.id}"}
    assert types["lib_authorship"] == {"from": "authorships", "key": "{author.id}", "title": "author.display_name"}
    assert types["lib_venue"]["from"] == "venue"
    assert "object 'meta' has no key" in text


def test_object_of_objects_uses_star_records_and_key(tmp_path: Path) -> None:
    payload = {"T1_CH_CERN": {"tier": 1}, "T2_US_MIT": {"tier": 2}}
    text = draft([_write(tmp_path / "sites.json", payload)])
    _manifest, raw = _parsed(text, tmp_path)
    body = next(iter(raw["types"].values()))
    assert body["read"]["records"] == "*" and body["key"] == "{_key}"


def test_samples_are_capped(tmp_path: Path) -> None:
    rows = [{"id": n} for n in range(SAMPLE_LIMIT + 50)]
    text = draft([_write(tmp_path / "many.json", rows)])
    assert f"({SAMPLE_LIMIT} sample records)" in text


def test_url_draft_with_cursor_paging_and_auth(tmp_path: Path) -> None:
    rows = [{"id": f"https://x.org/W{n}", "display_name": f"W {n}", "publication_date": "2024-05-01"} for n in range(3)]
    server = LocalServer({"/works": lambda r: (
        json_response({"meta": {"next_cursor": "abc"}, "results": rows})
        if r.headers.get("Authorization") == "Bearer t" else json_response({}, status=401)
    )})
    try:
        text = draft(
            [f"{server.base}/v1/works?filter=x:1&per-page=3&cursor=*".replace("/v1/works", "/works")],
            auth="bearer:API_TOKEN", env={"API_TOKEN": "t"}, type_name="api_work",
        )
    finally:
        server.close()
    _manifest, raw = _parsed(text, tmp_path)
    (name, body), = raw["types"].items()
    assert name == "api_work"
    read = body["read"]
    assert read["paging"] == {"cursor": {"param": "cursor", "next": "meta.next_cursor"}}
    assert read["auth"] == {"bearer": {"token": "API_TOKEN"}}
    assert read["records"] == "results"
    assert body["dates"] == {"published": "publication_date"}
    assert '"t"' not in text  # the variable is named, its value never written


def test_yaml_samples_draft_like_json(tmp_path: Path) -> None:
    path = tmp_path / "tickets.yaml"
    path.write_text("issues:\n  - {id: T-1, title: One, opened: 2026-01-02}\n  - {id: T-2, title: Two, opened: 2026-01-03}\n")
    text = draft([str(path)])
    _manifest, raw = _parsed(text, tmp_path)
    (_name, body), = raw["types"].items()
    assert body["read"]["records"] == "issues"
    assert body["read"]["files"].endswith(".yaml")


def test_auth_option_refuses_a_value(tmp_path: Path) -> None:
    with pytest.raises(DraftError):
        parse_auth_option("bearer:abc123")
    assert parse_auth_option("header:X-Api-Key:API_KEY").header_name == "X-Api-Key"


def test_unreachable_url_is_a_clear_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.substrate.sources import manifest_http

    monkeypatch.setattr(manifest_http.time, "sleep", lambda s: None)
    server = LocalServer({"/x": lambda r: (500, {}, b"down")})
    try:
        with pytest.raises(DraftError) as caught:
            draft([f"{server.base}/x"], env={})
    finally:
        server.close()
    assert "HTTP 500" in str(caught.value)


def test_rate_limited_url_names_the_wait_and_the_reason(tmp_path: Path) -> None:
    server = LocalServer({"/x": lambda r: (
        429, {"Retry-After": "44000"}, b"daily budget used up; add an API key",
    )})
    try:
        with pytest.raises(DraftError) as caught:
            draft([f"{server.base}/x"], env={})
    finally:
        server.close()
    assert "HTTP 429" in str(caught.value)
    assert "daily budget used up" in str(caught.value)
    assert "rate-limited" in caught.value.fix
    assert "--auth" in caught.value.fix


def test_cli_source_draft_prints_the_manifest(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    from okg.substrate.cli.main import main

    path = _write(tmp_path / "tickets.json", [{"id": "T-1", "title": "One"}, {"id": "T-2", "title": "Two"}])
    assert main(["source", "draft", path]) == 0
    text = capsys.readouterr().out
    assert text.startswith("# Drafted by `okg source draft`")
    parse_manifest(text, base_dir=tmp_path)


def test_cli_source_draft_reports_an_unreadable_sample(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    from okg.substrate.cli.main import main

    bad = tmp_path / "notes.txt"
    bad.write_text("hello")
    assert main(["source", "draft", str(bad)]) != 0
    captured = capsys.readouterr()
    assert "cannot tell the format" in captured.out + captured.err
