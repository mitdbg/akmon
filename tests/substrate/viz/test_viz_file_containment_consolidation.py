"""The viz reader local-file body + profile-artifact server now route through the
SHARED resolve_within_approved_root primitive (constrain-graph-file-and-parser-
ingestion; adversarial-review finding #29 consolidation).

Both were previously per-site inline containment; these tests prove the shared
boundary holds: an in-root regular file is served, a traversal / out-of-root /
oversized / symlink-escaping path is denied.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import okg.viz.endpoints as endpoints

_reader_document_body_from_local_file = endpoints._reader_document_body_from_local_file


# ---- _reader_document_body_from_local_file (pure function, CWD-rooted) ----


def test_reader_serves_in_root_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "notes").mkdir()
    f = tmp_path / "notes" / "doc.md"
    f.write_text("hello body")
    assert _reader_document_body_from_local_file({"path": "notes/doc.md"}) == "hello body"


def test_reader_denies_traversal_and_out_of_root(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    secret = tmp_path.parent / "secret.md"
    secret.write_text("TOP SECRET")
    # A ../ traversal that escapes CWD is denied (None), never read.
    assert _reader_document_body_from_local_file({"path": "../secret.md"}) is None
    # An absolute out-of-root path is denied too.
    assert _reader_document_body_from_local_file({"path": str(secret)}) is None


def test_reader_denies_symlink_escaping_root(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    outside = tmp_path.parent / "outside.md"
    outside.write_text("escaped")
    link = tmp_path / "link.md"
    link.symlink_to(outside)
    assert _reader_document_body_from_local_file({"path": "link.md"}) is None


def test_reader_denies_oversized_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(endpoints, "_READER_MAX_LOCAL_TEXT_BYTES", 8)
    f = tmp_path / "big.md"
    f.write_text("x" * 64)
    assert _reader_document_body_from_local_file({"path": "big.md"}) is None


def test_reader_sha_mismatch_denied(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    f = tmp_path / "doc.md"
    f.write_text("body")
    assert _reader_document_body_from_local_file(
        {"path": "doc.md", "content_sha256": "0" * 64}
    ) is None


# ---- system_profile_artifact HTTP endpoint ----


pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from okg.viz.app import build_app  # noqa: E402


class _Ctx:
    deployment_name = "demo"


def _artifact_client(monkeypatch, profile_json: Path):
    import okg.substrate.profiling.artifacts as artifacts

    monkeypatch.setattr(artifacts, "find_profile", lambda pid, dep: profile_json)
    app = build_app(ctx_provider=_Ctx, is_ready=lambda: True)
    return TestClient(app, client=("127.0.0.1", 40000))


def test_artifact_in_root_served(monkeypatch, tmp_path):
    (tmp_path / "profile.json").write_text("{}")
    (tmp_path / "flame.svg").write_text("<svg/>")
    client = _artifact_client(monkeypatch, tmp_path / "profile.json")
    resp = client.get("/viz/api/system/profile/p1/artifact/flame.svg")
    assert resp.status_code == 200
    assert resp.text == "<svg/>"


def test_artifact_traversal_denied(monkeypatch, tmp_path):
    (tmp_path / "profile.json").write_text("{}")
    (tmp_path.parent / "secret.svg").write_text("SECRET")
    client = _artifact_client(monkeypatch, tmp_path / "profile.json")
    # A ../ traversal escaping the profile dir is denied (404, no bytes).
    resp = client.get("/viz/api/system/profile/p1/artifact/../secret.svg")
    assert resp.status_code == 404
    assert "SECRET" not in resp.text


def test_artifact_missing_denied(monkeypatch, tmp_path):
    (tmp_path / "profile.json").write_text("{}")
    client = _artifact_client(monkeypatch, tmp_path / "profile.json")
    resp = client.get("/viz/api/system/profile/p1/artifact/nope.svg")
    assert resp.status_code == 404
