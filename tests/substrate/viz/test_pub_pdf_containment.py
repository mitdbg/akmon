"""pub_pdf approved-root containment (constrain-graph-file-and-parser-ingestion).

The /viz/api/pubs/{id}/pdf route serves a graph-controlled `local_pdf_path`. It
must resolve that path inside an explicitly-configured approved corpus root
before returning bytes: an absolute out-of-root path, a `..` traversal, or a
symlink escaping the root is refused, and an unconfigured root fails closed.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from okg.viz import reads  # noqa: E402
from okg.viz.app import build_app  # noqa: E402


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Ctx:
    def __init__(self, *, config, deployment_dir):
        self.config = config
        self.deployment_dir = Path(deployment_dir)
        self.session = object()

    def conn(self):
        return _Conn()


def _client(monkeypatch, *, config, node_attrs, node_type="publication"):
    ctx = _Ctx(config=config, deployment_dir=Path("/nonexistent-deployment"))
    fake_node = SimpleNamespace(type=node_type, attrs=node_attrs)
    monkeypatch.setattr(reads, "fetch_node", lambda *a, **k: fake_node)
    app = build_app(ctx_provider=lambda: ctx, is_ready=lambda: True)
    return TestClient(app)


def test_in_root_pdf_is_served(tmp_path, monkeypatch):
    root = tmp_path / "corpus"
    root.mkdir()
    pdf = root / "p.pdf"
    pdf.write_bytes(b"%PDF-1.4 hello")
    client = _client(
        monkeypatch,
        config={"approved_pdf_corpus_roots": [str(root)]},
        node_attrs={"local_pdf_path": str(pdf)},
    )
    resp = client.get("/viz/api/pubs/pub1/pdf")
    assert resp.status_code == 200
    assert resp.content == b"%PDF-1.4 hello"


def test_absolute_out_of_root_is_refused(tmp_path, monkeypatch):
    root = tmp_path / "corpus"
    root.mkdir()
    secret = tmp_path / "secret.pem"
    secret.write_text("KEY")
    client = _client(
        monkeypatch,
        config={"approved_pdf_corpus_roots": [str(root)]},
        node_attrs={"local_pdf_path": str(secret)},
    )
    resp = client.get("/viz/api/pubs/pub1/pdf")
    assert resp.status_code == 404
    assert b"KEY" not in resp.content


def test_traversal_is_refused(tmp_path, monkeypatch):
    root = tmp_path / "corpus"
    root.mkdir()
    secret = tmp_path / "secret.pem"
    secret.write_text("KEY")
    client = _client(
        monkeypatch,
        config={"approved_pdf_corpus_roots": [str(root)]},
        node_attrs={"local_pdf_path": str(root / ".." / "secret.pem")},
    )
    assert client.get("/viz/api/pubs/pub1/pdf").status_code == 404


def test_symlink_escaping_root_is_refused(tmp_path, monkeypatch):
    root = tmp_path / "corpus"
    root.mkdir()
    secret = tmp_path / "secret.pem"
    secret.write_text("KEY")
    link = root / "link.pdf"
    link.symlink_to(secret)
    client = _client(
        monkeypatch,
        config={"approved_pdf_corpus_roots": [str(root)]},
        node_attrs={"local_pdf_path": str(link)},
    )
    resp = client.get("/viz/api/pubs/pub1/pdf")
    assert resp.status_code == 404
    assert b"KEY" not in resp.content


def test_unconfigured_roots_fails_closed(tmp_path, monkeypatch):
    pdf = tmp_path / "p.pdf"
    pdf.write_bytes(b"%PDF")
    client = _client(
        monkeypatch,
        config={},  # no approved_pdf_corpus_roots
        node_attrs={"local_pdf_path": str(pdf)},
    )
    assert client.get("/viz/api/pubs/pub1/pdf").status_code == 500
