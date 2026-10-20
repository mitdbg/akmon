"""CLI mailbox-serve authenticator wiring (enforce-mailbox-poros-authz, task 6).

The `okg federation mailbox serve` path builds an authenticator from
`--principals` / OKG_MAILBOX_PRINCIPALS and passes it to build_mailbox_app. With
no principals configured the served app fails closed (unauthenticated
read/finalize is rejected).
"""
from __future__ import annotations

import argparse
import json

import pytest

from okg.substrate.cli.federation import _load_mailbox_authenticator

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.federation.mailbox import (  # noqa: E402
    InMemoryMailboxStore,
    MailboxService,
    build_mailbox_app,
)


def test_no_principals_yields_fail_closed_authenticator():
    args = argparse.Namespace(principals=None)
    assert _load_mailbox_authenticator(args) is None


def test_principals_file_builds_working_authenticator(tmp_path):
    path = tmp_path / "principals.json"
    path.write_text(json.dumps({"okg:research-lab": "tok"}))
    auth = _load_mailbox_authenticator(argparse.Namespace(principals=str(path)))
    assert auth is not None
    # The authenticator accepts the configured principal/token and rejects others.
    req_ok = type("R", (), {"headers": {
        "x-okg-mailbox-principal": "okg:research-lab", "x-okg-mailbox-token": "tok"}})()
    assert auth(req_ok) == "okg:research-lab"


def test_env_principals_build_authenticator(monkeypatch):
    monkeypatch.setenv("OKG_MAILBOX_PRINCIPALS", json.dumps({"okg:x": "t"}))
    auth = _load_mailbox_authenticator(argparse.Namespace(principals=None))
    assert auth is not None


def test_served_app_fails_closed_without_principals():
    service = MailboxService(store=InMemoryMailboxStore())
    auth = _load_mailbox_authenticator(argparse.Namespace(principals=None))
    client = TestClient(build_mailbox_app(service, authenticator=auth))
    # Unauthenticated poll / read is rejected.
    assert client.get("/inbox/okg:research-lab").status_code == 401
    assert client.get("/messages/anything").status_code == 401


def test_served_app_authenticates_with_principals(tmp_path):
    path = tmp_path / "principals.json"
    path.write_text(json.dumps({"okg:research-lab": "tok"}))
    auth = _load_mailbox_authenticator(argparse.Namespace(principals=str(path)))
    service = MailboxService(store=InMemoryMailboxStore())
    client = TestClient(build_mailbox_app(service, authenticator=auth))
    headers = {"x-okg-mailbox-principal": "okg:research-lab", "x-okg-mailbox-token": "tok"}
    # Authenticated poll of own inbox succeeds (empty).
    resp = client.get("/inbox/okg:research-lab", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["messages"] == []
