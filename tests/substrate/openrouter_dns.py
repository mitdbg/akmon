"""Resolve the OpenRouter host to a public address without network access.

Every OpenRouter request passes the central egress guard, which resolves the
host and refuses private addresses. Tests that drive the backend over
``httpx.MockTransport`` use this fixture so the REAL guard runs on a host that
resolves to a public address, with no DNS lookup on the test machine.
"""
from __future__ import annotations

import socket

import pytest

_REAL_GETADDRINFO = socket.getaddrinfo
#: Hosts these tests treat as public, each with a publicly routable address.
PUBLIC_TEST_HOSTS = {"openrouter.ai": "104.18.2.115", "api.other.example": "104.18.3.115"}


def _getaddrinfo(host, *args, **kwargs):
    address = PUBLIC_TEST_HOSTS.get(str(host).lower())
    if address is not None:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))]
    return _REAL_GETADDRINFO(host, *args, **kwargs)


@pytest.fixture
def openrouter_dns(monkeypatch):
    from okg.substrate.library.sources import _url_safety

    monkeypatch.setattr(_url_safety.socket, "getaddrinfo", _getaddrinfo)
    monkeypatch.delenv(_url_safety.EGRESS_ALLOW_HOSTS_ENV, raising=False)
