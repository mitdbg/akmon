"""HttpMetadataProvider against a fake session.

PACT `opensearch-reference-integration` (issue #2014), req.ori.source.

The "an unreadable cluster retracts nothing" claim was previously proven
only through the RECORDED provider's missing-file path. That is a
different code path from the one a real deployment uses, so it proved the
wrong thing. These drive the live provider itself with a session that
returns timeouts, 403s, 500s and redirects, and assert that the run never
completes a scope it could not read.

No network, no container, no database: the fake session answers in
process.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from okg.substrate.library.sources._opensearch_client import (
    HttpMetadataProvider,
    OpenSearchMetadataError,
    redact_url,
)
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.opensearch_catalog import (
    OpenSearchCatalogSource,
)


FIXTURES = Path(__file__).parent / "opensearch_fixtures" / "baseline"

ENDPOINTS = {
    "/": "root",
    "/_cluster/health": "health",
    "/_cat/indices": "cat_indices",
    "/_mapping": "mapping",
    "/_settings": "settings",
    "/_alias": "alias",
}

REQUIRED_PATHS = ("/", "/_cluster/health", "/_cat/indices", "/_mapping",
                  "/_alias")


def _body_for(path: str) -> Any:
    for prefix, stem in ENDPOINTS.items():
        if path == prefix or path.startswith(prefix + "?"):
            return json.loads((FIXTURES / f"{stem}.json").read_text())
    raise AssertionError(f"unexpected path {path!r}")


class _Response:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        self._raw = json.dumps(body).encode("utf-8")

    def iter_content(self, chunk_size: int = 65536):
        for i in range(0, len(self._raw), chunk_size):
            yield self._raw[i:i + chunk_size]

    def json(self) -> Any:
        return json.loads(self._raw.decode("utf-8"))


class _FakeSession:
    """Serves the recorded fixtures, with one named endpoint made to fail.

    `calls` records what was asked for, so a test can also assert that a
    refusal happened BEFORE the remaining endpoints were read.
    """

    def __init__(
        self,
        *,
        fail_path: str | None = None,
        status: int | None = None,
        raises: Exception | None = None,
    ) -> None:
        self.fail_path = fail_path
        self.status = status
        self.raises = raises
        self.calls: list[str] = []
        self.kwargs: list[dict] = []

    def get(self, url: str, **kwargs: Any) -> _Response:
        parts = urlsplit(url)
        path = parts.path + (f"?{parts.query}" if parts.query else "")
        self.calls.append(path)
        self.kwargs.append(kwargs)
        matches = self.fail_path is not None and (
            path == self.fail_path or path.startswith(self.fail_path + "?")
        )
        if matches:
            if self.raises is not None:
                raise self.raises
            return _Response(self.status or 500, {"error": "refused"})
        return _Response(200, _body_for(path))


def _source(session: _FakeSession) -> OpenSearchCatalogSource:
    provider = HttpMetadataProvider(
        base_url="http://127.0.0.1:9209",
        session=session,
        excluded_globs=("top_queries-*",),
    )
    return OpenSearchCatalogSource(
        provider=provider,
        namespace="osref",
        services=[{
            "service_id": "okg-api", "name": "OKG API",
            "indices": ["app-logs-2026.09"],
        }],
    )


# ── the happy path, so the failures below mean something ──────────────

def test_the_live_provider_reads_the_same_graph_as_the_recorded_one() -> None:
    session = _FakeSession()
    facts = list(_source(session).run("run-1", mode="reconcile").facts)
    indices = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "search_index"
    ]
    assert len(indices) == 3
    assert sorted(session.calls)[:2] == ["/", "/_alias"]


# ── every required endpoint, three ways to fail ───────────────────────

@pytest.mark.parametrize("path", REQUIRED_PATHS)
@pytest.mark.parametrize(
    ("status", "raises"),
    [
        (None, TimeoutError("read timed out")),
        (403, None),
        (500, None),
        (302, None),
    ],
    ids=["timeout", "forbidden", "server-error", "redirect"],
)
def test_a_failed_required_endpoint_raises_and_completes_no_scope(
    path: str, status: int | None, raises: Exception | None,
) -> None:
    """Each of these must RAISE.

    Under `missing_from_completed_scope` a run that returned a completed
    scope here would retract every node this source owns, so one 403 on
    one endpoint would empty the graph. A redirect counts as a failure on
    purpose: redirects are not followed, so a 302 is an unread response.
    """
    session = _FakeSession(fail_path=path, status=status, raises=raises)
    with pytest.raises(OpenSearchMetadataError):
        list(_source(session).run("run-1", mode="reconcile").facts)


@pytest.mark.parametrize("path", REQUIRED_PATHS)
def test_flipping_required_to_false_would_be_caught(path: str) -> None:
    """The guard against the guard.

    If `required` were flipped on any of these endpoints, the provider
    would return None and `parse_snapshot` would be handed a None it
    cannot use. This asserts the failure is loud whichever way it goes --
    a silent None reaching the parser must not produce facts.
    """
    session = _FakeSession(fail_path=path, status=500)
    provider = HttpMetadataProvider(
        base_url="http://127.0.0.1:9209", session=session,
    )
    with pytest.raises(Exception) as excinfo:
        provider.fetch()
    assert not isinstance(excinfo.value, AssertionError)


# ── the one endpoint that is allowed to fail ──────────────────────────

def test_a_forbidden_settings_call_reads_as_retention_unknown() -> None:
    """403 on /_settings is survivable, and says so.

    The cluster is still readable; what is lost is the retention policy.
    Rendering that as "no policy attached" would assert something on no
    evidence, so every index reports retention_read false instead.
    """
    session = _FakeSession(fail_path="/_settings", status=403)
    facts = list(_source(session).run("run-1", mode="reconcile").facts)
    indices = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "search_index"
    ]
    assert indices
    for node in indices:
        assert node.attrs["retention_read"] is False
        assert node.attrs["retention"] is None


def test_a_settings_failure_does_not_silently_become_no_policy() -> None:
    """The same cluster, read two ways, must not look the same.

    With settings readable, two indices carry a policy. Without, none
    does -- but the distinguishing flag has to be there, or a reader
    cannot tell "no policy" from "could not tell".
    """
    readable = list(_source(_FakeSession()).run("r", mode="reconcile").facts)
    blocked = list(
        _source(_FakeSession(fail_path="/_settings", status=403))
        .run("r", mode="reconcile").facts
    )

    def _retentions(facts: list[Any]) -> set:
        return {
            (f.attrs["retention"], f.attrs["retention_read"])
            for f in facts
            if isinstance(f, NodeFact) and f.subtype == "search_index"
        }

    assert (None, True) in _retentions(readable)
    assert any(r is not None for r, _ in _retentions(readable))
    assert _retentions(blocked) == {(None, False)}


# ── the request bounds ────────────────────────────────────────────────

def test_every_request_refuses_redirects() -> None:
    session = _FakeSession()
    _source(session).run("run-1", mode="reconcile")
    assert session.kwargs, "no request was made"
    for kwargs in session.kwargs:
        assert kwargs["allow_redirects"] is False


def test_a_non_http_base_url_is_refused() -> None:
    for url in ("file:///etc/passwd", "ftp://host/x", "gopher://host"):
        with pytest.raises(OpenSearchMetadataError):
            HttpMetadataProvider(base_url=url)


def test_an_oversized_body_is_refused_rather_than_read() -> None:
    session = _FakeSession()
    provider = HttpMetadataProvider(
        base_url="http://127.0.0.1:9209", session=session,
        max_response_bytes=16,
    )
    with pytest.raises(OpenSearchMetadataError) as excinfo:
        provider.fetch()
    assert "bytes" in str(excinfo.value)


def test_the_total_time_budget_bounds_the_whole_read() -> None:
    """The per-socket timeout bounds one quiet period, not the read.

    A server dribbling one byte per interval resets the socket timeout
    forever, so the budget is tracked as a deadline across all six calls.
    """
    session = _FakeSession()
    provider = HttpMetadataProvider(
        base_url="http://127.0.0.1:9209", session=session,
        total_budget_seconds=-1.0,
    )
    with pytest.raises(OpenSearchMetadataError) as excinfo:
        provider.fetch()
    assert "time budget" in str(excinfo.value)


# ── credentials never reach a message ─────────────────────────────────

def test_a_credential_in_the_url_is_stripped_from_every_message() -> None:
    """Error text is the one place a credential leaks with nobody
    deciding to write it down."""
    assert redact_url("https://user:pa55w0rd@es.example:9200/_mapping") == (
        "https://<redacted>@es.example:9200/_mapping"
    )
    assert redact_url("http://127.0.0.1:9209/_alias") == (
        "http://127.0.0.1:9209/_alias"
    )

    session = _FakeSession(fail_path="/_mapping", status=500)
    provider = HttpMetadataProvider(
        base_url="https://user:pa55w0rd@es.example:9200", session=session,
    )
    with pytest.raises(OpenSearchMetadataError) as excinfo:
        provider.fetch()
    rendered = str(excinfo.value) + json.dumps(
        getattr(excinfo.value, "data", {}) or {}, default=str,
    )
    assert "pa55w0rd" not in rendered
    assert "user" not in rendered
    assert "<redacted>" in rendered


def test_an_underlying_exception_message_is_not_pasted_into_the_error() -> None:
    """A driver's exception text can itself carry the URL it was given,
    credential included, so only its TYPE is reported."""
    leaky = RuntimeError("failed connecting to https://user:pa55w0rd@es:9200")
    session = _FakeSession(fail_path="/", raises=leaky)
    provider = HttpMetadataProvider(
        base_url="https://user:pa55w0rd@es:9200", session=session,
    )
    with pytest.raises(OpenSearchMetadataError) as excinfo:
        provider.fetch()
    assert "pa55w0rd" not in str(excinfo.value)
    assert "RuntimeError" in str(excinfo.value)
