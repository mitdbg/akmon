"""Parameter handling on the console search route (PACT
operator-console-search-filters).

These cases need no database. Every one of them is about a request the route
must refuse BEFORE it reads anything, so the context provider here raises if it
is ever called: a refusal that opened a pinned session would still be a
refusal, but it would have paid for a graph read the caller never gets, and the
version-pinning requirement says the refusal comes first.

The defect these guard against is silence. The console sent ``source``, ``sort``
and ``since``; the route read only ``q`` and ``type`` and dropped the rest
without a word, so Explore would have shown one result set while its rail
claimed another.
"""
from __future__ import annotations

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from okg.viz.read_cache import CLIENT_ONLY_PARAMS
from okg.viz.record_endpoints import register_operator_record_routes


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


VERSION = "okg:demo:branch:main:gen:1"
BASE = {"version": VERSION, "q": "ingest"}


def _detail(response) -> dict:
    return response.json()["detail"]


@pytest.mark.parametrize("value", ["newest", "oldest", "", "NEWEST", "rank"])
def test_sort_is_refused_with_its_cause(client: TestClient, value: str) -> None:
    """Any sort but relevance is refused, and the refusal says why."""
    response = client.get(
        "/viz/api/console/records/search", params={**BASE, "sort": value},
    )
    assert response.status_code == 422
    detail = _detail(response)
    assert detail["reason"] == "filter_has_no_stored_basis"
    assert detail["parameter"] == "sort"
    assert detail["value"] == value
    # The cause is stated, not merely named: an operator reading this must
    # learn that the graph has no per-record date to sort by.
    assert "no per-record date" in detail["error"]


def test_sort_relevance_is_the_accepted_no_op(client: TestClient) -> None:
    """relevance is what the route already does, so it is not a refusal.

    It still reaches the read path -- which this fixture refuses -- and that is
    the point: it was NOT rejected as an unhonorable filter.
    """
    with pytest.raises(_ReadAttempted):
        client.get(
            "/viz/api/console/records/search",
            params={**BASE, "sort": "relevance"},
        )


@pytest.mark.parametrize("value", ["month", "year", "2026-01-01", ""])
def test_since_is_refused_with_its_cause(client: TestClient, value: str) -> None:
    response = client.get(
        "/viz/api/console/records/search", params={**BASE, "since": value},
    )
    assert response.status_code == 422
    detail = _detail(response)
    assert detail["reason"] == "filter_has_no_stored_basis"
    assert detail["parameter"] == "since"
    assert detail["value"] == value
    assert "no per-record date" in detail["error"]


def test_unknown_parameter_name_is_refused_and_named(client: TestClient) -> None:
    """A name the route does not know is refused, and the name is reported.

    This is the guard against the whole defect class: a client that starts
    sending a filter the server never grew cannot be answered as though the
    filter did not exist.
    """
    response = client.get(
        "/viz/api/console/records/search",
        params={**BASE, "ordering": "asc", "author": "riley"},
    )
    assert response.status_code == 422
    detail = _detail(response)
    assert detail["reason"] == "unknown_parameter"
    assert detail["parameters"] == ["author", "ordering"]
    assert "author" in detail["error"] and "ordering" in detail["error"]


def test_a_named_cause_wins_over_the_generic_unknown_name(
    client: TestClient,
) -> None:
    """sort and since are reported with their cause, not as unknown names.

    Both are also outside the known set, so ordering decides which refusal the
    caller sees; the one that explains itself must win.
    """
    response = client.get(
        "/viz/api/console/records/search",
        params={**BASE, "since": "month", "nonsense": "1"},
    )
    assert _detail(response)["reason"] == "filter_has_no_stored_basis"


def test_every_parameter_the_route_honors_is_accepted(
    client: TestClient,
) -> None:
    """The honored set reaches the read path rather than being refused."""
    with pytest.raises(_ReadAttempted):
        client.get(
            "/viz/api/console/records/search",
            params={
                "version": VERSION,
                "q": "ingest",
                "type": "issue",
                "source": "source-07-git",
                "limit": 20,
                "offset": 0,
            },
        )


def test_refusals_do_not_leak_which_sources_exist(client: TestClient) -> None:
    """No refusal message names a producer or says whether one exists."""
    response = client.get(
        "/viz/api/console/records/search",
        params={**BASE, "since": "month", "source": "secret-producer"},
    )
    assert response.status_code == 422
    assert "secret-producer" not in response.text


# --- what every route accepts, and what it refuses --------------------------

ROUTES: dict[str, dict[str, str]] = {
    "search": {"version": VERSION, "q": "ingest"},
    "types": {"version": VERSION},
    "list": {"version": VERSION, "type": "issue"},
    "detail": {"version": VERSION, "id": "issue:1"},
    "versions": {},
}


#: The record-detail route is served at /records/detail but names its read
#: "record", which is the key its known-parameter set is filed under.
READ_OF_ROUTE = {
    "search": "search", "types": "types", "list": "list",
    "detail": "record", "versions": "versions",
}


def _path(read: str) -> str:
    return (
        "/viz/api/console/versions" if read == "versions"
        else f"/viz/api/console/records/{read}"
    )


@pytest.mark.parametrize("read", sorted(ROUTES))
@pytest.mark.parametrize("name", sorted(CLIENT_ONLY_PARAMS))
def test_a_client_only_parameter_is_accepted_by_every_route(
    client: TestClient, read: str, name: str,
) -> None:
    """A Refresh click must not be answered with a refusal.

    The shipped client appends `refresh` to every read of a refreshed view, so
    a route that refuses names it does not know has to accept it. It selects no
    records and changes no result set, so accepting it is not the silent
    dropping of a filter this guard exists to prevent.

    Enumerated from `read_cache.CLIENT_ONLY_PARAMS` -- the same list the cache
    key strips -- so a global parameter added there cannot be forgotten here.
    """
    with pytest.raises(_ReadAttempted):
        client.get(_path(read), params={**ROUTES[read], name: "1"})


@pytest.mark.parametrize("read", sorted(ROUTES))
def test_every_route_refuses_a_name_it_does_not_know(
    client: TestClient, read: str,
) -> None:
    """Not only search: an ignored parameter is the same defect anywhere."""
    response = client.get(_path(read), params={**ROUTES[read], "nonsense": "1"})

    assert response.status_code == 422
    detail = _detail(response)
    assert detail["reason"] == "unknown_parameter"
    assert detail["parameters"] == ["nonsense"]


@pytest.mark.parametrize("read", sorted(ROUTES))
def test_every_route_accepts_exactly_the_parameters_it_declares(
    client: TestClient, read: str,
) -> None:
    """Each declared name reaches the read path rather than being refused."""
    from okg.viz.record_endpoints import _KNOWN_PARAMS

    params = {name: "1" for name in _KNOWN_PARAMS[READ_OF_ROUTE[read]]}
    params.update(ROUTES[read])
    with pytest.raises(_ReadAttempted):
        client.get(_path(read), params=params)


def test_a_repeated_parameter_is_refused_rather_than_collapsed(
    client: TestClient,
) -> None:
    """Starlette keeps the last of a repeat; the caller asked for both."""
    response = client.get(
        "/viz/api/console/records/search",
        params=[("version", VERSION), ("q", "a"), ("source", "x"), ("source", "y")],
    )
    assert response.status_code == 422
    detail = _detail(response)
    assert detail["reason"] == "repeated_parameter"
    assert detail["parameters"] == ["source"]


def test_a_repeated_parameter_is_refused_on_every_name_that_takes_one_value(
    client: TestClient,
) -> None:
    response = client.get(
        "/viz/api/console/records/search",
        params=[
            ("version", VERSION), ("version", VERSION),
            ("q", "a"), ("q", "b"), ("offset", "0"), ("offset", "5"),
        ],
    )
    assert _detail(response)["parameters"] == ["offset", "q", "version"]


def test_a_repeat_is_reported_before_an_unknown_name(client: TestClient) -> None:
    """Both are wrong; the caller is told about the one it can act on first."""
    response = client.get(
        "/viz/api/console/records/search",
        params=[("version", VERSION), ("q", "a"), ("q", "b"), ("nonsense", "1")],
    )
    assert _detail(response)["reason"] == "repeated_parameter"


# --- console-search-profile-subtypes (issue #2545): typed scope refusals ---


def _refuse_with(exc: Exception):
    def resolve(*_args, **_kwargs):
        raise exc

    return resolve


def test_a_profile_refusal_reaches_the_page_typed_with_its_names() -> None:
    from fastapi import HTTPException

    from okg.substrate.mcp.search_profiles import ProfileSubtypeAbsent
    from okg.viz.console_search_scope import resolve_console_search_scope

    refusal = ProfileSubtypeAbsent(
        "search profile 'analyst' names subtype(s) ['pgae'] that no module defines",
        data={"profile": "analyst", "absent": ["pgae"]},
        fix_hint="fix the subtype spelling",
    )
    with pytest.raises(HTTPException) as caught:
        resolve_console_search_scope(
            _refuse_with(refusal), None, None,
            deployment="demo", subtype_filter=None, version=VERSION,
        )
    detail = caught.value.detail
    assert caught.value.status_code == 503
    assert detail["reason"] == "search_profile_subtype_absent"
    assert detail["data"]["absent"] == ["pgae"]
    assert detail["next_check"] == "fix the subtype spelling"


def test_a_malformed_search_block_keeps_the_manifest_path_off_the_page() -> None:
    from fastapi import HTTPException

    from okg.substrate.mcp.search_profiles import SearchProfileConfigInvalid
    from okg.viz.console_search_scope import resolve_console_search_scope

    refusal = SearchProfileConfigInvalid(
        "deployment.yaml `search:` must be a mapping; got list. "
        "(Manifest: /home/operator/private/deployment.yaml)",
        fix_hint="declare `search:` as a mapping",
    )
    with pytest.raises(HTTPException) as caught:
        resolve_console_search_scope(
            _refuse_with(refusal), None, None,
            deployment="demo", subtype_filter=None, version=VERSION,
        )
    detail = caught.value.detail
    assert detail["reason"] == "search_profile_config_invalid"
    assert "/home/operator" not in repr(detail)
    assert detail["data"] is None


def test_an_unrelated_failure_is_not_turned_into_a_scope_refusal() -> None:
    from okg.viz.console_search_scope import resolve_console_search_scope

    with pytest.raises(RuntimeError):
        resolve_console_search_scope(
            _refuse_with(RuntimeError("connection to 10.0.0.1 failed")), None, None,
            deployment="demo", subtype_filter=None, version=VERSION,
        )
