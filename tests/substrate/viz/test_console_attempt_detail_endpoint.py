"""How GET /viz/api/console/attempts/detail refuses, over HTTP.

PACT operator-console-live-updates. The composer's own refusals are proven in
tests/substrate/console_views/test_attempt_detail_composition.py. What is
proven HERE is the mapping from those refusals to a status code and a
response body, which is what a browser actually receives -- and which lives
in src/okg/viz/endpoints.py, not in the composer.

Four answers, and each is wrong in a way the composer's own tests cannot see:

* an id no checkpoint could take is 400 MALFORMED, a different answer from
  lost;
* an id this branch does not hold is 404 with a CONSTANT body, so an id
  belonging to another deployment is byte-for-byte an id that never existed
  -- a body echoing the id or a count would itself be information about
  another deployment's history;
* an unknown query parameter is 422 by COUNT, because FastAPI drops extras
  silently and a client would believe a filter it sent was applied;
* any other failure is 500 carrying the exception TYPE and never str(exc),
  whose psycopg form names the host, the IP and the role.

No database: the composer is the unit under test elsewhere, and what this
file drives is the route. The connection is a stub the route opens and the
stubbed composer never reads.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Iterator

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.console_views import attempt_detail  # noqa: E402
from okg.viz.endpoints import build_router  # noqa: E402
from okg.viz.console_perf_budgets import (  # noqa: E402
    statement_budget,
    uncovered_console_routes,
)

ROUTE = "/viz/api/console/attempts/detail"
DEPLOYMENT = "lane-d-home-render"
GENERATION = "okg:lane-d-home-render:branch:default:gen:5c5404d36118"
#: A real checkpoint id shape, and the id the refusal tests ask for.
HELD_ID = "okg:lane-d-home-render:20260921T024809349784Z:5c5404d36118"


class _Context:
    deployment_name = DEPLOYMENT
    session = SimpleNamespace(generation_id=GENERATION, branch_name="default")

    @contextmanager
    def conn(self) -> Iterator[object]:
        yield object()


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(build_router(_Context))
    return TestClient(app, raise_server_exceptions=False)


def _compose(monkeypatch: pytest.MonkeyPatch, raises: BaseException) -> list[str]:
    """Make the composer raise, and record whether it was reached at all."""
    reached: list[str] = []

    def _fake(conn: Any, **kwargs: Any) -> dict[str, Any]:
        reached.append(str(kwargs["attempt_id"]))
        raise raises

    monkeypatch.setattr(attempt_detail, "compose_attempt_detail", _fake)
    return reached


# --------------------------------------------------------------------------- #
# 400: an id no checkpoint could take                                          #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "bad",
    [
        "postgres://okg:pw@db.example:5432/okg",
        "/Users/example/okg/deployments/lane-d.yaml",
        "okg:lane-d:gen:x' OR 1=1--",
        "~/.okg/local.env",
    ],
)
def test_a_non_checkpoint_shaped_id_is_a_named_400(bad: str) -> None:
    response = _client().get(ROUTE, params={"id": bad})

    assert response.status_code == 400, response.text
    detail = response.json()["detail"]
    assert detail["reason"] == "attempt_id_not_checkpoint_shaped"
    assert detail["accepted"] == ["id"]
    # The refusal does not echo what it refused: a DSN or a path handed back
    # in an error body is the same disclosure as one served in a payload.
    body = response.text
    assert bad not in body
    assert "db.example" not in body
    assert "/Users/example" not in body


# --------------------------------------------------------------------------- #
# 404: a CONSTANT body                                                         #
# --------------------------------------------------------------------------- #
def test_an_id_this_deployment_does_not_hold_is_a_constant_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reached = _compose(monkeypatch, attempt_detail.AttemptNotFound(HELD_ID))
    response = _client().get(ROUTE, params={"id": HELD_ID})

    assert reached == [HELD_ID]
    assert response.status_code == 404, response.text
    # The WHOLE body, not just its reason: a count, a deployment name or the
    # id itself would each let a reader tell "does not exist" from "exists,
    # elsewhere", and that difference is information about another
    # deployment's history.
    assert response.json() == {"detail": {"reason": "attempt_not_found"}}
    assert "5c5404d36118" not in response.text
    assert DEPLOYMENT not in response.text


def test_two_different_missing_ids_answer_byte_for_byte_the_same(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    bodies = []
    for attempt_id in (HELD_ID, "okg:some-other-deployment:gen:ffffffffffff"):
        _compose(monkeypatch, attempt_detail.AttemptNotFound(attempt_id))
        bodies.append(client.get(ROUTE, params={"id": attempt_id}).content)
    assert bodies[0] == bodies[1]


# --------------------------------------------------------------------------- #
# 422: an unknown parameter is refused, not dropped                            #
# --------------------------------------------------------------------------- #
def test_an_unknown_query_parameter_is_refused_by_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FastAPI drops an extra silently. A client that sent an outcome filter
    would then believe it was applied to what it got back."""
    reached = _compose(monkeypatch, AssertionError("the composer ran"))
    response = _client().get(
        ROUTE, params={"id": HELD_ID, "okg_secret_source_name": "x", "limit": "5"},
    )

    assert response.status_code == 422, response.text
    assert reached == []  # refused before the read, not after it
    detail = response.json()["detail"]
    assert detail["reason"] == "unknown_query_parameter"
    assert detail["accepted"] == ["id"]
    assert detail["unknown_count"] == 2
    # COUNTED, not echoed: the names are caller-supplied text.
    assert "okg_secret_source_name" not in response.text


def test_the_accepted_parameter_alone_is_not_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reached = _compose(monkeypatch, attempt_detail.AttemptNotFound(HELD_ID))
    assert _client().get(ROUTE, params={"id": HELD_ID}).status_code == 404
    assert reached == [HELD_ID]


# --------------------------------------------------------------------------- #
# 500: the exception TYPE, never its text                                      #
# --------------------------------------------------------------------------- #
def test_an_unexpected_failure_serves_the_type_and_no_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OperationalError(RuntimeError):
        """Stands for psycopg's, whose str() names the host, IP and role."""

    _compose(monkeypatch, OperationalError(
        'connection to server at "db.example" (10.0.0.5), port 5432 failed: '
        'FATAL: password authentication failed for user "okg_writer"',
    ))
    response = _client().get(ROUTE, params={"id": HELD_ID})

    assert response.status_code == 500, response.text
    detail = response.json()["detail"]
    assert detail["reason"] == "attempt_detail_composer_failed"
    assert detail["error_type"] == "OperationalError"
    assert detail["generation_id"] == GENERATION
    body = response.text
    for secret in ("db.example", "10.0.0.5", "okg_writer", "password", "FATAL"):
        assert secret not in body, secret
    assert "Traceback" not in body


def test_no_refusal_body_carries_a_path_a_dsn_or_a_hostname(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One sweep over every branch, because a body is easy to widen later."""
    client = _client()
    bodies = [client.get(ROUTE, params={"id": "/Users/example/okg"}).text]
    _compose(monkeypatch, attempt_detail.AttemptNotFound(HELD_ID))
    bodies.append(client.get(ROUTE, params={"id": HELD_ID}).text)
    bodies.append(client.get(ROUTE, params={"id": HELD_ID, "x": "1"}).text)
    _compose(monkeypatch, RuntimeError("/Users/example/okg at db.example"))
    bodies.append(client.get(ROUTE, params={"id": HELD_ID}).text)

    for body in bodies:
        json.loads(body)
        for shape in ("://", "/Users/", "db.example", "Traceback"):
            assert shape not in body, (shape, body)


# --------------------------------------------------------------------------- #
# the route's declared cost budget                                             #
# --------------------------------------------------------------------------- #
def test_the_route_ships_with_a_declared_budget() -> None:
    """Issue #2119: a console route carries a declared, tested budget.

    ``uncovered_console_routes`` reads the LIVE router, so a route registered
    with neither a budget nor a stated exclusion fails here. The assertion is
    scoped to THIS route on purpose: the same completeness gate over the whole
    console (tests/okg-workspace/test_console_view_performance.py) is red today
    for routes other changes own, so a scoped check states this route's own
    position instead of waiting on theirs.

    The second half is what keeps the declaration from rotting into a number
    nothing measures: the budget must name this route, and the test named in
    it is the one that measures it.
    """
    assert ROUTE not in uncovered_console_routes(build_router(_Context))

    budget = statement_budget("attempt-detail")
    assert budget.path == ROUTE
    assert budget.measured <= budget.ceiling
    assert budget.enforced_by.startswith(
        "tests/substrate/console_views/test_attempt_detail_live_reads.py::"
    )
