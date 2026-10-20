"""Console reads serve no server path, connection string or exception text.

Issue #2130, PACT console-reads-mask-paths-and-exception-text. The database-
free half: the rule itself, the worktree label, the
blocked-reason code, the view-diagnostics registry and run error, and the
failure body of EVERY console GET route with every connection raising text
that names a path, a host, an address and a password. The half that needs a
real publication is `test_console_reads_serve_no_server_text.py`, which reuses
the route recipes below.
"""
from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterator

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.console_views import generations as generations_module  # noqa: E402
from okg.substrate.console_views import view_diagnostics  # noqa: E402
from okg.substrate.console_views import worktrees  # noqa: E402
from okg.substrate.console_views.served_text import (  # noqa: E402
    CONNECTION_WITHHELD,
    DEPLOYMENT_DIRECTORY,
    EXCEPTION_TEXT_WITHHELD,
    PATH_WITHHELD,
    READ_FAILED_SENTENCE,
    SERVED_TEXT_RULE,
    SYNC_ROOT,
    mask_served_text,
)
from okg.viz.app import build_app  # noqa: E402
from okg.viz.console_perf_budgets import console_get_routes  # noqa: E402
from okg.viz.endpoints import build_router  # noqa: E402

LEAK_HOME = "/Users/example"
LEAK_DSN = "postgresql://okg:hunter2@db.leak-probe.example:5432/okg"
LEAK_EXCEPTION = (
    'psycopg.OperationalError: connection to server at "db.leak-probe.example" '
    '(10.9.8.7), port 5432 failed: FATAL: password authentication failed for '
    f'user "okg" ({LEAK_DSN}) reading {LEAK_HOME}/inbox/secret.yaml'
)
LEAK_TRACEBACK = (
    "Traceback (most recent call last):\n"
    f'  File "{LEAK_HOME}/okg/src/okg/views/leak_probe.py", line 12, in build\n'
    f"ValueError: cannot read {LEAK_HOME}/inbox/secret.yaml"
)

#: What no console body may contain. `Error:` with a colon is exception text;
#: an exception's TYPE name alone (`error_type: "RuntimeError"`) is not.
LEAK_PATTERN = re.compile(
    r"/Users/|/home/|/private/|/var/folders/|Traceback|\b\w+(?:Error|Exception):"
    r"|postgres(?:ql)?://|hunter2|leak-probe\.example|10\.9\.8\.7|File \""
)


def hits(body: Any, pattern: re.Pattern[str] = LEAK_PATTERN, at: str = "") -> list[str]:
    """Every field (or key) of ``body`` the pattern matches, by trail."""
    found: list[str] = []
    if isinstance(body, dict):
        for key, value in body.items():
            trail = f"{at}.{key}"
            if pattern.search(str(key)):
                found.append(f"{trail} (key)")
            found.extend(hits(value, pattern, trail))
    elif isinstance(body, list):
        for index, value in enumerate(body):
            found.extend(hits(value, pattern, f"{at}[{index}]"))
    elif isinstance(body, str):
        match = pattern.search(body)
        if match:
            found.append(f"{at or '.'} = {body[max(0, match.start() - 30):match.end() + 30]!r}")
    return found


# --------------------------------------------------------------------------- #
# one request per console GET route                                            #
# --------------------------------------------------------------------------- #
#: Every console GET route, with the parameters that make it do real work.
#: `test_every_console_get_route_has_a_recipe` fails the moment a route is
#: added without one, so a new read cannot skip the sweep.
RECIPES: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "/viz/api/console/status": lambda s: {},
    "/viz/api/console/subtype": lambda s: {"name": "document"},
    "/viz/api/console/generations": lambda s: {"limit": 50},
    "/viz/api/console/attempts": lambda s: {"range": "30d"},
    "/viz/api/console/attempts/detail": lambda s: {"id": s["attempt"]},
    "/viz/api/console/sources": lambda s: {},
    "/viz/api/console/tasks": lambda s: {},
    "/viz/api/console/agents": lambda s: {},
    "/viz/api/console/health": lambda s: {},
    "/viz/api/console/decisions": lambda s: {},
    "/viz/api/console/worktrees": lambda s: {},
    "/viz/api/console/source-detail": lambda s: {"name": s["source"]},
    "/viz/api/console/source-diagnostics": lambda s: {"name": s["source"]},
    "/viz/api/console/enricher-diagnostics": lambda s: {},
    "/viz/api/console/view-diagnostics": lambda s: {"name": s["view"]},
    "/viz/api/console/producers": lambda s: {"name": s["source"]},
    "/viz/api/console/producer-metrics": lambda s: {"producers": f"view:{s['view']}"},
    "/viz/api/console/view": lambda s: {"id": "okg_workspace.decisions"},
    "/viz/api/console/versions": lambda s: {},
    "/viz/api/console/ontology-rollup": lambda s: {"version": s["version"]},
    "/viz/api/console/records/types": lambda s: {"version": s["version"]},
    "/viz/api/console/records/list": lambda s: {"version": s["version"], "type": "document"},
    "/viz/api/console/records/search": lambda s: {"version": s["version"], "q": "leak probe"},
    "/viz/api/console/records/detail": lambda s: {"version": s["version"], "id": s["record"]},
    "/viz/api/console/records/raw": lambda s: {"version": s["version"], "id": s["record"]},
    "/viz/api/console/records/artifact": lambda s: {"version": s["version"], "id": s["record"]},
    "/viz/api/console/records/artifact/bytes": lambda s: {"version": s["version"], "id": s["record"]},
    "/viz/api/console/records/payload": lambda s: {"version": s["version"], "id": s["record"]},
    # The Graph view's four reads (PACT console-graph-views). The record read
    # names the seeded document, which the database sweep links to a session
    # whose attributes carry a server path, so its neighbour group is real.
    "/viz/api/console/graph/types": lambda s: {"version": s["version"]},
    "/viz/api/console/graph/source-links": lambda s: {"version": s["version"]},
    "/viz/api/console/graph/record": lambda s: {"version": s["version"], "id": s["record"]},
    "/viz/api/console/graph/ontology": lambda s: {"version": s["version"]},
}


@pytest.mark.parametrize(
    "leak",
    [LEAK_HOME, LEAK_DSN, LEAK_EXCEPTION, LEAK_TRACEBACK, "reached db.leak-probe.example"],
)
def test_the_leak_pattern_catches_every_probe_value_alone(leak: str) -> None:
    """Each probe value, served on its own, is a leak -- the host name included.

    The probe host was renamed to `db.leak-probe.example`; a pattern still
    naming the old host would let a body that leaks only the host pass.
    """
    assert LEAK_PATTERN.search(leak), leak


def test_every_console_get_route_has_a_recipe() -> None:
    """A console GET route with no request recipe fails here, by name."""
    live = console_get_routes(build_router(lambda: None))
    assert live - set(RECIPES) == set(), "console routes with no sweep recipe"
    assert set(RECIPES) - live == set(), "recipes for routes that no longer exist"


# --------------------------------------------------------------------------- #
# the rule                                                                     #
# --------------------------------------------------------------------------- #
ROOTS = [
    ("/srv/okg/deployments/w", DEPLOYMENT_DIRECTORY),
    ("/srv/sync-roots/w", SYNC_ROOT),
]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("/srv/okg/deployments/w/deployment.yaml", f"{DEPLOYMENT_DIRECTORY}/deployment.yaml"),
        (
            "2.8% available on /srv/okg/deployments/w, floor 6%",
            f"2.8% available on {DEPLOYMENT_DIRECTORY}, floor 6%",
        ),
        (
            "recorded '/srv/sync-roots/w/deployments/w', running '/srv/okg/deployments/w'",
            f"recorded '{SYNC_ROOT}/deployments/w', running '{DEPLOYMENT_DIRECTORY}'",
        ),
        # A sibling of a root is not under it.
        ("/srv/okg/deployments/w2/x", PATH_WITHHELD),
        (f"{LEAK_HOME}/notes.md", PATH_WITHHELD),
        ("see ~/notes and --dir=/opt/x", f"see {PATH_WITHHELD} and --dir={PATH_WITHHELD}"),
        ("C:\\Users\\okg\\x", PATH_WITHHELD),
        (LEAK_DSN, CONNECTION_WITHHELD),
        ("host=db.leak-probe.example dbname=okg user=okg", CONNECTION_WITHHELD),
        ("password=hunter2", CONNECTION_WITHHELD),
        ("https://u:hunter2@example.test/a", CONNECTION_WITHHELD),
        (LEAK_EXCEPTION, EXCEPTION_TEXT_WITHHELD),
        (LEAK_TRACEBACK, EXCEPTION_TEXT_WITHHELD),
        # Left alone: a ratio, a relative path, a command with no path.
        ("p95/p50, N/A", "p95/p50, N/A"),
        ("deployments/w/deployment.yaml", "deployments/w/deployment.yaml"),
        ("uv run okg sync poll --deployment w --json", "uv run okg sync poll --deployment w --json"),
    ],
)
def test_the_rule_rewrites_what_it_should_and_nothing_else(value: str, expected: str) -> None:
    masked, trails = mask_served_text({"v": value}, roots=ROOTS)
    assert masked["v"] == expected
    assert trails == ([] if expected == value else ["v"])


def test_keys_are_rewritten_and_named_without_colliding() -> None:
    masked, trails = mask_served_text({"/a/x": 1, "/b/y": {"k": "/c/z"}})
    assert masked == {PATH_WITHHELD: 1, f"{PATH_WITHHELD}~2": {"k": PATH_WITHHELD}}
    assert sorted(trails) == [PATH_WITHHELD, f"{PATH_WITHHELD}~2", f"{PATH_WITHHELD}~2.k"]


def test_the_rule_states_its_own_blind_spot() -> None:
    assert "NOT detected" in SERVED_TEXT_RULE
    assert "hostname" in SERVED_TEXT_RULE


# --------------------------------------------------------------------------- #
# the worktree label, the blocked reason, the view registry                    #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("cwd", "label"),
    [
        (f"{LEAK_HOME}/okg/.claude/worktrees/lane-d/okg", "lane-d/okg"),
        (f"{LEAK_HOME}/.codex/worktrees/abcd/okg", "abcd/okg"),
        # Outside worktrees/: the last segment only, never the parent.
        (f"{LEAK_HOME}/projects/okg", "okg"),
        ("C:\\Users\\example\\okg", "okg"),
        ("/", "(unnamed)"),
    ],
)
def test_a_worktree_label_never_carries_the_account(cwd: str, label: str) -> None:
    assert worktrees._label(cwd) == label
    assert "example" not in worktrees._label(cwd)


@pytest.mark.parametrize(
    ("recorded", "served", "why"),
    [
        (None, None, None),
        ({"reason": "checkpoint_attestation_failed", "message": LEAK_EXCEPTION},
         "checkpoint_attestation_failed", None),
        ({"code": "invariant_failed"}, "invariant_failed", None),
        ({"message": LEAK_EXCEPTION}, None, "blocked_reason_not_code_shaped"),
        ({"reason": f"{LEAK_HOME}/x", "error": LEAK_TRACEBACK}, None, "blocked_reason_not_code_shaped"),
        ("disk full on /Users/example", None, "blocked_reason_not_code_shaped"),
        ("publish_blocked", "publish_blocked", None),
    ],
)
def test_blocked_reason_is_a_code_or_null_with_why(recorded: Any, served: Any, why: Any) -> None:
    assert generations_module._blocked_reason_code(recorded) == (served, why)
    assert generations_module._blocked_summary(recorded) == served


class _Cursor:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.description = [SimpleNamespace(name=name) for name in (
            "definition_id", "logical_key", "run_id", "target_generation_id",
            "status", "trigger_kind", "metrics", "error", "started_at",
            "completed_at", "row_count",
        )]

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def execute(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def fetchall(self) -> list[tuple]:
        return self._rows


def test_view_diagnostics_names_the_registry_file_and_masks_the_run_error(
    monkeypatch, tmp_path: Path,
) -> None:
    entries = [SimpleNamespace(view_def_id="leak_probe", view_name="Leak probe")]
    monkeypatch.setattr(view_diagnostics, "load_views_registry", lambda _path: entries)
    now = datetime(2026, 9, 22, tzinfo=timezone.utc)
    rows = [(
        "leak_probe", "leak_probe", "run-1", 9, "failed", "full", {},
        {"error_type": "ValueError", "message": LEAK_EXCEPTION, "traceback": LEAK_TRACEBACK},
        now, now, None,
    )]
    payload = view_diagnostics.compose_view_diagnostics(
        SimpleNamespace(cursor=lambda: _Cursor(rows)),
        deployment_name="w",
        deployment_dir=tmp_path / LEAK_HOME.lstrip("/"),
        session=SimpleNamespace(generation_id="gen:9"),
        selected_view="leak_probe",
    )
    assert hits(payload) == []
    assert payload["read_authority"]["registry"] == "views_registry.yaml"
    assert payload["read_authority"]["registry_path_unavailable_reason"] == "server_path_not_served"
    error = payload["views"][0]["latest_run"]["error"]
    assert error["error_type"] == "ValueError"
    assert error["error_text_withheld"] is True
    assert error["withheld_keys"] == ["message", "traceback"]


# --------------------------------------------------------------------------- #
# every console route's failure body                                           #
# --------------------------------------------------------------------------- #
class _LeakyPool:
    def connection(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(LEAK_EXCEPTION)

    def getconn(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(LEAK_EXCEPTION)


class _LeakyCtx:
    """A request context whose every connection raises leaky text."""

    def __init__(self, deployment_dir: Path) -> None:
        self.dsn = LEAK_DSN
        self.deployment_name = "leak-probe"
        self.deployment_dir = deployment_dir
        self.manifest: dict[str, Any] = {}
        self.config: dict[str, Any] = {}
        self.pool = _LeakyPool()
        self.session = SimpleNamespace(
            generation_id="okg:leak-probe:branch:default:gen:20260922T000000000000Z:abc",
            branch_name="default",
            deployment_name="leak-probe",
        )

    @contextmanager
    def conn(self) -> Iterator[Any]:
        raise RuntimeError(LEAK_EXCEPTION)
        yield  # pragma: no cover


FAILURE_STATE = {
    "attempt": "okg:leak-probe:branch:default:gen:20260922T000000000000Z:abc",
    "source": "leakprobe",
    "view": "leak_probe",
    "record": "document:leak-probe",
    "version": "okg:leak-probe:branch:default:gen:20260922T000000000000Z:abc",
}


def _raise_leak(*_args: Any, **_kwargs: Any) -> Any:
    raise RuntimeError(LEAK_TRACEBACK)


@pytest.mark.parametrize("provider_raises", [False, True], ids=["connection", "context"])
def test_no_console_failure_body_serves_exception_text(
    monkeypatch, tmp_path: Path, provider_raises: bool,
) -> None:
    """Every console GET route, failing, serves a code, a type and a sentence.

    Two passes. In the first every connection the context hands out raises
    text naming a path, a host, an address and a password. In the second the context itself cannot be built, which is the route every
    unexpected error takes to the app's own failure handler.
    """
    deployment_dir = tmp_path / LEAK_HOME.lstrip("/")
    deployment_dir.mkdir(parents=True)
    ctx = _LeakyCtx(deployment_dir)

    def provider() -> Any:
        if provider_raises:
            raise RuntimeError(LEAK_EXCEPTION)
        return ctx

    client = TestClient(
        build_app(ctx_provider=provider, is_ready=lambda: True),
        raise_server_exceptions=False,
    )
    leaked: dict[str, list[str]] = {}
    statuses: dict[str, int] = {}
    for route, recipe in sorted(RECIPES.items()):
        response = client.get(route, params=recipe(FAILURE_STATE))
        statuses[route] = response.status_code
        try:
            body: Any = response.json()
        except ValueError:
            body = response.text
        found = hits(body)
        if found:
            leaked[route] = found
    assert leaked == {}, leaked
    # Every route really failed: a 2xx here would mean the pass proved nothing
    # about that route's failure body.
    assert {route: code for route, code in statuses.items() if code < 400 and not route.endswith(
        ("source-diagnostics", "producers", "enricher-diagnostics"),
    )} == {}


def test_a_console_failure_body_has_the_one_shape(monkeypatch, tmp_path: Path) -> None:
    deployment_dir = tmp_path / "d"
    deployment_dir.mkdir()
    client = TestClient(
        build_app(ctx_provider=lambda: _LeakyCtx(deployment_dir), is_ready=lambda: True),
        raise_server_exceptions=False,
    )
    # A composer failure inside the route.
    monkeypatch.setattr(
        worktrees, "compose_worktrees", _raise_leak,
    )
    body = client.get("/viz/api/console/worktrees").json()
    # The connection raises before the composer runs; either way the body is
    # the app handler's or the route's, and both are the one shape.
    detail = body.get("detail", body)
    assert detail["error"] == READ_FAILED_SENTENCE
    assert detail["error_type"] == "RuntimeError"
    assert detail["error_text_unavailable_reason"] == "exception_text_not_served"
    assert isinstance(detail["reason"], str) and detail["reason"]


def test_producer_metrics_names_a_bad_parameter_and_serves_nothing_else(
    monkeypatch, tmp_path: Path,
) -> None:
    from okg.substrate.console_views import producer_metrics

    class _Ctx(_LeakyCtx):
        @contextmanager
        def conn(self) -> Iterator[Any]:
            yield SimpleNamespace()

    client = TestClient(
        build_app(ctx_provider=lambda: _Ctx(tmp_path), is_ready=lambda: True),
        raise_server_exceptions=False,
    )
    refused = client.get(
        "/viz/api/console/producer-metrics",
        params={"start": "2026-09-22T00:00:00Z", "end": "2026-09-21T00:00:00Z"},
    )
    assert refused.status_code == 400, refused.text
    detail = refused.json()["detail"]
    assert detail["reason"] == "producer_metrics_parameter_invalid"
    assert detail["parameter"] == "start"
    assert detail["error"] == "producer metric start must be before end"

    def bad_value(*_args: Any, **_kwargs: Any) -> Any:
        raise ValueError(f"cannot parse {LEAK_HOME}/deployments/x/views_registry.yaml")

    monkeypatch.setattr(producer_metrics, "compose_producer_metrics", bad_value)
    failed = client.get("/viz/api/console/producer-metrics")
    assert failed.status_code == 503, failed.text
    assert hits(failed.json()) == []
    assert failed.json()["detail"]["error_type"] == "ValueError"


def test_a_credential_named_setting_is_withheld_whatever_it_looks_like() -> None:
    from okg.substrate.console_views.served_text import CREDENTIAL_WITHHELD

    masked, trails = mask_served_text({"settings": {
        "password": "hunter2", "api_key": "abc", "dsn": "okg_workspace",
        "db_pass": "x", "token_count": 3, "passes": "two", "user": "okg",
    }})
    assert masked["settings"] == {
        "password": CREDENTIAL_WITHHELD, "api_key": CREDENTIAL_WITHHELD,
        "dsn": CREDENTIAL_WITHHELD, "db_pass": CREDENTIAL_WITHHELD,
        "token_count": 3, "passes": "two", "user": "okg",
    }
    assert sorted(trails) == [
        "settings.api_key", "settings.db_pass", "settings.dsn", "settings.password",
    ]


def test_source_diagnostics_producer_rows_serve_no_settings_secret() -> None:
    """The issue's second finding: a producer's settings served verbatim.

    The design fixture models it with a DSN inside `producers[].settings`;
    the composer copied the runtime rows whole.
    """
    from okg.substrate.console_views import source_diagnostics

    summary = source_diagnostics._producer_summary({"runtime_liveness": {
        "status": "degraded",
        "producers": [{
            "producer": "watch", "status": "failing",
            "settings": {"dsn": LEAK_DSN, "password": "hunter2", "root": f"{LEAK_HOME}/inbox"},
            "last_error": LEAK_TRACEBACK,
        }],
    }})
    assert hits(summary) == []
    assert summary["configured_count"] == 1
    row = summary["producers"][0]
    assert row["producer"] == "watch" and row["status"] == "failing"
    assert set(summary["masked_fields"]) == {
        "producers[0].settings.dsn", "producers[0].settings.password",
        "producers[0].settings.root", "producers[0].last_error",
    }
