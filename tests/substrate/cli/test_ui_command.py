"""`okg ui`: one command serves the built operator UI and its read API.

PACT okg-ui-command. The command resolves the deployment the way sibling
read commands do, refuses by name when no built frontend bundle exists, and
serves the bundle and `/viz/api/*` from one loopback-only process that
refuses every write method. No database is touched here: DSN resolution, the
background graph load and `uvicorn.run` are replaced with recorders, so the
tests prove the wiring and the refusals, not a live read.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.cli import main as cli_main
from okg.substrate.cli.dsn_authority import ReadDsnResolution

_INDEX_MARKER = "okg-ui-test-index"
_WRITE_METHODS = ("POST", "PUT", "PATCH", "DELETE")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "OKG_DSN",
        "OKG_DEPLOYMENT",
        "OKG_ENVIRONMENT_DESCRIPTOR",
        "OKG_OPERATOR_UI_DIST",
        "OKG_VIZ_OPERATOR_TOKEN",
        "OKG_VIZ_HOST",
        "OKG_VIZ_PORT",
        "OKG_AGENT",
        "OKG_AUTH_GATED_PORTS",
    ):
        # setenv first so monkeypatch records the original value and
        # restores it: the command writes several of these into os.environ.
        monkeypatch.setenv(name, "unset-by-test")
        monkeypatch.delenv(name)


def _make_bundle(dist: Path, marker: str) -> Path:
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text(
        f"<!doctype html><html><body>{marker}</body></html>",
        encoding="utf-8",
    )
    (dist / "assets" / "app.js").write_text("console.log('ui');\n", encoding="utf-8")
    return dist


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    return _make_bundle(tmp_path / "operator-ui-dist", _INDEX_MARKER)


@pytest.fixture
def recorders(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Replace every side effect the command has with a recorder."""
    seen: dict[str, Any] = {"resolve": [], "preload": [], "run": []}

    def fake_resolve(args: Any, *, surface: str) -> ReadDsnResolution:
        seen["resolve"].append({"deployment": args.deployment, "surface": surface})
        return ReadDsnResolution(
            dsn="postgresql://reader:******@127.0.0.1:1/fake",
            deployment=args.deployment,
            authority={"mode": "test"},
            target=None,
        )

    def fake_preload(**kwargs: Any) -> None:
        seen["preload"].append(kwargs)

    def fake_run(app: Any, **kwargs: Any) -> None:
        seen["run"].append({"app": app, **kwargs})

    import uvicorn

    from okg.substrate.cli import dsn_authority
    from okg.viz import boot

    monkeypatch.setattr(dsn_authority, "resolve_read_dsn", fake_resolve)
    monkeypatch.setattr(boot, "preload_in_background", fake_preload)
    monkeypatch.setattr(uvicorn, "run", fake_run)
    return seen


def _client(recorders: dict[str, Any]) -> Any:
    from fastapi.testclient import TestClient

    (run,) = recorders["run"]
    return TestClient(run["app"])


def test_ui_serves_bundle_and_read_api_from_one_process(
    bundle: Path, recorders: dict[str, Any]
) -> None:
    code = cli_main.main(
        [
            "ui",
            "--deployment",
            "demo",
            "--dist",
            str(bundle),
            "--generation",
            "7",
            "--port",
            "8799",
        ]
    )

    assert code == 0
    assert recorders["resolve"] == [{"deployment": "demo", "surface": "okg ui"}]
    assert len(recorders["preload"]) == 1
    preload = recorders["preload"][0]
    assert preload["deployment_name"] == "demo"
    assert preload["generation_id"] == "7"
    assert preload["dsn"].startswith("postgresql://")
    (run,) = recorders["run"]
    assert run["host"] == "127.0.0.1"
    assert run["port"] == 8799
    assert run["proxy_headers"] is False
    # A loopback-only server registers no auth-gated network port.
    assert "OKG_AUTH_GATED_PORTS" not in os.environ

    client = _client(recorders)
    index = client.get("/")
    assert index.status_code == 200
    assert _INDEX_MARKER in index.text
    asset = client.get("/assets/app.js")
    assert asset.status_code == 200
    assert "console.log" in asset.text
    health = client.get("/viz/api/health")
    assert health.status_code == 200
    assert health.json()["ready"] is False


def test_ui_front_door_refuses_every_write_method(
    bundle: Path, recorders: dict[str, Any]
) -> None:
    assert cli_main.main(["ui", "--deployment", "demo", "--dist", str(bundle)]) == 0
    client = _client(recorders)
    for method in _WRITE_METHODS:
        for path in ("/viz/api/coordination/approval/decide", "/", "/assets/app.js"):
            response = client.request(method, path, json={})
            assert response.status_code == 405, (method, path)
            assert response.headers["allow"] == "GET, HEAD, OPTIONS"
            assert response.json()["error"]["code"] == "operator_ui_read_only"


def test_ui_head_is_served_for_the_static_bundle_only(
    bundle: Path, recorders: dict[str, Any]
) -> None:
    assert cli_main.main(["ui", "--deployment", "demo", "--dist", str(bundle)]) == 0
    client = _client(recorders)

    page = client.head("/")
    assert page.status_code == 200
    assert page.content == b""
    assert client.head("/assets/app.js").status_code == 200
    # The read API's GET routes do not accept HEAD; FastAPI answers its own
    # 405, which is what the docs and the PACT requirement state.
    api = client.head("/viz/api/health")
    assert api.status_code == 405


class _WriteServiceRecorder:
    """Stands in for every write service; records any use at all."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def __getattr__(self, name: str) -> Any:
        self._calls.append(name)

        def _called(*args: Any, **kwargs: Any) -> Any:
            self._calls.append(f"{name}()")
            raise AssertionError(f"write service used: {name}")

        return _called


def test_ui_options_reaches_cors_preflight_but_never_a_write_handler(
    bundle: Path,
) -> None:
    from fastapi.testclient import TestClient

    from okg.viz.app import build_app
    from okg.viz.auth import OPERATOR_TOKEN_HEADER
    from okg.viz.ui_server import build_ui_app

    calls: list[str] = []
    recorder = _WriteServiceRecorder(calls)

    def provider() -> Any:
        raise AssertionError("no graph read is expected")

    api_app = build_app(
        provider,
        is_ready=lambda: False,
        # A configured token proves the front door, not the token check,
        # keeps writes out: the request below carries the right token.
        operator_token="test-operator-token",
        operator_control_service=recorder,  # type: ignore[arg-type]
        approval_control_service=recorder,  # type: ignore[arg-type]
        setup_plan_service=recorder,  # type: ignore[arg-type]
    )
    client = TestClient(build_ui_app(api_app, bundle))
    write_path = "/viz/api/coordination/approval/decide"
    auth = {OPERATOR_TOKEN_HEADER: "test-operator-token"}

    preflight = client.options(
        write_path,
        headers={
            **auth,
            "Origin": "http://127.0.0.1:5173",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == "http://127.0.0.1:5173"
    # A plain OPTIONS also reaches the read API, which answers its own 405:
    # the route serves POST only, so no handler runs.
    plain = client.options(write_path, headers=auth)
    assert plain.status_code == 405
    assert plain.json() == {"detail": "Method Not Allowed"}
    for method in _WRITE_METHODS:
        refused = client.request(method, write_path, headers=auth, json={})
        assert refused.status_code == 405, method
        assert refused.headers["allow"] == "GET, HEAD, OPTIONS"
        assert refused.json()["error"]["code"] == "operator_ui_read_only"
    assert calls == []

    # Control: called directly, the read API accepts this token on this
    # route (422 for the empty body, not 401 or 405), so the front door is
    # the only thing that kept the writes above out.
    direct = TestClient(api_app).post(write_path, headers=auth, json={})
    assert direct.status_code == 422
    assert calls == []


def test_ui_refuses_by_name_when_no_bundle_was_built(
    tmp_path: Path,
    recorders: dict[str, Any],
    capsys: pytest.CaptureFixture[str],
) -> None:
    missing = tmp_path / "never-built"

    code = cli_main.main(["ui", "--deployment", "demo", "--dist", str(missing)])

    assert code == 1
    err = capsys.readouterr().err
    assert "operator_ui_bundle_missing" in err
    assert str(missing) in err
    assert "npm run operator-ui:build" in err
    assert recorders["run"] == []
    assert recorders["preload"] == []


def test_ui_refuses_a_bundle_directory_without_index_html(
    bundle: Path,
    recorders: dict[str, Any],
    capsys: pytest.CaptureFixture[str],
) -> None:
    (bundle / "index.html").unlink()

    code = cli_main.main(["ui", "--deployment", "demo", "--dist", str(bundle)])

    assert code == 1
    err = capsys.readouterr().err
    assert "operator_ui_bundle_missing" in err
    assert "index.html" in err
    assert recorders["run"] == []


def test_ui_serves_the_bundle_named_by_the_environment(
    tmp_path: Path,
    recorders: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_bundle = _make_bundle(tmp_path / "env-bundle", "marker-from-env-bundle")
    monkeypatch.setenv("OKG_OPERATOR_UI_DIST", str(env_bundle))

    assert cli_main.main(["ui", "--deployment", "demo"]) == 0
    page = _client(recorders).get("/")
    assert page.status_code == 200
    assert "marker-from-env-bundle" in page.text


def test_ui_dist_flag_wins_over_the_environment(
    tmp_path: Path,
    recorders: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_bundle = _make_bundle(tmp_path / "env-bundle", "marker-from-env-bundle")
    flag_bundle = _make_bundle(tmp_path / "flag-bundle", "marker-from-flag-bundle")
    monkeypatch.setenv("OKG_OPERATOR_UI_DIST", str(env_bundle))

    assert (
        cli_main.main(["ui", "--deployment", "demo", "--dist", str(flag_bundle)]) == 0
    )
    page = _client(recorders).get("/")
    assert "marker-from-flag-bundle" in page.text
    assert "marker-from-env-bundle" not in page.text


def test_ui_refuses_without_a_deployment_name(
    bundle: Path,
    recorders: dict[str, Any],
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = cli_main.main(
        [
            "ui",
            "--dsn",
            "postgresql://u@127.0.0.1:1/x",
            "--dist",
            str(bundle),
        ]
    )

    assert code == 1
    err = capsys.readouterr().err
    assert "--deployment" in err
    assert recorders["run"] == []
    assert recorders["preload"] == []


@pytest.mark.parametrize(
    "host", ["0.0.0.0", "::", "::ffff:127.0.0.1", "192.168.1.10", "example.org"]
)
@pytest.mark.parametrize("token", [None, "a-configured-operator-token"])
def test_ui_refuses_every_non_loopback_bind_even_with_a_token(
    host: str,
    token: str | None,
    bundle: Path,
    recorders: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    if token is not None:
        monkeypatch.setenv("OKG_VIZ_OPERATOR_TOKEN", token)

    code = cli_main.main(
        ["ui", "--deployment", "demo", "--dist", str(bundle), "--host", host]
    )

    assert code == 1
    err = capsys.readouterr().err
    assert "operator_ui_bind_refused" in err
    assert "ssh" in err.lower()
    assert recorders["run"] == []
    assert recorders["preload"] == []
    assert "OKG_AUTH_GATED_PORTS" not in os.environ


@pytest.mark.parametrize("host", ["127.0.0.1", "::1", "localhost"])
def test_ui_accepts_loopback_hosts(
    host: str, bundle: Path, recorders: dict[str, Any]
) -> None:
    code = cli_main.main(
        ["ui", "--deployment", "demo", "--dist", str(bundle), "--host", host]
    )

    assert code == 0
    assert recorders["run"][0]["host"] == host


@pytest.mark.parametrize(
    ("argv", "env_port"),
    [
        (["--port", "0"], None),
        (["--port", "70000"], None),
        ([], "not-a-port"),
        ([], "0"),
    ],
)
def test_ui_refuses_an_unusable_port_with_a_named_error(
    argv: list[str],
    env_port: str | None,
    bundle: Path,
    recorders: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    if env_port is not None:
        monkeypatch.setenv("OKG_VIZ_PORT", env_port)

    code = cli_main.main(["ui", "--deployment", "demo", "--dist", str(bundle), *argv])

    assert code == 1
    err = capsys.readouterr().err
    assert "operator_ui_bind_refused" in err
    assert "Traceback" not in err
    assert recorders["run"] == []
    assert recorders["preload"] == []


# ---- --auth-token-env (issue #3278): a shared host's other accounts ----

_UI_TOKEN_ENV = "OKG_TEST_UI_TOKEN"
_UI_TOKEN = "ui-read-token-0123456789abcdef"


def _basic_auth(password: str) -> dict[str, str]:
    import base64

    raw = base64.b64encode(f"operator:{password}".encode()).decode()
    return {"Authorization": f"Basic {raw}"}


def test_ui_without_the_token_answers_nothing_but_a_sign_in_challenge(
    bundle: Path, recorders: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(_UI_TOKEN_ENV, _UI_TOKEN)
    code = cli_main.main(
        ["ui", "--deployment", "demo", "--dist", str(bundle),
         "--auth-token-env", _UI_TOKEN_ENV]
    )
    assert code == 0
    client = _client(recorders)
    for path in ("/", "/assets/app.js", "/viz/api/health", "/viz/api/deployment"):
        denied = client.get(path)
        assert denied.status_code == 401, path
        # A Basic challenge makes the browser ask for the token itself.
        assert denied.headers["www-authenticate"].startswith('Basic realm="okg ui"')
        assert _INDEX_MARKER not in denied.text
    wrong = client.get("/", headers=_basic_auth("wrong"))
    assert wrong.status_code == 401
    # Writes are refused without revealing anything either.
    assert client.post("/viz/api/repin", json={}).status_code == 401


def test_ui_with_the_token_serves_the_page_and_the_read_api(
    bundle: Path, recorders: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(_UI_TOKEN_ENV, _UI_TOKEN)
    code = cli_main.main(
        ["ui", "--deployment", "demo", "--dist", str(bundle),
         "--auth-token-env", _UI_TOKEN_ENV]
    )
    assert code == 0
    client = _client(recorders)
    auth = _basic_auth(_UI_TOKEN)
    index = client.get("/", headers=auth)
    assert index.status_code == 200
    assert _INDEX_MARKER in index.text
    assert client.get("/assets/app.js", headers=auth).status_code == 200
    health = client.get("/viz/api/health", headers=auth)
    assert health.status_code == 200
    assert health.json()["ready"] is False
    bearer = client.get(
        "/viz/api/health", headers={"Authorization": f"Bearer {_UI_TOKEN}"}
    )
    assert bearer.status_code == 200
    # Still read-only behind the token.
    assert client.post("/viz/api/repin", headers=auth, json={}).status_code == 405


def test_ui_refuses_an_unset_token_variable_before_serving(
    bundle: Path,
    recorders: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv(_UI_TOKEN_ENV, raising=False)
    code = cli_main.main(
        ["ui", "--deployment", "demo", "--dist", str(bundle),
         "--auth-token-env", _UI_TOKEN_ENV]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert _UI_TOKEN_ENV in err
    assert "Traceback" not in err
    assert recorders["run"] == []
    assert recorders["preload"] == []


def test_ui_refuses_a_sensitive_deployment_without_a_token(
    bundle: Path,
    recorders: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "deployments"
    (root / "sens-dep").mkdir(parents=True)
    (root / "sens-dep" / "deployment.yaml").write_text(
        "name: sens-dep\nnomos:\n  runtime_enforcement: enforce\n", encoding="utf-8"
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)

    code = cli_main.main(["ui", "--deployment", "sens-dep", "--dist", str(bundle)])
    assert code == 1
    err = capsys.readouterr().err
    assert "--auth-token-env" in err
    assert "sens-dep" in err
    assert "Traceback" not in err
    assert recorders["run"] == []
    assert recorders["preload"] == []

    # Same deployment, token configured: it serves.
    monkeypatch.setenv(_UI_TOKEN_ENV, _UI_TOKEN)
    code = cli_main.main(
        ["ui", "--deployment", "sens-dep", "--dist", str(bundle),
         "--auth-token-env", _UI_TOKEN_ENV]
    )
    assert code == 0
    assert len(recorders["run"]) == 1
