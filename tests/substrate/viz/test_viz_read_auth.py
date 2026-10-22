"""Read-API authentication for the viz server (issue #3278).

Before: every GET under ``/viz/api/*`` answered anyone who could reach the
port. ``OKG_VIZ_OPERATOR_TOKEN`` gated only the mutating POST routes, and the
launcher accepted a non-loopback bind once that token was set, so graph reads
went to the network unauthenticated. On a shared host even a loopback bind is
reachable by every local account.

After: ``--auth-token-env VARIABLE_NAME`` (the ``okg mcp-serve`` flag and
resolver) makes every ``/viz/api/*`` request present the token, and a
non-loopback bind refuses to start without it. No database is needed: the
routes used here answer from the in-process context.
"""
from __future__ import annotations

import base64
import os
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.viz import auth as viz_auth  # noqa: E402
from okg.viz.app import build_app  # noqa: E402
from okg.viz.read_cache import make_etag  # noqa: E402

_TOKEN = "viz-read-token-0123456789abcdef"
_DEPLOYMENT = "private-deployment-name"
# A field only the deployment payload carries: its presence means data was
# served, its absence that the gate answered instead.
_PAYLOAD_KEY = "viz_theme"


class _Ctx:
    deployment_name = _DEPLOYMENT
    manifest = None
    session = SimpleNamespace(generation_id=7)


def _app(**kwargs: Any) -> Any:
    return build_app(ctx_provider=_Ctx, is_ready=lambda: True, **kwargs)


def _basic(password: str, user: str = "operator") -> dict[str, str]:
    raw = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {raw}"}


_BEARER = {"Authorization": f"Bearer {_TOKEN}"}

# Data and metadata routes that need no database with the context above.
_READ_PATHS = (
    "/viz/api/deployment",
    "/viz/api/deployments",
    "/viz/api/tools",
    "/viz/api/health",
    "/viz/api/openapi.json",
    "/viz/api/docs",
)


# ---- the gap on dev: reads answer without any credential ----


def test_without_a_read_token_reads_stay_open_as_before() -> None:
    # Control: no --auth-token-env keeps today's behavior on loopback.
    client = TestClient(_app())
    resp = client.get("/viz/api/deployment")
    assert resp.status_code == 200
    assert _PAYLOAD_KEY in resp.json()


# ---- with a token: every /viz/api/* request is gated ----


@pytest.mark.parametrize("path", _READ_PATHS)
@pytest.mark.parametrize("peer", [("127.0.0.1", 40000), ("10.0.0.9", 51111)])
def test_unauthenticated_read_is_refused(path: str, peer: tuple[str, int]) -> None:
    client = TestClient(_app(api_token=_TOKEN), client=peer)
    resp = client.get(path)
    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == 'Bearer realm="okg-viz"'
    assert resp.json() == {
        "ok": False,
        "error": "authentication required",
        "path": path,
    }
    assert _PAYLOAD_KEY not in resp.text


def test_operator_token_does_not_open_reads() -> None:
    # The mutation credential is not a read credential.
    client = TestClient(_app(api_token=_TOKEN, operator_token="operator-tok"))
    resp = client.get(
        "/viz/api/deployment",
        headers={viz_auth.OPERATOR_TOKEN_HEADER: "operator-tok"},
    )
    assert resp.status_code == 401


@pytest.mark.parametrize(
    "header",
    [
        "Bearer wrong-token",
        f"Bearer {_TOKEN}x",
        "Bearer",
        "Bearer ",
        f"Token {_TOKEN}",
        _TOKEN,
        "Basic !!!not-base64!!!",
        "Basic " + base64.b64encode(_TOKEN.encode()).decode(),  # no colon
        _basic("wrong")["Authorization"],
        _basic("")["Authorization"],
        "Bearer éè-non-ascii",
    ],
)
def test_wrong_or_malformed_credentials_are_refused_not_errors(header: str) -> None:
    client = TestClient(_app(api_token=_TOKEN), raise_server_exceptions=True)
    resp = client.get(
        "/viz/api/deployment",
        # Raw bytes, so a non-ASCII credential reaches the server as sent.
        headers={"Authorization": header.encode("utf-8")},
    )
    assert resp.status_code == 401


def test_bearer_token_reads_data() -> None:
    client = TestClient(_app(api_token=_TOKEN), client=("10.0.0.9", 51111))
    resp = client.get("/viz/api/deployment", headers=_BEARER)
    assert resp.status_code == 200
    assert _PAYLOAD_KEY in resp.json()


def test_basic_credential_with_the_token_as_password_reads_data() -> None:
    client = TestClient(_app(api_token=_TOKEN))
    resp = client.get("/viz/api/deployment", headers=_basic(_TOKEN, user="anyone"))
    assert resp.status_code == 200
    assert _PAYLOAD_KEY in resp.json()


def test_a_cache_revalidation_cannot_answer_an_unauthenticated_caller() -> None:
    # The read cache answers 304 for a matching ETag before any route runs.
    # The auth gate must sit outside it, or the cache would leak existence and
    # generation of a cached read without the token.
    path = "/viz/api/console/health"
    etag = make_etag(path, "", 7)
    client = TestClient(_app(api_token=_TOKEN))
    denied = client.get(path, headers={"If-None-Match": etag})
    assert denied.status_code == 401
    allowed = client.get(path, headers={"If-None-Match": etag, **_BEARER})
    assert allowed.status_code == 304


def test_mutations_still_need_the_operator_token_too() -> None:
    client = TestClient(_app(api_token=_TOKEN, operator_token="operator-tok"))
    resp = client.post("/viz/api/repin", headers=_BEARER, json={})
    assert resp.status_code == 401
    assert "operator authentication required" in resp.text


def test_non_api_paths_carry_no_data_and_are_not_gated() -> None:
    client = TestClient(_app(api_token=_TOKEN))
    resp = client.get("/")
    assert resp.status_code == 410
    assert _PAYLOAD_KEY not in resp.text


def test_cors_preflight_still_answers_so_a_browser_can_read_the_401() -> None:
    client = TestClient(_app(api_token=_TOKEN))
    resp = client.options(
        "/viz/api/deployment",
        headers={
            "Origin": "http://127.0.0.1:5173",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp.status_code == 200


def test_empty_token_is_refused_at_build_time() -> None:
    with pytest.raises(ValueError):
        _app(api_token="")


# ---- token resolution: the mcp-serve rules, viz wording, no value leak ----


def test_no_flag_means_no_token() -> None:
    assert viz_auth.resolve_api_token(None) is None


def test_named_variable_resolves(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_TEST_VIZ_READ_TOKEN", _TOKEN)
    assert viz_auth.resolve_api_token("OKG_TEST_VIZ_READ_TOKEN") == _TOKEN


@pytest.mark.parametrize(
    ("value", "reason"),
    [(None, "unset"), ("", "empty"), ("   ", "empty"), (f"{_TOKEN}\n", "padded")],
)
def test_unusable_variable_refuses(
    monkeypatch: pytest.MonkeyPatch, value: str | None, reason: str
) -> None:
    name = "OKG_TEST_VIZ_READ_TOKEN"
    if value is None:
        monkeypatch.delenv(name, raising=False)
    else:
        monkeypatch.setenv(name, value)
    with pytest.raises(SubstrateError) as info:
        viz_auth.resolve_api_token(name)
    assert info.value.data["reason"] == reason
    assert "viz read API bearer authentication" in str(info.value)
    assert _TOKEN not in str(info.value)


def test_a_pasted_token_value_is_withheld_from_the_refusal() -> None:
    with pytest.raises(SubstrateError) as info:
        viz_auth.resolve_api_token(f"${_TOKEN}")
    assert _TOKEN not in str(info.value)
    assert info.value.data["auth_token_env"] == "<withheld>"


# ---- launcher: fail closed on a network bind ----


def test_launcher_refuses_network_bind_with_only_the_operator_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # On dev this bind was accepted and declared auth-gated, yet every read
    # route answered the network without a credential.
    from okg.substrate.cli.connector_security import AUTH_GATED_PORTS_ENV
    from okg.viz.__main__ import _register_auth_gated_bind

    monkeypatch.delenv(AUTH_GATED_PORTS_ENV, raising=False)
    with pytest.raises(SystemExit) as info:
        _register_auth_gated_bind("0.0.0.0", 8765, "operator-tok")
    assert "--auth-token-env" in str(info.value)
    assert "8765" not in (os.environ.get(AUTH_GATED_PORTS_ENV) or "")


def test_launcher_accepts_network_bind_with_both_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli.connector_security import AUTH_GATED_PORTS_ENV
    from okg.viz.__main__ import _register_auth_gated_bind

    monkeypatch.delenv(AUTH_GATED_PORTS_ENV, raising=False)
    _register_auth_gated_bind("0.0.0.0", 8765, "operator-tok", api_token=_TOKEN)
    assert "8765" in (os.environ.get(AUTH_GATED_PORTS_ENV) or "")


def _patch_launcher(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    import uvicorn

    from okg.viz import app as viz_app
    from okg.viz import boot as viz_boot
    from okg.viz import coordination_services

    seen: dict[str, Any] = {"preload": 0, "run": 0}

    def preload(**_: Any) -> None:
        seen["preload"] += 1

    def fake_build_app(*_args: Any, **kwargs: Any) -> object:
        seen["build_app"] = kwargs
        return object()

    def fake_run(*_args: Any, **_kwargs: Any) -> None:
        seen["run"] += 1

    monkeypatch.setattr(viz_boot, "get_ctx_lazy", lambda: (lambda: None, lambda: True))
    monkeypatch.setattr(viz_boot, "preload_in_background", preload)
    monkeypatch.setattr(viz_app, "build_app", fake_build_app)
    monkeypatch.setattr(uvicorn, "run", fake_run)
    monkeypatch.setattr(
        coordination_services,
        "build_native_coordination_services",
        lambda **_: SimpleNamespace(
            operator_control_service=None,
            approval_control_service=None,
            setup_plan_service=None,
        ),
    )
    for name in ("OKG_VIZ_HOST", "OKG_VIZ_PORT", "OKG_DSN", "OKG_DEPLOYMENT"):
        monkeypatch.setenv(name, "unset-by-test")
        monkeypatch.delenv(name)
    monkeypatch.delenv("OKG_VIZ_OPERATOR_TOKEN", raising=False)
    return seen


def test_launcher_refuses_an_unset_token_variable_before_any_load(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from okg.viz import __main__ as viz_main

    seen = _patch_launcher(monkeypatch)
    monkeypatch.delenv("OKG_TEST_VIZ_READ_TOKEN", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        ["okg.viz", "--dsn", "postgresql://fake/okg",
         "--auth-token-env", "OKG_TEST_VIZ_READ_TOKEN"],
    )
    assert viz_main.main() == 1
    assert "OKG_TEST_VIZ_READ_TOKEN" in capsys.readouterr().err
    assert seen["preload"] == 0
    assert seen["run"] == 0


def test_launcher_passes_the_resolved_token_to_the_app(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from okg.viz import __main__ as viz_main

    seen = _patch_launcher(monkeypatch)
    monkeypatch.setenv("OKG_TEST_VIZ_READ_TOKEN", _TOKEN)
    monkeypatch.setattr(
        "sys.argv",
        ["okg.viz", "--dsn", "postgresql://fake/okg",
         "--auth-token-env", "OKG_TEST_VIZ_READ_TOKEN"],
    )
    assert viz_main.main() == 0
    assert seen["build_app"]["api_token"] == _TOKEN
    assert seen["run"] == 1
    out = capsys.readouterr()
    # The banner names the variable, never the value.
    assert "$OKG_TEST_VIZ_READ_TOKEN" in out.err
    assert _TOKEN not in out.err + out.out


# ---- follow-ups (issue #3278): sensitive deployments, mount_viz ----

_SENSITIVE_MODES = ("audit", "enforce")


def _deployments_root(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, name: str, nomos: str,
) -> None:
    """One deployment under a private OKG_DEPLOYMENTS_DIR with a nomos block."""
    root = tmp_path / "deployments"
    (root / name).mkdir(parents=True)
    (root / name / "deployment.yaml").write_text(
        f"name: {name}\n{nomos}", encoding="utf-8"
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)


@pytest.mark.parametrize("mode", _SENSITIVE_MODES)
def test_sensitive_deployment_refuses_an_unauthenticated_start_on_loopback(
    mode: str, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _deployments_root(
        tmp_path, monkeypatch, "sens-dep", f"nomos:\n  runtime_enforcement: {mode}\n"
    )
    with pytest.raises(SubstrateError) as info:
        viz_auth.require_read_auth_for_sensitive_deployment(
            "sens-dep", None, surface="okg.viz"
        )
    assert "--auth-token-env" in str(info.value)
    assert "sens-dep" in str(info.value)
    # The refusal carries its registered code (docs/reference/error_codes.yaml).
    assert info.value.code == "viz_read_auth_required"
    assert info.value.data["reason"] == "sensitive_deployment"
    # With a token the same deployment starts.
    viz_auth.require_read_auth_for_sensitive_deployment(
        "sens-dep", _TOKEN, surface="okg.viz"
    )


def test_deployment_without_nomos_enforcement_starts_unauthenticated(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _deployments_root(
        tmp_path, monkeypatch, "plain-dep", "nomos:\n  runtime_enforcement: off\n"
    )
    viz_auth.require_read_auth_for_sensitive_deployment(
        "plain-dep", None, surface="okg.viz"
    )


def test_kill_switch_override_does_not_remove_the_token_requirement(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The requirement follows the manifest's mode, not the env override
    (Jason decision 29, pending); one warning names the variable."""
    import logging

    import okg.substrate.nomos.runtime_enforcement as enforcement

    _deployments_root(
        tmp_path, monkeypatch, "sens-dep", "nomos:\n  runtime_enforcement: enforce\n"
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.setattr(enforcement, "_READ_AUTH_OVERRIDE_WARNED", False)
    with caplog.at_level(logging.WARNING, logger=enforcement.__name__):
        for _ in range(2):
            with pytest.raises(SubstrateError) as info:
                viz_auth.require_read_auth_for_sensitive_deployment(
                    "sens-dep", None, surface="okg.viz"
                )
            assert info.value.code == "viz_read_auth_required"
    warnings = [
        r for r in caplog.records if "OKG_NOMOS_RUNTIME_ENFORCEMENT" in r.getMessage()
    ]
    assert len(warnings) == 1
    # A token still starts it.
    viz_auth.require_read_auth_for_sensitive_deployment(
        "sens-dep", _TOKEN, surface="okg.viz"
    )


def test_override_cannot_make_a_manifest_off_deployment_sensitive(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _deployments_root(
        tmp_path, monkeypatch, "plain-dep", "nomos:\n  runtime_enforcement: off\n"
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    viz_auth.require_read_auth_for_sensitive_deployment(
        "plain-dep", None, surface="okg.viz"
    )


def test_unreadable_manifest_refuses_instead_of_assuming_not_sensitive(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _deployments_root(tmp_path, monkeypatch, "bad-dep", "nomos: [unclosed\n")
    with pytest.raises(SubstrateError) as info:
        viz_auth.require_read_auth_for_sensitive_deployment(
            "bad-dep", None, surface="okg.viz"
        )
    assert "cannot tell" in str(info.value)
    assert info.value.code == "viz_read_auth_required"
    assert info.value.data["reason"] == "sensitivity_unknown"


def test_launcher_refuses_a_sensitive_deployment_before_any_load(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.viz import __main__ as viz_main

    seen = _patch_launcher(monkeypatch)
    _deployments_root(
        tmp_path, monkeypatch, "sens-dep", "nomos:\n  runtime_enforcement: enforce\n"
    )
    monkeypatch.setattr(
        "sys.argv",
        ["okg.viz", "--dsn", "postgresql://fake/okg", "--deployment", "sens-dep"],
    )
    assert viz_main.main() == 1
    assert "--auth-token-env" in capsys.readouterr().err
    assert seen["preload"] == 0
    assert seen["run"] == 0

    # The same deployment starts once a token is configured.
    monkeypatch.setenv("OKG_TEST_VIZ_READ_TOKEN", _TOKEN)
    monkeypatch.setattr(
        "sys.argv",
        ["okg.viz", "--dsn", "postgresql://fake/okg", "--deployment", "sens-dep",
         "--auth-token-env", "OKG_TEST_VIZ_READ_TOKEN"],
    )
    assert viz_main.main() == 0
    assert seen["run"] == 1


def test_mount_viz_refuses_a_network_host_without_a_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import uvicorn

    from okg.viz.app import mount_viz

    started: list[Any] = []
    monkeypatch.setattr(uvicorn, "Server", lambda *_a, **_k: started.append(1))
    with pytest.raises(SubstrateError) as info:
        mount_viz(lambda: None, port=18999, host="0.0.0.0")
    assert "api_token" in str(info.value)
    assert started == []


def test_mount_viz_passes_the_token_to_the_app(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import uvicorn

    from okg.viz import app as viz_app

    seen: dict[str, Any] = {}

    def fake_build_app(*_a: Any, **kwargs: Any) -> object:
        seen.update(kwargs)
        return object()

    class _Server:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            pass

        async def serve(self) -> None:
            return None

    monkeypatch.setattr(viz_app, "build_app", fake_build_app)
    monkeypatch.setattr(uvicorn, "Server", _Server)
    thread = viz_app.mount_viz(
        lambda: None, port=18999, host="0.0.0.0", api_token=_TOKEN
    )
    assert thread is not None
    thread.join(timeout=5)
    assert seen["api_token"] == _TOKEN
