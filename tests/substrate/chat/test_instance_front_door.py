"""The single-operator front door (PACT ``chat-instance-lifecycle``,
``req.chat-instance-lifecycle.front-door-proxy``).

Two artifacts, one plan. :func:`render_nginx_front_door` produces the file an
operator deploys; :func:`serve_front_door` runs the same route table so the
behaviour can be exercised on a machine with no nginx binary.
:func:`routes_agree` is asserted here so "two renderers over one plan" stays a
checkable claim rather than a hope, and the LIVE arm serves the nginx file
itself and is recorded as an ``operator_report``.

Why the assertions are on CONTENT and not on status codes
---------------------------------------------------------
A 200 from a reverse proxy is satisfied by the proxy's own default page, its
error page, and by anything else that happens to be on the upstream. So the
root arm asserts on a marker the instance puts in its own index, the API arm
asserts on a body the instance produced, and the whole thing is bracketed by
a control that STOPS the instance and requires the same two assertions to
FAIL. Without that control, a proxy serving its own page would pass every
positive arm in this file.

Why an https site base is a refusal rather than a listener
----------------------------------------------------------
The listen port is DERIVED from ``chat_site_base``'s scheme, so an ``https``
base rendered ``listen 443;`` with no ``ssl`` directive, no certificate and no
key, in front of a proxy that performs no authentication. The arms below prove
the two ways out — a COMPLETE TLS block from operator-supplied paths, or a
refusal naming the two missing inputs — and prove the agreement check that
binds them by MUTATION, striking the certificate out of a configuration that
had one and requiring the check to say so.

Why the console path answers rather than routing, and when it routes
--------------------------------------------------------------------
The ``/console`` prefix was measured negative at the console build the
read-handshake spike used: assets were requested at the origin root, 404, and
the app never booted while the front answered 200 with a blank page
(``evidence/read-handshake-transcript.md:31``, ``:350-354``). With no evidence
about a bundle, or with a bundle measured to have that defect, nothing here
serves the console. Nor is the location simply omitted — omitted, ``/console``
falls through to the catch-all, reaches the chat instance, and renders Open
WebUI's single-page index: a 200 that looks exactly like a console that
failed to load. The location returns a sentence naming
``operator-ui-base-aware-build``, and the arm asserts on that sentence.

A bundle MEASURED base-aware is a different fact, and the arms below prove the
front door acts on it: the verdict was computed and published in the plan all
along while all three renderers ignored it, so a console that would have
booted was 503'd exactly like one that would not. The permitted arms assert on
content served by a console upstream and on the forwarded path keeping its
prefix — a base-aware bundle asks for ``/console/…``, so an upstream reached
with the prefix stripped is the same 404 storm from the other end.
"""

from __future__ import annotations

import contextlib
import http.server
import threading
from dataclasses import replace

import pytest

from okg.substrate.chat.front_door import (
    CONSOLE_NOT_CONFIGURED_STATUS,
    CONSOLE_OWNER_PACT,
    CONSOLE_PREFIX,
    SINGLE_OPERATOR_HEADER_MARKER,
    TlsMaterial,
    console_blocks,
    fetch,
    front_door_route_table,
    plan_front_door,
    render_nginx_front_door,
    routes_agree,
    serve_front_door,
)
from okg.substrate.chat.instance import (
    PINNED_VERSION,
    ChatInstanceError,
    container_name_for,
    probe_version,
    stand_up,
)
from okg.substrate.chat.sso import ConsoleBuild

from .instance_harness import (
    ProcessRuntime,
    chat_block,
    env,
    free_port,
    reachable_probe,
    wait_until,
)

DEPLOYMENT = "fixture-deployment"
GRAPH_DSN = "postgresql://okg:pw@127.0.0.1:5432/okg_fixture_graph"
APP_DSN = "postgresql://okg:pw@127.0.0.1:5432/okg_fixture_chat_app"
APP_ENV = "OKG_CHAT_APP_DATABASE_URL"
SECRET_ENV = "OKG_CHAT_WEBUI_SECRET_KEY"


@pytest.fixture
def runtime():
    made = ProcessRuntime()
    try:
        yield made
    finally:
        made.shutdown()


@pytest.fixture
def standing(runtime, tmp_path):
    """A real instance behind a real front door, both on real sockets."""
    port = free_port()
    report = stand_up(
        DEPLOYMENT,
        chat_block=chat_block(),
        graph_dsn=GRAPH_DSN,
        runtime=runtime,
        environ=env(**{APP_ENV: APP_DSN, SECRET_ENV: "fixture-key"}),
        instance_port_override=port,
        state_root=tmp_path,
        reachability_probe=reachable_probe,
        ready_timeout=20.0,
        ready_interval=0.05,
    )
    plan = plan_front_door(
        DEPLOYMENT, chat_block=chat_block(), instance_port=port,
    )
    with contextlib.closing(serve_front_door(plan)) as server:
        yield report, plan, server


# ---------------------------------------------------------------------------
# Behaviour, through a real proxy in front of a real instance
# ---------------------------------------------------------------------------

def test_front_door_root_serves_the_instance_index_by_content(standing):
    """The root is the instance's own index, identified by a marker in it."""
    _report, _plan, server = standing

    status, body = fetch(f"{server.base_url}/")

    assert status == 200
    assert f'data-okg-instance="{DEPLOYMENT}"' in body
    assert "<title>Open WebUI</title>" in body


def test_front_door_chat_api_path_reaches_the_instance(standing):
    """A chat API path routed through the proxy is answered by the instance."""
    _report, _plan, server = standing

    status, body = fetch(f"{server.base_url}/api/v1/models/export")

    assert status == 200
    assert DEPLOYMENT in body
    assert PINNED_VERSION in body

    # And the version endpoint answers the pinned version THROUGH the proxy.
    assert probe_version(server.base_url) == PINNED_VERSION


def test_front_door_assertions_fail_when_the_instance_is_stopped(
    standing, runtime,
):
    """The control that makes the two arms above non-vacuous.

    With the instance stopped, the front door still answers — that is what a
    reverse proxy does — but nothing it returns carries the instance's marker
    or its version. If a proxy default page could satisfy the positive arms,
    it would satisfy them here too.
    """
    _report, _plan, server = standing
    assert f'data-okg-instance="{DEPLOYMENT}"' in fetch(f"{server.base_url}/")[1]

    runtime.kill_instance(container_name_for(DEPLOYMENT))
    assert wait_until(lambda: probe_version(server.base_url) is None)

    status, body = fetch(f"{server.base_url}/")
    assert f'data-okg-instance="{DEPLOYMENT}"' not in body
    assert "<title>Open WebUI</title>" not in body
    assert status != 200

    api_status, api_body = fetch(f"{server.base_url}/api/v1/models/export")
    assert PINNED_VERSION not in api_body
    assert api_status != 200


def test_console_path_returns_an_explicit_not_configured_response(standing):
    """A sentence naming the owning PACT, never a blank page or a chat index."""
    _report, _plan, server = standing

    status, body = fetch(f"{server.base_url}{CONSOLE_PREFIX}")

    assert status == CONSOLE_NOT_CONFIGURED_STATUS
    assert CONSOLE_OWNER_PACT in body
    assert "not configured" in body.lower()
    # Explicitly NOT the chat site's index, which is what an omitted location
    # would have produced.
    assert "<title>Open WebUI</title>" not in body
    assert body.strip(), "an empty body is the blank page this arm excludes"

    # And a path UNDER the prefix behaves the same way.
    sub_status, sub_body = fetch(f"{server.base_url}{CONSOLE_PREFIX}/#/instances")
    assert sub_status == CONSOLE_NOT_CONFIGURED_STATUS
    assert CONSOLE_OWNER_PACT in sub_body


# ---------------------------------------------------------------------------
# The generated configuration
# ---------------------------------------------------------------------------

def test_generated_configuration_declares_single_operator_local_auth():
    """The file says what it is in its own header."""
    plan = plan_front_door(
        DEPLOYMENT, chat_block=chat_block(), instance_port=8080,
    )
    configuration = render_nginx_front_door(plan)

    header = configuration.split("server {", 1)[0]
    assert SINGLE_OPERATOR_HEADER_MARKER in header
    assert "NOT the multi-user" in header
    assert "chat-instance-lifecycle" in header
    assert f"server_name {plan.hostname};" in configuration
    assert f"proxy_pass {plan.chat_upstream};" in configuration


def test_an_ipv6_instance_host_is_bracketed_in_the_chat_upstream():
    """Fail-before (at da399f22c4): `http://:::8080`, which does not parse."""
    plan = plan_front_door(
        DEPLOYMENT, chat_block=chat_block(), instance_port=8080, instance_host="::",
    )
    assert plan.chat_upstream == "http://[::]:8080"


def test_generated_configuration_has_no_console_location():
    """No location SERVES the console — the scenario's normative wording.

    The console location that does exist carries no ``proxy_pass``, ``alias``
    or ``root``: nothing routes anywhere, and nothing claims to serve. The
    behavioural half of this claim is
    ``test_console_path_returns_an_explicit_not_configured_response``.
    """
    plan = plan_front_door(
        DEPLOYMENT, chat_block=chat_block(), instance_port=8080,
    )
    configuration = render_nginx_front_door(plan)

    # TWO blocks, and both are checked. A bare `location /console` is a PREFIX
    # match in nginx, so it also swallows `/console-old`, which the served
    # handler does not — the pair `location ^~ /console/` plus
    # `location = /console` is what makes the two artifacts match the same set
    # of paths. Checking only one of them would leave the other free to serve.
    blocks = console_blocks(configuration)
    assert len(blocks) == 2, (
        "the exact and prefix console locations must both be rendered, or the "
        "file and the served handler match different paths"
    )
    for console_block in blocks:
        # The not-configured body is a quoted argument to `return`, and its
        # prose legitimately contains English words that are directive names
        # elsewhere. Directives live outside the quotes; console_blocks has
        # already stripped them.
        for serving in ("proxy_pass", "alias", "root", "try_files"):
            assert serving not in console_block, (
                f"the console location contains {serving!r}: it claims to "
                "serve a console that does not boot under a prefix"
            )
        assert f"return {CONSOLE_NOT_CONFIGURED_STATUS}" in console_block
    assert CONSOLE_OWNER_PACT in configuration
    assert plan.console_permitted is False

    # Non-vacuity: the CHAT location does carry a proxy_pass, so the check
    # above is looking at a block that could have had one.
    chat_block_text = configuration.split("location / {", 1)[1].split("}", 1)[0]
    assert "proxy_pass" in chat_block_text


def test_rendered_nginx_and_served_proxy_implement_the_same_routes():
    """Two renderers over one plan only holds while something checks it."""
    plan = plan_front_door(
        DEPLOYMENT, chat_block=chat_block(), instance_port=8080,
    )
    assert routes_agree(plan, render_nginx_front_door(plan)) == []

    paths = [route["path"] for route in front_door_route_table(plan)]
    assert paths == [CONSOLE_PREFIX, "/"]


# ---------------------------------------------------------------------------
# Transport: the listener and the scheme it was derived from must agree
# ---------------------------------------------------------------------------

#: Paths, not material. Nothing in this suite writes or reads a key.
TLS = TlsMaterial(
    certificate="/etc/ssl/okg/fixture.fullchain.pem",
    private_key="/etc/ssl/okg/fixture.key",
)


def _https_block(base: str = "https://chat.fixture.invalid"):
    return chat_block(deep_links={
        "operator_ui_base": "http://127.0.0.1:5173",
        "chat_site_base": base,
    })


def test_https_site_base_without_tls_material_refuses():
    """The defect, stated as a refusal.

    The listen port is derived from the scheme, so ``https`` produced
    ``listen 443;`` — no ``ssl``, no certificate, no key — in front of a proxy
    that performs no authentication. Every client speaks TLS to 443 and every
    reader of the file sees a terminated front. Neither is true.
    """
    with pytest.raises(ChatInstanceError) as caught:
        plan_front_door(
            DEPLOYMENT, chat_block=_https_block(), instance_port=8080,
        )

    assert caught.value.data["reason"] == "tls_material_undeclared"
    assert caught.value.data["listen_port"] == 443
    assert caught.value.data["missing"] == [
        "--tls-certificate", "--tls-private-key",
    ]


def test_https_site_base_with_tls_material_renders_a_complete_server_block():
    """Supplied the two paths, the block terminates TLS for real."""
    plan = plan_front_door(
        DEPLOYMENT, chat_block=_https_block(), instance_port=8080, tls=TLS,
    )
    configuration = render_nginx_front_door(plan)

    assert plan.scheme == "https"
    assert "listen 443 ssl;" in configuration
    assert f"ssl_certificate {TLS.certificate};" in configuration
    assert f"ssl_certificate_key {TLS.private_key};" in configuration
    assert "ssl_protocols TLSv1.2 TLSv1.3;" in configuration
    # The file says what it terminates, next to what it authenticates.
    header = configuration.split("server {", 1)[0]
    assert "TRANSPORT: TLS terminated here" in header
    assert SINGLE_OPERATOR_HEADER_MARKER in header
    assert routes_agree(plan, configuration) == []


def test_no_rendered_configuration_listens_plain_on_a_tls_port():
    """The invariant, checked against the renderers rather than trusted.

    Asserted by MUTATION: the certificate directives are struck out of a
    configuration that had them, and the agreement check must report it. A
    check that returns an empty list for every input would pass the arm above
    and catch nothing.
    """
    plan = plan_front_door(
        DEPLOYMENT, chat_block=_https_block(), instance_port=8080, tls=TLS,
    )
    configuration = render_nginx_front_door(plan)
    assert routes_agree(plan, configuration) == []

    stripped = configuration.replace(f"ssl_certificate {TLS.certificate};", "")
    assert routes_agree(plan, stripped), "a missing certificate went unreported"

    downgraded = configuration.replace("listen 443 ssl;", "listen 443;")
    problems = " ".join(routes_agree(plan, downgraded))
    assert "does not listen with ssl" in problems

    # The same lie from the other side, and the reason the check lives in
    # routes_agree rather than only in plan_front_door: a plan built anywhere
    # else that listens plain on 443 is caught against the file it produced.
    plain_on_443 = replace(plan, listen_port=443, scheme="http", tls=None)
    forged = render_nginx_front_door(plain_on_443)
    assert "listen 443;" in forged
    assert any(
        "speak TLS to" in problem
        for problem in routes_agree(plain_on_443, forged)
    )


def test_a_plain_site_base_on_a_tls_port_refuses():
    """``http://host:443`` is the same lie told through the port instead."""
    with pytest.raises(ChatInstanceError) as caught:
        plan_front_door(
            DEPLOYMENT,
            chat_block=_https_block("http://chat.fixture.invalid:443"),
            instance_port=8080,
        )
    assert caught.value.data["reason"] == "plain_scheme_on_tls_port"


def test_tls_material_for_a_plain_site_base_refuses():
    """Refused, not ignored: a certificate believed to be in use and is not."""
    with pytest.raises(ChatInstanceError) as caught:
        plan_front_door(
            DEPLOYMENT, chat_block=chat_block(), instance_port=8080, tls=TLS,
        )
    assert caught.value.data["reason"] == "tls_material_for_plain_site_base"


def test_the_reference_front_door_refuses_a_plan_that_terminates_tls():
    """The proof surface speaks plain HTTP and will not pretend otherwise."""
    plan = plan_front_door(
        DEPLOYMENT, chat_block=_https_block(), instance_port=8080, tls=TLS,
    )
    with pytest.raises(ChatInstanceError) as caught:
        serve_front_door(plan)
    assert caught.value.data["reason"] == "reference_front_door_terminates_no_tls"


def test_a_plain_http_configuration_says_so_in_its_own_header():
    """The control for the transport header: it states the plain case too.

    A header that only speaks up for TLS would leave the plain case reading
    the same as it always did — which is how the missing directives went
    unremarked in the first place.
    """
    plan = plan_front_door(
        DEPLOYMENT, chat_block=chat_block(), instance_port=8080,
    )
    configuration = render_nginx_front_door(plan)

    assert plan.tls is None
    assert plan.scheme == "http"
    assert "listen 8080;" in configuration
    assert "ssl_certificate" not in configuration
    assert "TRANSPORT: plain HTTP" in configuration
    assert "travels in the clear" in configuration
    assert routes_agree(plan, configuration) == []


def test_multi_user_deployment_is_refused_rather_than_forking_a_second_proxy():
    """chat-sso-integration owns the multi-user front; this one refuses.

    Two ``server`` blocks on one ``server_name`` are not two fronts: nginx
    matches the first and the second is dead configuration.
    """
    multi_user = chat_block(sso={
        "mode": "multi_user",
        "provider_id": "okg-chat-sso",
        "issuer": "https://idp.example.edu/",
        "audience": "okg-mcp",
        "discovery_endpoint": (
            "https://idp.example.edu/.well-known/openid-configuration"
        ),
        "client_id": "okg-chat",
        "client_secret_env": "OKG_CHAT_OIDC_SECRET",
        "principal_header": "X-Okg-Principal",
        "groups_claim": "groups",
        "admin_group": "fixture-admins",
        "user_group": "fixture-users",
    })

    with pytest.raises(ChatInstanceError) as caught:
        plan_front_door(DEPLOYMENT, chat_block=multi_user, instance_port=8080)

    assert caught.value.data["reason"] == "multi_user_front_owned_elsewhere"
    assert caught.value.data["owner"] == "chat-sso-integration"


# ---------------------------------------------------------------------------
# The console, once its bundle is measured fit to be served under the prefix
# ---------------------------------------------------------------------------

#: A bundle that asks for its assets UNDER the prefix. This is what
#: ``console_base_awareness`` measures as base-aware, and what the front door
#: must therefore route rather than refuse.
BASE_AWARE_INDEX = (
    "<!doctype html><html><head>"
    '<script type="module" src="/console/assets/app.js"></script>'
    '<link rel="stylesheet" href="/console/assets/app.css">'
    "</head><body><div id=\"okg-console-root\"></div></body></html>\n"
)

#: The bundle the read-handshake spike measured: root-relative assets that the
#: browser requests at the ORIGIN, which the prefix never sees.
ROOT_RELATIVE_INDEX = (
    "<!doctype html><html><head>"
    '<script type="module" src="/assets/app.js"></script>'
    "</head><body></body></html>\n"
)

CONSOLE_MARKER = "okg-console-root"


class _ConsoleUpstream(http.server.BaseHTTPRequestHandler):
    """A stand-in console origin that RECORDS the paths it was asked for.

    Recording is the point: whether the prefix survives the hop is not
    observable from the response body, and it is the exact property a
    base-aware bundle depends on.
    """

    paths: list[str]
    index: str

    def log_message(self, *_args) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's contract
        type(self).paths.append(self.path)
        body = self.index.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@contextlib.contextmanager
def _console_origin(index: str = BASE_AWARE_INDEX):
    """A running console upstream, yielding ``(base_url, paths_it_was_asked)``."""
    paths: list[str] = []
    handler = type(
        "_BoundConsole", (_ConsoleUpstream,), {"paths": paths, "index": index},
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", paths
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _bundle(tmp_path, markup: str):
    index = tmp_path / "console-index.html"
    index.write_text(markup)
    return ConsoleBuild(index_path=index)


def test_a_measured_base_aware_bundle_is_routed_instead_of_not_configured(
    tmp_path,
):
    """The regression this wiring exists for, in the plan and in both renderers.

    ``console_permitted`` was computed, published in the plan and read by
    nobody: the route table, the nginx renderer and the served handler each
    emitted the not-configured answer unconditionally, so a bundle measured
    fit to serve got exactly the treatment of one measured unfit.
    """
    with _console_origin() as (upstream, _paths):
        plan = plan_front_door(
            DEPLOYMENT,
            chat_block=chat_block(),
            instance_port=8080,
            console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
            console_upstream=upstream,
        )

        assert plan.console_permitted is True
        assert plan.console_evidence == "bundle_measured"
        assert plan.console_upstream == upstream

        console_route = front_door_route_table(plan)[0]
        assert console_route["path"] == CONSOLE_PREFIX
        assert console_route["action"] == "proxy"
        assert console_route["upstream"] == upstream

        configuration = render_nginx_front_door(plan)
        blocks = console_blocks(configuration)
        assert len(blocks) == 2
        for console_block in blocks:
            assert f"proxy_pass {upstream};" in console_block
            assert f"return {CONSOLE_NOT_CONFIGURED_STATUS}" not in console_block
            # No trailing slash and no path on the upstream: either would make
            # nginx REPLACE the matched prefix, which is what keeps the prefix
            # on the forwarded request and the whole content of "base-aware".
            assert f"proxy_pass {upstream}/;" not in console_block
        assert "The operator console IS served here" in configuration
        assert routes_agree(plan, configuration) == []


def test_the_served_console_route_reaches_the_upstream_with_its_prefix_intact(
    tmp_path,
):
    """Content from the console origin, and the paths that origin was asked for.

    Asserting only on the body would pass for a front door that stripped the
    prefix, which is precisely the misconfiguration a base-aware bundle cannot
    survive: its own asset requests carry ``/console/`` and would arrive at
    the upstream as ``/assets/…``.
    """
    with _console_origin() as (upstream, paths):
        plan = plan_front_door(
            DEPLOYMENT,
            chat_block=chat_block(),
            instance_port=8080,
            console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
            console_upstream=upstream,
        )
        with contextlib.closing(serve_front_door(plan)) as server:
            status, body = fetch(f"{server.base_url}{CONSOLE_PREFIX}")
            assert status == 200
            assert CONSOLE_MARKER in body
            assert CONSOLE_OWNER_PACT not in body

            asset_status, _ = fetch(
                f"{server.base_url}{CONSOLE_PREFIX}/assets/app.js",
            )
            assert asset_status == 200

    assert paths == [CONSOLE_PREFIX, f"{CONSOLE_PREFIX}/assets/app.js"]


def test_a_bundle_measured_not_base_aware_still_answers_not_configured(tmp_path):
    """Fail-closed, and MEASURED so: the gate is not merely default-off.

    Without this arm the wiring above would be satisfied by a front door that
    served the console unconditionally — which is the failure the read
    handshake actually saw.
    """
    plan = plan_front_door(
        DEPLOYMENT,
        chat_block=chat_block(),
        instance_port=8080,
        console_build=_bundle(tmp_path, ROOT_RELATIVE_INDEX),
    )

    assert plan.console_permitted is False
    assert plan.console_evidence == "bundle_measured"
    assert plan.console_upstream is None
    assert front_door_route_table(plan)[0]["action"] == "not_configured"

    configuration = render_nginx_front_door(plan)
    assert f"return {CONSOLE_NOT_CONFIGURED_STATUS}" in configuration
    assert routes_agree(plan, configuration) == []

    with contextlib.closing(serve_front_door(plan)) as server:
        status, body = fetch(f"{server.base_url}{CONSOLE_PREFIX}")
    assert status == CONSOLE_NOT_CONFIGURED_STATUS
    assert CONSOLE_OWNER_PACT in body


def test_a_permitted_console_with_no_upstream_refuses(tmp_path):
    """Never inferred. ``operator_ui_base`` is the console's PUBLIC base, and
    on a fronted deployment that is this hostname — using it would point the
    front door at itself."""
    with pytest.raises(ChatInstanceError) as caught:
        plan_front_door(
            DEPLOYMENT,
            chat_block=chat_block(),
            instance_port=8080,
            console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
        )

    assert caught.value.data["reason"] == "console_upstream_undeclared"
    assert caught.value.data["missing"] == ["--console-upstream"]


def test_a_console_upstream_for_an_unpermitted_bundle_refuses(tmp_path):
    """Refused, not discarded into a 503 that reads as the flag never passed."""
    with pytest.raises(ChatInstanceError) as caught:
        plan_front_door(
            DEPLOYMENT,
            chat_block=chat_block(),
            instance_port=8080,
            console_build=_bundle(tmp_path, ROOT_RELATIVE_INDEX),
            console_upstream="http://127.0.0.1:5173",
        )
    assert caught.value.data["reason"] == "console_upstream_for_unpermitted_bundle"

    # And with no bundle evidence at all — the state every deployment is in
    # until operator-ui-base-aware-build lands.
    with pytest.raises(ChatInstanceError) as bare:
        plan_front_door(
            DEPLOYMENT,
            chat_block=chat_block(),
            instance_port=8080,
            console_upstream="http://127.0.0.1:5173",
        )
    assert bare.value.data["console_evidence"] == "no_evidence"


def test_routes_agree_catches_a_console_the_plan_permits_and_the_file_refuses(
    tmp_path,
):
    """The agreement check, asserted by MUTATION in the permitted direction.

    A check that only ever inspected the not-configured shape would return an
    empty list for every permitted plan and catch nothing — which is how the
    three call sites drifted from the verdict in the first place.
    """
    with _console_origin() as (upstream, _paths):
        plan = plan_front_door(
            DEPLOYMENT,
            chat_block=chat_block(),
            instance_port=8080,
            console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
            console_upstream=upstream,
        )
        configuration = render_nginx_front_door(plan)
        assert routes_agree(plan, configuration) == []

        # The pre-wiring file: a permitted plan rendered with the 503 block.
        regressed = render_nginx_front_door(
            replace(plan, console_permitted=False, console_upstream=None),
        )
        problems = " ".join(routes_agree(plan, regressed))
        assert "does not proxy to its upstream" in problems

        # And the opposite lie: the console block pointed somewhere else.
        elsewhere = configuration.replace(
            f"proxy_pass {upstream};", "proxy_pass http://127.0.0.1:9;",
        )
        assert routes_agree(plan, elsewhere)


# ---------------------------------------------------------------------------
# The four ways the two artifacts were still able to disagree
# ---------------------------------------------------------------------------

def test_a_console_upstream_carrying_a_path_refuses(tmp_path):
    """The prefix-stripping defect, arriving through the flag that enables it.

    In nginx a ``proxy_pass`` with a URI part REPLACES the matched location
    prefix, so ``--console-upstream http://host/ui`` would forward
    ``/console/assets/app.js`` as ``/ui/assets/app.js`` — the prefix stripped
    from under a bundle that asks for it. The served handler concatenates and
    would forward ``/ui/console/assets/app.js``, so the two artifacts would
    also disagree with each other. Refused, because no reading of a path here
    is one both renderers can honour.
    """
    for bad in (
        "http://127.0.0.1:5173/ui",
        "http://127.0.0.1:5173/ui/",
        "http://127.0.0.1:5173?x=1",
        "http://127.0.0.1:5173#frag",
    ):
        with pytest.raises(ChatInstanceError) as caught:
            plan_front_door(
                DEPLOYMENT,
                chat_block=chat_block(),
                instance_port=8080,
                console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
                console_upstream=bad,
            )
        assert caught.value.data["reason"] in {
            "console_upstream_is_not_a_bare_origin",
        }, bad

    # Control: the bare origin the refusals point at is accepted, and so is
    # the same origin written with a lone trailing slash — that spelling means
    # the origin and normalizes to it, so it is not one of the shapes that
    # could reach nginx as a prefix-replacing URI part.
    for good in ("http://127.0.0.1:5173", "http://127.0.0.1:5173/"):
        plan = plan_front_door(
            DEPLOYMENT,
            chat_block=chat_block(),
            instance_port=8080,
            console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
            console_upstream=good,
        )
        assert plan.console_upstream == "http://127.0.0.1:5173"


def test_the_two_artifacts_match_the_same_paths_around_the_prefix(tmp_path):
    """``/console-old`` must not be a console path in one artifact only.

    nginx prefix-matches a bare ``location /console``, so it would send
    ``/console-old`` to the console upstream while the handler sent it to
    chat. The rendered file therefore uses ``location ^~ /console/`` plus
    ``location = /console``, which is exactly the handler's own test.
    """
    plan = plan_front_door(
        DEPLOYMENT,
        chat_block=chat_block(),
        instance_port=8080,
        console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
        console_upstream="http://127.0.0.1:5173",
    )
    configuration = render_nginx_front_door(plan)

    assert f"location ^~ {CONSOLE_PREFIX}/ {{" in configuration
    assert f"location = {CONSOLE_PREFIX} {{" in configuration
    # The bare prefix form is what would over-match; it must not be emitted.
    assert f"location {CONSOLE_PREFIX} {{" not in configuration

    # And the served handler agrees on the same three cases.
    with contextlib.closing(serve_front_door(plan)) as server:
        # /console-old is NOT a console path: it goes to chat, whose upstream
        # is not listening here, so the front door's own 502 names it.
        status, body = fetch(f"{server.base_url}{CONSOLE_PREFIX}-old")
        assert status == 502
        assert plan.chat_upstream in body
        assert plan.console_upstream not in body


def test_routes_agree_catches_a_root_location_pointed_somewhere_else(tmp_path):
    """The chat check is scoped to ``location /``, not searched over the file.

    With a console block that can also carry a ``proxy_pass``, a whole-file
    search is satisfied by the console alone whenever the two upstreams
    coincide — leaving a root location pointed anywhere at all unreported.
    """
    plan = plan_front_door(
        DEPLOYMENT,
        chat_block=chat_block(),
        instance_port=8080,
        console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
        # Deliberately the SAME origin the chat upstream resolves to.
        console_upstream="http://127.0.0.1:8080",
    )
    configuration = render_nginx_front_door(plan)
    assert plan.console_upstream == plan.chat_upstream
    assert routes_agree(plan, configuration) == []

    root = configuration.split("location / {", 1)[1].split("}", 1)[0]
    hijacked = configuration.replace(
        root, root.replace(f"proxy_pass {plan.chat_upstream};",
                           "proxy_pass http://elsewhere.invalid:1;"),
    )
    problems = " ".join(routes_agree(plan, hijacked))
    assert "does not proxy / to the plan's chat upstream" in problems


def test_a_console_index_referencing_no_assets_is_not_a_measurement(tmp_path):
    """Absence recorded as a measurement is the defect this PACT is named for.

    "Found no offending reference" is not "found references and all were
    safe". An empty file, a truncated write or the wrong file would otherwise
    be PERMITTED on evidence ``bundle_measured`` and served.
    """
    for markup in ("", "<html><body>hello</body></html>"):
        with pytest.raises(ChatInstanceError) as caught:
            plan_front_door(
                DEPLOYMENT,
                chat_block=chat_block(),
                instance_port=8080,
                console_build=_bundle(tmp_path, markup),
                console_upstream="http://127.0.0.1:5173",
            )
        # It is refused as an UNPERMITTED bundle, and the verdict behind it
        # says no_evidence rather than bundle_measured.
        assert caught.value.data["reason"] == (
            "console_upstream_for_unpermitted_bundle"
        )
        assert caught.value.data["console_evidence"] == "no_evidence"
        assert "references no assets" in caught.value.data["console_detail"]

    # Control: the same path with a real built index measures and permits.
    plan = plan_front_door(
        DEPLOYMENT,
        chat_block=chat_block(),
        instance_port=8080,
        console_build=_bundle(tmp_path, BASE_AWARE_INDEX),
        console_upstream="http://127.0.0.1:5173",
    )
    assert (plan.console_permitted, plan.console_evidence) == (
        True, "bundle_measured",
    )


def test_the_not_configured_body_states_the_evidence_it_actually_has(tmp_path):
    """The 503 body must not assert a measurement nobody took.

    Its fixed sentence used to read "its assets are requested at the origin
    root, so they 404" — a measured fact about one bundle — and was served for
    the no-evidence case too.
    """
    unmeasured = plan_front_door(
        DEPLOYMENT, chat_block=chat_block(), instance_port=8080,
    )
    with contextlib.closing(serve_front_door(unmeasured)) as server:
        _status, body = fetch(f"{server.base_url}{CONSOLE_PREFIX}")
    assert "Evidence: no_evidence" in body
    assert "nothing here says whether its bundle is base-aware" in body
    assert "so they 404" not in body

    measured = plan_front_door(
        DEPLOYMENT,
        chat_block=chat_block(),
        instance_port=8080,
        console_build=_bundle(tmp_path, ROOT_RELATIVE_INDEX),
    )
    with contextlib.closing(serve_front_door(measured)) as server:
        _status, measured_body = fetch(f"{server.base_url}{CONSOLE_PREFIX}")
    assert "Evidence: bundle_measured" in measured_body
    assert "asks for assets at the origin root" in measured_body
    # The two bodies are DIFFERENT, which is the whole point.
    assert measured_body != body
    # And the rendered file carries the same distinction.
    assert "Evidence: no_evidence" in render_nginx_front_door(unmeasured)
    assert "Evidence: bundle_measured" in render_nginx_front_door(measured)


def test_front_door_refuses_without_a_declared_chat_site_base():
    """The hostname is generated from the manifest, never guessed."""
    hostless = chat_block(deep_links={"operator_ui_base": "http://127.0.0.1:5173"})

    with pytest.raises(ChatInstanceError) as caught:
        plan_front_door(DEPLOYMENT, chat_block=hostless, instance_port=8080)

    assert caught.value.data["reason"] == "chat_site_base_undeclared"
    assert caught.value.data["manifest_key"] == "chat.deep_links.chat_site_base"
