"""The app database is a distinct per-deployment Postgres database (PACT
``chat-instance-lifecycle``,
``req.chat-instance-lifecycle.app-database-isolated``).

The defect this suite exists to exclude is silent by construction. Open WebUI
resolves ``DATABASE_URL = os.getenv('DATABASE_URL',
f'sqlite:///{DATA_DIR}/webui.db')`` at ``env.py:267``, with ``DATA_DIR``
resolved at ``env.py:222`` — re-measured read-only against the running
``ghcr.io/open-webui/open-webui:v0.11.0`` container on 2026-08-10, where
``/app/backend/data/webui.db`` is present and 4.9 MB. So an instance we
started with a wrong, unreachable, or absent ``DATABASE_URL`` comes up
perfectly healthy on a private SQLite file, and every readback the rest of the
program performs succeeds against the wrong store.

Three arms, each with its positive control:

* **distinct** — a row the instance itself wrote is in the app database and
  not in the graph database. Zero rows on both sides FAILS this arm; that is
  the vacuous pass the acceptance names. Needs two real Postgres databases and
  a real instance, so it carries the ``postgres`` marker and is the arm the
  serialized Postgres lane runs;
* **collision** — three ways two DSNs can differ while resolving to one
  database, each paired with a genuinely distinct DSN that provisions, so the
  refusal is proved to be discriminating rather than blanket;
* **unreachable** — refuses against a genuinely closed port, and the
  vendor-default probe is shown able to report PRESENT before it is asserted
  ABSENT.
"""

from __future__ import annotations

import json
import re

import pytest

from okg.substrate.chat.instance import (
    VENDOR_DEFAULT_STORE,
    AppDatabase,
    ChatInstanceError,
    InstanceRecord,
    PgTarget,
    build_instance_spec,
    container_name_for,
    normalize_dsn,
    peer_instances,
    read_instance,
    record_instance,
    resolve_app_database,
    resolve_instance_port,
    stand_up,
    vendor_default_store_present,
)
from okg.substrate.deployments.chat_block import schema_field_census

from .instance_harness import (
    ProcessRuntime,
    chat_block,
    env,
    free_port,
    reachable_probe,
)

DEPLOYMENT = "fixture-deployment"
APP_ENV = "OKG_CHAT_APP_DATABASE_URL"
SECRET_ENV = "OKG_CHAT_WEBUI_SECRET_KEY"

#: The deployment's GRAPH database. Every collision variant below resolves to
#: exactly this target while spelling itself differently.
GRAPH_DSN = "postgresql://okg:pw@localhost:5432/okg_fixture_graph"

#: A genuinely different database on the same server. The positive control for
#: every collision variant: if the refusal fired on this too, it would be
#: refusing on server identity rather than on database identity.
DISTINCT_DSN = "postgresql://okg:pw@localhost:5432/okg_fixture_chat_app"


@pytest.fixture
def runtime():
    made = ProcessRuntime()
    try:
        yield made
    finally:
        made.shutdown()


def _resolve(dsn: str, *, peers=()):
    return resolve_app_database(
        DEPLOYMENT,
        env_var=APP_ENV,
        environ=env(**{APP_ENV: dsn}),
        graph_dsn=GRAPH_DSN,
        peers=peers,
    )


def _collision_pair(variant: str, *, distinct: str) -> None:
    """One spelling variant of the graph DSN refuses; a distinct one resolves.

    Both halves in one helper because either alone is uninformative: a refusal
    with no paired success cannot be told from a check that refuses everything.
    """
    with pytest.raises(ChatInstanceError) as caught:
        _resolve(variant)
    assert caught.value.data["reason"] == "app_database_is_graph_database"
    assert caught.value.data["graph_database"]["database"] == "okg_fixture_graph"
    assert "okg_fixture_graph" in str(caught.value)

    resolved = _resolve(distinct)
    assert resolved.target.database == "okg_fixture_chat_app"


# ---------------------------------------------------------------------------
# Collision, on normalized connection parameters and never on string equality
# ---------------------------------------------------------------------------

def test_graph_dsn_collision_refused_on_credentials_variant():
    """Different user and password, same database. Refused."""
    _collision_pair(
        "postgresql://someone_else:different@localhost:5432/okg_fixture_graph",
        distinct="postgresql://someone_else:different@localhost:5432/"
                 "okg_fixture_chat_app",
    )


def test_graph_dsn_collision_refused_on_query_parameter_variant():
    """Query parameters change the session, not which database it is."""
    _collision_pair(
        "postgresql://okg:pw@localhost:5432/okg_fixture_graph"
        "?sslmode=require&application_name=chat",
        distinct="postgresql://okg:pw@localhost:5432/okg_fixture_chat_app"
                 "?sslmode=require&application_name=chat",
    )


def test_graph_dsn_collision_refused_on_localhost_host_form():
    """`127.0.0.1` and `localhost` are one host, and the check must see it."""
    _collision_pair(
        "postgresql://okg:pw@127.0.0.1:5432/okg_fixture_graph",
        distinct="postgresql://okg:pw@127.0.0.1:5432/okg_fixture_chat_app",
    )
    # And the normalization that makes it so, asserted directly.
    assert normalize_dsn(
        "postgresql://a:b@localhost/x", label="t",
    ) == normalize_dsn("postgresql://c:d@127.0.0.1:5432/x?sslmode=require", label="t")


def test_distinct_dsn_provisions_as_positive_control(runtime, tmp_path):
    """A genuinely distinct app database stands the instance up.

    The control for all three collision arms at the LIFECYCLE level rather
    than at the resolver level: it proves the refusals above are not simply
    the stand-up path being broken for every DSN.
    """
    port = free_port()
    report = stand_up(
        DEPLOYMENT,
        chat_block=chat_block(),
        graph_dsn=GRAPH_DSN,
        runtime=runtime,
        environ=env(**{APP_ENV: DISTINCT_DSN, SECRET_ENV: "fixture-key"}),
        instance_port_override=port,
        state_root=tmp_path,
        reachability_probe=reachable_probe,
        ready_timeout=20.0,
        ready_interval=0.05,
    )
    assert report.app_database.target == PgTarget(
        host="127.0.0.1", port=5432, database="okg_fixture_chat_app",
    )
    assert report.app_database.target != normalize_dsn(GRAPH_DSN, label="graph")
    # The launch spec carries DATABASE_URL, which is what keeps F1's fallback
    # out of reach.
    assert "DATABASE_URL" in runtime.run_calls[0].env_var_names()


def test_second_deployment_may_not_share_an_app_database(tmp_path):
    """One app database per deployment, checked against recorded peers."""
    record_instance(
        InstanceRecord(
            deployment="other-deployment",
            container=container_name_for("other-deployment"),
            host="127.0.0.1",
            port=8091,
            image="ghcr.io/open-webui/open-webui:v0.11.0",
            app_database=normalize_dsn(DISTINCT_DSN, label="app"),
            port_source="operator_flag",
            recorded_at="2026-08-10T00:00:00+00:00",
        ),
        state_root=tmp_path,
    )
    peers = [read_instance("other-deployment", state_root=tmp_path)]

    with pytest.raises(ChatInstanceError) as caught:
        _resolve(DISTINCT_DSN, peers=peers)
    assert caught.value.data["reason"] == "app_database_shared_with_peer"
    assert caught.value.data["holder"] == "other-deployment"

    # Control: a different database on the same server is fine.
    resolved = _resolve(
        "postgresql://okg:pw@localhost:5432/okg_fixture_chat_app_two", peers=peers,
    )
    assert resolved.target.database == "okg_fixture_chat_app_two"


# ---------------------------------------------------------------------------
# The collision check cannot be SKIPPED, only passed or refused
# ---------------------------------------------------------------------------

def test_unresolvable_graph_dsn_refuses_rather_than_skipping_the_check(
    runtime, tmp_path,
):
    """No graph DSN is a REFUSAL, and specifically not a silent pass.

    ``None`` is the ordinary value, not an exotic one: the manifest loader
    returns it for the ``dsn: ${OKG_DSN}`` shape this repo uses whenever the
    variable is unexported (``deployments/__init__.py:977-981``). So a check
    guarded on having a graph DSN is a check that is off by default.

    The app DSN below is deliberately the one that MUST be refused — it
    resolves to the graph database. If the guard returns, this DSN provisions,
    and Open WebUI migrates its own tables into the deployment's graph
    database. Asserting the refusal here is asserting that cannot happen.
    """
    with pytest.raises(ChatInstanceError) as caught:
        stand_up(
            DEPLOYMENT,
            chat_block=chat_block(),
            graph_dsn=None,
            runtime=runtime,
            environ=env(**{APP_ENV: GRAPH_DSN, SECRET_ENV: "fixture-key"}),
            instance_port_override=free_port(),
            state_root=tmp_path,
            reachability_probe=reachable_probe,
            ready_timeout=5.0,
            ready_interval=0.05,
        )

    assert caught.value.data["reason"] == "graph_dsn_unresolvable"
    assert caught.value.data["manifest_key"] == "postgres.dsn"
    # Post-state: the refusal is decided before anything is created.
    assert runtime.run_calls == []
    assert read_instance(DEPLOYMENT, state_root=tmp_path) is None


@pytest.mark.parametrize("absent", [None, "", "   "])
def test_every_unresolvable_graph_dsn_spelling_refuses(absent):
    """``None`` is not the only way the graph DSN arrives unusable."""
    with pytest.raises(ChatInstanceError) as caught:
        resolve_app_database(
            DEPLOYMENT,
            env_var=APP_ENV,
            environ=env(**{APP_ENV: DISTINCT_DSN}),
            graph_dsn=absent,
        )
    assert caught.value.data["reason"] == "graph_dsn_unresolvable"


def test_the_report_names_the_graph_database_it_was_proved_distinct_from(
    runtime, tmp_path,
):
    """The check having run is a POSITIVE fact in the report, not an inference.

    Absence of a refusal is what the skipped check also looked like. So the
    report carries the two targets that were compared, and a reader can redo
    the comparison. A ``PgTarget`` is host, port and database name — no
    credential rides along.
    """
    report = stand_up(
        DEPLOYMENT,
        chat_block=chat_block(),
        graph_dsn=GRAPH_DSN,
        runtime=runtime,
        environ=env(**{APP_ENV: DISTINCT_DSN, SECRET_ENV: "fixture-key"}),
        instance_port_override=free_port(),
        state_root=tmp_path,
        reachability_probe=reachable_probe,
        ready_timeout=20.0,
        ready_interval=0.05,
    )

    witness = report.as_dict()["app_database"]["proved_distinct_from_graph_database"]
    assert witness == normalize_dsn(GRAPH_DSN, label="graph").as_dict()
    assert witness["database"] != report.app_database.target.database
    assert "pw" not in json.dumps(report.as_dict())


# ---------------------------------------------------------------------------
# A peer we cannot read is not a peer with nothing to declare
# ---------------------------------------------------------------------------

def _corrupt_peer(tmp_path, deployment: str, contents: str) -> None:
    path = tmp_path / deployment / "chat-instance.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


@pytest.mark.parametrize(
    "contents, why",
    [
        ("{not json at all", "truncated mid-write"),
        ("[]", "a list where the record should be"),
        ('{"deployment": "other-deployment"}', "no app_database target"),
        ('{"app_database": {"port": 5432}}', "an app_database naming no database"),
    ],
)
def test_unreadable_peer_state_refuses_rather_than_shrinking_the_peer_set(
    tmp_path, contents, why,
):
    """A corrupt peer record refuses; it does NOT read as "no such peer".

    That reading is the defect: the peer drops out of the set
    :func:`peer_instances` builds, its app database drops out of the collision
    check with it, and this deployment is handed a SHARED app database — the
    outcome the whole requirement exists to exclude.
    """
    _corrupt_peer(tmp_path, "other-deployment", contents)

    with pytest.raises(ChatInstanceError) as caught:
        peer_instances(exclude=DEPLOYMENT, state_root=tmp_path)
    assert caught.value.data["reason"] == "instance_state_unreadable", why
    assert "other-deployment" in caught.value.data["path"]


def test_a_shared_app_database_is_refused_and_not_lost_with_the_record(tmp_path):
    """The pair that shows what the refusal above is protecting.

    First with an intact peer record: the shared app database is refused by
    name. Then with that same record corrupted: still a refusal — the previous
    behaviour returned ``None`` here, which let the identical DSN through.
    """
    record_instance(
        InstanceRecord(
            deployment="other-deployment",
            container=container_name_for("other-deployment"),
            host="127.0.0.1",
            port=8091,
            image="ghcr.io/open-webui/open-webui:v0.11.0",
            app_database=normalize_dsn(DISTINCT_DSN, label="app"),
            port_source="operator_flag",
            recorded_at="2026-08-10T00:00:00+00:00",
        ),
        state_root=tmp_path,
    )
    intact = peer_instances(exclude=DEPLOYMENT, state_root=tmp_path)
    with pytest.raises(ChatInstanceError) as shared:
        _resolve(DISTINCT_DSN, peers=intact)
    assert shared.value.data["reason"] == "app_database_shared_with_peer"

    _corrupt_peer(tmp_path, "other-deployment", '{"deployment": "other-')
    with pytest.raises(ChatInstanceError) as unreadable:
        peer_instances(exclude=DEPLOYMENT, state_root=tmp_path)
    assert unreadable.value.data["reason"] == "instance_state_unreadable"


def test_a_deployment_with_no_state_file_is_still_simply_absent(tmp_path):
    """The control: ABSENT must keep meaning absent, or every stand-up refuses."""
    assert read_instance("never-stood-up", state_root=tmp_path) is None
    assert peer_instances(exclude=DEPLOYMENT, state_root=tmp_path) == []


# ---------------------------------------------------------------------------
# Unreachable, and the vendor default store
# ---------------------------------------------------------------------------

def test_unreachable_dsn_refuses_and_creates_no_vendor_default_store(
    runtime, tmp_path,
):
    """A closed port refuses, and the store probe is shown able to say PRESENT.

    The absence assertion is worthless on its own — a probe that always
    answers "absent" satisfies it. So this test first drives the probe against
    a container launched WITHOUT ``DATABASE_URL`` and requires it to report
    the store present, then asserts absence on the refused run. The store's
    location is not assumed: :data:`VENDOR_DEFAULT_STORE` is the path measured
    at the pinned version (``env.py:222`` + ``env.py:267``) and recorded in
    this module's docstring.
    """
    closed_port = free_port()  # nothing is bound here
    unreachable = f"postgresql://okg:pw@127.0.0.1:{closed_port}/okg_fixture_chat_app"
    instance_port = free_port()

    with pytest.raises(ChatInstanceError) as caught:
        stand_up(
            DEPLOYMENT,
            chat_block=chat_block(),
            graph_dsn=GRAPH_DSN,
            runtime=runtime,
            environ=env(**{APP_ENV: unreachable, SECRET_ENV: "fixture-key"}),
            instance_port_override=instance_port,
            state_root=tmp_path,
            # The DEFAULT probe: a real TCP connect to a real closed port.
            ready_timeout=5.0,
            ready_interval=0.05,
        )

    assert caught.value.data["reason"] == "app_database_unreachable"
    assert caught.value.data["resolved"]["port"] == closed_port
    assert VENDOR_DEFAULT_STORE in (caught.value.fix_hint or "")
    # The DSN carries a password; the refusal must not echo it.
    assert "pw" not in str(caught.value.data.get("detail", ""))

    # Post-state: no container, so no store.
    container = container_name_for(DEPLOYMENT)
    assert runtime.run_calls == []
    assert vendor_default_store_present(runtime, container) is False
    assert read_instance(DEPLOYMENT, state_root=tmp_path) is None

    # The probe's positive control: an instance launched with no DATABASE_URL
    # is exactly the case F1 describes, and the probe reports it.
    spec = build_instance_spec(
        "vendor-default-control",
        app_database=AppDatabase(
            env_var=APP_ENV,
            target=normalize_dsn(DISTINCT_DSN, label="app"),
            dsn=DISTINCT_DSN,
            graph_target=normalize_dsn(GRAPH_DSN, label="graph"),
        ),
        secret_env=SECRET_ENV,
        environ=env(**{SECRET_ENV: "fixture-key"}),
        port_plan=resolve_instance_port(
            "vendor-default-control", chat_block(), override=free_port(),
        ),
    )
    stripped = type(spec)(
        deployment=spec.deployment,
        container="vendor-default-control",
        image=spec.image,
        host=spec.host,
        host_port=spec.host_port,
        container_port=spec.container_port,
        env={"WEBUI_SECRET_KEY": "fixture-key"},  # no DATABASE_URL: F1's case
        labels=spec.labels,
    )
    runtime.run(stripped)
    assert vendor_default_store_present(runtime, "vendor-default-control") is True


# ---------------------------------------------------------------------------
# The manifest cannot carry the DSN
# ---------------------------------------------------------------------------

#: Any key that could hold a connection string. ``_env``-suffixed names are
#: exempt by construction: they name a VARIABLE, which is the indirection the
#: no-secrets rule exists to force.
_DSN_SHAPED = re.compile(
    r"(dsn|database_url|db_url|conn(ection)?_?string|app_database|postgres)",
    re.IGNORECASE,
)


def test_manifest_schema_admits_no_app_database_dsn_key():
    """The schema is structurally incapable of carrying the app DSN.

    Read out of ``chat_block.schema_field_census()`` at every path, not just
    the top level, so a ``dsn:`` nested under ``chat.mcp`` would be caught.
    Mechanical rather than a promise: this fails the moment somebody adds one.
    """
    census = schema_field_census()
    assert census, "the census is empty; this assertion would pass vacuously"
    offenders = [
        f"{path}.{key}"
        for path, keys in census.items()
        for key in keys
        if _DSN_SHAPED.search(key) and not key.endswith("_env")
    ]
    assert offenders == []

    # Non-vacuity: the pattern does match a DSN-shaped name.
    assert _DSN_SHAPED.search("app_database_dsn")


# ---------------------------------------------------------------------------
# The distinct-database arm. Real Postgres, real instance, real row.
# ---------------------------------------------------------------------------

def _create_chat_account(base_url: str, email: str) -> None:
    """Sign one account up THROUGH the instance's own API.

    The scenario's "one chat account is created in it". Created through the
    HTTP surface rather than by an INSERT, because the row has to be one the
    instance wrote: a row this test inserted would prove the test can reach the
    database, which was never in doubt.
    """
    import json as _json
    import urllib.request

    payload = _json.dumps(
        {"name": "OKG Arm", "email": email, "password": "arm-fixture-password"}
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/v1/auths/signup",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
        assert response.status == 200, (
            f"signup answered {response.status}; without an account row this "
            f"arm has nothing to locate"
        )


@pytest.mark.postgres
@pytest.mark.slow
@pytest.mark.chat_live
def test_app_row_lands_in_app_database_and_not_in_graph_database(tmp_path):
    """A ROW the instance wrote is in the app database and not the graph one.

    Deselected from the hermetic run (``-m "not postgres"``) because it needs
    two real Postgres databases and a real Open WebUI container: no stand-in
    can produce a row that Open WebUI's own migrations wrote. It FAILS rather
    than skips when its inputs are absent — an arm that did not run is not an
    arm that passed, and a skip in this suite would read as one.

    Rows, not tables. The acceptance is explicit that a table-level comparison
    does not close it: "a query returning zero rows on BOTH sides fails this
    arm — the row must be observed present somewhere first". Counting tables
    answers a different question (did migrations run) and its zero side is
    satisfied by any database nothing ever connected to, including an empty
    one. So the assertion below is the account row, located by the address it
    was signed up with, and the graph side is the same query.

    Inputs, both operator state and neither ever a literal in this file:
    ``OKG_CHAT_TEST_APP_DSN`` and ``OKG_CHAT_TEST_GRAPH_DSN``, naming two
    databases this test may write to. Point them at scratch databases: the
    instance runs its full migration set against the app one.
    """
    import os

    import psycopg

    from okg.substrate.chat.instance import DockerRuntime, tear_down

    app_dsn = os.environ.get("OKG_CHAT_TEST_APP_DSN")
    graph_dsn = os.environ.get("OKG_CHAT_TEST_GRAPH_DSN")
    if not app_dsn or not graph_dsn:
        pytest.fail(
            "this arm needs OKG_CHAT_TEST_APP_DSN and OKG_CHAT_TEST_GRAPH_DSN "
            "naming two scratch Postgres databases, plus a working container "
            "runtime. Unconfigured is NOT passed"
        )
    assert normalize_dsn(app_dsn, label="app") != normalize_dsn(
        graph_dsn, label="graph",
    ), "the two test DSNs resolve to one database; this arm would be vacuous"

    runtime = DockerRuntime()
    port = free_port()
    deployment = "chat-instance-app-db-arm"
    email = "arm@okg.invalid"
    try:
        report = stand_up(
            deployment,
            chat_block=chat_block(),
            graph_dsn=graph_dsn,
            runtime=runtime,
            environ={
                APP_ENV: app_dsn,
                SECRET_ENV: os.environ.get(SECRET_ENV, "arm-fixture-key"),
            },
            instance_port_override=port,
            state_root=tmp_path,
            ready_timeout=180.0,
        )
        assert report.version
        _create_chat_account(report.base_url, email)

        def account_rows(dsn: str) -> int:
            """Rows in the instance's account table, or 0 when it has none.

            A missing table is a stronger absence than an empty one, and both
            answer "no account row here", so the two collapse to a count. The
            arm's non-vacuity comes from the app side being asserted NON-zero
            first, not from this function.
            """
            with psycopg.connect(dsn) as conn:
                present = conn.execute(
                    "SELECT to_regclass('public.\"user\"') IS NOT NULL"
                ).fetchone()
                if not present or not present[0]:
                    return 0
                row = conn.execute(
                    'SELECT count(*) FROM "user" WHERE email = %s', (email,),
                ).fetchone()
                return int(row[0]) if row else 0

        app_rows = account_rows(app_dsn)
        graph_rows = account_rows(graph_dsn)
        # Observed PRESENT first: zero on both sides is the vacuous pass the
        # acceptance names, and it is what a table-level count could not tell
        # apart from an empty database nothing connected to.
        assert app_rows == 1, (
            f"the instance wrote no account row for {email} into the app "
            f"database; the arm cannot distinguish isolation from an instance "
            f"that wrote nothing"
        )
        assert graph_rows == 0
    finally:
        tear_down(deployment, runtime=runtime, state_root=tmp_path)
