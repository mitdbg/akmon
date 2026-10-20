from __future__ import annotations

import logging
import shlex
from urllib.parse import quote

import pytest

from okg.substrate.db.connection import dsn_from_env, redact, redact_value
from okg.substrate.errors import SubstrateError


def test_dsn_from_env_returns_configured_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_TEST_FIXTURE_DSN", "postgres://user:pass@host/db")

    assert dsn_from_env("OKG_TEST_FIXTURE_DSN") == "postgres://user:pass@host/db"


def test_dsn_from_env_rejects_invalid_or_missing_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OKG_MISSING_FIXTURE_DSN", raising=False)

    with pytest.raises(SubstrateError, match="invalid dsn_env name"):
        dsn_from_env("not-valid")
    with pytest.raises(SubstrateError, match="is not set") as exc_info:
        dsn_from_env("OKG_MISSING_FIXTURE_DSN")

    assert exc_info.value.code == "dsn_not_set"


def test_redact_hides_password() -> None:
    assert (
        redact("postgres://user:secret@localhost/db")
        == "postgres://user:***@localhost/db"
    )
    nested = {"command": ["pg_dump", "postgres://user:secret@localhost/db"]}
    assert redact_value(nested) == {
        "command": ["pg_dump", "postgres://user:***@localhost/db"]
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "host=localhost dbname=okg user=ci password='secret with space'",
            "host=localhost dbname=okg user=ci password=***",
        ),
        (
            "host=localhost password=my secret dbname=okg user=ci",
            "host=localhost password=*** dbname=okg user=ci",
        ),
        (
            "host=localhost password=my\nsecret dbname=okg user=ci",
            "host=localhost password=*** dbname=okg user=ci",
        ),
        # An unterminated quote runs to the end: the shared masker cannot
        # know where the operator meant the value to stop, so it over-masks.
        (
            "host=localhost password='my secret dbname=okg user=ci",
            "host=localhost password=***",
        ),
        (
            "postgresql://ci@localhost/okg?sslmode=require&password=secret",
            "postgresql://ci@localhost/okg?sslmode=require&password=***",
        ),
        # A query password runs to the end of the DSN: a raw `&` inside it
        # would otherwise show the rest of the password as a "parameter".
        (
            "postgresql://ci@localhost/okg?password=secret&sslmode=require",
            "postgresql://ci@localhost/okg?password=***",
        ),
        (
            "postgresql://ci@localhost/okg?passfile=/private/credential",
            "postgresql://ci@localhost/okg?passfile=***",
        ),
        # libpq accepts an empty username in the URI authority; the earlier
        # `[^:/@]+` let this spelling through with the password intact.
        (
            "postgresql://:secret@localhost/okg",
            "postgresql://:***@localhost/okg",
        ),
        # Design section 1.6 item 2: a raw `@` in a URL password. The old
        # masker stopped at the FIRST `@` and showed `ss:w/rd`.
        (
            "postgresql://okg:Pa@ss:w/rd@db.internal:5432/okg",
            "postgresql://okg:***@db.internal:5432/okg",
        ),
        (
            "postgresql://okg:Pa%40ss%3Aw%2Frd@db.internal:5432/okg",
            "postgresql://okg:***@db.internal:5432/okg",
        ),
        # Design section 1.6 item 2: secrets the old masker did not know.
        (
            "host=db.internal sslpassword=sslS3cret dbname=okg",
            "host=db.internal sslpassword=*** dbname=okg",
        ),
        (
            "postgresql://okg@db.internal/okg?sslpassword=sslS3cret",
            "postgresql://okg@db.internal/okg?sslpassword=***",
        ),
        ("PGPASSWORD=envS3cret", "PGPASSWORD=***"),
    ],
)
def test_redact_hides_all_libpq_credential_spellings(
    value: str,
    expected: str,
) -> None:
    assert redact(value) == expected


#: A multi-line diagnostic: a libpq error whose DETAIL line carries an
#: unquoted keyword password, followed by the traceback an operator needs.
MULTI_LINE_DIAGNOSTIC = (
    "psycopg.OperationalError: connection failed\n"
    "DETAIL: host=db.internal password=Hunter2\n"
    "Traceback (most recent call last):\n"
    '  File "okg/cli.py", line 12, in main\n'
    "line 5 dbname=okg"
)
#: What the shared masker shows for it. The unquoted password ends its line,
#: so the next line may be its wrapped remainder and is masked too (the
#: one-line rule of #2425); every later line is kept.
MULTI_LINE_DIAGNOSTIC_SHOWN = (
    "psycopg.OperationalError: connection failed\n"
    "DETAIL: host=db.internal password=***\n"
    "***\n"
    '  File "okg/cli.py", line 12, in main\n'
    "line 5 dbname=okg"
)


def test_redact_value_keeps_every_line_of_a_diagnostic() -> None:
    """Design section 1.6 item 1: the old masker ran an unquoted
    `password=` to the next `key=` token, deleting the traceback lines in
    between. `redact_value` masks line by line now, and hides at most the
    one line after an unquoted password."""
    shown = redact_value({"error": [MULTI_LINE_DIAGNOSTIC]})["error"][0]

    assert "Hunter2" not in shown
    assert shown == MULTI_LINE_DIAGNOSTIC_SHOWN


def test_redact_is_still_one_dsn_across_a_line_break() -> None:
    """`redact` reads its input as ONE DSN, so a password holding a raw line
    break cannot show its second line. Multi-line text belongs to
    `dsn.redact_text`, which `redact_value` uses."""
    shown = redact("host=h password=my\nsecret dbname=okg")

    assert "secret" not in shown
    assert shown == "host=h password=*** dbname=okg"


def test_the_redact_names_are_the_shared_module() -> None:
    """The compatibility names delegate; no private masker is left here."""
    from okg.substrate.db import connection, dsn

    assert connection.redact is dsn.redact
    assert connection._redact is dsn.redact


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ""),
        ("", ""),
        (
            "postgresql://okg:x@h/okg?options=-c%20statement_timeout%3D5s",
            "-c statement_timeout=5s",
        ),
        ("host=h dbname=okg options='-c lock_timeout=1s'", "-c lock_timeout=1s"),
        ("postgresql://okg:x@h/okg", ""),
        # libpq cannot parse it: psycopg.connect raises its own error later.
        ("host=h password=my secret dbname=okg", ""),
        ("not a dsn", ""),
    ],
)
def test_dsn_options_reads_the_dsn_through_the_shared_parser(
    value: str | None, expected: str
) -> None:
    from okg.substrate.db.connection import _dsn_options

    assert _dsn_options(value) == expected


# ---------------------------------------------------------------------------
# Issue #1310 -- the idle-in-transaction bound every okg session carries.
# ---------------------------------------------------------------------------


def test_connection_kwargs_bounds_idle_in_transaction_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.db.connection import (
        DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
        IDLE_IN_TRANSACTION_TIMEOUT_ENV,
        connection_kwargs,
    )

    monkeypatch.delenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, raising=False)

    assert connection_kwargs()["options"] == (
        # The session search_path pin (issue #1968) leads every options
        # string; the idle bound follows it.
        "-c search_path=public "
        "-c idle_in_transaction_session_timeout="
        f"{DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS}"
    )


def test_idle_in_transaction_env_override_wins_and_zero_disables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.db.connection import (
        IDLE_IN_TRANSACTION_TIMEOUT_ENV,
        connection_kwargs,
        idle_in_transaction_timeout_ms,
    )

    monkeypatch.setenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, "45000")
    assert idle_in_transaction_timeout_ms() == 45000
    assert connection_kwargs()["options"] == (
        "-c search_path=public -c idle_in_transaction_session_timeout=45000"
    )

    # 0 is the only way back to the previous unbounded behavior, and it must
    # emit no idle-in-transaction setting at all rather than `...=0`, so a
    # server-level setting is left in force instead of being overridden to
    # "no bound". The search_path pin (issue #1968) is unrelated to this knob
    # and stays.
    monkeypatch.setenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, "0")
    assert idle_in_transaction_timeout_ms() == 0
    options = connection_kwargs()["options"]
    assert "idle_in_transaction_session_timeout" not in options
    assert options == "-c search_path=public"


def test_idle_in_transaction_env_refuses_a_set_but_invalid_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A masked default here would silently restore unbounded sessions."""
    from okg.substrate.db.connection import (
        IDLE_IN_TRANSACTION_TIMEOUT_ENV,
        idle_in_transaction_timeout_ms,
    )

    monkeypatch.setenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, "five minutes")
    with pytest.raises(SubstrateError, match="non-negative integer"):
        idle_in_transaction_timeout_ms()

    monkeypatch.setenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, "-1")
    with pytest.raises(SubstrateError, match="non-negative integer"):
        idle_in_transaction_timeout_ms()


def test_idle_in_transaction_reads_the_deployment_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.db import connection as conn_mod

    monkeypatch.delenv(
        conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, raising=False,
    )
    monkeypatch.setattr(
        conn_mod,
        "_manifest_idle_in_transaction_timeout_ms",
        lambda deployment: 900_000,
    )

    assert conn_mod.idle_in_transaction_timeout_ms(
        deployment="some-deployment",
    ) == 900_000
    # Explicit process env still wins over the manifest, matching `chronos`.
    monkeypatch.setenv(conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, "1000")
    assert conn_mod.idle_in_transaction_timeout_ms(
        deployment="some-deployment",
    ) == 1000


def test_unresolvable_deployment_falls_back_to_the_built_in_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connecting is not the place to refuse an unresolvable deployment."""
    from okg.substrate.db.connection import (
        DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
        IDLE_IN_TRANSACTION_TIMEOUT_ENV,
        idle_in_transaction_timeout_ms,
    )

    monkeypatch.delenv(IDLE_IN_TRANSACTION_TIMEOUT_ENV, raising=False)

    assert idle_in_transaction_timeout_ms(
        deployment="no-such-deployment-1310",
    ) == DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS


def test_malformed_manifest_value_raises_through_the_resolver(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A manifest bound that cannot be read must not degrade to the default.

    Silently using the built-in default here would leave an operator who wrote
    a malformed value believing their bound is in force when it is not.
    """
    from okg.substrate.db import connection as conn_mod
    from okg.substrate.deployments import DeploymentConfigError

    monkeypatch.delenv(
        conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, raising=False,
    )
    conn_mod._MANIFEST_IDLE_TIMEOUT_CACHE.clear()

    dep = tmp_path / "broken-bound-demo"
    (dep / "schemas").mkdir(parents=True)
    (dep / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    (dep / "deployment.yaml").write_text(
        "name: broken-bound-demo\n"
        "description: test\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "postgres:\n"
        "  idle_in_transaction_session_timeout_ms: five-minutes\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))

    with pytest.raises(DeploymentConfigError):
        conn_mod.idle_in_transaction_timeout_ms(deployment="broken-bound-demo")
    conn_mod._MANIFEST_IDLE_TIMEOUT_CACHE.clear()


def test_a_dsn_carried_options_string_survives_the_idle_bound() -> None:
    """An `options` KEYWORD replaces the DSN's own; it must be merged first.

    Without this, a DSN encoding `lock_timeout` or `statement_timeout` would
    have that silently dropped the moment the factory added its own option.
    """
    from psycopg.conninfo import make_conninfo

    from okg.substrate.db.connection import connection_kwargs

    dsn = "postgresql://u@h/db?options=-clock_timeout%3D1000ms"
    kwargs = connection_kwargs(dsn=dsn)

    assert "-clock_timeout=1000ms" in kwargs["options"]
    assert "idle_in_transaction_session_timeout" in kwargs["options"]
    # And the merged value is what libpq actually receives.
    resolved = make_conninfo(dsn, **{"options": kwargs["options"]})
    assert "-clock_timeout=1000ms" in resolved


def _capture_psycopg_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, object]:
    """Patch `psycopg.connect` and return the dict it records its call in."""
    import psycopg

    seen: dict[str, object] = {}

    def fake_connect(dsn: str, **kwargs: object) -> object:
        seen["dsn"] = dsn
        seen["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    return seen


def test_connect_forwards_row_factory_to_psycopg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`row_factory` reaches psycopg.connect, and the session settings stay.

    Callers that need a row factory on the connection used to have no way to
    ask this factory for one, so they called `psycopg.connect` directly and
    lost the keepalives, the connect timeout, and the idle-in-transaction
    bound. The passthrough exists so those callers can come back.
    """
    from psycopg.rows import dict_row

    from okg.substrate.db.connection import (
        DEFAULT_CONNECT_TIMEOUT_SECONDS,
        connect,
    )

    monkeypatch.setenv("OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS", "60000")
    seen = _capture_psycopg_connect(monkeypatch)

    connect("postgresql://u@h/db", row_factory=dict_row)

    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["row_factory"] is dict_row
    # Everything the factory exists to apply is still applied.
    assert kwargs["keepalives"] == 1
    assert kwargs["keepalives_idle"] == 30
    assert kwargs["keepalives_interval"] == 10
    assert kwargs["keepalives_count"] == 3
    assert kwargs["connect_timeout"] == DEFAULT_CONNECT_TIMEOUT_SECONDS
    assert "-c idle_in_transaction_session_timeout=60000" in kwargs["options"]


def test_connect_omits_row_factory_when_not_asked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default stays psycopg's own row factory: the key is not passed."""
    from okg.substrate.db.connection import connect

    seen = _capture_psycopg_connect(monkeypatch)

    connect("postgresql://u@h/db")

    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    assert "row_factory" not in kwargs
    assert kwargs["keepalives"] == 1


def test_connect_appends_caller_options_to_the_session_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller's `options` is ADDED to the factory's, never substituted.

    libpq treats an `options` keyword as a replacement for the connection
    string's own, so a caller that needed a `statement_timeout` used to reach
    for `psycopg.connect` and silently drop the idle-in-transaction bound and
    anything the DSN carried. Coming through the factory keeps all three.
    """
    from okg.substrate.db.connection import connect

    monkeypatch.setenv("OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS", "60000")
    seen = _capture_psycopg_connect(monkeypatch)

    connect(
        "postgresql://u@h/db?options=-c%20search_path%3Dokg",
        options="-c statement_timeout=5000",
    )

    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    options = kwargs["options"]
    assert "-c search_path=okg" in options
    assert "-c idle_in_transaction_session_timeout=60000" in options
    assert "-c statement_timeout=5000" in options


def test_connect_omits_options_when_the_caller_asks_for_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No caller `options` leaves the factory's own settings and nothing else.

    Those are the search_path pin (issue #1968) and the idle bound; a caller
    that asks for neither must not acquire a third setting from anywhere.
    """
    from okg.substrate.db.connection import connect

    monkeypatch.setenv("OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS", "60000")
    seen = _capture_psycopg_connect(monkeypatch)

    connect("postgresql://u@h/db")

    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["options"] == (
        "-c search_path=public -c idle_in_transaction_session_timeout=60000"
    )


def test_a_caller_disabling_the_idle_bound_wins_over_the_factory_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller's `idle_in_transaction_session_timeout=0` must come LAST.

    Appending is not enough on its own: libpq applies `-c` settings in order
    and the last one for a setting wins, so a caller that legitimately holds
    one transaction open across slow client-side work only actually disables
    the bound if its own value is emitted AFTER the factory's. Asserting
    membership alone would pass just as happily with the order reversed, which
    is the case where the bound still fires and kills the session.
    """
    from okg.substrate.db.connection import connect

    monkeypatch.setenv("OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS", "60000")
    seen = _capture_psycopg_connect(monkeypatch)

    connect(
        "postgresql://u@h/db",
        options="-c idle_in_transaction_session_timeout=0",
    )

    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    options = str(kwargs["options"])
    assert options.index("idle_in_transaction_session_timeout=0") > options.index(
        "idle_in_transaction_session_timeout=60000"
    ), (
        "the caller's disable must be emitted after the factory's own value; "
        f"libpq would otherwise keep the factory bound. options={options!r}"
    )


#: Call sites that hold ONE transaction open across slow client-side work and
#: therefore ask the factory to disable the idle-in-transaction bound for that
#: session. Each entry is (module path, enclosing function). Recorded here so
#: that dropping the option from one of them -- which would restore the
#: 300-second bound and kill a long clone, backfill, or merge-back gate with
#: SQLSTATE 25P03 -- fails a test rather than a production run.
_IDLE_BOUND_DISABLED_SITES = (
    ("src/okg/substrate/enrichers/backfill.py", "run_enricher_backfill"),
    ("src/okg/substrate/environment_clone.py", "create_environment_clone_backup"),
    # merge_back() runs the gated transaction in _merge_back_locked(), then
    # the post-publish index rebuild (okg#3112) after its connection closes.
    ("src/okg/substrate/merge_back.py", "_merge_back_locked"),
)


@pytest.mark.parametrize(("path", "function"), _IDLE_BOUND_DISABLED_SITES)
def test_the_long_transaction_sites_still_disable_the_idle_bound(
    path: str, function: str,
) -> None:
    """Each long-transaction site passes the disable through the factory."""
    import ast
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[2]
    tree = ast.parse((root / path).read_text(encoding="utf-8"), filename=path)
    target = next(
        (
            node for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function
        ),
        None,
    )
    assert target is not None, f"{path} no longer defines {function}()"

    disabling = [
        call for call in ast.walk(target)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "db_connect"
        and any(
            kw.arg == "options"
            and isinstance(kw.value, ast.Constant)
            and "idle_in_transaction_session_timeout=0" in str(kw.value.value)
            for kw in call.keywords
        )
    ]
    assert disabling, (
        f"{path}::{function} no longer asks the connection factory to disable "
        "the idle-in-transaction bound. It holds one transaction open across "
        "slow client-side work, so the 300-second default would terminate the "
        "session with SQLSTATE 25P03 mid-run."
    )


# ---------------------------------------------------------------------------
# Issue #1968 -- the pin is a default, and an override that does not resolve
# `public` first is worth one line of warning.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "supplied",
    [
        # Nowhere on the path at all.
        "tenant_a",
        '"tenant_a"',
        # On the path, but SHADOWED -- this is #1968's crash exactly: the okg
        # schema the migration creates resolves before public.
        "okg,public",
        "okg, public",
        # A hardened path that still forgets public.
        "pg_catalog,okg",
        "pg_catalog, okg",
    ],
)
def test_a_supplied_search_path_that_demotes_public_warns(
    caplog: pytest.LogCaptureFixture, supplied: str,
) -> None:
    """Containing `public` is not enough; it has to resolve first.

    The override still wins -- this is a warning, not a refusal -- but its
    first symptom otherwise appears far from the connection that caused it.
    """
    from okg.substrate.db.connection import connection_kwargs

    dsn = "postgresql://u@h/db?options=" + quote(
        f"-csearch_path={supplied}", safe=""
    )
    with caplog.at_level(logging.WARNING, logger="okg.substrate.db.connection"):
        options = connection_kwargs(dsn=dsn)["options"]

    assert "does not resolve 'public' first" in caplog.text, (
        "a search_path override that does not resolve public first must warn "
        f"and say why; log was {caplog.text!r}"
    )
    # The override is still emitted, and still after the pin.
    assert "search_path=public" in options


@pytest.mark.parametrize(
    "supplied",
    [
        "public",
        "public,okg",
        "public, okg",
        # A leading pg_catalog or pg_temp is searched first anyway, so naming
        # it does not demote public.
        "pg_catalog,public,okg",
        "pg_temp,public",
        "pg_catalog,pg_temp,public,okg",
    ],
)
def test_a_supplied_search_path_that_keeps_public_first_is_silent(
    caplog: pytest.LogCaptureFixture, supplied: str,
) -> None:
    """A warning on a correct override would train operators to ignore it."""
    from okg.substrate.db.connection import connection_kwargs

    dsn = "postgresql://u@h/db?options=" + quote(
        f"-csearch_path={supplied}", safe=""
    )
    with caplog.at_level(logging.WARNING, logger="okg.substrate.db.connection"):
        connection_kwargs(dsn=dsn)

    assert "does not resolve 'public' first" not in caplog.text, (
        f"an override that resolves public first must not warn; log was "
        f"{caplog.text!r}"
    )


def test_the_caller_options_path_warns_too(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The DSN is not the only way in; `options=` reaches the same session."""
    from okg.substrate.db.connection import connection_kwargs

    with caplog.at_level(logging.WARNING, logger="okg.substrate.db.connection"):
        connection_kwargs(options="-c search_path=pg_catalog,okg")

    assert "does not resolve 'public' first" in caplog.text


# ---------------------------------------------------------------------------
# Review of PR #2426: flag hints, whole-text callers, and four shapes the
# shared masker still shows.
# ---------------------------------------------------------------------------


def test_redact_value_masks_a_bare_string_line_by_line() -> None:
    """A top-level string, not only one inside a container."""
    shown = redact_value(MULTI_LINE_DIAGNOSTIC)

    assert shown == MULTI_LINE_DIAGNOSTIC_SHOWN


@pytest.mark.parametrize(
    "hint",
    [
        "pass a valid --dsn or use the worktree-isolated dev profile",
        "the --dsn value",
        "okg exchange send --deployment dep --dsn <sender-dsn>",
        'okg deployment up --dsn "${OKG_DEPLOYMENT_UP_DSN:?export '
        'OKG_DEPLOYMENT_UP_DSN to the original explicit target}" --resume',
    ],
)
def test_redact_value_and_redact_text_keep_the_word_after_dsn(hint: str) -> None:
    """Hints and runnable resume commands name `--dsn` on purpose. The
    whole-value flag rule would turn them into `--dsn <redacted-dsn>`."""
    from okg.substrate.sources.redaction import redact_text

    assert redact_value(hint) == hint
    assert redact_value({"fix_hint": [hint]}) == {"fix_hint": [hint]}
    assert redact_text(hint) == hint


def test_redact_value_still_masks_the_password_after_dsn() -> None:
    shown = redact_value("okg up --dsn postgresql://okg:zq7secret@h/db --yes")

    assert "zq7secret" not in shown
    assert shown == "okg up --dsn postgresql://okg:***@h/db --yes"


def test_the_upgrade_resume_command_survives_redact_value(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The review reproduction: `okg deployment up` persists its resume
    command through `redact_value`; it must stay runnable."""
    from okg.substrate import deployment_upgrade as du
    from types import SimpleNamespace

    monkeypatch.delenv("OKG_DEPLOYMENT_UP_DSN", raising=False)
    options = du.UpgradeOptions(
        deployment="dep",
        profile="dev",
        dsn="postgresql://okg:zq7secret@db.internal/okg",
        include=("pact",),
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
        resume=False,
    )
    command = du._next_command(SimpleNamespace(options=options))
    assert '--dsn "${OKG_DEPLOYMENT_UP_DSN:?export' in command

    payload = redact_value({"recovery_command": command})

    assert payload == {"recovery_command": command}
    assert "zq7secret" not in payload["recovery_command"]


#: The review's whole-text sample: a URL, a traceback with a decorator line,
#: a later `@` in an e-mail address, and a line that starts with `dbname=`.
WHOLE_TEXT_STDERR = (
    "ERROR: could not reach postgresql://okg:zq7secret@db.internal:5432/okg\n"
    "Traceback (most recent call last):\n"
    '  File "okg/supervisor.py", line 40, in install\n'
    "    @retry(times=3)\n"
    "hint: contact ops@example.org\n"
    "dbname=okg user=okg"
)


def _assert_every_line_survives(shown: str) -> None:
    assert "zq7secret" not in shown
    assert shown.endswith(
        WHOLE_TEXT_STDERR.replace("zq7secret", "***").split("\n", 1)[1]
    )
    assert "postgresql://okg:***@db.internal:5432/okg" in shown


def test_the_one_dsn_masker_drops_lines_of_whole_text() -> None:
    """Why the whole-text callers moved: `redact` reads one DSN, masking to
    the LAST `@` in the input, so it drops lines and names the wrong host."""
    shown = redact(WHOLE_TEXT_STDERR)

    assert "Traceback" not in shown
    assert "example.org" in shown.splitlines()[0]


def test_the_readiness_probe_keeps_every_line_of_the_error(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from okg.substrate.cli.deployment import _ready_nomos_active_state_status

    def _boom(*args, **kwargs):
        raise RuntimeError(WHOLE_TEXT_STDERR)

    monkeypatch.setattr("psycopg.connect", _boom)
    status = _ready_nomos_active_state_status(
        deployment="okg-workspace",
        dsn="postgresql://okg:zq7secret@db.internal:5432/okg",
        manifest={
            "name": "okg-workspace",
            "nomos": {
                "runtime_enforcement": "enforce",
                "deployment_posture": {"readiness_profiles": ["dogfood"]},
            },
        },
        deployment_dir=tmp_path,
        readiness_profile="general",
    )

    _assert_every_line_survives(status["findings"][0]["message"])


def test_the_lineage_rollout_blocker_keeps_every_line_of_the_error(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import okg.substrate.cli.deployment as dep
    from okg.substrate.cli import nomos_rollout_audit as audit

    def _boom(**kwargs):
        raise RuntimeError(WHOLE_TEXT_STDERR)

    manifest = tmp_path / "deployment.yaml"
    manifest.write_text("name: dep\n")
    monkeypatch.setattr(
        dep,
        "_nomos_source_lineage_resolve_entry_dsn",
        lambda entry: ("postgresql://okg:zq7secret@db.internal:5432/okg", None),
    )
    monkeypatch.setattr(dep, "_ready_nomos_active_state_status", _boom)
    state = audit._nomos_source_lineage_runtime_rollout_state(
        {"name": "dep", "path": str(manifest)}, readiness_profile="general",
    )

    assert state["mode"] == "runtime_unavailable"
    _assert_every_line_survives(state["blocker"]["message"])


def test_the_supervisor_stderr_is_masked_line_by_line() -> None:
    """`deployment_apply` masks supervisor stderr with the same call; the
    install path around it needs a live supervisor, so the call is checked
    in the source and its output here."""
    from pathlib import Path

    from okg.substrate import deployment_apply
    from okg.substrate.db.dsn import redact_text

    source = Path(deployment_apply.__file__).read_text(encoding="utf-8")
    assert "from okg.substrate.db.connection import redact\n" not in source
    assert '"stderr": redact_text(' in source
    _assert_every_line_survives(redact_text(WHOLE_TEXT_STDERR, dsn_flags=()))


#: Shapes the review found shown on this branch but masked on dev: the port
#: rule, a quoted value that `shlex.join` glues to the next quote, and a
#: quoted or wrapped value that crosses a line break. `okg.substrate.db.dsn`
#: masks them since #2425; this pins that on every public name this change
#: routes through it.
_SHOWN_SHAPES: dict[str, str] = {
    "port_shaped_password": "postgresql://u:12/zqa1 x@h/db",
    "shlex_joined_command": shlex.join(
        ["psql", "host=h password='zqa1 zqb2' dbname=x", "-c", "select 1"]
    ),
    "quoted_password_with_line_break": "host=h password='zqa1\nzqb2' dbname=x",
    "url_wrapped_across_lines": "conn=postgresql://u:zqa1\nzqb2@h/db failed",
    "unquoted_password_across_lines": "password=zqa1\nzqb2 dbname=x",
}
_MASKER_NAMES = (
    "connection.redact",
    "connection._redact",
    "connection.redact_value",
    "redaction.redact_dsn",
    "redaction.redact_text",
)


def _shape_cases() -> list:
    return [
        pytest.param(shape, name, id=f"{shape}-{name}")
        for shape in _SHOWN_SHAPES
        for name in _MASKER_NAMES
    ]


@pytest.mark.parametrize(("shape", "name"), _shape_cases())
def test_no_public_redact_name_shows_the_review_shapes(shape: str, name: str) -> None:
    from okg.substrate.db import connection
    from okg.substrate.sources import redaction

    masker = {
        "connection.redact": connection.redact,
        "connection._redact": connection._redact,
        "connection.redact_value": connection.redact_value,
        "redaction.redact_dsn": redaction.redact_dsn,
        "redaction.redact_text": redaction.redact_text,
    }[name]
    shown = masker(_SHOWN_SHAPES[shape])

    assert "zqa1" not in shown
    assert "zqb2" not in shown


#: Shapes the reviews of the revived branch found shown through the shared
#: masker but hidden on dev: a URL user holding a blank, a bracket, `#` or
#: `?` (also with the password wrapped before its `@`), and a keyword secret
#: written `key=` at a line end, whose value libpq takes from the next word
#: after any blanks and line breaks, even a `key=`-shaped one.
_DEV_HIDDEN_SHAPES = {
    "url_user_with_a_blank": "postgresql://my user:zq5Sekr@h/db",
    "url_user_with_a_bracket": "postgresql://u[x:zq5Sekr@h/db",
    "url_user_starting_with_a_bracket": "postgresql://[u:zq5Sekr@[::1]/db",
    "url_user_with_a_hash": "postgresql://my#user:zq5Sekr@h/db",
    "url_user_shaped_like_an_ipv6_host": "postgresql://[::1]:zq5Sekr@h/db",
    "url_user_shaped_like_a_zoned_ipv6_host": "postgresql://[fe80::1%eth0]:zq5Sekr@h/db",
    "url_user_with_a_blank_wrapped": "postgresql://my user:zq5Sekr\nmore@h/db",
    "password_value_on_the_next_line": "host=h password=\nzq5Sekr dbname=x",
    "bare_password_value_on_the_next_line": "password=\nzq5Sekr",
    "sslpassword_value_on_the_next_line": "sslpassword=\nzq5Sekr dbname=x",
    "password_value_after_a_blank_line": "password=\n\nzq5Sekr",
    "password_value_shaped_like_a_key": "password=\n  x=zq5Sekr",
}


@pytest.mark.parametrize("shape", sorted(_DEV_HIDDEN_SHAPES))
@pytest.mark.parametrize("name", _MASKER_NAMES)
def test_no_public_redact_name_shows_a_shape_dev_hid(shape: str, name: str) -> None:
    from okg.substrate.db import connection
    from okg.substrate.sources import redaction

    masker = {
        "connection.redact": connection.redact,
        "connection._redact": connection._redact,
        "connection.redact_value": connection.redact_value,
        "redaction.redact_dsn": redaction.redact_dsn,
        "redaction.redact_text": redaction.redact_text,
    }[name]

    assert "zq5Sekr" not in masker(_DEV_HIDDEN_SHAPES[shape])


@pytest.mark.parametrize(
    "text",
    [
        "postgresql://[::1]:5432/db",
        "connect to postgresql://[::1]:5432/db failed",
        "see https://docs.example.org for help: retry later",
        "connect postgresql://[fe80::1%eth0]:5432/db then mail ops@example.com",
        "postgresql://[::1]:5432,[::2]:5433/db failed; ask dba@example.com",
    ],
)
def test_the_loose_user_and_empty_value_rules_leave_plain_text_alone(text: str) -> None:
    """An IPv6 host is not a user, and a URL user is read loosely only when
    an `@` follows."""
    from okg.substrate.db.dsn import redact_text

    assert redact_text(text, dsn_flags=()) == text


@pytest.mark.parametrize(
    "text",
    [
        "://[u " * 150_000,
        ("://[" + ":" * 10) * 70_000,
        "://a b " * 150_000,
        "postgresql://my user:p\n" * 40_000,
        "password=\n" * 100_000,
    ],
    ids=["open-brackets", "bracket-colons", "blank-users", "wrapped-users", "empty-values"],
)
def test_the_loose_user_and_empty_value_rules_stay_linear(text: str) -> None:
    """About 1 MB each; a per-candidate scan to the end of the text took
    25 s on the first row before the IPv6 check was bounded."""
    import time

    from okg.substrate.db.dsn import redact, redact_text

    for masker in (redact_text, redact):
        started = time.perf_counter()
        masker(text)
        assert time.perf_counter() - started < 5
