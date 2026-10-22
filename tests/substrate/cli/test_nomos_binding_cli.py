"""`okg nomos binding|provider|grant add|disable|list` (#1180).

PACT nomos-binding-commands added the binding verbs; PACT
nomos-provider-grant-commands added the provider and grant verbs at the end of
this file, under the same rules.
"""
from __future__ import annotations

import getpass
import json
from typing import Any

import psycopg
import pytest
from psycopg.rows import dict_row

from okg.substrate.cli import main as cli
from okg.substrate.cli.main import _build_parser
from okg.substrate.nomos import (
    IdentityProvider,
    load_subject_bindings,
    upsert_identity_provider,
)

DEPLOYMENT = "nomos-binding-cli"
PROVIDER_ID = "team-sso"
ISSUER = "https://auth.example.org/realms/team"


def _run(capsys: Any, *argv: str) -> tuple[int, dict[str, Any]]:
    rc = cli.main(["nomos", "binding", *argv, "--json"])
    return rc, json.loads(capsys.readouterr().out)


def _db(dsn: str) -> list[str]:
    return ["--dsn", dsn, "--deployment", DEPLOYMENT]


def _register_provider(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        upsert_identity_provider(
            conn,
            deployment_name=DEPLOYMENT,
            provider=IdentityProvider(
                provider_id=PROVIDER_ID,
                provider_type="oidc",
                issuer=ISSUER,
                audiences=("okg-chat",),
                subject_claim="email",
            ),
        )
        conn.commit()


def _default_actor(dsn: str) -> str:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT current_user")
        db_user = cur.fetchone()[0]
    return f"os:{getpass.getuser()} db:{db_user}"


def _add(capsys: Any, dsn: str, *extra: str) -> tuple[int, dict[str, Any]]:
    return _run(
        capsys, "add", *_db(dsn),
        "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org",
        *extra,
    )


def _row(dsn: str, binding_id: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT subject, status, metadata FROM okg.nomos_subject_bindings "
            "WHERE deployment_name = %s AND binding_id = %s",
            (DEPLOYMENT, binding_id),
        )
        row = cur.fetchone()
    assert row is not None
    return dict(row)


ALICE_ID = "binding:team-sso:external_subject:alice@example.org"


def test_binding_verbs_parse() -> None:
    parser = _build_parser()
    args = parser.parse_args([
        "nomos", "binding", "add", "--dsn", "postgresql://x/y",
        "--deployment", "d", "--provider-id", "p", "--claim-value", "v",
        "--subject", "s",
    ])
    assert args.nomos_binding_subcmd == "add"
    assert args.claim_path == "external_subject"
    args = parser.parse_args([
        "nomos", "binding", "disable", "--dsn", "postgresql://x/y",
        "--deployment", "d", "--binding-id", "b",
    ])
    assert args.nomos_binding_subcmd == "disable"


def test_unknown_claim_path_is_refused_by_the_parser() -> None:
    # A binding on a path the claim envelope does not know matches nothing;
    # writing one would look like success and admit nobody.
    with pytest.raises(SystemExit):
        _build_parser().parse_args([
            "nomos", "binding", "add", "--dsn", "postgresql://x/y",
            "--deployment", "d", "--provider-id", "p", "--claim-value", "v",
            "--subject", "s", "--claim-path", "email",
        ])


@pytest.mark.parametrize(
    ("flags", "needle"),
    [
        (["--effective-until", "2026-10-01T00:00:00"], "UTC offset"),
        (["--effective-until", "next tuesday"], "not an ISO-8601 time"),
        (
            [
                "--effective-from", "2026-10-02T00:00:00+00:00",
                "--effective-until", "2026-10-01T00:00:00+00:00",
            ],
            "later than",
        ),
    ],
)
def test_bad_window_is_refused_before_connecting(
    capsys: Any, flags: list[str], needle: str,
) -> None:
    # The DSN points at nothing: the refusal must come before any connect.
    rc, payload = _run(
        capsys, "add", "--dsn", "postgresql://nobody@127.0.0.1:1/none",
        "--deployment", DEPLOYMENT, "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org", "--subject", "subject:a",
        *flags,
    )
    assert rc == 1
    assert payload["status"] == "error"
    assert needle in payload["error"]


def test_help_states_the_provider_revocation_limit(capsys: Any) -> None:
    with pytest.raises(SystemExit):
        cli.main(["nomos", "binding", "disable", "--help"])
    text = " ".join(capsys.readouterr().out.split())
    assert "does not ask the identity provider whether a token was revoked" in text
    assert "until their token expires" in text


@pytest.mark.postgres
def test_add_refuses_an_unknown_provider(clean_runtime: str, capsys: Any) -> None:
    rc, payload = _run(
        capsys, "add", *_db(clean_runtime),
        "--provider-id", "not-registered",
        "--claim-value", "alice@example.org", "--subject", "subject:a",
    )
    assert rc == 1
    assert "unknown identity provider 'not-registered'" in payload["error"]
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.nomos_subject_bindings "
            "WHERE deployment_name = %s",
            (DEPLOYMENT,),
        )
        assert cur.fetchone()[0] == 0


@pytest.mark.postgres
def test_add_disable_list_round_trip(clean_runtime: str, capsys: Any) -> None:
    _register_provider(clean_runtime)
    scope = f"deployment:{DEPLOYMENT}"

    rc, added = _run(
        capsys, "add", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org",
        "--subject", "subject:team:alice",
        "--effective-until", "2099-01-01T00:00:00+00:00",
    )
    assert rc == 0, added
    binding = added["binding"]
    binding_id = "binding:team-sso:external_subject:alice@example.org"
    assert binding["binding_id"] == binding_id
    # Provider-qualified: the issuer comes from the registered provider.
    assert binding["provider_id"] == PROVIDER_ID
    assert binding["issuer"] == ISSUER
    assert binding["scope"] == scope
    assert binding["status"] == "active"
    assert binding["in_effect"] is True
    assert binding["effective_until"].startswith("2099-01-01")

    rc, role = _run(
        capsys, "add", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID,
        "--claim-path", "roles", "--claim-value", "analyst",
        "--subject", "subject:team:analyst",
    )
    assert rc == 0, role
    assert role["binding"]["claim_path"] == "roles"

    with psycopg.connect(clean_runtime) as conn:
        loaded = load_subject_bindings(conn, deployment_name=DEPLOYMENT, scope=scope)
    assert {b.binding_id for b in loaded} == {
        binding_id, "binding:team-sso:roles:analyst",
    }

    rc, disabled = _run(
        capsys, "disable", *_db(clean_runtime),
        "--binding-id", binding_id, "--reason", "left the team",
    )
    assert rc == 0, disabled
    assert disabled["changed"] is True
    assert disabled["binding"]["status"] == "disabled"
    assert disabled["binding"]["in_effect"] is False
    assert disabled["binding"]["metadata"]["disabled_reason"] == "left the team"
    assert "disabled_at" in disabled["binding"]["metadata"]
    # No other binding on alice's claim, so nothing else is disabled...
    assert disabled["also_disabled"] == []
    # ...but the role binding may still admit alice; the command says so.
    assert [row["binding_id"] for row in disabled["still_matching"]] == [
        "binding:team-sso:roles:analyst",
    ]
    assert disabled["warning"].startswith("WARNING: ")

    # The request path no longer loads it...
    with psycopg.connect(clean_runtime) as conn:
        loaded = load_subject_bindings(conn, deployment_name=DEPLOYMENT, scope=scope)
    assert [b.binding_id for b in loaded] == ["binding:team-sso:roles:analyst"]

    # ...but the row is kept, and list shows it.
    rc, listed = _run(capsys, "list", *_db(clean_runtime))
    assert rc == 0
    assert listed["binding_count"] == 2
    assert listed["in_effect_count"] == 1
    by_id = {row["binding_id"]: row for row in listed["bindings"]}
    assert by_id[binding_id]["status"] == "disabled"
    # list shows who wrote and who disabled the binding, and why.
    assert by_id[binding_id]["written_by"] == _default_actor(clean_runtime)
    assert by_id[binding_id]["disabled_by"] == _default_actor(clean_runtime)
    assert by_id[binding_id]["disabled_reason"] == "left the team"
    assert "until their token expires" in listed["revocation_limit"]

    # Disabling twice changes nothing and keeps the first timestamp.
    first_disabled_at = disabled["binding"]["metadata"]["disabled_at"]
    rc, again = _run(
        capsys, "disable", *_db(clean_runtime), "--binding-id", binding_id,
    )
    assert rc == 0
    assert again["changed"] is False
    assert again["binding"]["metadata"]["disabled_at"] == first_disabled_at

    with psycopg.connect(clean_runtime) as conn, conn.cursor(
        row_factory=dict_row,
    ) as cur:
        cur.execute(
            "SELECT count(*) AS n FROM okg.nomos_subject_bindings "
            "WHERE deployment_name = %s",
            (DEPLOYMENT,),
        )
        assert cur.fetchone()["n"] == 2


@pytest.mark.postgres
def test_disable_refuses_an_unknown_binding(clean_runtime: str, capsys: Any) -> None:
    rc, payload = _run(
        capsys, "disable", *_db(clean_runtime), "--binding-id", "binding:none",
    )
    assert rc == 1
    assert "no subject binding 'binding:none'" in payload["error"]


@pytest.mark.postgres
def test_list_marks_an_expired_binding_not_in_effect(
    clean_runtime: str, capsys: Any,
) -> None:
    _register_provider(clean_runtime)
    rc, added = _run(
        capsys, "add", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID,
        "--claim-value", "bob@example.org", "--subject", "subject:team:bob",
        "--effective-until", "2020-01-01T00:00:00+00:00",
    )
    assert rc == 0, added
    rc, listed = _run(
        capsys, "list", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
    )
    assert rc == 0
    [row] = listed["bindings"]
    assert row["status"] == "active"
    assert row["in_effect"] is False
    assert listed["in_effect_count"] == 0


@pytest.mark.parametrize(
    "argv",
    [
        [
            "add", "--provider-id", PROVIDER_ID, "--claim-value", "g",
            "--subject", "subject:a",
            "--binding-id", "okg:chat-sso:binding:team-sso:g",
        ],
        [
            "add", "--provider-id", PROVIDER_ID, "--claim-value", "g",
            "--subject", "subject:a", "--replace",
            "--binding-id", "okg:chat-sso:binding:team-sso:g",
        ],
        ["disable", "--binding-id", "okg:chat-sso:binding:team-sso:g"],
    ],
    ids=["add", "replace", "disable"],
)
def test_sso_projection_binding_ids_are_refused(
    capsys: Any, argv: list[str],
) -> None:
    # The chat SSO group projection owns these rows and rewrites them on every
    # sync. The DSN points at nothing: the refusal comes before any connect.
    rc, payload = _run(
        capsys, *argv, "--dsn", "postgresql://nobody@127.0.0.1:1/none",
        "--deployment", DEPLOYMENT,
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.binding.reserved_id"
    assert "chat SSO group projection" in payload["error"]


def test_reserved_prefix_is_the_projection_prefix() -> None:
    from okg.substrate.chat import sso_groups
    from okg.substrate.nomos import identity_store

    assert (
        identity_store.SSO_PROJECTION_BINDING_PREFIX
        == sso_groups._BINDING_PREFIX
    )


@pytest.mark.postgres
def test_store_refuses_reserved_ids_too(clean_runtime: str) -> None:
    from okg.substrate.nomos import SubjectBinding
    from okg.substrate.nomos.identity_store import (
        ReservedSubjectBindingError,
        add_subject_binding,
        disable_subject_binding,
    )

    binding = SubjectBinding(
        binding_id="okg:chat-sso:binding:x", provider_id=PROVIDER_ID,
        issuer=ISSUER, claim_path="groups", claim_value="g", subject="s",
        scope=f"deployment:{DEPLOYMENT}",
    )
    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(ReservedSubjectBindingError):
            add_subject_binding(
                conn, deployment_name=DEPLOYMENT, binding=binding, actor="t",
                replace=True,
            )
        with pytest.raises(ReservedSubjectBindingError):
            disable_subject_binding(
                conn, deployment_name=DEPLOYMENT,
                binding_id=binding.binding_id, actor="t",
            )


@pytest.mark.postgres
def test_add_refuses_an_existing_binding_and_keeps_the_disable(
    clean_runtime: str, capsys: Any,
) -> None:
    # Finding 2: add, disable, add again used to reactivate the binding and
    # wipe disabled_at/disabled_reason.
    _register_provider(clean_runtime)
    rc, added = _add(capsys, clean_runtime, "--subject", "subject:team:alice")
    assert rc == 0, added
    assert added["action"] == "added"
    rc, disabled = _run(
        capsys, "disable", *_db(clean_runtime),
        "--binding-id", ALICE_ID, "--reason", "fired",
    )
    assert rc == 0, disabled

    rc, again = _add(capsys, clean_runtime, "--subject", "subject:team:alice")
    assert rc == 1
    assert again["error_code"] == "nomos.binding.exists"
    assert "--replace" in again["error"]
    row = _row(clean_runtime, ALICE_ID)
    assert row["status"] == "disabled"
    assert row["metadata"]["disabled_reason"] == "fired"
    assert "disabled_at" in row["metadata"]


@pytest.mark.postgres
def test_replace_refuses_a_subject_change(
    clean_runtime: str, capsys: Any,
) -> None:
    _register_provider(clean_runtime)
    rc, _ = _add(capsys, clean_runtime, "--subject", "subject:team:alice")
    assert rc == 0
    rc, moved = _add(
        capsys, clean_runtime, "--subject", "subject:team:admin", "--replace",
    )
    assert rc == 1
    assert moved["error_code"] == "nomos.binding.subject_change"
    assert "add one under a new id" in moved["error"]
    row = _row(clean_runtime, ALICE_ID)
    assert row["subject"] == "subject:team:alice"
    assert [e["action"] for e in row["metadata"]["events"]] == ["added"]


@pytest.mark.postgres
def test_replace_never_reenables_a_disabled_binding(
    clean_runtime: str, capsys: Any,
) -> None:
    # Finding 2: even with --replace, an add must not undo a disable.
    _register_provider(clean_runtime)
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--actor", "ops:carol",
    )
    assert rc == 0
    rc, _ = _run(
        capsys, "disable", *_db(clean_runtime), "--binding-id", ALICE_ID,
        "--reason", "fired", "--actor", "ops:dave",
    )
    assert rc == 0
    rc, replaced = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice", "--replace",
        "--priority", "5", "--reason", "rehired",
    )
    assert rc == 1, replaced
    assert replaced["error_code"] == "nomos.binding.disabled"
    assert "new binding id" in replaced["error"]
    row = _row(clean_runtime, ALICE_ID)
    assert row["status"] == "disabled"
    meta = row["metadata"]
    assert (meta["disabled_reason"], meta["disabled_by"]) == ("fired", "ops:dave")
    assert [
        (e["action"], e["actor"], e["reason"], e["previous_status"])
        for e in meta["events"]
    ] == [
        ("added", "ops:carol", None, None),
        ("disabled", "ops:dave", "fired", "active"),
    ]
    assert all(e["at"] for e in meta["events"])


@pytest.mark.postgres
def test_replace_changes_window_and_priority_and_appends_history(
    clean_runtime: str, capsys: Any,
) -> None:
    _register_provider(clean_runtime)
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--actor", "ops:carol",
    )
    assert rc == 0
    rc, again = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice", "--replace",
        "--priority", "5", "--effective-until", "2099-01-01T00:00:00+00:00",
        "--reason", "extend",
    )
    assert rc == 0, again
    assert again["action"] == "replaced"
    binding = again["binding"]
    assert binding["status"] == "active"
    assert binding["priority"] == 5
    assert binding["effective_until"].startswith("2099-01-01")
    assert [
        (e["action"], e["reason"], e["previous_status"])
        for e in binding["metadata"]["events"]
    ] == [("added", None, None), ("replaced", "extend", "active")]
    assert binding["written_by"] == _default_actor(clean_runtime)


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("flags", "needle"),
    [
        (["--claim-value", "mallory@example.org"], "claim_value"),
        (["--scope", "deployment:elsewhere"], "scope"),
    ],
    ids=["claim-value", "scope"],
)
def test_replace_refuses_a_match_change(
    clean_runtime: str, capsys: Any, flags: list[str], needle: str,
) -> None:
    # An explicit --binding-id could otherwise be rewritten to match another
    # user: the id's decision records would then name the wrong person.
    _register_provider(clean_runtime)
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--binding-id", "binding:alice",
    )
    assert rc == 0
    argv = [
        "add", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org",
        "--subject", "subject:team:alice", "--binding-id", "binding:alice",
        "--replace", *flags,
    ]
    rc, moved = _run(capsys, *argv)
    assert rc == 1, moved
    assert moved["error_code"] == "nomos.binding.match_change"
    assert needle in moved["error"]
    with psycopg.connect(clean_runtime) as conn, conn.cursor(
        row_factory=dict_row,
    ) as cur:
        cur.execute(
            "SELECT claim_value, scope FROM okg.nomos_subject_bindings "
            "WHERE deployment_name = %s AND binding_id = 'binding:alice'",
            (DEPLOYMENT,),
        )
        assert cur.fetchone() == {
            "claim_value": "alice@example.org",
            "scope": f"deployment:{DEPLOYMENT}",
        }


@pytest.mark.postgres
def test_disable_takes_every_binding_on_the_same_claim(
    clean_runtime: str, capsys: Any,
) -> None:
    # Finding 3: a second binding on the same user kept admitting them after
    # the first was disabled. A disable now takes every active binding on
    # the same provider, scope and claim -- a future-window one included --
    # and leaves other users' bindings alone.
    _register_provider(clean_runtime)
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--binding-id", "binding:alice:a",
    )
    assert rc == 0
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--binding-id", "binding:alice:b", "--priority", "3",
    )
    assert rc == 0
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:admin",
        "--binding-id", "binding:alice:later",
        "--effective-from", "2099-01-01T00:00:00+00:00",
    )
    assert rc == 0
    rc, _ = _run(
        capsys, "add", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
        "--claim-value", "bob@example.org", "--subject", "subject:team:bob",
        "--binding-id", "binding:bob",
    )
    assert rc == 0

    rc, disabled = _run(
        capsys, "disable", *_db(clean_runtime),
        "--binding-id", "binding:alice:a", "--reason", "left",
        "--actor", "ops:dave",
    )
    assert rc == 0, disabled
    assert disabled["changed"] is True
    assert disabled["also_disabled"] == ["binding:alice:b", "binding:alice:later"]
    assert disabled["still_matching"] == []
    assert disabled["warning"] is None
    for binding_id in ("binding:alice:a", "binding:alice:b", "binding:alice:later"):
        row = _row(clean_runtime, binding_id)
        assert row["status"] == "disabled", binding_id
        meta = row["metadata"]
        assert meta["disabled_with"] == "binding:alice:a"
        assert (meta["disabled_by"], meta["disabled_reason"]) == ("ops:dave", "left")
        assert meta["events"][-1]["action"] == "disabled"
    assert _row(clean_runtime, "binding:bob")["status"] == "active"

    # A repeat disable changes nothing and takes nothing else, even a
    # binding added for the same user since.
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--binding-id", "binding:alice:new",
    )
    assert rc == 0
    rc, again = _run(
        capsys, "disable", *_db(clean_runtime), "--binding-id", "binding:alice:a",
    )
    assert rc == 0
    assert (again["changed"], again["also_disabled"]) == (False, [])
    assert [row["binding_id"] for row in again["still_matching"]] == [
        "binding:alice:new",
    ]
    assert _row(clean_runtime, "binding:alice:new")["status"] == "active"


@pytest.mark.postgres
def test_actor_is_recorded_on_add_and_disable(
    clean_runtime: str, capsys: Any,
) -> None:
    _register_provider(clean_runtime)
    rc, added = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--actor", "ops:carol",
    )
    assert rc == 0
    assert added["binding"]["written_by"] == "ops:carol"
    rc, disabled = _run(
        capsys, "disable", *_db(clean_runtime), "--binding-id", ALICE_ID,
    )
    assert rc == 0
    assert disabled["binding"]["disabled_by"] == _default_actor(clean_runtime)
    assert disabled["binding"]["written_by"] == "ops:carol"
    rc, listed = _run(capsys, "list", *_db(clean_runtime))
    [row] = listed["bindings"]
    assert (row["written_by"], row["disabled_by"]) == (
        "ops:carol", _default_actor(clean_runtime),
    )
    # The recorded actor is a name: the DSN's password never reaches it.
    password = psycopg.conninfo.conninfo_to_dict(clean_runtime).get("password")
    if password:
        assert password not in json.dumps(row)


def test_empty_actor_is_refused(capsys: Any) -> None:
    rc, payload = _run(
        capsys, "disable", "--dsn", "postgresql://nobody@127.0.0.1:1/none",
        "--deployment", DEPLOYMENT, "--binding-id", "b", "--actor", " ",
    )
    assert rc == 1
    assert "--actor must not be empty" in payload["error"]


# --- Review round 2 (PR #2737): replace keeps what it is not given, alias
# claim paths, lock order, database errors. ---------------------------------


def _store_binding(
    dsn: str,
    binding_id: str,
    *,
    claim_path: str = "external_subject",
    claim_value: str = "alice@example.org",
    subject: str = "subject:team:alice",
    required_assurance: tuple[str, ...] = (),
) -> None:
    """Write a row through the store helper, as older tooling or the SSO
    projection would: paths and assurance the CLI cannot set."""
    from okg.substrate.nomos import SubjectBinding, upsert_subject_binding

    with psycopg.connect(dsn) as conn:
        upsert_subject_binding(
            conn,
            deployment_name=DEPLOYMENT,
            binding=SubjectBinding(
                binding_id=binding_id, provider_id=PROVIDER_ID, issuer=ISSUER,
                claim_path=claim_path, claim_value=claim_value,
                subject=subject, scope=f"deployment:{DEPLOYMENT}",
                required_assurance=required_assurance,
            ),
        )
        conn.commit()


def _window(dsn: str, binding_id: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT priority, required_assurance, effective_from, "
            "effective_until, status, metadata FROM okg.nomos_subject_bindings "
            "WHERE deployment_name = %s AND binding_id = %s",
            (DEPLOYMENT, binding_id),
        )
        row = cur.fetchone()
    assert row is not None
    return dict(row)


def test_clear_window_flags_need_replace(capsys: Any) -> None:
    rc, payload = _run(
        capsys, "add", "--dsn", "postgresql://nobody@127.0.0.1:1/none",
        "--deployment", DEPLOYMENT, "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org", "--subject", "subject:a",
        "--clear-effective-until",
    )
    assert rc == 1
    assert "only with --replace" in payload["error"]


def test_clear_and_set_of_one_window_bound_are_exclusive() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args([
            "nomos", "binding", "add", "--dsn", "postgresql://x/y",
            "--deployment", "d", "--provider-id", "p", "--claim-value", "v",
            "--subject", "s", "--replace",
            "--effective-until", "2099-01-01T00:00:00+00:00",
            "--clear-effective-until",
        ])


@pytest.mark.postgres
def test_replace_keeps_the_required_assurance(
    clean_runtime: str, capsys: Any,
) -> None:
    # Review finding 1: `add --replace` rebuilt the binding with no
    # required_assurance, silently dropping an MFA requirement.
    _register_provider(clean_runtime)
    _store_binding(
        clean_runtime, "binding:alice:mfa", required_assurance=("mfa",),
    )
    rc, replaced = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--binding-id", "binding:alice:mfa", "--replace", "--priority", "4",
    )
    assert rc == 0, replaced
    assert replaced["binding"]["required_assurance"] == ["mfa"]
    assert replaced["binding"]["priority"] == 4
    assert _window(clean_runtime, "binding:alice:mfa")["required_assurance"] == [
        "mfa",
    ]

    # A store caller naming a different assurance is refused, never applied.
    from okg.substrate.nomos import SubjectBinding
    from okg.substrate.nomos.identity_store import (
        SubjectBindingMatchChangeError,
        add_subject_binding,
    )

    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(SubjectBindingMatchChangeError, match="required_assurance"):
            add_subject_binding(
                conn, deployment_name=DEPLOYMENT, actor="t", replace=True,
                binding=SubjectBinding(
                    binding_id="binding:alice:mfa", provider_id=PROVIDER_ID,
                    issuer=ISSUER, claim_path="external_subject",
                    claim_value="alice@example.org",
                    subject="subject:team:alice",
                    scope=f"deployment:{DEPLOYMENT}",
                    required_assurance=("password",),
                ),
            )
    assert _window(clean_runtime, "binding:alice:mfa")["required_assurance"] == [
        "mfa",
    ]


@pytest.mark.postgres
def test_replace_keeps_the_window_and_priority_it_is_not_given(
    clean_runtime: str, capsys: Any,
) -> None:
    # Review finding 2: `add --replace` without window flags cleared both
    # bounds, so a binding that expired tomorrow never expired.
    _register_provider(clean_runtime)
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--priority", "3",
        "--effective-from", "2020-01-01T00:00:00+00:00",
        "--effective-until", "2099-01-01T00:00:00+00:00",
    )
    assert rc == 0

    rc, replaced = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice", "--replace",
        "--reason", "touch",
    )
    assert rc == 0, replaced
    row = _window(clean_runtime, ALICE_ID)
    assert row["priority"] == 3
    assert row["effective_from"].isoformat().startswith("2020-01-01")
    assert row["effective_until"].isoformat().startswith("2099-01-01")
    event = row["metadata"]["events"][-1]
    assert event["action"] == "replaced"
    assert event["before"]["priority"] == 3
    assert event["before"]["effective_until"].startswith("2099-01-01")

    # Moving the end keeps the start and the priority.
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice", "--replace",
        "--effective-until", "2098-01-01T00:00:00+00:00",
    )
    assert rc == 0
    row = _window(clean_runtime, ALICE_ID)
    assert (row["priority"], row["effective_from"].year, row["effective_until"].year) == (
        3, 2020, 2098,
    )

    # Removing a bound takes an explicit flag, and removes only that bound.
    rc, cleared = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice", "--replace",
        "--clear-effective-until",
    )
    assert rc == 0, cleared
    row = _window(clean_runtime, ALICE_ID)
    assert row["effective_until"] is None
    assert row["effective_from"].year == 2020
    assert row["priority"] == 3


@pytest.mark.postgres
def test_replace_refuses_a_window_that_ends_before_the_kept_start(
    clean_runtime: str, capsys: Any,
) -> None:
    _register_provider(clean_runtime)
    rc, _ = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice",
        "--effective-from", "2090-01-01T00:00:00+00:00",
    )
    assert rc == 0
    rc, refused = _add(
        capsys, clean_runtime, "--subject", "subject:team:alice", "--replace",
        "--effective-until", "2080-01-01T00:00:00+00:00",
    )
    assert rc == 1, refused
    assert refused["error_code"] == "nomos.binding.window"
    row = _window(clean_runtime, ALICE_ID)
    assert row["effective_until"] is None
    assert row["effective_from"].year == 2090


def test_claim_values_read_through_the_alias_table() -> None:
    from okg.substrate.nomos import IdentityClaimEnvelope
    from okg.substrate.nomos.identity import (
        CLAIM_PATH_ALIASES,
        canonical_claim_path,
        claim_path_aliases,
    )

    claim = IdentityClaimEnvelope(
        provider_id=PROVIDER_ID, provider_type="oidc", issuer=ISSUER,
        external_subject="alice@example.org", audiences=("okg-chat",),
        groups=("g",), roles=("r",), entitlements=("e",),
        assurance_values=("mfa",), mfa_satisfied=True, realm="R",
        service_principal="svc",
    )
    # What claim_values returned for each path before the table existed
    # (ac078aabd5): the refactor must not change a single answer.
    user, groups, roles, ents = ("alice@example.org",), ("g",), ("r",), ("e",)
    expected = {
        "external_subject": user, "subject": user, "sub": user,
        "kerberos_username": user, "mit_kerberos_username": user,
        "groups": groups, "group": groups, "roles": roles, "role": roles,
        "entitlements": ents, "entitlement": ents,
        "assurance": ("mfa",), "acr": ("mfa",),
        "mfa": ("true",), "mfa_satisfied": ("true",),
        "realm": ("R",), "kerberos_realm": ("R",),
        "service_principal": ("svc",), "spn": ("svc",),
        "email": (), "": (),
    }
    assert set(CLAIM_PATH_ALIASES) == set(expected) - {"email", ""}
    for path, values in expected.items():
        assert claim.claim_values(path) == values, path
    assert canonical_claim_path("email") is None
    assert claim_path_aliases("sub") == (
        "external_subject", "kerberos_username", "mit_kerberos_username",
        "sub", "subject",
    )
    assert claim_path_aliases("email") == ("email",)


@pytest.mark.postgres
def test_disable_takes_rows_on_an_alias_of_the_same_claim(
    clean_runtime: str, capsys: Any,
) -> None:
    # Review finding 3: the cascade compared claim paths exactly, so a row
    # for the same user under `sub` survived the disable and kept admitting
    # them; the still-matching check then called it `if_token_carries`.
    from okg.substrate.nomos import IdentityClaimEnvelope

    _register_provider(clean_runtime)
    rc, _ = _add(capsys, clean_runtime, "--subject", "subject:team:alice")
    assert rc == 0
    _store_binding(clean_runtime, "binding:alice:sub", claim_path="sub")
    _store_binding(
        clean_runtime, "binding:alice:kerberos", claim_path="kerberos_username",
    )
    # The SSO projection owns this one; the cascade must leave it, and the
    # warning must say it admits alice for certain.
    _store_binding(
        clean_runtime, "okg:chat-sso:binding:team-sso:alice", claim_path="subject",
    )
    _store_binding(
        clean_runtime, "binding:bob:sub", claim_path="sub",
        claim_value="bob@example.org", subject="subject:team:bob",
    )

    rc, disabled = _run(
        capsys, "disable", *_db(clean_runtime), "--binding-id", ALICE_ID,
    )
    assert rc == 0, disabled
    assert disabled["also_disabled"] == ["binding:alice:kerberos", "binding:alice:sub"]
    assert [
        (row["binding_id"], row["match"]) for row in disabled["still_matching"]
    ] == [("okg:chat-sso:binding:team-sso:alice", "same_claim")]
    assert _row(clean_runtime, "binding:bob:sub")["status"] == "active"

    # What the request gate would do: only the projection-owned row still
    # matches alice's verified claim.
    scope = f"deployment:{DEPLOYMENT}"
    alice = IdentityClaimEnvelope(
        provider_id=PROVIDER_ID, provider_type="oidc", issuer=ISSUER,
        external_subject="alice@example.org", audiences=("okg-chat",),
    )
    with psycopg.connect(clean_runtime) as conn:
        loaded = load_subject_bindings(conn, deployment_name=DEPLOYMENT, scope=scope)
    assert [b.binding_id for b in loaded if b.matches(alice, scope=scope)] == [
        "okg:chat-sso:binding:team-sso:alice",
    ]


def _wait_for_lock_wait(dsn: str, pid: int) -> None:
    import time

    deadline = time.monotonic() + 20
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        while time.monotonic() < deadline:
            cur.execute(
                "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s",
                (pid,),
            )
            found = cur.fetchone()
            if found is not None and found[0] == "Lock":
                return
            time.sleep(0.05)
    raise AssertionError(f"backend {pid} never waited on a lock")


@pytest.mark.postgres
def test_disable_locks_the_named_row_and_its_siblings_in_id_order(
    clean_runtime: str, capsys: Any,
) -> None:
    # Review finding 4: disable locked the named row first and its siblings
    # second, so disables of two sibling rows could each hold one and wait
    # for the other (a deadlock). Proof of the lock order: while a sibling
    # that sorts BEFORE the named row is held elsewhere, the disable must
    # wait on it without having locked the named row yet.
    import threading

    from okg.substrate.nomos.identity_store import disable_subject_binding

    _register_provider(clean_runtime)
    # Insert b before a so the table's storage order is the reverse of id
    # order: without ORDER BY binding_id the lock pass would take b first.
    for binding_id in ("binding:alice:b", "binding:alice:a"):
        rc, _ = _add(
            capsys, clean_runtime, "--subject", "subject:team:alice",
            "--binding-id", binding_id,
        )
        assert rc == 0

    blocker = psycopg.connect(clean_runtime)
    worker = psycopg.connect(clean_runtime)
    result: dict[str, Any] = {}
    thread: threading.Thread | None = None
    try:
        with blocker.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM okg.nomos_subject_bindings WHERE deployment_name = %s "
                "AND binding_id = 'binding:alice:a' FOR UPDATE",
                (DEPLOYMENT,),
            )

        def run() -> None:
            try:
                result["row"] = disable_subject_binding(
                    worker, deployment_name=DEPLOYMENT,
                    binding_id="binding:alice:b", actor="t",
                )
                worker.commit()
            except BaseException as exc:  # surfaced below
                result["error"] = exc

        thread = threading.Thread(target=run)
        thread.start()
        _wait_for_lock_wait(clean_runtime, worker.info.backend_pid)
        with psycopg.connect(clean_runtime) as probe, probe.cursor() as cur:
            # Raises LockNotAvailable if the disable took the named row
            # before its siblings.
            cur.execute(
                "SELECT 1 FROM okg.nomos_subject_bindings WHERE deployment_name = %s "
                "AND binding_id = 'binding:alice:b' FOR UPDATE NOWAIT",
                (DEPLOYMENT,),
            )
            probe.rollback()
    finally:
        # Release the held sibling first so the worker thread can finish
        # before its connection is closed, whichever way the probe went.
        blocker.rollback()
        if thread is not None:
            thread.join(timeout=20)
            assert not thread.is_alive()
        blocker.close()
        worker.close()
    assert "error" not in result, result.get("error")
    assert result["row"]["also_disabled"] == ["binding:alice:a"]
    assert _row(clean_runtime, "binding:alice:a")["status"] == "disabled"
    assert _row(clean_runtime, "binding:alice:b")["status"] == "disabled"


@pytest.mark.postgres
def test_a_database_error_is_a_json_error_not_a_traceback(
    clean_runtime: str, capsys: Any,
) -> None:
    # Review finding 4: a deadlock or lock timeout during a binding command
    # escaped as a raw traceback. It is now the command's JSON error, exit 1,
    # and the rolled-back transaction changed nothing.
    _register_provider(clean_runtime)
    rc, _ = _add(capsys, clean_runtime, "--subject", "subject:team:alice")
    assert rc == 0
    impatient = psycopg.conninfo.make_conninfo(
        clean_runtime, options="-c lock_timeout=200",
    )
    with psycopg.connect(clean_runtime) as blocker:
        with blocker.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM okg.nomos_subject_bindings WHERE deployment_name = %s "
                "AND binding_id = %s FOR UPDATE",
                (DEPLOYMENT, ALICE_ID),
            )
        rc, payload = _run(
            capsys, "disable", "--dsn", impatient, "--deployment", DEPLOYMENT,
            "--binding-id", ALICE_ID,
        )
        blocker.rollback()
    assert rc == 1, payload
    assert payload["status"] == "error"
    assert payload["error_code"] == "nomos.binding.database_error"
    assert payload["sqlstate"] == "55P03"
    assert "nothing was changed" in payload["error"]
    assert _row(clean_runtime, ALICE_ID)["status"] == "active"


@pytest.mark.postgres
def test_a_failed_commit_says_the_outcome_is_unknown(
    clean_runtime: str, capsys: Any, monkeypatch: Any,
) -> None:
    # Review round 2: an error DURING commit (a dropped connection) may come
    # after the server committed, so "nothing was changed" could be false.
    _register_provider(clean_runtime)
    rc, _ = _add(capsys, clean_runtime, "--subject", "subject:team:alice")
    assert rc == 0

    def lost_connection(self: Any) -> None:
        raise psycopg.OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(psycopg.Connection, "commit", lost_connection)
    rc, payload = _run(
        capsys, "disable", *_db(clean_runtime), "--binding-id", ALICE_ID,
    )
    monkeypatch.undo()
    assert rc == 1, payload
    assert payload["error_code"] == "nomos.binding.commit_outcome_unknown"
    assert "may or may not have been saved" in payload["error"]
    assert "okg nomos binding list" in payload["error"]
    assert "nothing was changed" not in payload["error"]


# --- `okg nomos provider add|disable|list` and `okg nomos grant add|disable|list`
# (PACT nomos-provider-grant-commands, #1180).

OIDC_BLOCK = {
    "provider_id": "cms-sso",
    "issuer": "https://auth.example.org/realms/cms",
    "audience": "example-kb",
    "subject_claim": "email",
    "roles_claim": "cms_roles",
    "principal_kind": "human_operator",
}
NOWHERE = ["--dsn", "postgresql://nobody@127.0.0.1:1/none", "--deployment", DEPLOYMENT]


def _run_noun(capsys: Any, noun: str, *argv: str) -> tuple[int, dict[str, Any]]:
    rc = cli.main(["nomos", noun, *argv, "--json"])
    return rc, json.loads(capsys.readouterr().out)


def _declare_oidc_auth(monkeypatch: Any, block: dict[str, Any] | None) -> None:
    """Make the deployment's manifest declare ``block`` as nomos.oidc_auth."""
    import types

    import okg.substrate.deployments as deployments

    manifest = types.SimpleNamespace(
        nomos={} if block is None else {"oidc_auth": dict(block)},
    )
    monkeypatch.setattr(deployments, "resolve_deployment", lambda name: manifest)


def _provider_row(dsn: str, provider_id: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT status, issuer, audiences, metadata, updated_at "
            "FROM okg.nomos_identity_providers "
            "WHERE deployment_name = %s AND provider_id = %s",
            (DEPLOYMENT, provider_id),
        )
        row = cur.fetchone()
    assert row is not None
    return dict(row)


def _grant_row(dsn: str, grant_id: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT subject, status, effect, actions, metadata "
            "FROM okg.nomos_permission_grants "
            "WHERE deployment_name = %s AND grant_id = %s",
            (DEPLOYMENT, grant_id),
        )
        row = cur.fetchone()
    assert row is not None
    return dict(row)


def _add_provider(capsys: Any, dsn: str, *extra: str) -> tuple[int, dict[str, Any]]:
    return _run_noun(
        capsys, "provider", "add", *_db(dsn),
        "--provider-id", PROVIDER_ID, "--issuer", ISSUER,
        "--audience", "okg-chat", "--subject-claim", "email",
        *extra,
    )


def _add_grant(
    capsys: Any, dsn: str, grant_id: str, *extra: str,
) -> tuple[int, dict[str, Any]]:
    return _run_noun(
        capsys, "grant", "add", *_db(dsn), "--grant-id", grant_id, *extra,
    )


def _decide(dsn: str, *, email: str = "alice@example.org") -> Any:
    """Run the request gate's evaluation on the rows the commands wrote."""
    from okg.substrate.nomos import (
        evaluate_identity_claim,
        load_identity_providers,
        load_permission_grants,
        normalize_identity_claim,
    )

    scope = f"deployment:{DEPLOYMENT}"
    with psycopg.connect(dsn) as conn:
        providers = load_identity_providers(conn, deployment_name=DEPLOYMENT)
        bindings = load_subject_bindings(conn, deployment_name=DEPLOYMENT, scope=scope)
        grants = load_permission_grants(conn, deployment_name=DEPLOYMENT, scope=scope)
    now = "2026-10-01T12:00:00Z"
    claim = normalize_identity_claim(
        {
            "provider_id": PROVIDER_ID,
            "verification_status": "valid",
            "issuer": ISSUER,
            "claims": {
                "email": email,
                "aud": "okg-chat",
                "exp": "2026-10-01T13:00:00Z",
            },
        },
        providers,
        now=now,
    )
    return evaluate_identity_claim(
        None,
        claim,
        bindings=bindings,
        permission_grants=grants,
        action="graph.read.mcp",
        resource={"source_id": "public_docs"},
        scope=scope,
        now=now,
    )


def test_provider_and_grant_verbs_parse() -> None:
    parser = _build_parser()
    for argv, attr, verb in (
        (["provider", "add", "--provider-id", "p"], "nomos_provider_subcmd", "add"),
        (["provider", "disable", "--provider-id", "p"], "nomos_provider_subcmd", "disable"),
        (["provider", "list"], "nomos_provider_subcmd", "list"),
        (
            ["grant", "add", "--grant-id", "g", "--subject", "s", "--action", "a"],
            "nomos_grant_subcmd",
            "add",
        ),
        (["grant", "disable", "--grant-id", "g"], "nomos_grant_subcmd", "disable"),
        (["grant", "list"], "nomos_grant_subcmd", "list"),
    ):
        args = parser.parse_args([
            "nomos", *argv, "--dsn", "postgresql://x/y", "--deployment", "d",
        ])
        assert getattr(args, attr) == verb


def test_grant_effect_is_allow_or_deny() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args([
            "nomos", "grant", "add", "--dsn", "postgresql://x/y",
            "--deployment", "d", "--grant-id", "g", "--subject", "s",
            "--action", "a", "--effect", "maybe",
        ])


@pytest.mark.parametrize(
    ("argv", "needle"),
    [
        (["provider", "add", "--provider-id", "p"], "needs --issuer, --audience"),
        (
            ["grant", "add", "--grant-id", "g", "--subject", "s", "--action", "a",
             "--match", "source_id"],
            "KEY=VALUE",
        ),
        (
            ["grant", "add", "--grant-id", "g", "--subject", "s", "--action", "a",
             "--match", "k=1", "--match", "k=2"],
            "names 'k' twice",
        ),
        (
            ["grant", "add", "--grant-id", "g", "--subject", "s", "--action", " "],
            "empty value",
        ),
    ],
)
def test_malformed_provider_or_grant_is_refused_before_connecting(
    capsys: Any, monkeypatch: Any, argv: list[str], needle: str,
) -> None:
    _declare_oidc_auth(monkeypatch, None)
    rc, payload = _run_noun(capsys, argv[0], *argv[1:], *NOWHERE)
    assert rc == 1
    assert payload["status"] == "error"
    assert needle in payload["error"]
    assert "database error" not in payload["error"]


@pytest.mark.parametrize(
    ("argv", "needle"),
    [
        # No trusted verifier: the server's start check refuses the row.
        (
            ["--provider-id", "cms-sso", "--issuer", OIDC_BLOCK["issuer"],
             "--audience", "example-kb", "--subject-claim", "email"],
            "mcp-oidc:jwks",
        ),
        # The subject claim defaults to sub, which the block does not name.
        (
            ["--provider-id", "cms-sso", "--issuer", OIDC_BLOCK["issuer"],
             "--audience", "example-kb", "--trusted-verifier", "mcp-oidc:jwks"],
            "subject_claim",
        ),
        (
            ["--provider-id", "cms-sso", "--issuer", OIDC_BLOCK["issuer"],
             "--audience", "example-kb", "--subject-claim", "email",
             "--trusted-verifier", "mcp-oidc:jwks", "--role-claim", "cms_roles"],
            "'roles'",
        ),
        (
            ["--provider-id", "cms-sso", "--issuer", OIDC_BLOCK["issuer"],
             "--audience", "other", "--subject-claim", "email",
             "--trusted-verifier", "mcp-oidc:jwks"],
            "audience",
        ),
        # --for-oidc-auth refuses a flag that disagrees with the manifest.
        (["--for-oidc-auth", "--issuer", "https://elsewhere.example.org"], "flags disagree"),
    ],
)
def test_provider_add_refuses_a_row_the_oidc_start_check_would_refuse(
    capsys: Any, monkeypatch: Any, argv: list[str], needle: str,
) -> None:
    _declare_oidc_auth(monkeypatch, OIDC_BLOCK)
    rc, payload = _run_noun(capsys, "provider", "add", *NOWHERE, *argv)
    assert rc == 1, payload
    assert payload["error_code"] == "nomos.provider.oidc_auth_mismatch"
    assert needle in payload["error"]


def test_for_oidc_auth_needs_a_declared_block(capsys: Any, monkeypatch: Any) -> None:
    _declare_oidc_auth(monkeypatch, None)
    rc, payload = _run_noun(capsys, "provider", "add", *NOWHERE, "--for-oidc-auth")
    assert rc == 1
    assert payload["error_code"] == "nomos.provider.oidc_auth_undeclared"
    assert "not_declared" in payload["error"]


def test_grant_disable_help_names_the_widening_rule(capsys: Any) -> None:
    with pytest.raises(SystemExit):
        cli.main(["nomos", "grant", "disable", "--help"])
    text = " ".join(capsys.readouterr().out.split())
    assert "--allow-widen" in text
    assert "highest-priority grant" in text


@pytest.mark.postgres
def test_provider_add_for_oidc_auth_passes_the_server_start_check(
    clean_runtime: str, capsys: Any, monkeypatch: Any,
) -> None:
    from okg.substrate.mcp.oidc_auth import (
        parse_oidc_auth,
        validate_oidc_auth_against_providers,
    )
    from okg.substrate.nomos import load_identity_providers

    _declare_oidc_auth(monkeypatch, OIDC_BLOCK)
    rc, added = _run_noun(
        capsys, "provider", "add", *_db(clean_runtime), "--for-oidc-auth",
        "--reason", "example-kb launch",
    )
    assert rc == 0, added
    assert added["oidc_auth_check"]["status"] == "passed"
    provider = added["provider"]
    assert provider["provider_id"] == "cms-sso"
    assert provider["subject_claim"] == "email"
    assert provider["audiences"] == ["example-kb"]
    assert provider["trusted_verifiers"] == ["mcp-oidc:jwks"]
    assert provider["status"] == "active" and provider["in_effect"] is True
    assert provider["written_by"] == _default_actor(clean_runtime)
    events = provider["metadata"]["events"]
    assert [event["action"] for event in events] == ["added"]
    assert events[0]["reason"] == "example-kb launch"
    # The row the command wrote is the row the MCP server checks at start.
    with psycopg.connect(clean_runtime) as conn:
        providers = load_identity_providers(conn, deployment_name=DEPLOYMENT)
    validate_oidc_auth_against_providers(
        parse_oidc_auth(OIDC_BLOCK), providers, deployment_name=DEPLOYMENT,
    )


@pytest.mark.postgres
def test_provider_add_never_rewrites_or_reenables(
    clean_runtime: str, capsys: Any, monkeypatch: Any,
) -> None:
    _declare_oidc_auth(monkeypatch, None)
    rc, added = _add_provider(capsys, clean_runtime)
    assert rc == 0, added
    assert added["oidc_auth_check"]["status"] == "not_declared"
    before = _provider_row(clean_runtime, PROVIDER_ID)

    # Same id, different issuer: refused, row untouched.
    rc, payload = _run_noun(
        capsys, "provider", "add", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--issuer", "https://evil.example.org",
        "--audience", "okg-chat",
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.provider.exists"
    assert _provider_row(clean_runtime, PROVIDER_ID) == before

    rc, disabled = _run_noun(
        capsys, "provider", "disable", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--reason", "compromised",
    )
    assert rc == 0, disabled
    after_disable = _provider_row(clean_runtime, PROVIDER_ID)
    # A re-add of the disabled id is refused and does not erase the disable.
    rc, payload = _add_provider(capsys, clean_runtime)
    assert rc == 1
    assert payload["error_code"] == "nomos.provider.exists"
    assert "never re-enables" in payload["error"]
    assert _provider_row(clean_runtime, PROVIDER_ID) == after_disable
    assert after_disable["status"] == "disabled"


@pytest.mark.postgres
def test_provider_disable_refuses_every_claim_and_keeps_the_row(
    clean_runtime: str, capsys: Any, monkeypatch: Any,
) -> None:
    _declare_oidc_auth(monkeypatch, None)
    assert _add_provider(capsys, clean_runtime)[0] == 0
    rc, _ = _run(
        capsys, "add", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org", "--subject", "subject:team:alice",
    )
    assert rc == 0
    rc, _ = _add_grant(
        capsys, clean_runtime, "grant:alice:read",
        "--subject", "subject:team:alice", "--action", "graph.read.mcp",
    )
    assert rc == 0
    # A second registration of the same issuer, to be reported on disable.
    rc, _ = _run_noun(
        capsys, "provider", "add", *_db(clean_runtime),
        "--provider-id", "team-sso-2", "--issuer", ISSUER, "--audience", "other",
    )
    assert rc == 0

    admitted = _decide(clean_runtime)
    assert admitted.claim.verification_status == "valid"
    assert admitted.permission_grant_ids == ("grant:alice:read",)

    rc, disabled = _run_noun(
        capsys, "provider", "disable", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--reason", "compromised",
    )
    assert rc == 0, disabled
    assert disabled["changed"] is True
    assert disabled["provider"]["status"] == "disabled"
    assert disabled["provider"]["in_effect"] is False
    assert disabled["provider"]["disabled_reason"] == "compromised"
    assert [row["provider_id"] for row in disabled["same_issuer_providers"]] == [
        "team-sso-2",
    ]
    assert disabled["warning"].startswith("WARNING: ")

    # The binding and the grant are still active, but the claim is refused.
    refused = _decide(clean_runtime)
    assert refused.claim.verification_status == "provider_disabled"
    assert refused.decision.effect == "deny"
    assert refused.permission_grant_ids == ()

    rc, again = _run_noun(
        capsys, "provider", "disable", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
    )
    assert rc == 0
    assert again["changed"] is False
    assert (
        again["provider"]["metadata"]["disabled_at"]
        == disabled["provider"]["metadata"]["disabled_at"]
    )

    rc, listed = _run_noun(capsys, "provider", "list", *_db(clean_runtime))
    assert rc == 0
    assert listed["provider_count"] == 2
    assert listed["in_effect_count"] == 1

    rc, payload = _run_noun(
        capsys, "provider", "disable", *_db(clean_runtime), "--provider-id", "nobody",
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.provider.not_found"


@pytest.mark.postgres
def test_grant_add_disable_list_round_trip(
    clean_runtime: str, capsys: Any, monkeypatch: Any,
) -> None:
    from okg.substrate.nomos import load_permission_grants

    _declare_oidc_auth(monkeypatch, None)
    assert _add_provider(capsys, clean_runtime)[0] == 0
    scope = f"deployment:{DEPLOYMENT}"

    # No binding yet: the grant is written, with a warning that it applies
    # to nobody.
    rc, added = _add_grant(
        capsys, clean_runtime, "grant:alice:public",
        "--subject", "subject:team:alice", "--action", "graph.read.mcp",
        "--match", "source_id=public_docs", "--priority", "10",
        "--reason", "onboarding",
    )
    assert rc == 0, added
    assert added["subject_bound"] is False
    assert "applies to nobody" in added["warning"]
    grant = added["grant"]
    assert grant["scope"] == scope
    assert grant["effect"] == "allow"
    assert grant["resource_match"] == {"source_id": "public_docs"}
    assert grant["classifications"] == ["*"]
    assert grant["written_by"] == _default_actor(clean_runtime)

    rc, _ = _run(
        capsys, "add", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org", "--subject", "subject:team:alice",
    )
    assert rc == 0
    # A broader grant for alice and a grant for someone else.
    rc, wide = _add_grant(
        capsys, clean_runtime, "grant:alice:all",
        "--subject", "subject:team:alice", "--action", "*",
    )
    assert rc == 0 and wide["subject_bound"] is True and wide["warning"] is None
    assert _add_grant(
        capsys, clean_runtime, "grant:bob:public",
        "--subject", "subject:team:bob", "--action", "graph.read.mcp",
    )[0] == 0

    assert _decide(clean_runtime).permission_grant_ids == ("grant:alice:public",)

    rc, disabled = _run_noun(
        capsys, "grant", "disable", *_db(clean_runtime),
        "--grant-id", "grant:alice:public", "--reason", "left the project",
    )
    assert rc == 0, disabled
    assert disabled["changed"] is True
    assert disabled["grant"]["status"] == "disabled"
    assert disabled["grant"]["disabled_reason"] == "left the project"
    # The broader grant still permits every request the disabled one did;
    # the command says so instead of reporting the access as cut.
    assert disabled["still_allowing"] == [
        {
            "grant_id": "grant:alice:all",
            "actions": ["*"],
            "classifications": ["*"],
            "resource_match": {},
            "priority": 0,
            "match": "covers",
        },
    ]
    assert disabled["warning"].startswith("WARNING: ")
    # And it is the grant the gate now decides with.
    assert _decide(clean_runtime).permission_grant_ids == ("grant:alice:all",)

    with psycopg.connect(clean_runtime) as conn:
        loaded = load_permission_grants(conn, deployment_name=DEPLOYMENT, scope=scope)
    assert {g.grant_id for g in loaded} == {"grant:alice:all", "grant:bob:public"}

    rc, again = _run_noun(
        capsys, "grant", "disable", *_db(clean_runtime), "--grant-id", "grant:alice:public",
    )
    assert rc == 0 and again["changed"] is False

    rc, listed = _run_noun(
        capsys, "grant", "list", *_db(clean_runtime), "--subject", "subject:team:alice",
    )
    assert rc == 0
    assert listed["grant_count"] == 2
    assert listed["active_count"] == 1
    by_id = {row["grant_id"]: row for row in listed["grants"]}
    assert by_id["grant:alice:public"]["status"] == "disabled"
    assert [e["action"] for e in by_id["grant:alice:public"]["metadata"]["events"]] == [
        "added", "disabled",
    ]

    rc, payload = _run_noun(
        capsys, "grant", "disable", *_db(clean_runtime), "--grant-id", "grant:none",
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.grant.not_found"


@pytest.mark.postgres
def test_grant_add_never_rewrites_or_reenables(
    clean_runtime: str, capsys: Any,
) -> None:
    rc, _ = _add_grant(
        capsys, clean_runtime, "grant:x",
        "--subject", "subject:team:alice", "--action", "graph.read.mcp",
    )
    assert rc == 0
    before = _grant_row(clean_runtime, "grant:x")
    # Same id, another subject: refused, row untouched.
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:x",
        "--subject", "subject:team:mallory", "--action", "*",
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.grant.exists"
    assert _grant_row(clean_runtime, "grant:x") == before

    assert _run_noun(
        capsys, "grant", "disable", *_db(clean_runtime), "--grant-id", "grant:x",
    )[0] == 0
    disabled = _grant_row(clean_runtime, "grant:x")
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:x",
        "--subject", "subject:team:alice", "--action", "graph.read.mcp",
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.grant.exists"
    assert "never re-enables" in payload["error"]
    assert _grant_row(clean_runtime, "grant:x") == disabled
    assert disabled["status"] == "disabled"


@pytest.mark.postgres
def test_disabling_a_deny_grant_needs_allow_widen(
    clean_runtime: str, capsys: Any, monkeypatch: Any,
) -> None:
    _declare_oidc_auth(monkeypatch, None)
    assert _add_provider(capsys, clean_runtime)[0] == 0
    assert _run(
        capsys, "add", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org", "--subject", "subject:team:alice",
    )[0] == 0
    assert _add_grant(
        capsys, clean_runtime, "grant:alice:read",
        "--subject", "subject:team:alice", "--action", "graph.read.mcp",
    )[0] == 0
    assert _add_grant(
        capsys, clean_runtime, "grant:alice:block",
        "--subject", "subject:team:alice", "--action", "graph.read.mcp",
        "--effect", "deny", "--priority", "50",
    )[0] == 0
    blocked = _decide(clean_runtime)
    assert blocked.decision.reason_code == "nomos.identity.permission_grant_deny"

    # Disabling the deny would let the allow grant decide: refused by default.
    rc, payload = _run_noun(
        capsys, "grant", "disable", *_db(clean_runtime), "--grant-id", "grant:alice:block",
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.grant.widens_access"
    assert _grant_row(clean_runtime, "grant:alice:block")["status"] == "active"
    assert _decide(clean_runtime).decision.reason_code == (
        "nomos.identity.permission_grant_deny"
    )

    rc, disabled = _run_noun(
        capsys, "grant", "disable", *_db(clean_runtime),
        "--grant-id", "grant:alice:block", "--allow-widen",
    )
    assert rc == 0, disabled
    assert disabled["changed"] is True
    assert "may now be admitted" in disabled["warning"]
    assert _decide(clean_runtime).permission_grant_ids == ("grant:alice:read",)


@pytest.mark.postgres
def test_provider_and_grant_database_errors_are_json(
    clean_runtime: str, capsys: Any, monkeypatch: Any,
) -> None:
    assert _add_grant(
        capsys, clean_runtime, "grant:x",
        "--subject", "subject:team:alice", "--action", "graph.read.mcp",
    )[0] == 0
    impatient = psycopg.conninfo.make_conninfo(
        clean_runtime, options="-c lock_timeout=200",
    )
    with psycopg.connect(clean_runtime) as blocker:
        with blocker.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM okg.nomos_permission_grants WHERE deployment_name = %s "
                "AND grant_id = 'grant:x' FOR UPDATE",
                (DEPLOYMENT,),
            )
        rc, payload = _run_noun(
            capsys, "grant", "disable", "--dsn", impatient,
            "--deployment", DEPLOYMENT, "--grant-id", "grant:x",
        )
        blocker.rollback()
    assert rc == 1, payload
    assert payload["error_code"] == "nomos.grant.database_error"
    assert payload["sqlstate"] == "55P03"
    assert _grant_row(clean_runtime, "grant:x")["status"] == "active"

    _declare_oidc_auth(monkeypatch, None)

    def lost_connection(self: Any) -> None:
        raise psycopg.OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(psycopg.Connection, "commit", lost_connection)
    rc, payload = _add_provider(capsys, clean_runtime)
    monkeypatch.undo()
    assert rc == 1, payload
    assert payload["error_code"] == "nomos.provider.commit_outcome_unknown"
    assert "okg nomos provider list" in payload["error"]


# --- review fixes (#2900): grant tie-break, typo'd deny grants, DSN fallback,
# provider warnings.


@pytest.mark.parametrize(
    ("extra", "needle"),
    [
        (["--match", "sourceid=google_drive"], "--match key 'sourceid'"),
        (["--action", "graph.raed.mcp"], "action 'graph.raed.mcp'"),
    ],
)
def test_a_deny_grant_that_can_never_match_is_refused_before_connecting(
    capsys: Any, extra: list[str], needle: str,
) -> None:
    rc, payload = _run_noun(
        capsys, "grant", "add", *NOWHERE, "--grant-id", "grant:block",
        "--subject", "subject:team:alice", "--effect", "deny",
        "--action", "graph.read.mcp", *extra,
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.grant.unknown_match"
    assert needle in payload["error"]
    assert "database error" not in payload["error"]


def test_dsn_falls_back_to_okg_dsn(capsys: Any, monkeypatch: Any) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    rc, payload = _run_noun(capsys, "grant", "list", "--deployment", DEPLOYMENT)
    assert rc == 1
    assert "pass --dsn or set $OKG_DSN" in payload["error"]
    monkeypatch.setenv("OKG_DSN", NOWHERE[1])
    rc, payload = _run_noun(capsys, "grant", "list", "--deployment", DEPLOYMENT)
    assert rc == 1
    # It used $OKG_DSN: the failure is the connection to that address.
    assert payload["error_code"] == "nomos.grant.database_error"


def test_provider_add_warns_on_stderr(capsys: Any, monkeypatch: Any) -> None:
    import okg.substrate.deployments as deployments

    def unreadable(name: str) -> Any:
        raise deployments.DeploymentResolutionError("no deployments directory")

    monkeypatch.setattr(deployments, "resolve_deployment", unreadable)
    rc = cli.main([
        "nomos", "provider", "add", *NOWHERE, "--provider-id", "p",
        "--issuer", "https://sso.example.org", "--audience", "a", "--json",
    ])
    captured = capsys.readouterr()
    assert rc == 1  # nothing listens at the DSN; the warnings came first
    assert "could not be read (manifest_unavailable" in captured.err
    assert "neither --trusted-verifier nor --require-verified-by" in captured.err


@pytest.mark.postgres
def test_an_allow_that_would_win_a_tie_against_a_deny_needs_allow_widen(
    clean_runtime: str, capsys: Any, monkeypatch: Any,
) -> None:
    _declare_oidc_auth(monkeypatch, None)
    assert _add_provider(capsys, clean_runtime)[0] == 0
    assert _run(
        capsys, "add", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
        "--claim-value", "alice@example.org", "--subject", "subject:team:alice",
    )[0] == 0
    common = ["--subject", "subject:team:alice", "--action", "graph.read.mcp"]
    assert _add_grant(
        capsys, clean_runtime, "grant:z-block", *common, "--effect", "deny",
    )[0] == 0
    assert _decide(clean_runtime).decision.reason_code == (
        "nomos.identity.permission_grant_deny"
    )

    # Same priority, and "grant:a-read" sorts first: it would decide first.
    rc, payload = _add_grant(capsys, clean_runtime, "grant:a-read", *common)
    assert rc == 1
    assert payload["error_code"] == "nomos.grant.widens_access"
    assert "grant:z-block" in payload["error"]
    assert _decide(clean_runtime).decision.reason_code == (
        "nomos.identity.permission_grant_deny"
    )
    # A higher-priority allow is refused too.
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:zz-read", *common, "--priority", "5",
    )
    assert payload["error_code"] == "nomos.grant.widens_access"
    # An allow the deny still decides before is fine.
    rc, payload = _add_grant(capsys, clean_runtime, "grant:zz-read", *common)
    assert rc == 0, payload
    assert _decide(clean_runtime).decision.reason_code == (
        "nomos.identity.permission_grant_deny"
    )
    # With --allow-widen the operator says they mean it.
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:a-read", *common, "--allow-widen",
    )
    assert rc == 0, payload
    assert _decide(clean_runtime).permission_grant_ids == ("grant:a-read",)


@pytest.mark.postgres
def test_a_deny_that_an_allow_would_decide_before_is_refused(
    clean_runtime: str, capsys: Any,
) -> None:
    common = ["--subject", "subject:team:alice", "--action", "graph.read.mcp"]
    assert _add_grant(
        capsys, clean_runtime, "grant:a-read", *common, "--priority", "10",
    )[0] == 0
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:b-block", *common, "--effect", "deny",
        "--priority", "5",
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.grant.shadowed"
    assert "--priority 11" in payload["error"]
    # Same priority, sorting after the allow: still shadowed.
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:b-block", *common, "--effect", "deny",
        "--priority", "10",
    )
    assert payload["error_code"] == "nomos.grant.shadowed"
    # A deny on a resource the allow cannot match does not overlap (a
    # single-string key; list-valued keys such as source_id always overlap).
    assert _add_grant(
        capsys, clean_runtime, "grant:a-read-docs", "--subject", "subject:team:bob",
        "--action", "graph.read.mcp", "--match", "resource_kind=mcp_tool",
        "--priority", "10",
    )[0] == 0
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:b-block-drive", "--subject", "subject:team:bob",
        "--action", "graph.read.mcp", "--match", "resource_kind=mcp_graph_read_boundary",
        "--effect", "deny",
    )
    assert rc == 0, payload
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:b-block", *common, "--effect", "deny",
        "--priority", "11",
    )
    assert rc == 0, payload


@pytest.mark.postgres
def test_an_allow_grant_with_an_unknown_key_is_written_with_a_warning(
    clean_runtime: str, capsys: Any,
) -> None:
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:typo", "--subject", "subject:team:alice",
        "--action", "graph.read.mcp", "--match", "sourceid=docs",
    )
    assert rc == 0, payload
    assert payload["match_warnings"] == [
        "WARNING: --match key 'sourceid' is not a string-valued resource key the gate "
        "reads; this allow grant will not match it",
    ]


# --- review round 2 (#2900): non-string match keys, list-valued keys,
# classifications, and the per-subject lock.


@pytest.mark.parametrize(
    ("extra", "needle"),
    [
        (["--match", "limit=100"], "--match key 'limit'"),
        (["--match", "row_cap=5"], "--match key 'row_cap'"),
        (["--match", "audit=true"], "--match key 'audit'"),
        (["--match", "source_lineage=x"], "--match key 'source_lineage'"),
        # A typo of node_ref: only the read filter's row-kind refs count.
        (["--match", "nod_ref=node:1"], "--match key 'nod_ref'"),
        (["--classification", "top-secret"], "classification 'top-secret'"),
    ],
)
def test_a_deny_on_a_key_or_class_the_gate_never_compares_equal_is_refused(
    capsys: Any, extra: list[str], needle: str,
) -> None:
    rc, payload = _run_noun(
        capsys, "grant", "add", *NOWHERE, "--grant-id", "grant:block",
        "--subject", "subject:team:alice", "--effect", "deny",
        "--action", "graph.read.mcp", *extra,
    )
    assert rc == 1
    assert payload["error_code"] == "nomos.grant.unknown_match"
    assert needle in payload["error"]


def test_the_gate_really_never_matches_a_string_on_a_number() -> None:
    from okg.substrate.nomos import PermissionGrant

    grant = PermissionGrant(
        grant_id="g", subject="s", actions=("graph.read.mcp",), scope="x",
        effect="deny", resource_match={"limit": "100"},
    )
    assert not grant.matches(
        subject="s", action="graph.read.mcp", resource={"limit": 100},
        classification="internal", scope="x",
    )


def test_different_bundles_or_ids_can_match_one_resource() -> None:
    """Why list-valued keys are never disjoint: both grants match here."""
    from okg.substrate.nomos import PermissionGrant

    def grant(key: str, value: str) -> Any:
        return PermissionGrant(
            grant_id=value, subject="s", actions=("graph.read.mcp",), scope="x",
            resource_match={key: value},
        )

    def both(key: str, a: str, b: str, resource: dict[str, Any]) -> bool:
        return all(
            g.matches(
                subject="s", action="graph.read.mcp", resource=resource,
                classification="internal", scope="x",
            )
            for g in (grant(key, a), grant(key, b))
        )

    assert both(
        "source_bundle", "public", "restricted",
        {"source_bundle_ids": ["public", "restricted"]},
    )
    assert both("source_id", "a", "b", {"source_ids": []})


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("key", "deny_value", "allow_value"),
    [("source_bundle", "restricted", "public"), ("source_id", "a", "b")],
)
def test_an_allow_on_another_list_value_still_needs_allow_widen(
    clean_runtime: str, capsys: Any, key: str, deny_value: str, allow_value: str,
) -> None:
    common = ["--subject", "subject:team:alice", "--action", "graph.read.mcp"]
    assert _add_grant(
        capsys, clean_runtime, "grant:z-block", *common, "--effect", "deny",
        "--match", f"{key}={deny_value}",
    )[0] == 0
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:a-read", *common,
        "--match", f"{key}={allow_value}", "--priority", "10",
    )
    assert rc == 1, payload
    assert payload["error_code"] == "nomos.grant.widens_access"
    # A scalar key with different values really is disjoint.
    assert _add_grant(
        capsys, clean_runtime, "grant:b-tools", *common, "--effect", "deny",
        "--match", "resource_kind=mcp_tool",
    )[0] == 0
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:a-graph", *common,
        "--match", "resource_kind=mcp_graph_read_boundary", "--match",
        f"{key}={allow_value}", "--priority", "10", "--allow-widen",
    )
    assert rc == 0, payload


@pytest.mark.postgres
def test_grant_add_takes_the_per_subject_lock(clean_runtime: str, capsys: Any) -> None:
    impatient = psycopg.conninfo.make_conninfo(
        clean_runtime, options="-c lock_timeout=200",
    )
    with psycopg.connect(clean_runtime) as holder:
        with holder.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"okg.nomos.grant:{DEPLOYMENT}:subject:team:alice",),
            )
        rc, payload = _run_noun(
            capsys, "grant", "add", "--dsn", impatient, "--deployment", DEPLOYMENT,
            "--grant-id", "grant:x", "--subject", "subject:team:alice",
            "--action", "graph.read.mcp",
        )
        holder.rollback()
    assert rc == 1, payload
    assert payload["error_code"] == "nomos.grant.database_error"
    assert payload["sqlstate"] == "55P03"
    # Another subject is not blocked by alice's lock.
    rc, payload = _add_grant(
        capsys, clean_runtime, "grant:y", "--subject", "subject:team:bob",
        "--action", "graph.read.mcp",
    )
    assert rc == 0, payload


def test_the_read_filter_row_ref_keys_are_accepted() -> None:
    from okg.substrate.nomos import PermissionGrant
    from okg.substrate.nomos.identity_store import grant_match_problems

    for key in ("node_ref", "edge_ref", "graph_ref"):
        grant = PermissionGrant(
            grant_id="g", subject="s", actions=("graph.read.mcp",), scope="x",
            effect="deny", resource_match={key: "node:1"},
        )
        assert grant_match_problems(grant) == []
