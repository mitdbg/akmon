"""A production deployment DSN in libpq's keyword/value spelling is refused
where the operator set it, not deep inside the first write.

libpq accepts two spellings of one connection string -- the URL and
``host=... dbname=...``. Chronos selects its storage backend by URL SCHEME,
so the keyword form has always been unusable here: it was routed to SQLite
and the publish path wrote a FILE named after the connection string (issue
#1971). PR #1995 closed that at the storage layer and for ``OKG_TEST_DSN``.
This module holds the same guarantee for the PRODUCTION resolvers -- the
ones behind the ``okg`` CLI, the read side, the deployment authority chain
and the publisher (issue #2001).

Every test here asserts three things about a refusal: it names the setting
the operator edited, it arrives from the resolver rather than from Chronos,
and any URL it offers actually parses.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import psycopg
import psycopg.conninfo
import pytest
import yaml

from okg.substrate.cli.dsn_authority import resolve_read_dsn
from okg.substrate.cli.main import (
    _resolve_deployment_dsn,
    _resolve_deployment_dsn_from_dir,
    _resolve_dsn,
)
from okg.substrate.deployment_authority import resolve_deployment_dsn_authority
from okg.substrate.dsn_form import keyword_form_refusal
from okg.substrate.deployments import (
    DeploymentResolutionError,
    require_postgres_dsn,
)
from okg.substrate.errors import SubstrateError

#: The spelling libpq documents and an operator may reasonably write. Test
#: input to a refusal; nothing in this module connects with it.
KEYWORD_DSN = (
    "user=okg password=secret dbname=okg_workspace host=127.0.0.1 port=5981"
)
URL_DSN = "postgresql://okg:secret@127.0.0.1:5981/okg_workspace"

#: A keyword DSN libpq itself cannot parse -- an unquoted password. The
#: refusal must still be the resolver's, not psycopg's (a P1 on PR #1995).
UNPARSABLE_KEYWORD_DSN = "password=my secret dbname=okg host=127.0.0.1"

#: No host and no user: rebuilding this naively yields ``postgresql:/okg``,
#: which libpq rejects. The suggestion has to spell the empty authority.
HOSTLESS_KEYWORD_DSN = "service=okg dbname=okg_workspace"


def _assert_names_source(message: str, *, source: str) -> None:
    """The refusal points at the setting, not at Chronos' backend choice."""
    assert source in message, message
    assert "keyword/value" in message, message
    assert "would write a FILE" in message, message


def _assert_offers_a_parsable_url(message: str) -> None:
    """Any URL offered is one psycopg accepts; otherwise none is offered."""
    offered = [
        word.rstrip(".")
        for word in message.split()
        if word.startswith("postgresql://")
    ]
    assert offered, message
    for candidate in offered:
        if candidate == "postgresql://user:password@host:port/dbname":
            continue  # the generic hint, not a rebuild of this DSN
        psycopg.conninfo.conninfo_to_dict(candidate)


@pytest.fixture(autouse=True)
def restore_okg_env_after_test():
    tracked = {
        key: os.environ.get(key)
        for key in (
            "OKG_DSN",
            "OKG_DEPLOYMENT",
            "OKG_ENVIRONMENT_DESCRIPTOR",
            "OKG_ENABLE_LOCAL_ENV_FOR_TESTS",
        )
    }
    yield
    for key, value in tracked.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture(autouse=True)
def no_ambient_deployment(monkeypatch):
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    """Write a deployment manifest and make the finders see it."""

    def _write(dsn: str, name: str = "dep") -> Path:
        directory = tmp_path / name
        directory.mkdir()
        (directory / "deployment.yaml").write_text(
            yaml.safe_dump({"name": name, "postgres": {"dsn": dsn}}),
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "okg.substrate.deployments.find_deployment_dir",
            lambda requested, _dir=directory, _name=name: (
                _dir if requested == _name else None
            ),
        )
        return directory

    return _write


# --- the CLI resolver: `okg doctor` and 90-odd other commands ------------


def test_cli_refuses_a_keyword_okg_dsn_naming_the_variable(
    deployment, monkeypatch,
):
    """`dsn: ${OKG_DSN}` plus a keyword-form OKG_DSN names OKG_DSN."""
    deployment("${OKG_DSN}")
    monkeypatch.setenv("OKG_DSN", KEYWORD_DSN)
    args = argparse.Namespace(deployment="dep", dsn=None)

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_deployment_dsn(args)

    message = str(excinfo.value)
    _assert_names_source(message, source="OKG_DSN")
    _assert_offers_a_parsable_url(message)
    assert excinfo.value.data["dsn_form"] == "libpq_keyword"


def test_cli_refuses_a_keyword_dsn_written_into_the_manifest(deployment):
    """A literal keyword DSN names the manifest key and its file."""
    directory = deployment(KEYWORD_DSN)
    args = argparse.Namespace(deployment="dep", dsn=None)

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_deployment_dsn(args)

    message = str(excinfo.value)
    _assert_names_source(message, source="`postgres.dsn`")
    assert str(directory / "deployment.yaml") in message


def test_cli_refuses_a_keyword_dsn_flag(deployment):
    deployment("${OKG_DSN}")
    args = argparse.Namespace(deployment="dep", dsn=KEYWORD_DSN)

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_deployment_dsn(args)

    _assert_names_source(str(excinfo.value), source="--dsn")


def test_cli_refuses_a_keyword_dsn_from_a_deployment_dir(deployment):
    directory = deployment(KEYWORD_DSN)
    args = argparse.Namespace(deployment=None, dsn=None)

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_deployment_dsn_from_dir(args, directory)

    _assert_names_source(str(excinfo.value), source="`postgres.dsn`")


def test_dba_mode_refuses_a_keyword_okg_dsn(monkeypatch):
    """No deployment: OKG_DSN is read directly, and refused directly."""
    monkeypatch.setenv("OKG_DSN", KEYWORD_DSN)

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_dsn(argparse.Namespace(deployment=None, dsn=None))

    _assert_names_source(str(excinfo.value), source="OKG_DSN")


def test_cli_still_resolves_the_url_form(deployment, monkeypatch):
    """The control: the supported spelling is untouched."""
    deployment("${OKG_DSN}")
    monkeypatch.setenv("OKG_DSN", URL_DSN)

    assert (
        _resolve_deployment_dsn(argparse.Namespace(deployment="dep", dsn=None))
        == URL_DSN
    )


# --- the authority chain: read side, MCP, deployment commands -----------


def test_authority_chain_returns_a_named_refusal_envelope(
    deployment, monkeypatch,
):
    """Callers read `reason`; it must name OKG_DSN, not Chronos."""
    deployment("${OKG_DSN}")
    monkeypatch.setenv("OKG_DSN", KEYWORD_DSN)

    resolution = resolve_deployment_dsn_authority("dep")

    assert resolution["dsn"] is None
    assert resolution["next_action"] == "fix_deployment_dsn_form"
    assert resolution["authority"]["dsn_form"] == "libpq_keyword"
    _assert_names_source(resolution["reason"], source="OKG_DSN")
    _assert_offers_a_parsable_url(resolution["reason"])


def test_authority_chain_refuses_an_explicit_keyword_dsn(deployment):
    deployment("${OKG_DSN}")

    resolution = resolve_deployment_dsn_authority(
        "dep", explicit_dsn=KEYWORD_DSN,
    )

    assert resolution["dsn"] is None
    _assert_names_source(resolution["reason"], source="--dsn")


def test_read_side_refuses_a_keyword_okg_dsn(monkeypatch):
    monkeypatch.setenv("OKG_DSN", KEYWORD_DSN)

    with pytest.raises(SubstrateError) as excinfo:
        resolve_read_dsn(
            argparse.Namespace(deployment=None, dsn=None), surface="search",
        )

    _assert_names_source(str(excinfo.value), source="OKG_DSN")
    assert excinfo.value.data["surface"] == "search"


# --- the publisher boundary ---------------------------------------------


class _Manifest:
    def __init__(self, dsn: str) -> None:
        self.postgres_dsn = dsn


def test_publisher_boundary_refuses_a_keyword_dsn():
    with pytest.raises(DeploymentResolutionError) as excinfo:
        require_postgres_dsn(_Manifest(KEYWORD_DSN), deployment="dep")

    message = str(excinfo.value)
    _assert_names_source(message, source="`postgres.dsn`")
    assert "'dep'" in message
    _assert_offers_a_parsable_url(message)


def test_publisher_boundary_passes_the_url_form():
    assert require_postgres_dsn(_Manifest(URL_DSN), deployment="dep") == URL_DSN


# --- the suggestion is always one psycopg accepts ------------------------


def test_an_unparsable_keyword_dsn_still_gets_the_resolver_refusal(
    monkeypatch,
):
    """psycopg's own parse error must not escape the explaining function."""
    monkeypatch.setenv("OKG_DSN", UNPARSABLE_KEYWORD_DSN)

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_dsn(argparse.Namespace(deployment=None, dsn=None))

    message = str(excinfo.value)
    _assert_names_source(message, source="OKG_DSN")
    assert "postgresql://user:password@host:port/dbname" in message


@pytest.mark.parametrize(
    "dsn",
    [
        # Each password word carries a unique `zq9` token, so the check below
        # cannot be satisfied by, or collide with, ordinary message words.
        "password=zq9one zq9two dbname=okg host=127.0.0.1",
        "password=zq9one\nzq9two dbname=okg host=127.0.0.1",
        "password='zq9one zq9two dbname=okg host=127.0.0.1",
        # The old masker stopped an unquoted password at the next ` key=`
        # token, and here that "key" is the password's second word: the
        # refusal showed `zq9two=x`.
        "password=zq9one zq9two=x dbname=okg host=127.0.0.1",
    ],
)
def test_a_malformed_password_is_fully_masked(dsn: str):
    """A DSN libpq cannot parse is not echoed at all: the masker cannot
    tell where its password ends, so no fragment of it may be shown."""
    from okg.substrate.dsn_form import keyword_form_refusal

    message = keyword_form_refusal("OKG_DSN", dsn)

    assert "zq9" not in message
    assert "not shown: libpq cannot parse it" in message
    _assert_names_source(message, source="OKG_DSN")


def test_the_unparsable_fixture_is_not_echoed():
    from okg.substrate.dsn_form import keyword_form_refusal

    message = keyword_form_refusal("OKG_DSN", UNPARSABLE_KEYWORD_DSN)

    assert "password=my" not in message
    assert "secret dbname" not in message


RAW_PASSWORD = "Pa@ss:w/rd x"


def test_a_raw_password_with_url_metacharacters_is_masked_and_offered():
    """A keyword DSN whose quoted password holds `@ : /` and a blank: the
    refusal masks it, and the URL offered percent-encodes it and parses."""
    from okg.substrate.dsn_form import keyword_form_refusal, url_form_suggestion

    keyword = f"user=okg password='{RAW_PASSWORD}' dbname=okg host=db.internal"
    message = keyword_form_refusal("OKG_DSN", keyword)
    suggestion = url_form_suggestion(keyword)

    for fragment in ("Pa@ss", "ss:w", "w/rd", "Pa%40ss"):
        assert fragment not in message
        assert fragment not in (suggestion or "")
    assert suggestion == "postgresql://okg:***@db.internal/okg"
    _assert_offers_a_parsable_url(message)


def test_dsn_with_dbname_keeps_a_percent_encoded_password():
    from okg.substrate.db import dsn
    from okg.substrate.dsn_form import dsn_with_dbname

    base = "postgresql://okg:Pa%40ss%3Aw%2Frd@db.internal:5432/okg?sslmode=require"
    rebuilt = dsn_with_dbname(base, "okg_other")

    assert dsn.parse(rebuilt).password == "Pa@ss:w/rd"
    assert dsn.parse(rebuilt).dbname == "okg_other"
    assert dsn.parse(rebuilt).params["sslmode"] == "require"
    assert rebuilt.startswith("postgresql://")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (KEYWORD_DSN, True),
        (URL_DSN, False),
        # A `://` anywhere still means "not this function's business", as
        # before the move; `dsn.form_of` is the libpq-exact test.
        ("host=h application_name=http://x dbname=okg", False),
        ("/var/lib/okg/graph.sqlite", False),
        ("", False),
    ],
)
def test_is_keyword_form_dsn_keeps_its_rule(value: str, expected: bool):
    from okg.substrate.dsn_form import is_keyword_form_dsn

    assert is_keyword_form_dsn(value) is expected


def test_the_dsn_form_names_are_the_shared_module():
    from okg.substrate import dsn_form
    from okg.substrate.db import dsn

    assert dsn_form.LIBPQ_KEYWORD_PARAM is dsn.LIBPQ_KEYWORD_PARAM
    assert dsn_form.keyword_form_refusal("S", KEYWORD_DSN) == (
        dsn._keyword_form_refusal("S", KEYWORD_DSN)
    )


def test_a_hostless_keyword_dsn_is_offered_a_hostless_url(monkeypatch):
    """`urlunsplit` drops an empty authority; the suggestion must not."""
    monkeypatch.setenv("OKG_DSN", HOSTLESS_KEYWORD_DSN)

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_dsn(argparse.Namespace(deployment=None, dsn=None))

    message = str(excinfo.value)
    assert "postgresql:///okg_workspace" in message
    _assert_offers_a_parsable_url(message)


def test_the_refusal_never_rewrites_the_dsn(monkeypatch):
    """Refuse, do not normalise: the spellings are not interchangeable."""
    monkeypatch.setenv("OKG_DSN", KEYWORD_DSN)

    with pytest.raises(SubstrateError):
        _resolve_dsn(argparse.Namespace(deployment=None, dsn=None))

    assert os.environ["OKG_DSN"] == KEYWORD_DSN


# --- the refusal must not print the password ----------------------------
#
# Review of PR #2039 found this: the refusal interpolated the raw DSN and
# offered a suggestion URL carrying `user:password@host`. The `***` seen in
# the first receipt came only from the CLI's `--json` branch, which runs
# `redact_value` over the error payload. Every other surface leaked:
# plain (non-JSON) `okg db status` prints `err.message` verbatim,
# `require_postgres_dsn` raises a plain exception that never reaches that
# branch, and the authority chain's `reason` is returned verbatim by
# `okg.substrate.mcp.deployment_entry`, which never redacts.
#
# Redaction therefore belongs INSIDE the message builder, never in a caller.

#: A password that would be unmistakable in any output that leaked it.
REAL_PASSWORD = "sw0rdf1sh-Pa55phrase"
SECRET_KEYWORD_DSN = (
    f"user=okg password={REAL_PASSWORD} dbname=okg_workspace "
    "host=127.0.0.1 port=5992"
)


def _assert_no_password(blob: object, *, label: str) -> None:
    text = blob if isinstance(blob, str) else json.dumps(blob, default=str)
    assert REAL_PASSWORD not in text, f"{label} leaked the password: {text}"


def test_the_message_builder_masks_the_password():
    from okg.substrate.dsn_form import keyword_form_refusal, url_form_suggestion

    _assert_no_password(
        keyword_form_refusal("OKG_DSN", SECRET_KEYWORD_DSN), label="message",
    )
    _assert_no_password(
        url_form_suggestion(SECRET_KEYWORD_DSN) or "", label="suggestion",
    )


def test_the_cli_refusal_masks_the_password_on_every_field(monkeypatch):
    """`_text_error` prints `message` and `fix_hint` with no redaction."""
    monkeypatch.setenv("OKG_DSN", SECRET_KEYWORD_DSN)

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_dsn(argparse.Namespace(deployment=None, dsn=None))

    err = excinfo.value
    _assert_no_password(str(err), label="message")
    _assert_no_password(err.data, label="data")
    _assert_no_password(err.fix_hint or "", label="fix_hint")


def test_the_publisher_refusal_masks_the_password():
    """DeploymentResolutionError never passes through the CLI's redactor."""
    with pytest.raises(DeploymentResolutionError) as excinfo:
        require_postgres_dsn(_Manifest(SECRET_KEYWORD_DSN), deployment="dep")

    _assert_no_password(str(excinfo.value), label="publisher refusal")


def test_the_authority_envelope_masks_the_password(deployment, monkeypatch):
    """`okg.substrate.mcp.deployment_entry` returns this envelope verbatim."""
    deployment("${OKG_DSN}")
    monkeypatch.setenv("OKG_DSN", SECRET_KEYWORD_DSN)

    resolution = resolve_deployment_dsn_authority("dep")

    assert resolution["dsn"] is None
    _assert_no_password(resolution, label="authority envelope")


# --- the CLI must surface the authority chain's refusal -----------------


def test_the_cli_raises_the_authority_refusal_not_the_unset_env_message(
    deployment, monkeypatch,
):
    """When interpolation leaves a `${...}`, the CLI consults the authority
    chain. That chain now REFUSES a keyword DSN by returning `dsn: None`
    plus a reason -- so the old `if resolution.get("dsn")` branch is never
    taken, and without this the operator gets `references unset env var(s)`
    and is told to export a variable they already set.
    """
    deployment("${OKG_UNSET_FOR_THIS_TEST}")
    monkeypatch.delenv("OKG_UNSET_FOR_THIS_TEST", raising=False)
    refusal = {
        "dsn": None,
        "reason": keyword_form_refusal("OKG_DSN", KEYWORD_DSN),
        "next_action": "fix_deployment_dsn_form",
        "authority": {"mode": "deployment_manifest", "dsn_form": "libpq_keyword"},
    }
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.resolve_deployment_dsn_authority",
        lambda *a, **k: refusal,
    )

    with pytest.raises(SubstrateError) as excinfo:
        _resolve_deployment_dsn(argparse.Namespace(deployment="dep", dsn=None))

    message = str(excinfo.value)
    assert "unset env var" not in message, message
    _assert_names_source(message, source="OKG_DSN")
    assert excinfo.value.data["dsn_form"] == "libpq_keyword"


# --- the inventory must not re-adopt the refused string -----------------


def test_the_inventory_does_not_fall_back_to_the_refused_manifest_dsn(
    tmp_path, monkeypatch,
):
    """`okg deployment list` fell back to `manifests[name].dsn` whenever the
    authority chain returned no DSN, which silently re-adopted the very
    string the chain had just refused and then tried to connect with it.
    """
    from okg.substrate.cli import deployment_inventory

    root = tmp_path / "deployments"
    directory = root / "dep"
    directory.mkdir(parents=True)
    (directory / "schema").mkdir()
    (directory / "source_registry.yaml").write_text(
        "sources: []\n", encoding="utf-8",
    )
    (directory / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "dep",
            "schema_dir": "schema",
            "source_registry": "source_registry.yaml",
            "postgres": {"dsn": SECRET_KEYWORD_DSN},
        }),
        encoding="utf-8",
    )
    claude_json = tmp_path / "claude.json"
    claude_json.write_text(json.dumps({"mcpServers": {}}), encoding="utf-8")

    probed: list[str] = []
    monkeypatch.setattr(
        deployment_inventory,
        "_probe",
        lambda name, dsn, branch: probed.append(dsn) or {
            "reachable": False, "node_count": None, "error": "probed",
        },
    )

    out = deployment_inventory.collect_deployment_inventory(
        deployments_root=root,
        claude_json_path=claude_json,
        project_mcp_paths=[],
        project_key=str(tmp_path),
        probe=True,
    )

    row = next(r for r in out["deployments"] if r["name"] == "dep")
    assert probed == [], f"connected with the refused DSN: {probed}"
    assert "keyword/value" in (row["error"] or ""), row["error"]
    _assert_no_password(out, label="inventory rows")
