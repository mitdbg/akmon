from __future__ import annotations

import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import _cmd_doctor
from okg.substrate.ingest.runner import RunResult
from okg.substrate.library.sources.base import (
    PREFLIGHT_STATUSES,
    SourceHealth,
    SourcePreflightResult,
)
from okg.substrate.sources.preflight import (
    credential_env_preflight,
    file_ref_preflight,
)
from okg.substrate.sources.redaction import (
    redact_endpoint,
    redact_mapping,
    redact_text,
)


def test_preflight_statuses_serialize_without_secret_values() -> None:
    for status in PREFLIGHT_STATUSES:
        result = SourcePreflightResult(
            source_name="issue_api",
            status=status,
            mode="live",
            credential_refs=("EXAMPLE_API_TOKEN",),
            alias_refs={"EXAMPLE_API_TOKEN": ("LEGACY_EXAMPLE_TOKEN",)},
            reason="checked token ref",
        )
        data = result.as_dict()
        assert data["status"] == status
        assert "EXAMPLE_API_TOKEN" in data["credential_refs"]
        assert "secret-value" not in json.dumps(data)
    health = SourceHealth(
        status="auth_failed",
        endpoint="https://user:secret@example.test/path?token=secret",
        reason="Authorization: Bearer glpat-supersecret",
        cache_path="/tmp/CACHE_TOKEN=abcdefghijklmnopqrstuvwxyz123456",
    )
    health_blob = json.dumps(health.as_dict())
    assert "glpat-supersecret" not in health_blob
    assert "token=secret" not in health_blob
    assert "user:secret" not in health_blob
    assert "abcdefghijklmnopqrstuvwxyz123456" not in health_blob


def test_oauth_preflight_points_at_connector_route_not_okg_add(monkeypatch) -> None:
    # `okg add` rejects connectors by design; the OAuth preflight must direct
    # users at the real connector setup path (the registered-agent route
    # planner), not the stale `okg add <source>` instruction.
    from okg.substrate.library.oauth.preflight import oauth_preflight
    from okg.substrate.library.oauth.providers import GMAIL

    monkeypatch.delenv(GMAIL.client_id_env, raising=False)
    monkeypatch.delenv(GMAIL.client_secret_env, raising=False)
    res = oauth_preflight(
        source_name="gmail_api",
        deployment_name="okg-workspace",
        provider=GMAIL,
    )
    assert res.status == "missing_credential"
    assert "okg add" not in res.reason
    assert "set up my OKG" in res.reason


def test_redaction_helpers_cover_tokens_headers_urls_and_dsns() -> None:
    text = (
        "Authorization: Bearer glpat-supersecret "
        "EXAMPLE_API_TOKEN=abcdefghijklmnopqrstuvwxyz123456 "
        "postgres://user:password@example/db"
    )
    redacted = redact_text(text)
    assert "glpat-supersecret" not in redacted
    assert "abcdefghijklmnopqrstuvwxyz123456" not in redacted
    assert "password" not in redacted
    assert "Authorization=<redacted>" in redacted
    assert (
        redact_text("EXAMPLE_COOKIE_FILE is missing")
        == "EXAMPLE_COOKIE_FILE is missing"
    )

    endpoint = redact_endpoint(
        "https://example.test/path?q=token-secret#frag"
    )
    assert endpoint == "https://example.test/path"

    mapping = redact_mapping({
        "token": "glpat-secret",
        "nested": {"Authorization": "Bearer abcdefghijklmnopqrstuvwxyz"},
    })
    blob = json.dumps(mapping)
    assert "glpat-secret" not in blob
    assert "abcdefghijklmnopqrstuvwxyz" not in blob


def test_redaction_masks_a_short_bare_gitlab_token() -> None:
    """A bare GitLab token under 24 characters is masked by its prefix alone.

    The generic long-token branch needs 24 characters and the header branch
    needs `Authorization:`, so only the `glpat-` branch covers this (#2781).
    The tail holds `_` and `-` so dropping either from the branch's class
    leaves part of the token in the output. The value is assembled at runtime
    so the source holds no token literal.
    """
    token = "gl" + "pat-" + "a_b-FAKE0sentinel"
    assert len(token) < 24

    redacted = redact_text(f"clone failed using {token} for the mirror")

    assert token not in redacted
    assert redacted == "clone failed using <redacted> for the mirror"


def test_redaction_masks_a_long_bare_token_of_every_character_kind() -> None:
    """The generic branch masks a 24+ character run of letters, digits, `_` and `-`.

    Every character kind the branch admits appears every five characters, so
    dropping any one splits the run into pieces too short to mask (#2781).
    """
    token = "aB3_-" * 5 + "aB3"
    assert len(token) >= 24

    redacted = redact_text(f"retry with {token} failed")

    assert token not in redacted
    assert redacted == "retry with <redacted> failed"


# A content digest is not a secret and must survive redaction whole (#2213).
# One row per digest algorithm the exemption names, each with its real hex
# length, mixing digits and the letters a-f.
_DIGESTS = [
    ("sha256", 64), ("sha512", 128), ("sha1", 40), ("md5", 32), ("blake2b", 128),
]


@pytest.mark.parametrize(
    ("algorithm", "length"),
    _DIGESTS,
    ids=[algorithm for algorithm, _ in _DIGESTS],
)
def test_redaction_keeps_each_content_digest(algorithm: str, length: int) -> None:
    digest = f"{algorithm}:" + ("0a1b2c3d4e5f" * 11)[:length]

    text = f"fingerprint mismatch: expected {digest}"

    assert redact_text(text) == text


#: A preflight diagnostic in the shape an operator pastes: a libpq error whose
#: DETAIL line carries an unquoted keyword password, then the traceback.
_MULTI_LINE_DIAGNOSTIC = (
    "psycopg.OperationalError: connection failed\n"
    "DETAIL: host=db.internal password=Hunter2\n"
    "Traceback (most recent call last):\n"
    '  File "okg/cli.py", line 12, in main\n'
    "line 5 dbname=okg"
)


def test_redact_text_keeps_every_line_after_a_keyword_password() -> None:
    """Design section 1.6 item 1: the DSN layer ran an unquoted `password=`
    to the next `key=` token, deleting the traceback lines in between. The
    one line after an unquoted password may be its wrapped remainder, so it
    is masked (#2425); every later line is kept."""
    shown = redact_text(_MULTI_LINE_DIAGNOSTIC)

    assert "Hunter2" not in shown
    assert shown.splitlines()[2] == "***"
    assert 'File "okg/cli.py", line 12, in main' in shown
    assert shown.endswith("line 5 dbname=okg")


def test_redact_text_masks_a_raw_at_and_the_other_secret_keys() -> None:
    """Design section 1.6 item 2, through the generic diagnostic redactor."""
    raw = "postgresql://okg:Pa@ss:w/rd@db.internal:5432/okg"
    shown = redact_text(
        f"{raw}\nhost=db.internal sslpassword=sslS3cret dbname=okg\n"
        "postgresql://okg@db.internal/okg?sslpassword=sslS3cret"
    )

    for fragment in ("Pa@ss", "ss:w", "w/rd", "sslS3cret"):
        assert fragment not in shown
    assert shown.splitlines()[0] == "postgresql://okg:***@db.internal:5432/okg"


def test_redact_dsn_is_the_shared_line_aware_masker() -> None:
    """The shared masker, with no whole-value `--dsn` flag rule."""
    from okg.substrate.db import dsn
    from okg.substrate.sources import redaction

    assert redaction.redact_dsn.func is dsn.redact_text
    assert redaction.redact_dsn.keywords == {"dsn_flags": ()}


def test_credential_env_preflight_accepts_alias(monkeypatch) -> None:
    monkeypatch.delenv("EXAMPLE_API_TOKEN", raising=False)
    monkeypatch.setenv("LEGACY_EXAMPLE_TOKEN", "secret-value")
    result = credential_env_preflight(
        "issue_api",
        "EXAMPLE_API_TOKEN",
        aliases=("LEGACY_EXAMPLE_TOKEN",),
        required=True,
    )
    assert result.status == "ok"
    assert result.alias_refs == {
        "EXAMPLE_API_TOKEN": ("LEGACY_EXAMPLE_TOKEN",)
    }
    assert "secret-value" not in json.dumps(result.as_dict())
    missing = credential_env_preflight(
        "issue_api",
        "EXAMPLE_API_TOKEN",
        aliases=("LEGACY_EXAMPLE_TOKEN",),
        required=True,
        env={},
    )
    assert missing.status == "missing_credential"


def test_file_ref_preflight_accepts_cookie_alias(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cookie = tmp_path / "example.cookie"
    cookie.write_text("secret-cookie")
    monkeypatch.delenv("EXAMPLE_COOKIE_FILE", raising=False)
    monkeypatch.setenv("LEGACY_EXAMPLE_COOKIE_FILE", str(cookie))
    result = file_ref_preflight(
        "example_web",
        "EXAMPLE_COOKIE_FILE",
        aliases=("LEGACY_EXAMPLE_COOKIE_FILE",),
        required=True,
    )
    data = result.as_dict()
    assert result.status == "ok"
    assert data["alias_refs"] == {
        "EXAMPLE_COOKIE_FILE": ["LEGACY_EXAMPLE_COOKIE_FILE"],
    }
    assert "secret-cookie" not in json.dumps(data)
    assert str(cookie) not in json.dumps(data)
    assert "checked_at" in data


def test_run_result_includes_source_health() -> None:
    health = SourceHealth(
        status="ok",
        mode="cache",
        record_count=500,
        content_hash="abc123",
    )
    result = RunResult(
        run_id="r1",
        source_name="docs",
        mode="scope_complete",
        started_at=__import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ),
        finished_at=__import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ),
        nodes_appended=1,
        edges_appended=0,
        retractions_node=0,
        retractions_edge=0,
        source_health=health,
    )
    data = result.as_dict()
    assert data["source_health"]["mode"] == "cache"
    assert data["source_health"]["record_count"] == 500


def _write_deployment(
    root: Path,
    *,
    required_status: str | None = "missing_credential",
    optional_status: str | None = None,
) -> None:
    sys.modules.pop("auth_sources", None)
    dep = root / "demo"
    dep.mkdir()
    # `find_deployment_dir` only resolves real deployment candidates, not
    # source-registry-only directories.
    (dep / "deployment.yaml").write_text(
        "profile:\n  name: test\n",
    )
    module = dep / "auth_sources.py"
    module.write_text(
        "from okg.substrate.library.sources.base import "
        "SourcePreflightResult, SourceRun\n\n"
        "class RequiredSource:\n"
        "    name = 'required'\n"
        "    profile = 'live_overlay'\n"
        "    def preflight(self, mode='live'):\n"
        f"        return SourcePreflightResult(status={required_status!r}, "
        "credential_refs=('REQ_TOKEN',), reason='no token')\n"
        "    def run(self, run_id, *, mode='cursor'):\n"
        "        return SourceRun(facts=[])\n\n"
        "class OptionalSource:\n"
        "    name = 'optional'\n"
        "    profile = 'live_overlay'\n"
        "    def preflight(self, mode='live'):\n"
        f"        return SourcePreflightResult(status={optional_status!r}, "
        "credential_refs=('OPT_TOKEN',), reason='no token')\n"
        "    def run(self, run_id, *, mode='cursor'):\n"
        "        return SourceRun(facts=[])\n\n"
        "class NoPreflightSource:\n"
        "    name = 'no_preflight'\n"
        "    profile = 'live_overlay'\n"
        "    def run(self, run_id, *, mode='cursor'):\n"
        "        return SourceRun(facts=[])\n\n"
        "class BrokenPreflightSource:\n"
        "    name = 'broken_preflight'\n"
        "    profile = 'live_overlay'\n"
        "    def preflight(self, mode='live'):\n"
        "        raise TypeError('Authorization: Bearer glpat-supersecret')\n"
        "    def run(self, run_id, *, mode='cursor'):\n"
        "        return SourceRun(facts=[])\n"
    )
    sources = {}
    if required_status is not None:
        sources["required"] = {
            "ownership_id": "demo.required",
            "source_class": "live_overlay",
            "deletion_semantics": "external_overlay",
            "publication_mode": "live_overlay",
            "module": "auth_sources",
            "class": "RequiredSource",
            "credential_refs": ["REQ_TOKEN"],
            "required_for_baseline": True,
        }
    if optional_status is not None:
        sources["optional"] = {
            "ownership_id": "demo.optional",
            "source_class": "live_overlay",
            "deletion_semantics": "external_overlay",
            "publication_mode": "live_overlay",
            "module": "auth_sources",
            "class": "OptionalSource",
            "credential_refs": ["OPT_TOKEN"],
            "required_for_baseline": False,
        }
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources})
    )


def test_source_auth_doctor_fails_for_required_missing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_deployment(tmp_path, required_status="missing_credential")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    args = Namespace(
        check="source-auth",
        deployment="demo",
        dsn=None,
        warm=False,
    )
    rc = _cmd_doctor(args, as_json=True)
    assert rc == 1
    data = json.loads(capsys.readouterr().out)
    assert data["ok"] is False
    assert data["sources"][0]["status"] == "missing_credential"
    assert data["sources"][0]["required"] is True
    assert "REQ_TOKEN" in data["sources"][0]["credential_refs"]


def test_source_auth_doctor_warns_for_optional_missing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_deployment(
        tmp_path,
        required_status=None,
        optional_status="missing_credential",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    args = Namespace(
        check="source-auth",
        deployment="demo",
        dsn=None,
        warm=False,
    )
    rc = _cmd_doctor(args, as_json=True)
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert data["ok"] is True
    assert data["sources"][0]["status"] == "missing_credential"
    assert data["sources"][0]["required"] is False


def test_source_auth_doctor_reports_not_applicable_without_preflight(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_deployment(tmp_path, required_status=None)
    dep = tmp_path / "demo"
    registry = yaml.safe_load((dep / "source_registry.yaml").read_text())
    registry["sources"]["no_preflight"] = {
        "ownership_id": "demo.no_preflight",
        "source_class": "live_overlay",
        "deletion_semantics": "external_overlay",
        "publication_mode": "live_overlay",
        "module": "auth_sources",
        "class": "NoPreflightSource",
        "required_for_baseline": True,
    }
    (dep / "source_registry.yaml").write_text(yaml.safe_dump(registry))
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    args = Namespace(
        check="source-auth",
        deployment="demo",
        dsn=None,
        warm=False,
    )
    rc = _cmd_doctor(args, as_json=True)
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert data["sources"][0]["status"] == "not_applicable"
    assert data["sources"][0]["required"] is True


def test_source_auth_doctor_reports_preflight_type_error_once(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_deployment(tmp_path, required_status=None)
    dep = tmp_path / "demo"
    registry = yaml.safe_load((dep / "source_registry.yaml").read_text())
    registry["sources"]["broken_preflight"] = {
        "ownership_id": "demo.broken_preflight",
        "source_class": "live_overlay",
        "deletion_semantics": "external_overlay",
        "publication_mode": "live_overlay",
        "module": "auth_sources",
        "class": "BrokenPreflightSource",
        "credential_refs": ["BROKEN_TOKEN"],
        "required_for_baseline": True,
    }
    (dep / "source_registry.yaml").write_text(yaml.safe_dump(registry))
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    args = Namespace(
        check="source-auth",
        deployment="demo",
        dsn=None,
        warm=False,
    )
    rc = _cmd_doctor(args, as_json=True)
    assert rc == 1
    data = json.loads(capsys.readouterr().out)
    blob = json.dumps(data)
    assert data["sources"][0]["status"] == "endpoint_failed"
    assert "glpat-supersecret" not in blob
    assert "Authorization=<redacted>" in blob


def _write_runtime_identity_deployment(root: Path, *, contract: str | None) -> None:
    """A registry that, like okg-workspace since #2150, authors no
    ``deployment`` or ``dsn`` param for an adapter that requires one."""
    dep = root / "idem"
    dep.mkdir()
    (dep / "deployment.yaml").write_text("profile:\n  name: test\n")
    document: dict = {
        "sources": {
            "profile-measurements": {
                "module": "okg.substrate.library.sources.profile_measurements",
                "class": "ProfileMeasurementSource",
                "ownership_id": "idem.profile-measurements",
                "source_class": "discovery_crawl",
                "record_identity_kind": "scoped_locator",
                "record_identity_fields": ["path"],
                "source_revision_kind": "content_hash",
                "deletion_semantics": "missing_from_completed_scope",
                "publication_mode": "published_generation",
                "params": {"repo": "idem", "repo_root": str(root)},
            },
        },
    }
    if contract is not None:
        document["admission_contract"] = contract
    (dep / "source_registry.yaml").write_text(yaml.safe_dump(document))


@pytest.mark.parametrize("contract", [None, "strict_v1"])
def test_source_auth_doctor_injects_runtime_deployment_identity(
    tmp_path: Path,
    monkeypatch,
    capsys,
    contract: str | None,
) -> None:
    """Doctor constructs the adapter with the worker's injected params.

    Before the fix doctor passed the raw registry params, so an adapter
    whose ``deployment`` the runtime supplies failed with a missing
    ``deployment`` argument that ingest never hits.
    """
    _write_runtime_identity_deployment(tmp_path, contract=contract)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    args = Namespace(
        check="source-auth",
        deployment="idem",
        dsn="postgresql://doctor@127.0.0.1:1/unused",
        warm=False,
    )
    rc = _cmd_doctor(args, as_json=True)
    data = json.loads(capsys.readouterr().out)
    assert rc == 0, data
    (source,) = data["sources"]
    assert source["status"] == "not_applicable", source
    assert "deployment" not in json.dumps(source.get("reason") or "")
