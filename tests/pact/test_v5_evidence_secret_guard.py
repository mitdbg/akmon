"""An evidence artifact must not carry an unmasked credential.

Evidence artifacts are committed, and the repository goes public. Agents
captured end-to-end transcripts whose `--postgres-dsn` line carried a live
Postgres password into PACT evidence. `pact evidence attach` now refuses such
an artifact, and `pact check` refuses a row this branch adds or edits that
names one, while only warning about a row that already merged.

Every parametrized case below carries an explicit, value-free id: a test id
lands in the junit XML this change attaches as its own evidence, and an id
spelling a fake password would make that junit trip the very scan it proves.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pact import check as checker
from pact.cli import main
from pact.credentials import KIND_BEARER, KIND_PASSWORD, KIND_PASSWORD_KEY, KIND_PRIVATE_KEY, KIND_TOKEN, KIND_URI, Finding, scan_bytes, scan_text
from pact.digest import file_sha256
from pact.store import load
from _v5_repo import MANIFEST, commit_all, git, make_repo, write_manifest

EVIDENCE = "pact/changes/demo/evidence"
# Assembled at run time so this source file never spells a credential that a
# repository-wide secret scanner would also flag.
LIVE = "Zq" + "7-live-value-" + "41"
# A random-looking token body, and the prefixes, split for the same reason.
BODY = "Q7m2Xk9" + "Lp4Rt8Vw1Nz6" + "Hc3Jd5Fg0Bs"
GHP, PAT, ANT = "gh" + "p_", "github" + "_pat_", "sk-" + "ant-"
PEM = "-----BEGIN " + "RSA PRIVATE KEY-----"

POSITIVE = [
    ("uri_postgresql", f"okg ingest --postgres-dsn postgresql://postgres:{LIVE}@127.0.0.1:55432/okg", KIND_URI),
    ("uri_postgres", f"DSN=postgres://okg_admin:{LIVE}@db.internal/okg", KIND_URI),
    ("uri_driver_suffix", f"url: postgresql+psycopg://reader:{LIVE}@localhost/okg", KIND_URI),
    ("uri_in_xml_attribute", f'<testcase name="run" message="postgresql://u:{LIVE}@h:5/db" />', KIND_URI),
    ("libpq_keyword", f"host=127.0.0.1 port=5432 user=okg password={LIVE} dbname=okg", KIND_PASSWORD),
    ("pgpassword_env", f"PGPASSWORD={LIVE} psql -h 127.0.0.1", KIND_PASSWORD),
    ("pgpassword_export_quoted", f"export PGPASSWORD='{LIVE}'", KIND_PASSWORD),
    ("postgres_password_env", f"docker run -e POSTGRES_PASSWORD={LIVE} postgres:16", KIND_PASSWORD),
    ("bearer_header", f"Authorization: Bearer {LIVE}", KIND_BEARER),
    ("bearer_curl", f'curl -H "authorization: bearer {LIVE}" https://example.invalid', KIND_BEARER),
    ("bearer_json", f'{{"Authorization": "Bearer {LIVE}"}}', KIND_BEARER),
    ("uri_mysql", f"mysql://root:{LIVE}@127.0.0.1:3306/app", KIND_URI),
    ("uri_mariadb", f"mariadb://app:{LIVE}@db/app", KIND_URI),
    ("uri_redis_empty_user", f"redis://:{LIVE}@cache:6379/0", KIND_URI),
    ("uri_rediss", f"rediss://default:{LIVE}@cache:6380", KIND_URI),
    ("uri_amqp", f"amqp://guest:{LIVE}@broker:5672/", KIND_URI),
    ("uri_mongodb", f"mongodb://admin:{LIVE}@mongo:27017/db", KIND_URI),
    ("uri_mongodb_srv", f"mongodb+srv://admin:{LIVE}@cluster0.example.net/db", KIND_URI),
    ("yaml_env_key", f"      POSTGRES_PASSWORD: {LIVE}", KIND_PASSWORD_KEY),
    ("yaml_key", f"password: {LIVE}", KIND_PASSWORD_KEY),
    ("json_key", f'{{"user": "okg", "password": "{LIVE}"}}', KIND_PASSWORD_KEY),
    ("token_github_classic", f"GH_TOKEN={GHP}{BODY}", KIND_TOKEN),
    ("token_github_fine_grained", f"token {PAT}{BODY}", KIND_TOKEN),
    ("token_anthropic", f"ANTHROPIC_API_KEY {ANT}api03-{BODY}", KIND_TOKEN),
    ("private_key", PEM, KIND_PRIVATE_KEY),
]

NEGATIVE = [
    ("uri_masked_six", "postgresql://postgres:******@127.0.0.1:55432/okg"),
    ("uri_masked_three", "postgresql://postgres:***@127.0.0.1/okg"),
    ("uri_local_default_okg", "postgresql://postgres:okg@127.0.0.1:5432/okg"),
    ("uri_fixture_placeholder", "postgresql://user:password@localhost/db"),
    ("uri_template_braces", "postgresql://postgres:{password}@localhost/okg"),
    ("uri_template_env", "postgresql://postgres:${PGPASSWORD}@localhost/okg"),
    ("uri_template_angle", "postgresql://postgres:<password>@localhost/okg"),
    ("uri_no_password", "postgresql://postgres@127.0.0.1:5432/okg"),
    ("uri_empty_password", "postgresql://postgres:@127.0.0.1:5432/okg"),
    ("uri_host_port_only", "postgresql://127.0.0.1:5432/okg"),
    ("password_masked", "password=****** host=127.0.0.1"),
    ("pgpassword_masked", "PGPASSWORD=****** psql"),
    ("pgpassword_env_reference", "PGPASSWORD=$OKG_PW psql"),
    ("password_placeholder", "password=okg"),
    ("password_empty", "password= host=x"),
    ("other_word_ending_password", "db_password_file=/run/secrets/pg"),
    ("bearer_masked", "Authorization: Bearer ******"),
    ("bearer_template", "Authorization: Bearer ${GITHUB_TOKEN}"),
    ("prose", "the password is masked before the transcript is written"),
    ("value_labelled_fake", "postgresql://u:fake-sentinel-pw@h/db"),
    ("pytest_id_masked_suffix", "test_redacts[password=***]"),
    ("bearer_in_prose", "send Authorization: Bearer and the token from the vault"),
    ("password_built_in_code", "password=uuid.uuid4().hex"),
    ("uri_redis_no_credentials", "redis://cache:6379/0"),
    ("bearer_json_masked", '{"Authorization": "Bearer ******"}'),
    ("yaml_key_masked", "password: ******"),
    ("yaml_key_template", "POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}"),
    ("json_key_placeholder", '{"password": "okg"}'),
    ("json_key_count", '{"uri_password": 23}'),
    ("json_key_list", '"password": ["a", "b"]'),
    ("annotation", "def connect(password: str) -> None:"),
    ("yaml_key_code", "password: secrets.token_urlsafe(16)"),
    ("token_repeated_unit", f"{GHP}" + "Ab1" * 12),
    ("token_alphabet_run", f"{GHP}abcdefghijklmnopqrstuvwxyz0123456789"),
    ("token_too_short", f"{GHP}short"),
    ("token_prefix_alone", f"tokens start with {GHP} or {PAT}"),
    ("public_key", "-----BEGIN " + "PUBLIC KEY-----"),
]


@pytest.mark.parametrize(("line", "kind"), [(line, kind) for _, line, kind in POSITIVE], ids=[name for name, _, _ in POSITIVE])
def test_each_pattern_finds_an_unmasked_credential(line: str, kind: str) -> None:
    assert scan_text(f"first line\n{line}\nlast line\n") == [Finding(2, kind)]


@pytest.mark.parametrize("line", [line for _, line in NEGATIVE], ids=[name for name, _ in NEGATIVE])
def test_a_masked_placeholder_or_absent_value_is_not_a_finding(line: str) -> None:
    assert scan_text(line) == []


def test_a_junit_test_id_is_skipped_but_its_failure_text_is_not() -> None:
    """A test id is test source, already public; captured output is not.
    The blanking applies only to a JUnit file (`.xml` with `<testcase`)."""
    xml = (
        '<testcase classname="tests.test_dsn" name="test_mask[PGPASSWORD=Zq7live]" time="0.1">'
        f'<failure message="leaked">okg ingest --postgres-dsn postgresql://postgres:{LIVE}@127.0.0.1/okg</failure>'
        "</testcase>"
    ).encode()
    assert scan_bytes(xml, "pact/changes/demo/evidence/junit.xml") == [Finding(1, KIND_URI)]
    # The same text in any other file type is scanned whole, test id included.
    assert scan_bytes(xml, "pact/changes/demo/evidence/junit.txt") == [Finding(1, KIND_URI), Finding(1, KIND_PASSWORD)]
    # An .xml file that is not JUnit is not blanked either.
    other = b'<config name="PGPASSWORD=Zq7live" />'
    assert scan_bytes(other, "pact/changes/demo/evidence/config.xml") == [Finding(1, KIND_PASSWORD)]


def test_binary_and_non_utf8_artifacts_are_skipped() -> None:
    secret = f"PGPASSWORD={LIVE}".encode()
    assert scan_bytes(b"\x89PNG\r\n\x1a\n\x00" + secret) == []
    assert scan_bytes(b"\xff\xfe" + secret) == []
    assert scan_bytes(secret) == [Finding(1, KIND_PASSWORD)]


# --- attach and check -------------------------------------------------------


@pytest.fixture
def repo(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = make_repo(tmp_path)
    write_manifest(root, MANIFEST)
    (root / "src").mkdir()
    (root / "src" / "a.py").write_text("x = 1\n", encoding="utf-8")
    commit_all(root, "feat")
    monkeypatch.chdir(root)
    return root


def _write(root: Path, relative: str, text: str) -> Path:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    return target


def _attach(relative: str) -> int:
    return main(["evidence", "attach", "demo", "--kind", "executed_test", "--requirement", "req.demo", "--artifact", relative, "--json"])


TRANSCRIPT = f"$ okg publish\n$ okg ingest --postgres-dsn postgresql://postgres:{LIVE}@127.0.0.1:55432/okg\nok\n"


def test_attach_refuses_an_artifact_with_an_unmasked_password(repo: Path, capsys) -> None:
    relative = f"{EVIDENCE}/e2e.txt"
    _write(repo, relative, TRANSCRIPT)
    exit_code = _attach(relative)
    assert exit_code == 1
    envelope = json.loads(capsys.readouterr().out)
    assert envelope["verdict"] == "refused"
    assert envelope["blocked_by"] == [f"evidence_artifact_contains_secret:{relative}:2"]
    # The message names the line and the kind and says how to fix it; it
    # must never repeat the value it found.
    assert "line 2" in envelope["text"]
    assert "uri password" in envelope["text"]
    assert "******" in envelope["text"]
    assert LIVE not in json.dumps(envelope)
    assert load(repo / "pact/changes/demo/pact.yaml").evidence == []


def test_attach_accepts_the_same_artifact_once_masked(repo: Path, capsys) -> None:
    relative = f"{EVIDENCE}/e2e.txt"
    _write(repo, relative, TRANSCRIPT.replace(LIVE, "******"))
    exit_code = _attach(relative)
    assert exit_code == 0
    envelope = json.loads(capsys.readouterr().out)
    assert envelope["verdict"] == "recorded"


def _row(repo: Path, relative: str, text: str) -> None:
    target = _write(repo, relative, text)
    data = json.loads(load(repo / "pact/changes/demo/pact.yaml").model_dump_json(exclude_none=True))
    data["evidence"] = [{
        "kind": "executed_test", "requirement": "req.demo", "status": "pass",
        "artifact": relative, "sha256": file_sha256(target.read_bytes()),
        "head": git(repo, "rev-parse", "HEAD"),
    }]
    write_manifest(repo, data)


def test_check_refuses_a_new_row_whose_artifact_holds_a_secret(repo: Path) -> None:
    # Written by hand, as a row that bypassed attach would be.
    _row(repo, f"{EVIDENCE}/e2e.txt", TRANSCRIPT)
    commit_all(repo, "a receipt carrying a live password")
    for result in (checker.run(repo, "demo"), checker.run(repo, "demo", strict=True)):
        assert result.verdict == "refused"
        assert "evidence_artifact_contains_secret:0:2" in result.blocked_by
        assert result.evidence["proven_requirements"] == []
    assert checker.run(repo, "demo").evidence["broken"] == 1


def test_check_only_warns_about_a_row_that_already_merged(repo: Path) -> None:
    """Same scope rule as the generated-artifact guard: a receipt someone
    else recorded and merged cannot red this pull request."""
    _row(repo, f"{EVIDENCE}/e2e.txt", TRANSCRIPT)
    commit_all(repo, "a receipt carrying a live password")
    git(repo, "checkout", "-q", "dev")
    git(repo, "merge", "-q", "--ff-only", "feature")
    git(repo, "fetch", "-q", "origin", "dev")
    git(repo, "checkout", "-q", "feature")
    (repo / "src" / "b.py").write_text("y = 2\n", encoding="utf-8")
    commit_all(repo, "later work on the same branch")

    result = checker.run(repo, "demo")

    assert result.evidence["preexisting"] == 1
    assert "evidence_artifact_contains_secret:0:2" in result.warnings
    assert not any(token.startswith("evidence_artifact_contains_secret") for token in result.blocked_by)


def test_check_accepts_a_new_row_with_a_masked_artifact(repo: Path) -> None:
    _row(repo, f"{EVIDENCE}/e2e.txt", TRANSCRIPT.replace(LIVE, "******"))
    commit_all(repo, "a masked receipt")
    result = checker.run(repo, "demo")
    assert not any("contains_secret" in token for token in result.blocked_by + result.warnings)
    assert result.evidence["broken"] == 0


def test_check_refuses_a_changed_evidence_file_no_row_names(repo: Path) -> None:
    """A transcript committed without `evidence attach` is still published."""
    _write(repo, f"{EVIDENCE}/transcript.txt", TRANSCRIPT)
    _write(repo, f"{EVIDENCE}/clean.txt", "ok\n")
    commit_all(repo, "commit a transcript without attaching it")
    result = checker.run(repo, "demo")
    assert result.verdict == "refused"
    assert f"evidence_artifact_contains_secret:{EVIDENCE}/transcript.txt:2" in result.blocked_by
    assert not any("clean.txt" in token for token in result.blocked_by)
    assert "evidence_files_unscanned" not in result.warnings


def test_check_leaves_a_merged_evidence_file_alone(repo: Path) -> None:
    _write(repo, f"{EVIDENCE}/transcript.txt", TRANSCRIPT)
    commit_all(repo, "commit a transcript without attaching it")
    git(repo, "checkout", "-q", "dev")
    git(repo, "merge", "-q", "--ff-only", "feature")
    git(repo, "fetch", "-q", "origin", "dev")
    git(repo, "checkout", "-q", "feature")
    (repo / "src" / "b.py").write_text("y = 2\n", encoding="utf-8")
    commit_all(repo, "later work on the same branch")

    result = checker.run(repo, "demo")

    assert not any(token.startswith("evidence_artifact_contains_secret") for token in result.blocked_by + result.warnings)
    assert "evidence_files_unscanned" not in result.warnings
