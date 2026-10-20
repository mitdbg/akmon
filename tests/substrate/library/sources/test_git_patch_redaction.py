"""Stored commit diffs hide the secret shapes found in real history (issue #3468).

A gitleaks scan of the comp-ops repositories' stored diffs found GitLab
tokens, JWTs, a base64-encoded private key in a Kubernetes secret, and a
`--NotebookApp.password='...'` flag that the patch redactor let through.
Every value here is fake and assembled at runtime (tests/substrate/_fake_credentials.py).
"""

from __future__ import annotations

import base64
import os
import subprocess
from pathlib import Path

from okg.substrate.library.sources._git_graph_history_emit import (
    _REDACTED,
    PATCH_REDACTION_VERSION,
    _redact_patch,
)
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.git_graph_history import GitHistorySource
from tests.substrate._fake_credentials import fake_credential, pem_begin

_BODY = "".join(chr(ord("A") + (i * 7) % 26) for i in range(64))


def _pem(kind: str = "RSA", lines: int = 6) -> str:
    end = pem_begin(kind).replace("BEGIN", "END")
    return "\n".join([pem_begin(kind), *([_BODY] * lines), end])


def _patch(*added: str) -> str:
    return "\n".join(
        ["--- a/f", "+++ b/f", f"@@ -0,0 +1,{len(added)} @@", *[f"+{a}" for a in added]]
    )


def _redact(text: str) -> str:
    return _redact_patch(text, repo_root=Path("/nonexistent"))


def test_gitlab_tokens_are_redacted() -> None:
    for prefix in ("glpat-", "gldt-", "glrt-", "glcbt-", "GR1348941"):
        token = fake_credential(prefix, "Ab3dEf6hIj9kLm2nOp5q")
        out = _redact(_patch(f'GITLAB_URL = "https://gitlab.example/api?private={token}"'))
        assert token not in out, prefix
        assert _REDACTED in out


def test_jwts_are_redacted() -> None:
    jwt = fake_credential("eyJ", "hbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.c2lnbmF0dXJlLXZhbHVl")
    out = _redact(_patch(f"    sample = '{jwt}'  # recorded response"))
    assert jwt not in out
    assert "# recorded response" in out


def test_a_base64_encoded_private_key_is_redacted() -> None:
    encoded = base64.b64encode(_pem().encode()).decode()
    out = _redact(_patch("apiVersion: v1", "kind: Secret", "data:", f"  tls.key: {encoded}"))
    assert encoded[:40] not in out
    assert "  tls.key: " + _REDACTED in out
    assert "kind: Secret" in out


def test_a_base64_encoded_certificate_is_kept() -> None:
    cert = pem_begin("RSA").replace("RSA PRIVATE KEY", "CERTIFICATE") + "\n" + _BODY
    encoded = base64.b64encode(cert.encode()).decode()
    out = _redact(_patch(f"  tls.crt: {encoded}"))
    assert encoded in out


def test_a_wrapped_encoded_key_is_redacted_line_by_line() -> None:
    encoded = base64.b64encode(_pem(lines=10).encode()).decode()
    chunks = [encoded[i : i + 76] for i in range(0, len(encoded), 76)]
    out = _redact(_patch("key: |", *[f"  {c}" for c in chunks], "next: value"))
    for chunk in chunks:
        assert chunk not in out
    assert out.endswith("+next: value")


def test_a_pem_key_cut_off_by_the_hunk_is_redacted() -> None:
    """The hunk ends before the END line, so the paired pattern cannot match."""
    cut = "\n".join(_pem(lines=8).split("\n")[:5])
    out = _redact(_patch(*cut.split("\n")))
    assert _BODY not in out
    assert out.count(_REDACTED) == 4
    assert all(line.startswith("+") for line in out.split("\n")[3:])


def test_an_encoded_key_wrapped_at_forty_columns_is_redacted() -> None:
    encoded = base64.b64encode(_pem().encode()).decode()
    chunks = [encoded[i : i + 40] for i in range(0, len(encoded), 40)]
    out = _redact(_patch("tls.key: |", *[f"  {c}" for c in chunks]))
    for chunk in chunks:
        assert chunk not in out


def test_an_encoded_key_split_into_quoted_pieces_is_redacted() -> None:
    encoded = base64.b64encode(_pem().encode()).decode()
    pieces = [encoded[i : i + 20] for i in range(0, len(encoded), 20)]
    lines = [f'KEY = ("{pieces[0]}"', *[f'       "{p}"' for p in pieces[1:]], ")"]
    out = _redact(_patch(*lines))
    for piece in pieces:
        assert piece not in out


def test_a_url_safe_encoded_key_is_redacted() -> None:
    encoded = base64.urlsafe_b64encode(_pem().encode()).decode()
    out = _redact(_patch(f"  tls.key: {encoded}"))
    assert encoded[20:60] not in out
    assert "  tls.key: " + _REDACTED in out


def test_a_key_cut_off_at_the_start_of_the_hunk_is_redacted() -> None:
    tail = _pem(lines=8).split("\n")[4:]
    out = _redact(_patch(*tail))
    assert _BODY not in out


def test_a_pgp_private_key_block_is_redacted() -> None:
    begin = "-----BEGIN PGP PRIVATE " + "KEY BLOCK-----"
    out = _redact(_patch(begin, "", _BODY, _BODY, "=AbCd"))
    assert _BODY not in out


def test_code_and_certificates_after_a_key_are_kept() -> None:
    encoded_cert = base64.b64encode(
        (pem_begin("RSA").replace("RSA PRIVATE KEY", "CERTIFICATE") + "\n" + _BODY).encode()
    ).decode()
    encoded_key = base64.b64encode(_pem().encode()).decode()
    lines = (
        f"if line.startswith('{pem_begin('RSA')}'):",
        "    return parsePrivateKeyHeader(line)",
        "path = 'src/okg/substrate/library'",
        '"integrity": "sha512-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789==",',
        f"  tls.key: {encoded_key}",
        f"  ca.crt: {encoded_cert}",
    )
    out = _redact(_patch(*lines))
    for line in lines[1:4] + lines[5:]:
        assert f"+{line}" in out


def test_a_jvm_password_option_is_redacted() -> None:
    value = fake_credential("change", "it-Xy9")
    out = _redact(_patch(f"ENV JAVA_OPTS=-Djavax.net.ssl.keyStorePassword={value}"))
    assert value not in out


def test_routable_and_feature_flag_gitlab_tokens_and_unsigned_jwts() -> None:
    routable = fake_credential("glpat-", "Ab3dEf6hIj9kLm2nOp5q.01.1a2b3c4d5")
    flag = fake_credential("glffct-", "Ab3dEf6hIj9kLm2nOp5q")
    unsigned = fake_credential("eyJ", "hbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.")
    out = _redact(_patch(f"a={routable}", f"b=x_{flag}", f"c={unsigned}"))
    for value in (routable, flag, unsigned):
        assert value not in out
    assert ".01." not in out


def test_a_cut_off_key_with_windows_line_endings_is_redacted() -> None:
    cut = _pem(lines=8).split("\n")[:5]
    out = _redact(_patch(*[f"{line}\r" for line in cut]))
    assert _BODY not in out


def test_a_cut_off_key_after_armor_headers_is_redacted() -> None:
    pgp = "-----BEGIN PGP PRIVATE " + "KEY BLOCK-----"
    out = _redact(_patch(pgp, "Version: GnuPG v2", "", _BODY, _BODY))
    assert _BODY not in out
    rsa = [pem_begin("RSA"), "Proc-Type: 4,ENCRYPTED", "DEK-Info: AES-128-CBC,0011", "", _BODY]
    out = _redact(_patch(*rsa))
    assert _BODY not in out
    assert "DEK-Info: AES-128-CBC,0011" in out


def test_a_quoted_jvm_option_is_redacted() -> None:
    value = fake_credential("hunter", "22-Xy9")
    out = _redact(_patch(f'args: ["-Dpassword={value}", "-jar", "app.jar"]'))
    assert value not in out


def test_a_complete_pem_key_is_still_redacted() -> None:
    out = _redact(_patch(*_pem().split("\n")))
    assert _BODY not in out


def test_a_double_dash_flag_password_is_redacted() -> None:
    value = fake_credential("sha1:", "0f1e2d3c4b5a:9a8b7c6d5e4f3a2b1c0d")
    out = _redact(_patch(f"    command: start-notebook.sh --NotebookApp.password='{value}'"))
    assert value not in out
    assert "--NotebookApp.password='" + _REDACTED + "'" in out


def test_encryption_and_signing_keys_are_redacted() -> None:
    value = fake_credential("k3y-", "Zq8Wv2Lp0Rt6Nm4")
    out = _redact(_patch(f"BYOK_ENCRYPTION_KEY={value}", f"jwt.signingKey: {value}"))
    assert value not in out


def test_ordinary_patch_text_is_kept() -> None:
    lines = (
        "version = '1.2.3'",
        "openapi-schema-pydantic==1.2.4",
        "kubectl create secret generic app-secrets --from-file=config.json",
        "checksum = 0123456789abcdef0123456789abcdef",
    )
    out = _redact(_patch(*lines))
    for line in lines:
        assert f"+{line}" in out


def _git(root: Path, *args: str, at: int = 0) -> None:
    env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": "Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.test",
        "GIT_COMMITTER_NAME": "Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.test",
        "GIT_AUTHOR_DATE": f"@{1_700_000_000 + at} +0000",
        "GIT_COMMITTER_DATE": f"@{1_700_000_000 + at} +0000",
    }
    subprocess.run(["git", *args], cwd=root, env=env, check=True, capture_output=True)


def _diff_patches(facts) -> list[str]:  # noqa: ANN001
    return [
        f.attrs["patch"]
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "git_file_diff" and f.op == "I"
    ]


def test_stored_diffs_hide_the_shapes_and_an_old_cursor_rewrites_them(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q", "-b", "main")
    token = fake_credential("glpat-", "Ab3dEf6hIj9kLm2nOp5q")
    encoded = base64.b64encode(_pem().encode()).decode()
    (root / "deploy.yaml").write_text(f"kind: Secret\ndata:\n  tls.key: {encoded}\n")
    (root / "ci.cfg").write_text(f"token_url = https://gitlab.example/?t={token}\n")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "Add config", at=10)

    first = GitHistorySource(repo="r", repo_root=root, store_diffs=True).run(
        "run-1", mode="cursor"
    )
    patches = _diff_patches(list(first.facts))
    assert len(patches) == 2
    assert all(token not in p and encoded[:40] not in p for p in patches)
    assert first.next_cursor["diff_settings"]["redaction"] == PATCH_REDACTION_VERSION == 2

    # A cursor written before these shapes (no redaction version) re-walks,
    # so stored patches are rewritten with the current rules.
    old = dict(first.next_cursor)
    old["diff_settings"] = {
        k: v for k, v in old["diff_settings"].items() if k != "redaction"
    }
    rewalk = GitHistorySource(repo="r", repo_root=root, store_diffs=True).run(
        "run-2", mode="cursor", cursor=old
    )
    assert len(_diff_patches(list(rewalk.facts))) == 2
    again = GitHistorySource(repo="r", repo_root=root, store_diffs=True).run(
        "run-3", mode="cursor", cursor=rewalk.next_cursor
    )
    assert list(again.facts) == []
