"""Source-health and error text masks signed URLs, cookies, and auth= (#3942).

Sample secret values are fake and built at runtime so no secret-looking
literal sits in the file.
"""
from __future__ import annotations

import pytest

from okg.substrate.sources.redaction import redact_endpoint, redact_text

FAKE = "fake" + "sig" + "123"


@pytest.mark.parametrize(
    "name",
    [
        "sig",
        "signature",
        "X-Amz-Signature",
        "X-Amz-Credential",
        "X-Amz-Security-Token",
        "token",
        "access_token",
        "key",
    ],
)
@pytest.mark.parametrize("lead", ["?", "&"])
def test_signed_url_query_values_are_masked(name: str, lead: str) -> None:
    prefix = "https://files.example.org/a/b.pdf"
    url = prefix + ("?x=1" if lead == "&" else "") + f"{lead}{name}={FAKE}&n=2"
    out = redact_text(f"GET {url} failed")
    assert FAKE not in out
    assert f"{lead}{name}=<redacted>" in out
    assert "files.example.org/a/b.pdf" in out
    assert "&n=2" in out


@pytest.mark.parametrize("header", ["Cookie", "Set-Cookie", "cookie"])
def test_cookie_header_values_are_masked(header: str) -> None:
    out = redact_text(f"reply {header}: sid={FAKE}; Path=/; HttpOnly\nnext line")
    assert FAKE not in out
    assert "HttpOnly" not in out
    assert f"{header}=<redacted>" in out
    assert "next line" in out


def test_auth_assignment_is_masked() -> None:
    out = redact_text(f"connect auth={FAKE} to host")
    assert FAKE not in out
    assert "auth=<redacted>" in out
    assert "to host" in out


@pytest.mark.parametrize(
    "assignment, shown",
    [
        ('auth="{v}"', "auth=<redacted>"),
        ("auth='{v}'", "auth=<redacted>"),
        ('auth = "{v}"', "auth=<redacted>"),
        ('auth = "{v} with space"', "auth=<redacted>"),
        ("basic_auth={v}", "basic_auth=<redacted>"),
        ("http_auth={v}", "http_auth=<redacted>"),
    ],
)
def test_quoted_and_prefixed_auth_assignments_are_masked(
    assignment: str, shown: str
) -> None:
    out = redact_text("connect " + assignment.format(v=FAKE) + " to host")
    assert FAKE not in out
    assert "with space" not in out
    assert shown in out
    assert out.endswith(" to host")


def test_auth_lookalike_words_are_untouched() -> None:
    text = "author=bob oauth=enabled unauth=1 authority=example.org"
    assert redact_text(text) == text


@pytest.mark.parametrize(
    "text",
    [
        "{{'Cookie': 'sid={v}'}} done",
        '{{"cookie":"sid={v}"}} done',
        "{{'Set-Cookie': 'sid={v}; Path=/'}} done",
        '{{"Set-Cookie": "sid={v}; Path=/"}} done',
    ],
)
def test_quoted_cookie_keys_are_masked_and_stop_at_the_quote(text: str) -> None:
    out = redact_text(text.format(v=FAKE))
    assert FAKE not in out
    assert "Path" not in out
    assert "<redacted>" in out
    assert out.endswith("done")


@pytest.mark.parametrize(
    "text",
    [
        '{"Cookie": "theme=\\"dark\\"; sid=' + FAKE + '"} done',
        'Cookie: "x"; sid=' + FAKE + " done",
        "Cookie: 'a=1'; sid=" + FAKE + " done",
    ],
)
def test_cookie_values_with_escaped_or_leading_quotes_are_fully_masked(
    text: str,
) -> None:
    out = redact_text(text)
    assert FAKE not in out
    assert "<redacted>" in out


@pytest.mark.parametrize("text", ['auth="' + FAKE, "auth='" + FAKE])
def test_unterminated_quoted_auth_value_is_masked(text: str) -> None:
    out = redact_text("connect " + text)
    assert FAKE not in out
    assert "auth=<redacted>" in out


def test_glued_text_after_a_masked_token_is_masked_too() -> None:
    out = redact_text(f"GET https://h.example.org/x?token={FAKE}#frag{FAKE} failed")
    assert FAKE not in out
    assert "token=<redacted>" in out
    kept = redact_text(f"https://h.example.org/x?token={FAKE}&n=2")
    assert kept.endswith("token=<redacted>&n=2")


@pytest.mark.parametrize(
    "header",
    [
        "Authorization: Basic {v}",
        "Authorization: token {v}",
        "Authorization: Digest {v}",
        "Authorization: Negotiate {v}",
        "Proxy-Authorization: Basic {v}",
    ],
)
def test_authorization_schemes_are_masked(header: str) -> None:
    out = redact_text(header.format(v=FAKE) + " next")
    assert FAKE not in out
    assert "Authorization=<redacted>" in out
    assert out.endswith(" next")


def test_harmless_text_is_untouched() -> None:
    text = (
        "author=bob oauth=enabled cookies are disabled; "
        "see https://host.example.org/docs/key?page=2 for the signature scheme"
    )
    assert redact_text(text) == text


def test_redact_endpoint_masks_schemeless_and_drops_url_query() -> None:
    assert FAKE not in (redact_endpoint(f"host.example.org/x?sig={FAKE}") or "")
    assert redact_endpoint(f"https://host.example.org/x?sig={FAKE}") == (
        "https://host.example.org/x"
    )
