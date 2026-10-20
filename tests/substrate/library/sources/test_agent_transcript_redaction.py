from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from okg.substrate.library.sources.base import NodeFact
from tests.substrate._fake_credentials import PEM_EC_BEGIN, PEM_OPENSSH_BEGIN, PEM_RSA_BEGIN


CANARY = "transcript-canary-value-DO-NOT-PERSIST"
JWT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiJ0cmFuc2NyaXB0LWNhbmFyeSJ9."
    "c2lnbmF0dXJlLWNhbmFyeS12YWx1ZQ"
)
BASE64_BLOB = (
    "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo"
    "wMTIzNDU2Nzg5QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo"
    "wMTIzNDU2Nzg5QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo="
)
PRIVATE_KEY = (
    f"{PEM_OPENSSH_BEGIN}\n"
    f"{CANARY}\n"
    "-----END OPENSSH PRIVATE KEY-----"
)


PEM_BODY = [
    "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdz",
    "c2gtcnNhAAAAAwEAAQAAAYEAy1x2Qw7Zk9Yp3Rr4Tt6Uu8Vv0Ww2Xx4Yy6Zz8Aa0",
    "MIIEpAIBAAKCAQEA7Xn5vQ==",
]


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def _leaky_command() -> str:
    return "\n".join(
        [
            "docker run -e OPENAI_API_KEY=" + CANARY + " python:3.12",
            "curl -H 'Authorization: Bearer " + CANARY + "' https://example.com/docs",
            "DATABASE_URL=postgres://reader:" + CANARY + "@localhost:5432/okg",
            "{\"api_key\": \"" + CANARY + "\"}",
            "{\"AWS_SECRET_ACCESS_KEY\": \"" + CANARY + "\"}",
            "{\"MY_TOKEN_FOR_X\": \"" + CANARY + "\"}",
            "JWT=" + JWT,
            "SESSION_BLOB=" + BASE64_BLOB,
            PRIVATE_KEY,
            "uv run okg deployment ready okg-workspace --json",
            "contact dev@example.com",
        ]
    )


def test_scrubber_redacts_extended_secret_shapes_without_losing_context() -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
        transcript_secret_classes,
    )

    scrubbed = scrub_transcript_text(_leaky_command())

    for raw in (CANARY, JWT, BASE64_BLOB):
        assert raw not in scrubbed
    assert "OPENAI_API_KEY=***" in scrubbed
    assert "Authorization: Bearer ***" in scrubbed
    assert "DATABASE_URL=***" in scrubbed
    assert '"api_key": "***"' in scrubbed
    assert '"AWS_SECRET_ACCESS_KEY": "***"' in scrubbed
    assert '"MY_TOKEN_FOR_X": "***"' in scrubbed
    assert f"{PEM_OPENSSH_BEGIN}" in scrubbed
    assert "-----END OPENSSH PRIVATE KEY-----" in scrubbed
    assert "uv run okg deployment ready okg-workspace --json" in scrubbed
    assert "https://example.com/docs" in scrubbed
    assert "dev@example.com" in scrubbed
    assert {
        "authorization_header",
        "base64_blob",
        "credential_url",
        "jwt",
        "private_key_block",
        "secret_assignment",
    } <= set(transcript_secret_classes(_leaky_command()))


def test_codex_and_claude_sources_scrub_extended_canaries_before_graph_attrs(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    repo.mkdir()
    codex_transcript = tmp_path / "codex" / "sessions" / "extended.jsonl"
    _write_jsonl(
        codex_transcript,
        [
            {
                "type": "session_meta",
                "timestamp": "2026-07-03T12:00:00Z",
                "payload": {
                    "id": "codex-extended",
                    "cwd": str(repo),
                    "model": "gpt-5",
                    "timestamp": "2026-07-03T12:00:00Z",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-07-03T12:00:01Z",
                "payload": {
                    "type": "function_call",
                    "name": "exec_command",
                    "arguments": json.dumps({"cmd": _leaky_command()}),
                },
            },
        ],
    )
    codex_facts = list(
        CodexSessionsSource(
            repo="okg",
            repo_root=repo,
            codex_sessions_root=codex_transcript.parent,
            archived_sessions_root=None,
            since="all",
            capture_tool_use_events=["exec_command"],
        ).run("codex-redaction", mode="scope_complete").facts
    )

    projects_root = tmp_path / "claude-projects"
    claude_session_dir = projects_root / "repo"
    claude_transcript = claude_session_dir / "extended.jsonl"
    _write_jsonl(
        claude_transcript,
        [
            {
                "type": "user",
                "sessionId": "claude-extended",
                "timestamp": "2026-07-03T12:00:00Z",
                "cwd": "/repo",
                "message": {
                    "role": "user",
                    "content": "run the credential smoke test",
                },
            },
            {
                "type": "assistant",
                "sessionId": "claude-extended",
                "timestamp": "2026-07-03T12:00:01Z",
                "cwd": "/repo",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "Bash",
                            "input": {"command": _leaky_command()},
                        }
                    ],
                },
            },
        ],
    )
    claude_facts = list(
        AgentSessionsSource(
            repo="okg",
            claude_projects_root=projects_root,
            cwd_filter=[Path("/repo")],
            since="all",
            cost_estimation=False,
            capture_tool_use_events=["Bash"],
        ).run("claude-redaction", mode="scope_complete").facts
    )

    stored = json.dumps(
        {
            "codex": [
                fact.attrs
                for fact in codex_facts
                if isinstance(fact, NodeFact)
            ],
            "claude": [
                fact.attrs
                for fact in claude_facts
                if isinstance(fact, NodeFact)
            ],
        },
        sort_keys=True,
    )
    for raw in (CANARY, JWT, BASE64_BLOB):
        assert raw not in stored
    assert "OPENAI_API_KEY=***" in stored
    assert "Authorization: Bearer ***" in stored
    assert "uv run okg deployment ready okg-workspace --json" in stored
    assert "transcript-canary-value" not in stored
    # The JSON-quoted multi-word key shape reaches graph attrs through the real
    # source path, not just through scrub_transcript_text in isolation.
    assert '\\"AWS_SECRET_ACCESS_KEY\\": \\"***\\"' in stored


SECRET_VALUE = "abcd/efgh+ijklmnop"

# Every shape here leaked before the key pattern was made a single group with
# the secret word allowed anywhere in the name. The pair is (input, expected).
SECRET_ASSIGNMENT_SHAPES = [
    # The reported shape: a JSON-quoted key whose name is several secret-ish
    # words. The optional quotes used to bind to opposite alternation branches,
    # so the separator landed on the closing quote and nothing matched.
    (
        f'"AWS_SECRET_ACCESS_KEY": "{SECRET_VALUE}"',
        '"AWS_SECRET_ACCESS_KEY": "***"',
    ),
    (
        f"'AWS_SECRET_ACCESS_KEY': '{SECRET_VALUE}'",
        "'AWS_SECRET_ACCESS_KEY': '***'",
    ),
    (
        f'"AWS_SECRET_ACCESS_KEY" : "{SECRET_VALUE}"',
        '"AWS_SECRET_ACCESS_KEY" : "***"',
    ),
    (
        f"AWS_SECRET_ACCESS_KEY={SECRET_VALUE}",
        "AWS_SECRET_ACCESS_KEY=***",
    ),
    (
        f'"aws_secret_access_key": "{SECRET_VALUE}"',
        '"aws_secret_access_key": "***"',
    ),
    # The secret word sits in the MIDDLE of the key name, not at the end. The
    # pattern used to anchor the word at the end, so it matched only `MY_TOKEN`
    # and then the separator landed on `_FOR_X`.
    (
        f'"MY_TOKEN_FOR_X": "{SECRET_VALUE}"',
        '"MY_TOKEN_FOR_X": "***"',
    ),
    (
        f"'MY_TOKEN_FOR_X': '{SECRET_VALUE}'",
        "'MY_TOKEN_FOR_X': '***'",
    ),
    (
        f"MY_TOKEN_FOR_X={SECRET_VALUE}",
        "MY_TOKEN_FOR_X=***",
    ),
    (
        f"MY_SECRET_FOR_X={SECRET_VALUE}",
        "MY_SECRET_FOR_X=***",
    ),
    # Shapes that already worked, kept so a future pattern change cannot trade
    # one shape for another.
    (f'"api_key": "{SECRET_VALUE}"', '"api_key": "***"'),
    (f"OPENAI_API_KEY={SECRET_VALUE}", "OPENAI_API_KEY=***"),
    # CLI flag forms. Shell command text is this scrubber's primary input
    # (`_agent_sessions_extract` and `codex_sessions` both feed it `Bash: ...`),
    # and `--password=` is the commonest way a credential appears there. An
    # earlier revision of this fix put `-` in the key pattern's lookbehind,
    # which left a hyphen-prefixed key with no admissible start and silently
    # dropped every one of these.
    (f"mysql --password={SECRET_VALUE}", "--password=***"),
    (f"curl --api-key={SECRET_VALUE}", "--api-key=***"),
    (f"myapp --token={SECRET_VALUE}", "--token=***"),
    (f"myapp --secret={SECRET_VALUE}", "--secret=***"),
    (f"cmd -password={SECRET_VALUE}", "-password=***"),
    (f"svc --svc-account-password={SECRET_VALUE}", "--svc-account-password=***"),
    # Digit-prefixed key names. The consuming part must admit a leading digit,
    # or these lose their only admissible start the same way.
    (f'{{"1password": "{SECRET_VALUE}"}}', '"1password": "***"'),
    (f"2FA_TOKEN={SECRET_VALUE}", "2FA_TOKEN=***"),
]


def test_quoted_and_multiword_secret_keys_are_redacted() -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    leaked = []
    for raw, expected in SECRET_ASSIGNMENT_SHAPES:
        scrubbed = scrub_transcript_text(raw)
        if SECRET_VALUE in scrubbed or expected not in scrubbed:
            leaked.append((raw, scrubbed))
    assert not leaked, f"secret assignment shapes not redacted: {leaked}"


def test_quoted_multiword_secret_keys_survive_realistic_json_blocks() -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    block = json.dumps(
        {
            "AWS_SECRET_ACCESS_KEY": SECRET_VALUE,
            "AWS_SESSION_TOKEN": SECRET_VALUE,
            "MY_TOKEN_FOR_X": SECRET_VALUE,
            "AWS_DEFAULT_REGION": "us-east-1",
        },
        indent=2,
    )

    scrubbed = scrub_transcript_text(block)

    assert SECRET_VALUE not in scrubbed
    # Non-credential context in the same block is preserved.
    assert "us-east-1" in scrubbed
    assert "AWS_DEFAULT_REGION" in scrubbed


def test_long_secret_key_names_are_still_redacted() -> None:
    """Nothing in the key pattern may bound what can MATCH, only what it costs.

    Two distinct ways to get this wrong, both of which shipped in earlier
    revisions of this fix and both of which leak silently:

    - Bounding the CONSUMING quantifier stops the key group mid-identifier for
      a long key name, leaving the separator on a word character.
    - Fronting the arms with a bounded `(?=[A-Za-z0-9_-]{0,64}WORD)` cost gate
      makes a key whose credential word starts past the window unmatchable,
      because the lookbehind forbids restarting inside the identifier. A
      64-character prefix was enough: `MY_AAA...A_PASSWORD=` leaked outright
      while a 63-character prefix redacted.

    So both a long TAIL after the credential word and a long PREFIX before it
    must redact.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
        transcript_secret_classes,
    )

    keys = []
    # Long tail after the credential word.
    for key_length in (64, 65, 80, 200, 1_000):
        keys.append("MY_SECRET_" + "A" * (key_length - 10))
    # Long prefix before the credential word -- straddles the old 64 window.
    for prefix_length in (62, 63, 64, 65, 80, 200, 1_000):
        keys.append("MY_" + "A" * prefix_length + "_PASSWORD")
    keys.append(
        "GITHUB_ACTIONS_SELF_HOSTED_RUNNER_REGISTRATION_ORGANIZATION_LEVEL_TOKEN"
    )

    leaked = []
    for key in keys:
        for raw in (f'"{key}": "{SECRET_VALUE}"', f"{key}={SECRET_VALUE}"):
            if SECRET_VALUE in scrub_transcript_text(raw):
                leaked.append((len(key), raw[:48]))
            if "secret_assignment" not in transcript_secret_classes(raw):
                leaked.append((len(key), f"unclassified: {raw[:48]}"))
    assert not leaked, f"long secret key names leaked: {leaked}"


def test_interior_arm_coverage_ceiling_is_a_ceiling_not_a_regression() -> None:
    """Pin the one bound left in the key pattern as a KNOWN LIMIT.

    The interior-segment arm looks at most 64 characters into a key name for an
    interior credential word, so `MY_<62+ characters>_TOKEN_FOR_X` is not
    redacted. That is a ceiling on how much NEW coverage the arm adds, not a
    regression: the original rule did not redact this shape at any prefix
    length either, because it required the credential word at the END of the
    key name. Alternation is leftmost-first over the whole regex, so this arm
    is reached only after the unbounded suffix and bare-word arms have failed,
    which is why its bound cannot subtract from them.

    Recorded so that if the bound is ever raised or removed, this test fails
    and the limit is revisited deliberately rather than by accident.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    inside = "MY_" + "A" * 40 + "_TOKEN_FOR_X"
    beyond = "MY_" + "A" * 80 + "_TOKEN_FOR_X"

    assert SECRET_VALUE not in scrub_transcript_text(f"{inside}={SECRET_VALUE}")
    # Known limit. The suffix arm does not cover it either -- the key ends in
    # `_X`, not a credential word -- so nothing redacts it, exactly as before.
    assert SECRET_VALUE in scrub_transcript_text(f"{beyond}={SECRET_VALUE}")


def test_quoted_multiword_secret_keys_are_classified_as_secret_assignment() -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        transcript_secret_classes,
    )

    unclassified = [
        raw
        for raw, _ in SECRET_ASSIGNMENT_SHAPES
        if "secret_assignment" not in transcript_secret_classes(raw)
    ]
    assert not unclassified, f"not classified as secret_assignment: {unclassified}"


# Lines sampled from real local agent transcripts that contain a credential
# WORD but are not credential assignments. Treating the word list as a plain
# substring test destroyed all of these; requiring a secret word to be a whole
# `_`/`-` delimited segment is what keeps them intact. Each must survive
# scrubbing byte-for-byte.
BENIGN_TRANSCRIPT_LINES = [
    "total_tokens: 194233",
    "cache_read_input_tokens: 30112",
    "prompt_tokens=17",
    "completion_tokens=204",
    "tokenizer: gpt-4o",
    "keyword: performance",
    "keywords: [graph, substrate]",
    "sort_keys=True",
    "keys: [alpha, beta]",
    "KeyError: 'user_id'",
    "KeyboardInterrupt: user aborted",
    "monkeypatch: setattr",
    "ssh -o StrictHostKeyChecking=no",
    "ssh_key_path=/Users/dev/.ssh/id_ed25519.pub",
    "credentials_file=/Users/dev/.aws/config",
    "scope_key_column: id",
    "row_key_column: ts",
    "key_field: name",
    "detailkeys: 3",
    # Key names whose FIRST segment is a credential word. These are left to the
    # suffix and bare-word arms, exactly as the original rule did. Letting the
    # interior arm match here -- by making its prefix group optional, so a
    # trailing delimiter alone sufficed -- fired on hundreds of distinct key
    # names across real local transcripts.
    "token_budget: 4000",
    "token_delta= +3929949",
    "token_usage: 4",
    "token_count: 512",
    "token_env: OKG_TOKEN",
    "secret_ref: OKG_GITHUB_WEBHOOK_SECRET",
    "secret_handling: secret_ref_required",
    "dsn_env: OKG_DSN",
    "api_key_env: SEMANTIC_SCHOLAR_API_KEY",
    "password_file: /etc/okg/pw",
    "uv run okg deployment ready okg-workspace --json",
    "model=gpt-5",
    "cwd=/Users/dev/projects/okg",
    '{"name": "exec_command", "timeout": 30}',
    "git commit -m 'fix the parser'",
]


def test_ordinary_transcript_text_is_not_over_redacted() -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
        transcript_secret_classes,
    )

    destroyed = [
        (line, scrub_transcript_text(line))
        for line in BENIGN_TRANSCRIPT_LINES
        if scrub_transcript_text(line) != line
    ]
    assert not destroyed, f"ordinary transcript text was redacted: {destroyed}"

    flagged = [
        line
        for line in BENIGN_TRANSCRIPT_LINES
        if transcript_secret_classes(line) != []
    ]
    assert not flagged, f"ordinary transcript text flagged as secret: {flagged}"


def test_redaction_is_a_superset_of_the_previous_rule() -> None:
    """No key shape that used to be redacted may stop being redacted.

    The key pattern is built from three arms, the first two of which reproduce
    the original suffix-anchored and bare-word rules exactly so that widening
    the third can never subtract.

    The hyphen-prefixed CLI flags below were redacted by the original rule and
    were silently dropped by an earlier revision of this fix, which put `-` in
    the key pattern's lookbehind and so left a hyphen-prefixed key with no
    admissible start. Shell command text is this scrubber's primary input, so
    that was a worse leak than the one being fixed, and no pre-existing test
    covered it.

    The digit-prefixed names (`1password`, `0token`, `2FA_TOKEN`) were NOT
    redacted by the original rule -- they are new coverage, kept here so the
    same lookbehind mistake cannot silently remove them either.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
        transcript_secret_classes,
    )

    must_redact_keys = [
        # Covered by the original rule.
        "OPENAI_API_KEY", "api_key", "API-KEY", "TOKEN", "PASSWORD", "SECRET",
        "DSN", "OKG_DSN", "DATABASE_URL", "DB_URL", "GITHUB_TOKEN", "authToken",
        "accessToken", "refresh_token", "x_key", "PRIMARY_KEY", "MONKEY",
        "--password", "--token", "--api-key", "--secret", "-password",
        "svc-account-password", "my-api-key",
        # Hyphen-joined DATABASE_URL/DB_URL. These are the only credential words
        # with no shorter form in the suffix list, so before they were added
        # there they were reachable only through the bare-word arm -- which is
        # anchored at the key start and so needs an admissible start on the `D`,
        # exactly what the lookbehind forbids. They leaked.
        "staging-DB_URL", "prod-DATABASE_URL", "okg-DB_URL", "a-DATABASE_URL",
        # Three or more leading hyphens. `-{0,2}` admitted at most two, and the
        # lookbehind killed every start inside the rest of the run.
        "---password", "----token", "-----api_key", "---AWS_SECRET_ACCESS_KEY",
        # New coverage.
        "1password", "1PASSWORD", "0token", "2FA_TOKEN",
    ]
    leaked = []
    for key in must_redact_keys:
        for raw in (
            f"{key}={SECRET_VALUE}",
            f'"{key}": "{SECRET_VALUE}"',
            f"Bash: mysql {key}={SECRET_VALUE} -e 'select 1'",
        ):
            scrubbed = scrub_transcript_text(raw)
            if SECRET_VALUE in scrubbed:
                leaked.append((key, raw))
            if "secret_assignment" not in transcript_secret_classes(raw):
                leaked.append((key, f"unclassified: {raw}"))
    assert not leaked, f"shapes that used to be redacted now leak: {leaked}"


def test_scrubbing_a_long_unbroken_token_stays_linear() -> None:
    """A long whitespace-free token must not stall the scrubber.

    Several patterns have a greedy run before a required suffix -- the key
    pattern's identifier before `_TOKEN`/`KEY`/..., `_CREDENTIAL_URL_RE`'s
    `[a-z0-9+.-]*` before `://`. Each matches to the end of a long run and then
    backtracks a character at a time. That walk is O(n) and fine to pay ONCE;
    it becomes O(n^2) when every offset is a candidate start.

    Measured before the fixes: 0.8s at 5K characters, 12.8s at 20K and 80.6s at
    50K for a single unbroken token, and 78s at 200K for hyphen-dense text.
    That extrapolates to hours for the megabyte-scale tokens real transcripts
    carry (base64 blobs, minified JSON, hashes). Chunked ingest routes
    transcripts through here that were previously skipped for being oversized,
    so this sits directly on the source-sync path.

    Asserting a shape-independent wall-clock bound rather than a growth ratio.
    The input size is chosen so the two regimes are unambiguous: at 200K
    characters the fixed cost is ~1s at worst and the broken cost is ~78s, so a
    15s bound cannot be straddled by machine load in either direction. (An
    earlier version used 100K/20s, where the broken cost happened to land just
    under the bound -- the test passed with the bug reintroduced.)

    A wall-clock bound at a single size detects a return to quadratic
    behaviour. It does NOT detect a smaller constant-factor regression, which
    is why each shape below names the specific defect it exists to catch.
    """
    import time

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    shapes = {
        "one-long-run": "y" * 200_000,
        "many-boundaries": "aB3-_" * 40_000,
        "hyphen-dense": "a-" * 100_000,
        "underscore-dense": "a_" * 100_000,
        "base64ish": "QUJDRA+/0123abcz" * 12_500,
        "dot-dense": "a." * 100_000,
        # The identifier consume behind a real credential word.
        "long-key-name": "SECRET_" + "A" * 200_000 + "=v",
        # The interior-segment arm. Spelling that arm as two nested unbounded
        # quantifiers made this shape quadratic (50.3s at 100K) while every
        # other shape here stayed linear.
        "repeated-secret-words": "SECRET_" * 28_000,
        # Hyphen-dense text that CONTAINS a credential word. Stays fast only
        # because `-` is in the key pattern's lookbehind; excluding it to let
        # CLI flags match made every character after a hyphen a fresh start,
        # costing tens of seconds at 48K. Plain hyphen-dense text above does
        # NOT detect this, which is why both are here.
        "hyphen-dense-with-credential-word": "a-key-" * 33_000,
        "hyphen-dense-with-interior-word": "x-token-y-" * 20_000,
        # CONSECUTIVE hyphens, which none of the alternating shapes can detect:
        # their runs are length 1, so the leading `-*+` never has anything to
        # give back. That run must be possessive -- backtracking into it is
        # useless, since no arm can start on a `-` -- and a plain `-*` costs
        # 6.35s at 800K and 5.04s on 1MB of markdown horizontal rules.
        "one-long-hyphen-run": "-" * 200_000,
        "markdown-horizontal-rules": ("-" * 80 + "\n") * 2_500,
    }
    for name, text in shapes.items():
        started = time.perf_counter()
        scrub_transcript_text(text)
        elapsed = time.perf_counter() - started
        assert elapsed < 15.0, f"scrub took {elapsed:.1f}s on {name} ({len(text)} chars)"


def test_retention_queue_is_report_only_and_redacted(tmp_path: Path) -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        TranscriptRetentionPolicy,
        build_transcript_retention_queue,
    )

    transcript = tmp_path / "old-session.jsonl"
    transcript.write_text(
        json.dumps({"cmd": "API_KEY=" + CANARY, "private_key": PRIVATE_KEY}),
        encoding="utf-8",
    )
    old_timestamp = datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp()
    os.utime(transcript, (old_timestamp, old_timestamp))

    queue = build_transcript_retention_queue(
        [transcript],
        now=datetime(2026, 7, 3, tzinfo=timezone.utc),
        policy=TranscriptRetentionPolicy(max_age_days=30, max_bytes=1_000_000),
    )

    assert transcript.exists()
    assert len(queue) == 1
    record = queue[0]
    serialized = json.dumps(record, sort_keys=True)
    assert record["retention_state"] == "review_required"
    assert record["cleanup_mode"] == "report_only"
    assert record["delete_without_operator_approval"] is False
    assert record["quarantine_recommended"] is True
    assert "older_than_policy" in record["review_reasons"]
    assert "secret_material_detected" in record["review_reasons"]
    assert "private_key_block" in record["sensitive_classes"]
    assert CANARY not in serialized
    assert str(transcript) not in serialized


def test_retention_queue_redacts_private_key_before_preview_truncation(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        TranscriptRetentionPolicy,
        build_transcript_retention_queue,
        scrub_transcript_text,
    )

    body_line = "M" * 64
    long_private_key = (
        f"{PEM_OPENSSH_BEGIN}\n"
        + "\n".join([body_line] * 20)
        + "\n-----END OPENSSH PRIVATE KEY-----"
    )
    transcript = tmp_path / "old-private-key.jsonl"
    transcript.write_text(long_private_key, encoding="utf-8")
    old_timestamp = datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp()
    os.utime(transcript, (old_timestamp, old_timestamp))

    queue = build_transcript_retention_queue(
        [transcript],
        now=datetime(2026, 7, 3, tzinfo=timezone.utc),
        policy=TranscriptRetentionPolicy(max_age_days=30, max_bytes=1_000_000),
    )

    assert len(queue) == 1
    assert body_line not in queue[0]["redacted_preview"]
    assert body_line not in scrub_transcript_text(
        f"{PEM_OPENSSH_BEGIN}\n" + body_line
    )
    # The unterminated case redacts the key AND keeps what follows it. This
    # test already covered the first half; without the second, the scrubber
    # could satisfy it by discarding the remainder of the transcript, which is
    # exactly what it used to do.
    unterminated = scrub_transcript_text(
        f"{PEM_OPENSSH_BEGIN}\n" + body_line + "\nthe run finished\n"
    )
    assert body_line not in unterminated
    assert "the run finished" in unterminated


def test_unterminated_private_key_header_preserves_following_context() -> None:
    """A PEM header with no END must not cost the rest of the transcript.

    The previous implementation RETURNED at the first unterminated header,
    discarding every following byte. That is silent context loss in graph text,
    not a redaction: the dropped lines were never sensitive.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    body = "\n".join(PEM_BODY)
    cases = {
        "header on its own line": (
            f"line one\n{PEM_OPENSSH_BEGIN}\n"
            + body
            + "\nthe deploy finished at 12:04\nimportant context\n",
            ["line one", "the deploy finished at 12:04", "important context"],
        ),
        "two unterminated headers": (
            f"first\n{PEM_OPENSSH_BEGIN}\n"
            + body
            + f"\nmiddle line here\n{PEM_RSA_BEGIN}\n"
            + body
            + "\nlast line here\n",
            ["first", "middle line here", "last line here"],
        ),
        "terminated then unterminated": (
            f"alpha\n{PEM_RSA_BEGIN}\n"
            + body
            + "\n-----END RSA PRIVATE KEY-----\nbeta gamma\n"
            f"{PEM_OPENSSH_BEGIN}\n"
            + body
            + "\ndelta epsilon\n",
            ["alpha", "beta gamma", "delta epsilon"],
        ),
        "blank line inside the body": (
            f"before it\n{PEM_OPENSSH_BEGIN}\n"
            + body
            + "\n\nthe deploy finished\n",
            ["before it", "the deploy finished"],
        ),
    }

    lost = []
    for label, (text, must_survive) in cases.items():
        scrubbed = scrub_transcript_text(text)
        for fragment in must_survive:
            if fragment not in scrubbed:
                lost.append((label, fragment, scrubbed))
        for line in PEM_BODY:
            if line in scrubbed:
                lost.append((label, f"KEY MATERIAL SURVIVED: {line}", scrubbed))
    assert not lost, f"context lost or key material survived: {lost}"


def test_unterminated_private_key_body_never_survives() -> None:
    """The preservation fix must not weaken the redaction guarantee.

    An unterminated block has no end marker, so the redaction is bounded by the
    shape of the content instead: the remainder of the header's own line, plus
    the contiguous run of lines that could still be base64. A line that cannot
    be base64 key material ends it. These are the shapes where that bound has to
    hold anyway.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
        transcript_secret_classes,
    )

    body = "\n".join(PEM_BODY)
    cases = {
        "multi-line body then prose": (
            f"before\n{PEM_OPENSSH_BEGIN}\n" + body + "\nrun done ok\n"
        ),
        "encrypted PEM encapsulated headers": (
            f"before\n{PEM_RSA_BEGIN}\n"
            "Proc-Type: 4,ENCRYPTED\n"
            "DEK-Info: AES-128-CBC,9F2B1C4A\n"
            "\n" + body + "\nrun finished now\n"
        ),
        # A JSON-escaped transcript puts the whole key on one line, so the
        # payload sits on the header's own line rather than below it.
        "header mid-line": (
            f'prev\n{{"key": "{PEM_RSA_BEGIN}' + PEM_BODY[0] + '"}\n'
        ),
        "body runs to end of input": (
            f"before\n{PEM_OPENSSH_BEGIN}\n" + body
        ),
        "no newline after the header": f"before\n{PEM_OPENSSH_BEGIN}",
        # CRLF. Lines are split on "\n", so a CRLF line keeps a trailing "\r".
        # If the body pattern rejects that, the redaction stops at the header
        # and the WHOLE key survives -- which it did until `\r` was allowed.
        "CRLF line endings": (
            f"before\r\n{PEM_OPENSSH_BEGIN}\r\n"
            + "\r\n".join(PEM_BODY)
            + "\r\nrun finished here\r\n"
        ),
        # A key inside diff output: a hunk removing it prefixes every line with
        # `-`, which is not in the base64 alphabet.
        "inside a removal diff hunk": (
            "diff --git a/id_rsa b/id_rsa\n"
            f"{PEM_OPENSSH_BEGIN}\n"
            + "\n".join("-" + line for line in PEM_BODY)
            + "\nthe patch applied cleanly\n"
        ),
        # A quoted paste.
        "quoted paste": (
            f"someone wrote\n> {PEM_OPENSSH_BEGIN}\n"
            + "\n".join("> " + line for line in PEM_BODY)
            + "\nend of quote here\n"
        ),
        # YAML block scalar: the body is indented.
        "indented YAML block scalar": (
            f"key: |\n  {PEM_OPENSSH_BEGIN}\n"
            + "\n".join("  " + line for line in PEM_BODY)
            + "\n  the run finished\n"
        ),
        # A stray line in the middle of the body must not strand the rest of
        # the key: scanning tolerates a short run of unrecognised lines.
        "stray line inside the body": (
            f"before\n{PEM_OPENSSH_BEGIN}\n"
            + PEM_BODY[0]
            + "\n[2026-07-25 12:04:11] log line interleaved\n"
            + "\n".join(PEM_BODY[1:])
            + "\nrun finished here\n"
        ),
    }

    survived = []
    for label, text in cases.items():
        scrubbed = scrub_transcript_text(text)
        for line in PEM_BODY:
            if line in scrubbed:
                survived.append((label, line))
        if "private_key_block" not in transcript_secret_classes(text):
            survived.append((label, "not classified as private_key_block"))
    assert not survived, f"key material survived scrubbing: {survived}"


def test_key_material_survives_no_distance_from_the_header() -> None:
    """The bound must not be a line count.

    An earlier revision tolerated a fixed number of unrecognised lines before
    giving up. Key material further from the header than that survived in full,
    and the constant was arbitrary -- for any N there was an input at N+1. What
    separates a key line from a stray line is LENGTH, not position: an RFC 7468
    payload line is 64 characters, and prose words are not.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    canary = "KEYMATERIALCANARYAAAABBBBCCCCDDDD"
    survived = []
    for strays in (1, 2, 3, 4, 5, 12, 40):
        text = (
            f"{PEM_RSA_BEGIN}\n"
            + PEM_BODY[0]
            + "\n"
            + "".join(f"x x / y y {i}\n" for i in range(strays))
            + canary
            + "\ntail line here\n"
        )
        if canary in scrub_transcript_text(text):
            survived.append(strays)
    assert not survived, f"key material survived at stray-line distances {survived}"

    # Nor may it be bounded by payload LENGTH. A later revision required a
    # 32-character payload before it would resume past a stray line, which left
    # every remaining line of a narrow-wrapped key in the output.
    for columns in (16, 24, 31, 64):
        body_line = "QUJDRA" + "A" * (columns - 6)
        text = (
            f"{PEM_RSA_BEGIN}\n"
            + "\n".join([body_line] * 4)
            + "\n[2026-07-25 12:04:11] interleaved log line\n"
            + "\n".join([body_line] * 4)
            + "\ntrailing prose line here\n"
        )
        if body_line in scrub_transcript_text(text):
            survived.append(f"{columns}-column wrap")
    assert not survived, f"key material survived: {survived}"


def test_a_mentioned_pem_header_keeps_the_prose_around_it() -> None:
    """A header nobody used costs prose nothing, line by line.

    Redaction after an unterminated header is per-line, so a transcript that
    merely quotes or discusses PEM format keeps every line of prose. What it
    does lose is bounded and specific: the remainder of the header's own line,
    because a JSON-escaped transcript carries the whole payload exactly there,
    and any isolated line that is nothing but base64 characters, because that is
    indistinguishable from a narrow-wrapped payload line. Both are single
    tokens, never a span.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    text = (
        f"the file starts with {PEM_OPENSSH_BEGIN} as you can see\n"
        "and then the base64 payload follows on the next lines\n"
        "done\n"
        "see the runbook for the full format\n"
    )
    scrubbed = scrub_transcript_text(text)
    # Text before the marker on its own line is context and survives.
    assert f"the file starts with {PEM_OPENSSH_BEGIN}" in scrubbed
    assert "and then the base64 payload follows on the next lines" in scrubbed
    assert "see the runbook for the full format" in scrubbed
    # A bare base64-shaped word is redacted; the prose around it is not.
    assert "\ndone\n" not in scrubbed

    on_its_own_line = (
        f"{PEM_OPENSSH_BEGIN}\n"
        "that header is what a PEM file opens with\n"
        "and nothing here is key material\n"
        "\n"
        "a blank line above must not drag the redaction down to here\n"
    )
    scrubbed = scrub_transcript_text(on_its_own_line)
    assert "that header is what a PEM file opens with" in scrubbed
    assert "and nothing here is key material" in scrubbed
    assert "a blank line above must not drag the redaction down to here" in scrubbed

    # Asserted as EXACT output, not with `in`. A blank line must not count as
    # key material: when it did, a span-based rule was dragged to the last
    # blank line and ate paragraph-formatted prose. Substring assertions
    # survive that mutation, so they cannot pin this on their own.
    paragraphs = (
        f"{PEM_OPENSSH_BEGIN}\n"
        "\n"
        "This is an ordinary paragraph.\n"
        "\n"
        "And a second paragraph.\n"
    )
    assert scrub_transcript_text(paragraphs) == (
        f"{PEM_OPENSSH_BEGIN}\n"
        "***\n"
        "\n"
        "This is an ordinary paragraph.\n"
        "\n"
        "And a second paragraph.\n"
    )


def test_key_material_before_a_second_header_on_one_line_is_redacted() -> None:
    """A payload running straight into the next header must not escape.

    The text preceding a BEGIN marker on its own line is normally context and
    is emitted verbatim. While already redacting, though, that position is
    exactly where a payload with no trailing newline ends up, and emitting it
    unconditionally leaked the key.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    payload = "C3J27XDCG2LmlZGEONYlgCtjfIZ4SOcMz9CPVNPkNa1H"
    scrubbed = scrub_transcript_text(
        f"{PEM_RSA_BEGIN}\n"
        + payload
        + f"{PEM_EC_BEGIN}\nx\n"
    )

    assert payload not in scrubbed
    # Ordinary prose in that same position is still context and survives.
    kept = scrub_transcript_text(
        f"some notes here {PEM_RSA_BEGIN}\n" + PEM_BODY[0] + "\n"
    )
    assert "some notes here " in kept


def test_decorated_key_bodies_are_redacted() -> None:
    """A key that arrives decorated must still be redacted.

    Keys reach transcripts through tools that prefix every line: `cat -n` and
    this repo's own file reader emit `number TAB line`, `rg -n` emits
    `path:number:line`, a JSON dump emits `"line",`, a coloured terminal emits
    ANSI escapes. A classifier that matched whole lines failed on all of them
    and left such keys completely unredacted. Matching a base64 RUN inside the
    line is what makes the decoration irrelevant.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    decorations = {
        "cat -n / file reader": lambda i, line: f"{i:6d}\t{line}",
        "nl": lambda i, line: f"{i:6d}  {line}",
        "rg -n": lambda i, line: f"keys/id_rsa:{i}:{line}",
        "json array": lambda i, line: f'    "{line}",',
        "ansi colour": lambda i, line: f"\x1b[32m{line}\x1b[0m",
    }

    survived = []
    for label, decorate in decorations.items():
        text = (
            decorate(1, f"{PEM_OPENSSH_BEGIN}")
            + "\n"
            + "\n".join(decorate(i + 2, line) for i, line in enumerate(PEM_BODY))
            + "\n"
        )
        scrubbed = scrub_transcript_text(text)
        survived.extend(
            (label, line) for line in PEM_BODY if line in scrubbed
        )
    assert not survived, f"decorated key material survived: {survived}"


def test_prose_after_an_unterminated_key_is_kept_unless_key_shaped() -> None:
    """Pin both halves of the fail-closed trade, including what it costs.

    Ordinary prose after an unterminated key body survives -- that is the
    reported defect, fixed. But once key material is present its extent is
    unknowable, so the redaction runs to the LAST body-shaped line in the
    remainder. A tail whose lines are single base64-shaped words is therefore
    redacted along with the key.

    That cost is deliberate and is the reason it is asserted here rather than
    left implicit. The alternatives were tried and both leaked: tolerating N
    unrecognised lines leaked any key N+1 lines out, and requiring a
    32-character payload to resume leaked every remaining line of a key wrapped
    at 16 columns.
    """

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    # Multi-word prose is not key-shaped, so it survives.
    kept = scrub_transcript_text(
        f"{PEM_RSA_BEGIN}\n"
        + PEM_BODY[0]
        + "\nthe deploy failed at 12:04\nsee the log for details\n"
    )
    assert PEM_BODY[0] not in kept
    assert "the deploy failed at 12:04" in kept
    assert "see the log for details" in kept

    # A tail of single base64-shaped words is indistinguishable from a
    # narrow-wrapped key body, so it is redacted with the key.
    eaten = scrub_transcript_text(
        f"{PEM_RSA_BEGIN}\n"
        + PEM_BODY[0]
        + "\ndone\nOK\nfinished\nTRAILING PROSE HERE\n"
    )
    assert PEM_BODY[0] not in eaten
    assert "done" not in eaten
    assert "TRAILING PROSE HERE" in eaten


# A single scan call that runs longer than this is treated as a hang. It is a
# wall-clock alarm, so it interrupts a runaway regex or loop instead of waiting
# for it; the fixed scans below take well under a second each.
_SCAN_HANG_GUARD_SECONDS = 60.0

# The growth check times each shape at n and at 4n. A linear scan costs about
# 4 times as much at 4n (exponent 1.0); every quadratic this test names costs
# about 16 times as much (exponent near 2.0; see the test docstring and the
# PACT timing-tests-robust mutation run). The bound, 4**1.4 (about 7), is the
# old 2x step's exponent: it still fails n^1.5 extra work and a quadratic
# diluted by linear work (both measured 1.44 to 1.48 here), and it accepts a
# linear scan whose large call was measured up to 1.74 times slower than its
# small one by noise, where the old 2x step accepted only 1.32 times.
_GROWTH_FACTOR = 4
_MAX_GROWTH_EXPONENT = 1.4
# At most this many interleaved (n, 4n) rounds. The check stops at the first
# round where the best times so far are within the bound, so a quiet runner
# pays for one round; a quadratic is outside the bound in every round.
_MAX_GROWTH_ROUNDS = 5

_TERMINATED_UNIT = (
    f"{PEM_RSA_BEGIN}\n"
    + PEM_BODY[0]
    + "\n-----END RSA PRIVATE KEY-----\nsome ordinary prose line here\n"
)
_ONE_LINE_UNIT = _TERMINATED_UNIT.replace("\n", "")
_UNTERMINATED_UNIT = f"{PEM_OPENSSH_BEGIN}\n" + PEM_BODY[0] + "\n"


def _repeat_to(unit: str, size: int) -> str:
    return unit * (size // len(unit))


def _timed_call_seconds(fn, text: str) -> float:
    """CPU seconds of this thread for one call, failing the test if it hangs.

    Thread CPU time, not wall time: a runner that is descheduled or
    throttled does not charge that wait to the call.
    """
    import gc
    import signal
    import time

    def _hang(signum, frame):  # noqa: ARG001 - signal handler signature
        raise TimeoutError(
            f"{fn.__name__} ran longer than {_SCAN_HANG_GUARD_SECONDS:.0f}s "
            f"on {len(text)} chars"
        )

    gc.collect()
    previous = signal.signal(signal.SIGALRM, _hang)
    signal.setitimer(signal.ITIMER_REAL, _SCAN_HANG_GUARD_SECONDS)
    try:
        started = time.thread_time()
        fn(text)
        return time.thread_time() - started
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _growth_exponent(fn, make_text, size: int) -> tuple[float, float, float, int]:
    """Best CPU time at `size` and at `_GROWTH_FACTOR * size`, their growth
    exponent log(t(4n) / t(n)) / log(4), and the rounds it took.

    The two sizes alternate within each round, so a busy spell on a shared
    runner lands on both sizes rather than on all the large calls at once.
    The 2x version timed three small calls, then three large ones, and its CI
    failures look like that: on #2945 two shapes run back to back both failed
    together. Each size keeps its best (lowest) time across rounds.
    """
    import math

    small, large = make_text(size), make_text(_GROWTH_FACTOR * size)
    t_small = t_large = math.inf
    for rounds in range(1, _MAX_GROWTH_ROUNDS + 1):
        t_small = min(t_small, _timed_call_seconds(fn, small))
        t_large = min(t_large, _timed_call_seconds(fn, large))
        exponent = math.log(t_large / t_small, _GROWTH_FACTOR)
        if exponent <= _MAX_GROWTH_EXPONENT:
            break
    return t_small, t_large, exponent, rounds


# Shape name -> (function under test, input builder, input size n). Each n is
# chosen so one call on the fixed code takes at least ~20 ms on the operator
# laptop, so timer resolution and scheduler noise stay small next to the
# measured cost; slower CI runners only make the calls longer.
_PEM_SCALING_SHAPES = {
    # TERMINATED blocks of a single kind. Pairing each header with its END by
    # scanning that kind's END offsets from the front of the list is O(n^2)
    # in the number of blocks; an earlier revision shipped exactly that. The
    # unterminated shape cannot detect it: with no END markers the list is
    # always empty. ~30 ms at n on the laptop.
    "terminated-blocks": (
        "_replace_private_key_blocks",
        lambda size: _repeat_to(_TERMINATED_UNIT, size),
        3_200_000,
    ),
    # The same blocks with NO newlines between them -- the shape a transcript
    # takes when keys are JSON-escaped onto single lines. Re-finding the
    # line's newline per block is O(blocks x line length). ~23 ms at n.
    "one-line-blocks": (
        "_replace_private_key_blocks",
        lambda size: _repeat_to(_ONE_LINE_UNIT, size),
        3_200_000,
    ),
    # Many headers with no END. The pre-fix `BEGIN.*?END` DOTALL regex walked
    # to end of input at every header. ~49 ms at n.
    "unterminated-headers": (
        "_replace_private_key_blocks",
        lambda size: _repeat_to(_UNTERMINATED_UNIT, size),
        3_200_000,
    ),
    # A long whitespace run under an unterminated header. A body-line regex
    # built from several optional whitespace runs backtracks cubically here
    # (32.7 s at 3,200 spaces), so this shape trips the hang guard rather than
    # the exponent. ~39 ms at n.
    "long-space-body-line": (
        "_replace_private_key_blocks",
        lambda size: f"{PEM_RSA_BEGIN}\n" + " " * size + "!\nrest\n",
        3_200_000,
    ),
    # Classification carried the same unterminated-header quadratic, because it
    # searched the block pattern before the header pattern. ~65 ms at n.
    "classify-unterminated-headers": (
        "transcript_secret_classes",
        lambda size: _repeat_to(_UNTERMINATED_UNIT, size),
        200_000,
    ),
}


@pytest.mark.parametrize("shape", sorted(_PEM_SCALING_SHAPES))
def test_scrubbing_many_unterminated_private_key_headers_stays_linear(shape: str) -> None:
    """Guard the PEM scan against the quadratics it replaced.

    Checks growth, not an absolute budget. An absolute CPU budget failed a
    correct implementation on a slow CI runner (issue #2717: 5.2 s against a
    5.0 s budget, where the laptop measures about 1.4 s). Quadrupling the
    input multiplies a linear scan's time by about 4 on any machine, and a
    quadratic one's by about 16, so the exponent log4(t(4n) / t(n))
    separates them independent of runner speed. Each call is timed alone (the
    PEM scan or the classifier, not the whole scrub pipeline) so the other
    linear passes cannot dilute a quadratic.

    A 2x step with best-of-three per size (issue #2717's version) still
    failed correct code on loaded CI pods: n^1.94, n^1.75, n^2.02 and n^1.62,
    the large calls measured 3 to 4 times slower than the small ones where
    linear predicts 2. The 4x step, the interleaved rounds in `_growth_exponent`, and
    thread CPU time are the answer (PACT timing-tests-robust).

    Every named quadratic, reintroduced as a mutant, fails this test; the
    PACTs for #2717 and timing-tests-robust carry the mutation runs. A cubic
    or worse regression trips the per-call hang guard instead.

    Chunking a transcript that contains a real PEM block is precisely what
    separates BEGIN from END, so these inputs are reachable through normal
    ingest.
    """

    from okg.substrate.library.sources import _transcript_redaction

    fn_name, make_text, size = _PEM_SCALING_SHAPES[shape]
    fn = getattr(_transcript_redaction, fn_name)
    if shape.startswith("classify"):
        assert "private_key_block" in fn(make_text(size))
    t_small, t_large, exponent, rounds = _growth_exponent(fn, make_text, size)
    assert exponent <= _MAX_GROWTH_EXPONENT, (
        f"{fn_name} on {shape} grows as n^{exponent:.2f} "
        f"(best of {rounds} rounds: {t_small * 1000:.1f} ms CPU at {size} chars, "
        f"{t_large * 1000:.1f} ms at {_GROWTH_FACTOR * size}); linear is n^1"
    )


def test_bounding_the_secret_regexes_did_not_narrow_redaction() -> None:
    """The quantifier bounds narrow COST, not what is redacted."""
    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    for text, secret in (
        ('OKG_DSN: "postgres://user:pw@host/db"', "pw@host"),
        ("api_key = 'sk-abcdefgh12345678'", "sk-abcdefgh12345678"),
        ('{"password": "hunter2-hunter2"}', "hunter2-hunter2"),
        ('MY_SECRET="s3cr3t-value-here"', "s3cr3t-value-here"),
        ("{'GITHUB_TOKEN': 'ghp_xxxxxxxxxxxx'}", "ghp_xxxxxxxxxxxx"),
        ("export DATABASE_URL=postgres://u:pw@h/db", "pw@h"),
    ):
        assert secret not in scrub_transcript_text(text), text

    # The 64-character key bound is comfortably past any real key name, so a
    # realistically-long one is still redacted.
    long_key = "MY_VERY_LONG_SERVICE_SPECIFIC_CREDENTIAL_ACCESS_TOKEN"
    assert len(long_key) < 64
    assert "swordfish" not in scrub_transcript_text(f'{long_key}="swordfish"')
