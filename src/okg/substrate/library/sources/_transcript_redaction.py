"""Credential scrubbing for transcript-derived graph text."""

from __future__ import annotations

import bisect
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

_PLACEHOLDER = "***"

# A key name ENDING in one of these is credential-shaped. This is the original
# rule and it is reproduced exactly: it is what covers `--password=`,
# `OPENAI_API_KEY=`, `1password`, and every other shape already relied on.
#
# This replaces the `{0,64}` bound that previously sat on the consuming prefix.
# That bound was added for COST and documented as one that "does not change
# which key names match" -- but it does. The rule requires the credential word
# at the END of the key, so bounding the prefix makes a longer key name
# unmatchable rather than merely cheaper, and `MY_<65 characters>_PASSWORD=`
# leaked outright. Cost is handled by the lookbehind on _SECRET_KEY_PATTERN
# below, which bounds the number of candidate STARTS instead of bounding what
# is allowed to match.
#
# DATABASE_URL and DB_URL are here as well as in _BARE_SECRET_WORD_PATTERN.
# They are the only credential words with no shorter form in this list (`_URL`
# is far too generic to be a suffix on its own), so without them a hyphen-joined
# key like `staging-DB_URL=` can only be reached through the bare-word arm --
# which is anchored at the start of the key and so needs an admissible start on
# the `D`, exactly what the lookbehind forbids. That leaked.
_SECRET_SUFFIX_PATTERN = (
    r"(?:_TOKEN|_PASSWORD|_SECRET|_DSN|_API_KEY|_KEY"
    r"|DATABASE_URL|DB_URL"
    r"|TOKEN|PASSWORD|SECRET|DSN|API[-_]?KEY|KEY)"
)

# A key name that IS one of these, with no surrounding identifier.
_BARE_SECRET_WORD_PATTERN = (
    r"(?:TOKEN|PASSWORD|SECRET|DSN|API_KEY|API-KEY|DATABASE_URL|DB_URL|OKG_DSN)"
)

# Words strong enough to redact on when they appear as an INTERIOR segment of a
# key name (`MY_TOKEN_FOR_X`) -- delimited by `_` or `-` on BOTH sides, which
# the arm below enforces with a mandatory prefix group.
#
# The prefix must be mandatory. Making it optional lets the word match in
# LEADING position on a trailing delimiter alone, which fires on a large class
# of ordinary transcript text -- `token_budget`, `secret_ref`, `token_env`,
# `dsn_env`, `secret_handling`, `token_usage`, `api_key_env`, `password_file`
# and hundreds of similar key names found across real local transcripts. A key
# name whose FIRST segment is a credential word is left to the suffix and
# bare-word arms, which is what the original rule did.
#
# Bare KEY is deliberately absent. Interior KEY matching reads as credential-
# shaped but is overwhelmingly ordinary transcript text -- `ssh_key_path`,
# `scope_key_column`, `key_field`, `sort_keys`. Requiring separators on both
# sides is what keeps `total_tokens`, `keyword`, `KeyError`, and `monkeypatch`
# intact: none of them has a secret word as a whole interior segment.
_INTERIOR_SECRET_WORD_PATTERN = (
    r"(?:API[-_]?KEY|PASSPHRASE|PASSWORD|SECRET|TOKEN|DSN)"
)

# A secret key NAME.
#
# This pattern is interpolated into larger regexes between optional quotes, so
# it MUST be a single self-contained group. It previously carried a top-level
# unparenthesized `|`, which bound across the ENCLOSING group rather than
# within the pattern: `(?P<key>['"]? A|B ['"]?)` gave branch A only the leading
# quote and branch B only the trailing one. A key reaching branch A could not
# consume its closing quote, so the separator landed on `"` and
# `"AWS_SECRET_ACCESS_KEY": "..."` was never redacted. Wrapping the whole thing
# in one group is the fix for that leak.
#
# The three arms are ordered so the first two reproduce the original rule
# exactly and the third only ADDS the interior-segment case. Redaction is
# therefore a superset of the original: no shape that used to be redacted stops
# being redacted.
#
# COST is controlled by the LOOKBEHIND, not by any bound on what can match.
#
# The original shape was a greedy `[A-Za-z0-9_-]*` followed by a suffix
# alternation: on a long run of word characters it matches to the end and then
# backtracks one character at a time hunting for `_TOKEN`/`KEY`/... That walk
# is O(n), and it is fine to pay it ONCE. The original paid it at every offset,
# because the quoted regex anchored nothing, which is what made it O(n^2):
# ~80s to scrub a single 50K-character token and 328s on 100K characters of
# hyphen-dense text. Chunked ingest routes transcripts through here that were
# previously skipped for being oversized, so this sits directly on the
# source-sync path.
#
# `(?<![A-Za-z0-9_-])` admits at most one start per identifier run, which makes
# the total O(n). Note `-` IS in the lookbehind class: excluding it to let CLI
# flags match would make every character after a hyphen a fresh start, and a
# hyphen-dense run containing a credential word would go quadratic again
# (`a-key-` repeated measured 5.3s at 16K characters, 20.8s at 32K).
#
# Nothing here bounds what can MATCH, deliberately. An earlier revision fronted
# the arms with a bounded `(?=[A-Za-z0-9_-]{0,64}WORD)` cost gate; because the
# lookbehind forbids restarting inside an identifier, that gate silently made a
# key whose credential word begins past offset 63 unmatchable --
# `MY_..._PASSWORD=` with a 64-character prefix leaked outright. A bound on
# admissibility is a LEAK, not a saving. The same argument rules out bounding
# the consuming parts, which would stop the key group mid-identifier and leave
# the separator on a word character.
#
# CLI flags are handled by letting the key ABSORB its leading hyphens rather
# than by weakening the lookbehind. `--password=` is the commonest way a
# credential appears in shell command text, which is this scrubber's primary
# input, and an earlier revision dropped every such form.
_SECRET_KEY_PATTERN = "".join((
    # `-*+`, not `-{0,2}`: any bound on the leading hyphen run is an
    # admissibility bound, because the lookbehind forbids restarting inside the
    # run. With `-{0,2}`, `mysql ---password=` leaked while `--password=`
    # redacted.
    #
    # POSSESSIVE, because backtracking into the run is provably useless: every
    # arm below begins with `[A-Za-z0-9_]` or a literal word character, so
    # giving a hyphen back can only leave the next arm starting on a `-`, which
    # none of them can match. Plain `-*` gives the hyphens back one at a time
    # and pays arm 3's lookahead at each position, which is linear but with a
    # brutal constant: 6.35s to scrub 800K consecutive hyphens, and 5.04s on a
    # 1MB file of markdown horizontal rules. Possessive is the same language at
    # a fraction of the cost.
    r"(?:(?<![A-Za-z0-9_-])-*+",
    r"(?:",
    # Arm 1: an identifier ending in a secret word (the original rule).
    r"[A-Za-z0-9_][A-Za-z0-9_-]*", _SECRET_SUFFIX_PATTERN,
    # Arm 2: a bare secret word (the original rule).
    r"|", _BARE_SECRET_WORD_PATTERN,
    # Arm 3 (new): a secret word as an interior `_`/`-` delimited segment.
    #
    # Written as a lookahead plus one greedy consume, never as two nested
    # unbounded quantifiers. The natural spelling
    # `(?:[A-Za-z0-9_-]*[_-])?WORD[_-][A-Za-z0-9_-]*` is quadratic at a single
    # start: the prefix backtracks to every `_`, and each one then consumes to
    # the end again. On `SECRET_` repeated that measured 2.6s at 25K characters
    # and 50.3s at 100K.
    #
    # The prefix group is MANDATORY, so the credential word needs a delimiter on
    # both sides and cannot match in leading position. See
    # _INTERIOR_SECRET_WORD_PATTERN for what that costs when it is optional.
    #
    # The `{0,64}` here bounds only how far into a key name this NEW arm looks
    # for an interior credential word. Unlike the removed cost gate it cannot
    # cause a leak, because alternation is leftmost-first over the whole regex:
    # this arm is reached only when arms 1 and 2 have already failed the overall
    # match, so it can only ADD coverage, never remove it.
    r"|(?=[A-Za-z0-9_-]{0,64}[_-]", _INTERIOR_SECRET_WORD_PATTERN, r"[_-])",
    r"[A-Za-z0-9_][A-Za-z0-9_-]*",
    r"))",
))
_CREDENTIAL_URL_RE = re.compile(
    # `{0,32}` is for COST: unbounded, this walks to the end of a long
    # `[a-z0-9+.-]` run at every word boundary hunting for `://`, which is
    # quadratic on hyphen- or dot-dense text (`a-a-a-...` took 78s at 200K
    # characters). A URL scheme is short -- `postgres` is 8, the longest
    # IANA-registered scheme is under 32.
    #
    # Unlike the bounds removed from the key pattern, this one is a genuine
    # narrowing: a scheme of 34 or more characters no longer redacts. No
    # registered scheme approaches that length, so it is disclosed rather than
    # worked around.
    r"(?P<scheme>\b[a-z][a-z0-9+.-]{0,32}://)"
    r"(?P<user>[^/\s:@]+):"
    r"(?P<password>[^@\s/]+)@",
    re.IGNORECASE,
)
_AUTHORIZATION_RE = re.compile(
    r"(?i)(\bAuthorization\s*[:=]\s*)"
    r"(?:(Bearer|Basic|Token)\s+)?"
    r"([^\s,;'\"}]+)"
)
_BEARER_RE = re.compile(r"(?i)(\bBearer\s+)([A-Za-z0-9._~+/=-]{8,})")
_SECRET_ASSIGNMENT_RE = re.compile(
    # No leading `\b`: the key pattern now absorbs a CLI flag's leading hyphens,
    # and there is no word boundary between a space and a `-`, so a `\b` here
    # would reject `--password=`. _SECRET_KEY_PATTERN's own lookbehind is the
    # stronger anchor and is what bounds cost.
    r"(?i)(?P<key>"
    + _SECRET_KEY_PATTERN
    + r")(?P<sep>\s*[:=]\s*)(?P<quote>['\"]?)(?P<value>[^,\s}\]`'\"<>]+)"
)
_QUOTED_SECRET_ASSIGNMENT_RE = re.compile(
    # No `\b` after the optional quote. It was previously added here as a cost
    # anchor -- correctly, since without one every offset was a candidate start
    # and the O(n) backtracking walk ran n times -- but it also narrows what is
    # redacted, which the comment it carried did not account for. There is no
    # word boundary between `"` and `-`, so `"--password": "..."` stopped
    # matching entirely.
    #
    # _SECRET_KEY_PATTERN's own lookbehind supersedes it: it admits at most one
    # start per identifier run, which is the same cost property, and it lets the
    # key absorb its leading hyphens rather than rejecting them.
    r"(?is)(?P<key>['\"]?"
    + _SECRET_KEY_PATTERN
    + r"['\"]?)(?P<sep>\s*[:=]\s*)(?P<quote>['\"])(?P<value>.*?)(?P=quote)"
)
_TOKEN_PATTERNS = (
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{8,}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{8,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{8,}\b"),
    re.compile(r"\bxox[A-Za-z0-9]*-[A-Za-z0-9-]{6,}\b"),
)
_JWT_RE = re.compile(
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"
)
_BASE64_BLOB_RE = re.compile(
    r"(?<![A-Za-z0-9+/])(?:[A-Za-z0-9+/]{80,}={0,2})(?![A-Za-z0-9+/])"
)
_PRIVATE_KEY_HEADER_RE = re.compile(
    r"-----BEGIN (?P<kind>[A-Z0-9 ]*PRIVATE KEY)-----"
)

_PRIVATE_KEY_END_RE = re.compile(r"-----END (?P<kind>[A-Z0-9 ]*PRIVATE KEY)-----")

# A run of base64 characters anywhere in a line.
_PEM_PAYLOAD_RUN_RE = re.compile(r"[A-Za-z0-9+/]+={0,2}")
_PEM_ENCAPSULATED_HEADER_RE = re.compile(r"(?:Proc-Type|DEK-Info|Version)[ \t]*:")

# How long a base64 run must be before a line counts as carrying key material.
#
# This is the one constant left, and it is a property of the ENCODING rather
# than of layout, which is why it survives where the earlier bounds did not.
# RFC 7468 wraps payloads at 64 characters; no tool wraps a PEM body below 40.
# Ordinary prose has no 16-character base64 run -- the longest word in this
# module's own comments is 13 -- so 16 sits with wide margin on both sides.
#
# Earlier revisions bounded the redaction by LAYOUT instead, and every one
# leaked. Tolerating N unrecognised lines left any key N+1 lines out in the
# output. Requiring a 32-character line to resume left every line of a key
# wrapped at 16 or 24 columns. Classifying whole lines left every key that
# arrives decorated -- `cat -n`, `nl`, `rg -n`, a JSON array of lines, ANSI
# colour -- entirely unredacted, because the decoration made no line match.
# Scanning for a RUN inside the line is immune to all of that: the decoration
# simply is not part of the run.
_PEM_MIN_PAYLOAD_RUN = 16


def _longest_payload_run(line: str) -> int:
    """Length of the longest base64 run in a line, ignoring anything around it.

    Scanning for a run rather than matching the whole line is what makes this
    immune to per-line decoration. A key arrives decorated more often than not
    -- `cat -n` and this repo's own file reader emit `number TAB line`, `rg -n`
    emits `path:number:line`, a JSON dump emits `"line",`, a coloured terminal
    emits ANSI escapes -- and a whole-line match fails on every one of them,
    which previously left such keys completely unredacted.

    Uses one regex with a single quantifier. An earlier form combined several
    optional whitespace runs around an optional payload, whose overlapping
    stars backtracked cubically: one line of N spaces cost 0.07s at N=400 and
    32.7s at N=3200, a worse complexity class than the quadratic this change
    exists to remove.
    """

    longest = 0
    for match in _PEM_PAYLOAD_RUN_RE.finditer(line):
        longest = max(longest, match.end() - match.start())
    return longest


def _carries_key_material(line: str) -> bool:
    """Whether a line following an unterminated header holds key material.

    A line qualifies when it carries a base64 run at least
    `_PEM_MIN_PAYLOAD_RUN` long, or when it is entirely base64 at any length,
    or when it is an RFC 1421 encapsulated header.

    The any-length clause is not conditioned on the previous line having
    qualified. Requiring that left a block's FINAL payload line -- which is
    whatever is left over, often only a few characters -- in the output whenever
    a stray line preceded it. The cost is that an isolated short base64-shaped
    word after an unterminated header (`done`, `OK`) is redacted too; that is one
    token, not a span, and it is the safe direction.

    A blank line is deliberately NOT key material. Treating it as such let a
    single blank line downstream drag a span-based redaction to itself, which
    ate ordinary paragraph-formatted prose.
    """

    if _longest_payload_run(line) >= _PEM_MIN_PAYLOAD_RUN:
        return True
    stripped = line.strip(" \t\r")
    if not stripped:
        return False
    if _PEM_ENCAPSULATED_HEADER_RE.match(stripped):
        return True
    if stripped[0] in "-+>":
        stripped = stripped[1:].strip(" \t")
    return bool(stripped) and _PEM_PAYLOAD_RUN_RE.fullmatch(stripped) is not None


@dataclass(frozen=True)
class TranscriptRetentionPolicy:
    """Report-only local transcript retention thresholds."""

    max_age_days: int = 30
    max_bytes: int = 5_000_000


def scrub_transcript_text(text: str) -> str:
    """Replace credential-shaped substrings in transcript text with ``***``.

    This scrubber is intentionally narrow to transcript storage: it preserves
    useful command/message context while removing credential values before the
    source computes content hashes or emits graph attrs.
    """

    if not text:
        return text

    out = _replace_private_key_blocks(text)
    out = _QUOTED_SECRET_ASSIGNMENT_RE.sub(_replace_assignment, out)
    out = _SECRET_ASSIGNMENT_RE.sub(_replace_assignment, out)
    out = _AUTHORIZATION_RE.sub(_replace_authorization, out)
    out = _BEARER_RE.sub(r"\1" + _PLACEHOLDER, out)
    out = _CREDENTIAL_URL_RE.sub(
        lambda match: (
            f"{match.group('scheme')}{match.group('user')}:"
            f"{_PLACEHOLDER}@"
        ),
        out,
    )
    for pattern in _TOKEN_PATTERNS:
        out = pattern.sub(_PLACEHOLDER, out)
    out = _JWT_RE.sub(_PLACEHOLDER, out)
    return _BASE64_BLOB_RE.sub(_PLACEHOLDER, out)


def transcript_secret_classes(text: str) -> list[str]:
    """Classify secret-shaped transcript material without returning values."""

    if not text:
        return []
    classes = set()
    # Header search only. The previous `block.search(...) or header.search(...)`
    # was redundant -- every block contains a header, so the disjunction was
    # already equivalent to the header term -- and the block regex carried the
    # same quadratic blowup here that it did in scrubbing, since a text with
    # many unterminated headers made it rescan to end of input at each one.
    if _PRIVATE_KEY_HEADER_RE.search(text):
        classes.add("private_key_block")
    if _QUOTED_SECRET_ASSIGNMENT_RE.search(text) or _SECRET_ASSIGNMENT_RE.search(text):
        classes.add("secret_assignment")
    if _AUTHORIZATION_RE.search(text) or _BEARER_RE.search(text):
        classes.add("authorization_header")
    if _CREDENTIAL_URL_RE.search(text):
        classes.add("credential_url")
    if _JWT_RE.search(text):
        classes.add("jwt")
    if _BASE64_BLOB_RE.search(text):
        classes.add("base64_blob")
    for pattern in _TOKEN_PATTERNS:
        if pattern.search(text):
            classes.add("token_value")
            break
    return sorted(classes)


def transcript_contains_secret_material(text: str) -> bool:
    return bool(transcript_secret_classes(text))


def build_transcript_retention_queue(
    paths: Iterable[Path | str],
    *,
    now: datetime | None = None,
    policy: TranscriptRetentionPolicy | None = None,
) -> list[dict[str, Any]]:
    """Return report-only review records for old or sensitive transcripts.

    The function never deletes or rewrites transcripts. It reads local files,
    classifies risk, and returns metadata safe to persist in diagnostics,
    quarantine queues, or graph evidence.
    """

    effective_policy = policy or TranscriptRetentionPolicy()
    effective_now = now or datetime.now(timezone.utc)
    queue: list[dict[str, Any]] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        try:
            stat = path.stat()
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        age_days = max(0, int((effective_now - mtime).total_seconds() // 86400))
        sensitive_classes = transcript_secret_classes(text)
        too_old = age_days >= effective_policy.max_age_days
        too_large = stat.st_size >= effective_policy.max_bytes
        if not (too_old or too_large or sensitive_classes):
            continue
        queue.append({
            "path_ref": f"transcript:{_sha(str(path.resolve()))}",
            "path_suffix": path.suffix,
            "age_days": age_days,
            "size_bytes": stat.st_size,
            "retention_state": "review_required",
            "cleanup_mode": "report_only",
            "delete_without_operator_approval": False,
            "quarantine_recommended": bool(sensitive_classes),
            "sensitive_classes": sensitive_classes,
            "review_reasons": _review_reasons(
                too_old=too_old,
                too_large=too_large,
                sensitive_classes=sensitive_classes,
            ),
            "redacted_preview": scrub_transcript_text(text)[:500],
        })
    return queue


def _replace_private_key_blocks(text: str) -> str:
    """Replace PEM key material with the placeholder, keeping everything else.

    One forward pass over lines. A BEGIN header whose matching END exists is
    replaced wholesale. A header with no END switches the scan into a mode where
    each following line is redacted individually if it carries key material, and
    passed through untouched otherwise.

    Redacting LINE BY LINE rather than as a span is the point. Every previous
    revision tried to find where an unterminated key ends and bound a single
    span to it, and each bound leaked in a different way -- a line-count
    tolerance left any key one line further out, a payload-length threshold left
    narrow-wrapped keys, and a whole-line classifier left every key that arrived
    decorated by `cat -n` or `rg -n`. Per-line redaction needs no such bound: a
    line either carries key material or it does not, and lines that do not are
    kept, however many of them sit between lines that do.

    Cost is linear. The previous implementation ran a `BEGIN.*?END` regex under
    DOTALL whose lazy body expands to end of input before failing, at every
    BEGIN -- quadratic on repeated unterminated headers, 0.24s at 50K characters
    and 15.2s at 400K. END markers are now indexed in one pass instead, so a
    missing END costs nothing per header; searching per distinct key kind was
    itself quadratic in the number of kinds (12.2s at 3.2M characters).
    """

    if "-----BEGIN " not in text:
        return text

    ends_by_kind: dict[str, list[int]] = {}
    for end_match in _PRIVATE_KEY_END_RE.finditer(text):
        ends_by_kind.setdefault(end_match.group("kind"), []).append(end_match.start())

    pieces: list[str] = []
    pos = 0
    length = len(text)
    redacting = False
    placeholder_open = False

    # Position of the newline ending the current line, or `length` if the line
    # runs to the end of the input. Cached rather than recomputed per iteration:
    # a terminated block leaves `pos` mid-line, so k blocks on one line would
    # each rescan that whole line for its newline AND re-slice it, making the
    # scan O(k * line length). A transcript with keys JSON-escaped onto single
    # lines is exactly that shape, and it measured 6.6s at 6.4M characters
    # against the pre-fix module's 0.082s -- a fourth quadratic, introduced
    # while removing a third.
    newline = -1

    while pos < length:
        if newline < pos:
            found = text.find("\n", pos)
            newline = length if found == -1 else found
        line_end = newline
        next_pos = length if newline >= length else newline + 1

        # Bounded search rather than a slice, for the same reason.
        header = _PRIVATE_KEY_HEADER_RE.search(text, pos, line_end)
        if header is not None:
            kind = header.group("kind")
            end_marker = f"-----END {kind}-----"
            absolute_header_end = header.end()
            # Binary search, not a linear scan from the front of the list.
            # Scanning restarts at element 0 for every header, which is O(n^2)
            # in the number of same-kind TERMINATED blocks -- 2.1s at 3.2M
            # characters against dev's 0.039s, a quadratic reintroduced while
            # removing a different one. The linearity test did not catch it
            # because its input has no END markers at all, so this list is
            # always empty there.
            starts = ends_by_kind.get(kind, ())
            cursor = bisect.bisect_left(starts, absolute_header_end)
            end_index = starts[cursor] if cursor < len(starts) else -1
            if placeholder_open:
                pieces.append("\n")
                placeholder_open = False
            # Text before the marker on this line is context, not key material
            # -- unless a previous header left us redacting, in which case a
            # payload running straight into this header without a newline is
            # exactly what sits here, and emitting it raw leaks the key.
            prefix = text[pos:header.start()]
            if prefix and not (redacting and _carries_key_material(prefix)):
                pieces.append(prefix)
            pieces.append(f"-----BEGIN {kind}-----\n{_PLACEHOLDER}")
            placeholder_open = True
            if end_index == -1:
                # No END anywhere after this header. Keep scanning, redacting
                # only the lines that carry key material.
                redacting = True
                pos = next_pos
            else:
                pieces.append(f"\n{end_marker}")
                placeholder_open = False
                redacting = False
                pos = end_index + len(end_marker)
                if pos < length and text[pos] == "\n":
                    pieces.append("\n")
                    pos += 1
            continue

        line = text[pos:line_end]
        if redacting and _carries_key_material(line):
            if not placeholder_open:
                pieces.append(_PLACEHOLDER)
                placeholder_open = True
        else:
            if placeholder_open:
                pieces.append("\n")
                placeholder_open = False
            pieces.append(line)
            if newline < length:
                pieces.append("\n")
        pos = next_pos

    return "".join(pieces)


def _replace_assignment(match: re.Match[str]) -> str:
    quote = match.group("quote") or ""
    return f"{match.group('key')}{match.group('sep')}{quote}{_PLACEHOLDER}{quote}"


def _replace_authorization(match: re.Match[str]) -> str:
    scheme = match.group(2)
    if scheme:
        return f"{match.group(1)}{scheme} {_PLACEHOLDER}"
    return f"{match.group(1)}{_PLACEHOLDER}"


def _review_reasons(
    *,
    too_old: bool,
    too_large: bool,
    sensitive_classes: list[str],
) -> list[str]:
    reasons: list[str] = []
    if too_old:
        reasons.append("older_than_policy")
    if too_large:
        reasons.append("larger_than_policy")
    if sensitive_classes:
        reasons.append("secret_material_detected")
    return reasons


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
