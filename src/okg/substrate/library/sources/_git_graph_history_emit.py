"""GitHistorySource — emit GitCommit, GitAuthor, GitBranch, GitTag nodes.

Walks the commit DAG in-process with libgit2 (``in_proc_walk_commits``)
and enumerates refs in-process, with ``git for-each-ref`` only as the
fallback. Authors are deduplicated by email across commits and across
repos. Cross-module edges (`AFFECTS` to `SourceFile`) are emitted
regardless of whether the file's `SourceFile` node has been loaded.
Each commit node carries its first message line (`message_subject`) and,
when the message has more, the redacted and capped rest
(`message_body`).

State persisted between runs (under ``source.state``)::

    {
        "ref_bindings": {"refs/heads/main": "<sha>", ...},
        "seen_commit_shas": ["<sha>", ...],
        "message_schema_version": 5,
        "head_sha": "<sha>",
        "detect_renames": true,   # only when rename detection is on
    }

An incremental run walks the commits reachable from the current ref tips
and not from the prior ones. A cursor whose ``message_schema_version`` is
not the current one walks every reachable commit once, so commit nodes and
edges written before a field change are rewritten with the new fields.
A cursor written with the other ``detect_renames`` setting (a cursor without
the key was written with it off) also walks every reachable commit once, so
the stored ``affects`` edges of a rename match the current setting.

A file endpoint for a path the head tree no longer holds carries
``deleted_at`` and ``deleted_commit_sha`` (issue #3468). When the head moved
since the last run, every path touched by a commit on either side of the
move is re-emitted, so a fast-forward or a merge of commits already walked
on another ref still marks the files it removes.
"""

from __future__ import annotations

import base64
import binascii
import fnmatch
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pygit2

from okg.substrate.library.sources._git_helpers import (
    _r,
    in_proc_path_deletions,
    in_proc_walk_commits,
    paths_touched_between,
    run_git,
)
from okg.substrate.library.sources.source_specs import EdgeSpec, NodeSpec
from okg.substrate.support import redact_text


class ConflictingFileChangeError(RuntimeError):
    """One commit's changed files list one path with two change types."""


def _claim_change_type(
    change_types: dict[str, str], sha: str, path: str, change_type: str
) -> None:
    """Record ``path``'s change type in this commit, or refuse a second,
    different one.

    One commit changes a file one way. Two rows that disagree (a type change
    split into a delete and an add, a rename source also listed as added)
    would write two `affects` edges and let their order pick the stored
    change type, so the ingest stops instead (issue #3666).
    """
    prior = change_types.setdefault(path, change_type)
    if prior != change_type:
        raise ConflictingFileChangeError(
            f"commit {sha} lists {path!r} as both {prior} and {change_type}; "
            "one commit changes a file one way, so this history read is refused"
        )


def _parse_since(spec: str) -> int | None:
    """Parse a `--since=...` string into a Unix timestamp.

    Accepts YYYY-MM-DD (legacy default) and YYYY-MM-DDTHH:MM:SS forms.
    Returns None on parse failure (caller treats as "no filter").
    Per `add-pygit2-in-process-git-helpers`: pygit2's revwalk doesn't
    take a since-cutoff, so we filter client-side.
    """
    try:
        # Try full ISO first
        return int(datetime.fromisoformat(spec).timestamp())
    except (ValueError, TypeError):
        pass
    try:
        return int(datetime.strptime(spec, "%Y-%m-%d").timestamp())
    except (ValueError, TypeError):
        return None


logger = logging.getLogger(__name__)

#: Version of the commit fields a cursor's commits were emitted with.
#: 1 (absent from the cursor) = subject only; 2 = subject plus
#: `message_body`; 3 = merge commits' `affects` edges carry `via_merge`
#: (issue #3468); 4 = file endpoints of deleted paths carry `deleted_at` and
#: `deleted_commit_sha` (issue #3468); 5 = commit nodes carry `author` and
#: `author_name` (issue #3649). A cursor at any other version re-walks
#: every commit.
MESSAGE_SCHEMA_VERSION = 5

#: Most UTF-8 bytes `message_body` may hold, truncation marker included.
MESSAGE_BODY_MAX_BYTES = 16_384


def _cap_utf8(text: str, max_bytes: int, what: str) -> str:
    """Cap already-redacted ``text`` at ``max_bytes`` UTF-8 bytes.

    Text over the cap is cut on a character boundary and ends with a
    marker line ``[<what> truncated: kept N of M bytes]``, so a reader can
    see it was cut; the marker counts toward the cap.
    """
    encoded = text.encode("utf-8")
    total = len(encoded)
    if total <= max_bytes:
        return text
    # The marker's own length depends on the kept count, which is at most
    # `total`; size it with `total` so the result never exceeds the cap.
    marker_len = len(f"\n[{what} truncated: kept {total} of {total} bytes]".encode())
    kept_bytes = encoded[: max(0, max_bytes - marker_len)]
    kept = kept_bytes.decode("utf-8", errors="ignore")
    kept_len = len(kept.encode("utf-8"))
    return f"{kept}\n[{what} truncated: kept {kept_len} of {total} bytes]"


def _cap_message_body(body: str) -> str:
    """Cap an already-redacted body at ``MESSAGE_BODY_MAX_BYTES``."""
    return _cap_utf8(body, MESSAGE_BODY_MAX_BYTES, "message body")


#: Defaults for the opt-in commit diffs (`store_diffs`).
DIFF_MAX_FILE_BYTES = 32_768
DIFF_MAX_COMMIT_BYTES = 262_144
#: Files never diffed unless `diff_exclude_globs` replaces this list:
#: generated files (lockfiles, minified bundles, source maps) whose diffs
#: are noise, and secret-bearing files whose old text must not reach the
#: graph. `fnmatch` lets `*` cross `/`, so a leading `*` matches any
#: directory.
DEFAULT_DIFF_EXCLUDE_GLOBS: tuple[str, ...] = (
    "*.lock",
    "*package-lock.json",
    "*pnpm-lock.yaml",
    "*go.sum",
    "*.min.js",
    "*.min.css",
    "*.map",
    "*.env",
    "*.env.*",
    "*.pem",
    "*.key",
    "*.p12",
    "*.pfx",
    "*.keystore",
    "*.jks",
    "*id_rsa*",
    "*id_ed25519*",
    "*id_ecdsa*",
    "*secret*",
    "*credential*",
)

# Secret shapes `redact_text` (written for bug reports) misses in config
# diffs: a key naming a secret followed by `=` or `:`, with an optional
# quote on either side (`DB_PASSWORD=...`, `"clientSecret": "..."`,
# `api_key: '...'`), a PEM private key block, AWS access key ids, Slack
# webhooks and `sk-` API keys. They apply to `+` and `-` lines alike: a
# removed line is exactly where a rotated secret sits.
#: `key <sep> value` on ONE line (the caller splits lines, and `[ \t]` never
#: crosses one, so a diff line's `+`/`-` prefix is never taken as a value).
#: The key starts at a token boundary (not after a word character, `.`,
#: `-` or `<`, so the `secret` inside an earlier `<secret:redacted>` marker
#: is not a key) and is at most 100 characters, which keeps matching linear
#: in the line length. Whether the key names a secret is decided in Python
#: (`_is_secret_key`), not by the pattern.
_PATCH_KEY_VALUE_RE = re.compile(
    r"""(?x)
    # Not inside a word, a dotted name or a tag; after `-` only when that
    # `-` starts a command-line flag (`--NotebookApp.password='...'`,
    # `-Djavax.net.ssl.keyStorePassword=...`), never inside `x-password`.
    (?<![\w.<])(?<![\w.]-)
    (?P<key>["']?[A-Za-z_][\w.-]{0,99}["']?)
    (?P<sep>[ \t]*[:=][ \t]*)
    (?:
        (?P<quote>["'])(?P<quoted>[^"'\n]*)(?P=quote)
      | (?P<bare>[^\s"',;}]+)
    )
    """
)
#: Key words that name a secret, matched inside the lower-cased key with
#: `_`, `-` and `.` removed (so `DB_PASSWORD`, `clientSecret`, `api-key`).
_SECRET_KEY_WORDS = (
    "password",
    "passwd",
    "passphrase",
    "secret",
    "token",
    "apikey",
    "accesskey",
    "privatekey",
    "credential",
    "authkey",
    "encryptionkey",
    "signingkey",
    "masterkey",
)
#: Whole key segments (split on `_`, `-`, `.`) that name a secret but are
#: too short to match inside other words (`DB_PASS`, `PG_PWD`).
_SECRET_KEY_SEGMENTS = frozenset({"pass", "pwd", "pw"})
_PATCH_BEARER_RE = re.compile(r"(?i)\b(bearer|basic)([ \t]+)[A-Za-z0-9._~+/=-]{8,}")
_PATCH_PEM_RE = re.compile(
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
    re.DOTALL,
)
_PATCH_TOKEN_RES = (
    re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    re.compile(r"https://hooks\.slack\.com/services/[^\s\"']+"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}"),
    re.compile(r"\bxox[abposr]-[A-Za-z0-9-]{8,}"),
    # GitLab personal, deploy, runner, trigger, CI job, feed, feature-flag,
    # incoming-mail, SCIM, agent and OAuth tokens (a routable token's `.01.`
    # suffix included), and runner registration tokens.
    re.compile(
        r"(?<![A-Za-z0-9])gl(?:pat|dt|rt|ptt|cbt|ft|ffct|imt|soat|agent|oas)"
        r"-[A-Za-z0-9_.-]{20,}"
    ),
    re.compile(r"(?<![A-Za-z0-9])GR1348941[A-Za-z0-9_-]{20,}"),
    # JWTs, an unsigned one (empty signature) included.
    re.compile(r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*"),
)
#: A PEM or PGP private-key header and footer, for keys a hunk cut off.
_PATCH_KEY_HEADER_RE = re.compile(
    r"-----BEGIN (?:[A-Z0-9 ]*PRIVATE KEY|PGP PRIVATE KEY BLOCK)-----"
)
_PATCH_KEY_FOOTER_RE = re.compile(
    r"-----END (?:[A-Z0-9 ]*PRIVATE KEY|PGP PRIVATE KEY BLOCK)-----"
)
#: Start of the base64 encoding of `-----BEGIN` (how a Kubernetes secret or
#: a `.env` file carries a PEM key), standard or URL-safe, and the rest of
#: the run.
_PATCH_B64_PEM_RE = re.compile(r"LS0tLS1CRUdJTi[A-Za-z0-9+/_-]*={0,2}")
#: Labels after `-----BEGIN ` that are public material, kept when encoded.
_PUBLIC_PEM_LABELS = (
    "CERTIFICATE",
    "TRUSTED CERTIFICATE",
    "NEW CERTIFICATE REQUEST",
    "CERTIFICATE REQUEST",
    "X509 CRL",
    "PUBLIC KEY",
    "RSA PUBLIC KEY",
    "PKCS7",
)
#: A line that is one base64 run once quotes, commas, `+` joins, brackets
#: and indentation are stripped: the body of a wrapped or quoted key.
_PATCH_KEY_LINE_STRIP = " \t\r'\"`,+()\\;"
#: PGP armor and encrypted-PEM headers (`Version:`, `DEK-Info:`) inside a key.
_PATCH_KEY_ARMOR_RE = re.compile(r"[ \t]*[A-Za-z][A-Za-z-]*:[ \t]")
_PATCH_KEY_LINE_RE = re.compile(r"[A-Za-z0-9+/_-]+={0,2}")


_REDACTED = "<secret:redacted>"
#: Version of the patch redaction rules; recorded in the cursor's diff
#: settings. 1 (absent) = before issue #3468's shapes; 2 = with them.
PATCH_REDACTION_VERSION = 2


def _is_secret_key(key: str) -> bool:
    bare = key.strip("\"'").lower()
    squashed = re.sub(r"[_.\-]", "", bare)
    if any(word in squashed for word in _SECRET_KEY_WORDS):
        return True
    return any(part in _SECRET_KEY_SEGMENTS for part in re.split(r"[_.\-]", bare))


def _redact_key_value(match: re.Match[str]) -> str:
    if not _is_secret_key(match.group("key")):
        # A value can hold settings of its own
        # (`JAVA_OPTS=-Djavax.net.ssl.keyStorePassword=...`): look one level
        # in, no deeper, so cost stays linear in the line.
        value = match.group("quoted") if match.group("quote") else match.group("bare")
        if value and "=" in value:
            start = match.start("quoted" if match.group("quote") else "bare") - match.start()
            whole = match.group(0)
            inner = _PATCH_KEY_VALUE_RE.sub(_redact_secret_key_value, value)
            return whole[:start] + inner + whole[start + len(value):]
        return match.group(0)
    return _redact_secret_key_value(match)


def _redact_secret_key_value(match: re.Match[str]) -> str:
    if not _is_secret_key(match.group("key")):
        return match.group(0)
    head = match.group("key") + match.group("sep")
    if match.group("quote") is not None:
        value = match.group("quoted")
        if not value or value == _REDACTED:
            return match.group(0)
        quote = match.group("quote")
        return f"{head}{quote}{_REDACTED}{quote}"
    value = match.group("bare")
    if value.startswith(_REDACTED):
        return match.group(0)
    return f"{head}{_REDACTED}"


def _redact_patch(text: str, *, repo_root: Path) -> str:
    """Redact a diff: ``redact_text`` plus the config-secret shapes above.

    Key/value redaction runs one line at a time, so it can neither swallow
    the next line's diff prefix nor cost more than linear time on one long
    line.
    """
    out = redact_text(text, repo_root=repo_root)[0]
    out = _PATCH_PEM_RE.sub(_REDACTED, out)
    for pattern in _PATCH_TOKEN_RES:
        out = pattern.sub(_REDACTED, out)
    out = _PATCH_BEARER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{_REDACTED}", out)
    lines = [_redact_patch_line(line) for line in out.split("\n")]
    return "\n".join(_redact_key_runs(lines))


def _is_key_line(body: str) -> bool:
    """Whether a line is nothing but one base64 run (quotes, commas, `+`
    joins, brackets and indentation aside), as a wrapped key body is."""
    stripped = body.strip(_PATCH_KEY_LINE_STRIP)
    return bool(stripped) and _PATCH_KEY_LINE_RE.fullmatch(stripped) is not None


def _is_encoded_public_pem(encoded: str) -> bool:
    """Whether base64 text starting with encoded `-----BEGIN ` decodes to a
    certificate or public-key header. Anything else -- a private key, or a
    run too short to tell -- counts as a key."""
    text = encoded.replace("-", "+").replace("_", "/")
    text = text[: (min(len(text), 400) // 4) * 4]
    try:
        decoded = base64.b64decode(text, validate=True).decode("ascii", "replace")
    except (binascii.Error, ValueError):
        return False
    label = decoded[len("-----BEGIN "):]
    return any(label.startswith(f"{name}-----") for name in _PUBLIC_PEM_LABELS)


def _redact_key_runs(lines: list[str]) -> list[str]:
    """Redact key material the paired-PEM pattern cannot see.

    A diff hunk can cut a PEM or PGP key off at either end, and a Kubernetes
    secret, `.env` file or source string carries a key base64-encoded,
    often wrapped or split into quoted pieces. Forward pass: a line with a
    key header, or with an encoded run that is not a certificate or public
    key (decoded together with the key lines after it, so a short first
    piece still reads), starts a key; the encoded run is replaced in place
    and each following line that is nothing but a base64 run is replaced by
    the marker, keeping its diff prefix; armor headers (`Version:`,
    `DEK-Info:`) and a blank line keep it going. A footer, or any other line, ends it. Backward pass: the base64-only lines just above a footer whose header
    is not in the patch are replaced the same way.
    """
    parts = [
        (line[:1], line[1:]) if line[:1] in ("+", "-", " ") else ("", line)
        for line in lines
    ]
    out = [prefix + body for prefix, body in parts]
    in_key = False
    for i, (prefix, body) in enumerate(parts):
        runs = list(_PATCH_B64_PEM_RE.finditer(body))
        if runs:
            following = "".join(
                b.strip(_PATCH_KEY_LINE_STRIP)
                for _p, b in parts[i + 1 : i + 5]
                if _is_key_line(b)
            )
            secret = [
                m
                for m in runs
                if not _is_encoded_public_pem(
                    m.group(0) + (following if m.end() == len(body.rstrip(_PATCH_KEY_LINE_STRIP)) else "")
                )
            ]
            if secret:
                for m in reversed(secret):
                    body = body[: m.start()] + _REDACTED + body[m.end():]
                out[i] = prefix + body
                in_key = True
                continue
        if _PATCH_KEY_HEADER_RE.search(body):
            in_key = True
            continue
        if _PATCH_KEY_FOOTER_RE.search(body):
            if not in_key:
                j = i - 1
                while j >= 0 and _is_key_line(parts[j][1]):
                    out[j] = parts[j][0] + _REDACTED
                    j -= 1
            in_key = False
            continue
        if in_key and _is_key_line(body):
            out[i] = prefix + _REDACTED
            continue
        if in_key and (not body.strip() or _PATCH_KEY_ARMOR_RE.match(body)):
            # PGP armor or encrypted-PEM headers, and the blank line after
            # them, sit between a key's header and its body.
            continue
        in_key = False
    return out


def _redact_patch_line(line: str) -> str:
    # A content line's first character is the diff prefix (`+`, `-` or a
    # space); match after it, so `-DB_PASS=...` has its key at a boundary.
    if line[:1] in ("+", "-", " "):
        return line[:1] + _PATCH_KEY_VALUE_RE.sub(_redact_key_value, line[1:])
    return _PATCH_KEY_VALUE_RE.sub(_redact_key_value, line)


def _parse_since_strict(spec: str, *, name: str) -> int:
    """`_parse_since` that refuses what it cannot parse, instead of
    treating it as "no filter"."""
    parsed = _parse_since(spec)
    if parsed is None:
        raise ValueError(
            f"{name} must be a date (YYYY-MM-DD) or an ISO timestamp; got {spec!r}"
        )
    return parsed


@dataclass
class _CommitRecord:
    sha: str
    parents: list[str]
    authored_at: str
    committed_at: str
    author_email: str
    author_name: str
    subject: str
    file_changes: list[
        tuple[str, str, str | None]
    ]  # (status, new_path, old_path_for_rename)
    # Raw message after the subject line ("" when none); redacted and
    # capped at emission.
    body: str = ""
    # The diffs this commit stores, already redacted and capped (from
    # `GitHistorySource._diff_candidates`), and the count of changed files
    # left out; both None when diffs are off or the commit does not qualify
    # (a merge, or before `diffs_since`). The raw patches are not kept, so a
    # whole-history walk holds at most the capped text.
    diff_rows: list[tuple[Any, str, int, str]] | None = None
    diff_omitted: int | None = None



@dataclass
class GitHistorySource:
    """Ingest source for git commit history.

    Parameters
    ----------
    repo_root : Path
        Path to the working tree root.
    repo : str
        Canonical short name used in node IDs.
    include_remotes : bool
        Whether to ingest `refs/remotes/...` branches. Default False.
    history_since : str | None
        Optional date filter (`YYYY-MM-DD` or any git-accepted form)
        applied ONLY on the cold-start crawl. Subsequent incremental
        runs ingest all newly-reachable commits regardless of date.
    branch_filter : list[str] | None
        If set, only refs whose name matches one of these (exact)
        names are emitted. Useful to exclude noisy branches.
    source_file_exclude_globs : list[str] | None
        File paths matching any glob are outside this source's declared scope;
        neither their substitute endpoint nor an ``affects`` edge is emitted.
    detect_renames : bool
        Whether to run libgit2 rename/copy similarity detection while
        walking commit diffs. Defaults false because routine source-sync
        only needs file-affect facts and can represent renames as delete
        plus add; enabling similarity detection is explicit backfill work.
        Changing it in either direction makes the next run re-walk every
        reachable commit of the repository once, so each rename's
        ``affects`` edge is rewritten as ``renamed`` (or back to ``added``).
    store_diffs : bool
        Opt-in (default false): emit one ``git_file_diff`` node per changed
        text file of each non-merge commit, holding its redacted, capped
        unified diff, so an old value and the commit that changed it are
        searchable. The remaining ``diff_*`` settings apply only when true.
    diff_max_file_bytes, diff_max_commit_bytes : int
        Cap on one file's stored patch, and on the sum of a commit's stored
        patches (smallest first; files that do not fit are counted in the
        commit's ``diff_omitted_file_count``).
    diff_exclude_globs : list[str] | None
        Paths never diffed; ``None`` means ``DEFAULT_DIFF_EXCLUDE_GLOBS``, a
        list replaces it. ``source_file_exclude_globs`` also applies.
    diffs_since : str | None
        Date or ISO timestamp; commits committed before it get no diff (the
        commit node itself is still emitted). Bounds the cost of turning
        diffs on for a long history.
    """

    repo_root: Path
    repo: str
    include_remotes: bool = False
    history_since: str | None = None
    branch_filter: list[str] | None = None
    source_file_stub_globs: list[str] | None = None
    source_file_exclude_globs: list[str] | None = None
    detect_renames: bool = False
    store_diffs: bool = False
    diff_max_file_bytes: int = DIFF_MAX_FILE_BYTES
    diff_max_commit_bytes: int = DIFF_MAX_COMMIT_BYTES
    diff_exclude_globs: list[str] | None = None
    diffs_since: str | None = None
    state: dict[str, Any] = field(default_factory=dict)
    _emit_nodes: list[NodeSpec] | None = None
    _emit_edges: list[EdgeSpec] | None = None
    _emit_aliases: dict[str, str] = field(default_factory=dict)
    _stale: list[str] = field(default_factory=list)
    _raw_ref_bindings: dict[str, str] = field(default_factory=dict)
    _ref_snapshot_exact: bool = False
    _history_read_exact: bool = True

    ownership_id: str = "git_graph.history"
    supports_incremental: bool = True
    run_state_kind: str = (
        "metadata"  # bind_run_state passes us state.metadata as a dict
    )

    def __post_init__(self) -> None:
        if isinstance(self.repo_root, str):
            self.repo_root = Path(self.repo_root).expanduser()
        self._diffs_since_ts: int | None = None
        # YAML reads an unquoted `2025-01-01` as a date; the cursor stores
        # JSON, so compare and record the string form or every run would see
        # changed settings and re-walk.
        if self.diffs_since is not None:
            self.diffs_since = str(self.diffs_since)
        if self.store_diffs:
            for name in ("diff_max_file_bytes", "diff_max_commit_bytes"):
                value = getattr(self, name)
                if isinstance(value, bool) or not isinstance(value, int) or value < 1024:
                    raise ValueError(f"{name} must be an integer >= 1024; got {value!r}")
            if self.diffs_since is not None:
                self._diffs_since_ts = _parse_since_strict(
                    str(self.diffs_since), name="diffs_since"
                )

    def diff_settings(self) -> dict[str, Any] | None:
        """The diff settings a cursor records; ``None`` when diffs are off,
        so a cursor written before diffs existed reads as "off"."""
        if not self.store_diffs:
            return None
        return {
            "max_file_bytes": self.diff_max_file_bytes,
            "max_commit_bytes": self.diff_max_commit_bytes,
            "exclude_globs": list(self._diff_exclude_globs()),
            "since": self.diffs_since,
            # Bumped when patch redaction learns new secret shapes, so a
            # cursor written with diffs on re-walks and rewrites every stored
            # patch (issue #3468: GitLab tokens, JWTs, encoded and cut-off
            # PEM keys, `--flag.password=` values).
            "redaction": PATCH_REDACTION_VERSION,
        }

    def _diff_exclude_globs(self) -> tuple[str, ...]:
        if self.diff_exclude_globs is None:
            return DEFAULT_DIFF_EXCLUDE_GLOBS
        return tuple(self.diff_exclude_globs)

    # --- Source protocol ----------------------------------------------------

    def nodes(self) -> Iterable[NodeSpec]:
        if self._emit_nodes is None:
            self._compute_emit()
        yield from self._emit_nodes or []

    def edges(self) -> Iterable[EdgeSpec]:
        if self._emit_edges is None:
            self._compute_emit()
        yield from self._emit_edges or []

    def aliases(self) -> dict[str, str]:
        if self._emit_nodes is None:
            self._compute_emit()
        return dict(self._emit_aliases)

    # --- State hooks --------------------------------------------------------

    def set_run_state(self, state: Any) -> None:
        self.state = dict(state) if state else {}

    def get_run_state(self) -> dict[str, Any]:
        return dict(self.state)

    @property
    def stale_node_ids(self) -> list[str]:
        return list(self._stale)

    # --- Implementation ------------------------------------------------------

    def _compute_emit(self) -> None:
        prior_refs: dict[str, str] = self.state.get("ref_bindings") or {}
        prior_raw_refs: dict[str, str] = (
            self.state.get("raw_ref_bindings") or {}
        )
        prior_seen: set[str] = set(self.state.get("seen_commit_shas") or [])
        # A cursor written with other commit message fields (before
        # `message_body`, the key is absent) re-walks every reachable commit
        # once, even with unchanged refs, so the commit nodes it already
        # emitted are rewritten with the current fields. prior_refs stay as
        # they are: they still decide which branch/tag nodes are re-emitted
        # and which refs are stale. The same holds for a cursor written with
        # other diff settings (`store_diffs` turned on or off, a cap, an
        # exclude list or `diffs_since` changed); a cursor with no settings
        # key was written with diffs off. Likewise for `detect_renames`: a
        # rename's `affects` edge to the new path is `added` with it off and
        # `renamed` with it on, and a cursor without the key had it off.
        rewalk = bool(prior_refs) and (
            self.state.get("message_schema_version") != MESSAGE_SCHEMA_VERSION
            or self.state.get("diff_settings") != self.diff_settings()
            or bool(self.state.get("detect_renames", False))
            != bool(self.detect_renames)
        )
        if rewalk:
            prior_seen = set()

        # Enumerate current refs
        cur_refs = self._list_refs()
        head_sha = self._head_sha()
        prior_head: str | None = self.state.get("head_sha")

        # Short-circuit: if refs are exactly unchanged, no commits or
        # ref-pointer updates can have happened. Emit nothing. A checkout of
        # another branch moves only HEAD, which decides the deleted paths.
        if (
            not rewalk
            and cur_refs == prior_refs
            and self._raw_ref_bindings == prior_raw_refs
            and (prior_head is None or prior_head == head_sha)
        ):
            self._emit_nodes = []
            self._emit_edges = []
            self._emit_aliases = {}
            self.state = self._cursor_state(
                cur_refs,
                seen=prior_seen,
                snapshot_exact=self._ref_snapshot_exact,
                head_sha=head_sha,
            )
            return

        # Branch + tag node emission — only for refs whose head moved or
        # are newly present (skip refs whose binding is unchanged).
        nodes: list[NodeSpec] = []
        edges: list[EdgeSpec] = []
        aliases: dict[str, str] = {}

        local_refs, remote_refs, tag_refs = self._partition_refs(cur_refs)

        for ref_name, sha in sorted(local_refs.items()):
            short_name = ref_name[len("refs/heads/") :]
            if self.branch_filter and short_name not in self.branch_filter:
                continue
            node_id = f"branch:{self.repo}:{short_name}"
            aliases[short_name] = node_id
            # Skip emission if ref binding unchanged
            if prior_refs.get(ref_name) == sha:
                continue
            nodes.append(
                NodeSpec(
                    node_id=node_id,
                    subtype="git_branch",
                    label=short_name,
                    attrs={
                        "name": short_name,
                        "repo": self.repo,
                        "head_commit_sha": sha,
                        "is_remote": False,
                    },
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(
                    src=node_id,
                    dst=f"commit:{self.repo}:{sha}",
                    edge_type="references",
                )
            )

        if self.include_remotes:
            for ref_name, sha in sorted(remote_refs.items()):
                short_name = ref_name[len("refs/remotes/") :]
                node_id = f"branch:{self.repo}:{short_name}"
                if prior_refs.get(ref_name) == sha:
                    continue
                # Remote name is the first path component (e.g., "origin/main" → "origin")
                remote_name = short_name.split("/", 1)[0] if "/" in short_name else None
                nodes.append(
                    NodeSpec(
                        node_id=node_id,
                        subtype="git_branch",
                        label=short_name,
                        attrs={
                            "name": short_name,
                            "repo": self.repo,
                            "head_commit_sha": sha,
                            "is_remote": True,
                            "remote_name": remote_name,
                        },
                        source=self.ownership_id,
                    )
                )
                edges.append(
                    EdgeSpec(
                        src=node_id,
                        dst=f"commit:{self.repo}:{sha}",
                        edge_type="references",
                    )
                )

        for ref_name, (sha, is_annotated) in sorted(tag_refs.items()):
            short_name = ref_name[len("refs/tags/") :]
            node_id = f"tag:{self.repo}:{short_name}"
            if prior_refs.get(ref_name) == cur_refs.get(ref_name):
                continue
            nodes.append(
                NodeSpec(
                    node_id=node_id,
                    subtype="git_tag",
                    label=short_name,
                    attrs={
                        "name": short_name,
                        "repo": self.repo,
                        "target_commit_sha": sha,
                        "is_annotated": is_annotated,
                    },
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(
                    src=node_id,
                    dst=f"commit:{self.repo}:{sha}",
                    edge_type="references",
                )
            )

        # Detect deleted refs (present last run, absent now)
        for prior_ref in sorted(prior_refs):
            if prior_ref in cur_refs:
                continue
            short_name = prior_ref.split("/", 2)[-1]
            if prior_ref.startswith("refs/heads/") or prior_ref.startswith(
                "refs/remotes/"
            ):
                self._stale.append(f"branch:{self.repo}:{short_name}")
            elif prior_ref.startswith("refs/tags/"):
                self._stale.append(f"tag:{self.repo}:{short_name}")

        # Determine which commits to emit
        commit_records = list(
            self._collect_commits(
                prior_seen, cur_refs.values(), prior_refs, full_walk=rewalk
            )
        )
        commit_records.sort(key=lambda record: record.sha)

        # Authors: dedup by email across this run. Prefer the longest display
        # name, then the lexicographically smallest equal-length name. The tie
        # break must not depend on revwalk root order: ref tips are deduplicated
        # from a set, and process hash randomization used to make a fixed repo
        # alternate between equal-length names across executions.
        #
        # Per dogfood-canary DQ audit (2026-05-19): normalize email to
        # lowercase before using it as the author key. Git lets each
        # commit's `User <Email>` line be whatever the author typed —
        # `User@Example.com` and `user@example.com` got
        # emitted as two distinct author nodes for the same person.
        # Email RFC says local-part case is technically significant,
        # but in practice every consumer treats it as case-insensitive.
        author_names: dict[str, str] = {}
        for rec in commit_records:
            if rec.author_email:
                email_key = rec.author_email.strip().lower()
                # Prefer the longest display name seen for the email
                # (handles `Alex` vs `Alex Example`).
                candidate = rec.author_name or ""
                prior = author_names.get(email_key)
                if (
                    prior is None
                    or len(candidate) > len(prior)
                    or (len(candidate) == len(prior) and candidate < prior)
                ):
                    author_names[email_key] = candidate

        for email, name in sorted(author_names.items()):
            node_id = f"author:{email}"
            nodes.append(
                NodeSpec(
                    node_id=node_id,
                    subtype="git_author",
                    label=name or email,
                    attrs={"email": email, "display_name": name},
                    source=self.ownership_id,
                    # `author:<email>` is deliberately NOT repo-scoped, so
                    # every GitHistorySource in a multi-repo deployment
                    # declares the same identity. `authoritative` is a claim
                    # of exclusive ownership only one of them can hold: the
                    # last writer reattributes the row and every sibling's
                    # coverage proof then reads its own author nodes as
                    # missing and refuses to advance -- a race whose failing
                    # subset varies per run. A person who commits to two
                    # repos is not owned by either history source, so the
                    # honest declaration is `substitute`, the same mode the
                    # historical file endpoints below already carry.
                    ownership_mode="substitute",
                )
            )
            aliases[email] = node_id
            # Also alias the as-observed casing so historical commits
            # referencing the upper-case form still resolve to the
            # canonical lowercase node.
            for rec in commit_records:
                if (
                    rec.author_email
                    and rec.author_email != email
                    and rec.author_email.strip().lower() == email
                ):
                    aliases[rec.author_email] = node_id

        # Commits + their edges
        new_seen: set[str] = set(prior_seen)
        historical_file_nodes: set[str] = set()
        file_node_attrs: dict[str, dict[str, Any]] = {}

        def _emit_file_node(path: str) -> bool:
            """Emit a deterministic substitute for each historical file endpoint.

            The adapter wrapper carries the explicit ``substitute`` ownership mode
            to the graph boundary. Membership must not consult the checkout: refs
            are the source's declared change token, and dirty/missing worktree
            state cannot change a completed history record set.
            """
            if not path or any(
                fnmatch.fnmatch(path, glob)
                for glob in (self.source_file_exclude_globs or ())
            ):
                return False
            node_id = f"file:{self.repo}:{path}"
            if node_id in historical_file_nodes:
                return True
            historical_file_nodes.add(node_id)
            attrs: dict[str, Any] = {
                "path": path,
                "repo": self.repo,
                # This is intentionally false even when the checkout has
                # the path: the substitute carries no blob metadata. A
                # GitFilesSource row, when present, remains authoritative.
                "current_in_worktree": False,
                "source_provider": "git_history",
            }
            file_node_attrs[path] = attrs
            nodes.append(
                NodeSpec(
                    node_id=node_id,
                    subtype="source_file",
                    label=path,
                    attrs=attrs,
                    source=self.ownership_id,
                    ownership_mode="substitute",
                )
            )
            return True

        for rec in commit_records:
            commit_id = f"commit:{self.repo}:{rec.sha}"
            redacted_subject, _markers = redact_text(
                rec.subject,
                repo_root=self.repo_root,
            )
            # Redact before capping: a cut made first could split a secret
            # so its pattern no longer matches and part of it survives.
            message_body = (
                _cap_message_body(
                    redact_text(rec.body, repo_root=self.repo_root)[0]
                )
                if rec.body
                else ""
            )
            # `patches` is None unless diffs are on and the commit qualifies
            # (not a merge, not before `diffs_since`).
            diff_rows = rec.diff_rows or []
            diff_omitted = rec.diff_omitted
            nodes.append(
                NodeSpec(
                    node_id=commit_id,
                    subtype="git_commit",
                    label=redacted_subject[:80],
                    attrs={
                        "commit_sha": rec.sha,
                        # Short forms operators and tools actually write
                        # (git's default 7, GitHub/GitLab 7-12). The BM25
                        # index matches whole lexemes only, so a prefix
                        # query never hits the 40-char SHA by itself.
                        "commit_sha_prefixes": " ".join(
                            rec.sha[:n] for n in range(7, 13)
                        ),
                        "repo": self.repo,
                        "message_subject": redacted_subject,
                        "authored_at": rec.authored_at,
                        "committed_at": rec.committed_at,
                        # `created_at` is declared in the schema for
                        # every artifact/process subtype; for commits
                        # the natural value is the authored timestamp.
                        # 2026-04-25 fix (debug-substrate-okg-self
                        # Phase 8): prior to this, `created_at` was
                        # NULL on every commit because the legacy
                        # TimeResolver (a Phase B enricher) didn't
                        # run on the substrate.
                        "created_at": rec.authored_at,
                        "parent_count": len(rec.parents),
                        "is_merge": len(rec.parents) >= 2,
                        # The author as plain fields, so commits can be
                        # counted and filtered by author the way tickets
                        # are by assignee (issue #3649). `author` is the
                        # id of the `git_author` node the `assigned_to`
                        # edge below starts from; a commit with no author
                        # e-mail has neither that edge nor this field,
                        # never an empty or invented id. `author_name` is
                        # the name as this commit records it: the author
                        # node keeps one name per e-mail.
                        **(
                            {
                                "author": (
                                    "author:"
                                    f"{rec.author_email.strip().lower()}"
                                )
                            }
                            if rec.author_email
                            else {}
                        ),
                        **(
                            {"author_name": rec.author_name}
                            if rec.author_name
                            else {}
                        ),
                        **(
                            {"message_body": message_body}
                            if message_body
                            else {}
                        ),
                        **(
                            {"diff_omitted_file_count": diff_omitted}
                            if diff_omitted is not None
                            else {}
                        ),
                    },
                    source=self.ownership_id,
                )
            )
            new_seen.add(rec.sha)
            # Per-edge observed_at = the commit's committed_at, so the
            # timeline of REFERENCES / AFFECTS / ASSIGNED_TO / DERIVES_FROM
            # edges out of this commit lines up with the commit history.
            commit_ts = rec.committed_at or rec.authored_at

            # Author edge — same lower-case normalization as node id.
            if rec.author_email:
                edges.append(
                    EdgeSpec(
                        src=f"author:{rec.author_email.strip().lower()}",
                        dst=commit_id,
                        edge_type="assigned_to",
                        attrs={"role": "author", "observed_at": commit_ts},
                    )
                )

            # Parent edges
            for idx, parent_sha in enumerate(rec.parents):
                edges.append(
                    EdgeSpec(
                        src=commit_id,
                        dst=f"commit:{self.repo}:{parent_sha}",
                        edge_type="derives_from",
                        attrs={"parent_index": idx, "observed_at": commit_ts},
                    )
                )

            # File-change edges (cross-module: target SourceFile may not be loaded).
            # A merge is diffed against its first parent, so it "changes"
            # every file its branch changed; `via_merge` says the change was
            # authored in another commit and brought here (issue #3468).
            merge_attrs = {"via_merge": True} if len(rec.parents) >= 2 else {}
            change_types: dict[str, str] = {}
            for status, new_path, old_path in rec.file_changes:
                change_type, target_path, paired_delete = self._decode_status(
                    status, new_path, old_path
                )
                _claim_change_type(change_types, rec.sha, target_path, change_type)
                if paired_delete:
                    _claim_change_type(
                        change_types, rec.sha, paired_delete, "deleted"
                    )
                if _emit_file_node(target_path):
                    edges.append(
                        EdgeSpec(
                            src=commit_id,
                            dst=f"file:{self.repo}:{target_path}",
                            edge_type="affects",
                            attrs={
                                "change_type": change_type,
                                "observed_at": commit_ts,
                                **(
                                    {"renamed_from_path": old_path}
                                    if change_type == "renamed" and old_path
                                    else {}
                                ),
                                **merge_attrs,
                            },
                        )
                    )
                if paired_delete and _emit_file_node(paired_delete):
                    edges.append(
                        EdgeSpec(
                            src=commit_id,
                            dst=f"file:{self.repo}:{paired_delete}",
                            edge_type="affects",
                            attrs={
                                "change_type": "deleted",
                                "observed_at": commit_ts,
                                **merge_attrs,
                            },
                        )
                    )

            # Opt-in per-file diffs: one node per stored patch, contained by
            # the commit and referencing the file it changed.
            for status, old_path, patch_text, patch_bytes, path in diff_rows:
                diff_id = f"diff:{self.repo}:{rec.sha}:{path}"
                change_type, _target, _paired = self._decode_status(
                    status, path, old_path
                )
                nodes.append(
                    NodeSpec(
                        node_id=diff_id,
                        subtype="git_file_diff",
                        label=f"{rec.sha[:7]} {path}",
                        attrs={
                            "repo": self.repo,
                            "commit_sha": rec.sha,
                            "path": path,
                            "change_type": change_type,
                            **(
                                {"renamed_from_path": old_path}
                                if change_type == "renamed" and old_path
                                else {}
                            ),
                            "message_subject": redacted_subject,
                            "committed_at": rec.committed_at,
                            "created_at": rec.committed_at,
                            "patch": patch_text,
                            "patch_bytes": patch_bytes,
                            "patch_truncated": patch_bytes
                            > self.diff_max_file_bytes,
                        },
                        source=self.ownership_id,
                    )
                )
                edges.append(
                    EdgeSpec(
                        src=commit_id,
                        dst=diff_id,
                        edge_type="contains",
                        attrs={"observed_at": commit_ts},
                    )
                )
                if _emit_file_node(path):
                    edges.append(
                        EdgeSpec(
                            src=diff_id,
                            dst=f"file:{self.repo}:{path}",
                            edge_type="references",
                            attrs={"observed_at": commit_ts},
                        )
                    )

            # SHA aliases (full SHA + 7-char prefix)
            aliases[rec.sha] = commit_id
            if len(rec.sha) >= 7:
                aliases[rec.sha[:7]] = commit_id

        if head_sha is not None:
            # A head that moved can bring in commits this source already
            # walked on another ref (a fast-forward, or a merge of a branch
            # seen before) or leave commits behind (a reset): every path
            # those commits touched may have gained or lost its deletion
            # attributes, so re-emit it with the new head's answer.
            if not rewalk and prior_head is not None and prior_head != head_sha:
                for path in sorted(self._paths_touched_since(prior_head, head_sha)):
                    _emit_file_node(path)
            for path, deletion in in_proc_path_deletions(
                self.repo_root, file_node_attrs, head_sha=head_sha
            ).items():
                file_node_attrs[path]["deleted_at"] = deletion.deleted_at
                file_node_attrs[path]["deleted_commit_sha"] = deletion.deleted_sha

        self._emit_nodes = nodes
        self._emit_edges = edges
        self._emit_aliases = aliases
        self.state = self._cursor_state(
            cur_refs,
            seen=new_seen,
            snapshot_exact=self._ref_snapshot_exact and self._history_read_exact,
            head_sha=head_sha,
        )

    def _head_sha(self) -> str | None:
        """The commit HEAD names, or None for a repository with no commit.

        A repository libgit2 cannot open also gives None, with a warning, as
        `_list_refs` already treats it: the run then emits no deletion
        attributes rather than failing.
        """
        try:
            repo = _r(self.repo_root)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "git history %s: cannot open the repository to read HEAD (%s); "
                "no deleted-path attributes this run",
                self.repo,
                e,
            )
            return None
        if repo.head_is_unborn:
            return None
        return str(repo.head.peel(pygit2.Commit).id)

    def _paths_touched_since(self, old_sha: str, new_sha: str) -> set[str]:
        """Paths touched by commits on either side of a head move.

        An old head git no longer has (history rewritten and collected)
        gives no paths: the run then marks only the paths its commits touch.
        """
        repo = _r(self.repo_root)
        try:
            repo[old_sha].peel(pygit2.Commit)
        except (KeyError, ValueError, pygit2.GitError):
            logger.warning(
                "git history %s: previous head %s is gone; deleted-path "
                "attributes cover only the paths new commits touch",
                self.repo,
                old_sha,
            )
            return set()
        return paths_touched_between(self.repo_root, old_sha, new_sha)

    def _cursor_state(
        self,
        cur_refs: dict[str, str],
        *,
        seen: set[str],
        snapshot_exact: bool,
        head_sha: str | None,
    ) -> dict[str, Any]:
        """The cursor every exit path writes (the unchanged-refs short
        circuit included, so a settings re-walk never repeats)."""
        state: dict[str, Any] = {
            "ref_bindings": cur_refs,
            "raw_ref_bindings": dict(self._raw_ref_bindings),
            "snapshot_exact": snapshot_exact,
            "seen_commit_shas": sorted(seen),
            "message_schema_version": MESSAGE_SCHEMA_VERSION,
        }
        if head_sha is not None:
            state["head_sha"] = head_sha
        settings = self.diff_settings()
        if settings is not None:
            state["diff_settings"] = settings
        # Recorded only when on, so a cursor written with the default matches
        # the cursors written before the key existed.
        if bool(self.detect_renames):
            state["detect_renames"] = True
        return state

    def _commit_record(self, rec: Any) -> _CommitRecord:
        """Adapt one ``_git_helpers.CommitRecord``; with diffs on, pick its
        stored diffs now so the raw patches are not kept for the whole walk."""
        diff_rows: list[tuple[str, str | None, str, int, str]] | None = None
        diff_omitted: int | None = None
        if self.store_diffs and rec.patches is not None:
            diff_rows, diff_omitted = self._diff_candidates(
                rec.patches, rec.oversized_patch_paths
            )
        return _CommitRecord(
            sha=rec.sha,
            parents=rec.parents,
            authored_at=rec.author_iso,
            committed_at=rec.committer_iso,
            author_email=rec.author_email,
            author_name=rec.author_name,
            subject=rec.subject,
            file_changes=rec.name_status,
            body=rec.body,
            diff_rows=diff_rows,
            diff_omitted=diff_omitted,
        )

    def _diff_candidates(
        self,
        patches: Iterable[Any],
        oversized_paths: Iterable[str],
    ) -> tuple[list[tuple[str, str | None, str, int, str]], int]:
        """Pick the diffs one commit stores.

        Returns ``([(status, old_path, capped_patch, redacted_bytes, path)],
        omitted)``. Excluded paths (a rename or copy is excluded when either
        side matches) are dropped without a count: they are out of scope by
        configuration. Each remaining patch is redacted, then capped at
        ``diff_max_file_bytes``; patches are kept smallest first (then by
        path) while their sum fits ``diff_max_commit_bytes``. ``omitted``
        counts the files that did not fit plus the files the walk could not
        diff (too large, or a blob missing locally).
        """
        excludes = (
            *self._diff_exclude_globs(),
            *(self.source_file_exclude_globs or ()),
        )

        def excluded(path: str | None) -> bool:
            return bool(path) and any(
                fnmatch.fnmatch(path, glob) for glob in excludes
            )

        prepared: list[tuple[int, str, str, str | None, str, int]] = []
        for file_patch in patches:
            if excluded(file_patch.path) or excluded(file_patch.old_path):
                continue
            redacted = _redact_patch(file_patch.text, repo_root=self.repo_root)
            capped = _cap_utf8(redacted, self.diff_max_file_bytes, "patch")
            prepared.append(
                (
                    len(capped.encode("utf-8")),
                    file_patch.path,
                    file_patch.status,
                    file_patch.old_path,
                    capped,
                    len(redacted.encode("utf-8")),
                )
            )
        prepared.sort(key=lambda row: (row[0], row[1]))
        kept: list[tuple[str, str | None, str, int, str]] = []
        used = 0
        omitted = sum(1 for path in oversized_paths if not excluded(path))
        for size, path, status, old_path, capped, redacted_bytes in prepared:
            if used + size > self.diff_max_commit_bytes:
                omitted += 1
                continue
            used += size
            kept.append((status, old_path, capped, redacted_bytes, path))
        kept.sort(key=lambda row: row[4])
        return kept, omitted

    # --- Ref enumeration ----------------------------------------------------

    def _list_refs(self) -> dict[str, str]:
        """Return refs as `{full_ref_name: commit_sha}`.

        Per `add-pygit2-in-process-git-helpers`: enumerates refs
        in-process via libgit2. Annotated tags resolve to their
        target commit sha (matches what `_partition_refs` would do
        downstream).
        """
        refs: dict[str, str] = {}
        raw_refs: dict[str, str] = {}
        self._ref_snapshot_exact = False
        try:
            from okg.substrate.library.sources._git_helpers import _r

            repo = _r(self.repo_root)
        except Exception as e:  # noqa: BLE001
            logger.warning("in-process ref enumeration failed: %s", e)
            return refs

        # Ref enumeration is the revwalk's POSITIVE SET (see `nodes()`, which
        # passes `cur_refs.values()` to `_collect_commits`), so it must stay
        # independent of `include_remotes`. On a clone -- the shape
        # `external_repos` always produces -- every branch but one lives under
        # `refs/remotes/`, so gating enumeration would drop most reachable
        # commits and make the next completed-scope run retract them.
        # `include_remotes` gates BRANCH-NODE emission only; the probe covers
        # remotes unconditionally to match this walk.
        history_ref_prefixes = ["refs/heads/", "refs/tags/", "refs/remotes/"]
        try:
            ref_names = list(repo.references)
        except Exception as e:  # noqa: BLE001
            logger.warning("in-process ref enumeration failed: %s", e)
            return self._list_refs_via_git(reason="in_proc_ref_list_failed")

        for ref_name in ref_names:
            if not ref_name.startswith(tuple(history_ref_prefixes)):
                continue
            try:
                ref = repo.references[ref_name]
                resolved = ref.resolve()
                raw_refs[ref_name] = str(resolved.target)
                # Peel the direct reference resolve() returned, not `ref`:
                # peeling a symbolic ref resolves it again, and a ref that
                # moved in between would make the walked tip differ from
                # the recorded tip.
                target = resolved.peel(pygit2.Commit)
                refs[ref.name] = str(target.id)
            except (pygit2.GitError, KeyError, TypeError, ValueError) as e:
                logger.debug(
                    "skipping git history ref %s that does not peel to a commit: %s",
                    ref_name,
                    e,
                )
                continue
            except Exception as e:  # noqa: BLE001
                logger.debug(
                    "skipping unreadable git history ref %s: %s",
                    ref_name,
                    e,
                )
                continue
        if repo.head_is_detached:
            try:
                head_sha = str(repo.head.peel(pygit2.Commit).id)
                refs["HEAD"] = head_sha
                raw_refs["HEAD"] = head_sha
            except (pygit2.GitError, KeyError, TypeError, ValueError) as e:
                logger.debug("skipping unreadable detached HEAD: %s", e)
        if not refs:
            return self._list_refs_via_git(reason="in_proc_ref_list_empty")
        self._raw_ref_bindings = raw_refs
        self._ref_snapshot_exact = True
        return refs

    def _list_refs_via_git(self, *, reason: str) -> dict[str, str]:
        refs: dict[str, str] = {}
        raw_refs: dict[str, str] = {}
        self._ref_snapshot_exact = False
        try:
            # Same positive-set reasoning as `_list_refs`: enumeration is
            # unconditional, `include_remotes` gates branch-node emission only.
            prefixes = ["refs/heads", "refs/tags", "refs/remotes"]
            out = run_git(
                self.repo_root,
                [
                    "for-each-ref",
                    "--format=%(refname)%00%(objecttype)%00%(objectname)%00%(*objecttype)%00%(*objectname)",
                    *prefixes,
                ],
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("git ref enumeration fallback failed after %s: %s", reason, e)
            return refs

        for line in out.splitlines():
            ref_name, object_type, object_sha, peeled_type, peeled_sha = (
                line.split("\0") + ["", "", "", "", ""]
            )[:5]
            if not ref_name:
                continue
            if object_sha:
                raw_refs[ref_name] = object_sha
            if ref_name.startswith(("refs/heads/", "refs/remotes/")):
                if object_type == "commit" and object_sha:
                    refs[ref_name] = object_sha
                else:
                    logger.debug(
                        "skipping git history ref %s with non-commit object type %s",
                        ref_name,
                        object_type,
                    )
                continue
            if ref_name.startswith("refs/tags/"):
                if object_type == "tag" and peeled_type == "commit" and peeled_sha:
                    refs[ref_name] = peeled_sha
                elif object_type == "commit" and object_sha:
                    refs[ref_name] = object_sha
                else:
                    logger.debug(
                        "skipping git history tag %s that does not peel to a commit",
                        ref_name,
                    )
        try:
            run_git(self.repo_root, ["symbolic-ref", "-q", "HEAD"])
        except Exception:  # noqa: BLE001 - nonzero is the detached-HEAD signal.
            try:
                head_sha = run_git(
                    self.repo_root,
                    ["rev-parse", "--verify", "HEAD^{commit}"],
                ).strip()
            except Exception as e:  # noqa: BLE001
                logger.debug("skipping unreadable detached HEAD: %s", e)
            else:
                if head_sha:
                    refs["HEAD"] = head_sha
                    raw_refs["HEAD"] = head_sha
        self._raw_ref_bindings = raw_refs
        self._ref_snapshot_exact = True
        return refs

    def _partition_refs(
        self, refs: dict[str, str]
    ) -> tuple[dict[str, str], dict[str, str], dict[str, tuple[str, bool]]]:
        """Partition refs into (local_branches, remotes, tags).

        Per `add-pygit2-in-process-git-helpers`: tag annotation
        status is read from the underlying git object via pygit2.
        Annotated tags (`pygit2.Tag` objects) get is_annotated=True;
        lightweight tags point directly at commits.
        """
        local: dict[str, str] = {}
        remote: dict[str, str] = {}
        tags: dict[str, tuple[str, bool]] = {}
        for ref, sha in refs.items():
            if ref.startswith("refs/heads/"):
                local[ref] = sha
            elif ref.startswith("refs/remotes/"):
                remote[ref] = sha
            elif ref.startswith("refs/tags/"):
                # Ref enumeration captured both the raw object target and its
                # immutable peeled commit. Derive annotation from that same
                # capture instead of reopening the live ref after the fact.
                raw_target = self._raw_ref_bindings.get(ref, sha)
                tags[ref] = (sha, raw_target != sha)
        return local, remote, tags

    # --- Commit walking ------------------------------------------------------

    def _collect_commits(
        self,
        prior_seen: set[str],
        cur_ref_shas: Iterable[str],
        prior_refs: dict[str, str],
        *,
        full_walk: bool = False,
    ) -> Iterable[_CommitRecord]:
        """Walk new commits since last run.

        Per `add-pygit2-in-process-git-helpers`: in-process commit
        walker via libgit2. Cold start walks every commit reachable
        from current refs; incremental walks only commits newly
        reachable since the prior cursor. `history_since` is
        applied client-side after the walk (libgit2's walker
        doesn't accept a since-filter directly). ``full_walk`` makes a
        run with a prior cursor walk like a cold start (the message
        schema upgrade re-walk).
        """
        from okg.substrate.library.sources._git_helpers import _r

        is_cold_start = full_walk or not prior_refs
        cur_shas = sorted(set(cur_ref_shas))
        if not cur_shas:
            return []
        try:
            _r(self.repo_root)
        except Exception as e:
            self._history_read_exact = False
            raise RuntimeError(
                "git history repository could not be opened; "
                "completion authority is unavailable"
            ) from e
        self._history_read_exact = True

        # Build positive + negative ref sets
        if is_cold_start:
            # Walk everything reachable from current refs; history_since
            # filter applied below.
            positive = cur_shas
            negative: list[str] = []
        else:
            positive = cur_shas
            negative = sorted(set(prior_refs.values()))

        # Compile a since-cutoff timestamp if requested
        since_ts: int | None = None
        if is_cold_start and self.history_since:
            since_ts = _parse_since(self.history_since)

        # Only a diffs-on walk passes the patch options, so a diffs-off walk
        # calls the walker exactly as before.
        patch_kwargs: dict[str, Any] = (
            {"with_patches": True, "patches_since": self._diffs_since_ts}
            if self.store_diffs
            else {}
        )
        records: list[_CommitRecord] = []
        try:
            for rec in in_proc_walk_commits(
                self.repo_root,
                positive,
                negative,
                detect_renames=self.detect_renames,
                **patch_kwargs,
            ):
                if since_ts is not None:
                    # Filter by committer time. Strict ">" matches the
                    # legacy `--since=YYYY-MM-DD` semantics (date-only).
                    try:
                        c_time = int(
                            datetime.fromisoformat(rec.committer_iso).timestamp()
                        )
                    except (ValueError, TypeError):
                        c_time = 0
                    if c_time < since_ts:
                        continue
                records.append(self._commit_record(rec))
        except (pygit2.GitError, KeyError) as e:
            # Most likely cause on incremental: a negative sha is no
            # longer reachable after force-push or gc. Fall back to a
            # full reachable walk minus already-seen.
            logger.warning(
                "Incremental in-proc walk failed (%s); falling back to full walk",
                e,
            )
            records = []
            try:
                for rec in in_proc_walk_commits(
                    self.repo_root,
                    cur_shas,
                    (),
                    detect_renames=self.detect_renames,
                    **patch_kwargs,
                ):
                    if rec.sha in prior_seen:
                        continue
                    records.append(self._commit_record(rec))
            except Exception as full_walk_error:  # noqa: BLE001
                self._history_read_exact = False
                raise RuntimeError(
                    "git history incremental and full walks failed; "
                    "completion authority is unavailable"
                ) from full_walk_error
        return records

    # --- Status decoding -----------------------------------------------------

    @staticmethod
    def _decode_status(
        status: str, new_path: str, old_path: str | None
    ) -> tuple[str, str, str | None]:
        """Decode a name-status row → (change_type, target_path, paired_delete_path).

        For renames we emit AFFECTS to the new path with change_type=renamed
        AND a paired AFFECTS to the old path with change_type=deleted, so
        queries can find both ends.
        """
        s = status[:1]
        if s == "A":
            return "added", new_path, None
        if s == "M" or s == "T":
            return "modified", new_path, None
        if s == "D":
            return "deleted", new_path, None
        if s == "R":
            return "renamed", new_path, old_path
        if s == "C":
            return "added", new_path, None  # copy: treat new path as added
        return "modified", new_path, None
