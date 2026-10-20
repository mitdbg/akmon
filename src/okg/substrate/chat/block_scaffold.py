"""Write a deployment's ``chat:`` block from answers (issue #2288).

No shipped profile declares a ``chat:`` block, and every chat verb refuses a
deployment without one. Before this module the only way through was to
hand-edit ``deployment.yaml``, which the Sprint 14 usability rule (#1789,
#1795) fails. ``okg chat setup`` now takes the answers as flags -- provider,
model, ports, search scope -- and this module turns them into a block that the
existing loader (:func:`okg.substrate.deployments.chat_block.parse_chat_block`)
accepts, then writes it.

Rules:

* **References, never values.** The provider credential is recorded as the
  NAME of an environment variable (``chat.provider.api_key_env``). Nothing
  here reads any environment variable's value, so no credential can reach the
  file. A ``--provider-key-env`` answer that looks like a key rather than a
  name is refused before anything else runs, and the answer itself is never
  repeated in an error, a note, or a conflict report
  (:func:`key_env_refusal`). Whether the named variable is set is checked by
  name only, and reported as a warning.
* **Validated before written.** The composed manifest is parsed by the same
  loaders every chat verb and every search call use. A block they refuse is
  never written.
* **Append, not re-dump.** New top-level keys are appended to the existing
  text, so every other byte of the manifest (comments and CRLF line endings
  included) survives. A file whose last value would change when a final
  newline is added, or that cannot take an appended key (a ``...`` end
  marker, several documents, a flow-style root), is refused with a typed
  error instead.
* **Idempotent, never overwriting.** A manifest that already declares a
  ``chat:`` block is left untouched. Answers that agree with it are a no-op;
  an answer that disagrees refuses, naming the field, rather than rewriting
  the operator's configuration. Every answer field is compared
  (:data:`ANSWER_FLAGS`); none is dropped silently.
* **Nothing guessed silently.** Ports come from a flag or from the port the
  install recorded (``profile.init_answers.mcp_port``); the search scope from
  a flag, the manifest's own profiles, or the extraction module's document
  subtypes. Every choice is reported with where it came from, and so are
  the searchable record types the deployment's modules declare that the
  chosen search profile leaves out.
"""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qsl, unquote, urlsplit

import yaml

from okg.substrate.errors import SubstrateError

#: Where the scaffolded system prompt lives, relative to the deployment dir.
SYSTEM_PROMPT_REF = "chat/system_prompt.md"
#: The search profile the scaffold declares when the manifest declares none.
SCAFFOLD_SEARCH_PROFILE = "chat"
#: The extraction module's searchable subtypes. Every shipped profile loads
#: that module, and the guided local-folder source writes exactly these.
DOCUMENT_SUBTYPES: tuple[str, ...] = ("document", "document_chunk")
EXTRACTION_MODULE = "extraction"
#: The read tools a chat preset is given (design.md section 4).
DEFAULT_TOOLS: tuple[str, ...] = (
    "search", "inspect", "expand", "filter", "map", "aggregate", "query",
)

PROVIDER_OPENAI = "openai"
PROVIDER_OLLAMA = "ollama"
PROVIDER_COMPATIBLE = "openai-compatible"


@dataclass(frozen=True)
class ProviderPreset:
    base_url: str | None
    api_key_env: str | None


#: Named providers. ``openai-compatible`` has no base URL of its own, so it
#: needs ``--provider-base-url``.
PROVIDER_PRESETS: Mapping[str, ProviderPreset] = {
    PROVIDER_OPENAI: ProviderPreset("https://api.openai.com/v1", "OPENAI_API_KEY"),
    # The instance runs in a container; a host-local Ollama is reached through
    # the host gateway alias `okg chat setup` gives the container.
    PROVIDER_OLLAMA: ProviderPreset("http://host.docker.internal:11434/v1", None),
    PROVIDER_COMPATIBLE: ProviderPreset(None, None),
}

DEFAULT_SYSTEM_PROMPT = """\
You answer questions about this deployment's knowledge graph.

Use the graph tools (search, inspect, expand, filter, map, aggregate, query)
to find the answer before you reply. Cite the records you used and the
generation the answer comes from. If the graph holds nothing relevant, say so
instead of answering from general knowledge.
"""

DEFAULT_SUGGESTION = "What kinds of records does this graph hold?"

STATUS_WRITTEN = "written"
STATUS_UNCHANGED = "unchanged"

#: What an error says in place of a ``--provider-key-env`` answer. The answer
#: is never repeated: it may be the key itself.
WITHHELD_KEY_ENV_ANSWER = "the value given for --provider-key-env"
WITHHELD = "<withheld>"

#: Prefixes that provider and platform keys start with. A value that starts
#: with one and runs on for at least ``_KEY_PREFIX_MIN_TAIL`` more characters
#: is taken for a key, not a variable name -- but for a prefix a variable
#: name could also start with (letters, digits and ``_`` only), only when
#: the rest has no ``_``: real keys after such a prefix are letters and
#: digits, while ``hf_inference_api_key`` or ``ASIA_PACIFIC_OPENAI_KEY`` are
#: names. Hyphenated prefixes would also fail the name pattern; they are
#: listed so the refusal says what it saw.
KEY_PREFIXES: tuple[str, ...] = (
    "sk-", "sk_", "sk_live_", "sk_test_", "pk-", "pk_", "pk_live_",
    "pk_test_", "rk_", "rk_live_", "rk_test_", "gsk_", "xai-", "AIza",
    "hf_", "ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_", "glpat-",
    "xoxb-", "xoxp-", "AKIA", "ASIA", "r8_", "pplx-", "nvapi-", "fw_",
    "csk-", "tgp_",
)
#: Prefixes whose keys do carry ``_`` after the prefix.
_PREFIXES_WITH_UNDERSCORE_TAIL: frozenset[str] = frozenset({"github_pat_"})
_KEY_PREFIX_MIN_TAIL = 12
#: One CamelCase or snake piece: an acronym, a capitalised or lower-case
#: word, or a number.
_WORD_PIECE_RE = re.compile(r"[A-Z]+(?![a-z])|[A-Z][a-z]+|[a-z]+|[0-9]+")
_HEX_RE = re.compile(r"[0-9a-fA-F]+")
#: Words average at least this many characters a piece; random tokens,
#: which switch case every character or two, do not.
_WORD_MIN_AVERAGE = 4
#: Longest ``--provider-key-env`` accepted. Real variable names are short.
KEY_ENV_MAX_LENGTH = 64
#: A value of at least this many characters with upper and lower case, no
#: underscore, and pieces that do not read as words is a token, not a name.
_TOKEN_MIN_LENGTH = 24
#: At this length a digit or mixed case is enough (base62 and hex keys);
#: CamelCase words are still names, and 32+ hex digits never are.
_TOKEN_LONG_LENGTH = 32

_KEY_ENV_SHAPE_TEXT: Mapping[str, str] = {
    "provider_key_prefix": "starts like a provider key",
    "not_a_var_name": "is not a valid environment-variable name",
    "too_long": f"is longer than {KEY_ENV_MAX_LENGTH} characters",
    "token_like": "looks like a generated token (long, letters and digits, no underscores)",
}

#: Every :class:`ChatAnswers` field and the flag that carries it. On a
#: deployment that already declares ``chat:``, :func:`_conflicts` compares
#: each one; a test holds this equal to the dataclass's fields so a new
#: answer cannot be dropped silently.
ANSWER_FLAGS: Mapping[str, str] = {
    "provider": "--provider",
    "provider_base_url": "--provider-base-url",
    "provider_key_env": "--provider-key-env",
    "model": "--model",
    "mcp_port": "--mcp-port",
    "instance_port": "--instance-port",
    "search_profile": "--scope",
    "search_subtypes": "--search-subtype",
}


class ChatBlockScaffoldError(SubstrateError):
    """The answers cannot be turned into a valid ``chat:`` block.

    ``data['reason']`` names why; ``data['flag']`` names the flag to supply or
    correct where one exists.
    """

    code = "chat_block_scaffold_refused"


@dataclass(frozen=True)
class ChatAnswers:
    """What the operator told ``okg chat setup``. Names, never secret values."""

    provider: str | None = None
    provider_base_url: str | None = None
    provider_key_env: str | None = None
    model: str | None = None
    mcp_port: int | None = None
    instance_port: int | None = None
    search_profile: str | None = None
    search_subtypes: tuple[str, ...] = ()

    @property
    def given(self) -> bool:
        """True when any chat-block answer was supplied.

        ``instance_port`` alone does not count: it is also the existing
        one-off override flag of ``okg chat setup``.
        """
        return any((
            self.provider, self.provider_base_url, self.provider_key_env,
            self.model, self.mcp_port is not None, self.search_profile,
            self.search_subtypes,
        ))


@dataclass
class ScaffoldResult:
    status: str
    manifest: str
    chat_block: dict[str, Any] = field(default_factory=dict)
    written: list[str] = field(default_factory=list)
    decisions: dict[str, str] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    #: Things the operator should act on that do not stop the step.
    warnings: list[str] = field(default_factory=list)
    #: Which searchable record types chat covers (see :func:`search_coverage`).
    coverage: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "manifest": self.manifest,
            "chat_block": _shown_block(self.chat_block),
            "written": list(self.written),
            "decisions": dict(self.decisions),
            "notes": list(self.notes),
            "warnings": list(self.warnings),
            "coverage": dict(self.coverage),
        }


def _refuse(message: str, *, reason: str, fix_hint: str,
            flag: str | None = None, **data: Any) -> ChatBlockScaffoldError:
    payload: dict[str, Any] = {"reason": reason, **data}
    if flag:
        payload["flag"] = flag
    return ChatBlockScaffoldError(message, data=payload, fix_hint=fix_hint)


def _has_key_prefix(value: str) -> bool:
    """True when ``value`` starts like a provider key (see KEY_PREFIXES)."""
    for prefix in KEY_PREFIXES:
        if not value.startswith(prefix):
            continue
        tail = value[len(prefix):]
        if len(tail) < _KEY_PREFIX_MIN_TAIL:
            continue
        name_like = "-" not in prefix and prefix not in _PREFIXES_WITH_UNDERSCORE_TAIL
        if name_like and "_" in tail:
            continue
        return True
    return False


def _reads_as_words(value: str) -> bool:
    """True for CamelCase words such as ``LiteLLMMasterKey2024Prod``.

    Every letter run splits into pieces of at least two letters, one of them
    is a word of four or more letters, and the pieces average four
    characters. Generated tokens switch case at random, so they leave short
    pieces (``aZ3kQ9`` -> a, Z, 3, k, Q, 9).
    """
    pieces = _WORD_PIECE_RE.findall(value)
    if "".join(pieces) != value:
        return False
    letters = [p for p in pieces if not p.isdigit()]
    return (
        all(len(p) >= 2 for p in letters)
        and any(len(p) >= 4 and p[1:].islower() for p in letters)
        and len(value) >= _WORD_MIN_AVERAGE * len(pieces)
    )


def key_env_refusal(value: str) -> str | None:
    """Why ``value`` cannot be the NAME of a variable, or None.

    Returns a shape code, never the value. Realistic names pass
    (``MY_KEY``, ``OPENAI_API_KEY``, ``hf_inference_api_key``,
    ``LiteLLMMasterKey2024Prod``); a known key prefix followed by a key-like
    tail, anything over :data:`KEY_ENV_MAX_LENGTH` characters, a long run of
    hex digits, and a long mixed token that does not read as words are
    refused.
    """
    from okg.substrate.mcp.http_auth import _ENV_VAR_NAME_RE

    if _has_key_prefix(value):
        return "provider_key_prefix"
    if not _ENV_VAR_NAME_RE.match(value):
        return "not_a_var_name"
    if len(value) > KEY_ENV_MAX_LENGTH:
        return "too_long"
    if "_" not in value:
        has_digit = any(c.isdigit() for c in value)
        mixed = any(c.isupper() for c in value) and any(c.islower() for c in value)
        if len(value) >= _TOKEN_LONG_LENGTH and has_digit and _HEX_RE.fullmatch(value):
            return "token_like"
        long_enough = (mixed and len(value) >= _TOKEN_MIN_LENGTH) or (
            (has_digit or mixed) and len(value) >= _TOKEN_LONG_LENGTH
        )
        if long_enough and not _reads_as_words(value):
            return "token_like"
    return None


#: Query, fragment or path parameter names that carry a credential. Matched
#: case-insensitively, and also as the last ``-``/``_`` part or the last
#: camelCase word of a longer name (``x-api-key``, ``client_secret``,
#: ``accessToken``, ``dbPwd``). Azure's ``api-version`` is not one of them.
CREDENTIAL_PARAM_NAMES: frozenset[str] = frozenset({
    "key", "api_key", "apikey", "api-key", "token", "access_token", "secret",
    "password", "sig", "signature", "auth", "code",
    # camelCase and common spellings, compared lower-cased (#2735 review).
    "accesstoken", "apitoken", "authtoken", "sessiontoken", "jwt", "bearer",
    "credential", "credentials", "passwd", "pwd",
})
_CREDENTIAL_PARAM_SUFFIXES: tuple[str, ...] = (
    "key", "token", "secret", "password", "signature", "passwd", "pwd",
    "credential", "credentials",
)
#: One camelCase word: an acronym, a capitalised or lower-case word, a number.
_CAMEL_WORD_RE = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|[0-9]+")
#: Longest base URL accepted. Decoding and scanning are linear, but nothing
#: real comes near this, and a longer value is refused rather than scanned.
BASE_URL_MAX_LENGTH = 2048

_BASE_URL_SHAPE_TEXT: Mapping[str, str] = {
    "not_a_url": "is not a URL",
    "not_http": "does not start with http:// or https://",
    "no_host": "names no host",
    "has_credentials": "carries a user name or password",
    "credential_parameter": "carries a credential-named query or path parameter",
    "key_shaped_part": "has a part that starts like a provider key",
    "token_shaped_part": "has a path part that looks like a generated token",
    "too_long": f"is longer than {BASE_URL_MAX_LENGTH} characters",
}
_BASE_URL_CREDENTIAL_SHAPES: frozenset[str] = frozenset({
    "has_credentials", "credential_parameter", "key_shaped_part",
    "token_shaped_part",
})

#: A path part is token-shaped (#2735) when, after percent-decoding, it holds
#: either run below. Both need 40 characters: a real base URL's longest
#: machine-made parts are 32 (a UUID without dashes, a Cloudflare account ID
#: in ``/client/v4/accounts/<id>/ai/v1`` or ``gateway.ai.cloudflare.com/v1/
#: <id>/...``), and a SHA-1 hex digest or a base62 key is 40 or more.
#:
#: * 40 or more hex digits in a row.
#: * 40 or more base64url characters (letters, digits, ``-``, ``_``) in a
#:   row that mix upper case, lower case and digits, where lower-case runs of
#:   :data:`_TOKEN_WORD_MIN` or more letters -- words -- are under
#:   :data:`_TOKEN_WORD_SHARE` of the letters. Long deployment and model
#:   names (``MyCompanyGPT4oProductionEastUS2024Deploy``,
#:   ``Llama3p1x70BInstructAWQ4bitVllmServingEndpointA1``) are mostly words;
#:   a random base62 token of 40 characters is caught about 94 times in 100,
#:   and of 84 characters about 98 times in 100.
#:
#: A run that ``-`` or ``_`` splits into :data:`_NAME_MIN_PIECES` or more
#: pieces of at most :data:`_NAME_MAX_PIECE` characters is a name, not a
#: token: Azure deployment names such as
#: ``o3-mini-2025-01-31-DataZone-Std-EUW-Prod2`` (#2735 review). A random
#: base64url token of 40 characters has that shape about once in 100, and
#: of 64 characters or more almost never.
#:
#: Not caught: a lower-case-only or upper-case-only token that is not hex,
#: and any token shorter than 40 characters (a 32-digit hex Azure key among
#: them), unless it starts with a :data:`KEY_PREFIXES` prefix.
_TOKEN_MIN_RUN = 40
_TOKEN_HEX_RUN_RE = re.compile(r"[0-9a-fA-F]{%d,}" % _TOKEN_MIN_RUN)
_TOKEN_B64URL_RUN_RE = re.compile(r"[A-Za-z0-9_-]{%d,}" % _TOKEN_MIN_RUN)
_TOKEN_WORD_MIN = 5
_TOKEN_WORD_RE = re.compile(r"[a-z]{%d,}" % _TOKEN_WORD_MIN)
_TOKEN_WORD_SHARE = 0.2
_NAME_MIN_PIECES = 4
_NAME_MAX_PIECE = 12
#: Where a provider key may start inside a decoded path part: its start, or
#: just after any character that is not a letter or digit (``/x%2Fsk-``,
#: ``/.sk-``, ``/key-sk-``, ``/%00sk-``).
_KEY_BOUNDARY_RE = re.compile(r"(?:^|(?<=[^A-Za-z0-9]))(?=[A-Za-z])")
#: The key-like run after a prefix found inside a part: base64url characters.
_KEY_TAIL_RUN_RE = re.compile(r"[A-Za-z0-9_-]*")


def _credential_param(name: str) -> bool:
    stripped = name.strip()
    lowered = stripped.lower()
    if lowered in CREDENTIAL_PARAM_NAMES:
        return True
    last = re.split(r"[-_]", lowered)[-1]
    if len(lowered) > len(last) and last in _CREDENTIAL_PARAM_SUFFIXES:
        return True
    words = _CAMEL_WORD_RE.findall(re.split(r"[-_]", stripped)[-1])
    return len(words) > 1 and words[-1].lower() in _CREDENTIAL_PARAM_SUFFIXES


def _decoded(text: str) -> str:
    """``text`` with percent-encoding removed until none is left.

    Repeated, so ``%2573k-`` reads as ``sk-``. Each round that changes the
    text shortens it, so the loop ends.
    """
    while True:
        plain = unquote(text)
        if plain == text:
            return plain
        text = plain


def _path_params(segment: str) -> list[tuple[str, str]]:
    """The ``name=value`` parameters of one decoded path segment.

    ``urlsplit`` leaves ``;name=value`` in the path, so these are read here:
    every ``;`` piece, and the segment's own head when it holds ``=``.
    """
    head, *rest = segment.split(";")
    pieces = [*rest, head] if "=" in head else rest
    return [(name, value) for name, _, value in (p.partition("=") for p in pieces)]


def _token_shaped(text: str) -> bool:
    """True when ``text`` holds a token-shaped run (see _TOKEN_MIN_RUN)."""
    if _TOKEN_HEX_RUN_RE.search(text):
        return True
    for run in _TOKEN_B64URL_RUN_RE.findall(text):
        if not (
            any(c.isupper() for c in run)
            and any(c.islower() for c in run)
            and any(c.isdigit() for c in run)
        ):
            continue
        pieces = [piece for piece in re.split(r"[-_]", run) if piece]
        if len(pieces) >= _NAME_MIN_PIECES and max(map(len, pieces)) <= _NAME_MAX_PIECE:
            continue
        letters = sum(c.isalpha() for c in run)
        words = sum(len(word) for word in _TOKEN_WORD_RE.findall(run))
        if words < _TOKEN_WORD_SHARE * letters:
            return True
    return False


def _key_tail_is_token_like(tail: str) -> bool:
    """True when the run after a key prefix found mid-part looks generated.

    A prefix inside a part (``/key-sk-...``) needs more than the prefix
    shape: the run after it must hold a digit or mix cases, and must not
    read as words, so ``/task-skills-...`` or ``/desk-...`` stay names.
    """
    run = _KEY_TAIL_RUN_RE.match(tail)
    body = run.group(0) if run else ""
    alnum = "".join(c for c in body if c.isalnum())
    if len(alnum) < _KEY_PREFIX_MIN_TAIL:
        return False
    has_digit = any(c.isdigit() for c in alnum)
    mixed = any(c.isupper() for c in alnum) and any(c.islower() for c in alnum)
    return (has_digit or mixed) and not _reads_as_words(alnum)


def _holds_key(text: str) -> bool:
    """True when ``text`` starts like a provider key, or holds one after a boundary.

    At the start the :func:`_has_key_prefix` shape is enough, as before; at
    any later boundary (:data:`_KEY_BOUNDARY_RE`) the run after the prefix
    must also look generated (:func:`_key_tail_is_token_like`) (#2735
    review).
    """
    if _has_key_prefix(text):
        return True
    for match in _KEY_BOUNDARY_RE.finditer(text):
        start = match.start()
        if start == 0:
            continue
        rest = text[start:]
        if not _has_key_prefix(rest):
            continue
        prefix = next(p for p in sorted(KEY_PREFIXES, key=len, reverse=True) if rest.startswith(p))
        if _key_tail_is_token_like(rest[len(prefix):]):
            return True
    return False


def base_url_refusal(value: str) -> str | None:
    """Why ``value`` cannot be a provider base URL, or None.

    Returns a shape code (:data:`_BASE_URL_SHAPE_TEXT`), never the value:
    a refused URL may be, or carry, the key itself. Accepted: ``http(s)``
    with a host, no ``user:password@``, no credential-named query, fragment
    or ``;`` path parameter, no part that starts like a provider key, and no
    token-shaped path part. Every check reads the percent-decoded text, so
    ``%73k-`` is ``sk-`` (#2735). A key prefix counts at any boundary inside
    a part, not only at its start (:func:`_holds_key`). A value longer than
    :data:`BASE_URL_MAX_LENGTH` is refused before it is decoded.
    """
    if value and len(value) > BASE_URL_MAX_LENGTH:
        return "too_long"
    if not value or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value):
        return "not_a_url"
    try:
        parts = urlsplit(value)
        host = parts.hostname
        parts.port  # noqa: B018 -- raises ValueError on a malformed port
    except ValueError:
        return "not_a_url"
    if parts.scheme.lower() not in ("http", "https") or not parts.netloc:
        return "not_http"
    if not host:
        return "no_host"
    if parts.username is not None or parts.password is not None:
        return "has_credentials"
    segments = [_decoded(segment) for segment in parts.path.split("/") if segment]
    params = [
        (_decoded(name), _decoded(param))
        for name, param in (
            *parse_qsl(parts.query, keep_blank_values=True),
            *parse_qsl(parts.fragment, keep_blank_values=True),
            *(pair for segment in segments for pair in _path_params(segment)),
        )
    ]
    if any(_credential_param(name) for name, _ in params):
        return "credential_parameter"
    pieces = [
        *(segment.split(";")[0] for segment in segments),
        *(param for _, param in params),
        *(name for name, _ in params),
    ]
    if any(_holds_key(piece) for piece in pieces):
        return "key_shaped_part"
    if any(_token_shaped(segment) for segment in segments):
        return "token_shaped_part"
    return None


def _segment_hidden(segment: str) -> bool:
    """True when :func:`masked_base_url` withholds this raw path segment.

    A segment carrying a ``;`` or ``=`` parameter is withheld whole, as a
    query is; so is one that, raw or percent-decoded, holds a provider key
    (:func:`_holds_key`) or a token-shaped run.
    """
    plain = _decoded(segment)
    return (
        ";" in plain
        or "=" in plain
        or _holds_key(segment)
        or _holds_key(plain)
        or _token_shaped(plain)
    )


def masked_base_url(value: Any) -> str:
    """``value`` as it may be printed: a provider base URL with no credential.

    Every okg output that shows a provider base URL goes through this. It
    drops ``user:password@``, and withholds the query, the fragment and any
    path segment :func:`_segment_hidden` names. A URL with none of these
    comes back unchanged; one that does not parse, or is not a string, is
    withheld whole, and so is one longer than :data:`BASE_URL_MAX_LENGTH`.
    What is written to the instance is never masked.
    """
    if not isinstance(value, str) or not value or len(value) > BASE_URL_MAX_LENGTH:
        return WITHHELD
    try:
        parts = urlsplit(value)
    except ValueError:
        return WITHHELD
    if not parts.scheme or not parts.netloc:
        return WITHHELD
    _, at, hostport = parts.netloc.rpartition("@")
    netloc = f"{WITHHELD}@{hostport}" if at else hostport
    path = "/".join(
        WITHHELD if _segment_hidden(segment) else segment
        for segment in parts.path.split("/")
    )
    shown = f"{parts.scheme}://{netloc}{path}"
    if parts.query:
        shown += f"?{WITHHELD}"
    if parts.fragment:
        shown += f"#{WITHHELD}"
    return shown


def base_url_masked_parts(value: Any) -> list[str]:
    """The pieces :func:`masked_base_url` hides, plus the whole value.

    A caller scrubs these by identity from text it did not write -- a vendor
    error that echoes the URL it was given, raw or percent-decoded. Empty
    when nothing is hidden.
    """
    if not isinstance(value, str) or not value or masked_base_url(value) == value:
        return []
    out = [value]
    if len(value) > BASE_URL_MAX_LENGTH:
        return out
    try:
        parts = urlsplit(value)
    except ValueError:
        return out
    userinfo, at, _ = parts.netloc.rpartition("@")
    if at and userinfo:
        out.append(userinfo)
    hidden = [
        *(piece for piece in (parts.query, parts.fragment) if piece),
        *(seg for seg in parts.path.split("/") if seg and _segment_hidden(seg)),
    ]
    for piece in hidden:
        out.append(piece)
        plain = _decoded(piece)
        if plain != piece:
            out.append(plain)
    return out


def _shown_url(value: Any) -> Any:
    """A declared base URL as :func:`masked_base_url` shows it.

    WITHHELD when it may carry a credential; None stays None. A URL that
    passes the refusal still goes through the one masking helper, so a query
    such as ``?deployment=...`` is never printed (#2344).
    """
    if isinstance(value, str) and base_url_refusal(value) is None:
        return masked_base_url(value)
    return WITHHELD if value is not None else None


def _shown_block(block: Mapping[str, Any]) -> dict[str, Any]:
    """``block`` as it may be printed: its provider base URL masked.

    The written block keeps the raw URL; only this rendering (the configure
    step's ``--json`` data) goes through :func:`masked_base_url`.
    """
    shown = dict(block)
    provider = shown.get("provider")
    if isinstance(provider, Mapping) and "base_url" in provider:
        shown["provider"] = {**provider, "base_url": _shown_url(provider["base_url"])}
    return shown


def secret_answers(answers: ChatAnswers) -> list[str]:
    """Answers that may carry a credential and must be scrubbed by identity.

    ``okg chat setup`` adds these to what it redacts from every error.
    """
    out: list[str] = []
    if answers.provider_key_env and key_env_refusal(answers.provider_key_env):
        out.append(answers.provider_key_env)
    if answers.provider_base_url is not None and base_url_refusal(answers.provider_base_url):
        out.append(answers.provider_base_url)
    return out


def validate_answers(answers: ChatAnswers) -> None:
    """Refuse answers that may carry a credential. Runs before anything else.

    The refusal names the flag and the shape it saw, never the value.
    """
    key_env = answers.provider_key_env
    if key_env is not None:
        shape = key_env_refusal(key_env)
        if shape is not None:
            raise _refuse(
                "--provider-key-env takes the NAME of the variable that holds "
                f"the provider key, and {WITHHELD_KEY_ENV_ANSWER} "
                f"{_KEY_ENV_SHAPE_TEXT[shape]}. It is withheld here because "
                "it may be the key itself",
                reason="provider_key_env_not_a_name", flag="--provider-key-env",
                path="chat.provider.api_key_env", observed=WITHHELD, shape=shape,
                fix_hint=(
                    "put the key in a variable first (export MY_PROVIDER_KEY=<key>), "
                    "then pass the name: --provider-key-env MY_PROVIDER_KEY"
                ),
            )
    base_url = answers.provider_base_url
    shape = base_url_refusal(base_url) if base_url is not None else None
    if shape is not None:
        credential = shape in _BASE_URL_CREDENTIAL_SHAPES
        raise _refuse(
            f"the value given for --provider-base-url {_BASE_URL_SHAPE_TEXT[shape]}. "
            "It is withheld here because it may be, or carry, a credential",
            reason=(
                "provider_base_url_has_credentials" if credential
                else "provider_base_url_not_a_url"
            ),
            flag="--provider-base-url", path="chat.provider.base_url",
            observed=WITHHELD, shape=shape,
            fix_hint=(
                "pass the provider API base alone, e.g. "
                "--provider-base-url https://api.example.invalid/v1 (no "
                "user:password@, no key in the query), and name the variable "
                "that holds the key with --provider-key-env"
                if credential else
                "pass an http:// or https:// URL with a host, e.g. "
                "--provider-base-url http://host.docker.internal:8000/v1"
            ),
        )


def _preset(name: str) -> ProviderPreset:
    preset = PROVIDER_PRESETS.get(name)
    if preset is None:
        raise _refuse(
            f"--provider {name!r} is not a provider this command knows",
            reason="provider_unknown", flag="--provider",
            requested=name, known=sorted(PROVIDER_PRESETS),
            fix_hint=(
                f"pass one of {sorted(PROVIDER_PRESETS)}; any other "
                f"OpenAI-compatible server is --provider {PROVIDER_COMPATIBLE} "
                "--provider-base-url <url>"
            ),
        )
    return preset


def _resolve_provider(answers: ChatAnswers) -> tuple[dict[str, Any], str]:
    name = answers.provider
    if name is None:
        if not answers.provider_base_url:
            raise _refuse(
                "no model provider was named, so the chat block cannot say "
                "where the model runs",
                reason="provider_missing", flag="--provider",
                fix_hint=(
                    f"pass --provider {PROVIDER_OPENAI}, --provider "
                    f"{PROVIDER_OLLAMA}, or --provider {PROVIDER_COMPATIBLE} "
                    "with --provider-base-url"
                ),
            )
        name = PROVIDER_COMPATIBLE
    preset = _preset(name)
    base_url = answers.provider_base_url or preset.base_url
    if not base_url:
        raise _refuse(
            f"--provider {name} has no base URL of its own",
            reason="provider_base_url_missing", flag="--provider-base-url",
            fix_hint="pass --provider-base-url, e.g. http://host.docker.internal:8000/v1",
        )
    block: dict[str, Any] = {"base_url": base_url}
    key_env = answers.provider_key_env or preset.api_key_env
    if key_env:
        block["api_key_env"] = key_env
    return block, name


def _resolve_mcp_port(answers: ChatAnswers, raw: Mapping[str, Any]) -> tuple[int, str]:
    if answers.mcp_port is not None:
        return int(answers.mcp_port), "flag --mcp-port"
    profile = raw.get("profile")
    recorded = None
    if isinstance(profile, Mapping) and isinstance(profile.get("init_answers"), Mapping):
        recorded = profile["init_answers"].get("mcp_port")
    if isinstance(recorded, int) and not isinstance(recorded, bool):
        return recorded, "profile.init_answers.mcp_port (recorded by okg install)"
    raise _refuse(
        "the graph endpoint port is unknown: no --mcp-port was given and the "
        "install recorded none",
        reason="mcp_port_missing", flag="--mcp-port",
        fix_hint="pass --mcp-port <port> for the deployment's mcp-serve",
    )


def _resolve_search(
    answers: ChatAnswers, raw: Mapping[str, Any],
) -> tuple[str, dict[str, Any] | None, str]:
    """(profile name, search block to ADD or None, where the choice came from)."""
    search = raw.get("search")
    has_search = "search" in raw
    declared: list[str] = []
    if isinstance(search, Mapping) and isinstance(search.get("profiles"), Mapping):
        declared = sorted(str(n) for n in search["profiles"])
    if declared:
        if answers.search_subtypes:
            raise _refuse(
                "the manifest already declares search profiles, so "
                "--search-subtype has no profile to create",
                reason="search_subtypes_conflict", flag="--search-subtype",
                declared=declared,
                fix_hint=f"drop --search-subtype and pass --scope, one of {declared}",
            )
        if answers.search_profile:
            if answers.search_profile not in declared:
                raise _refuse(
                    f"--scope {answers.search_profile!r} is not "
                    f"declared; the manifest declares {declared}",
                    reason="search_profile_undeclared", flag="--scope",
                    requested=answers.search_profile, declared=declared,
                    fix_hint=f"pass one of {declared}",
                )
            return answers.search_profile, None, "flag --scope"
        default = search.get("default_profile") if isinstance(search, Mapping) else None
        if isinstance(default, str) and default in declared:
            return default, None, "search.default_profile"
        if len(declared) == 1:
            return declared[0], None, "the manifest's only search profile"
        raise _refuse(
            f"the manifest declares several search profiles {declared} and "
            "no default; the chat scope is not picked for you",
            reason="search_profile_ambiguous", flag="--scope",
            declared=declared, fix_hint=f"pass --scope, one of {declared}",
        )
    if has_search:
        # A bare `search:` line loads as None; it is still a declared key,
        # and appending a second top-level `search:` would duplicate it.
        raise _refuse(
            "the manifest declares a search: block without profiles; adding "
            "one inside it would rewrite that block",
            reason="search_block_without_profiles",
            fix_hint="declare a profile under search.profiles, then re-run",
        )
    if answers.search_profile:
        raise _refuse(
            f"--scope {answers.search_profile!r} names a profile, and "
            "the manifest declares none",
            reason="search_profile_undeclared", flag="--scope",
            requested=answers.search_profile, declared=[],
            fix_hint="drop --scope; pass --search-subtype to scope the new profile",
        )
    if answers.search_subtypes:
        subtypes = list(dict.fromkeys(answers.search_subtypes))
        origin = "flag --search-subtype"
    else:
        modules = raw.get("modules") or []
        if EXTRACTION_MODULE not in modules:
            raise _refuse(
                "the deployment does not load the extraction module, so there "
                "is no document subtype to scope chat search to",
                reason="search_subtypes_missing", flag="--search-subtype",
                fix_hint="pass --search-subtype <subtype> once per subtype chat should search",
            )
        subtypes = list(DOCUMENT_SUBTYPES)
        origin = "the extraction module's document subtypes"
    block = {"profiles": {SCAFFOLD_SEARCH_PROFILE: {"subtypes": subtypes}}}
    return SCAFFOLD_SEARCH_PROFILE, block, origin


def compose_chat_block(
    deployment: str, raw: Mapping[str, Any], answers: ChatAnswers,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, str]]:
    """(chat block, search block to add or None, decisions). Pure."""
    from okg.substrate.deployments.chat_block import (
        _surface_declares_chat_prompt,
        _surface_declares_chat_tools,
    )

    validate_answers(answers)

    if not answers.model:
        raise _refuse(
            "no model was named, so the chat site would offer nothing",
            reason="model_missing", flag="--model",
            fix_hint="pass --model <id the provider serves>, e.g. gpt-4o-mini",
        )
    if answers.instance_port is None:
        raise _refuse(
            "no port was named for the chat site",
            reason="instance_port_missing", flag="--instance-port",
            fix_hint="pass --instance-port <free host port>, e.g. 3000",
        )
    provider, provider_name = _resolve_provider(answers)
    mcp_port, mcp_origin = _resolve_mcp_port(answers, raw)
    if int(answers.instance_port) == mcp_port:
        raise _refuse(
            f"--instance-port {answers.instance_port} is the graph endpoint's "
            "port; one port cannot serve both",
            reason="instance_port_equals_mcp_port", flag="--instance-port",
            fix_hint="pass a different --instance-port",
        )
    profile, search_block, search_origin = _resolve_search(answers, raw)

    preset: dict[str, Any] = {
        "display_name": f"{deployment} graph",
        "description": "Answers grounded in the published graph.",
    }
    if not _surface_declares_chat_prompt(raw):
        preset["system_prompt_ref"] = SYSTEM_PROMPT_REF
    preset["suggestions"] = [DEFAULT_SUGGESTION]
    mcp: dict[str, Any] = {"port": mcp_port}
    if not _surface_declares_chat_tools(raw):
        mcp["tools"] = list(DEFAULT_TOOLS)
    block: dict[str, Any] = {
        "enabled": True,
        "ui": {"site_name": f"{deployment} graph"},
        "models": {"builtin": [answers.model], "default": answers.model},
        "preset": preset,
        "search_profile": profile,
        "mcp": mcp,
        "provider": provider,
        "instance_port": int(answers.instance_port),
    }
    decisions = {
        "provider": provider_name,
        "mcp_port": mcp_origin,
        "search_profile": search_origin,
        "instance_port": "flag --instance-port",
    }
    return block, search_block, decisions


def _shown_name(value: Any) -> Any:
    """A declared variable name, or WITHHELD when it may be a key."""
    if isinstance(value, str) and key_env_refusal(value) is None:
        return value
    return WITHHELD if value is not None else None


def _declared_profile_subtypes(raw: Mapping[str, Any], profile: Any) -> list[str] | None:
    search = raw.get("search")
    profiles = search.get("profiles") if isinstance(search, Mapping) else None
    entry = profiles.get(profile) if isinstance(profiles, Mapping) else None
    subtypes = entry.get("subtypes") if isinstance(entry, Mapping) else None
    if isinstance(subtypes, (list, tuple)):
        return [str(s) for s in subtypes]
    return None


def _conflicts(
    existing: Mapping[str, Any], raw: Mapping[str, Any], answers: ChatAnswers,
) -> tuple[list[dict[str, Any]], list[str]]:
    """(answers that disagree with an existing block, notes).

    Every field in :data:`ANSWER_FLAGS` is compared. No environment value is
    read, and the ``--provider-key-env`` answer is never repeated.
    """
    out: list[dict[str, Any]] = []
    notes: list[str] = []

    def add(path: str, declared: Any, answered: Any, flag: str) -> None:
        out.append({"path": path, "declared": declared, "answered": answered, "flag": flag})

    def check(path: str, declared: Any, answered: Any, flag: str) -> None:
        if answered is not None and declared != answered:
            add(path, declared, answered, flag)

    def sub(key: str) -> Mapping[str, Any]:
        value = existing.get(key)
        return value if isinstance(value, Mapping) else {}

    provider = sub("provider")
    # A hand-written URL that may carry a credential is compared, never shown.
    declared_url = provider.get("base_url")
    shown_url = _shown_url(declared_url)
    if answers.provider_base_url is not None:
        if declared_url != answers.provider_base_url:
            add("chat.provider.base_url", shown_url,
                masked_base_url(answers.provider_base_url),
                ANSWER_FLAGS["provider_base_url"])
    if answers.provider is not None:
        preset = _preset(answers.provider)
        if preset.base_url is not None:
            if answers.provider_base_url is None:
                if declared_url != preset.base_url:
                    add("chat.provider.base_url", shown_url, preset.base_url,
                        ANSWER_FLAGS["provider"])
        elif answers.provider_base_url is None:
            named = {p.base_url: n for n, p in PROVIDER_PRESETS.items() if p.base_url}
            if declared_url in named:
                add("chat.provider.base_url", shown_url,
                    f"--provider {answers.provider} with no --provider-base-url "
                    f"(the declared URL is the {named[declared_url]} preset)",
                    ANSWER_FLAGS["provider"])
            else:
                notes.append(
                    f"--provider {answers.provider} named no --provider-base-url; "
                    f"the declared base URL {shown_url} is kept"
                )
    if answers.provider_key_env is not None:
        declared_key = provider.get("api_key_env")
        if declared_key != answers.provider_key_env:
            add("chat.provider.api_key_env", _shown_name(declared_key),
                WITHHELD_KEY_ENV_ANSWER, ANSWER_FLAGS["provider_key_env"])
    check("chat.models.default", sub("models").get("default"), answers.model,
          ANSWER_FLAGS["model"])
    check("chat.mcp.port", sub("mcp").get("port"), answers.mcp_port,
          ANSWER_FLAGS["mcp_port"])
    check("chat.instance_port", existing.get("instance_port"),
          answers.instance_port, ANSWER_FLAGS["instance_port"])
    check("chat.search_profile", existing.get("search_profile"),
          answers.search_profile, ANSWER_FLAGS["search_profile"])
    if answers.search_subtypes:
        profile = answers.search_profile or existing.get("search_profile")
        declared_subs = _declared_profile_subtypes(raw, profile)
        answered_subs = list(dict.fromkeys(answers.search_subtypes))
        if declared_subs is None or set(declared_subs) != set(answered_subs):
            add(f"search.profiles.{profile}.subtypes", declared_subs, answered_subs,
                ANSWER_FLAGS["search_subtypes"])
    return out, notes


def _append_text(text: str, additions: Mapping[str, Any], newline: str = "\n") -> str:
    """Append each key as its own block, in the file's own line ending.

    The caller has already refused a file whose final line cannot take a
    newline without changing its value.
    """
    out = text if (not text or text.endswith("\n")) else text + newline
    for key, value in additions.items():
        dumped = yaml.safe_dump({key: value}, sort_keys=False, default_flow_style=False)
        out += dumped.replace("\n", newline) if newline != "\n" else dumped
    return out


def _load(text: str, manifest: Path, *, reason: str, fix_hint: str) -> Any:
    """Parse ``text``; a parser error becomes a typed refusal.

    Only the line and column are reported: the parser's own message quotes
    the file, which may hold values that do not belong in a report.
    """
    from okg.substrate import fastyaml

    try:
        return fastyaml.safe_load(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        location = {}
        if mark is not None:
            location = {"line": mark.line + 1, "column": mark.column + 1}
        raise _refuse(
            f"{manifest} cannot be read as one YAML mapping "
            f"({type(exc).__name__}"
            + (f" at line {location['line']}" if location else "")
            + "); it is left untouched",
            reason=reason, fix_hint=fix_hint, **location,
        ) from None


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def search_coverage(
    dep_dir: Path, profile_subtypes: list[str] | None,
) -> tuple[dict[str, Any], list[str]]:
    """(coverage, warnings): which searchable record types chat leaves out.

    Composes the deployment's catalog from its modules, offline, and compares
    every subtype that declares searchable text with the chat profile's
    subtypes plus their refinements. Contacts nothing.
    """
    from okg.substrate.catalog.modules_compose import compose_catalog

    if not profile_subtypes:
        return {}, ["the chat search profile names no subtypes to compare"]
    try:
        catalog = compose_catalog(dep_dir)
    except Exception as exc:  # reported, never swallowed: see the warning
        return {}, [
            "could not list the record types this deployment's modules "
            f"declare ({type(exc).__name__}: {exc}); the chat scope was not "
            "compared with them"
        ]
    searchable = {
        name for name, meta in catalog.subtype_metadata.items()
        if meta.get("searchable_text_fields")
    }
    covered = set(profile_subtypes)
    changed = True
    while changed:
        changed = False
        for child, parent in catalog.subtype_parent.items():
            if parent in covered and child not in covered:
                covered.add(child)
                changed = True
    unknown = sorted(set(profile_subtypes) - catalog.subtypes)
    by_module: dict[str, list[str]] = {}
    for name in sorted(searchable - covered):
        by_module.setdefault(catalog.subtype_origin.get(name, "?"), []).append(name)
    # The example offered per module is its most connected record type: the
    # one the most declared relationships (edge narrowings) start or end at
    # -- paper for scholarly, code_symbol for code_graph.
    links: dict[str, int] = {}
    for row in catalog.narrowings:
        for end in (row.parent_subtype, row.child_subtype):
            links[end] = links.get(end, 0) + 1
    suggested = {
        module: min(names, key=lambda n: (-links.get(n, 0), n))
        for module, names in by_module.items()
    }
    coverage = {
        "searched": sorted(covered & catalog.subtypes),
        "not_searched_by_module": by_module,
        "suggested_by_module": suggested,
        "not_searched_count": sum(len(v) for v in by_module.values()),
        "searchable_count": len(searchable),
        "unknown_profile_subtypes": unknown,
    }
    warnings: list[str] = []
    if unknown:
        warnings.append(
            f"the chat search profile names subtypes this deployment's modules "
            f"do not declare: {unknown}"
        )
    return coverage, warnings


def _coverage_note(coverage: Mapping[str, Any]) -> str | None:
    by_module = coverage.get("not_searched_by_module") or {}
    if not by_module:
        return None
    suggested = coverage.get("suggested_by_module") or {}
    ranked = sorted(by_module.items(), key=lambda kv: (-len(kv[1]), kv[0]))

    def label(mod: str, names: list[str]) -> str:
        example = suggested.get(mod) or names[0]
        return f"{mod} ({len(names)}, e.g. {example})"

    shown = ", ".join(label(mod, names) for mod, names in ranked[:8])
    more = f", and {len(ranked) - 8} more modules" if len(ranked) > 8 else ""
    return (
        f"chat will not search {coverage['not_searched_count']} of the "
        f"{coverage['searchable_count']} searchable record types this "
        f"deployment's modules declare; by module: {shown}{more}. The full "
        "list is under coverage.not_searched_by_module; to widen a new chat "
        "profile, pass --search-subtype once per type to add, such as the "
        "examples named here"
    )


def _key_env_warning(name: Any, environ: Mapping[str, str]) -> str | None:
    """A warning when the named variable is unset. Checks presence only."""
    if not isinstance(name, str):
        return None
    if key_env_refusal(name) is not None:
        return (
            "chat.provider.api_key_env in deployment.yaml does not look like a "
            "variable name (the value is withheld); fix it by hand"
        )
    if name not in environ:
        return (
            f"${name} is not set in this shell; the chat site reads the key "
            "from it when it starts, so export it before a full "
            "`okg chat setup` run"
        )
    return None


def ensure_chat_block(
    deployment: str,
    answers: ChatAnswers,
    *,
    deployment_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> ScaffoldResult:
    """Write the ``chat:`` block when absent; verify agreement when present.

    ``environ`` is consulted for variable presence only (``name in
    environ``); no value is read.

    Raises :class:`ChatBlockScaffoldError`, or the loaders' own errors for a
    composed block they refuse (nothing is written then).
    """
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.deployments.chat_block import parse_chat_block
    from okg.substrate.mcp.search_profiles import parse_search_config

    env = os.environ if environ is None else environ
    # First, on every path: an answer that may be a credential is refused
    # before it can reach a conflict report, a note, or the file.
    validate_answers(answers)

    dep_dir = deployment_dir or find_deployment_dir(deployment)
    manifest = (dep_dir / "deployment.yaml") if dep_dir else None
    if manifest is None or not manifest.is_file():
        raise _refuse(
            f"deployment {deployment!r} has no deployment.yaml",
            reason="manifest_absent", deployment=deployment,
            fix_hint="check the deployment name and OKG_DEPLOYMENTS_DIR, or run `okg install`",
        )
    try:
        # Bytes, not read_text: universal newlines would turn CRLF into LF
        # and the append would rewrite every line ending in the file.
        text = manifest.read_bytes().decode("utf-8")
    except UnicodeDecodeError:
        raise _refuse(
            f"{manifest} is not UTF-8 text", reason="manifest_not_utf8",
            fix_hint="save deployment.yaml as UTF-8, then re-run",
        ) from None
    newline = "\r\n" if "\r\n" in text else "\n"
    raw = _load(
        text, manifest, reason="manifest_unparseable",
        fix_hint="fix the YAML error at the reported line, then re-run",
    ) or {}
    if not isinstance(raw, Mapping):
        raise _refuse(
            f"{manifest} is not a mapping", reason="manifest_malformed",
            fix_hint="restore deployment.yaml from the install",
        )

    if "chat" in raw:
        # The loader's verdict on what is already there comes first: a broken
        # block must be reported as broken, never as "unchanged".
        parse_chat_block(raw, manifest_path=manifest)
        existing = raw["chat"] if isinstance(raw.get("chat"), Mapping) else {}
        conflicts, conflict_notes = _conflicts(existing, raw, answers)
        if conflicts:
            raise _refuse(
                "the deployment already declares a chat: block that disagrees "
                f"with {[c['flag'] for c in conflicts]}; it is not overwritten",
                reason="chat_block_answer_conflicts", conflicts=conflicts,
                fix_hint=(
                    "re-run with answers that match the declared block (or "
                    "without them), or remove the chat: block to write a new one"
                ),
            )
        result = ScaffoldResult(
            status=STATUS_UNCHANGED, manifest=str(manifest),
            chat_block=dict(existing),
            notes=["the manifest already declares a chat: block; left as is",
                   *conflict_notes],
        )
        provider = existing.get("provider")
        if isinstance(provider, Mapping):
            warning = _key_env_warning(provider.get("api_key_env"), env)
            if warning:
                result.warnings.append(warning)
        _report_coverage(
            result, dep_dir, _declared_profile_subtypes(raw, existing.get("search_profile")),
        )
        return result

    block, search_block, decisions = compose_chat_block(deployment, raw, answers)
    if text and not text.endswith("\n"):
        # A final line with no newline may sit inside a block scalar, whose
        # value the added newline would change. Refuse rather than alter it.
        terminated = _load(
            text + newline, manifest, reason="manifest_cannot_be_appended",
            fix_hint="end deployment.yaml with a newline, then re-run",
        )
        if terminated != raw:
            raise _refuse(
                f"{manifest} does not end with a newline, and adding one would "
                "change the value on its last line; it is left untouched",
                reason="manifest_final_value_unterminated",
                fix_hint=(
                    "end deployment.yaml with a newline yourself (check the "
                    "last value is still what you want), then re-run"
                ),
            )
    additions: dict[str, Any] = {}
    if search_block is not None:
        additions["search"] = search_block
    additions["chat"] = block
    new_text = _append_text(text, additions, newline)
    new_raw = _load(
        new_text, manifest, reason="manifest_cannot_be_appended",
        fix_hint=(
            "the file ends with a `...` marker, holds more than one document, "
            "or is a flow-style mapping; make it one block-style mapping, "
            "then re-run"
        ),
    ) or {}
    if not isinstance(new_raw, Mapping) or {
        k: v for k, v in new_raw.items() if k not in additions
    } != dict(raw):
        raise _refuse(
            f"appending to {manifest} would change what it already declares; "
            "it is left untouched",
            reason="manifest_cannot_be_appended",
            fix_hint="make deployment.yaml one block-style mapping, then re-run",
        )
    # The same two loaders every chat verb and every search call use. A
    # composed block either of them refuses is never written.
    try:
        parse_search_config(new_raw.get("search"))
    except SubstrateError as exc:
        _add_search_error_path(exc, raw)
        raise
    parse_chat_block(new_raw, manifest_path=manifest)

    result = ScaffoldResult(
        status=STATUS_WRITTEN, manifest=str(manifest), chat_block=block,
        decisions=decisions,
    )
    prompt_ref = block["preset"].get("system_prompt_ref")
    if prompt_ref:
        prompt_path = dep_dir / prompt_ref
        if prompt_path.exists():
            result.notes.append(f"kept the existing system prompt {prompt_path}")
        else:
            _atomic_write(prompt_path, DEFAULT_SYSTEM_PROMPT)
            result.written.append(str(prompt_path))
    _atomic_write(manifest, new_text)
    result.written.append(str(manifest))
    result.notes.append(
        f"provider {decisions['provider']} at "
        f"{masked_base_url(block['provider']['base_url'])}"
        + (
            f", key read from ${block['provider']['api_key_env']} at run time"
            if block["provider"].get("api_key_env") else ", no key"
        )
    )
    result.notes.append(
        f"graph endpoint port {block['mcp']['port']} ({decisions['mcp_port']})"
    )
    result.notes.append(
        f"chat search profile {block['search_profile']!r} "
        f"({decisions['search_profile']})"
    )
    warning = _key_env_warning(block["provider"].get("api_key_env"), env)
    if warning:
        result.warnings.append(warning)
    _report_coverage(
        result, dep_dir, _declared_profile_subtypes(new_raw, block["search_profile"]),
    )
    return result


def _add_search_error_path(exc: SubstrateError, raw: Mapping[str, Any]) -> None:
    """Give a search-settings error the ``path`` it lacks, from names only.

    The search loader names the profile and key but not the path, and the
    setup report keeps error data by allowlist. A profile name is put in the
    path only when the operator's own file declares it; an answer never is.
    """
    if not isinstance(exc.data, dict) or "path" in exc.data:
        return
    search = raw.get("search")
    profiles = search.get("profiles") if isinstance(search, Mapping) else None
    profile = exc.data.get("profile")
    path = "search"
    if isinstance(profiles, Mapping) and isinstance(profile, str) and profile in profiles:
        path = f"search.profiles.{profile}"
        key = exc.data.get("key")
        if isinstance(key, str):
            path = f"{path}.{key}"
    elif profile is not None:
        path = "search.profiles"
    exc.data["path"] = path


def _report_coverage(
    result: ScaffoldResult, dep_dir: Path, profile_subtypes: list[str] | None,
) -> None:
    coverage, warnings = search_coverage(dep_dir, profile_subtypes)
    result.coverage = coverage
    result.warnings.extend(warnings)
    note = _coverage_note(coverage)
    if note:
        result.notes.append(note)


__all__ = [
    "ANSWER_FLAGS",
    "ChatAnswers",
    "ChatBlockScaffoldError",
    "DEFAULT_TOOLS",
    "DOCUMENT_SUBTYPES",
    "PROVIDER_PRESETS",
    "SCAFFOLD_SEARCH_PROFILE",
    "SYSTEM_PROMPT_REF",
    "ScaffoldResult",
    "compose_chat_block",
    "ensure_chat_block",
    "base_url_refusal",
    "base_url_masked_parts",
    "key_env_refusal",
    "masked_base_url",
    "search_coverage",
    "secret_answers",
    "validate_answers",
]
