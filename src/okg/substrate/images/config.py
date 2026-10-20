"""`agent_surface.images`: the one image setting block (#3046).

One parsed object serves both halves of image support. Ingest reads
``render`` and ``file_max_bytes``; the read side reads ``budget`` and ``url``
and renders URL images at the same ``render`` settings, so a stored image and
a fetched one look the same.

    agent_surface:
      images:
        render: {dpi: 100, max_side: 1600}
        file_max_bytes: 52428800
        budget: {max_panels: 4, max_bytes: 1500000}
        url: {prefixes: ["https://cds.cern.ch/record/"], timeout_s: 10, max_bytes: 10485760}

Every key is optional. A deployment without the block gets
`ImagesConfig.default()`: ingest renders at the defaults, and no URL is ever
fetched, because ``url.prefixes`` defaults to empty. Parsing is strict at every
level: an unknown key is refused by name, and a value of the wrong type or
outside its bounds is refused naming the field.

`UrlPrefix` and `UrlFetchPolicy.admits` are a collaborator's, taken from
``claude/inspect-image-kind`` (``mcp/image_sources.py``) so the allowlist has
one definition; his read side imports them from here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from okg.substrate.errors import SubstrateError

LABEL = "agent_surface.images"

DPI_MIN, DPI_MAX, DPI_DEFAULT = 36, 300, 100
MAX_SIDE_MIN, MAX_SIDE_MAX, MAX_SIDE_DEFAULT = 64, 8192, 1600
FILE_MAX_BYTES_MIN, FILE_MAX_BYTES_MAX = 1, 1024 * 1024 * 1024
FILE_MAX_BYTES_DEFAULT = 50 * 1024 * 1024
MAX_PANELS_MIN, MAX_PANELS_MAX, MAX_PANELS_DEFAULT = 1, 16, 4
BUDGET_BYTES_MIN, BUDGET_BYTES_MAX = 1024, 20 * 1024 * 1024
BUDGET_BYTES_DEFAULT = 1_500_000
URL_TIMEOUT_MIN, URL_TIMEOUT_MAX, URL_TIMEOUT_DEFAULT = 0.1, 120.0, 10.0
URL_BYTES_MIN, URL_BYTES_MAX = 1024, 100 * 1024 * 1024
URL_BYTES_DEFAULT = 10 * 1024 * 1024

BLOCK_KEYS = frozenset({"render", "file_max_bytes", "budget", "url"})
RENDER_KEYS = frozenset({"dpi", "max_side"})
BUDGET_KEYS = frozenset({"max_panels", "max_bytes"})
URL_KEYS = frozenset({"prefixes", "timeout_s", "max_bytes"})


class ImagesConfigInvalid(SubstrateError):
    """`agent_surface.images` cannot mean what it says. `data.field` names
    the offending key; an unknown key lists the accepted ones."""

    code = "images_config_invalid"


# ── URL allowlist ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class UrlPrefix:
    """One allowlisted prefix, compared by parts: host and port exactly,
    path by prefix. A string `startswith` would let
    ``https://cds.cern.ch.evil.example/`` through ``https://cds.cern.ch``.

    From a collaborator's ``claude/inspect-image-kind``. One change: any
    ``@`` in the authority is refused, so an empty userinfo
    (``https://@host/``) is treated as the credentials it spells."""

    host: str
    port: int
    path: str

    @classmethod
    def parse(cls, prefix: str) -> "UrlPrefix":
        parts = urlsplit(prefix)
        if parts.scheme != "https" or not parts.hostname or "@" in parts.netloc:
            raise ValueError(f"image URL prefix {prefix!r} must be an https URL with a host and no credentials")
        if parts.query or parts.fragment:
            raise ValueError(f"image URL prefix {prefix!r} must not carry a query or fragment")
        path = parts.path or "/"
        if not path.endswith("/"):
            path += "/"  # a prefix names a directory, so /files does not admit /files-secret
        return cls(host=parts.hostname.lower(), port=parts.port or 443, path=path)

    def admits(self, url: str) -> bool:
        parts = urlsplit(url)
        if parts.scheme != "https" or "@" in parts.netloc or not parts.hostname:
            return False
        try:
            port = parts.port or 443
        except ValueError:  # a malformed port admits nothing
            return False
        path = parts.path or "/"
        if url_path_problem(path) is not None:
            return False
        return (
            parts.hostname.lower() == self.host
            and port == self.port
            and path.startswith(self.path)
        )


def url_path_problem(path: str) -> str | None:
    """Why a URL path could name a different place than it appears to, or
    None. A server resolves `..` and `.` segments (and their percent-encoded
    or backslash forms) before it serves, so `/figs/../secret` would pass a
    `/figs/` prefix and fetch `/secret` (#3046 read-side review)."""
    if "\\" in path or "%5c" in path.lower():
        return "path has a backslash"
    decoded = unquote(path)
    for segment in decoded.split("/"):
        if segment in (".", ".."):
            return f"path has a {segment!r} segment"
    return None


# ── the parsed block ───────────────────────────────────────────────────

@dataclass(frozen=True)
class RenderConfig:
    """How a file or URL image is rendered to the PNG that is served."""

    dpi: int = DPI_DEFAULT
    max_side: int = MAX_SIDE_DEFAULT


@dataclass(frozen=True)
class BudgetConfig:
    """How much image one read call may send."""

    max_panels: int = MAX_PANELS_DEFAULT
    max_bytes: int = BUDGET_BYTES_DEFAULT


@dataclass(frozen=True)
class UrlConfig:
    """Which public URLs the read side may fetch. No prefix, no fetch."""

    prefixes: tuple[UrlPrefix, ...] = ()
    timeout_s: float = URL_TIMEOUT_DEFAULT
    max_bytes: int = URL_BYTES_DEFAULT

    @property
    def fetch_enabled(self) -> bool:
        return bool(self.prefixes)

    def admits(self, url: str) -> bool:
        """True when some prefix admits `url`; always False with none."""
        return any(p.admits(url) for p in self.prefixes)


@dataclass(frozen=True)
class ImagesConfig:
    """The parsed `agent_surface.images` block."""

    render: RenderConfig = field(default_factory=RenderConfig)
    file_max_bytes: int = FILE_MAX_BYTES_DEFAULT
    budget: BudgetConfig = field(default_factory=BudgetConfig)
    url: UrlConfig = field(default_factory=UrlConfig)

    @classmethod
    def default(cls) -> "ImagesConfig":
        """The settings for a deployment that declares no block."""
        return cls()

    @classmethod
    def parse(cls, raw: Any, *, where: str = "") -> "ImagesConfig":
        """Parse the block's body. ``None`` gives the defaults."""
        if raw is None:
            return cls.default()
        body = _mapping(raw, LABEL, where)
        _strict_keys(body, BLOCK_KEYS, LABEL, where)

        render_raw = _mapping(body.get("render"), f"{LABEL}.render", where, optional=True)
        _strict_keys(render_raw, RENDER_KEYS, f"{LABEL}.render", where)
        render = RenderConfig(
            dpi=_int(render_raw, "dpi", f"{LABEL}.render", DPI_DEFAULT, DPI_MIN, DPI_MAX, where),
            max_side=_int(
                render_raw, "max_side", f"{LABEL}.render",
                MAX_SIDE_DEFAULT, MAX_SIDE_MIN, MAX_SIDE_MAX, where,
            ),
        )

        file_max_bytes = _int(
            body, "file_max_bytes", LABEL,
            FILE_MAX_BYTES_DEFAULT, FILE_MAX_BYTES_MIN, FILE_MAX_BYTES_MAX, where,
        )

        budget_raw = _mapping(body.get("budget"), f"{LABEL}.budget", where, optional=True)
        _strict_keys(budget_raw, BUDGET_KEYS, f"{LABEL}.budget", where)
        budget = BudgetConfig(
            max_panels=_int(
                budget_raw, "max_panels", f"{LABEL}.budget",
                MAX_PANELS_DEFAULT, MAX_PANELS_MIN, MAX_PANELS_MAX, where,
            ),
            max_bytes=_int(
                budget_raw, "max_bytes", f"{LABEL}.budget",
                BUDGET_BYTES_DEFAULT, BUDGET_BYTES_MIN, BUDGET_BYTES_MAX, where,
            ),
        )

        url_raw = _mapping(body.get("url"), f"{LABEL}.url", where, optional=True)
        _strict_keys(url_raw, URL_KEYS, f"{LABEL}.url", where)
        url = UrlConfig(
            prefixes=_prefixes(url_raw.get("prefixes"), where),
            timeout_s=_seconds(url_raw, where),
            max_bytes=_int(
                url_raw, "max_bytes", f"{LABEL}.url",
                URL_BYTES_DEFAULT, URL_BYTES_MIN, URL_BYTES_MAX, where,
            ),
        )
        return cls(render=render, file_max_bytes=file_max_bytes, budget=budget, url=url)


# ── strict parsing helpers ─────────────────────────────────────────────

def _refuse(message: str, *, field: str, where: str, fix_hint: str, **data: Any) -> ImagesConfigInvalid:
    return ImagesConfigInvalid(
        f"deployment.yaml `{field}` {message}.{where}",
        data={"field": field, **data},
        fix_hint=fix_hint,
    )


def _mapping(raw: Any, label: str, where: str, *, optional: bool = False) -> Mapping[str, Any]:
    if raw is None and optional:
        return {}
    if not isinstance(raw, Mapping):
        raise _refuse(
            f"must be a mapping; got {type(raw).__name__}",
            field=label, where=where, fix_hint=f"declare `{label}` as a mapping",
        )
    return raw


def _strict_keys(body: Mapping[str, Any], allowed: frozenset[str], label: str, where: str) -> None:
    unknown = sorted(str(k) for k in set(body) - allowed)
    if unknown:
        raise ImagesConfigInvalid(
            f"deployment.yaml `{label}` has unknown key(s) "
            f"{', '.join(repr(k) for k in unknown)}; accepted keys are "
            f"{sorted(allowed)}.{where}",
            data={"field": label, "unknown_keys": unknown, "accepted_keys": sorted(allowed)},
            fix_hint=f"fix the key spelling under `{label}`",
        )


def _int(
    body: Mapping[str, Any], key: str, label: str,
    default: int, low: int, high: int, where: str,
) -> int:
    name = f"{label}.{key}"
    raw = body.get(key)
    if raw is None:
        return default
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise _refuse(
            f"must be an integer; got {type(raw).__name__}",
            field=name, where=where, fix_hint=f"set `{name}` to an integer from {low} to {high}",
            min=low, max=high,
        )
    if not low <= raw <= high:
        raise _refuse(
            f"is {raw}, outside {low}..{high}",
            field=name, where=where, fix_hint=f"set `{name}` from {low} to {high}",
            value=raw, min=low, max=high,
        )
    return raw


def _seconds(body: Mapping[str, Any], where: str) -> float:
    name = f"{LABEL}.url.timeout_s"
    raw = body.get("timeout_s")
    if raw is None:
        return URL_TIMEOUT_DEFAULT
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise _refuse(
            f"must be a number of seconds; got {type(raw).__name__}",
            field=name, where=where,
            fix_hint=f"set `{name}` from {URL_TIMEOUT_MIN} to {URL_TIMEOUT_MAX}",
            min=URL_TIMEOUT_MIN, max=URL_TIMEOUT_MAX,
        )
    # `not low <= raw <= high` is also true for NaN, which is refused here.
    if not URL_TIMEOUT_MIN <= raw <= URL_TIMEOUT_MAX:
        raise _refuse(
            f"is {raw}, outside {URL_TIMEOUT_MIN}..{URL_TIMEOUT_MAX}",
            field=name, where=where,
            fix_hint=f"set `{name}` from {URL_TIMEOUT_MIN} to {URL_TIMEOUT_MAX}",
            value=raw, min=URL_TIMEOUT_MIN, max=URL_TIMEOUT_MAX,
        )
    return float(raw)


def _prefixes(raw: Any, where: str) -> tuple[UrlPrefix, ...]:
    name = f"{LABEL}.url.prefixes"
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise _refuse(
            f"must be a list of https URL prefixes; got {type(raw).__name__}",
            field=name, where=where, fix_hint=f"declare `{name}` as a list",
        )
    parsed: list[UrlPrefix] = []
    for index, item in enumerate(raw):
        if not isinstance(item, str):
            raise _refuse(
                f"entry {index} must be a string; got {type(item).__name__}",
                field=name, where=where, fix_hint="write each prefix as an https URL string",
                index=index,
            )
        if any(ch == "\x00" or ch.isspace() for ch in item):
            raise _refuse(
                f"entry {index} ({item!r}) contains NUL or whitespace",
                field=name, where=where, fix_hint="write each prefix as an https URL string",
                index=index,
            )
        try:
            parsed.append(UrlPrefix.parse(item))
        except ValueError as exc:
            raise _refuse(
                f"entry {index} is refused: {exc}",
                field=name, where=where,
                fix_hint="use https URLs with a host, no credentials, no query or fragment",
                index=index,
            ) from None
    return tuple(parsed)


__all__ = [
    "BudgetConfig",
    "ImagesConfig",
    "ImagesConfigInvalid",
    "RenderConfig",
    "UrlConfig",
    "UrlPrefix",
]
