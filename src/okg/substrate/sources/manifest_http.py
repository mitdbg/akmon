"""Source manifest, phase 2: read records from a JSON API.

`read_inputs` reads every read a manifest declares -- local files through
`okg.substrate.sources.manifest`, JSON APIs here -- into one `InputRead`
that the generic reader and the `add-source --manifest` preview both map.

HTTP rules (design: pact/changes/no-code-source-manifest-http/design.md):

- GET only, a 30-second timeout per request.
- A 429 or 5xx response, or a connection error, is retried up to 3 times,
  honoring `Retry-After`. A read that still fails is incomplete.
- A response that is not JSON, or has no list at `records`, makes the read
  incomplete; it is never an empty read.
- Paging (`offset`, `page`, `cursor`, `link_header`) stops on an empty page
  or a missing next value. A read that reaches `max_pages` with more to
  fetch is incomplete.
- Auth values come from environment variables the manifest names. Every
  message is redacted of those values; a redirect to another host drops
  the credentials.
- Each page's `ETag` / `Last-Modified` is kept, so the next run can ask
  the server with a conditional GET whether anything changed.
"""
from __future__ import annotations

import email.utils
import hashlib
import http.client
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from base64 import b64encode
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

from okg.substrate.sources.manifest import (
    _MISSING,
    FileReadError,
    InputRead,
    Manifest,
    ManifestProblem,
    ReadSpec,
    extract_records,
    read_one_files_read,
    resolve_path,
)

TIMEOUT_S = 30.0
MAX_RETRIES = 3
#: A server that asks to wait longer than this fails the read instead.
MAX_RETRY_AFTER_S = 120.0
_BACKOFF_S = (1.0, 2.0, 4.0)
_RETRY_STATUSES = frozenset({429}) | frozenset(range(500, 600))
USER_AGENT = "okg-source-manifest/2"
REDACTED = "***"

Sleep = Callable[[float], None]


class HttpReadFailed(Exception):
    """One HTTP read could not be completed; the message is already redacted.

    ``status`` is the HTTP status that ended the read, when there was one."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


def _error_detail(exc: urllib.error.HTTPError) -> str:
    """The start of an error response's body; the server often says why."""
    try:
        return exc.read(300).decode("utf-8", "replace").strip()[:300]
    except Exception:  # noqa: BLE001 - the status is the message
        return ""


@dataclass
class _Response:
    url: str
    status: int
    body: bytes
    headers: Mapping[str, str]


# --------------------------------------------------------------------------
# Auth, TLS and redaction
# --------------------------------------------------------------------------


def auth_values(read: ReadSpec, env: Mapping[str, str]) -> dict[str, str]:
    """role -> value for the read's auth; a missing variable fails the read."""
    values: dict[str, str] = {}
    missing = []
    for role, name in read.auth.env.items():
        value = env.get(name)
        if value is None or value == "":
            missing.append(name)
        else:
            values[role] = value
    if missing:
        raise HttpReadFailed(
            f"{read.where}: the environment variable(s) {', '.join('$' + m for m in missing)} "
            f"that {read.auth.mode.replace('_', ' ')} auth names are not set"
        )
    return values


def secrets_of(read: ReadSpec, values: Mapping[str, str]) -> list[str]:
    """Values that must never appear in a message (file paths are not secret)."""
    if read.auth.mode == "client_certificate":
        return []
    out = [v for v in values.values() if v]
    if read.auth.mode == "basic":
        out.append(b64encode(f"{values.get('user', '')}:{values.get('password', '')}".encode()).decode())
    return out


def mask_secret_values(text: str, secrets: list[str]) -> str:
    """Blank out each credential value (a token, a header value) in a message.

    Not DSN masking: these are HTTP credentials, so `okg.substrate.db.dsn`
    does not apply."""
    for secret in sorted(secrets, key=len, reverse=True):
        if secret:
            text = text.replace(secret, REDACTED)
    return text


def auth_headers(read: ReadSpec, values: Mapping[str, str]) -> dict[str, str]:
    mode = read.auth.mode
    if mode == "bearer":
        return {"Authorization": f"Bearer {values['token']}"}
    if mode == "header":
        return {str(read.auth.header_name): values["value"]}
    if mode == "basic":
        token = b64encode(f"{values['user']}:{values['password']}".encode()).decode()
        return {"Authorization": f"Basic {token}"}
    return {}


def ssl_context(read: ReadSpec, values: Mapping[str, str]) -> ssl.SSLContext:
    context = ssl.create_default_context()
    if read.auth.mode != "client_certificate":
        return context
    cert, key = values["cert"], values["key"]
    ca_dir = values.get("ca_dir")
    try:
        if ca_dir:
            ca_path = Path(ca_dir)
            if ca_path.is_dir():
                context.load_verify_locations(capath=str(ca_path))
                for pem in sorted(ca_path.glob("*.pem")):
                    context.load_verify_locations(cafile=str(pem))
            else:
                context.load_verify_locations(cafile=str(ca_path))
        context.load_cert_chain(certfile=cert, keyfile=key)
    except (OSError, ssl.SSLError) as exc:
        raise HttpReadFailed(
            f"{read.where}: cannot load the client certificate or CA files named by "
            f"{', '.join('$' + n for n in read.auth.env.values())}: {exc}"
        ) from exc
    return context


class _SameHostRedirect(urllib.request.HTTPRedirectHandler):
    """Follow redirects, but drop credentials when the host changes or the
    redirect leaves https."""

    def __init__(self, credential_headers: list[str]) -> None:
        super().__init__()
        self._credential_headers = [h.lower() for h in credential_headers]

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        before, after = urlsplit(req.full_url), urlsplit(newurl)
        downgraded = before.scheme == "https" and after.scheme != "https"
        if new is not None and (after.netloc != before.netloc or downgraded):
            for name in list(new.headers):
                if name.lower() in self._credential_headers:
                    del new.headers[name]
            for name in list(new.unredirected_hdrs):
                if name.lower() in self._credential_headers:
                    del new.unredirected_hdrs[name]
        return new


# --------------------------------------------------------------------------
# One request, with retries
# --------------------------------------------------------------------------


def _retry_after(headers: Mapping[str, str]) -> Optional[float]:
    value = headers.get("Retry-After") or headers.get("retry-after")
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - time.time())


class _Client:
    def __init__(
        self, read: ReadSpec, env: Mapping[str, str], *, sleep: Optional[Sleep], timeout: float,
    ) -> None:
        self.read = read
        values = auth_values(read, env)
        self.secrets = secrets_of(read, values)
        self.headers = {"Accept": "application/json", "User-Agent": USER_AGENT, **auth_headers(read, values)}
        context = ssl_context(read, values)
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=context),
            _SameHostRedirect([h for h in self.headers if h not in ("Accept", "User-Agent")]),
        )
        self.sleep = sleep or time.sleep
        self.timeout = timeout
        self.requests = 0
        self.retries = 0

    def fail(self, message: str, status: Optional[int] = None) -> HttpReadFailed:
        return HttpReadFailed(mask_secret_values(message, self.secrets), status)

    def get(self, url: str, *, extra: Optional[Mapping[str, str]] = None, conditional: bool = False) -> _Response:
        attempt = 0
        while True:
            self.requests += 1
            request = urllib.request.Request(url, headers={**self.headers, **(extra or {})}, method="GET")
            wait: Optional[float] = None
            try:
                with self.opener.open(request, timeout=self.timeout) as response:
                    return _Response(response.geturl(), response.status, response.read(), dict(response.headers))
            except urllib.error.HTTPError as exc:
                status = exc.code
                headers = dict(exc.headers or {})
                if conditional and status == 304:
                    return _Response(url, 304, b"", headers)
                if status not in _RETRY_STATUSES or attempt >= MAX_RETRIES:
                    detail = _error_detail(exc)
                    tries = f" after {attempt + 1} attempts" if attempt else ""
                    raise self.fail(
                        f"GET {url} answered HTTP {status}{tries}"
                        + (f": {detail}" if detail else ""),
                        status,
                    ) from None
                wait = _retry_after(headers)
                if wait is not None and wait > MAX_RETRY_AFTER_S:
                    detail = _error_detail(exc)
                    raise self.fail(
                        f"GET {url} answered HTTP {status} and asked to wait {int(wait)} s, "
                        f"more than {int(MAX_RETRY_AFTER_S)} s"
                        + (f": {detail}" if detail else ""),
                        status,
                    ) from None
            except (
                urllib.error.URLError, TimeoutError, ConnectionError, ssl.SSLError,
                http.client.HTTPException,
            ) as exc:
                reason = getattr(exc, "reason", exc)
                if attempt >= MAX_RETRIES:
                    raise self.fail(f"GET {url} failed after {attempt + 1} attempts: {reason}") from None
            except ValueError as exc:  # an invalid URL: retrying cannot help
                raise self.fail(f"GET {url} cannot be sent: {exc}") from None
            attempt += 1
            self.retries += 1
            self.sleep(wait if wait is not None else _BACKOFF_S[min(attempt - 1, len(_BACKOFF_S) - 1)])


# --------------------------------------------------------------------------
# Paging
# --------------------------------------------------------------------------


def with_params(url: str, params: Mapping[str, Any]) -> str:
    """`url` with `params` set (replacing any earlier value)."""
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in params]
    query += [(k, str(v)) for k, v in params.items()]
    encoded = urlencode(query, quote_via=quote, safe=":,*/@")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, encoded, parts.fragment))


def _link_next(headers: Mapping[str, str], base: str) -> Optional[str]:
    value = headers.get("Link") or headers.get("link")
    if not value:
        return None
    for part in value.split(","):
        section = part.strip().split(";")
        if len(section) < 2:
            continue
        target = section[0].strip()
        rels = [s.strip().replace(" ", "") for s in section[1:]]
        if target.startswith("<") and target.endswith(">") and any(
            r in ('rel="next"', "rel=next") for r in rels
        ):
            return urljoin(base, target[1:-1])
    return None


def _parse_json(response: _Response, client: _Client) -> Any:
    content_type = response.headers.get("Content-Type") or response.headers.get("content-type") or "unknown"
    charset = "utf-8"
    if "charset=" in content_type:
        charset = content_type.split("charset=", 1)[1].split(";")[0].strip() or "utf-8"
    try:
        text = response.body.decode("utf-8-sig" if charset.lower() in ("utf-8", "utf8") else charset)
        return json.loads(text)
    except (UnicodeDecodeError, LookupError, json.JSONDecodeError) as exc:
        snippet = response.body[:120].decode("utf-8", "replace").strip()
        raise client.fail(
            f"GET {response.url} answered HTTP {response.status} with content type {content_type}, "
            f"which is not JSON ({exc.__class__.__name__}): {snippet!r}"
        ) from None


@dataclass
class HttpRead:
    """What one HTTP read fetched."""

    pages: list[tuple[str, list[Any]]]
    page_meta: list[dict[str, Any]]
    requests: int
    retries: int


def fetch_read(
    read: ReadSpec,
    *,
    env: Optional[Mapping[str, str]] = None,
    sleep: Optional[Sleep] = None,
    timeout: float = TIMEOUT_S,
) -> HttpRead:
    """Fetch every page of one HTTP read, or raise `HttpReadFailed`."""
    client = _Client(read, os.environ if env is None else env, sleep=sleep, timeout=timeout)
    paging = read.paging
    url = str(read.url)
    offset = 0
    page_no = int(paging.params.get("first", 1)) if paging.mode == "page" else 0
    if paging.mode == "offset":
        url = with_params(url, {paging.params["offset_param"]: 0, paging.params["limit_param"]: paging.params["size"]})
    elif paging.mode == "page":
        url = with_params(url, {paging.params["page_param"]: page_no, paging.params["size_param"]: paging.params["size"]})
    pages: list[tuple[str, list[Any]]] = []
    meta: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    while True:
        response = client.get(url)
        payload = _parse_json(response, client)
        try:
            rows = extract_records(payload, read.records, f"GET {url}")
        except FileReadError as exc:
            raise client.fail(str(exc)) from None
        label = f"GET {url}"
        pages.append((label, rows))
        seen_urls.add(url)
        meta.append({
            "url": url,
            "sha256": hashlib.sha256(response.body).hexdigest(),
            "etag": response.headers.get("ETag") or response.headers.get("etag"),
            "last_modified": response.headers.get("Last-Modified") or response.headers.get("last-modified"),
            "records": len(rows),
        })
        if paging.mode == "none" or not rows:
            break
        next_url: Optional[str]
        if paging.mode == "offset":
            offset += int(paging.params["size"])
            next_url = with_params(url, {paging.params["offset_param"]: offset})
        elif paging.mode == "page":
            page_no += 1
            next_url = with_params(url, {paging.params["page_param"]: page_no})
        elif paging.mode == "cursor":
            value = resolve_path(payload, str(paging.params["next"]))
            if value is _MISSING or isinstance(value, (Mapping, list)) or str(value) == "":
                next_url = None
            else:
                next_url = with_params(url, {paging.params["param"]: value})
        else:  # link_header
            next_url = _link_next(response.headers, response.url)
        if next_url is None:
            break
        if next_url in seen_urls:
            raise client.fail(f"{read.where}: paging returned to {next_url}, which was already read")
        if len(pages) >= read.max_pages:
            raise client.fail(
                f"{read.where}: reached max_pages ({read.max_pages}) and the API still has more pages"
            )
        url = next_url
    return HttpRead(pages=pages, page_meta=meta, requests=client.requests, retries=client.retries)


def unchanged_since(
    read: ReadSpec,
    page_meta: list[Mapping[str, Any]],
    *,
    env: Optional[Mapping[str, str]] = None,
    sleep: Optional[Sleep] = None,
    timeout: float = TIMEOUT_S,
) -> bool:
    """True when every earlier page carries a validator and the server
    answers a conditional GET for each with 304 Not Modified."""
    if not page_meta or any(not (p.get("etag") or p.get("last_modified")) for p in page_meta):
        return False
    try:
        client = _Client(read, os.environ if env is None else env, sleep=sleep, timeout=timeout)
        for page in page_meta:
            extra = {}
            if page.get("etag"):
                extra["If-None-Match"] = str(page["etag"])
            if page.get("last_modified"):
                extra["If-Modified-Since"] = str(page["last_modified"])
            if client.get(str(page["url"]), extra=extra, conditional=True).status != 304:
                return False
    except HttpReadFailed:
        return False
    return True


# --------------------------------------------------------------------------
# Every read of a manifest
# --------------------------------------------------------------------------


def page_digest_key(read: ReadSpec, index: int) -> str:
    return f"http:{read.where}:{index}"


def page_item_bytes(sha256: str) -> bytes:
    """The bytes the change probe hashes for one page: its body digest."""
    return sha256.encode("ascii")


def read_inputs(
    manifest: Manifest,
    *,
    env: Optional[Mapping[str, str]] = None,
    sleep: Optional[Sleep] = None,
    timeout: float = TIMEOUT_S,
) -> InputRead:
    """Read every read: files (refused when a glob matches nothing) and HTTP.

    A failed HTTP read is recorded in `failed` (it makes the run incomplete);
    the other reads still return their records.
    """
    out = InputRead(files=[], records=[], failed=[])
    for type_name, read in manifest.all_reads():
        if read.kind == "files":
            out.extend(read_one_files_read(type_name, read))
            continue
        started = time.monotonic()
        try:
            fetched = fetch_read(read, env=env, sleep=sleep, timeout=timeout)
        except HttpReadFailed as exc:
            out.failed.append(ManifestProblem(
                "http_read_failed", str(exc),
                "fix the cause named; the records of the other reads apply and nothing is "
                "removed until this read completes",
                path=read.where, file=read.url,
            ))
            out.reads.append({
                "read": read.where, "type": type_name, "kind": "http", "what": read.describe(),
                "pages": 0, "records": 0, "complete": False, "error": str(exc),
                "elapsed_s": round(time.monotonic() - started, 3),
            })
            continue
        for index, ((label, rows), meta) in enumerate(zip(fetched.pages, fetched.page_meta)):
            out.records.append((label, rows))
            out.record_types.append(type_name)
            out.digests[page_digest_key(read, index)] = hashlib.sha256(
                page_item_bytes(meta["sha256"]),
            ).hexdigest()
        out.http_pages[read.where] = fetched.page_meta
        out.reads.append({
            "read": read.where, "type": type_name, "kind": "http", "what": read.describe(),
            "pages": len(fetched.pages), "records": sum(len(rows) for _l, rows in fetched.pages),
            "requests": fetched.requests, "retries": fetched.retries, "complete": True,
            "elapsed_s": round(time.monotonic() - started, 3),
        })
    return out


__all__ = [
    "HttpRead",
    "HttpReadFailed",
    "fetch_read",
    "mask_secret_values",
    "page_digest_key",
    "page_item_bytes",
    "read_inputs",
    "unchanged_since",
    "with_params",
]
