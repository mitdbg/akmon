"""gmail_api source — Gmail v1 API with strict label allowlist.

Per `refine-academic-pi-with-comms-and-calendar` Tier C: only
emails carrying at least one allowlisted label emit nodes.
Wholesale inbox ingest is impossible by design.

Per `finish-gmail-source`: full backfill (paginated
`messages.list` + per-message `format=full` fetch), incremental
sync via the `history.list` cursor stored in the runner's
`cursor=` blob, EmailThread aggregation, and body extraction
into `document` nodes (Redactor-truncated, PII-hashed per
`pii_mode`). A stale historyId (404) falls back to full
backfill loudly. Uses the substrate OAuth library
(`RefreshHandler` + keyring `TokenStore`) — token custody and
`okg doctor --check source-auth` preflight unchanged.
"""
from __future__ import annotations

import base64
import hashlib
import logging as _logging
import urllib.error
from email.utils import getaddresses, parseaddr, parsedate_to_datetime
from typing import Any, Iterable, Iterator, Optional

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
    SourceRun,
)
from okg.substrate.library.oauth.providers import GMAIL
from okg.substrate.library.oauth.refresh import RefreshHandler
from okg.substrate.library.oauth.redaction import Redactor


_log = _logging.getLogger(__name__)

_API = "https://gmail.googleapis.com/gmail/v1/users/me"


class GmailApiSource:
    """Gmail v1 API connector. Label-allowlist required."""

    name: str = "gmail_api"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        deployment_name: str,
        gmail_labels: Optional[list[str]] = None,
        pii_mode: str = "plain",
        max_body_kb: int = 50,
        page_size: int = 100,
        fetch_bodies: bool = True,
        ownership_id: str = "gmail_api",
        refresh_handler: Optional[RefreshHandler] = None,
        **_ignored: Any,
    ) -> None:
        self.deployment_name = deployment_name
        self.gmail_labels = gmail_labels or []
        self.redactor = Redactor(
            max_body_kb=max_body_kb, pii_mode=pii_mode,
        )
        self.page_size = page_size
        self.fetch_bodies = fetch_bodies
        self.ownership_id = ownership_id
        # Injection point for tests; production constructs lazily
        # so import-time has no keyring dependency.
        self._refresh_handler = refresh_handler

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        """Per dogfood F-7a: report OAuth token state to
        `okg doctor --check source-auth`. Returns `ok` when env
        vars + keyring token are in place; `missing_credential`
        with an actionable reason otherwise.
        """
        from okg.substrate.library.oauth.preflight import oauth_preflight
        return oauth_preflight(
            source_name=self.name,
            deployment_name=self.deployment_name,
            provider=GMAIL,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        **_ignored: Any,
    ) -> SourceRun:
        if not self.gmail_labels:
            # No allowlist = no ingest. Per `refine-academic-pi`
            # this is by design, not a silent fallback. The
            # source SHALL emit a config warning + zero rows.
            _log.warning(
                "gmail_api: no gmail_labels configured; refusing "
                "to ingest (per strict-allowlist contract). Set "
                "`gmail_labels` in source_registry.yaml."
            )
            return SourceRun(facts=iter([]), completed_scope=True)

        # OAuth + the profile call are unrecoverable failures —
        # propagate so the ingest fails loud rather than emitting
        # zero facts.
        refresh = self._refresh_handler or RefreshHandler(
            provider=GMAIL, deployment_name=self.deployment_name,
        )
        profile = refresh.authed_request(f"{_API}/profile")
        # The next run's history anchor is this run's profile
        # historyId, captured before we read anything.
        next_cursor = {"gmail_history_id": str(profile["historyId"])}

        prior_history_id = (cursor or {}).get("gmail_history_id")
        first_history_page: Optional[dict[str, Any]] = None
        if prior_history_id:
            # Eager first page: stale-cursor detection must happen
            # here, not mid-generator, so the fallback decision is
            # loud and taken before facts stream.
            try:
                first_history_page = refresh.authed_request(
                    f"{_API}/history?startHistoryId={prior_history_id}"
                    f"&maxResults={self.page_size}"
                )
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    _log.warning(
                        "gmail_api: stored historyId %s is stale "
                        "(404 from history.list); falling back to "
                        "full backfill — never a silent empty delta",
                        prior_history_id,
                    )
                    first_history_page = None
                    prior_history_id = None
                else:
                    raise

        incremental = bool(prior_history_id and first_history_page is not None)
        if incremental:
            message_ids = self._history_message_ids(
                refresh, prior_history_id, first_history_page,
            )
        else:
            message_ids = self._backfill_message_ids(refresh)

        return SourceRun(
            facts=self._stream(refresh, message_ids),
            # A delta walk is not a full-scope walk: absence from
            # this run must never be read as retraction authority.
            completed_scope=not incremental,
            next_cursor=next_cursor,
        )

    # -- message-id enumeration ------------------------------------

    def _backfill_message_ids(
        self, refresh: RefreshHandler,
    ) -> Iterator[str]:
        label_q = " OR ".join(f"label:{label}" for label in self.gmail_labels)
        page_token: Optional[str] = None
        while True:
            url = (
                f"{_API}/messages?q={label_q}"
                f"&maxResults={self.page_size}"
            )
            if page_token:
                url += f"&pageToken={page_token}"
            page = refresh.authed_request(url)
            for ref in page.get("messages", []) or []:
                if ref.get("id"):
                    yield ref["id"]
            page_token = page.get("nextPageToken")
            if not page_token:
                return

    def _history_message_ids(
        self,
        refresh: RefreshHandler,
        start_history_id: str,
        first_page: dict[str, Any],
    ) -> Iterator[str]:
        """Walk history.list pages, yielding each changed message
        id once. Additive-only: deletions are not retracted here
        (mutable_api reconciliation owns that)."""
        seen: set[str] = set()
        page: Optional[dict[str, Any]] = first_page
        while page is not None:
            for item in page.get("history", []) or []:
                changed = list(item.get("messagesAdded", []) or [])
                changed += list(item.get("labelsAdded", []) or [])
                for entry in changed:
                    mid = (entry.get("message") or {}).get("id")
                    if mid and mid not in seen:
                        seen.add(mid)
                        yield mid
            token = page.get("nextPageToken")
            if not token:
                return
            page = refresh.authed_request(
                f"{_API}/history?startHistoryId={start_history_id}"
                f"&maxResults={self.page_size}&pageToken={token}"
            )

    # -- fact emission ----------------------------------------------

    def _stream(
        self,
        refresh: RefreshHandler,
        message_ids: Iterable[str],
    ) -> Iterator[Any]:
        threads: dict[str, dict[str, Any]] = {}
        for mid in message_ids:
            try:
                msg = refresh.authed_request(
                    f"{_API}/messages/{mid}?format=full"
                )
            except Exception:  # noqa: BLE001 — one message failure shouldn't block the whole sync
                _log.exception(
                    "gmail_api: fetch failed for message %s", mid,
                )
                continue

            # Defense-in-depth: re-verify allowlist match here in
            # case the query/history missed.
            msg_labels = msg.get("labelIds", []) or []
            if not any(label in msg_labels for label in self.gmail_labels):
                continue

            yield from self._emit_message(msg, threads)

        for tid, agg in threads.items():
            thread_node_id = f"thread:gmail:{tid}"
            yield NodeFact(
                node_id=thread_node_id,
                subtype="email_thread",
                attrs={
                    "thread_id": thread_node_id,
                    "subject": agg["subject"],
                    "first_date": agg["first_date"],
                    "latest_date": agg["latest_date"],
                    "participants_refs": sorted(agg["participants"]),
                    "participants_domains": sorted(agg["participant_domains"]),
                    "message_count": agg["count"],
                    "external_ref": tid,
                },
                source_record_id={"thread_id": tid},
                source_revision={"content_sha": _sha(repr(sorted(agg.items())))},
            )
            yield ProgressMarker(
                record_id=f"thread:{tid}",
                fingerprint=_sha(repr(sorted(agg.items()))),
            )

    def _emit_message(
        self,
        msg: dict[str, Any],
        threads: dict[str, dict[str, Any]],
    ) -> Iterator[Any]:
        mid = msg["id"]
        email_id = f"email:gmail:{mid}"
        payload = msg.get("payload", {}) or {}
        headers = {
            (h.get("name") or "").title(): h.get("value")
            for h in (payload.get("headers") or [])
        }
        subject = headers.get("Subject", "") or ""
        date_iso = _date_to_iso(headers.get("Date", "") or "")
        snippet = (msg.get("snippet", "") or "")[:200]
        msg_labels = msg.get("labelIds", []) or []

        sender_name, sender_addr = parseaddr(headers.get("From", "") or "")
        sender_domain = _email_domain(sender_addr)
        sender_ref = (
            self.redactor.redact_display_name(sender_name)
            if sender_name
            else self.redactor.redact_email_address(sender_addr)
        )
        recipient_addresses = [
            addr
            for _, addr in getaddresses([headers.get("To", "") or ""])
            if addr
        ]
        recipients_refs = [
            self.redactor.redact_email_address(addr)
            for addr in recipient_addresses
        ]
        cc_addresses = [
            addr
            for _, addr in getaddresses([headers.get("Cc", "") or ""])
            if addr
        ]
        cc_refs = [
            self.redactor.redact_email_address(addr)
            for addr in cc_addresses
        ]

        thread_id = msg.get("threadId")
        thread_ref = f"thread:gmail:{thread_id}" if thread_id else None

        body_doc_ref = None
        body_facts: list[Any] = []
        if self.fetch_bodies:
            body_text = _extract_body(payload)
            if body_text:
                body_text, body_attrs = self.redactor.redact_body(body_text)
                body_doc_ref = f"doc:gmail:{mid}"
                body_facts.append(NodeFact(
                    node_id=body_doc_ref,
                    subtype="document",
                    attrs={
                        "doc_id": body_doc_ref,
                        "title": subject or f"email {mid}",
                        "text": body_text,
                        "format": "email_body",
                        **body_attrs,
                    },
                    source_record_id={"message_id": mid, "part": "body"},
                    source_revision={"content_sha": _sha(body_text)},
                ))

        yield NodeFact(
            node_id=email_id,
            subtype="email",
            attrs={
                "email_id": email_id,
                "subject": subject,
                "snippet": snippet,
                "date": date_iso,
                "sender_ref": sender_ref,
                "sender_domain": sender_domain,
                "recipients_refs": recipients_refs,
                "recipients_domains": _email_domains(recipient_addresses),
                "cc_refs": cc_refs,
                "cc_domains": _email_domains(cc_addresses),
                "thread_ref": thread_ref,
                "body_doc_ref": body_doc_ref,
                "labels": msg_labels,
                "external_ref": mid,
            },
            source_record_id={"message_id": mid},
            source_revision={"content_sha": _sha(repr(msg))},
        )
        yield from body_facts

        if thread_id:
            yield EdgeFact(
                src=f"thread:gmail:{thread_id}",
                dst=email_id,
                edge_type="contains",
                source_record_id={"thread_id": thread_id},
            )
            agg = threads.setdefault(thread_id, {
                "subject": subject,
                "first_date": date_iso,
                "latest_date": date_iso,
                "participants": set(),
                "participant_domains": set(),
                "count": 0,
            })
            agg["count"] += 1
            agg["participants"].add(sender_ref)
            agg["participants"].update(recipients_refs)
            if sender_domain:
                agg["participant_domains"].add(sender_domain)
            agg["participant_domains"].update(
                _email_domains(recipient_addresses),
            )
            if date_iso:
                if not agg["first_date"] or date_iso < agg["first_date"]:
                    agg["first_date"] = date_iso
                if not agg["latest_date"] or date_iso > agg["latest_date"]:
                    agg["latest_date"] = date_iso
                    agg["subject"] = subject

        yield ProgressMarker(record_id=mid, fingerprint=_sha(repr(msg)))


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _email_domain(email: str) -> str:
    _, sep, domain = (email or "").partition("@")
    if not sep:
        return ""
    return domain.strip().lower()


def _email_domains(addresses: Iterable[str]) -> list[str]:
    return sorted({domain for addr in addresses if (domain := _email_domain(addr))})


def _date_to_iso(date_header: str) -> str:
    if not date_header:
        return ""
    try:
        return parsedate_to_datetime(date_header).isoformat()
    except (TypeError, ValueError):
        return date_header


def _extract_body(payload: dict[str, Any]) -> str:
    """Walk MIME parts; prefer text/plain, fall back to text/html.

    Gmail base64url-encodes part bodies in `body.data`.
    """
    plain: list[str] = []
    html: list[str] = []
    stack = [payload]
    while stack:
        part = stack.pop()
        mime = part.get("mimeType", "")
        data = (part.get("body") or {}).get("data")
        if data:
            text = _b64url_decode(data)
            if mime == "text/plain":
                plain.append(text)
            elif mime == "text/html":
                html.append(text)
        stack.extend(part.get("parts", []) or [])
    return "\n".join(plain) if plain else "\n".join(html)


def _b64url_decode(data: str) -> str:
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
