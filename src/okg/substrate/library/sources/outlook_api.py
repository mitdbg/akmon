"""outlook_mail_api + outlook_calendar_api — Microsoft Graph delta sources.

Per `add-msgraph-sources`: mirrors the Google sources' contracts
for M365-tenant universities. One tenant-internal Entra app (the
shared `msgraph` OAuth provider, delegated Mail.Read +
Calendars.Read) serves both sources.

Contracts mirrored from gmail_api / google_calendar_api /
calendar_ics:
  - strict allowlists (folders / calendar names); unconfigured
    means a loud warning + zero rows, never a silent empty run
  - Redactor on mail bodies (truncation + PII hashing)
  - EmailThread aggregation via conversationId
  - event allowlist (okg: tag or recurring-pattern match)
  - incremental sync via Graph delta queries; the deltaLink for
    each folder/calendar persists in the runner's cursor blob
  - a stale delta token (410 Gone) restarts that folder's walk
    loudly, never silently

DEVIATION recorded during implementation (mirrors
finish-gmail-source): `msal` adoption dropped — Entra v2 token
refresh is a standard OAuth2 refresh-grant POST that the
substrate RefreshHandler already implements against the keyring
store; a second auth stack around the same token adds surface,
not safety. Device-code login for headless installs is an
install-flow follow-on, not a source concern.

Cursor mechanics: Graph only reveals the deltaLink on the LAST
page of a walk, but the runner binds `SourceRun.next_cursor`
before consuming facts and persists it to the watermark after.
The sources therefore hand the runner a mutable dict and fill it
in during the fact stream — by watermark time it carries the
fresh deltaLinks. Do not replace that dict with a new object
mid-run.
"""
from __future__ import annotations

import hashlib
import logging
import re
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, Optional

from okg.substrate.library.oauth.providers import MSGRAPH
from okg.substrate.library.oauth.redaction import Redactor
from okg.substrate.library.oauth.refresh import RefreshHandler
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
    SourceRun,
)
from okg.substrate.library.sources.calendar_ics import (
    _DEFAULT_RECURRING_PATTERNS,
    _infer_kind,
)

_log = logging.getLogger(__name__)

_API = "https://graph.microsoft.com/v1.0"

_MAIL_SELECT = (
    "subject,from,toRecipients,ccRecipients,receivedDateTime,"
    "conversationId,bodyPreview,body"
)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _email_domain(email: str) -> str:
    _, sep, domain = (email or "").partition("@")
    if not sep:
        return ""
    return domain.strip().lower()


def _email_domains(addresses: Iterable[str]) -> list[str]:
    return sorted({domain for addr in addresses if (domain := _email_domain(addr))})


class _GraphDeltaWalker:
    """Shared delta-page walk with loud 410 (stale token) restart."""

    def __init__(self, refresh: RefreshHandler, source_name: str) -> None:
        self.refresh = refresh
        self.source_name = source_name

    def walk(
        self,
        scope_label: str,
        fresh_url: str,
        prior_deltalink: Optional[str],
        deltalinks_out: dict[str, str],
    ) -> Iterator[dict[str, Any]]:
        """Yield delta items for one folder/calendar; record the
        closing deltaLink into `deltalinks_out[scope_label]`."""
        url = prior_deltalink or fresh_url
        restarted = False
        while url:
            try:
                page = self.refresh.authed_request(url)
            except urllib.error.HTTPError as exc:
                if exc.code == 410 and not restarted:
                    # Graph sync contract: 410 Gone means the delta
                    # token expired; restart the full walk. Loud by
                    # contract — never a silent empty delta.
                    _log.warning(
                        "%s: delta token for %s is stale (410); "
                        "restarting full delta walk",
                        self.source_name,
                        scope_label,
                    )
                    url = fresh_url
                    restarted = True
                    continue
                raise
            for item in page.get("value", []) or []:
                if "@removed" in item:
                    # Additive-only (mirrors gmail_api): deletions
                    # are reconciliation's job, not the delta walk's.
                    continue
                yield item
            if page.get("@odata.deltaLink"):
                deltalinks_out[scope_label] = page["@odata.deltaLink"]
                return
            url = page.get("@odata.nextLink")


class OutlookMailApiSource:
    """Graph /messages/delta connector. Folder allowlist required."""

    name: str = "outlook_mail_api"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        deployment_name: str,
        outlook_folders: Optional[list[str]] = None,
        pii_mode: str = "plain",
        max_body_kb: int = 50,
        page_size: int = 50,
        ownership_id: str = "outlook_mail_api",
        refresh_handler: Optional[RefreshHandler] = None,
        **_ignored: Any,
    ) -> None:
        self.deployment_name = deployment_name
        self.outlook_folders = outlook_folders or []
        self.redactor = Redactor(max_body_kb=max_body_kb, pii_mode=pii_mode)
        self.page_size = page_size
        self.ownership_id = ownership_id
        self._refresh_handler = refresh_handler

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        from okg.substrate.library.oauth.preflight import oauth_preflight
        return oauth_preflight(
            source_name=self.name,
            deployment_name=self.deployment_name,
            provider=MSGRAPH,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        **_ignored: Any,
    ) -> SourceRun:
        if not self.outlook_folders:
            _log.warning(
                "outlook_mail_api: no outlook_folders configured; "
                "refusing to ingest (per strict-allowlist contract). "
                "Set `outlook_folders` in source_registry.yaml."
            )
            return SourceRun(facts=iter([]), completed_scope=True)

        refresh = self._refresh_handler or RefreshHandler(
            provider=MSGRAPH, deployment_name=self.deployment_name,
        )
        # Eager folder resolution: auth failures and bad folder
        # names surface here, before facts stream.
        folders = self._resolve_folders(refresh)
        prior = dict((cursor or {}).get("mail_deltalinks") or {})
        # Mutable on purpose — see module docstring (deltaLinks land
        # during the walk; the runner persists this dict afterward).
        next_cursor: dict[str, Any] = {"mail_deltalinks": dict(prior)}
        return SourceRun(
            facts=self._stream(
                refresh, folders, prior, next_cursor["mail_deltalinks"],
            ),
            completed_scope=not bool(prior),
            next_cursor=next_cursor,
        )

    def _resolve_folders(
        self, refresh: RefreshHandler,
    ) -> dict[str, str]:
        """Map allowlisted displayNames -> folder ids; missing
        names warn loudly."""
        page = refresh.authed_request(
            f"{_API}/me/mailFolders?$top=200&$select=id,displayName"
        )
        by_name = {
            (f.get("displayName") or "").lower(): f["id"]
            for f in page.get("value", []) or []
            if f.get("id")
        }
        resolved: dict[str, str] = {}
        for name in self.outlook_folders:
            fid = by_name.get(name.lower())
            if fid is None:
                _log.warning(
                    "outlook_mail_api: allowlisted folder %r not "
                    "found in mailbox; skipping it loudly",
                    name,
                )
                continue
            resolved[name] = fid
        return resolved

    def _stream(
        self,
        refresh: RefreshHandler,
        folders: dict[str, str],
        prior: dict[str, str],
        deltalinks_out: dict[str, str],
    ) -> Iterator[Any]:
        walker = _GraphDeltaWalker(refresh, self.name)
        threads: dict[str, dict[str, Any]] = {}
        for folder_name, fid in folders.items():
            fresh_url = (
                f"{_API}/me/mailFolders/{fid}/messages/delta"
                f"?$select={_MAIL_SELECT}&$top={self.page_size}"
            )
            for item in walker.walk(
                folder_name, fresh_url, prior.get(folder_name),
                deltalinks_out,
            ):
                yield from self._emit_message(item, folder_name, threads)

        for cid, agg in threads.items():
            thread_node_id = f"thread:outlook:{cid}"
            fingerprint = _sha(repr(sorted(agg.items())))
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
                    "external_ref": cid,
                },
                source_record_id={"thread_id": cid},
                source_revision={"content_sha": fingerprint},
            )
            yield ProgressMarker(
                record_id=f"thread:{cid}", fingerprint=fingerprint,
            )

    def _emit_message(
        self,
        item: dict[str, Any],
        folder_name: str,
        threads: dict[str, dict[str, Any]],
    ) -> Iterator[Any]:
        mid = item["id"]
        email_id = f"email:outlook:{mid}"
        subject = item.get("subject", "") or ""
        date_iso = item.get("receivedDateTime", "") or ""
        snippet = (item.get("bodyPreview", "") or "")[:200]

        sender = (item.get("from") or {}).get("emailAddress") or {}
        sender_address = sender.get("address") or ""
        sender_domain = _email_domain(sender_address)
        sender_ref = (
            self.redactor.redact_display_name(sender.get("name") or "")
            or self.redactor.redact_email_address(sender_address)
        )
        recipient_addresses = [
            (r.get("emailAddress") or {}).get("address") or ""
            for r in item.get("toRecipients", []) or []
        ]
        recipients_refs = [
            self.redactor.redact_email_address(addr)
            for addr in recipient_addresses
        ]
        cc_addresses = [
            (r.get("emailAddress") or {}).get("address") or ""
            for r in item.get("ccRecipients", []) or []
        ]
        cc_refs = [
            self.redactor.redact_email_address(addr)
            for addr in cc_addresses
        ]

        cid = item.get("conversationId")
        thread_ref = f"thread:outlook:{cid}" if cid else None

        body_doc_ref = None
        body_facts: list[Any] = []
        body_content = (item.get("body") or {}).get("content") or ""
        if body_content:
            body_text, body_attrs = self.redactor.redact_body(body_content)
            body_doc_ref = f"doc:outlook:{mid}"
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
                "recipients_refs": [r for r in recipients_refs if r],
                "recipients_domains": _email_domains(recipient_addresses),
                "cc_refs": [r for r in cc_refs if r],
                "cc_domains": _email_domains(cc_addresses),
                "thread_ref": thread_ref,
                "body_doc_ref": body_doc_ref,
                "labels": [folder_name],
                "external_ref": mid,
            },
            source_record_id={"message_id": mid},
            source_revision={"content_sha": _sha(repr(item))},
        )
        yield from body_facts

        if cid:
            yield EdgeFact(
                src=f"thread:outlook:{cid}",
                dst=email_id,
                edge_type="contains",
                source_record_id={"thread_id": cid},
            )
            agg = threads.setdefault(cid, {
                "subject": subject,
                "first_date": date_iso,
                "latest_date": date_iso,
                "participants": set(),
                "participant_domains": set(),
                "count": 0,
            })
            agg["count"] += 1
            agg["participants"].add(sender_ref)
            agg["participants"].update(r for r in recipients_refs if r)
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

        yield ProgressMarker(record_id=mid, fingerprint=_sha(repr(item)))


class OutlookCalendarApiSource:
    """Graph calendarView/delta connector. Calendar allowlist
    required; event-level allowlist mirrors calendar_ics."""

    name: str = "outlook_calendar_api"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        deployment_name: str,
        outlook_calendars: Optional[list[str]] = None,
        recurring_patterns: Optional[list[str]] = None,
        window_past_months: int = 12,
        window_future_months: int = 18,
        window_start: Optional[str] = None,
        window_end: Optional[str] = None,
        page_size: int = 50,
        ownership_id: str = "outlook_calendar_api",
        refresh_handler: Optional[RefreshHandler] = None,
        **_ignored: Any,
    ) -> None:
        self.deployment_name = deployment_name
        self.outlook_calendars = outlook_calendars or []
        self.patterns = [
            re.compile(p, re.I)
            for p in (recurring_patterns or _DEFAULT_RECURRING_PATTERNS)
        ]
        self.window_past_months = window_past_months
        self.window_future_months = window_future_months
        self.window_start = window_start
        self.window_end = window_end
        self.page_size = page_size
        self.ownership_id = ownership_id
        self._refresh_handler = refresh_handler

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        from okg.substrate.library.oauth.preflight import oauth_preflight
        return oauth_preflight(
            source_name=self.name,
            deployment_name=self.deployment_name,
            provider=MSGRAPH,
        )

    def _window(self) -> tuple[str, str]:
        if self.window_start and self.window_end:
            return self.window_start, self.window_end
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=30 * self.window_past_months)
        end = now + timedelta(days=30 * self.window_future_months)
        return (
            start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        **_ignored: Any,
    ) -> SourceRun:
        if not self.outlook_calendars:
            _log.warning(
                "outlook_calendar_api: no outlook_calendars "
                "configured; refusing to ingest (per strict-allowlist "
                "contract). Set `outlook_calendars` in "
                "source_registry.yaml."
            )
            return SourceRun(facts=iter([]), completed_scope=True)

        refresh = self._refresh_handler or RefreshHandler(
            provider=MSGRAPH, deployment_name=self.deployment_name,
        )
        calendars = self._resolve_calendars(refresh)
        prior = dict((cursor or {}).get("calendar_deltalinks") or {})
        next_cursor: dict[str, Any] = {"calendar_deltalinks": dict(prior)}
        return SourceRun(
            facts=self._stream(
                refresh, calendars, prior,
                next_cursor["calendar_deltalinks"],
            ),
            completed_scope=not bool(prior),
            next_cursor=next_cursor,
        )

    def _resolve_calendars(
        self, refresh: RefreshHandler,
    ) -> dict[str, str]:
        page = refresh.authed_request(
            f"{_API}/me/calendars?$top=50&$select=id,name"
        )
        by_name = {
            (c.get("name") or "").lower(): c["id"]
            for c in page.get("value", []) or []
            if c.get("id")
        }
        resolved: dict[str, str] = {}
        for name in self.outlook_calendars:
            cid = by_name.get(name.lower())
            if cid is None:
                _log.warning(
                    "outlook_calendar_api: allowlisted calendar %r "
                    "not found; skipping it loudly",
                    name,
                )
                continue
            resolved[name] = cid
        return resolved

    def _stream(
        self,
        refresh: RefreshHandler,
        calendars: dict[str, str],
        prior: dict[str, str],
        deltalinks_out: dict[str, str],
    ) -> Iterator[Any]:
        walker = _GraphDeltaWalker(refresh, self.name)
        window_start, window_end = self._window()
        for cal_name, cid in calendars.items():
            feed_id = f"feed:outlook:{cid}"
            yield NodeFact(
                node_id=feed_id,
                subtype="calendar_feed",
                attrs={
                    "feed_id": feed_id,
                    "name": cal_name,
                    "source_kind": "outlook_calendar",
                    "external_ref": cid,
                },
                source_record_id={"calendar_id": cid},
                source_revision={"content_sha": "static"},
            )
            yield ProgressMarker(record_id=feed_id, fingerprint=cid)

            fresh_url = (
                f"{_API}/me/calendars/{cid}/calendarView/delta"
                f"?startDateTime={window_start}&endDateTime={window_end}"
                f"&$top={self.page_size}"
            )
            for item in walker.walk(
                cal_name, fresh_url, prior.get(cal_name), deltalinks_out,
            ):
                yield from self._emit_event(item, feed_id)

    def _emit_event(
        self, item: dict[str, Any], feed_id: str,
    ) -> Iterator[Any]:
        subject = item.get("subject", "") or ""
        preview = item.get("bodyPreview", "") or ""
        text = f"{subject} {preview}"
        if "okg:" not in text.lower() and not any(
            p.search(text) for p in self.patterns
        ):
            return  # PII guard — zero metadata retention
        ev_id_raw = item.get("id", "")
        event_id = f"event:outlook:{ev_id_raw}"
        fingerprint = _sha(repr(item))
        yield NodeFact(
            node_id=event_id,
            subtype="calendar_event",
            attrs={
                "event_id": event_id,
                "title": subject,
                "kind": _infer_kind(subject),
                "start_dt": (item.get("start") or {}).get("dateTime", ""),
                "end_dt": (item.get("end") or {}).get("dateTime", ""),
                "location": (
                    (item.get("location") or {}).get("displayName", "")
                ),
                "external_ref": ev_id_raw,
                "feed_ref": feed_id,
            },
            source_record_id={"event_id": ev_id_raw},
            source_revision={"content_sha": fingerprint},
        )
        yield EdgeFact(
            src=event_id, dst=feed_id, edge_type="at_feed",
            source_record_id={"event_id": ev_id_raw},
        )
        yield ProgressMarker(record_id=ev_id_raw, fingerprint=fingerprint)
