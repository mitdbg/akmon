"""slack_api source — Slack Web API with channel allowlist.

Per `refine-academic-pi-with-comms-and-calendar` Tier C:
emits ChatChannel + ChatMessage from operator-configured
channels only. DMs + private groups NEVER ingested
(scopes minimized — no `im:history` / `groups:history`).

Uses substrate OAuth library + Redactor for body truncation.

Scaffold — full pagination + thread-handling + user-cache
require careful work. Channel-allowlist enforcement +
emission shape committed.
"""
from __future__ import annotations

import hashlib
import logging as _logging
from typing import Any, Iterable, Optional

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
    SourceRun,
)
from okg.substrate.library.oauth.providers import SLACK
from okg.substrate.library.oauth.refresh import RefreshHandler
from okg.substrate.library.oauth.redaction import Redactor


class SlackApiSource:
    """Slack Web API connector. Channel-allowlist required."""

    name: str = "slack_api"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        deployment_name: str,
        slack_channels: Optional[list[str]] = None,
        pii_mode: str = "plain",
        max_body_kb: int = 50,
        ownership_id: str = "slack_api",
        **_ignored: Any,
    ) -> None:
        self.deployment_name = deployment_name
        self.slack_channels = slack_channels or []
        self.redactor = Redactor(
            max_body_kb=max_body_kb, pii_mode=pii_mode,
        )
        self.ownership_id = ownership_id

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        """OAuth preflight for `okg doctor --check source-auth`
        (dogfood F-7a)."""
        from okg.substrate.library.oauth.preflight import oauth_preflight
        return oauth_preflight(
            source_name=self.name,
            deployment_name=self.deployment_name,
            provider=SLACK,
        )

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if not self.slack_channels:
            import logging
            logging.getLogger(__name__).warning(
                "slack_api: no slack_channels configured; refusing "
                "to ingest (per strict-allowlist contract). Set "
                "`slack_channels` in source_registry.yaml."
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        return SourceRun(facts=self._stream(), completed_scope=True)

    def _stream(self) -> Iterable[Any]:
        # RefreshHandler construction failure is unrecoverable —
        # token revoked / OAuth broken / cred store unreachable.
        # Let it propagate so the ingest fails loud rather than
        # silently producing zero facts.
        refresh = RefreshHandler(
            provider=SLACK, deployment_name=self.deployment_name,
        )

        log = _logging.getLogger(__name__)
        for ch_id in self.slack_channels:
            try:
                ch_info = refresh.authed_request(
                    f"https://slack.com/api/conversations.info?channel={ch_id}",
                )
            except Exception:  # noqa: BLE001 — one channel failure shouldn't block the whole sync
                log.exception(
                    "slack_api: conversations.info failed for channel %s",
                    ch_id,
                )
                continue
            ch = (ch_info or {}).get("channel") or {}
            ch_node_id = f"chat_channel:slack:{ch_id}"
            yield NodeFact(
                node_id=ch_node_id,
                subtype="chat_channel",
                attrs={
                    "channel_id": ch_node_id,
                    "platform": "slack",
                    "name": ch.get("name", ch_id),
                    "external_ref": ch_id,
                },
                source_record_id={"channel_id": ch_id},
                source_revision={"content_sha": _sha(str(ch))},
            )

            try:
                hist = refresh.authed_request(
                    f"https://slack.com/api/conversations.history?"
                    f"channel={ch_id}&limit=200",
                )
            except Exception:  # noqa: BLE001 — one channel failure shouldn't block the whole sync
                log.exception(
                    "slack_api: conversations.history failed for channel %s",
                    ch_id,
                )
                continue
            for msg in (hist or {}).get("messages", []) or []:
                ts = msg.get("ts", "")
                msg_id = f"chat_message:slack:{ch_id}:{ts}"
                body = msg.get("text", "") or ""
                content_hash = hashlib.sha256(
                    body.encode("utf-8")
                ).hexdigest()
                yield NodeFact(
                    node_id=msg_id,
                    subtype="chat_message",
                    attrs={
                        "message_id": msg_id,
                        "channel_ref": ch_node_id,
                        "sender_ref": msg.get("user", ""),
                        "ts": ts,
                        "external_ref": ts,
                    },
                    source_record_id={"channel_id": ch_id, "ts": ts},
                    source_revision={"content_sha": content_hash},
                )
                yield EdgeFact(
                    src=ch_node_id, dst=msg_id, edge_type="contains",
                    source_record_id={"channel_id": ch_id, "ts": ts},
                )
                yield ProgressMarker(
                    record_id=f"{ch_id}:{ts}", fingerprint=content_hash,
                )


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
