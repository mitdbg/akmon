"""Redaction layer for Tier-C connectors.

Applied at source-emit time:
- Body truncation (default 50KB cap; truncated Documents
  carry `truncated: true` + `original_byte_count`).
- Optional PII hashing (sender / recipient display names →
  sha256 prefix when `pii_mode: hashed`).

Gmail, Google Calendar, Slack all route through `Redactor`.
Future connectors (Notion, Linear) inherit.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass
class Redactor:
    """Applies body truncation + optional PII hashing."""

    max_body_kb: int = 50
    pii_mode: str = "plain"  # 'plain' | 'hashed'

    def redact_body(self, body: str) -> tuple[str, dict]:
        """Returns (possibly-truncated-body, attrs-to-merge-on-doc).

        attrs always includes `original_byte_count`; includes
        `truncated: true` when the body was actually truncated.
        """
        original = len(body.encode("utf-8"))
        cap = self.max_body_kb * 1024
        attrs = {"original_byte_count": original}
        if original > cap:
            # Truncate by chars conservatively — UTF-8 boundary
            # rounding is good enough for analytical use.
            return body[:cap], {**attrs, "truncated": True}
        return body, attrs

    def redact_display_name(self, name: str) -> str:
        """Hash the name if pii_mode=hashed; else passthrough."""
        if self.pii_mode != "hashed" or not name:
            return name
        return "hash:" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:16]

    def redact_email_address(self, email: str) -> str:
        """Hash local-part if pii_mode=hashed; keep domain."""
        if self.pii_mode != "hashed" or not email or "@" not in email:
            return email
        local, _, domain = email.partition("@")
        local_hash = hashlib.sha256(local.encode("utf-8")).hexdigest()[:16]
        return f"hash:{local_hash}@{domain}"
