"""Email (RFC822) parser. Wraps stdlib `email` — no third-party.

Wrap-invariant: zero third-party imports here; stdlib only.
"""

from __future__ import annotations

from email import message_from_bytes, message_from_string
from email.message import Message
from typing import Any

from okg.substrate.library.extraction._protocols import (
    ParsedDocument,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


class EmailParser:
    """RFC822 message -> ParsedDocument via stdlib `email`. The
    body is the best-effort decoded text/plain part; if the
    message is multipart/alternative without text/plain, the
    text/html part is decoded and returned verbatim (no
    HTML-stripping here — caller composes that with
    normalize.strip_html)."""

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, str):
            msg: Message = message_from_string(content)
        elif isinstance(content, (bytes, bytearray)):
            msg = message_from_bytes(bytes(content))
        else:
            raise TypeError(
                f"EmailParser.parse expected str/bytes; got {type(content).__name__}"
            )

        metadata: dict[str, Any] = {
            "from": msg.get("From"),
            "to": msg.get("To"),
            "cc": msg.get("Cc"),
            "subject": msg.get("Subject"),
            "date": msg.get("Date"),
            "message_id": msg.get("Message-ID"),
            "in_reply_to": msg.get("In-Reply-To"),
        }
        metadata = {k: v for k, v in metadata.items() if v is not None}

        body = self._extract_body(msg)
        return ParsedDocument(text=body, metadata=metadata)

    @staticmethod
    def _extract_body(msg: Message) -> str:
        if msg.is_multipart():
            # Prefer text/plain.
            for part in msg.walk():
                if (
                    part.get_content_type() == "text/plain"
                    and not part.get("Content-Disposition", "").startswith("attachment")
                ):
                    return _decode_part(part)
            # Fall back to text/html as raw text — caller can strip.
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    return _decode_part(part)
            return ""
        return _decode_part(msg)


def _decode_part(part: Message) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        s = part.get_payload()
        return s if isinstance(s, str) else str(s)
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except (LookupError, UnicodeDecodeError):
        return payload.decode("utf-8", errors="replace")


register_parser("email", EmailParser)
register_parser("eml", EmailParser)


__all__ = ["EmailParser"]
