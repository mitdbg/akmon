"""Inline native view slot embed directives.

View markdown can reference a declared output slot, such as a
relation-backed table, without embedding SQL or physical relation names
in the markdown document. The directive is parsed before markdown
rendering because the viz markdown renderer deliberately disables raw
HTML.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Mapping


VIEW_SLOT_EMBED_MAX_LIMIT = 500
_DIRECTIVE_RE = re.compile(
    r"^\s*<!--\s*okg:view-slot\s+(\{.*\})\s*-->\s*$",
    re.DOTALL,
)
_SLOT_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]*$")
_SQL_SHAPED_RE = re.compile(
    r"\b("
    r"select|insert|update|delete|drop|alter|create|truncate|merge|"
    r"with|from|join|schema|table|relation"
    r")\b",
    re.IGNORECASE,
)
_ALLOWED_KEYS = {
    "slot_name",
    "kind",
    "scope_key",
    "row_key",
    "limit",
    "title",
    "open",
}
_OPTIONAL_STRING_KEYS = {"scope_key", "row_key", "title"}


class ViewSlotEmbedError(ValueError):
    """Raised when a view-slot embed directive is invalid."""


@dataclass(frozen=True)
class ViewSlotEmbed:
    """Validated inline native view slot reference."""

    slot_name: str
    kind: str = "relation"
    scope_key: str | None = None
    row_key: str | None = None
    limit: int | None = None
    title: str | None = None
    open: bool | None = None

    def __post_init__(self) -> None:
        _validate_slot_name(self.slot_name)
        if self.kind != "relation":
            raise ViewSlotEmbedError(
                "okg:view-slot currently supports kind='relation' only"
            )
        for key in _OPTIONAL_STRING_KEYS:
            value = getattr(self, key)
            if value is None:
                continue
            if not isinstance(value, str):
                raise ViewSlotEmbedError(f"{key} must be a string")
            _reject_sql_shaped_value(key, value)
        if self.limit is not None:
            if not isinstance(self.limit, int) or isinstance(self.limit, bool):
                raise ViewSlotEmbedError("limit must be an integer")
            if self.limit <= 0:
                raise ViewSlotEmbedError("limit must be positive")
            if self.limit > VIEW_SLOT_EMBED_MAX_LIMIT:
                raise ViewSlotEmbedError(
                    f"limit must be <= {VIEW_SLOT_EMBED_MAX_LIMIT}"
                )
        if self.open is not None and not isinstance(self.open, bool):
            raise ViewSlotEmbedError("open must be a boolean")

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "slot_name": self.slot_name,
            "kind": self.kind,
        }
        for key in ("scope_key", "row_key", "limit", "title", "open"):
            value = getattr(self, key)
            if value is not None:
                payload[key] = value
        return payload

    def to_directive(self) -> str:
        return view_slot_embed_directive(**self.to_payload())


def view_slot_embed_directive(
    *,
    slot_name: str,
    kind: str = "relation",
    scope_key: str | None = None,
    row_key: str | None = None,
    limit: int | None = None,
    title: str | None = None,
    open: bool | None = None,
) -> str:
    """Return a validated ``okg:view-slot`` markdown directive."""

    embed = ViewSlotEmbed(
        slot_name=slot_name,
        kind=kind,
        scope_key=scope_key,
        row_key=row_key,
        limit=limit,
        title=title,
        open=open,
    )
    body = json.dumps(embed.to_payload(), sort_keys=True, separators=(",", ":"))
    return f"<!--okg:view-slot {body} -->"


def parse_view_slot_embed_directive(text: str) -> ViewSlotEmbed:
    """Parse and validate one raw ``okg:view-slot`` directive."""

    match = _DIRECTIVE_RE.match(text)
    if not match:
        raise ViewSlotEmbedError("not an okg:view-slot directive")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise ViewSlotEmbedError("directive payload must be JSON") from exc
    if not isinstance(payload, Mapping):
        raise ViewSlotEmbedError("directive payload must be an object")
    return view_slot_embed_from_payload(payload)


def view_slot_embed_from_payload(payload: Mapping[str, Any]) -> ViewSlotEmbed:
    """Build a validated embed from a JSON-like payload."""

    unknown = sorted(set(payload) - _ALLOWED_KEYS)
    if unknown:
        raise ViewSlotEmbedError(f"unsupported directive fields: {unknown}")
    if "slot_name" not in payload:
        raise ViewSlotEmbedError("slot_name is required")
    return ViewSlotEmbed(
        slot_name=_string_field(payload, "slot_name"),
        kind=str(payload.get("kind") or "relation"),
        scope_key=_optional_string_field(payload, "scope_key"),
        row_key=_optional_string_field(payload, "row_key"),
        limit=payload.get("limit"),
        title=_optional_string_field(payload, "title"),
        open=payload.get("open"),
    )


def _string_field(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ViewSlotEmbedError(f"{key} must be a non-empty string")
    _reject_sql_shaped_value(key, value)
    return value


def _optional_string_field(
    payload: Mapping[str, Any],
    key: str,
) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ViewSlotEmbedError(f"{key} must be a non-empty string")
    _reject_sql_shaped_value(key, value)
    return value


def _validate_slot_name(slot_name: str) -> None:
    if not isinstance(slot_name, str) or not slot_name:
        raise ViewSlotEmbedError("slot_name must be a non-empty string")
    if not _SLOT_NAME_RE.fullmatch(slot_name):
        raise ViewSlotEmbedError("slot_name contains unsupported characters")
    _reject_sql_shaped_value("slot_name", slot_name)


def _reject_sql_shaped_value(key: str, value: str) -> None:
    if _SQL_SHAPED_RE.search(value):
        raise ViewSlotEmbedError(
            f"{key} must not contain SQL-shaped text or relation identifiers"
        )


__all__ = [
    "VIEW_SLOT_EMBED_MAX_LIMIT",
    "ViewSlotEmbed",
    "ViewSlotEmbedError",
    "parse_view_slot_embed_directive",
    "view_slot_embed_directive",
    "view_slot_embed_from_payload",
]
