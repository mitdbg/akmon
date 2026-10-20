"""Typed shape for a lens definition's ``lens_promotion_mapping``.

The mapping is stored as JSONB on ``okg.lens_definitions``; this
module parses it into frozen dataclasses with ``__post_init__``
validation so malformed mappings fail at registration time rather
than at promotion time. Living in its own module avoids the
circular import between ``promote.py`` (consumes at promotion)
and ``putback.py`` (validates at register time).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .putback import LensPutbackError

_SUPPORTED_MUTATION_KINDS = frozenset({"node_attr"})


class LensPromotionMappingError(LensPutbackError):
    """Raised when a mapping is malformed.

    Subclasses :class:`LensPutbackError` so it integrates with the
    existing lens-error hierarchy without depending on
    :mod:`promote` (which would create a circular import between
    ``promote.py``, ``putback.py``, and this module).
    """


@dataclass(frozen=True)
class PromotionMutation:
    """One mutation produced by a matched rule."""

    kind: str
    subtype: str
    attr_field: str
    node_id_template: str = "{row_key}"

    def __post_init__(self) -> None:
        if self.kind not in _SUPPORTED_MUTATION_KINDS:
            raise LensPromotionMappingError(
                f"unsupported mutation.kind {self.kind!r} "
                f"(only {sorted(_SUPPORTED_MUTATION_KINDS)} supported)"
            )
        if not self.subtype.strip():
            raise LensPromotionMappingError(
                "mutation.subtype is required"
            )
        if not self.attr_field.strip():
            raise LensPromotionMappingError(
                "mutation.attr_field is required for node_attr"
            )
        if not self.node_id_template.strip():
            raise LensPromotionMappingError(
                "mutation.node_id_template must be non-empty"
            )

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PromotionMutation":
        if not isinstance(raw, Mapping):
            raise LensPromotionMappingError(
                f"rule.mutation must be an object; got "
                f"{type(raw).__name__}"
            )
        kind = str(raw.get("kind") or "")
        subtype = str(raw.get("subtype") or "")
        attr_field = str(raw.get("attr_field") or "")
        node_id_template = str(raw.get("node_id_template") or "{row_key}")
        return cls(
            kind=kind,
            subtype=subtype,
            attr_field=attr_field,
            node_id_template=node_id_template,
        )


@dataclass(frozen=True)
class PromotionRule:
    """One slot-or-path-keyed rule.

    A rule either matches a specific overlay slot, a specific field
    path, both, or any active overlay (when both are unset). The
    first rule that matches an overlay row wins.
    """

    id: str
    mutation: PromotionMutation
    slot_name: str | None = None
    field_path: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise LensPromotionMappingError(
                "rule.id is required and must be non-empty"
            )

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PromotionRule":
        if not isinstance(raw, Mapping):
            raise LensPromotionMappingError(
                f"each rule must be an object; got "
                f"{type(raw).__name__}"
            )
        rule_id = str(raw.get("id") or "")
        slot_name = raw.get("slot_name")
        if slot_name is not None and not isinstance(slot_name, str):
            raise LensPromotionMappingError(
                f"rule {rule_id!r}: slot_name must be a string"
            )
        field_path_raw = raw.get("field_path")
        if field_path_raw is None:
            field_path: tuple[str, ...] | None = None
        else:
            if not isinstance(field_path_raw, list):
                raise LensPromotionMappingError(
                    f"rule {rule_id!r}: field_path must be a list"
                )
            field_path = tuple(str(p) for p in field_path_raw)
        mutation_raw = raw.get("mutation")
        if mutation_raw is None:
            raise LensPromotionMappingError(
                f"rule {rule_id!r}: rule missing 'mutation' object"
            )
        mutation = PromotionMutation.from_dict(mutation_raw)
        return cls(
            id=rule_id,
            mutation=mutation,
            slot_name=slot_name,
            field_path=field_path,
        )


@dataclass(frozen=True)
class LensPromotionMapping:
    """Parsed + validated ``lens_promotion_mapping`` payload."""

    version: int
    rules: tuple[PromotionRule, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.version, int) or self.version < 1:
            raise LensPromotionMappingError(
                f"mapping.version must be an int >= 1; got "
                f"{self.version!r}"
            )
        if not self.rules:
            raise LensPromotionMappingError(
                "mapping must contain a non-empty 'rules' list"
            )
        seen_ids: set[str] = set()
        for rule in self.rules:
            if rule.id in seen_ids:
                raise LensPromotionMappingError(
                    f"duplicate rule id {rule.id!r}; rule ids must be "
                    "unique within a mapping"
                )
            seen_ids.add(rule.id)

    @classmethod
    def from_dict(
        cls,
        raw: Mapping[str, Any],
        *,
        definition_id: str | None = None,
    ) -> "LensPromotionMapping":
        """Parse a JSONB mapping dict; raise on malformed input.

        ``definition_id`` is woven into the error messages so the
        operator can locate the bad row.
        """

        prefix = (
            f"lens_promotion_mapping for {definition_id!r}: "
            if definition_id
            else ""
        )
        try:
            if not isinstance(raw, Mapping):
                raise LensPromotionMappingError(
                    f"{prefix}mapping must be an object; got "
                    f"{type(raw).__name__}"
                )
            version = raw.get("version")
            if not isinstance(version, int):
                raise LensPromotionMappingError(
                    f"{prefix}mapping.version must be an int; got "
                    f"{type(version).__name__}"
                )
            rules_raw = raw.get("rules")
            if not isinstance(rules_raw, list):
                raise LensPromotionMappingError(
                    f"{prefix}mapping.rules must be a list"
                )
            rules = tuple(
                PromotionRule.from_dict(rule_raw) for rule_raw in rules_raw
            )
            return cls(version=version, rules=rules)
        except LensPromotionMappingError as exc:
            if definition_id and prefix not in str(exc):
                raise LensPromotionMappingError(
                    f"{prefix}{exc}"
                ) from exc
            raise
