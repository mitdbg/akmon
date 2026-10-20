"""Deterministic route recommendation engine.

:func:`recommend_routes` is a pure function from a recommendation
context (file, file_extension, change, deployment, …) to an ordered
list of matched routes. The function does no I/O; the route manifest
is supplied by the caller (typically loaded once via
:func:`okg.substrate.routes.load_manifest_for_deployment`).

Match semantics (first cut — deliberately narrow):

* ``when`` is a flat mapping of key → value.
* For each key, the context value at the same key must equal the
  manifest value. Missing context keys never match.
* Special key ``file_extension`` is normalised so a manifest entry
  ``".py"`` matches a context ``file_extension`` of ``"py"`` or
  ``".py"``.
* Special key ``change_starts_with`` matches when
  ``context["change"]`` is a string that starts with the supplied
  prefix.
* All conditions in the ``when`` clause must hold for the rule to
  fire (AND semantics).

Richer matchers (filter expressions, score functions, weighted
ranking) ride on Phase 6.3 trace evidence: once the trace loop
records which recommended routes actually got followed, the
substrate has measured ground for promoting them.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .manifest import RouteManifest, RouteRule


@dataclass(frozen=True)
class RouteRecommendationContext:
    """Inputs the recommender matches against.

    Keep this dataclass narrow; if a new contextual signal earns its
    keep on Phase 6.3 trace evidence, promote it from
    ``extras`` to a named field.
    """

    deployment: str
    change: str | None = None
    file: str | None = None
    file_extension: str | None = None
    cwd: str | None = None
    extras: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        base: dict[str, Any] = {
            "deployment": self.deployment,
            "change": self.change,
            "file": self.file,
            "file_extension": self.file_extension,
            "cwd": self.cwd,
        }
        base.update(dict(self.extras))
        return base


@dataclass(frozen=True)
class RouteRecommendation:
    """One matched route. Order in the result list = priority."""

    route_id: str
    deployment: str
    rationale: str
    recommend: Mapping[str, Any]
    priority: int
    manifest_checksum: str

    def to_json(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "deployment": self.deployment,
            "rationale": self.rationale,
            "recommend": dict(self.recommend),
            "priority": self.priority,
            "manifest_checksum": self.manifest_checksum,
        }


def _normalise_file_extension(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if not s.startswith("."):
        s = "." + s
    return s.lower()


def _rule_matches(
    rule: RouteRule,
    context: Mapping[str, Any],
) -> bool:
    for key, expected in rule.when.items():
        if key == "file_extension":
            ctx_ext = _normalise_file_extension(context.get("file_extension"))
            exp_ext = _normalise_file_extension(expected)
            if ctx_ext is None or ctx_ext != exp_ext:
                return False
            continue
        if key == "change_starts_with":
            change = context.get("change")
            if not isinstance(change, str):
                return False
            if not change.startswith(str(expected)):
                return False
            continue
        # Default: strict equality on the named context field.
        if key not in context:
            return False
        if context[key] != expected:
            return False
    return True


def recommend_routes(
    manifest: RouteManifest | None,
    context: RouteRecommendationContext | Mapping[str, Any],
    *,
    limit: int | None = None,
) -> list[RouteRecommendation]:
    """Return ordered route recommendations matching ``context``.

    Returns an empty list when ``manifest`` is None (deployment has
    no routes.yaml) or when no rule matches.
    """
    if manifest is None:
        return []
    if isinstance(context, RouteRecommendationContext):
        deployment = context.deployment
        ctx_dict = context.as_dict()
    else:
        ctx_dict = dict(context)
        deployment_val = ctx_dict.get("deployment")
        if not isinstance(deployment_val, str) or not deployment_val:
            raise ValueError(
                "context must include a non-empty 'deployment' key"
            )
        deployment = deployment_val

    matched: list[RouteRecommendation] = []
    for rule in manifest.routes:
        if not _rule_matches(rule, ctx_dict):
            continue
        matched.append(
            RouteRecommendation(
                route_id=rule.id,
                deployment=deployment,
                rationale=rule.rationale,
                recommend=dict(rule.recommend),
                priority=rule.priority,
                manifest_checksum=manifest.checksum,
            )
        )
    matched.sort(key=lambda r: (r.priority, r.route_id))
    if limit is not None and limit >= 0:
        matched = matched[:limit]
    return matched
