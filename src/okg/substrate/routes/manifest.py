"""Route manifest schema and YAML loader.

A deployment's ``routes.yaml`` file declares route rules. Each rule
has:

* ``id`` — stable identifier (used as ``route_id`` in trace events).
* ``when`` — match predicate (currently simple key/value equality on
  the recommendation context; richer matchers ride on Phase 6.3
  trace evidence).
* ``recommend`` — the tool + argument shape the agent should run
  next.
* ``rationale`` — operator-facing explanation. Required.
* ``priority`` — optional integer (lower = higher priority); defaults
  to the rule's manifest position.

Manifest format::

    version: 1
    routes:
      - id: code-change-test
        when:
          file_extension: ".py"
        recommend:
          tool: pytest
          args: { paths: ["tests/"] }
        rationale: "Python source change -> run focused tests"

The manifest is intentionally narrow in this first cut: matchers are
deterministic key/value equality. The trace-loop evidence from
Phase 6.3 informs what richer matchers (filter expressions, score
functions, etc.) earn their complexity.
"""
from __future__ import annotations
from okg.substrate import fastyaml

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


class RouteManifestError(Exception):
    """Raised when a routes.yaml manifest fails to parse / validate."""


@dataclass(frozen=True)
class RouteRule:
    """One declarative route rule from a routes.yaml manifest."""

    id: str
    when: Mapping[str, Any]
    recommend: Mapping[str, Any]
    rationale: str
    priority: int

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "when": dict(self.when),
            "recommend": dict(self.recommend),
            "rationale": self.rationale,
            "priority": self.priority,
        }


@dataclass(frozen=True)
class RouteManifest:
    """One deployment's set of route rules."""

    version: int
    routes: tuple[RouteRule, ...]
    source_path: Path | None = None
    checksum: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "checksum": self.checksum,
            "routes": [r.to_json() for r in self.routes],
            "source_path": (
                str(self.source_path) if self.source_path else None
            ),
        }


def _manifest_checksum(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _parse_routes_payload(
    payload: Any,
    *,
    source: str,
) -> RouteManifest:
    if not isinstance(payload, Mapping):
        raise RouteManifestError(
            f"{source}: top-level must be a mapping, got "
            f"{type(payload).__name__}"
        )
    version = payload.get("version")
    if not isinstance(version, int) or version < 1:
        raise RouteManifestError(
            f"{source}: `version` must be a positive integer; "
            f"got {version!r}"
        )
    raw_routes = payload.get("routes")
    if not isinstance(raw_routes, list):
        raise RouteManifestError(
            f"{source}: `routes` must be a list; got "
            f"{type(raw_routes).__name__}"
        )

    rules: list[RouteRule] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_routes):
        if not isinstance(raw, Mapping):
            raise RouteManifestError(
                f"{source}: routes[{index}] must be a mapping; got "
                f"{type(raw).__name__}"
            )
        rid = str(raw.get("id") or "").strip()
        if not rid:
            raise RouteManifestError(
                f"{source}: routes[{index}].id is required"
            )
        if rid in seen_ids:
            raise RouteManifestError(
                f"{source}: duplicate route id {rid!r}"
            )
        seen_ids.add(rid)

        when = raw.get("when")
        if when is None:
            when = {}
        elif not isinstance(when, Mapping):
            raise RouteManifestError(
                f"{source}: routes[{index}].when must be a mapping "
                f"or omitted; got {type(when).__name__}"
            )

        recommend = raw.get("recommend")
        if not isinstance(recommend, Mapping):
            raise RouteManifestError(
                f"{source}: routes[{index}].recommend is required "
                "and must be a mapping"
            )
        if not recommend.get("tool"):
            raise RouteManifestError(
                f"{source}: routes[{index}].recommend.tool is required"
            )

        rationale = raw.get("rationale")
        if not isinstance(rationale, str) or not rationale.strip():
            raise RouteManifestError(
                f"{source}: routes[{index}].rationale is required "
                "and must be a non-empty string"
            )

        priority = raw.get("priority", index)
        if not isinstance(priority, int):
            raise RouteManifestError(
                f"{source}: routes[{index}].priority must be an "
                f"integer; got {type(priority).__name__}"
            )

        rules.append(
            RouteRule(
                id=rid,
                when=dict(when),
                recommend=dict(recommend),
                rationale=rationale.strip(),
                priority=priority,
            )
        )

    checksum = _manifest_checksum({
        "version": version,
        "routes": [r.to_json() for r in rules],
    })
    return RouteManifest(
        version=version,
        routes=tuple(rules),
        source_path=None,
        checksum=checksum,
    )


def load_manifest(path: Path | str) -> RouteManifest:
    """Load a routes manifest from a YAML or JSON file path.

    Raises :class:`RouteManifestError` for parse / validation failures.
    """
    import yaml  # local import keeps the substrate's required-deps surface tight

    manifest_path = Path(path)
    if not manifest_path.exists():
        raise RouteManifestError(
            f"routes manifest not found at {manifest_path}"
        )
    raw_text = manifest_path.read_text(encoding="utf-8")
    try:
        payload = fastyaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise RouteManifestError(
            f"{manifest_path}: YAML parse failed: {exc}"
        ) from exc
    if payload is None:
        raise RouteManifestError(
            f"{manifest_path}: file is empty"
        )
    manifest = _parse_routes_payload(payload, source=str(manifest_path))
    return RouteManifest(
        version=manifest.version,
        routes=manifest.routes,
        source_path=manifest_path,
        checksum=manifest.checksum,
    )


def load_manifest_for_deployment(
    deployment_dir: Path | str,
) -> RouteManifest | None:
    """Return the deployment's ``routes.yaml`` manifest, or None.

    A deployment without a ``routes.yaml`` returns None so callers
    can treat "no route file" as "no recommendations" without an
    error. A malformed file still raises ``RouteManifestError``.
    """
    base = Path(deployment_dir)
    candidate = base / "routes.yaml"
    if not candidate.exists():
        return None
    return load_manifest(candidate)
