"""Resolver registry for the pre-join MCP framework.

Per S-A7-R3:

- Substrate ships two built-in resolvers: `alias_table` and
  `identifier_pattern`.
- Deployments register additional resolvers under `resolvers:` in
  `deployment.yaml`, mapping `<slug>` to `{ module, function }`.
- A bundle step that references an unknown resolver slug fails
  registration at MCP server bootstrap with code
  `pre_join_tool_resolver_unknown`; other tools in the same YAML
  still register.

A resolver is a callable
    (text: str, ctx: ResolverContext) -> str | None
returning a node_id (or None when unresolved). The framework
calls resolvers inside the executor with the active connection +
session pin in ctx so the resolver can issue its own pinned reads
against substrate primitives.
"""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Protocol

import psycopg

from okg.substrate.mcp.framework.errors import (
    PreJoinResolverUnknown,
    PreJoinSchemaError,
)
from okg.substrate.mcp.session import Session


__all__ = [
    "Resolver",
    "ResolverContext",
    "ResolverRegistry",
    "build_substrate_resolvers",
    "alias_table_resolver",
    "identifier_pattern_resolver",
    "validate_resolver_refs",
]


@dataclass(frozen=True)
class ResolverContext:
    """Read-only context passed to every resolver invocation.

    Resolvers MUST NOT mutate anything via ``conn``; they consult
    the pinned graph and return a node_id. The substrate's
    `review_as` and pre-join executor surfaces enforce this by
    convention (no enforcement; resolvers are deployment-author
    code).
    """

    conn: psycopg.Connection
    session: Session
    # The catalog_version_id active at the session pin. Used by
    # `identifier_pattern` to read the right pattern table.
    catalog_version_id: int


Resolver = Callable[[str, ResolverContext], Optional[str]]


class _DeploymentResolverEntry(Protocol):
    """Shape of one `resolvers:` map entry in deployment.yaml."""

    module: str
    function: str


@dataclass
class ResolverRegistry:
    """Maps resolver slugs to callables.

    Built-in substrate resolvers are seeded by
    `build_substrate_resolvers`. Deployment-registered resolvers
    are added by `register_from_manifest` at MCP server
    bootstrap; that step also validates each pre-join tool's
    `resolve` references against the merged registry.
    """

    _resolvers: dict[str, Resolver]

    @classmethod
    def with_substrate_builtins(cls) -> "ResolverRegistry":
        return cls(_resolvers=dict(build_substrate_resolvers()))

    def __contains__(self, slug: str) -> bool:
        return slug in self._resolvers

    def get(self, slug: str) -> Optional[Resolver]:
        return self._resolvers.get(slug)

    def known_slugs(self) -> set[str]:
        return set(self._resolvers)

    def register(self, slug: str, resolver: Resolver) -> None:
        """Add or replace a resolver. Used by tests; deployments use
        `register_from_manifest`."""
        self._resolvers[slug] = resolver

    def register_from_manifest(
        self,
        manifest_resolvers: Mapping[str, Mapping[str, Any]],
    ) -> None:
        """Import each entry under `resolvers:` in deployment.yaml
        and add its function to the registry.

        manifest_resolvers shape:
            {
              "<slug>": {"module": "<py.mod.path>", "function": "<name>"},
              ...
            }

        A malformed entry (missing module/function) or an import
        failure raises PreJoinSchemaError so the substrate bootstrap
        path can log + skip. Slug collision with a substrate
        built-in is rejected — the operator must rename their
        deployment-resolver.
        """
        builtins = set(build_substrate_resolvers().keys())
        for slug, raw in (manifest_resolvers or {}).items():
            if not isinstance(slug, str) or not slug.strip():
                raise PreJoinSchemaError(
                    "deployment.yaml resolvers: key must be a "
                    "non-empty string",
                    data={"slug": slug},
                )
            if not isinstance(raw, dict):
                raise PreJoinSchemaError(
                    f"deployment.yaml resolvers.{slug}: entry must be "
                    "a mapping with `module` and `function`",
                    data={"slug": slug},
                )
            module = raw.get("module")
            function = raw.get("function")
            if not isinstance(module, str) or not module:
                raise PreJoinSchemaError(
                    f"deployment.yaml resolvers.{slug}: missing "
                    "`module`",
                    data={"slug": slug},
                )
            if not isinstance(function, str) or not function:
                raise PreJoinSchemaError(
                    f"deployment.yaml resolvers.{slug}: missing "
                    "`function`",
                    data={"slug": slug},
                )
            if slug in builtins:
                raise PreJoinSchemaError(
                    f"deployment.yaml resolvers.{slug}: collides with "
                    "substrate built-in; pick a distinct slug",
                    data={"slug": slug},
                )
            try:
                py_module = importlib.import_module(module)
            except ImportError as e:
                raise PreJoinSchemaError(
                    f"deployment.yaml resolvers.{slug}: could not "
                    f"import {module!r}: {e}",
                    data={"slug": slug, "module": module},
                ) from e
            try:
                fn = getattr(py_module, function)
            except AttributeError as e:
                raise PreJoinSchemaError(
                    f"deployment.yaml resolvers.{slug}: module "
                    f"{module!r} has no attribute {function!r}",
                    data={"slug": slug, "module": module,
                          "function": function},
                ) from e
            if not callable(fn):
                raise PreJoinSchemaError(
                    f"deployment.yaml resolvers.{slug}: "
                    f"{module}.{function} is not callable",
                    data={"slug": slug},
                )
            self._resolvers[slug] = fn


# ---- substrate-shipped built-ins ----


def alias_table_resolver(
    text: str, ctx: ResolverContext,
) -> Optional[str]:
    """Look up ``text`` in `okg.aliases` at the session pin's
    catalog version. Returns the canonical node_id if found, None
    otherwise.
    """
    if not isinstance(text, str) or not text:
        return None
    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT canonical FROM okg.aliases "
            "WHERE alias = %(alias)s "
            "  AND catalog_version_id = %(cv)s "
            "LIMIT 1",
            {"alias": text, "cv": ctx.catalog_version_id},
        )
        row = cur.fetchone()
    return row[0] if row else None


def identifier_pattern_resolver(
    text: str, ctx: ResolverContext,
) -> Optional[str]:
    """Try each catalog-registered identifier-pattern regex against
    ``text``. On a match, resolve the captured group via the alias
    table. Returns None if no pattern matches or no alias resolves.

    The substrate's identifier_patterns rows are
    (regex, id_type, validator_kind) tuples; we apply each in
    order. A future optimization could compile patterns once at
    catalog-load and cache them on the connection.
    """
    if not isinstance(text, str) or not text:
        return None
    import re

    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT regex FROM okg.identifier_patterns "
            "WHERE catalog_version_id = %(cv)s "
            "ORDER BY id_type",
            {"cv": ctx.catalog_version_id},
        )
        patterns = [row[0] for row in cur.fetchall()]
        for pattern in patterns:
            try:
                m = re.search(pattern, text)
            except re.error as exc:
                raise PreJoinSchemaError(
                    "active catalog contains an invalid identifier regex; "
                    "identifier resolution is unavailable",
                    data={
                        "catalog_version_id": ctx.catalog_version_id,
                        "regex": pattern,
                        "reason": str(exc),
                    },
                    fix_hint=(
                        "repair identifier_patterns.yaml and apply a valid catalog"
                    ),
                ) from exc
            if not m:
                continue
            # Prefer first capture group; fall back to full match.
            try:
                candidate = m.group(1)
            except IndexError:
                candidate = m.group(0)
            if not candidate:
                continue
            cur.execute(
                "SELECT canonical FROM okg.aliases "
                "WHERE alias = %(alias)s "
                "  AND catalog_version_id = %(cv)s "
                "LIMIT 1",
                {"alias": candidate, "cv": ctx.catalog_version_id},
            )
            row = cur.fetchone()
            if row:
                return row[0]
    return None


def build_substrate_resolvers() -> dict[str, Resolver]:
    """Return the substrate-shipped resolver registry."""
    return {
        "alias_table": alias_table_resolver,
        "identifier_pattern": identifier_pattern_resolver,
    }


# ---- validation hook ----


def validate_resolver_refs(
    specs: list,  # list[PreJoinToolSpec]; lazy-typed to avoid cycle
    registry: ResolverRegistry,
) -> tuple[list, list]:
    """Walk each spec's bundle; reject specs that reference an
    unknown resolver slug. Returns (passing, failing) so the
    bootstrap registration path can isolate unknown-resolver
    failures (S-A7-R3 negative scenario).

    `failing` items carry a PreJoinResolverUnknown error.
    """
    from okg.substrate.mcp.framework.schema import PreJoinValidationFailure

    passing: list = []
    failing: list = []
    for spec in specs:
        unknown_slug = _first_unknown_resolver_slug(spec, registry)
        if unknown_slug is None:
            passing.append(spec)
            continue
        failing.append(
            PreJoinValidationFailure(
                tool_name=spec.name,
                error=PreJoinResolverUnknown(
                    f"tool {spec.name!r} references unknown resolver "
                    f"{unknown_slug!r}",
                    data={
                        "name": spec.name,
                        "resolver": unknown_slug,
                    },
                ),
            )
        )
    return passing, failing


def _first_unknown_resolver_slug(spec, registry: ResolverRegistry) -> Optional[str]:
    """Return the first unknown resolver slug encountered in
    ``spec.bundle``, or None when every referenced slug is
    registered.
    """
    for step in spec.bundle:
        for value in step.args.values():
            if isinstance(value, dict):
                slug = value.get("resolve")
                if isinstance(slug, str) and slug and slug not in registry:
                    return slug
    return None
