"""``views_registry.yaml`` parsing + catalog projection + native view
publisher discovery.

The umbrella ``establish-views-contract`` declares
``deployments/<name>/views_registry.yaml`` as the durable deployment
authority for native view registrations. The Publisher reads that
registry directly at startup and constructs the list of
:class:`ViewMaterializer` adapters for the native V1 view stage.
``okg catalog load --apply`` still projects registry entries as
``view_definition`` graph nodes for queryability, but those graph rows
are evidence, not the bootstrap authority required for materialization.

YAML schema::

    deployment: af_pubs
    enabled: true            # optional; false disables all views

    views:
      - view_def_id: af_pubs.supersedure_health
        view_name: "Supersedure-chain health"
        view_def_factory: views.supersedure_health:supersedure_health_view_def
        build_corpus: views.supersedure_health:build_corpus
        schedule: ""           # empty = on-demand only
        params: {}             # optional planner params
        llm_client: stub       # stub | none | litellm:<model>

* ``view_def_id`` — stable, deployment-scoped slug (must equal
  the planner's ``ViewDefinition.view_id``).
* ``view_def_factory``, ``build_corpus`` — deployment-relative
  import paths (``module:attribute``). Resolved via
  ``importlib`` at Publisher startup from the registry; the deployment
  dir is on ``sys.path`` per the project convention.
* ``schedule`` — optional schedule expression consumed by DBOS runtime
  workflows. Empty means the view is refreshed on demand.
* ``params`` — JSON-serializable dict of planner kwargs.
* ``llm_client`` — small DSL for choosing the materializer's
  LLMClient. ``stub`` = DeterministicStubLLM (default), ``none``
  = no client (fails if the planner emits any GeneratedSpec at
  refresh time), ``litellm:<model>`` = ``LiteLLMClient(model)``.
* ``tracking_mode`` — ``disabled`` (default), ``optional``, or
  ``required``; enabled modes inherit the worker DSN without storing it in
  this registry.
* ``enabled`` — optional deployment-wide kill switch. ``false``
  makes the registry parse as empty, so ``okg catalog load``
  retracts live view definitions and publishers discover no
  materializers.

Idempotency. ``apply_views_registry`` is run-twice-safe:
re-applying with no YAML changes is a no-op because the Chronos
node upsert is keyed on ``node_id`` and re-writing identical attrs
produces no graph change. Retraction: any ``view_definition``
currently visible for the deployment whose ``view_def_id`` is NOT
in the YAML is removed with a retraction (``NodeFact`` op ``R``).

The projection helper deliberately does NOT route through the
publisher's transaction. It applies ``view_definition`` graph edits
directly through :class:`ChronosGraphStore` at the latest published
generation. Catalog load is an out-of-cycle registry projection;
native view publication itself still happens through completed
derived-artifact manifests.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import logging
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import psycopg
import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.contracts.deployment_product.read_assets import (
    ReadAssetContract,
    build_read_asset,
    resolve_read_assets,
)
from okg.substrate.contracts.deployment_product.typed_assets import (
    AssetDiagnostic,
    TypedAssetResolutionError,
)
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR
from okg.substrate.views.base import ViewMaterializer

logger = logging.getLogger(__name__)

# Keys that ``ChronosGraphStore.apply_facts`` stamps onto a node's
# persisted ``attrs`` JSONB at write time — they are NOT part of the
# registry-authored content of a ``view_definition``. Idempotency
# comparisons must ignore them, otherwise a reapply of an unchanged
# entry always re-upserts (the persisted attrs carry these extra keys
# while ``ViewRegistryEntry.to_node_attrs`` does not).
#
# * ``nomos_source_lineage`` — the Nomos source-lineage envelope
#   (``stamp_attrs_with_source_lineage``).
# * ``text`` — the derived searchable-text field
#   (``_stamp_searchable_text``); ``view_definition`` declares no
#   searchable fields today, but excluding it keeps the no-op robust
#   if that changes.
_WRITE_STAMPED_ATTR_KEYS: frozenset[str] = frozenset({
    LINEAGE_ATTR, "text", PRODUCER_TEXT_ATTR,
})


def _content_attrs(attrs: dict[str, Any] | None) -> dict[str, Any]:
    """Persisted attrs minus the substrate write-stamped keys.

    Returns the registry-authored content so it can be compared against
    a freshly computed ``ViewRegistryEntry.to_node_attrs()`` for a true
    idempotent no-op.
    """
    return {
        key: value
        for key, value in (attrs or {}).items()
        if key not in _WRITE_STAMPED_ATTR_KEYS
    }


class NoBaselineGenerationError(SubstrateError):
    """A views-registry apply was requested before the deployment has a
    published baseline generation.

    The ``view_definition`` projection writes into a published generation,
    so it cannot run until one exists. It is evidence only: materializers
    load directly from ``views_registry.yaml`` at publish time, so views
    still materialize. Callers that can tolerate the absence (e.g. a fresh
    ``catalog load`` before the first ``okg run``) should defer the
    projection to the next load rather than fail.
    """


__all__ = [
    "ViewRegistryEntry",
    "adapt_views_registry_entries",
    "apply_views_registry",
    "load_views_registry",
    "materializers_from_catalog",
    "materializers_from_registry",
    "materializers_from_registry_entries",
    "parse_views_registry",
]


_LEGACY_IMPORT_REF_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$"
)


# ---------------------------------------------------------------------------
# Data shape
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ViewRegistryEntry:
    """One parsed entry from ``views_registry.yaml``.

    Frozen so callers can use it as a hashable cache key. The
    Python wiring fields (``view_def_factory``, ``build_corpus``,
    ``llm_client``) carry their YAML values verbatim — resolution
    via ``importlib`` happens in :func:`materializers_from_registry`,
    not at parse time, so a typo in an import path raises at
    Publisher startup (the time you can fix it) not at YAML-load
    time (when the view might not yet exist).
    """

    view_def_id: str
    view_name: str
    deployment: str
    view_def_factory: str
    build_corpus: str
    schedule: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    llm_client: str = "stub"
    tracking_mode: str = "disabled"
    # Per `replace-portal-findings-with-views`: optional
    # `module:attribute` ref to an `AnchorResolver` callable.
    # Empty string = no edge projection for this view.
    anchor_resolver: str = ""
    # Node subtypes / edge types this view's corpus reads. Drive carry-forward
    # cancellation when the generation's Chronos write-set touches a declared
    # type (mirrors enricher `reads_subtypes` / `reads_edge_types`). Tuples so
    # the frozen dataclass stays hashable; the YAML parse yields lists.
    reads_subtypes: tuple[str, ...] = ()
    reads_edge_types: tuple[str, ...] = ()
    # Inputs a view's corpus reads from OUTSIDE the graph, from the closed
    # vocabulary in :data:`EXTERNAL_INPUT_TOKENS` / ``live:<relation>``. A view
    # with any such input is never carried forward: its output is rebuilt every
    # published generation and labelled with the age of the inputs.
    external_inputs: tuple[str, ...] = ()

    def to_read_asset_contract(self) -> ReadAssetContract:
        """Adapt this legacy row into a canonical, inert typed view asset.

        Executable references remain digest-bound strings in configuration.
        This method never imports them, alters ``sys.path``, writes graph
        state, or registers a materializer.
        """

        for field_name, ref in (
            ("view_def_factory", self.view_def_factory),
            ("build_corpus", self.build_corpus),
            ("anchor_resolver", self.anchor_resolver),
        ):
            if ref and not _LEGACY_IMPORT_REF_RE.fullmatch(ref):
                raise TypedAssetResolutionError((AssetDiagnostic(
                    code="legacy_view_registry_migration_required",
                    asset_id=f"{self.deployment}:{self.view_def_id}",
                    owner=self.deployment,
                    observed=ref,
                    message=(
                        f"legacy views_registry field {field_name!r} uses an "
                        "unsupported deployment-relative class or filesystem path"
                    ),
                    fix_hint=(
                        "migrate it to a plain module:attribute compatibility "
                        "reference before selecting the view asset"
                    ),
                ),))
        refresh_policy = (
            {"mode": "schedule", "schedule": self.schedule}
            if self.schedule else {"mode": "on-demand"}
        )
        return build_read_asset(
            owner=self.deployment,
            asset_id=self.view_def_id,
            kind="view",
            config_schema="okg.deployment_product.asset.view.config/v1",
            result_schema="okg.deployment_product.asset.view.result/v1",
            capability_schema="okg.deployment_product.asset.view.capability/v1",
            output_identity=f"native-view:{self.view_def_id}",
            refresh_policy=refresh_policy,
            # These closed sentinel values make the migration state explicit
            # without granting a public read. ``read_governed_native_view``
            # refuses every legacy adapter until a real Nomos-bound contract
            # replaces it; the token is publication/provenance authority only.
            policy_requirements={
                "audiences": ["legacy-migration"],
                "tenants": ["legacy-migration"],
                "visible_fields": ["content"],
                "decision_ids": [
                    f"nomos:migration-required:{self.deployment}:{self.view_def_id}"
                ],
            },
            lineage_sources=[
                *(f"subtype:{item}" for item in self.reads_subtypes),
                *(f"edge:{item}" for item in self.reads_edge_types),
                *(f"external:{item}" for item in self.external_inputs),
            ] or [f"legacy-registry:{self.deployment}"],
            config={
                "view_name": self.view_name,
                "deployment_name": self.deployment,
                "result_slots": ["json", "markdown"],
                "view_def_factory": self.view_def_factory,
                "build_corpus": self.build_corpus,
                "params": dict(self.params),
                "llm_client": self.llm_client,
                "tracking_mode": self.tracking_mode,
                "anchor_resolver": self.anchor_resolver,
                "reads_subtypes": list(self.reads_subtypes),
                "reads_edge_types": list(self.reads_edge_types),
            },
            provenance={
                "source": "compatibility",
                "adapter": "views_registry.yaml/v1",
                "legacy_id": self.view_def_id,
            },
            compatibility={"legacy_adapter": "views_registry.yaml/v1"},
        )

    def to_node_attrs(self) -> dict[str, Any]:
        """Serialize as the ``view_definition`` node's
        ``attrs`` JSONB.

        Carries every YAML field; the Publisher's discovery
        path round-trips through this dict.
        """
        contract = self.to_read_asset_contract()
        return {
            "view_def_id": self.view_def_id,
            "view_name": self.view_name,
            "deployment": self.deployment,
            "planner_ref": self.view_def_factory,
            "assembler_ref": self.view_def_factory,  # via factory
            "build_corpus_ref": self.build_corpus,
            "params": self.params,
            "schedule": self.schedule,
            "llm_client": self.llm_client,
            "tracking_mode": self.tracking_mode,
            "anchor_resolver_ref": self.anchor_resolver,
            "reads_subtypes": list(self.reads_subtypes),
            "reads_edge_types": list(self.reads_edge_types),
            **(
                {"external_inputs": list(self.external_inputs)}
                if self.external_inputs else {}
            ),
            "view_def_hash": self._hash(),
            "canonical_asset_id": contract.asset.qualified_id,
            "canonical_asset_digest": contract.asset.digest,
        }

    def _hash(self) -> str:
        """Stable identity hash over the wiring fields.

        A change here invalidates native fragment-cache reuse on the
        next refresh per the umbrella's view_def_hash rule. ``external_inputs``
        joins the hash only when declared, so a view that declares none keeps
        the hash it had before the field existed.
        """
        fields: dict[str, Any] = {
            "view_def_factory": self.view_def_factory,
            "build_corpus": self.build_corpus,
            "params": self.params,
            "llm_client": self.llm_client,
            "tracking_mode": self.tracking_mode,
            "anchor_resolver": self.anchor_resolver,
            "reads_subtypes": list(self.reads_subtypes),
            "reads_edge_types": list(self.reads_edge_types),
        }
        if self.external_inputs:
            fields["external_inputs"] = list(self.external_inputs)
        payload = json.dumps(fields, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Parse / validate
# ---------------------------------------------------------------------------


#: Entry-level keys a ``views_registry.yaml`` view may carry. Anything else is
#: refused at parse time (PACT view-output-carry-forward-3981): an unknown key
#: used to be dropped without a word, so a misspelt ``external_inputs`` or
#: ``reads_subtypes`` quietly left a view undeclared.
VIEW_ENTRY_KEYS = frozenset({
    "view_def_id",
    "view_name",
    "view_def_factory",
    "build_corpus",
    "anchor_resolver",
    "schedule",
    "params",
    "llm_client",
    "tracking_mode",
    "reads_subtypes",
    "reads_edge_types",
    "external_inputs",
})

#: Closed vocabulary for ``external_inputs``, plus the ``live:<relation>`` form.
#: ``publish_time``: the generation's ``published_at``. ``source_runs``: source
#: run status read up to publish time. ``live:<relation>``: call samples of a
#: declared live relation, up to publish time.
EXTERNAL_INPUT_TOKENS = ("publish_time", "source_runs")
LIVE_EXTERNAL_INPUT_PREFIX = "live:"
_LIVE_RELATION_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_.\-]*")


def is_valid_external_input(token: Any) -> bool:
    """True for ``publish_time``, ``source_runs`` or ``live:<relation>``."""
    if not isinstance(token, str):
        return False
    if token in EXTERNAL_INPUT_TOKENS:
        return True
    if token.startswith(LIVE_EXTERNAL_INPUT_PREFIX):
        return bool(
            _LIVE_RELATION_NAME_RE.fullmatch(
                token[len(LIVE_EXTERNAL_INPUT_PREFIX):]
            )
        )
    return False


def _read_external_inputs(
    value: Any,
    *,
    entry_id: str,
    source_path: Path | str,
) -> tuple[str, ...]:
    allowed = (
        f"{', '.join(EXTERNAL_INPUT_TOKENS)} or "
        f"{LIVE_EXTERNAL_INPUT_PREFIX}<relation>"
    )
    if value is None:
        return ()
    if not isinstance(value, list):
        raise SubstrateError(
            f"views_registry.yaml entry {entry_id!r} key 'external_inputs' "
            f"must be a list; allowed values are {allowed}; got {value!r}",
            data={"path": str(source_path), "view_def_id": entry_id},
        )
    seen: list[str] = []
    for token in value:
        if not is_valid_external_input(token):
            raise SubstrateError(
                f"views_registry.yaml entry {entry_id!r} key "
                f"'external_inputs' has unknown value {token!r}; allowed "
                f"values are {allowed}",
                data={
                    "path": str(source_path),
                    "view_def_id": entry_id,
                    "value": token,
                    "allowed": [
                        *EXTERNAL_INPUT_TOKENS,
                        f"{LIVE_EXTERNAL_INPUT_PREFIX}<relation>",
                    ],
                },
                fix_hint=f"use one of: {allowed}",
            )
        if token not in seen:
            seen.append(token)
    return tuple(seen)


def load_views_registry(deployment_dir: Path | str) -> list[ViewRegistryEntry]:
    """Load + validate ``views_registry.yaml`` for a deployment.

    Returns ``[]`` if the file does not exist (deployments
    without registered views are valid). Raises
    :class:`SubstrateError` on schema violations with a
    file-path-anchored message.
    """
    deployment_dir = Path(deployment_dir)
    registry_path = deployment_dir / "views_registry.yaml"
    if not registry_path.exists():
        return []
    text = registry_path.read_text(encoding="utf-8")
    try:
        data = fastyaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise SubstrateError(
            f"views_registry.yaml YAML parse failed: {exc}",
            data={"path": str(registry_path)},
            fix_hint="fix the YAML and re-run okg catalog load",
        ) from exc
    return parse_views_registry(data, source_path=registry_path)


def parse_views_registry(
    data: dict[str, Any],
    *,
    source_path: Path | str = "<inline>",
) -> list[ViewRegistryEntry]:
    """Validate a parsed YAML structure into a list of
    :class:`ViewRegistryEntry`.

    Separated from file IO so callers can pass a dict assembled
    from any source (tests, in-memory fixtures, alternate
    formats).
    """
    if not isinstance(data, dict):
        raise SubstrateError(
            f"views_registry.yaml top-level must be a mapping; "
            f"got {type(data).__name__}",
            data={"path": str(source_path)},
        )
    deployment = data.get("deployment")
    if not deployment or not isinstance(deployment, str):
        raise SubstrateError(
            "views_registry.yaml requires a top-level "
            "`deployment: <name>` field",
            data={"path": str(source_path)},
        )
    enabled = data.get("enabled", True)
    if not isinstance(enabled, bool):
        raise SubstrateError(
            "views_registry.yaml `enabled` must be a boolean",
            data={"path": str(source_path)},
        )
    if not enabled:
        return []
    raw_views = data.get("views", [])
    if not isinstance(raw_views, list):
        raise SubstrateError(
            "views_registry.yaml `views` must be a list",
            data={"path": str(source_path)},
        )

    entries: list[ViewRegistryEntry] = []
    seen_ids: set[str] = set()
    for i, raw in enumerate(raw_views):
        if not isinstance(raw, dict):
            raise SubstrateError(
                f"views_registry.yaml views[{i}] must be a mapping",
                data={"path": str(source_path)},
            )
        for required in ("view_def_id", "view_def_factory", "build_corpus"):
            if not raw.get(required):
                raise SubstrateError(
                    f"views_registry.yaml views[{i}] missing required "
                    f"field {required!r}",
                    data={"path": str(source_path)},
                )
        view_def_id = raw["view_def_id"]
        unknown_keys = sorted(
            str(key) for key in raw if key not in VIEW_ENTRY_KEYS
        )
        if unknown_keys:
            raise SubstrateError(
                f"views_registry.yaml entry {view_def_id!r} has unknown "
                f"key(s) {unknown_keys}; allowed keys are "
                f"{sorted(VIEW_ENTRY_KEYS)}",
                data={
                    "path": str(source_path),
                    "view_def_id": view_def_id,
                    "unknown_keys": unknown_keys,
                    "allowed_keys": sorted(VIEW_ENTRY_KEYS),
                },
                fix_hint=(
                    "remove the key or fix its spelling; an unknown key "
                    "used to be dropped silently"
                ),
            )
        if view_def_id in seen_ids:
            raise SubstrateError(
                f"views_registry.yaml has duplicate view_def_id "
                f"{view_def_id!r}",
                data={"path": str(source_path)},
            )
        seen_ids.add(view_def_id)
        # Validate import-string shape early so a typo fails at
        # parse time, not at Publisher startup.
        for ref_field in ("view_def_factory", "build_corpus"):
            ref = raw[ref_field]
            if not isinstance(ref, str) or ":" not in ref:
                raise SubstrateError(
                    f"views_registry.yaml views[{i}].{ref_field} "
                    f"must be a string of shape 'module:attribute'; "
                    f"got {ref!r}",
                    data={"path": str(source_path)},
                )
        anchor_resolver_raw = raw.get("anchor_resolver") or ""
        if anchor_resolver_raw and (
            not isinstance(anchor_resolver_raw, str)
            or ":" not in anchor_resolver_raw
        ):
            raise SubstrateError(
                f"views_registry.yaml views[{i}].anchor_resolver "
                f"must be a string of shape 'module:attribute' "
                f"or empty; got {anchor_resolver_raw!r}",
                data={"path": str(source_path)},
            )

        def _read_string_list(field_name: str) -> tuple[str, ...]:
            value = raw.get(field_name) or []
            if not isinstance(value, list) or not all(
                isinstance(item, str) and item for item in value
            ):
                raise SubstrateError(
                    f"views_registry.yaml views[{i}].{field_name} "
                    f"must be a list of non-empty strings; got {value!r}",
                    data={"path": str(source_path)},
                )
            return tuple(value)

        reads_subtypes = _read_string_list("reads_subtypes")
        reads_edge_types = _read_string_list("reads_edge_types")
        external_inputs = _read_external_inputs(
            raw.get("external_inputs"),
            entry_id=view_def_id,
            source_path=source_path,
        )
        tracking_mode = raw.get("tracking_mode") or "disabled"
        if tracking_mode not in {"disabled", "optional", "required"}:
            raise SubstrateError(
                f"views_registry.yaml views[{i}].tracking_mode must be "
                "disabled, optional, or required",
                data={"path": str(source_path)},
            )
        entries.append(ViewRegistryEntry(
            view_def_id=view_def_id,
            view_name=raw.get("view_name") or view_def_id,
            deployment=deployment,
            view_def_factory=raw["view_def_factory"],
            build_corpus=raw["build_corpus"],
            schedule=raw.get("schedule") or "",
            params=raw.get("params") or {},
            llm_client=raw.get("llm_client") or "stub",
            tracking_mode=tracking_mode,
            anchor_resolver=anchor_resolver_raw,
            reads_subtypes=reads_subtypes,
            reads_edge_types=reads_edge_types,
            external_inputs=external_inputs,
        ))
    return entries


def adapt_views_registry_entries(
    entries: list[ViewRegistryEntry],
):
    """Pure compatibility adapter into the canonical Wave 1 resolver."""

    contracts = tuple(entry.to_read_asset_contract() for entry in entries)
    return resolve_read_assets(contracts)


# ---------------------------------------------------------------------------
# Apply — upsert view_definition nodes into the Chronos graph
# ---------------------------------------------------------------------------


def _resolve_latest_published_generation(
    conn: psycopg.Connection, deployment_name: str,
) -> int:
    """Resolve the deployment's latest published generation.

    Prefer the Chronos checkpoint authority used by dogfood and MCP.
    Fall back to legacy ``okg.graph_generations`` for older deployments
    and tests that still seed integer generation rows directly.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    try:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoint = store.latest_generation(
            deployment_name=deployment_name,
            branch="default",
            status="published",
        )
    except (
        psycopg.errors.InvalidSchemaName,
        psycopg.errors.UndefinedColumn,
        psycopg.errors.UndefinedTable,
    ):
        conn.rollback()
        checkpoint = None
    if checkpoint is not None:
        metadata = dict(checkpoint.metadata or {})
        target_generation_id = metadata.get("target_generation_id")
        if target_generation_id is not None:
            return int(target_generation_id)
        checkpoint_id = str(checkpoint.checkpoint_id)
        if checkpoint_id.isdigit():
            return int(checkpoint_id)
        raise SubstrateError(
            "latest published Chronos generation is missing an integer "
            "target_generation_id required by views_registry",
            data={
                "deployment_name": deployment_name,
                "checkpoint_id": checkpoint_id,
            },
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT generation_id
              FROM okg.graph_generations
             WHERE deployment_name = %s
               AND status = 'published'
             ORDER BY generation_id DESC
             LIMIT 1
            """,
            (deployment_name,),
        )
        row = cur.fetchone()
    if row is None:
        raise NoBaselineGenerationError(
            f"no published generation for deployment "
            f"{deployment_name!r}; run `okg run` to establish a "
            f"baseline generation before loading the views "
            f"registry",
            data={"deployment_name": deployment_name},
        )
    return int(row[0])


@dataclass
class ApplyViewsRegistryResult:
    """Per-deployment summary of an apply run."""

    deployment: str
    generation_id: int
    upserted: int = 0
    retracted: int = 0
    deferred: bool = False


def apply_views_registry(
    conn: psycopg.Connection,
    entries: list[ViewRegistryEntry],
    *,
    deployment_name: str,
    generation_id: int | None = None,
    defer_if_unpublished: bool = False,
) -> ApplyViewsRegistryResult:
    """Upsert ``view_definition`` graph nodes from the registry.

    For each entry: upsert the ``view_definition`` node into the
    Chronos graph (``okg.graph_nodes``) at the resolved generation
    via :meth:`ChronosGraphStore.apply_facts`. The upsert is keyed
    on ``node_id``, so re-applying identical attrs is a no-op.

    Retraction: any ``view_definition`` currently visible for the
    deployment whose ``view_def_id`` is NOT in ``entries`` is
    removed with a retraction (``NodeFact`` op ``R``).

    The function is idempotent: running it twice with no YAML
    change leaves the graph unchanged.
    """
    # Resolve every legacy entry before even looking up the apply generation.
    # No graph write is reachable when adaptation or canonical selection fails.
    adapt_views_registry_entries(entries)

    if generation_id is None:
        try:
            generation_id = _resolve_latest_published_generation(
                conn, deployment_name,
            )
        except NoBaselineGenerationError:
            if not defer_if_unpublished:
                raise
            logger.info(
                "views_registry apply deferred for %s: no baseline "
                "generation yet (views still materialize from YAML at "
                "publish; projection runs on the next catalog load)",
                deployment_name,
            )
            return ApplyViewsRegistryResult(
                deployment=deployment_name,
                generation_id=-1,
                deferred=True,
            )

    # A strict environment may share a definition whose registry carries the
    # definition id. Accept that declared identity, then project runtime rows
    # under the environment's graph identity so staging never acquires
    # production-labelled view_definition nodes.
    accepted_deployments = _accepted_registry_deployments(deployment_name)
    runtime_entries: list[ViewRegistryEntry] = []
    for entry in entries:
        if entry.deployment not in accepted_deployments:
            raise SubstrateError(
                f"views_registry.yaml entry "
                f"{entry.view_def_id!r} declares "
                f"deployment={entry.deployment!r}, but apply was "
                f"called for deployment={deployment_name!r}",
            )
        runtime_entries.append(
            entry
            if entry.deployment == deployment_name
            else replace(entry, deployment=deployment_name)
        )
    entries = runtime_entries
    # Environment remapping changes owner identity and therefore the canonical
    # digest. Re-resolve the exact runtime candidates before graph mutation.
    resolution = adapt_views_registry_entries(entries)
    resolved_by_id = {
        contract.asset.asset_id: contract for contract in resolution.contracts
    }

    result = ApplyViewsRegistryResult(
        deployment=deployment_name,
        generation_id=generation_id,
    )

    desired_ids = {entry.view_def_id for entry in entries}

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    current = store.checkout()

    facts: list[NodeFact] = []

    # 1. Retract any currently-visible view_definition for this
    #    deployment whose view_def_id is not in the YAML.
    existing_rows = current.query(
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'view_definition'
           AND attrs->>'deployment' = :deployment
        """,
        {"deployment": deployment_name},
    )
    existing_attrs_by_id: dict[str, dict[str, Any]] = {}
    for row in existing_rows:
        attrs = row["attrs"] or {}
        existing_attrs_by_id[row["node_id"]] = attrs
        view_def_id = attrs.get("view_def_id")
        if view_def_id is None or view_def_id in desired_ids:
            continue
        facts.append(
            NodeFact(
                row["node_id"],
                "view_definition",
                {},
                {"view_def_id": view_def_id},
                {"generation_id": generation_id},
                op="R",
            )
        )
        result.retracted += 1
        logger.info(
            "views_registry retracted %r at gen=%d "
            "(removed from %s/views_registry.yaml)",
            view_def_id, generation_id, deployment_name,
        )

    # 2. Upsert each entry. The Chronos node upsert is keyed on
    #    node_id; re-writing identical attrs is a no-op.
    for entry in entries:
        node_id = f"view_definition:{entry.view_def_id}"
        attrs = entry.to_node_attrs()
        contract = resolved_by_id[entry.view_def_id]
        attrs["canonical_asset_id"] = contract.asset.qualified_id
        attrs["canonical_asset_digest"] = contract.asset.digest

        existing = existing_attrs_by_id.get(node_id)
        if existing is not None and _content_attrs(existing) == attrs:
            # Visible node matches the YAML — idempotent no-op.
            #
            # Compare on the registry-authored content only:
            # ``apply_facts`` stamps a substrate-managed
            # ``nomos_source_lineage`` envelope (and may derive a
            # searchable ``text`` field) onto the persisted attrs, so a
            # raw ``stored == to_node_attrs()`` would *never* match and
            # every reapply would re-upsert an unchanged view. Strip the
            # write-stamped keys before comparing so an unchanged
            # registry entry is a true no-op.
            continue

        facts.append(
            NodeFact(
                node_id,
                "view_definition",
                attrs,
                {"view_def_id": entry.view_def_id},
                {
                    "generation_id": generation_id,
                    "view_def_hash": entry._hash(),
                },
                op="I",
            )
        )
        result.upserted += 1
        logger.info(
            "views_registry upserted %r at gen=%d "
            "(deployment=%s)",
            entry.view_def_id, generation_id, deployment_name,
        )

    if facts:
        from okg.substrate.fact_application import apply_bounded_fact_batch

        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            row = cur.fetchone()
        catalog_version_id = (
            int(row[0]) if row and row[0] is not None else None
        )
        apply_bounded_fact_batch(
            conn,
            store,
            facts,
            deployment_name=deployment_name,
            producer_id="catalog:views_registry",
            producer_kind="system",
            intent="views_registry_projection",
            catalog_version_id=catalog_version_id,
            authority_scope={
                "generation_id": generation_id,
                "view_definition_ids": sorted(desired_ids),
                "completed_scope": True,
            },
            source="catalog:views_registry",
            retractions_allowed=True,
            strict_endpoints=True,
        )

    return result


# ---------------------------------------------------------------------------
# Discovery — registry/canonical and graph-projection compatibility
# ---------------------------------------------------------------------------


def materializers_from_registry(
    *,
    deployment_name: str,
    deployment_root: Path | str,
    runtime_dsn: str | None = None,
) -> list[ViewMaterializer]:
    """Build native view materializers from ``views_registry.yaml``.

    This is the Publisher's canonical discovery path. It does not read
    live graph rows because graph ``view_definition`` nodes are a
    queryable projection of the deployment registry, not the authority
    needed to decide which views should refresh after a publish.
    """

    entries = load_views_registry(deployment_root)
    return materializers_from_registry_entries(
        entries,
        deployment_name=deployment_name,
        deployment_root=deployment_root,
        runtime_dsn=runtime_dsn,
    )


def materializers_from_registry_entries(
    entries: list[ViewRegistryEntry],
    *,
    deployment_name: str,
    deployment_root: Path | str,
    runtime_dsn: str | None = None,
) -> list[ViewMaterializer]:
    """Build materializers from already-parsed registry entries."""

    deployment_root = Path(deployment_root)

    accepted_deployments = _accepted_registry_deployments(deployment_name)
    materializers: list[ViewMaterializer] = []
    accepted_entries: list[ViewRegistryEntry] = []
    for entry in sorted(entries, key=lambda item: item.view_def_id):
        if entry.deployment not in accepted_deployments:
            raise SubstrateError(
                f"views_registry.yaml entry {entry.view_def_id!r} "
                f"declares deployment={entry.deployment!r}, but "
                f"publisher discovery was called for "
                f"deployment={deployment_name!r}",
                data={
                    "view_def_id": entry.view_def_id,
                    "entry_deployment": entry.deployment,
                    "deployment_name": deployment_name,
                    "deployment_root": str(deployment_root),
                },
                fix_hint=(
                    "fix views_registry.yaml deployment or the deployment "
                    "name passed to the publisher"
                ),
            )
        accepted_entries.append(
            entry if entry.deployment == deployment_name
            else replace(entry, deployment=deployment_name)
        )

    # Canonical resolution is the last pure step. Only its selected,
    # digest-pinned assets may cross the import/materializer boundary.
    resolution = adapt_views_registry_entries(accepted_entries)
    resolved_by_id = {
        contract.asset.asset_id: contract for contract in resolution.contracts
    }
    _ensure_deployment_on_syspath(deployment_root)
    for entry in accepted_entries:
        contract = resolved_by_id[entry.view_def_id]
        materializers.append(
            _materializer_from_attrs(
                entry.to_node_attrs(),
                runtime_dsn=runtime_dsn,
                deployment=deployment_name,
                asset_id=contract.asset.qualified_id,
                asset_digest=contract.asset.digest,
                asset_execution_token=resolution.execution_token(contract.qualified_id),
            )
        )
    return materializers


def _accepted_registry_deployments(deployment_name: str) -> set[str]:
    """Return runtime and shared-definition names valid for discovery."""

    accepted = {deployment_name}
    descriptor = os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR")
    if not descriptor:
        return accepted

    from okg.substrate.deployments.environment import load_environment

    try:
        environment = load_environment(descriptor)
    except Exception as exc:  # noqa: BLE001 - active environment must fail closed.
        raise SubstrateError(
            "active environment descriptor is invalid during view discovery",
            data={"descriptor": descriptor, "error": str(exc)},
        ) from exc
    if environment.graph_deployment_name != deployment_name:
        raise SubstrateError(
            "active environment does not own requested view deployment",
            data={
                "environment_id": environment.id,
                "environment_deployment": environment.graph_deployment_name,
                "requested_deployment": deployment_name,
            },
        )
    accepted.add(environment.definition_id)
    return accepted


def materializers_from_catalog(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    deployment_root: Path | str,
) -> list[ViewMaterializer]:
    """Build materializers from projected ``view_definition`` graph rows.

    This helper is retained for compatibility with graph-projection
    tests and older callers. Publisher startup uses
    :func:`materializers_from_registry` so view refresh does not depend
    on live graph rows surviving across publish cycles.

    Parameters
    ----------
    conn :
        Open ``psycopg.Connection``.
    deployment_name :
        Filter ``view_definition`` rows by this deployment.
    deployment_root :
        Filesystem root of the deployment dir. Used as
        ``sys.path`` import root for the YAML's relative
        ``module:attr`` refs.
    """
    deployment_root = Path(deployment_root)

    from okg.substrate.graph_store import ChronosGraphStore

    try:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        rows = store.checkout().query(
            """
            SELECT attrs
              FROM okg.graph_nodes
             WHERE subtype = 'view_definition'
               AND attrs->>'deployment' = :deployment
             ORDER BY attrs->>'view_def_id'
            """,
            {"deployment": deployment_name},
        )
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        rows = []

    attrs_rows = [dict(row["attrs"] or {}) for row in rows]
    entries = [
        ViewRegistryEntry(
            view_def_id=attrs["view_def_id"],
            view_name=attrs.get("view_name") or attrs["view_def_id"],
            deployment=attrs.get("deployment") or deployment_name,
            view_def_factory=attrs["planner_ref"],
            build_corpus=attrs["build_corpus_ref"],
            schedule=attrs.get("schedule") or "",
            params=dict(attrs.get("params") or {}),
            llm_client=attrs.get("llm_client") or "stub",
            tracking_mode=attrs.get("tracking_mode") or "disabled",
            anchor_resolver=attrs.get("anchor_resolver_ref") or "",
            reads_subtypes=tuple(attrs.get("reads_subtypes") or ()),
            reads_edge_types=tuple(attrs.get("reads_edge_types") or ()),
            external_inputs=tuple(attrs.get("external_inputs") or ()),
        )
        for attrs in attrs_rows
    ]
    resolution = adapt_views_registry_entries(entries)
    resolved_by_id = {
        contract.asset.asset_id: contract for contract in resolution.contracts
    }
    _ensure_deployment_on_syspath(deployment_root)
    materializers: list[ViewMaterializer] = []
    for attrs in attrs_rows:
        contract = resolved_by_id[attrs["view_def_id"]]
        materializers.append(_materializer_from_attrs(
            attrs,
            asset_id=contract.asset.qualified_id,
            asset_digest=contract.asset.digest,
            asset_execution_token=resolution.execution_token(contract.qualified_id),
        ))
    return materializers


def _ensure_deployment_on_syspath(deployment_root: Path) -> None:
    """Make the deployment dir importable for registry module refs."""

    from okg.substrate.deployments import (
        evict_deployment_modules,
        inject_deployment_path,
    )

    inject_deployment_path(deployment_root)
    evict_deployment_modules(deployment_root, include_current=True)
    importlib.invalidate_caches()


def _materializer_from_attrs(
    attrs: dict[str, Any], *, runtime_dsn: str | None = None,
    deployment: str | None = None,
    asset_id: str | None = None,
    asset_digest: str | None = None,
    asset_execution_token: object | None = None,
) -> ViewMaterializer:
    view_def_id = attrs["view_def_id"]
    factory = _resolve_attr(attrs["planner_ref"], context=view_def_id)
    params = dict(attrs.get("params") or {})
    build_corpus = _resolve_attr(
        attrs["build_corpus_ref"], context=view_def_id,
    )
    llm_client = _build_llm_client(
        attrs.get("llm_client", "stub"),
        tracking_mode=attrs.get("tracking_mode", "disabled"),
        runtime_dsn=runtime_dsn,
        deployment=deployment or attrs.get("deployment"),
    )
    anchor_resolver_ref = attrs.get("anchor_resolver_ref") or ""
    anchor_resolver = (
        _resolve_attr(anchor_resolver_ref, context=view_def_id)
        if anchor_resolver_ref else None
    )
    reads_subtypes = tuple(attrs.get("reads_subtypes") or ())
    reads_edge_types = tuple(attrs.get("reads_edge_types") or ())
    external_inputs = tuple(attrs.get("external_inputs") or ())

    def view_def_factory(
        factory=factory,
        params=params,
    ):
        return factory(**params)

    return ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        llm_client=llm_client,
        name=view_def_id,
        anchor_resolver=anchor_resolver,
        reads_subtypes=reads_subtypes,
        reads_edge_types=reads_edge_types,
        external_inputs=external_inputs,
        asset_id=asset_id or attrs.get("canonical_asset_id"),
        asset_digest=asset_digest or attrs.get("canonical_asset_digest"),
        asset_execution_token=asset_execution_token,
    )


def _resolve_attr(ref: str, *, context: str) -> Any:
    """Resolve ``module:attribute`` to the live Python object."""
    module_path, _, attr = ref.partition(":")
    if not module_path or not attr:
        raise SubstrateError(
            f"view_definition {context!r} has malformed import "
            f"ref {ref!r}; expected 'module:attribute'",
        )
    # Dogfood workers are long-lived while deployment view modules are
    # edited in-place. Invalidate FileFinder caches so newly added
    # deployment modules are visible without requiring a worker restart.
    importlib.invalidate_caches()
    try:
        mod = importlib.import_module(module_path)
    except ImportError as exc:
        raise SubstrateError(
            f"view_definition {context!r} import failed: "
            f"module {module_path!r} not found "
            f"(check sys.path and the deployment-modules "
            f"convention)",
            data={"context": context, "ref": ref},
        ) from exc
    if not hasattr(mod, attr):
        raise SubstrateError(
            f"view_definition {context!r} import failed: "
            f"module {module_path!r} has no attribute {attr!r}",
            data={"context": context, "ref": ref},
        )
    return getattr(mod, attr)


def _build_llm_client(
    spec: str,
    *,
    tracking_mode: str | None = "disabled",
    runtime_dsn: str | None = None,
    deployment: str | None = None,
):
    """Parse the YAML's ``llm_client`` DSL into a concrete client.

    Lazy imports (LLMClient lives in okg.views.execution.llm)
    so the substrate can run without okg.views imports unless
    a deployment actually uses them.
    """
    if spec in (None, "", "stub"):
        from okg.views.execution.llm import DeterministicStubLLM
        return DeterministicStubLLM()
    if spec == "none":
        return None
    if spec.startswith("litellm:"):
        from okg.views.execution.llm import LiteLLMClient
        model = spec[len("litellm:"):].strip()
        if not model:
            raise SubstrateError(
                "llm_client 'litellm:<model>' requires a model "
                "name after the colon"
            )
        timeout_raw = os.environ.get("OKG_VIEW_LLM_TIMEOUT_SECONDS")
        timeout_s = 60.0
        if timeout_raw:
            try:
                timeout_s = float(timeout_raw)
            except ValueError as exc:
                raise SubstrateError(
                    "OKG_VIEW_LLM_TIMEOUT_SECONDS must be a number",
                    data={"value": timeout_raw},
                ) from exc
            if timeout_s <= 0:
                raise SubstrateError(
                    "OKG_VIEW_LLM_TIMEOUT_SECONDS must be positive",
                    data={"value": timeout_raw},
                )
        return LiteLLMClient(
            model=model,
            timeout_s=timeout_s,
            tracking_mode=tracking_mode,
            runtime_dsn=runtime_dsn,
            deployment=deployment,
        )
    raise SubstrateError(
        f"unknown llm_client spec {spec!r}; expected one of "
        f"'stub', 'none', or 'litellm:<model>'"
    )
