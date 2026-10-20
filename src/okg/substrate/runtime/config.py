"""DBOS runtime configuration loaded from a deployment manifest.

The ``runtime:`` block in ``deployment.yaml`` configures DBOS for a
deployment. Per `sdk-design.md` and the umbrella proposal:

- ``enabled``: whether DBOS worker scheduling is active for this deployment.
  Defaults to ``False`` for manifests that have not opted into a local
  worker process, but the old ``okg.jobs`` queue, publisher lease table, and
  substrate pg_cron paths are retired and do not come back when disabled.
- ``dbos_pool_size``: the COMBINED DBOS system + application connection
  budget (default 6 per `sdk-design.md` §4.10). DBOS runs two engines even
  when both URLs point at one database, so this value is split across them
  and applied as ``sys_db_pool_size`` and ``db_engine_kwargs.pool_size``.
  There is no ``app_db_pool_size`` key in DBOSConfig; passing one applies
  nothing and leaves both engines at their 20-connection defaults.
- ``okg_read_pool_size``: the OKG-side shared pool size. This is now WIRED:
  it sets the ``db.shared_pool`` concurrency term (see
  ``okg.substrate.db.concurrency_budget``), which is what
  ``db/pool.py`` opens the pool with. It was previously accepted by the
  manifest, surfaced by ``as_dict()``, and read by nothing — a
  manifest-reachable knob connected to no code, which is a worse form of
  the orphaned-knob defect than an internal one because it LOOKS like the
  deployment surface. ``okg_write_pool_size`` remains accepted for
  manifest compatibility but is NOT a separate pool: the substrate opens
  one pool per DSN, so a distinct write size has nothing to size. It is
  reported by ``as_dict()`` and otherwise unused; do not add a second pool
  to justify it. The Postgres-role CONNECTION LIMIT applied by
  ``role_split.sql`` remains the backstop: at most 12 concurrent
  connections under ``okg_substrate``.
- ``concurrency``: mapping of concurrency-term name → integer width
  (e.g. ``concurrency: {enrichment.model_calls: 8}``). This is how a
  deployment expresses an intra-stage width that has no dedicated
  runtime key — ``gate.invariants``, ``enrichment.model_calls``,
  ``embeddings.batch``, ``views.recheck_judge`` — without a code
  change. Applied via :meth:`RuntimeConfig.apply_to_concurrency_budget`
  as registry overrides before the feasibility check. An unknown term,
  an out-of-range width, or a term that already has a dedicated key
  (``db.shared_pool`` → ``okg_read_pool_size``, ``runtime.dbos_pool``
  → ``dbos_pool_size``) refuses at boot; nothing is silently ignored.
- ``dbos_schema``: Postgres schema in which DBOS owns its tables. Only
  ``"dbos"`` (:data:`DBOS_SYSTEM_SCHEMA`) is accepted; any other value
  refuses at load. The key stays accepted so manifests that spell out the
  default keep loading. See :func:`require_supported_dbos_schema`.
- ``admin_port``: TCP port for the DBOS admin/health HTTP server. DBOS
  runs one admin server per worker process, so when several deployments
  run runtime workers on the *same host* they MUST use distinct ports —
  otherwise the second worker to boot logs ``Errno 48 Address already in
  use`` and never gets its admin/health endpoint. ``None`` (default)
  lets DBOS use its built-in default port (3001), which is correct for a
  single co-located worker; set a unique port per co-located deployment.
- ``run_admin_server``: whether to start the DBOS admin/health HTTP
  server at all (default ``True``, matching DBOS). Set ``False`` to skip
  it entirely — no port is bound — for a deployment that does not need
  the Conductor/health endpoint (e.g. a secondary personal deployment).
- ``bypass_pgbouncer``: when ``True``, DBOS connections route directly
  to Postgres rather than through any transaction-pooled PgBouncer
  endpoint. OKG does not currently deploy PgBouncer; default ``False``.
- ``sdk_version``: the ``okg.deployment`` SDK version this deployment
  manifest targets. The substrate refuses to load a manifest whose
  declared major version is incompatible with the installed SDK
  (per `sdk-design.md` §6.4); ``1.0`` is the initial version.
- ``workflows``: list of Python module paths to import at deployment
  boot, before ``DBOS.launch()``. SDK decorators in these modules
  register their workflows with the singleton. Phase 2 (`build-deployment-sdk`)
  introduces the SDK; in Phase 1 this list is typically empty (the
  doctor scheduled workflow lives in the substrate, not in deployment
  code, until Phase 2).
- ``schedules``: dict of ``schedule_id`` → cron expression, overriding
  the ``default_cron`` declared by ``@deployment_scheduled`` decorators.
  Phase 1 has no SDK-decorated schedules so this is empty.

Per Decision 11, both ``system_database_url`` and ``database_url`` for
the DBOS instance point to the same Postgres DSN (the deployment's
existing OKG DSN). This module does not own the DSN — that lives in
``DeploymentManifest.postgres_dsn`` — but it codifies the rule that
the two URLs are co-located.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


# ---------------------------------------------------------------------------
# Defaults exported for the manifest-block schema documentation
# ---------------------------------------------------------------------------

#: The one Postgres schema that holds the DBOS system tables. DBOS itself
#: (``DBOSConfig.dbos_system_schema`` and ``dbos migrate``) and every okg
#: reader of ``workflow_status``, ``operation_outputs`` and the other DBOS
#: tables -- Python modules, ``db/views.sql`` functions, the doctor -- name
#: this schema. ``runtime.dbos_schema`` may only restate it.
DBOS_SYSTEM_SCHEMA = "dbos"


def require_supported_dbos_schema(value: object) -> str:
    """Return :data:`DBOS_SYSTEM_SCHEMA`, or refuse any other declared value.

    ``None`` (key absent) means the default. A different schema name used to
    load and then do nothing for DBOS: the worker and ``dbos migrate`` still
    created the tables in ``dbos``, while the role split and the upgrade
    bootstrap probe looked in the declared schema, so the two disagreed. Until
    every DBOS reader takes the schema as a parameter, the only honest answer
    to another value is a refusal at manifest load.
    """
    if value is None or value == DBOS_SYSTEM_SCHEMA:
        return DBOS_SYSTEM_SCHEMA
    raise ValueError(
        f"runtime.dbos_schema {value!r} is not supported: DBOS and every okg "
        f"reader of the DBOS tables use the schema {DBOS_SYSTEM_SCHEMA!r}. "
        f"Remove the key or set it to {DBOS_SYSTEM_SCHEMA!r}."
    )


RUNTIME_BLOCK_DEFAULTS: dict[str, Any] = {
    "enabled": False,
    "environment": "development",
    "dbos_pool_size": 6,
    "okg_read_pool_size": 12,
    "okg_write_pool_size": 4,
    "concurrency": {},
    "dbos_schema": DBOS_SYSTEM_SCHEMA,
    "admin_port": None,
    "run_admin_server": True,
    "bypass_pgbouncer": False,
    "sdk_version": "1.0",
    "python_extras": [],
    "smoke_sources": [],
    "smoke_reset_cursor": False,
    "workflows": [],
    "schedules": {},
    "lens_putback": {},
    "source_sync": {},
    "admission": {},
}


MACHINE_IDENTIFIER_PATTERN = r"[a-z][a-z0-9_.-]{0,63}"
_MACHINE_IDENTIFIER_RE = re.compile(rf"^{MACHINE_IDENTIFIER_PATTERN}$")
_ADMISSION_PRODUCER_KINDS = frozenset({
    "human", "agent", "workflow", "system", "source",
})


def validate_admission_intent(value: Any) -> str:
    """Return one bounded machine intent or raise ``ValueError``."""
    if not isinstance(value, str) or not _MACHINE_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            "admission intent must match "
            f"{MACHINE_IDENTIFIER_PATTERN!r}; got {value!r}"
        )
    return value


def _machine_identifier(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not _MACHINE_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{field_name} must match {MACHINE_IDENTIFIER_PATTERN!r}; "
            f"got {value!r}"
        )
    return value


def _non_empty_string_list(
    value: Any,
    *,
    field_name: str,
    allow_empty: bool = False,
) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(f"{field_name} must be a list of non-empty strings")
    if not allow_empty and not value:
        raise ValueError(f"{field_name} must not be empty")
    if len(set(value)) != len(value):
        raise ValueError(f"{field_name} must not contain duplicates")
    return list(value)


def validate_runtime_admission_config(raw: Any) -> dict[str, Any]:
    """Validate the closed ``runtime.admission.approver`` schema."""
    if raw in (None, {}):
        return {}
    if not isinstance(raw, dict):
        raise ValueError("runtime.admission must be a mapping")
    unknown = sorted(set(raw) - {"approver"})
    if unknown:
        raise ValueError(
            f"runtime.admission has unknown key(s): {unknown}; "
            "known keys: ['approver']"
        )
    approver = raw.get("approver")
    if not isinstance(approver, dict):
        raise ValueError("runtime.admission.approver must be a mapping")
    expected = {"enabled", "actor", "rules"}
    unknown_approver = sorted(set(approver) - expected)
    missing_approver = sorted(expected - set(approver))
    if unknown_approver or missing_approver:
        raise ValueError(
            "runtime.admission.approver must contain exactly enabled, actor, "
            f"and rules; unknown={unknown_approver} missing={missing_approver}"
        )
    if not isinstance(approver["enabled"], bool):
        raise ValueError("runtime.admission.approver.enabled must be bool")
    actor = _machine_identifier(
        approver["actor"],
        field_name="runtime.admission.approver.actor",
    )
    rules = approver["rules"]
    if not isinstance(rules, list) or not (1 <= len(rules) <= 64):
        raise ValueError(
            "runtime.admission.approver.rules must contain 1..64 mappings"
        )
    expected_rule = {
        "id", "producer_ids", "producer_kinds", "intents",
        "node_subtypes", "edge_types", "max_nodes", "max_edges",
    }
    normalized_rules: list[dict[str, Any]] = []
    rule_ids: set[str] = set()
    for index, rule in enumerate(rules):
        prefix = f"runtime.admission.approver.rules[{index}]"
        if not isinstance(rule, dict):
            raise ValueError(f"{prefix} must be a mapping")
        unknown_rule = sorted(set(rule) - expected_rule)
        missing_rule = sorted(expected_rule - set(rule))
        if unknown_rule or missing_rule:
            raise ValueError(
                f"{prefix} must use the closed rule schema; "
                f"unknown={unknown_rule} missing={missing_rule}"
            )
        rule_id = _machine_identifier(rule["id"], field_name=f"{prefix}.id")
        if rule_id in rule_ids:
            raise ValueError(
                "runtime.admission.approver rule ids must be unique; "
                f"duplicate={rule_id!r}"
            )
        rule_ids.add(rule_id)
        producer_ids = _non_empty_string_list(
            rule["producer_ids"], field_name=f"{prefix}.producer_ids",
        )
        producer_kinds = _non_empty_string_list(
            rule["producer_kinds"], field_name=f"{prefix}.producer_kinds",
        )
        unsupported_kinds = sorted(
            set(producer_kinds) - _ADMISSION_PRODUCER_KINDS
        )
        if unsupported_kinds:
            raise ValueError(
                f"{prefix}.producer_kinds has unsupported values: "
                f"{unsupported_kinds}"
            )
        intents = _non_empty_string_list(
            rule["intents"], field_name=f"{prefix}.intents",
        )
        for intent in intents:
            validate_admission_intent(intent)
        node_subtypes = _non_empty_string_list(
            rule["node_subtypes"],
            field_name=f"{prefix}.node_subtypes",
            allow_empty=True,
        )
        edge_types = _non_empty_string_list(
            rule["edge_types"],
            field_name=f"{prefix}.edge_types",
            allow_empty=True,
        )
        for bound_name in ("max_nodes", "max_edges"):
            bound = rule[bound_name]
            if isinstance(bound, bool) or not isinstance(bound, int) or bound <= 0:
                raise ValueError(f"{prefix}.{bound_name} must be a positive int")
        normalized_rules.append({
            "id": rule_id,
            "producer_ids": producer_ids,
            "producer_kinds": producer_kinds,
            "intents": intents,
            "node_subtypes": node_subtypes,
            "edge_types": edge_types,
            "max_nodes": int(rule["max_nodes"]),
            "max_edges": int(rule["max_edges"]),
        })
    return {
        "approver": {
            "enabled": approver["enabled"],
            "actor": actor,
            "rules": normalized_rules,
        },
    }


# Defaults for the optional ``runtime.source_sync`` block, read by the
# worker at boot. See ``consolidate-deployment-runtime``.
SOURCE_SYNC_BLOCK_DEFAULTS: dict[str, Any] = {
    "watch": {
        "enabled": True,
        "debounce_seconds": 5,
        "heartbeat_seconds": 60,
    },
    "poll": {
        "enabled": True,
        "slow_when_watch_active": True,
        "backstop_multiplier": 5,
        "backstop_dependent_fanout": None,
    },
    "webhook": {
        "enabled": False,
        "host": "127.0.0.1",
        "port": 8088,
    },
}


# Defaults for the optional ``runtime.lens_putback`` block, read by the
# ``okg_lens_putback_sweep`` workflow at execution time. See
# ``add-lens-putback-applier-workflow`` (deepen-dbos-integration Phase 0).
LENS_PUTBACK_BLOCK_DEFAULTS: dict[str, Any] = {
    "stale_after": "PT1H",
    "policy": "sweep_to_failed",
}


#: Concurrency terms whose width already has a dedicated ``runtime:`` key.
#: ``runtime.concurrency`` refuses them rather than accepting a second
#: spelling: two manifest keys writing one width is two sources of truth,
#: and whichever applied last would silently win.
_CONCURRENCY_DEDICATED_KEYS: dict[str, str] = {
    "db.shared_pool": "okg_read_pool_size",
    "runtime.dbos_pool": "dbos_pool_size",
}


# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RuntimeConfig:
    """Resolved DBOS runtime configuration for a deployment.

    Constructed from the manifest's ``runtime:`` block via
    :meth:`from_manifest_block`. All fields have defaults so that a
    manifest that omits the block, or omits individual fields,
    produces a sane disabled-by-default configuration.
    """

    enabled: bool = False
    # `runtime.environment`: deployment environment marker. Any value other
    # than "development" refuses the destructive `okg runtime reset` verb
    # (see runtime/dev.py). Absence defaults to "development" so the local
    # dev loop works without a manifest edit; a production deployment must
    # set `runtime.environment: production` (or any non-development value) to
    # fail closed against an accidental reset.
    environment: str = "development"
    dbos_pool_size: int = 6
    okg_read_pool_size: int = 12
    okg_write_pool_size: int = 4
    # `runtime.concurrency`: term name → width, for intra-stage lanes with
    # no dedicated runtime key (gate.invariants, enrichment.model_calls,
    # embeddings.batch, views.recheck_judge). Term names are validated
    # against the registry in `apply_to_concurrency_budget`, not here —
    # validation needs the declared-term census, and importing the registry
    # at manifest-parse time would be a load-order dependency the parse
    # does not otherwise have.
    concurrency: dict[str, int] = field(default_factory=dict)
    dbos_schema: str = DBOS_SYSTEM_SCHEMA
    # `runtime.admin_port` / `runtime.run_admin_server`: the DBOS admin
    # (health/Conductor) HTTP server is one-per-process. Co-located
    # deployment workers MUST use distinct ports or the second to boot
    # hits `Errno 48 Address already in use`. `admin_port=None` lets DBOS
    # use its built-in default (3001); set a unique port per co-located
    # deployment, or `run_admin_server=False` to skip the server entirely.
    admin_port: int | None = None
    run_admin_server: bool = True
    bypass_pgbouncer: bool = False
    sdk_version: str = "1.0"
    # Optional project extras required by this deployment's enabled sources.
    # Deployment setup and sealed-release builders consume this declaration;
    # the long-lived worker never guesses dependencies from imported modules.
    python_extras: tuple[str, ...] = field(default_factory=tuple)
    # Small, deterministic sources used by deployment-up's real publish proof.
    # This avoids applying one unsafe run mode to a heterogeneous source fleet.
    smoke_sources: tuple[str, ...] = field(default_factory=tuple)
    smoke_reset_cursor: bool = False
    workflows: tuple[str, ...] = field(default_factory=tuple)
    schedules: dict[str, str] = field(default_factory=dict)
    # `runtime.lens_putback`: opt-in tuning for the lens-putback sweep
    # workflow. Empty dict means "use library defaults"
    # (stale_after=PT1H, policy=sweep_to_failed). Validated lazily by
    # the workflow's _resolve_lens_putback_config helper rather than
    # eagerly at manifest load — keeps the config dataclass simple
    # and lets new fields land without manifest schema churn.
    lens_putback: dict[str, Any] = field(default_factory=dict)
    # `runtime.source_sync`: declarative source-sync trigger
    # supervision per `consolidate-deployment-runtime`. The worker
    # reads this block at boot and starts whichever trigger paths
    # are enabled as supervised threads inside its own process. Empty
    # dict means "use library defaults" (watch on, poll on, webhook
    # off). Validated lazily by the worker's _resolve_source_sync
    # helper.
    source_sync: dict[str, Any] = field(default_factory=dict)
    # Opt-in, fail-closed review admission approver. The nested shape is
    # validated eagerly by ``validate_runtime_admission_config``.
    admission: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_manifest_block(cls, raw: dict | None) -> "RuntimeConfig":
        """Construct from the ``runtime:`` block in a parsed manifest.

        Parameters
        ----------
        raw : dict | None
            The ``runtime:`` value from the manifest YAML (None if the
            block is absent).

        Returns
        -------
        RuntimeConfig
            With all fields populated from ``raw`` where present and
            from :data:`RUNTIME_BLOCK_DEFAULTS` otherwise.

        Raises
        ------
        ValueError
            If a present field is the wrong type (e.g. ``enabled``
            given as a string rather than a bool).
        """
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError(
                f"runtime: block must be a mapping, got "
                f"{type(raw).__name__}"
            )

        # Convention point (g): the runtime block is a closed schema. A key
        # present in the manifest but consumed by no field is a load-time
        # error, not a silent no-op — a mis-typed key name (`enviroment`,
        # `dbos_poolsize`) would otherwise be silently inert and leave the
        # operator believing a value took effect. The known-key census is
        # RUNTIME_BLOCK_DEFAULTS.
        unknown = sorted(set(raw.keys()) - set(RUNTIME_BLOCK_DEFAULTS))
        if unknown:
            keys_phrase = ", ".join(repr(k) for k in unknown)
            raise ValueError(
                f"runtime: block has unknown key(s): {keys_phrase}. "
                f"Known keys: {sorted(RUNTIME_BLOCK_DEFAULTS)}. A typo'd key "
                f"is silently ignored otherwise — fix the key name or extend "
                f"the runtime schema."
            )

        def _typed(key: str, expected: type) -> Any:
            if key not in raw:
                return RUNTIME_BLOCK_DEFAULTS[key]
            val = raw[key]
            if not isinstance(val, expected):
                raise ValueError(
                    f"runtime.{key} must be {expected.__name__}, "
                    f"got {type(val).__name__} ({val!r})"
                )
            return val

        workflows_raw = raw.get("workflows", [])
        if not isinstance(workflows_raw, list) or not all(
            isinstance(w, str) for w in workflows_raw
        ):
            raise ValueError(
                "runtime.workflows must be a list of dotted Python "
                "module paths (strings)"
            )

        python_extras_raw = raw.get("python_extras", [])
        if not isinstance(python_extras_raw, list) or not all(
            isinstance(extra, str) and extra.strip()
            for extra in python_extras_raw
        ):
            raise ValueError(
                "runtime.python_extras must be a list of non-empty project "
                "extra names (strings)"
            )

        smoke_sources_raw = raw.get("smoke_sources", [])
        if not isinstance(smoke_sources_raw, list) or not all(
            isinstance(source, str) and source.strip()
            for source in smoke_sources_raw
        ):
            raise ValueError(
                "runtime.smoke_sources must be a list of non-empty source "
                "names (strings)"
            )

        schedules_raw = raw.get("schedules", {}) or {}
        if not isinstance(schedules_raw, dict) or not all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in schedules_raw.items()
        ):
            raise ValueError(
                "runtime.schedules must be a mapping of schedule_id "
                "(str) → cron expression (str)"
            )

        lens_putback_raw = raw.get("lens_putback", {}) or {}
        if not isinstance(lens_putback_raw, dict):
            raise ValueError(
                "runtime.lens_putback must be a mapping "
                "(`stale_after: PT1H`, `policy: sweep_to_failed`)"
            )

        # `concurrency` is a mapping, so `_typed` (scalar types only) does
        # not cover it: keys must be term-name strings and values ints.
        # `bool` is a subclass of `int`, so reject it explicitly —
        # `gate.invariants: true` is a config error, not width 1.
        concurrency_raw = raw.get("concurrency", {}) or {}
        if not isinstance(concurrency_raw, dict) or not all(
            isinstance(k, str)
            and not isinstance(v, bool)
            and isinstance(v, int)
            for k, v in concurrency_raw.items()
        ):
            raise ValueError(
                "runtime.concurrency must be a mapping of concurrency term "
                "name (str) → integer width, e.g. "
                "`concurrency: {enrichment.model_calls: 8}`"
            )

        source_sync_raw = raw.get("source_sync", {}) or {}
        if not isinstance(source_sync_raw, dict):
            raise ValueError(
                "runtime.source_sync must be a mapping with "
                "optional `watch`, `poll`, `webhook` sub-blocks"
            )

        admission_raw = validate_runtime_admission_config(
            raw.get("admission", {})
        )

        # `admin_port` is int-or-null (not handled by `_typed`, whose
        # default is non-None). `bool` is a subclass of `int`, so reject
        # it explicitly — `admin_port: true` is a config error, not port 1.
        admin_port_raw = raw.get("admin_port", None)
        if admin_port_raw is not None and (
            isinstance(admin_port_raw, bool)
            or not isinstance(admin_port_raw, int)
        ):
            raise ValueError(
                "runtime.admin_port must be an int TCP port or null, got "
                f"{type(admin_port_raw).__name__} ({admin_port_raw!r})"
            )

        return cls(
            enabled=_typed("enabled", bool),
            environment=_typed("environment", str),
            dbos_pool_size=_typed("dbos_pool_size", int),
            okg_read_pool_size=_typed("okg_read_pool_size", int),
            okg_write_pool_size=_typed("okg_write_pool_size", int),
            concurrency=dict(concurrency_raw),
            dbos_schema=require_supported_dbos_schema(
                _typed("dbos_schema", str)
            ),
            admin_port=admin_port_raw,
            run_admin_server=_typed("run_admin_server", bool),
            bypass_pgbouncer=_typed("bypass_pgbouncer", bool),
            sdk_version=_typed("sdk_version", str),
            python_extras=tuple(dict.fromkeys(python_extras_raw)),
            smoke_sources=tuple(dict.fromkeys(smoke_sources_raw)),
            smoke_reset_cursor=_typed("smoke_reset_cursor", bool),
            workflows=tuple(workflows_raw),
            schedules=dict(schedules_raw),
            lens_putback=dict(lens_putback_raw),
            source_sync=dict(source_sync_raw),
            admission=admission_raw,
        )

    def apply_to_concurrency_budget(self) -> None:
        """Push manifest-declared widths onto the concurrency registry.

        This is what makes ``okg_read_pool_size``, ``dbos_pool_size``, and
        the ``runtime.concurrency`` mapping real rather than decorative:
        after this call, ``db/pool.py`` opens the shared pool at the
        declared size, every ``runtime.concurrency`` term resolves to its
        manifest width, and the summed connection demand reflects the
        manifest. Call it at worker boot, BEFORE any pool is constructed —
        once a pool is open its size is fixed and ``get_pool`` refuses a
        conflicting request rather than pretending.

        A malformed or out-of-range width raises ``BudgetError`` here, at
        boot, instead of becoming a ``PoolTimeout`` mid-publish; an unknown
        ``runtime.concurrency`` term, an out-of-range width, or a term with
        a dedicated runtime key raises ``ValueError`` naming the offender.
        """
        from okg.substrate.db.concurrency_budget import (
            _MAX_DECLARABLE_WIDTH,
            check_feasibility,
            declared_terms,
            set_override,
        )

        # `runtime.concurrency` entries are validated HERE, against the live
        # registry, and refuse with ValueError — the same class a bad runtime
        # key raises at manifest load. Never skipped or defaulted: a typo'd
        # term name silently ignored would leave the operator believing a
        # width took effect (the orphaned-knob defect, manifest edition).
        # ALL entries are validated BEFORE any override is applied so a
        # refused manifest leaves the registry untouched — apply-as-you-
        # validate would leave earlier overrides stuck when a later entry
        # raises, which matters for embedded/test callers that outlive the
        # failed apply.
        settable = {
            t.name for t in declared_terms()
        } - set(_CONCURRENCY_DEDICATED_KEYS)
        for name, declared_width in sorted(self.concurrency.items()):
            if name in _CONCURRENCY_DEDICATED_KEYS:
                dedicated = _CONCURRENCY_DEDICATED_KEYS[name]
                raise ValueError(
                    f"runtime.concurrency may not set {name!r}: it has the "
                    f"dedicated key runtime.{dedicated}. Two manifest "
                    f"spellings for one width is two sources of truth — use "
                    f"`{dedicated}: {declared_width}` instead."
                )
            if name not in settable:
                raise ValueError(
                    f"runtime.concurrency names unknown term {name!r}. "
                    f"Settable terms: {', '.join(sorted(settable))}. A typo'd "
                    f"term would otherwise be silently inert — fix the name "
                    f"or declare the term in the registry."
                )
            if not 1 <= declared_width <= _MAX_DECLARABLE_WIDTH:
                raise ValueError(
                    f"runtime.concurrency.{name} must be between 1 and "
                    f"{_MAX_DECLARABLE_WIDTH}, got {declared_width}. Not "
                    f"clamped — clamping is the shape default-value-as-"
                    f"fallback takes for concurrency knobs."
                )
        set_override("db.shared_pool", self.okg_read_pool_size)
        set_override("runtime.dbos_pool", self.dbos_pool_size)
        for name, declared_width in sorted(self.concurrency.items()):
            set_override(name, declared_width)
        # Each width can be individually legal while the combination cannot
        # fit. Checked here, at boot, so an over-subscribed manifest is refused
        # by name rather than surfacing as a PoolTimeout mid-publish.
        check_feasibility()

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of the resolved config.

        Used by ``okg deployment show`` and operator diagnostic tools.
        """
        return {
            "enabled": self.enabled,
            "environment": self.environment,
            "dbos_pool_size": self.dbos_pool_size,
            "okg_read_pool_size": self.okg_read_pool_size,
            "okg_write_pool_size": self.okg_write_pool_size,
            "concurrency": dict(self.concurrency),
            "dbos_schema": self.dbos_schema,
            "admin_port": self.admin_port,
            "run_admin_server": self.run_admin_server,
            "bypass_pgbouncer": self.bypass_pgbouncer,
            "sdk_version": self.sdk_version,
            "python_extras": list(self.python_extras),
            "smoke_sources": list(self.smoke_sources),
            "smoke_reset_cursor": self.smoke_reset_cursor,
            "workflows": list(self.workflows),
            "schedules": dict(self.schedules),
            "lens_putback": dict(self.lens_putback),
            "source_sync": dict(self.source_sync),
            "admission": dict(self.admission),
        }


def resolve_source_sync(runtime_cfg) -> dict:
    """Resolve ``runtime.source_sync`` with defaults filled in.

    Returns a structured dict matching :data:`SOURCE_SYNC_BLOCK_DEFAULTS`
    with operator overrides applied. Tolerates partial overrides
    (only watch.enabled set; rest defaulted).
    """
    raw = getattr(runtime_cfg, "source_sync", None) or {}
    if not isinstance(raw, dict):
        raw = {}
    resolved: dict[str, Any] = {
        kind: dict(default)
        for kind, default in SOURCE_SYNC_BLOCK_DEFAULTS.items()
    }
    for kind in ("watch", "poll", "webhook"):
        override = raw.get(kind)
        if isinstance(override, dict):
            resolved[kind].update(override)
    return resolved
