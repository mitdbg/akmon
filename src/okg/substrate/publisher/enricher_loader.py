"""Load deployment-registered enrichers from `deployment.yaml`.

Per `wire-deployment-enrichers` (2026-04-26). Closes the gap
left by `add-substrate-enricher-hook`: the Protocol shipped,
but the path from the deployment manifest to a constructed
`Publisher` instance didn't. This module is that path.

YAML shape:

    enrichers:
      - class: okg.substrate.library.linkers.declarative.DeclarativeLinker
      - class: okg.substrate.library.linkers.declarative.DeclarativeLinker
        config:
          rules_dir: deployments/<name>/cross_links
"""
from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from typing import Any, Optional

from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.llm.tracking import TrackingConfigurationError


__all__ = [
    "EnricherEnvelopeMissing",
    "EnricherLoadError",
    "EnricherNarrowingsMissing",
    "EnricherSupportPolicyInvalid",
    "active_enricher_profiles_from_env",
    "derive_write_envelope",
    "load_enrichers",
    "validate_enricher_envelopes",
    "validate_enricher_narrowings",
    "validate_enricher_support_policies",
]


class EnricherLoadError(SubstrateError):
    code = "enricher_load_error"


class EnricherEnvelopeMissing(SubstrateError):
    code = "enricher_envelope_missing"


class EnricherNarrowingsMissing(SubstrateError):
    """Raised at Publisher init when one or more loaded
    enrichers declare `requires_narrowings` triples that
    aren't in the active catalog version's `okg.narrowings`
    table.

    Per `add-enricher-required-narrowings` (2026-04-26).
    """

    code = "enricher_narrowings_missing"


class EnricherSupportPolicyInvalid(EnricherEnvelopeMissing):
    """A node-writing enricher declared no valid derived-node liveness policy.

    Raised from :func:`validate_enricher_support_policies`, which runs at
    worker boot (through the runtime preflight) and when the deployment
    publisher stack is built — both before any candidate is emitted. The runtime refusal in
    :func:`okg.substrate.derived_node_liveness.canonical_derived_support`
    is correct but fires per candidate inside a publisher cycle, which
    wedges publishing instead of failing the boot that introduced it
    (issue #1256).

    Subclasses :class:`EnricherEnvelopeMissing` so a caller that already
    refuses a dishonest write surface catches this too; it carries its own
    wire code because its ``data`` shape and fix are different.
    """

    code = "enricher_support_policy_invalid"


def _resolve_class(class_path: str):
    try:
        module_path, class_name = class_path.rsplit(".", 1)
    except ValueError as e:
        raise EnricherLoadError(
            f"class path {class_path!r} is not in 'pkg.mod.Class' "
            f"form",
            data={"class": class_path},
        ) from e
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise EnricherLoadError(
            f"could not import module {module_path!r}",
            data={"class": class_path, "import_error": str(e)},
            fix_hint=f"check that {module_path} is on the Python path",
        ) from e
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise EnricherLoadError(
            f"module {module_path!r} has no attribute {class_name!r}",
            data={"class": class_path},
        ) from e


def _instantiate_enricher(
    class_path: str,
    config: Optional[dict[str, Any]],
    *,
    runtime_dsn: str | None = None,
    deployment: str | None = None,
):
    cls = _resolve_class(class_path)
    cfg = config or {}
    try:
        parameters = inspect.signature(cls).parameters
    except (TypeError, ValueError):
        parameters = {}
    # These are worker-owned authority fields.  A deployment manifest may
    # select a tracking mode, but it cannot redirect telemetry to another
    # database or stamp another deployment identity.  Reject an override
    # before constructor/client creation so the offending value is never
    # serialized into an error record.
    for reserved, worker_value in (
        ("runtime_dsn", runtime_dsn),
        ("deployment", deployment),
    ):
        if reserved not in parameters:
            continue
        if reserved in cfg:
            raise EnricherLoadError(
                f"enricher {class_path!r} cannot override reserved runtime "
                f"authority field {reserved!r}",
                data={"class": class_path, "field": reserved},
                fix_hint=(
                    f"remove {reserved!r} from the deployment manifest; "
                    "the worker supplies it"
                ),
            )
        if worker_value is not None:
            cfg = {**cfg, reserved: worker_value}
    try:
        return cls(**cfg)
    except TrackingConfigurationError as e:
        raise EnricherLoadError(
            f"enricher {class_path!r} refused tracking configuration",
            data={
                "class": class_path,
                "config_keys": list(cfg.keys()),
                "underlying_code": e.code,
                "underlying_data": dict(e.data),
            },
            fix_hint=str(e),
        ) from e
    except (TypeError, ValueError) as e:
        raise EnricherLoadError(
            f"enricher {class_path!r} could not be instantiated "
            f"with config keys {list(cfg.keys())}",
            data={
                "class": class_path,
                "config_keys": list(cfg.keys()),
                "error": str(e),
            },
            fix_hint="check the constructor signature and configuration values",
        ) from e
    except SubstrateError as e:
        raise EnricherLoadError(
            f"enricher {class_path!r} refused to instantiate: "
            f"{type(e).__name__}: {e}",
            data={
                "class": class_path,
                "config_keys": list(cfg.keys()),
                "underlying_code": e.code,
                "underlying_data": dict(e.data or {}),
            },
            fix_hint=(
                e.fix_hint
                or f"see {type(e).__name__} for backend-specific "
                "guidance"
            ),
        ) from e


def active_enricher_profiles_from_env() -> set[str]:
    """Run-time active enricher profiles, from `OKG_ENRICHER_PROFILES`
    (comma-separated). Empty by default: a profile-tagged enricher is
    OPT-IN and does not run unless its profile is listed here. Untagged
    enrichers always run, so existing deployments are unaffected.
    """
    import os

    raw = os.environ.get("OKG_ENRICHER_PROFILES", "")
    return {p.strip() for p in raw.split(",") if p.strip()}


def load_enrichers(
    deployment_dir: Path | str,
    active_profiles: "set[str] | None" = None,
    *,
    runtime_dsn: str | None = None,
    deployment: str | None = None,
) -> list:
    """Read `<deployment_dir>/deployment.yaml`'s `enrichers:`
    list, instantiate each, return them in order.

    Returns `[]` when the manifest is missing OR the
    `enrichers:` block is absent / empty (no error). Any
    bad class path or constructor failure raises
    `EnricherLoadError` with chain-level context.

    Profile gating (`profile:` on an enricher entry): an enricher tagged
    with a `profile` is OPT-IN — it loads only when its profile is in
    `active_profiles`. Untagged enrichers always load. `active_profiles=None`
    disables gating and loads EVERY entry (used by validation/inspection so
    lint and doctor see the full pipeline); the run path passes a concrete
    set (see :func:`active_enricher_profiles_from_env`), empty by default so
    profiled enrichers stay off until explicitly requested.
    """
    deployment_dir = Path(deployment_dir)
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return []
    from okg.substrate.deployments import inject_deployment_path

    inject_deployment_path(deployment_dir)
    with manifest_path.open() as f:
        manifest = fastyaml.safe_load(f) or {}
    specs = manifest.get("enrichers") or []
    if not specs:
        return []
    if not isinstance(specs, list):
        raise EnricherLoadError(
            f"{manifest_path}: 'enrichers' must be a list",
            data={"path": str(manifest_path)},
        )
    enrichers: list = []
    for spec in specs:
        if not isinstance(spec, dict):
            raise EnricherLoadError(
                f"{manifest_path}: enricher entry must be a "
                f"mapping with `class:`",
                data={
                    "path": str(manifest_path),
                    "entry": spec,
                },
            )
        profile = spec.get("profile")
        if (
            active_profiles is not None
            and profile is not None
            and profile not in active_profiles
        ):
            continue  # opt-in enricher, its profile is not active this run
        cls_path = spec.get("class")
        config = spec.get("config")
        authority = spec.get("authority")
        if not cls_path:
            raise EnricherLoadError(
                f"{manifest_path}: enricher entry needs a "
                f"`class:` key",
                data={
                    "path": str(manifest_path),
                    "entry": spec,
                },
            )
        enricher = _instantiate_enricher(
            cls_path,
            config,
            runtime_dsn=runtime_dsn,
            deployment=deployment,
        )
        if authority is not None:
            if not isinstance(authority, dict):
                raise EnricherLoadError(
                    f"{manifest_path}: enricher authority must be a mapping",
                    data={
                        "path": str(manifest_path),
                        "class": cls_path,
                        "authority": authority,
                    },
                )
            setattr(enricher, "producer_policy", dict(authority))
            setattr(enricher, "producer_policy_source", {
                "deployment_manifest": str(manifest_path),
                "class": cls_path,
            })
        enrichers.append(enricher)
    return enrichers


def derive_write_envelope(enricher: Any, *, producer_id: str):
    """The enricher's :class:`WriteEnvelope` — explicit or derived, never
    inferred from observed output (spec §9.3, consistency-output-envelopes).

    An explicit ``write_envelope`` mapping on the enricher wins and is
    normalized loudly. Otherwise the envelope derives from the enricher's
    DECLARATIONS: nodes from ``emits_typed_subtype``, edge triples from
    ``requires_narrowings``, vocabularies from
    ``envelope_value_vocabularies``, classification from
    ``nondeterministic``. An enricher whose declarations yield an empty
    write surface has no envelope — that is an authoring error raised
    here, not a silent pass.

    Lint (req.envelope.honest-trust): an operator carrying a model client
    (``llm`` attribute set) while declaring ``nondeterministic = False`` is
    lying about its classification; refuse loudly.
    """
    from okg.substrate.producer_authority import normalize_write_envelope

    name = getattr(enricher, "name", type(enricher).__name__)
    nondeterministic = bool(getattr(enricher, "nondeterministic", False))
    if getattr(enricher, "llm", None) is not None and not nondeterministic:
        raise EnricherEnvelopeMissing(
            f"enricher {name!r} carries a model client (llm=) but declares "
            f"nondeterministic=False — a model-calling operator cannot "
            f"claim a deterministic classification",
            data={"enricher": name, "producer_id": producer_id},
            fix_hint=(
                "declare `nondeterministic = True` on the enricher class "
                "(model-backed operators are classified honestly; the "
                "trust lattice rank is unchanged)"
            ),
        )

    explicit = getattr(enricher, "write_envelope", None)
    if explicit is not None:
        return normalize_write_envelope(explicit, source_name=producer_id)

    triples = tuple(getattr(enricher, "requires_narrowings", ()) or ())
    typed_subtype = str(getattr(enricher, "emits_typed_subtype", "") or "")
    vocabularies = dict(
        getattr(enricher, "envelope_value_vocabularies", {}) or {}
    )
    # Additional node subtypes an enricher writes beyond its headline
    # `emits_typed_subtype`. Declared, never inferred — same contract as
    # everything else here. LLMExtractor uses it for `extraction_dropped`,
    # which its base class emits for every below-floor candidate: that node
    # was outside every extractor's declared signature, which was harmless
    # while node emission was ungoverned and becomes a whole-run rejection
    # once it is admitted (PACT admit-derived-node-emission).
    extra_subtypes = tuple(
        str(subtype)
        for subtype in (getattr(enricher, "emits_node_subtypes", ()) or ())
        if str(subtype)
    )
    derived: dict[str, Any] = {"nondeterministic": nondeterministic}
    node_subtypes = tuple(
        dict.fromkeys(
            ([typed_subtype] if typed_subtype else []) + list(extra_subtypes)
        )
    )
    if node_subtypes:
        derived["nodes"] = [
            {
                "subtype": subtype,
                "clause_id": f"{producer_id}:node:{subtype}",
            }
            for subtype in node_subtypes
        ]
    if triples:
        derived["edges"] = [
            {
                "src_subtype": str(src),
                "edge_type": str(edge),
                "dst_subtype": str(dst),
                "clause_id": f"{producer_id}:{src}:{edge}:{dst}",
            }
            for src, edge, dst in triples
        ]
    if vocabularies:
        derived["value_vocabularies"] = vocabularies
    # Dimensions this enricher cannot bound with a finite signature — the
    # same marker the agent write-back uses (spec §9.3). A structural
    # enricher parses whatever subtypes the source file contains, so its
    # node surface is genuinely open; declaring that is honest, whereas
    # enumerating a snapshot of it would be a lie that breaks on the next
    # file. Declared per-dimension, never inferred.
    unbounded = tuple(
        str(dimension)
        for dimension in (
            getattr(enricher, "unbounded_write_dimensions", ()) or ()
        )
        if str(dimension)
    )
    if unbounded:
        derived["unbounded"] = list(unbounded)
    if (
        "nodes" not in derived
        and "edges" not in derived
        and not unbounded
    ):
        raise EnricherEnvelopeMissing(
            f"enricher {name!r} declares no write envelope: no explicit "
            f"write_envelope, no emits_typed_subtype, no "
            f"requires_narrowings — an undeclared write surface cannot be "
            f"admitted and can never be reasoned about for reuse",
            data={"enricher": name, "producer_id": producer_id},
            fix_hint=(
                "declare requires_narrowings / emits_typed_subtype (the "
                "envelope derives from them), or an explicit "
                "write_envelope mapping (use unbounded: [...] for a "
                "genuinely free-form operator)"
            ),
        )
    return normalize_write_envelope(derived, source_name=producer_id)


def validate_enricher_envelopes(enrichers: list) -> dict[str, Any]:
    """Boot-time envelope validation: every loaded enricher has a
    normalizable write envelope (explicit or derived) and an honest
    classification. Returns ``{producer_id: WriteEnvelope}``. Pure — no
    catalog read; triple-vs-catalog validation stays with
    :func:`validate_enricher_narrowings`."""
    envelopes: dict[str, Any] = {}
    enrichers_by_producer: dict[str, Any] = {}
    for enricher in enrichers or []:
        name = getattr(enricher, "name", type(enricher).__name__)
        producer_id = f"enricher:{name}"
        prior_enricher = enrichers_by_producer.get(producer_id)
        replacement_modes = {
            getattr(candidate, "snapshot_replacement_mode", "append")
            for candidate in (prior_enricher, enricher)
            if candidate is not None
        }
        if prior_enricher is not None and "complete_producer" in replacement_modes:
            raise EnricherLoadError(
                f"duplicate enricher producer identity {producer_id!r}; "
                "complete-snapshot enricher names must be unique within one "
                "deployment",
                data={
                    "producer_id": producer_id,
                    "enricher_name": str(name),
                },
                fix_hint=(
                    "configure distinct enricher names so progress, write "
                    "envelopes, and snapshot replacement ownership cannot collide"
                ),
            )
        envelope = derive_write_envelope(enricher, producer_id=producer_id)
        _validate_node_provenance_declaration(enricher, envelope, name=name)
        envelopes[producer_id] = envelope
        enrichers_by_producer[producer_id] = enricher
    return envelopes


def _envelope_writes_nodes(envelope: Any) -> bool:
    """Whether this envelope declares a node write surface.

    An UNBOUNDED node surface has no clauses to enumerate, so keying only on
    ``signature["nodes"]`` would exempt exactly the broadest node writers.
    """
    signature = getattr(envelope, "signature", None) or {}
    unbounded = set(getattr(envelope, "unbounded", ()) or ())
    return bool(signature.get("nodes")) or "nodes" in unbounded


def _declares_write_surface(enricher: Any) -> bool:
    """Whether :func:`derive_write_envelope` can derive an envelope at all.

    An enricher with no declared write surface is refused, loudly and with a
    better message, by :func:`validate_enricher_envelopes` at Publisher
    construction. This predicate keeps the support-policy pass from
    pre-empting that refusal with a different error.
    """
    return any((
        getattr(enricher, "write_envelope", None) is not None,
        bool(getattr(enricher, "emits_typed_subtype", "")),
        bool(getattr(enricher, "emits_node_subtypes", ())),
        bool(getattr(enricher, "requires_narrowings", ())),
        bool(getattr(enricher, "unbounded_write_dimensions", ())),
    ))


def validate_enricher_support_policies(enrichers: list) -> None:
    """Refuse a node writer with no valid liveness declaration (issue #1256).

    Called on the two paths that load a deployment's registered enrichers for
    the write side — the worker boot preflight and
    ``build_deployment_publisher_stack`` — so the refusal names the enricher
    before DBOS admits work, instead of firing per candidate inside
    ``okg_publisher_cycle`` where it holds the write mutex and wedges the
    deployment.

    This function is a pure validator: it reads class declarations and raises.
    It writes no state, holds no fence and enqueues no workflow, so it is not
    a DBOS mutation boundary. The wording above avoids the epoch-boundary
    scanner's mutation vocabulary on purpose.

    Deliberately NOT in :func:`validate_enricher_envelopes`: that runs for
    every hand-built in-process writer stack too, including ones that never
    came from a deployment manifest. Deliberately NOT in
    :func:`load_enrichers`: that is also the ``doctor`` and deployment-upgrade
    inventory path, where refusing would replace a named contract error with
    a missing derived-producer inventory.
    """
    for enricher in enrichers or []:
        if not _declares_write_surface(enricher):
            continue
        name = getattr(enricher, "name", type(enricher).__name__)
        producer_id = f"enricher:{name}"
        _validate_node_support_policy(
            enricher,
            derive_write_envelope(enricher, producer_id=producer_id),
            name=name,
            producer_id=producer_id,
        )


def _validate_node_support_policy(
    enricher: Any,
    envelope: Any,
    *,
    name: str,
    producer_id: str,
) -> None:
    """An enricher that writes NODES must declare a valid support policy.

    ``support_policy`` decides whether a derived node survives its declared
    inputs being retracted. It is static per producer, so it is checked when
    the deployment's enrichers are loaded for publishing — at worker boot
    before DBOS is constructed, and when the publisher stack is built —
    rather than per emitted candidate, where the same refusal takes publishing down for the
    whole deployment and needs manual SQL to recover (issue #1256).

    Inherited declarations count: a subclass of a substrate extractor
    inherits that extractor derivation semantics along with its policy. What
    is refused is the absence of any declaration anywhere in the MRO, which
    is exactly the shape that used to reach the emitter as ``None``.
    """
    if not _envelope_writes_nodes(envelope):
        return

    from okg.substrate.derived_node_liveness import DERIVED_SUPPORT_POLICIES

    policy = getattr(enricher, "support_policy", None)
    if isinstance(policy, str) and policy in DERIVED_SUPPORT_POLICIES:
        return
    allowed = ", ".join(sorted(DERIVED_SUPPORT_POLICIES))
    raise EnricherSupportPolicyInvalid(
        f"enricher {name!r} writes nodes but declares no valid derived-node "
        f"support_policy (found {policy!r}) — the policy decides whether "
        f"its nodes survive their declared inputs being retracted, so it "
        f"cannot be defaulted; declare one of: {allowed}",
        data={
            "enricher": name,
            "producer_id": producer_id,
            "support_policy": repr(policy),
            "allowed": sorted(DERIVED_SUPPORT_POLICIES),
        },
        fix_hint=(
            "import DerivedSupportPolicy from "
            "okg.substrate.library.enrichers and declare "
            "`support_policy: DerivedSupportPolicy = \"all_members\"` "
            "(every declared input must stay live) or `\"any_member\"` "
            "(one live input is enough) on the node-writing enricher class"
        ),
    )


def _validate_node_provenance_declaration(
    enricher: Any,
    envelope: Any,
    *,
    name: str,
) -> None:
    """An enricher that writes NODES must declare it supplies provenance.

    PACT ``bind-derived-node-provenance``. A node emitted without declared
    inputs lands with an empty provenance envelope, and the substrate then
    fails closed on that emptiness in three places at once — the trust check
    rejects anything reading it, Nomos read enforcement hides it, and
    readiness quarantines the deployment. Every one of those is silent from
    the enricher author's side, which is exactly how the defect this PACT
    fixes went unnoticed.

    So the omission is refused at boot rather than discovered in production,
    mirroring the existing write-envelope contract where an undeclared write
    surface raises instead of passing quietly.

    This is a DECLARATION, not a proof: an author can set the flag and still
    forget to pass ``derived_from``. It forces the decision to be conscious;
    the runtime counter ``publisher_lineage_stamp.nodes_undeclared_provenance``
    measures whether it was actually honoured.
    """
    # An UNBOUNDED node surface has no clauses to enumerate, so keying only
    # on signature["nodes"] would let exactly the broadest node writers —
    # the ones with the least-bounded output — skip this check entirely.
    if not _envelope_writes_nodes(envelope):
        return
    if getattr(enricher, "declares_node_provenance", False):
        return
    raise EnricherEnvelopeMissing(
        f"enricher {name!r} writes nodes but does not declare node "
        f"provenance — a derived node emitted without declared inputs "
        f"lands with an empty lineage envelope, which is invisible to "
        f"Nomos-enforced reads, quarantines readiness, and is rejected as "
        f"an input by the trust check",
        data={"enricher": name},
        fix_hint=(
            "set `declares_node_provenance = True` on the enricher class "
            "and pass `derived_from=[...]` (the graph node ids the node was "
            "derived from) to every emit_node_fact call; see PACT "
            "bind-derived-node-provenance"
        ),
    )


def validate_enricher_narrowings(
    dsn: str,
    enrichers: list,
) -> None:
    """Check every enricher's `requires_narrowings` against
    the active catalog. Cross-module triples whose subtypes
    aren't in the active catalog are silently skipped (the
    enricher doesn't apply); known-subtype triples missing
    a narrowing raise `EnricherNarrowingsMissing`.

    Per `add-enricher-required-narrowings` (2026-04-26).
    """
    if not enrichers:
        return

    # Collect declared triples; deduped across enrichers.
    declared: dict[
        tuple[str, str, str], list[str]
    ] = {}
    for e in enrichers:
        triples = getattr(e, "requires_narrowings", ())
        for triple in triples:
            declared.setdefault(triple, []).append(
                getattr(e, "name", type(e).__name__),
            )

    if not declared:
        return  # all enrichers pre-declaration; backwards-compat

    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        )
        row = cur.fetchone()
        cv = row[0] if row and row[0] is not None else 0
        if cv == 0:
            # No applied catalog yet; nothing to validate against.
            return
        cur.execute(
            "SELECT name FROM okg.subtypes "
            "WHERE catalog_version_id = %s",
            (cv,),
        )
        present_subtypes = {r[0] for r in cur.fetchall()}
        cur.execute(
            "SELECT parent_subtype, edge_type, child_subtype "
            "FROM okg.narrowings "
            "WHERE catalog_version_id = %s",
            (cv,),
        )
        present_narrowings = {tuple(r) for r in cur.fetchall()}

    missing: list[dict] = []
    for triple, enricher_names in declared.items():
        parent, edge, child = triple
        # Cross-module: if either subtype isn't loaded in
        # this catalog, the enricher's reads_subtypes
        # validation will skip it elsewhere; don't gate on
        # the missing narrowing.
        if parent not in present_subtypes or child not in present_subtypes:
            continue
        if triple in present_narrowings:
            continue
        missing.append({
            "parent": parent, "edge_type": edge, "child": child,
            "required_by": enricher_names,
        })

    if missing:
        raise EnricherNarrowingsMissing(
            f"{len(missing)} narrowing(s) required by loaded "
            f"enrichers are absent from catalog v{cv}",
            data={"catalog_version_id": cv, "missing": missing},
            fix_hint=(
                "declare the missing narrowings in the relevant "
                "ontology schema's EdgeNarrowings (or remove the "
                "enricher from deployment.yaml's enrichers: list)"
            ),
        )
