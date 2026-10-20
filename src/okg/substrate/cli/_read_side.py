"""Read-side CLI: `okg search`, and the read-operator commands.

Per `define-memex-deployment-contract` R4 the substrate originally
shipped four read-side CLI wrappers (`search` / `get-node` /
`list-neighbors` / `traverse-path`) paralleling the MCP tools.
After the AKMON read-surface rewrite the latter three duplicated retired
compat names and had zero callers anywhere; this module ships
only `okg search`. For node-by-id / neighbor / multi-hop walks,
agents use the MCP read surface (`inspect` / `expand`) and
shell scripts read the Chronos graph (`okg.graph_nodes` /
`okg.graph_edges`) through a Chronos checkout.

Issue #3666 gives the command line every read operator again, this time
without a second implementation: `okg inspect|expand|filter|aggregate|map|
query`, and `okg search` when it is given anything only the MCP tool takes,
call the MCP server's own tool in this process (`_read_operators`).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import unicodedata
from typing import Any, Mapping

import psycopg

from okg.substrate.cli import _read_operators
from okg.substrate.cli.dsn_authority import resolve_read_dsn
from okg.substrate.cli.renamed_flags import add_renamed_argument
from okg.substrate.errors import SubstrateError
from okg.substrate.db.connection import connect as db_connect
# The operator read identity is shared with the operator UI's read boundary
# (PACT operator-ui-search-view); the CLI keeps its historical private names.
from okg.substrate.operator_read import (
    nomos_denied_error as _nomos_denied_error,
    resolve_operator_read_subject as _resolve_cli_read_subject,
    resolve_operator_search_scope as _resolve_search_scope,
)


def _resolve_dsn(args: argparse.Namespace) -> str:
    return resolve_read_dsn(args, surface="okg search").dsn


def _attach_resolution_metadata(result: Any, resolution) -> Any:
    if not isinstance(result, dict):
        return result
    enriched = dict(result)
    enriched.setdefault("deployment", resolution.deployment)
    enriched["authority"] = resolution.authority
    enriched["target"] = resolution.target
    return enriched


def _open_session(
    conn: psycopg.Connection,
    deployment: str,
    *,
    generation_id: int | str | None = None,
    nomos_access_context: Mapping[str, Any] | None = None,
    nomos_subject: str | None = None,
) -> Any:
    from okg.substrate.mcp.session import open_session
    return open_session(
        conn, deployment_name=deployment,
        requested_generation=generation_id,
        nomos_access_context=nomos_access_context,
        nomos_subject=nomos_subject,
    )


def _emit_error(err: SubstrateError, *, as_json: bool) -> int:
    # Every line a read command prints on stderr goes through `mask`: a
    # message, a fix hint or a data value can name a file of this host.
    mask = _read_operators.mask
    if as_json:
        print(
            json.dumps(_read_operators.mask_value(err.to_json())), file=sys.stderr,
        )
    else:
        fix = f"\n  fix_hint: {err.fix_hint}" if err.fix_hint else ""
        print(mask(f"ERROR [{err.code}]: {err.message}{fix}"), file=sys.stderr)
        if err.data:
            print(mask(f"  data: {err.data}"), file=sys.stderr)
    return 1


def _nomos_profile_record(
    deployment: str,
    requested_profile: str | None,
    *,
    subtype_filter: str | None,
    method: str,
) -> dict[str, Any]:
    """Profile fields for the Nomos resource block. Manifest-only, no DB.

    Shares the MCP surface's implementation so one deployment cannot produce
    two different audit records depending on which door the read came through.
    """
    from okg.substrate.mcp.server import _nomos_profile_record as record

    return record(
        deployment, requested_profile,
        subtype_filter=subtype_filter, method=method,
    )


def _authorize_search_with_nomos(
    conn: psycopg.Connection,
    *,
    deployment: str,
    session: Any,
    query: str,
    method: str,
    limit: int,
    offset: int,
    filter_obj: Mapping[str, Any] | None,
    select: list[str] | None,
    access_context: Mapping[str, Any] | None,
    subject: str | None = None,
    subject_meta: Mapping[str, Any] | None = None,
    requested_profile: str | None = None,
    rank_by: str = "self",
) -> tuple[str, Any | None]:
    """Authorize one ``okg search`` call; return ``(mode, outcome)``.

    The enforcement mode is resolved once and handed to the evaluation, so the
    mode the envelope reports is the mode that decided this call. ``off``
    evaluates nothing and returns no outcome.
    """
    from okg.substrate.nomos import (
        NomosRuntimeRequest,
        deployment_nomos_enforcement_mode,
        evaluate_runtime_boundary_with_active_policy,
    )

    mode = deployment_nomos_enforcement_mode(deployment)
    if mode == "off":
        return mode, None
    if subject is None:
        subject, subject_meta = _resolve_cli_read_subject(
            deployment=deployment, default_kind="cli_search",
        )
    meta = dict(subject_meta or {})
    # Audit trail: record how the presented subject was resolved and the OS
    # user behind the terminal. Rule matching is subset-based on rule.match
    # keys, so extra context facts never change a decision.
    decision_context = {
        key: value
        for key, value in (
            ("subject_source", meta.get("source")),
            ("os_user", meta.get("os_user")),
        )
        if value
    }
    outcome = evaluate_runtime_boundary_with_active_policy(
        conn,
        deployment_name=deployment,
        scope=f"deployment:{deployment}",
        request=NomosRuntimeRequest(
            surface="graph.read.cli_search",
            subject=subject,
            # Volatile publish coordinates stay OUT of the identity-bearing
            # resource (issue #635: they re-mint the boundary decision once
            # per publish); the audit-only target records them.
            resource={
                "resource_kind": "cli_search_boundary",
                "query": query,
                "method": method,
                "rank_by": rank_by,
                "limit": limit,
                "offset": offset,
                "filter": filter_obj or {},
                "select": select or [],
                "access_context": dict(access_context or {}),
                # A search profile NARROWS the read, so the record must name
                # the scope actually executed; without it the decision
                # describes a broader access than the one that happened.
                # A search profile NARROWS the read, so the record must name
                # it: decision identity is content-of-access. The profile that
                # WILL apply, plus its DECLARED (un-expanded) subtypes — both
                # readable from the manifest alone, so this needs no database
                # and can run ahead of the boundary check. Values are null, not
                # [], when unscoped: an empty list would read as "scoped to
                # nothing", a different (and impossible) state.
                **_nomos_profile_record(
                    deployment, requested_profile,
                    subtype_filter=(filter_obj or {}).get("subtype"),
                    method=method,
                ),
            },
            context=decision_context,
        ),
        target={
            "kind": "cli_search",
            "query": query,
            "method": method,
            "rank_by": rank_by,
            "generation_id": getattr(session, "generation_id", None),
            "branch_name": getattr(session, "branch_name", "default"),
            "catalog_version_id": getattr(session, "catalog_version_id", None),
        },
        performed_obligations=("audit",),
        enforcement_mode=mode,
    )
    if outcome is None:
        # Unreachable today: the search never passes ``require_active=False``,
        # so an enforce or audit evaluation always yields an outcome. Should
        # that change, refuse rather than report ``nomos_effect: null``, which
        # means mode ``off`` (issue #2279).
        err = SubstrateError(
            f"Nomos {mode} mode produced no decision for graph.read.cli_search",
            fix_hint="report this; an evaluated search must record a decision",
        )
        err.code = "nomos_search_decision_missing"
        err.data = {"surface": "graph.read.cli_search", "enforcement_mode": mode}
        raise err
    conn.commit()
    if not outcome.allowed:
        raise _nomos_denied_error(
            conn,
            deployment=deployment,
            surface="graph.read.cli_search",
            outcome=outcome,
        )
    return mode, outcome


def _search_nomos_fields(mode: str, outcome: Any | None) -> dict[str, Any]:
    """The Nomos fields of an ``okg search`` envelope (issue #2279).

    ``nomos_effect`` is what happened to THIS call: ``allow`` (or ``redact``/
    ``declassify``) when the policy allowed it, ``audit`` when audit mode let
    through a call the policy would refuse, and null when no decision was made
    (mode ``off``). A refused call never reaches here: it exits with
    ``nomos_runtime_denied`` and returns no rows. ``nomos_policy_effect`` keeps
    the policy's own verdict -- the one recorded under ``nomos_decision_id`` --
    so an audit-mode ``deny`` stays visible without reading as a refusal.
    """
    if outcome is None:
        return {
            "nomos_enforcement_mode": mode,
            "nomos_effect": None,
            "nomos_policy_effect": None,
            "nomos_decision_id": None,
        }
    return {
        "nomos_enforcement_mode": mode,
        "nomos_effect": outcome.call_effect,
        "nomos_policy_effect": outcome.final_effect,
        "nomos_decision_id": outcome.decision.decision_id,
    }


def _nomos_access_context(args: argparse.Namespace) -> Mapping[str, Any] | None:
    raw = getattr(args, "nomos_access_context", None)
    if raw is None:
        raw = getattr(args, "nomos_access_context_json", None)
    if raw is None:
        raw = os.getenv("OKG_NOMOS_ACCESS_CONTEXT")
    if raw in (None, ""):
        return None
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            err = SubstrateError(f"invalid Nomos access context JSON: {exc}")
            err.code = "nomos_access_context_invalid"
            raise err from exc
        if isinstance(parsed, Mapping):
            return dict(parsed)
    err = SubstrateError("nomos access context must be a JSON object")
    err.code = "nomos_access_context_invalid"
    raise err


def _parse_select(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    fields: list[str] = []
    for value in values:
        fields.extend(
            part.strip()
            for part in value.split(",")
            if part.strip()
        )
    return fields or None


def _parse_attr_filters(values: list[str] | None) -> dict[str, Any] | None:
    if not values:
        return None
    filters: dict[str, Any] = {}
    for raw in values:
        if "=" not in raw:
            raise SubstrateError(
                "invalid --attr-filter value; expected key=value",
                data={"attr_filter": raw},
                fix_hint=(
                    "pass repeatable exact filters like "
                    "`--attr-filter status=open --attr-filter priority=1`"
                ),
            )
        key, raw_value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise SubstrateError(
                "invalid --attr-filter value; key cannot be empty",
                data={"attr_filter": raw},
                fix_hint="use --attr-filter key=value",
            )
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value
        if not isinstance(value, (str, int, bool, type(None))):
            raise SubstrateError(
                "invalid --attr-filter value; exact filters only accept "
                "string, int, bool, or null values",
                data={
                    "attr_filter": raw,
                    "parsed_type": type(value).__name__,
                },
                fix_hint="use scalar exact matches; ranges and arrays are not supported",
            )
        filters[key] = value
    return filters


def _print_result(result: Any, *, as_json: bool, select: Any = None) -> None:
    """`okg search`'s own reply: the JSON envelope with `--json`, otherwise
    the text form every read command prints (`result_text.render`: the
    status line, `! ` notes, one block per row, `--- other fields`), with
    the applied search profile said in a note under the status line.

    Before #3666 the text form was a dump of every envelope key, Python
    reprs included (`total_count: None`), which matched no other command.
    """
    if as_json:
        json.dump(result, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
        return
    from okg.substrate.mcp.result_text import render

    # No MCP size facts: the shell prints its own text (PACT read-reply-size-3666).
    lines = render(
        _terminal_safe_value(result), tool="search", select=select, size_facts=False,
    ).split("\n")
    note = _profile_note(result)
    if note is not None:
        lines.insert(1, note)
    print("\n".join(lines))


def _profile_note(result: Any) -> str | None:
    """The `! ` note naming the search profile the page was scoped by.

    Read off the APPLIED profile in the result, not the requested one: a
    deployment `default_profile` scopes a caller who typed no `--scope`,
    and that is exactly the case where an operator must not be scoped
    without seeing it (#797). A page no profile scoped says so too, so the
    head never leaves the scope unsaid. None when the result carries no
    `search_profile` field.
    """
    if not isinstance(result, dict) or "search_profile" not in result:
        return None
    if result["search_profile"] is None:
        return "! search profile: none (unscoped)"
    from okg.substrate.mcp.result_text import scalar

    scoped = result.get("scoped_subtypes")
    scope = f"{len(scoped)} subtypes" if isinstance(scoped, list) and scoped else "unscoped"
    profile = scalar(_terminal_safe_value(result["search_profile"]))
    return f"! search profile {profile} applied: {scope}"


#: Character categories never printed raw from a search reply: control
#: (other than a line feed), format, surrogate, and line or paragraph
#: separators. Each prints as a space.
_UNSAFE_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Zl", "Zp"})


def _unsafe_char(char: str) -> bool:
    return char != "\n" and unicodedata.category(char) in _UNSAFE_CATEGORIES


def _terminal_safe_value(value: Any) -> Any:
    """`value` with every string's unsafe characters printed as a space, so
    untrusted text cannot emit terminal escape sequences."""
    if isinstance(value, str):
        if not any(_unsafe_char(char) for char in value):
            return value
        return "".join(" " if _unsafe_char(char) else char for char in value)
    if isinstance(value, dict):
        return {key: _terminal_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_terminal_safe_value(item) for item in value]
    return value


#: Env var the MCP server already reads (`mcp/deployment_entry.py`). The CLI
#: shares it deliberately: an operator and an agent querying the same
#: deployment must resolve the SAME embedder, or "semantic search" means two
#: different things depending on which door you came through.
_EMBEDDER_ENV = "OKG_MCP_EMBEDDER"


def _resolve_embedder_spec(
    args: argparse.Namespace, *, method: str, deployment: str,
) -> tuple[str | None, str | None]:
    """(spec, source) for a semantic search, or raise naming what is missing.

    An explicit argument is allowed for a standalone query. Otherwise the
    deployment's admitted plural ``embeddings`` block is authoritative. The SOURCE
    is returned and disclosed because "semantic search ran" and "semantic
    search ran against the embedder you meant" are different claims.

    Only the spec STRING is produced here -- no embedder is constructed. A
    `sentence-transformers:` spec loads a model from disk in `__init__`, and
    the corpus probes must be able to refuse an unembedded deployment before
    paying that cost. Parsing is a pure string operation, so it is safe to do
    after the Nomos boundary without ordering the two requirements against
    each other (PACT reachable-semantic-search).
    """
    explicit = getattr(args, "embedder", None)
    allow_unverified = getattr(
        args, "allow_unverified_embedding_provenance", False,
    )
    if method != "semantic":
        # Silently ignoring these would leave an operator believing an
        # embedder was used.
        if explicit:
            raise SubstrateError(
                f"--embedder applies only to --method semantic; got "
                f"--method {method}",
                fix_hint=(
                    "drop --embedder, or use --method semantic"
                ),
            )
        if allow_unverified:
            raise SubstrateError(
                "--allow-unverified-embedding-provenance applies only to "
                f"--method semantic; got --method {method}",
                fix_hint="drop the flag, or use --method semantic",
            )
        return None, None

    from okg.substrate.deployments import (
        DeploymentConfigError,
        DeploymentResolutionError,
        resolve_deployment,
    )

    try:
        config = resolve_deployment(deployment).embedding_config
    except DeploymentResolutionError:
        # A query with no resolvable deployment manifest is the only surface
        # where the explicit argument is authority. Environment state never
        # invents a standalone backend.
        if explicit:
            spec, source = str(explicit), "standalone_argument"
        else:
            err = SubstrateError(
                "semantic search has no resolvable deployment embedding "
                "configuration or explicit standalone --embedder",
                data={"method": "semantic"},
                fix_hint="pass --embedder for a standalone query, or use --method lexical",
            )
            err.code = "semantic_embedder_not_configured"
            raise err
    else:
        assertions = [
            ("--embedder", str(explicit) if explicit else None),
            (_EMBEDDER_ENV, os.environ.get(_EMBEDDER_ENV)),
        ]
        for label, asserted in assertions:
            if asserted is None or not asserted.strip():
                continue
            asserted = asserted.strip()
            if not config.enabled or asserted != config.backend:
                raise DeploymentConfigError(
                    f"{label} conflicts with deployment.yaml's canonical "
                    "embeddings backend"
                )
            import warnings

            warnings.warn(
                f"{label} is deprecated as deployment embedding authority; "
                "deployment.yaml embeddings.backend already supplies this value",
                UserWarning,
                stacklevel=2,
            )
        if not config.enabled or config.backend is None:
            err = SubstrateError(
                "semantic search needs a query embedder and none is enabled "
                "in deployment.yaml, so the query text cannot be turned into "
                "a vector. Nothing about the arguments you passed is wrong.",
                data={"method": "semantic", "env_var": _EMBEDDER_ENV},
                fix_hint=(
                    "enable deployment.yaml embeddings with an explicit "
                    "compatible backend, or use --method lexical"
                ),
            )
            err.code = "semantic_embedder_not_configured"
            raise err
        spec, source = config.backend, "deployment_manifest"
    # Validate the spec now so a typo is reported as a spec error rather than
    # surfacing later from inside the search. Parse only -- no construction.
    # A deployment's `sets` form backend is a set's (PACT
    # vector-set-pgvector-widths: `hash@<dims>`, OpenRouter widths above 2560).
    from okg.substrate.embeddings.identity import model_id_for_spec
    model_id_for_spec(
        spec,
        vector_set=source == "deployment_manifest" and bool(config.sets),
    )
    return spec, source


def _resolve_search_query(args: argparse.Namespace) -> str:
    """The one query text, from the positional QUERY or `--query`.

    The positional form is the documented one; `--query` stays as an alias.
    Both at once is refused unless they are the same text, and neither is
    refused, so the search never runs on a guessed or dropped query.
    """
    positional = getattr(args, "query_positional", None)
    option = getattr(args, "query", None)
    if positional is not None and option is not None:
        if positional != option:
            err = SubstrateError(
                "okg search received two different queries: the positional "
                "QUERY and --query. It will not guess which one you meant.",
                data={"positional": positional, "option": option},
                fix_hint="pass the query once, either as QUERY or as --query",
            )
            err.code = "search_query_conflict"
            raise err
        return positional
    if positional is not None:
        return positional
    if option is not None:
        return option
    err = SubstrateError(
        "okg search needs a query and none was given.",
        data={},
        fix_hint=(
            "pass the query as QUERY, for example "
            "okg search --deployment <name> 'words'"
        ),
    )
    err.code = "search_query_missing"
    raise err


#: The fix hint for a refused search argument, in CLI flag terms.
_SEARCH_FLAGS_HINT = (
    "check --method, --limit, --offset, --select, and --attr-filter values"
)


def cmd_search(
    args: argparse.Namespace, *, as_json: bool
) -> int:
    """`okg search --deployment X QUERY [page/projection/filter controls]`."""
    missing = _read_operators.require_deployment(args, "search", as_json=as_json)
    if missing is not None:
        return missing
    if _read_operators.search_takes_operator_path(args):
        return _read_operators.cmd_search_operator(args, as_json=as_json)
    from okg.substrate.mcp.passage_rollup import validate_rank_by
    from okg.substrate.mcp.search import search

    try:
        args.query = _resolve_search_query(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    try:
        resolution = resolve_read_dsn(args, surface="okg search")
        dsn = resolution.dsn
        method = args.method or "lexical"
        rank_by = validate_rank_by(
            getattr(args, "rank_by", None),
            method=method,
            max_per_parent_argument=(
                getattr(args, "max_per_parent", None) is not None
            ),
        )
        select = _parse_select(getattr(args, "select", None))
        filter_obj: dict[str, Any] = {}
        if args.subtype:
            filter_obj["subtype"] = args.subtype
        attr_filters = _parse_attr_filters(
            getattr(args, "attr_filter", None),
        )
        if attr_filters:
            filter_obj["attr_filters"] = attr_filters
        access_context = _nomos_access_context(args)
        subject, subject_meta = _resolve_cli_read_subject(
            deployment=args.deployment, default_kind="cli_search",
        )
        # Present the reconciled manifest read principal to the per-row
        # source-lineage filter too (via the session) — otherwise the
        # boundary allows while the row filter falls back to subject:$USER
        # and hides every row. Env-explicit and legacy-fallback subjects
        # keep the session subject unset so those paths behave exactly as
        # before (the row filter's own env chain, incl.
        # OKG_MCP_NOMOS_SUBJECT, still applies).
        session_subject = (
            subject
            if subject_meta.get("source") == "manifest_read_principal"
            else None
        )

        with db_connect(dsn) as conn:
            session = _open_session(
                conn, args.deployment,
                generation_id=getattr(args, "generation_id", None),
                nomos_access_context=access_context,
                nomos_subject=session_subject,
            )
            # The Nomos record gets the manifest-only view of the profile --
            # its name and DECLARED subtypes -- because that needs no database
            # and so can be computed BEFORE the boundary check. Resolving the
            # refinement closure reads the catalog, and no read may precede the
            # authorization that governs it; a denial must block before any
            # effect. The envelope carries the resolved closure.
            nomos_mode, nomos_outcome = _authorize_search_with_nomos(
                conn,
                deployment=args.deployment,
                session=session,
                query=args.query,
                method=method,
                limit=args.limit or 50,
                offset=args.offset or 0,
                filter_obj=filter_obj or None,
                select=select,
                access_context=access_context,
                subject=subject,
                subject_meta=subject_meta,
                requested_profile=getattr(args, "profile", None),
                rank_by=rank_by,
            )
            scope = _resolve_search_scope(
                conn,
                session,
                deployment=args.deployment,
                requested_profile=getattr(args, "profile", None),
                subtype_filter=args.subtype,
                method=method,
                requested_families=getattr(args, "source_family", None),
                requested_max_per_parent=getattr(
                    args, "max_per_parent", None,
                ),
            )
            # Resolved AFTER the Nomos boundary above. Keep the established
            # probe-before-construction guarantee, then build the same verified
            # execution-plan type used by writers. ``search()`` repeats its
            # readiness probe as a defence-in-depth read; it receives the plan
            # itself, never a backend reconstructed from an unchecked spec.
            embedder_spec, embedder_source = _resolve_embedder_spec(
                args, method=method, deployment=args.deployment,
            )
            embedding_plan = None
            if embedder_spec is not None:
                from okg.substrate.embeddings import (
                    EmbeddingConfig,
                    build_embedding_execution_plan,
                )
                from okg.substrate.embeddings.identity import model_id_for_spec
                from okg.substrate.mcp.search import vector_set_route_for_config
                from okg.substrate.mcp.semantic_readiness import (
                    assert_rankable,
                    probe_semantic_readiness,
                )

                if embedder_source == "deployment_manifest":
                    from okg.substrate.deployments import resolve_deployment

                    # The whole block, sets included, as the MCP server
                    # uses it: the search set's provider pin and prefixes,
                    # and the route to its registered vectors.
                    embedding_config = resolve_deployment(
                        args.deployment,
                    ).embedding_config
                else:
                    embedding_config = EmbeddingConfig(
                        enabled=True, backend=embedder_spec,
                    )
                # A registered, active search set serves the read (PACT
                # openrouter-embeddings-hnsw): search() refuses it before
                # embedding the query if it cannot rank. The old-column
                # readiness probe below applies only when no set serves it.
                route = vector_set_route_for_config(
                    conn, embedding_config,
                    generation_id=str(session.generation_id),
                )
                if route is None or route.status != "active":
                    requested_subtype = getattr(args, "subtype", None)
                    if requested_subtype is not None:
                        scoped_subtypes = [requested_subtype]
                    elif scope.subtypes is not None:
                        scoped_subtypes = list(scope.subtypes)
                    else:
                        scoped_subtypes = None
                    readiness = probe_semantic_readiness(
                        conn,
                        session,
                        scoped_subtypes=scoped_subtypes,
                    )
                    assert_rankable(
                        readiness,
                        # A `sets` form backend is read as a set's
                        # (`hash@<dims>`, wide OpenRouter widths), so a set
                        # the node columns cannot serve refuses as model
                        # skew, not as an invalid spec.
                        query_model_id=model_id_for_spec(
                            embedder_spec,
                            vector_set=bool(embedding_config.sets),
                        ),
                        allow_unverified_provenance=bool(
                            getattr(
                                args,
                                "allow_unverified_embedding_provenance",
                                False,
                            )
                        ),
                        scoped_subtypes=scoped_subtypes,
                    )
                # No probe call for a paid backend: the query's own embedding
                # is this command's only paid call.
                embedding_plan = build_embedding_execution_plan(
                    embedding_config,
                    paid_backend="no_probe",
                )
            result = search(
                conn, session,
                query=args.query, method=method,
                filter=filter_obj or None,
                limit=args.limit or 50,
                offset=args.offset or 0,
                select=select,
                scope=scope,
                rank_by=rank_by,
                embedder=embedding_plan,
                embedder_source=embedder_source,
                allow_unverified_provenance=bool(
                    getattr(
                        args, "allow_unverified_embedding_provenance", False,
                    )
                ),
                max_chars=getattr(args, "max_chars", None),
                expand_concepts=getattr(args, "expand_concepts", None),
                completeness=getattr(args, "completeness", None),
            )
            if embedding_plan is not None and isinstance(result, dict):
                result["embedder_spec"] = embedder_spec
                result["embedder_source"] = embedder_source
                result["embedding_plan_fingerprint"] = (
                    embedding_plan.fingerprint
                )
            result = _attach_resolution_metadata(result, resolution)
            if isinstance(result, dict):
                result.update(_search_nomos_fields(nomos_mode, nomos_outcome))
    except SubstrateError as exc:
        from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

        if isinstance(exc, InvalidOperatorArguments):
            # A search argument check (#2809): name the CLI flags, as the
            # plain ValueError branch below does.
            exc.fix_hint = _SEARCH_FLAGS_HINT
        return _emit_error(exc, as_json=as_json)
    except ValueError as exc:
        return _emit_error(
            SubstrateError(str(exc), fix_hint=_SEARCH_FLAGS_HINT),
            as_json=as_json,
        )
    except Exception as exc:
        return _emit_error(SubstrateError(str(exc)), as_json=as_json)

    _print_result(result, as_json=as_json, select=select)
    return 0


def register_subparsers(sub, *, add_json_flag=None) -> None:
    """Register `okg search` and the read-operator commands."""
    p_search = sub.add_parser(
        "search",
        help="Search nodes at the pinned generation",
        description=(
            "Run a lexical/semantic/alias search against the "
            "deployment's pinned generation. With only the flags listed "
            "first, the command runs the search itself; with any flag of "
            "the `MCP search tool` group it runs that tool and prints its "
            "reply."
        ),
    )
    if add_json_flag is not None:
        add_json_flag(p_search)
    else:
        p_search.add_argument(
            "--json",
            action="store_true",
            default=argparse.SUPPRESS,
        )
    _read_operators.add_deployment_flag(p_search)
    p_search.add_argument("--dsn", default=None)
    p_search.add_argument(
        "query_positional",
        nargs="?",
        default=None,
        metavar="QUERY",
        help="text to search for",
    )
    p_search.add_argument(
        "--query",
        default=None,
        help=(
            "alias for the positional QUERY; giving both with different text "
            "is refused"
        ),
    )
    p_search.add_argument(
        "--method", default="lexical",
        choices=list(_read_operators.SEARCH_METHODS),
        action=_read_operators.RecordGiven,
        help=(
            "lexical (default), semantic or alias run here; "
            "identifier_mentions, attrs and grep run the MCP search tool"
        ),
    )
    p_search.add_argument(
        "--limit", type=int, default=50, action=_read_operators.RecordGiven,
    )
    p_search.add_argument(
        "--offset", type=int, default=0, action=_read_operators.RecordGiven,
    )
    p_search.add_argument(
        "--generation-id", "--generation",
        dest="generation_id",
        default=None,
        help="pin the search to a published Chronos generation id",
    )
    p_search.add_argument(
        "--select",
        action="append",
        default=None,
        help=(
            "comma-separated row fields to return; repeatable. "
            "Unknown fields return valid_fields in the error envelope"
        ),
    )
    p_search.add_argument(
        "--subtype", default=None,
        help="restrict results to one subtype",
    )
    add_renamed_argument(
        p_search, "--scope", "--profile",
        dest="profile", default=None,
        help=(
            "deployment-declared search scope (search profile) naming the "
            "subtypes to rank within; 'all' is reserved and always means "
            "unscoped. Defaults to the deployment's search.default_profile "
            "when neither --scope nor --subtype is given"
        ),
    )
    p_search.add_argument(
        "--source-family",
        dest="source_family",
        action="append",
        default=None,
        help=(
            "deployment-declared source family to select; repeatable. "
            "Selects which producers are ranked -- it never reranks and never "
            "changes a score. Must be one of search.source_families, and must "
            "lie inside the applied profile's own families; an unknown name "
            "or an empty intersection is refused, not answered with an empty "
            "page. Use --scope all to leave a profile's authority scope"
        ),
    )
    p_search.add_argument(
        "--max-per-parent",
        dest="max_per_parent",
        type=int,
        default=None,
        help=(
            "cap how many rows one parent document may contribute, applied "
            "after ranking and before paging through the pinned "
            "catalog-declared passage relation. Rows with no declared parent "
            "are never suppressed. Must be >= 1"
        ),
    )
    p_search.add_argument(
        "--no-concept-expansion",
        dest="expand_concepts",
        action="store_false",
        default=None,
        help=(
            "do not expand the query with the deployment's declared "
            "search.concept_groups; rank the literal query instead. Expansion "
            "is already a no-op when none are declared. --method lexical only"
        ),
    )
    p_search.add_argument(
        "--completeness",
        choices=("bounded", "exact"),
        default=None,
        help=(
            "lexical retrieval mode. bounded (the default) returns the BM25 "
            "index's candidates with the count unknown, since one index scan "
            "can stop before filters run; exact reads every scoped match "
            "within a 200,000-row, 5-second bound, or refuses with "
            "lexical_index_scan_cap. --method lexical only"
        ),
    )
    p_search.add_argument(
        "--max-chars",
        dest="max_chars",
        type=int,
        default=None,
        help=(
            "per-string excerpt/preview budget (64-4096, default 280). Out of "
            "range it is refused, never clamped"
        ),
    )
    p_search.add_argument(
        "--rank-by",
        dest="rank_by",
        default=None,
        choices=["records", "self", "passages"],
        help=(
            "whose text to rank. 'records' (lexical default) returns one row "
            "per record, folding each passage into its record. 'self' (the "
            "default of other methods, and of a lexical search with "
            "--max-per-parent) ranks each row by its own attrs.text. "
            "'passages' ranks the --subtype through its declared passage "
            "relation and is valid only for lexical search"
        ),
    )
    p_search.add_argument(
        "--embedder", default=None,
        help=(
            "embedder spec for --method semantic: 'hash' or "
            "'sentence-transformers:<model>'. Falls back to OKG_MCP_EMBEDDER "
            "(the same variable the MCP server reads) so an operator and an "
            "agent querying one deployment resolve the same embedder. Invalid "
            "with --method lexical/alias"
        ),
    )
    p_search.add_argument(
        "--allow-unverified-embedding-provenance",
        dest="allow_unverified_embedding_provenance",
        action="store_true",
        default=False,
        help=(
            "rank nodes whose stored vector has NO recorded model. Off by "
            "default: unknown provenance is refused rather than assumed to "
            "match. This does NOT permit a model known to differ. Admitted "
            "rows are labelled embedding_provenance=unknown"
        ),
    )
    p_search.add_argument(
        "--nomos-access-context-json",
        default=None,
        help=(
            "Nomos fine-grained access context JSON object for benchmark/"
            "holdout reads; also accepted from OKG_NOMOS_ACCESS_CONTEXT"
        ),
    )
    p_search.add_argument(
        "--attr-filter",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help=(
            "repeatable exact attrs filter; VALUE may be JSON scalar "
            "(string, int, bool, null) or a raw string"
        ),
    )
    _read_operators.add_search_operator_flags(p_search)
    _read_operators.register_subparsers(sub, add_json_flag=add_json_flag)


DISPATCH = {
    "search": _read_operators.guarded(cmd_search),
    **_read_operators.DISPATCH,
}
