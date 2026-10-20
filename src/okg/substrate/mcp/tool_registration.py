"""Grouped MCP tool registration helpers for the substrate server."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from okg.substrate.mcp.operators import (
    CONDITIONAL_ADMIN_TOOLS,
    OPERATOR_TOOLS,
    WRITE_ADMIN_TOOLS,
)
from okg.substrate.mcp.response_bounds import LivePage, LiveStep, bound_read_tool
from okg.substrate.mcp.tool_profiles import assert_narrowing_is_servable
from okg.substrate.unknown import wire_callable

ToolFn = Callable[..., Any]
TracerFactory = Callable[[str], Callable[[ToolFn], ToolFn]]
ResponseEnvelopeFactory = Callable[[str, ToolFn], ToolFn]
ResponseMeasurement = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


def _register_tool(
    mcp: Any,
    *,
    tool_name: str,
    fn: ToolFn,
    traced: TracerFactory,
) -> None:
    # The MCP exit for tool results: an `Unknown` fact leaves as `null` with
    # its reason under `unknown_reasons` (issue #2573). Innermost, so the
    # tracer records the payload the caller receives.
    #
    # `structured_output=False` for every tool (issue #3059): no tool
    # publishes an `outputSchema`, and `ByteBoundedFastMCP.call_tool` serves
    # the tool's dictionary once, as one text block in the call's format.
    # The registered function keeps returning the dictionary to its direct
    # Python callers.
    mcp.tool(name=tool_name, structured_output=False)(
        traced(tool_name)(wire_callable(fn))
    )


def register_core_read_tools(
    mcp: Any,
    *,
    traced: TracerFactory,
    inspect_tool: ToolFn,
    search_tool: ToolFn,
    expand_tool: ToolFn,
    filter_tool: ToolFn,
    map_tool: ToolFn,
    aggregate_tool: ToolFn,
    query_tool: ToolFn,
    response_envelope: ResponseEnvelopeFactory | None = None,
    response_measurement: ResponseMeasurement | None = None,
    deployment_default_bytes: int | None = None,
    deployment_ceiling_bytes: int | None = None,
    expand_live_step: LiveStep | None = None,
    expand_live_page: LivePage | None = None,
) -> None:
    """Register the canonical AKMON graph-read algebra.

    ``deployment_default_bytes`` / ``deployment_ceiling_bytes`` carry
    ``agent_surface.response_bytes`` to the byte-bound wrapper. They reach the
    WRAPPER, never the signature: the signature publishes ``max_bytes`` as a
    plain optional integer in ``tools/list``, and a per-deployment value there
    would make every byte baseline deployment-dependent. They are also
    installed on the server itself, so a raised error on a read tool is
    bounded by the same budget the wrapper would have resolved.

    ``expand_live_step`` prepares an ``expand(live=...)`` call so the
    byte-bound wrapper can run the graph step and the live step in order and
    dispatch the adapter exactly once (issue #2141). It reaches ``expand``'s
    wrapper only, as does ``expand_live_page``, which serves
    ``expand(next=<token>)`` from a cut live answer's kept rows (#3666).
    """

    mcp.response_default_bytes = deployment_default_bytes
    mcp.response_ceiling_bytes = deployment_ceiling_bytes

    tools: tuple[tuple[str, ToolFn], ...] = (
        ("inspect", inspect_tool),
        ("search", search_tool),
        ("expand", expand_tool),
        ("filter", filter_tool),
        ("map", map_tool),
        ("aggregate", aggregate_tool),
        ("query", query_tool),
    )
    for tool_name, fn in tools:
        # Core read dictionaries are served ONCE, as one text block in the
        # call's format (issue #3059): `ByteBoundedFastMCP.call_tool` builds
        # the result from the raw dictionary, so nothing here declares an
        # output schema and the SDK never converts the dictionary itself.
        # `to_wire` runs INSIDE the byte bound (issue #2573): the bound
        # measures the payload with pydantic, which refuses an `Unknown`, and
        # the budget must include the `unknown_reasons` map the caller gets.
        registered_fn = bound_read_tool(
            tool_name,
            wire_callable(fn),
            measure_response=response_measurement,
            deployment_default_bytes=deployment_default_bytes,
            deployment_ceiling_bytes=deployment_ceiling_bytes,
            live_step=expand_live_step if tool_name == "expand" else None,
            live_page=expand_live_page if tool_name == "expand" else None,
        )
        if response_envelope is not None:
            # The envelope is deliberately OUTSIDE the byte-bound wrapper so
            # its metadata also appears on budget-validation and oversize
            # refusals. functools.wraps in both layers preserves Context
            # injection while keeping it out of the public input schema.
            registered_fn = response_envelope(tool_name, registered_fn)
        _register_tool(
            mcp,
            tool_name=tool_name,
            fn=registered_fn,
            traced=traced,
        )


def register_write_effect_tools(
    mcp: Any,
    *,
    traced: TracerFactory,
    record_tool: ToolFn,
    propose_tool: ToolFn,
) -> None:
    """Register admission-backed write-effect operators."""

    tools: tuple[tuple[str, ToolFn], ...] = (
        ("record", record_tool),
        ("propose", propose_tool),
    )
    for tool_name, fn in tools:
        _register_tool(mcp, tool_name=tool_name, fn=fn, traced=traced)


def register_source_admin_tools(
    mcp: Any,
    *,
    traced: TracerFactory,
    list_sources_tool: ToolFn,
    add_source_tool: ToolFn,
    remove_source_tool: ToolFn,
    source_status_tool: ToolFn,
    discover_sources_tool: ToolFn,
    propose_setup_tool: ToolFn,
    commit_setup_tool: ToolFn,
    connector_route_status_tool: ToolFn,
    connector_ingest_batch_tool: ToolFn,
) -> None:
    """Register source setup, source admin, and connector ingest tools."""

    tools: tuple[tuple[str, ToolFn], ...] = (
        ("list_sources", list_sources_tool),
        ("add_source", add_source_tool),
        ("remove_source", remove_source_tool),
        ("source_status", source_status_tool),
        ("discover_sources", discover_sources_tool),
        ("propose_setup", propose_setup_tool),
        ("commit_setup", commit_setup_tool),
        ("connector_route_status", connector_route_status_tool),
        ("connector_ingest_batch", connector_ingest_batch_tool),
    )
    for tool_name, fn in tools:
        _register_tool(mcp, tool_name=tool_name, fn=fn, traced=traced)


def register_conditional_admin_tools(
    mcp: Any,
    *,
    traced: TracerFactory,
    upload_tool: ToolFn | None = None,
) -> None:
    """Register admin tools whose backing deployment provisioning resolved.

    A ``None`` handler means the deployment has not provisioned that
    tool, so it is not registered at all — the caller has already decided
    this; passing ``None`` is how that decision reaches the surface.
    """

    tools: tuple[tuple[str, ToolFn | None], ...] = (("upload", upload_tool),)
    for tool_name, fn in tools:
        if fn is None:
            continue
        _register_tool(mcp, tool_name=tool_name, fn=fn, traced=traced)


def catalog_skipped_module_tool(
    mcp: Any,
    *,
    name: str,
    module: str,
    reason: str,
    boundary: str,
) -> None:
    """Record an intentionally non-admitted module helper without registering it."""

    skipped = list(getattr(mcp, "_okg_skipped_module_tools", ()))
    skipped.append(
        {
            "name": name,
            "module": module,
            "boundary": boundary,
            "reason": reason,
        }
    )
    mcp._okg_skipped_module_tools = tuple(skipped)


def apply_admitted_tool_surface(
    mcp: Any,
    *,
    allow_writes: bool,
    tool_allowlist: set[str] | frozenset[str] | None = None,
    provisioned_conditional_tools: set[str] | frozenset[str] = frozenset(),
    tool_profile: str | None = None,
    tool_profile_source: str = "env_allowlist",
    agent_surface_served: set[str] | frozenset[str] | None = None,
    access_rule_withheld: set[str] | frozenset[str] = frozenset(),
) -> None:
    """Apply the canonical MCP boundary and optional environment allowlist.

    ``access_rule_withheld`` names tools this deployment's access rules take
    off every surface (`query` under Nomos audit or enforce; issue #3666).
    They leave the boundary, and a narrowing that names one serves the rest
    rather than refusing: the rule is the deployment's, not a typo.

    ``provisioned_conditional_tools`` names the members of
    ``CONDITIONAL_ADMIN_TOOLS`` whose deployment-side backing resolved.
    It is a statement about the DEPLOYMENT and must be passed regardless
    of ``allow_writes``; a read-only launch withholds the tool through
    the write gate below, not by pretending the deployment provisions
    nothing.

    Three readbacks, deliberately not merged:

    * ``_okg_provisioned_conditional_tools`` — the deployment provisions
      these.
    * ``_okg_unprovisioned_conditional_tools`` — the deployment does not.
      NOT a defect.
    * ``_okg_conditional_tools_withheld_read_only`` — provisioned, but
      this server is read-only. Also not a defect.

    An unprovisioned conditional tool is NOT reported in
    ``_okg_missing_admitted_tools``. That readback means "an admitted
    tool failed to register", i.e. a defect; folding the cases together
    would make the defect signal unreadable.

    ``tool_allowlist`` narrows WITHIN the boundary-pruned set and is now
    fail-closed (PACT ``mcp-tool-surface-profiles``): a name that is not
    registered raises instead of contributing nothing. ``tool_profile`` and
    ``tool_profile_source`` are disclosure only — they name what chose the
    narrowing so the refusal and the readbacks can say so.

    ``agent_surface_served`` is a deployment's declared served set (PACT
    ``effective-surface-declaration``, issue #2012). It narrows the BOUNDARY,
    not the allowlist, and that placement is the point: a later
    ``--tools`` is checked against what the boundary leaves, so a
    profile naming a tool outside the declared surface REFUSES through
    ``assert_narrowing_is_servable`` instead of quietly re-widening past the
    deployment's own declaration. Applying it as an allowlist instead would
    make the last narrowing win, and ``--tools reader`` would serve
    seven tools on a deployment that declared six.
    """

    conditional = set(CONDITIONAL_ADMIN_TOOLS)
    provisioned = set(provisioned_conditional_tools) & conditional
    allowed_tools = set(OPERATOR_TOOLS)
    if allow_writes:
        allowed_tools.update(WRITE_ADMIN_TOOLS)
        allowed_tools.update(provisioned)
    if agent_surface_served is not None:
        # Intersected, never assigned: the declared set may not grant a tool
        # the launch withholds. A deployment declaring `record` in `served`
        # on a read-only launch still gets no `record` — the write gate is
        # the launch operator's, and a manifest cannot open it.
        allowed_tools &= set(agent_surface_served)
    withheld = set(access_rule_withheld)
    allowed_tools -= withheld
    mcp._okg_access_rule_withheld_tools = tuple(sorted(withheld))  # type: ignore[attr-defined]

    registered = set(mcp._tool_manager._tools)  # type: ignore[attr-defined]
    boundary_pruned = tuple(sorted(registered - allowed_tools))
    for tool_name in boundary_pruned:
        mcp._tool_manager._tools.pop(tool_name, None)  # type: ignore[attr-defined]

    post_boundary = set(mcp._tool_manager._tools)  # type: ignore[attr-defined]
    mcp._okg_default_tools = tuple(OPERATOR_TOOLS)  # type: ignore[attr-defined]
    mcp._okg_write_admin_tools = tuple(WRITE_ADMIN_TOOLS)  # type: ignore[attr-defined]
    mcp._okg_conditional_admin_tools = tuple(  # type: ignore[attr-defined]
        sorted(conditional)
    )
    mcp._okg_provisioned_conditional_tools = tuple(  # type: ignore[attr-defined]
        sorted(provisioned)
    )
    mcp._okg_unprovisioned_conditional_tools = tuple(  # type: ignore[attr-defined]
        sorted(conditional - provisioned)
    )
    mcp._okg_conditional_tools_withheld_read_only = tuple(  # type: ignore[attr-defined]
        () if allow_writes else sorted(provisioned)
    )
    mcp._okg_boundary_pruned_tools = boundary_pruned  # type: ignore[attr-defined]
    mcp._okg_missing_admitted_tools = tuple(  # type: ignore[attr-defined]
        sorted(allowed_tools - post_boundary)
    )

    mcp._okg_agent_surface_served = (  # type: ignore[attr-defined]
        tuple(sorted(agent_surface_served))
        if agent_surface_served is not None else None
    )
    mcp._okg_tool_profile = tool_profile  # type: ignore[attr-defined]
    mcp._okg_tool_profile_source = (  # type: ignore[attr-defined]
        tool_profile_source if tool_allowlist is not None else "unnarrowed"
    )

    if tool_allowlist is not None:
        tool_allowlist = set(tool_allowlist) - withheld
        registered = set(mcp._tool_manager._tools)  # type: ignore[attr-defined]
        # Fail closed BEFORE pruning. A set difference cannot tell a tool the
        # operator did not ask for from a tool that does not exist, so the
        # requested names are checked against what is actually registered
        # first; otherwise a typo just serves a smaller surface in silence.
        assert_narrowing_is_servable(
            tool_allowlist,
            registered=registered,
            profile=tool_profile,
            source=tool_profile_source,
        )
        pruned = tuple(sorted(registered - set(tool_allowlist)))
        for tool_name in pruned:
            mcp._tool_manager._tools.pop(tool_name, None)  # type: ignore[attr-defined]
        mcp._okg_tool_allowlist = tuple(sorted(tool_allowlist))  # type: ignore[attr-defined]
        mcp._okg_pruned_tools = pruned  # type: ignore[attr-defined]

    mcp._okg_served_tools = tuple(  # type: ignore[attr-defined]
        sorted(mcp._tool_manager._tools)  # type: ignore[attr-defined]
    )
