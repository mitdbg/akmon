"""Setup-planning surface: discover_sources + propose_setup + helpers.

Extracted from ``mcp/source_admin.py`` by
``extract-mcp-setup-planning`` (2026-05) — the audit's
biggest-single-extraction-win recommendation (~660 LOC).

Two MCP tools live here:
  - ``discover_sources(conn, deployment)`` — survey what's on
    the operator's machine.
  - ``propose_setup(conn, deployment, ...)`` — generate a plan
    of questions + a list of sources to wire on commit.

Plus 8 private helpers (``_format_propose_summary``,
``_build_already_wired``, ``_connector_route_summary``,
``_credentialed_connector_row``, ``_explicit_project_candidates``,
``_looks_like_full_person_name``, ``_load_init_answers``,
``_setup_questions_for_plan``).

The in-process plan cache (`_PLANS` / `_stash_plan` / `_load_plan`)
stays in ``source_admin`` because ``commit_setup`` needs to look
up plans by id; ``propose_setup`` here defer-imports
``_stash_plan`` to register a new plan. Same shape for the
cross-cutting formatters (``_truncate_summary``,
``_format_count_summary``).

``mcp/source_admin.py`` re-exports the two public tools +
``_credentialed_connector_row`` so:
  - ``server.py``'s thin wrappers (``_source_admin.discover_sources``
    and ``_source_admin.propose_setup``) keep working.
  - ``connector_ingest.py``'s deferred imports of
    ``_credentialed_connector_row`` and ``discover_sources`` keep
    resolving via source_admin.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import psycopg
import yaml
from okg.substrate import fastyaml

from okg.substrate.mcp.source_catalog import (
    SOURCE_CATALOG,
    _CREDENTIAL_SOURCE_IDS,
    _DEEP_INDEXING_SOURCE_IDS,
    _LOCAL_PROJECT_SOURCE_IDS,
    _SOURCE_FAMILY_RANK,
)


_log = logging.getLogger(__name__)


def _source_family(source_id: str) -> str:
    return _SOURCE_FAMILY_RANK.get(source_id, (90, "other"))[1]


def _source_priority_key(entry: dict) -> tuple[int, str]:
    source_id = entry.get("source_id") or ""
    return (_SOURCE_FAMILY_RANK.get(source_id, (90, "other"))[0], source_id)


# Defer-import shim for source_admin internals — populated on
# first use, then accessed as module globals. Avoids the
# circular-import deadlock that a top-level
# `from okg.substrate.mcp.source_admin import ...` would create
# (source_admin re-imports from this module at module-load time).
_counts_for: Any = None
_deployment_dir: Any = None
_format_count_summary: Any = None
_load_registry_yaml: Any = None
_short_name_for_source_id: Any = None
_stash_plan: Any = None
_truncate_summary: Any = None


def _load_source_admin_helpers() -> None:
    """Bind source_admin helpers into this module's globals on
    first call. None-guarded so test monkeypatches survive."""
    global _counts_for, _deployment_dir, _format_count_summary
    global _load_registry_yaml, _short_name_for_source_id
    global _stash_plan, _truncate_summary
    from okg.substrate.mcp.source_admin import (
        _counts_for as _cf,
        _deployment_dir as _dd,
        _format_count_summary as _fcs,
        _load_registry_yaml as _lry,
        _short_name_for_source_id as _sns,
        _stash_plan as _sp,
        _truncate_summary as _ts,
    )
    if _counts_for is None:
        _counts_for = _cf
    if _deployment_dir is None:
        _deployment_dir = _dd
    if _format_count_summary is None:
        _format_count_summary = _fcs
    if _load_registry_yaml is None:
        _load_registry_yaml = _lry
    if _short_name_for_source_id is None:
        _short_name_for_source_id = _sns
    if _stash_plan is None:
        _stash_plan = _sp
    if _truncate_summary is None:
        _truncate_summary = _ts


def discover_sources(
    conn: psycopg.Connection,  # noqa: ARG001 — reserved for future use
    deployment: str,
    *,
    install_answer_overrides: Optional[dict[str, Any]] = None,
) -> dict:
    """Walk the deployment's source-defaults + the operator's
    filesystem + env, return ranked per-source candidates.

    Read-only: doesn't touch the registry, doesn't run ingest.
    Idempotent against an unchanged filesystem snapshot.

    Per `improve-add-source-operator-experience`: this is the
    foundation of the intent-and-approval flow. The agent
    calls this first (usually transparently via
    `propose_setup`) to learn what's actually on the
    operator's machine instead of asking them path by path.
    """
    _load_source_admin_helpers()
    from okg.substrate.library.sources.discovery import (
        DiscoveryEnv,
    )
    from okg.substrate.library.sources.discovery.registry import (
        DISCOVERERS, discover_for_source_id,
    )

    try:
        _path, raw = _load_registry_yaml(deployment)
    except FileNotFoundError as e:
        return {
            "status": "deployment_not_found",
            "error": str(e),
            "summary": f"No deployment `{deployment}`.",
        }

    dep_dir = _deployment_dir(deployment)
    env = DiscoveryEnv.for_deployment(dep_dir)
    if install_answer_overrides:
        env.install_answers.update({
            k: v for k, v in install_answer_overrides.items()
            if v not in (None, "")
        })

    candidates_out: list[dict] = []
    n_high = 0
    n_skip = 0
    n_needs_op = 0
    sources_in_registry = raw.get("sources") or {}
    # Walk every source the registry knows about. Sources without
    # a discoverer entry get an empty candidate list with a
    # "no automatic discovery" blocker.
    for source_id in sorted(
        sources_in_registry,
        key=lambda sid: _SOURCE_FAMILY_RANK.get(sid, (90, "other"))[0],
    ):
        short = _short_name_for_source_id(source_id)
        kind = (
            SOURCE_CATALOG.get(short, (source_id, "unknown"))[1]
            if short else "unknown"
        )
        cands = discover_for_source_id(source_id, env)
        candidates_out.append({
            "source_id": source_id,
            "short": short,
            "kind": kind,
            "source_family": _source_family(source_id),
            "candidates": [c.to_dict() for c in cands],
            "blockers": (
                []
                if cands or source_id in DISCOVERERS
                else ["no automatic discovery; explicit params required"]
            ),
        })
        if any(c.confidence == "high" for c in cands):
            n_high += 1
        elif not cands:
            n_skip += 1
        else:
            n_needs_op += 1

    return {
        "status": "ok",
        "deployment": deployment,
        "candidates": candidates_out,
        "n_high_confidence": n_high,
        "n_skippable": n_skip,
        "n_needs_operator": n_needs_op,
        "summary": (
            f"Discovered {n_high} high-confidence source(s); "
            f"{n_needs_op} need(s) input; "
            f"{n_skip} skippable."
        ),
        "inspected_paths": list(env.inspected_paths),
    }


# In-process LRU for plans, keyed by `plan_id`. TTL 30 minutes.
# Per Decision 6 of the design — plans are snapshots of "the
# operator's machine at time T" and shouldn't persist across
# MCP-server restarts.


def _format_propose_summary(
    would_wire: list[dict],
    n_skip: int,
    max_listed: int = 3,
    max_len: int = 80,
    *,
    n_already_wired: int = 0,
    okg_workspace_skipped: bool = False,
) -> str:
    """Build the one-line operator-facing summary for
    `propose_setup` — name the first `max_listed` short_names
    of `would_wire` + a `(+N)` overflow indicator, plus
    `Skip <n>.`

    When `would_wire` is empty AND `n_already_wired > 0`,
    surfaces the "fully-set-up deployment" case so the
    operator knows nothing's actionable — rather than the
    pre-fix `"Nothing to wire (skip M). Approve with
    commit_setup."` which left the operator wondering why
    they should approve a no-op.
    """
    _load_source_admin_helpers()
    n_wire = len(would_wire)
    if n_wire == 0:
        if okg_workspace_skipped and n_already_wired == 0:
            return _truncate_summary(
                "Nothing to wire yet. Choose a local repo to index.",
                max_len,
            )
        if n_already_wired > 0:
            if okg_workspace_skipped:
                return _truncate_summary(
                    f"Nothing new to wire. "
                    f"{n_already_wired} already configured. "
                    "Local repos skipped.",
                    max_len,
                )
            return _truncate_summary(
                f"Nothing new to wire. "
                f"{n_already_wired} already configured. "
                f"Skip {n_skip}.",
                max_len,
            )
        return _truncate_summary(
            f"Nothing to wire (skip {n_skip}). "
            f"Approve with `commit_setup`.",
            max_len,
        )
    shorts = [w.get("short") or w.get("source_id") for w in would_wire]
    listed = shorts[:max_listed]
    extra = n_wire - len(listed)
    name_part = ", ".join(listed)
    if extra > 0:
        name_part = f"{name_part} (+{extra})"
    if okg_workspace_skipped:
        s = f"Wire {n_wire}: {name_part}. Skip local repos until chosen."
        return _truncate_summary(s, max_len)
    s = f"Wire {n_wire}: {name_part}. Skip {n_skip}."
    return _truncate_summary(s, max_len)


def _build_already_wired(
    conn: psycopg.Connection,
    sources: dict,
) -> list[dict]:
    """Return one entry per source whose `enabled_by_default`
    is true in the registry, with `{short, source_id,
    summary}`. The agent uses this to give the operator a
    complete picture ("you already have 8 wired, would add 3
    more")."""
    _load_source_admin_helpers()
    out: list[dict] = []
    for source_id, entry in sorted(sources.items()):
        if not entry.get("enabled_by_default", False):
            continue
        ownership_id = entry.get("ownership_id") or source_id
        counts = _counts_for(conn, ownership_id)
        short = _short_name_for_source_id(source_id) or source_id
        out.append({
            "short": short,
            "source_id": source_id,
            "summary": _format_count_summary(counts),
        })
    return out


def _connector_route_summary(route: Optional[dict]) -> str:
    _load_source_admin_helpers()
    if not route:
        return "No trusted connector route is connected yet."
    auth = route.get("auth_owner") or "unknown"
    custody = route.get("token_custody") or "unknown"
    readonly = "read-only" if not route.get("supports_write") else "read/write"
    allowlists = route.get("allowlist_fields") or []
    allowlist_part = (
        f" Requires {', '.join(allowlists)} allowlist."
        if allowlists else ""
    )
    return _truncate_summary(
        f"{route.get('label')}: auth={auth}, tokens={custody}, "
        f"{readonly}.{allowlist_part}",
        160,
    )


def _credentialed_connector_row(entry: dict) -> dict:
    _load_source_admin_helpers()
    from okg.substrate.library.sources.discovery import (
        ConnectorRoute,
        RouteVettingError,
        select_connector_route,
    )

    source_id = entry["source_id"]
    short = entry["short"] or source_id
    routes: list[ConnectorRoute] = []
    for cand in entry.get("candidates") or []:
        for raw in cand.get("connector_routes") or []:
            try:
                routes.append(ConnectorRoute(**raw))
            except (TypeError, ValueError):
                continue
    vetting_failure: RouteVettingError | None = None
    try:
        selected = select_connector_route(routes)
    except RouteVettingError as exc:
        # Per harden-connector-route-vetting: a security-vetting
        # failure on the policy-preferred route surfaces as its own
        # setup state with the criterion named — never a silent
        # fallback to a lower-ranked route.
        selected = None
        vetting_failure = exc
    selected_dict = selected.to_dict() if selected else None
    if vetting_failure is not None:
        setup_state = "vetting_failed"
        reason = (
            f"route {vetting_failure.route_id} refused by security "
            f"vetting: {', '.join(vetting_failure.failed_criteria)}"
        )
    elif selected:
        setup_state = "needs_allowlist"
        reason = "allowlist required before ingest"
    else:
        setup_state = "needs_connection"
        reason = "connect app in host/provider/gateway"
    diagnostics: dict = {"reason": reason}
    if vetting_failure is not None:
        diagnostics["vetting_failed_route"] = vetting_failure.route_id
        diagnostics["vetting_failed_criteria"] = (
            vetting_failure.failed_criteria
        )
    return {
        "source_id": source_id,
        "short": short,
        "source_family": _source_family(source_id),
        "setup_state": setup_state,
        "selected_route": selected_dict,
        "route_summary": _connector_route_summary(selected_dict),
        "available_routes_count": len(routes),
        "blocked_routes": [
            route.to_dict() for route in routes
            if selected is None or route.route_id != selected.route_id
        ],
        "diagnostics": diagnostics,
    }


def _explicit_project_candidates(
    *,
    source_id: str,
    deployment: str,
    project_paths: list[str],
) -> list[dict]:
    """Build high-confidence candidates from explicit repo paths.

    Default discovery must not treat the framework checkout as an
    operator project. This helper is only called when the agent passes
    `project_paths`, which represents explicit operator intent.
    """
    _load_source_admin_helpers()
    if source_id not in _LOCAL_PROJECT_SOURCE_IDS:
        return []
    out: list[dict] = []
    for raw_path in project_paths:
        repo = Path(raw_path).expanduser().resolve()
        if not repo.is_dir() or not (repo / ".git").exists():
            continue
        params: dict[str, object]
        candidate_path = repo
        if source_id in {"git_files", "git_history", "code_structure"}:
            params = {"repo": deployment, "repo_root": str(repo)}
        elif source_id == "openspec":
            openspec_root = repo / "openspec"
            if not openspec_root.is_dir():
                continue
            params = {
                "repo": deployment,
                "openspec_root": str(openspec_root),
            }
            candidate_path = openspec_root
        elif source_id == "agent_sessions":
            params = {"repo": deployment, "cwd_filter": [str(repo)]}
        else:
            params = {}
        out.append({
            "params": params,
            "path": str(candidate_path),
            "confidence": "high",
            "evidence": f"operator-selected project repo: {repo}",
            "summary": f"{source_id} for {repo.name}",
        })
    return out


def _looks_like_full_person_name(value: object) -> bool:
    _load_source_admin_helpers()
    raw = str(value or "").strip()
    tokens = [
        tok.strip(".,")
        for tok in raw.split()
        if any(ch.isalpha() for ch in tok)
    ]
    return len(tokens) >= 2 and len(tokens[-1]) >= 2


def _load_init_answers(deployment: str) -> dict[str, Any]:
    _load_source_admin_helpers()
    try:
        dep_dir = _deployment_dir(deployment)
    except FileNotFoundError:
        return {}
    dy = dep_dir / "deployment.yaml"
    if not dy.is_file():
        return {}
    try:
        raw = fastyaml.safe_load(dy.read_text()) or {}
    except yaml.YAMLError:
        # Malformed deployment.yaml: log loud + return empty so
        # the wizard can still proceed with identity questions
        # the operator may not have answered yet. Post-MCP-audit
        # cleanup — the prior silent swallow meant a YAML typo
        # silently re-asked the operator every identity question.
        _log.exception(
            "_load_init_answers: deployment.yaml failed to parse "
            "for %s; treating as empty init_answers",
            deployment,
        )
        return {}
    return (raw.get("profile") or {}).get("init_answers") or {}


def _setup_questions_for_plan(
    deployment: str,
    registry_sources: dict,
    *,
    identity_overrides: dict[str, Optional[str]],
) -> list[dict[str, Any]]:
    """Focused human questions that improve setup quality.

    These are not form fields. They are prompts for cases where
    proceeding silently would produce bad public-source lookups.
    """
    _load_source_admin_helpers()
    answers = _load_init_answers(deployment)
    merged = {
        **answers,
        **{
            k: v for k, v in identity_overrides.items()
            if v not in (None, "")
        },
    }
    has_precise_identity = bool(
        merged.get("orcid")
        or merged.get("dblp_pid")
        or _looks_like_full_person_name(merged.get("pi_name"))
    )
    identity_source_ids = {
        "dblp_by_orcid",
        "nsf_award_search",
        "doe_osti",
        "mit_course_catalog",
    }
    identity_sources_active = any(
        sid in registry_sources
        and not (registry_sources.get(sid) or {}).get(
            "enabled_by_default", False,
        )
        for sid in identity_source_ids
    )
    if has_precise_identity or not identity_sources_active:
        return []
    weak_name = str(merged.get("pi_name") or "").strip()
    question = (
        "What full name, ORCID, or DBLP PID should I use for "
        "publications, grants, and courses?"
    )
    if weak_name:
        question = (
            f"I only know the name `{weak_name}`, which is too ambiguous "
            "for public scholarly lookup. What full name, ORCID, or DBLP "
            "PID should I use?"
        )
    return [{
        "id": "public_identity",
        "question": question,
        "why": (
            "One-token or missing names produce ambiguous DBLP, NSF, "
            "DOE, and course-catalog results."
        ),
        "answer_fields": ["pi_name", "orcid", "dblp_pid"],
        "skip_option": (
            "Skip public identity sources for now and continue with "
            "local files."
        ),
    }]


def propose_setup(
    conn: psycopg.Connection,
    deployment: str,
    *,
    include_threshold: str = "high",
    prefer_paths: Optional[list[str]] = None,
    exclude_paths: Optional[list[str]] = None,
    only: Optional[list[str]] = None,
    project_paths: Optional[list[str]] = None,
    pi_name: Optional[str] = None,
    orcid: Optional[str] = None,
    dblp_pid: Optional[str] = None,
) -> dict:
    """Discover + bundle into a single approval-shape plan.

    Returns `{plan_id, deployment, would_wire, would_skip,
    summary}`. Agent reads `summary` aloud, operator says
    "yes," agent calls `commit_setup(plan_id)`.

    Narrowing kwargs:
      - `include_threshold`: confidence cutoff for
        `would_wire`. Accepts `"high" | "medium" | "low"`
        (case sensitive). Default `"high"`.
      - `prefer_paths`: list of substring matches. When
        multiple candidates exist for a source, prefer ones
        whose path contains any of these substrings.
      - `exclude_paths`: substring exclusions. Drop
        candidates whose path matches.
      - `only`: narrow the proposal to these source_ids
        (or short names). Everything else moves to
        would_skip with reason "not in `only` filter".
      - `project_paths`: explicit operator-selected repository
        paths. Only with this argument will repo/code/OpenSpec/
        agent-session sources be proposed for Workspace Graph
        ingestion.
      - `pi_name`, `orcid`, `dblp_pid`: optional identity answers
        collected conversationally after a prior proposal asked for
        them. These affect only the generated plan; commit persists
        them as per-source params for approved sources.
    """
    _load_source_admin_helpers()
    threshold_order = {"high": 3, "medium": 2, "low": 1, "n/a": 0}
    cutoff = threshold_order.get(include_threshold, 3)
    prefer = prefer_paths or []
    exclude = exclude_paths or []
    only_set = set(only or [])
    explicit_project_paths = project_paths or []

    identity_overrides = {
        "pi_name": pi_name,
        "orcid": orcid,
        "dblp_pid": dblp_pid,
    }
    # Look up discover_sources via the source_admin module so
    # `mock.patch("okg.substrate.mcp.source_admin.discover_sources",
    # ...)` (used by 9 propose_setup tests) keeps overriding what
    # we actually invoke. Direct local-name reference would
    # bypass the patched attribute.
    from okg.substrate.mcp import source_admin as _sa
    discovery = _sa.discover_sources(
        conn,
        deployment,
        install_answer_overrides=identity_overrides,
    )
    if discovery.get("status") != "ok":
        return discovery  # propagate deployment_not_found

    # Per `trim-setup-to-graph-reality`: read registry-enabled
    # state up-front so `would_wire` represents only the true
    # new-wire delta. Already-enabled sources go into
    # `already_wired` (with their row counts) and are excluded
    # from `would_wire` even if discovery picks them up.
    try:
        _path, raw = _load_registry_yaml(deployment)
        registry_sources = raw.get("sources") or {}
    except FileNotFoundError:
        registry_sources = {}
    enabled_source_ids: set[str] = {
        sid for sid, entry in registry_sources.items()
        if entry.get("enabled_by_default", False)
    }
    enabled_short_names: set[str] = {
        s for s in (
            _short_name_for_source_id(sid)
            for sid in enabled_source_ids
        ) if s
    }

    would_wire: list[dict] = []
    would_skip: list[dict] = []
    credentialed_connectors: list[dict] = []
    okg_workspace_skipped = False

    for entry in discovery["candidates"]:
        source_id = entry["source_id"]
        short = entry["short"] or source_id
        cands = list(entry["candidates"])
        if explicit_project_paths and source_id in _LOCAL_PROJECT_SOURCE_IDS:
            cands = _explicit_project_candidates(
                source_id=source_id,
                deployment=deployment,
                project_paths=explicit_project_paths,
            )

        # `only` filter takes precedence.
        if only_set and (
            source_id not in only_set and short not in only_set
        ):
            would_skip.append({
                "source_id": source_id, "short": short,
                "reason": "not in `only` filter",
            })
            continue

        # Per `trim-setup-to-graph-reality`: if the source is
        # already enabled in the registry, exclude it from
        # would_wire entirely — it's surfaced via
        # already_wired with its current row counts. The
        # operator's approval should reflect the true delta.
        if source_id in enabled_source_ids or short in enabled_short_names:
            continue

        if source_id in _CREDENTIAL_SOURCE_IDS or entry["kind"] == "connector":
            credentialed_connectors.append(
                _credentialed_connector_row(entry),
            )
            continue

        # Apply path filters + confidence threshold.
        filtered = []
        for c in cands:
            c_path = c.get("path", "") or ""
            if exclude and any(x in c_path for x in exclude):
                continue
            c_score = threshold_order.get(
                c.get("confidence", "n/a"), 0,
            )
            if c_score < cutoff:
                continue
            filtered.append(c)
        if not filtered:
            reason_bits = []
            if (
                source_id in _LOCAL_PROJECT_SOURCE_IDS
                and not explicit_project_paths
            ):
                reason_bits.append("Workspace Graph repos skipped until chosen")
                okg_workspace_skipped = True
            elif entry["blockers"]:
                reason_bits.append(entry["blockers"][0])
            if cands and not filtered:
                reason_bits.append(
                    f"below {include_threshold}-confidence threshold"
                )
            elif not cands:
                reason_bits.append("no candidates discovered")
            would_skip.append({
                "source_id": source_id, "short": short,
                "reason": "; ".join(reason_bits) or "skipped",
            })
            continue

        # Apply prefer_paths ranking.
        if prefer:
            filtered.sort(
                key=lambda c: -sum(
                    1 for p in prefer if p in (c.get("path") or "")
                ),
            )
        chosen = filtered[0]
        would_wire.append({
            "source_id": source_id,
            "short": short,
            "kind": entry["kind"],
            "source_family": _source_family(source_id),
            "params": chosen["params"],
            "path": chosen["path"],
            "summary": chosen.get("summary") or "",
            "confidence": chosen["confidence"],
            "alternates_count": max(0, len(filtered) - 1),
        })

    would_wire.sort(key=_source_priority_key)
    credentialed_connectors.sort(key=_source_priority_key)
    local_public = [
        row for row in would_wire
        if row.get("source_family") not in {
            "selected_project_repo", "project_agent_memory",
            "deep_indexing", "communications_calendar",
        }
    ]
    deep_indexing = [
        row for row in would_wire
        if row["source_id"] in _DEEP_INDEXING_SOURCE_IDS
        or row.get("source_family") in {"deep_indexing", "project_agent_memory"}
    ]
    n_skip = len(would_skip)

    # Build already_wired from the same registry snapshot we
    # used for the enabled-source set above.
    already_wired = _build_already_wired(conn, registry_sources)

    plan = {
        "deployment": deployment,
        "include_threshold": include_threshold,
        "prefer_paths": prefer,
        "exclude_paths": exclude,
        "only": sorted(only_set),
        "project_paths": explicit_project_paths,
        "identity_overrides": {
            k: v for k, v in identity_overrides.items()
            if v not in (None, "")
        },
        "would_wire": would_wire,
        "would_skip": would_skip,
        "already_wired": already_wired,
        "local_public": local_public,
        "credentialed_connectors": credentialed_connectors,
        "deep_indexing": deep_indexing,
        "setup_questions": _setup_questions_for_plan(
            deployment,
            registry_sources,
            identity_overrides=identity_overrides,
        ),
    }
    plan_id = _stash_plan(plan)
    return {
        "status": "ok",
        "plan_id": plan_id,
        **plan,
        "summary": _format_propose_summary(
            would_wire,
            n_skip,
            n_already_wired=len(already_wired),
            okg_workspace_skipped=okg_workspace_skipped,
        ),
    }

