"""Per-source-id discoverers — what each academic-researcher source
looks for on the operator's machine.

Per the proposal: each `Source` class MAY implement
`discover(env)` directly. As a low-friction alternative, the
registry below maps `source_id` to a discoverer function so
existing source modules don't have to change. The MCP tool's
`discover_sources` walks this registry first, then falls back
to `cls.discover()` if the class implements it directly.

Each discoverer returns `list[Candidate]` — ranked, most-
likely first.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from .candidate import (
    Candidate,
    Confidence,
    ConnectorRoute,
    ConnectorRouteKind,
    ConnectorTrustTier,
)
from .env import DiscoveryEnv
from .heuristics import (
    COMMON_BIB_ROOTS,
    COMMON_CV_PATHS,
    COMMON_NOTES_ROOTS,
    COMMON_PAPER_ROOTS,
    find_dirs_with_content,
    make_path_candidate,
    score_dir_confidence,
)


# ---------- per-source discoverers ----------


def _discover_notes_dir(env: DiscoveryEnv) -> list[Candidate]:
    """Look for likely notes directories under the home dir.

    Ranks Obsidian / Logseq vaults higher than plain
    `~/notes` because the formats are more structured.
    """
    env.start_scan()
    try:
        hits = find_dirs_with_content(
            env,
            candidate_roots=COMMON_NOTES_ROOTS,
            suffixes=(".md", ".markdown", ".txt"),
            min_files=1,
        )
    finally:
        env.end_scan()
    candidates: list[Candidate] = []
    for path, count, by_suffix in hits:
        # Boost confidence if the path name suggests a
        # purpose-built notes app.
        confidence = score_dir_confidence(file_count=count)
        if any(
            marker in str(path).lower()
            for marker in ("obsidian", "logseq", "zettelkasten")
        ) and count >= 3:
            confidence = Confidence.HIGH
        # Per add-academic-corpora-sources: an `.obsidian/` marker
        # identifies a vault regardless of the directory's name —
        # the highest-signal notes corpus a professor has.
        is_obsidian_vault = env.dir_exists(path / ".obsidian")
        if is_obsidian_vault:
            confidence = Confidence.HIGH
        candidates.append(make_path_candidate(
            param_key="notes_dir",
            path=path,
            file_count=count,
            by_suffix=by_suffix,
            confidence=confidence,
            source_label="notes",
            env=env,
        ))
        if is_obsidian_vault:
            candidates[-1].evidence = (
                f"Obsidian vault (.obsidian/ marker); "
                f"{candidates[-1].evidence}"
            )
            candidates[-1].extra["obsidian_vault"] = True
        lower_path = str(path).lower()
        if any(
            marker in lower_path
            for marker in (
                "reviewer", "reviewing", "teaching", "talks",
                ".research-notes",
            )
        ):
            candidates[-1].extra["corpus_family"] = "local_research_corpus"
    return candidates


def _discover_paper_pdf_inbox(env: DiscoveryEnv) -> list[Candidate]:
    # `param_key` SHALL be `inbox_path` — that's what
    # `PaperPdfSource.__init__` accepts (see
    # `scholarly/paper_pdf_source.py`), and what the source-default
    # YAML's templating uses. Pre-fix this emitted
    # `paper_pdf_inbox_dir`, which silently broke every
    # `okg add papers` since the wizard catalog shipped (caught
    # by `tighten-setup-feedback-loop`'s signature-agreement test).
    env.start_scan()
    try:
        hits = find_dirs_with_content(
            env,
            candidate_roots=COMMON_PAPER_ROOTS,
            suffixes=(".pdf",),
            min_files=1,
        )
    finally:
        env.end_scan()
    candidates: list[Candidate] = []
    for path, count, by_suffix in hits:
        confidence = score_dir_confidence(
            file_count=count, high_threshold=5, medium_threshold=2,
        )
        candidates.append(make_path_candidate(
            param_key="inbox_path",
            path=path,
            file_count=count,
            by_suffix=by_suffix,
            confidence=confidence,
            source_label="papers",
            env=env,
        ))
    return candidates


def _discover_cv_ingest_llm(env: DiscoveryEnv) -> list[Candidate]:
    """Find a CV file. Picks the most-recently-modified
    candidate. Confidence drops if the operator's deployment
    has no LLM API key configured (the source can't actually
    run without it)."""
    found: list[tuple[Path, float]] = []
    for rel in COMMON_CV_PATHS:
        abs_path = env.home_dir / rel
        if env.file_exists(abs_path):
            try:
                found.append((abs_path, abs_path.stat().st_mtime))
            except OSError:
                continue
    if not found:
        return []
    # Pick the most recent.
    found.sort(key=lambda fm: -fm[1])
    best_path, _ = found[0]
    # LLM cv extractor needs an API key. Without one, the
    # source will fail at ingest — flag confidence accordingly.
    has_key = bool(
        env.env_var("OPENAI_API_KEY") or env.env_var("ANTHROPIC_API_KEY")
    )
    confidence = Confidence.MEDIUM if has_key else Confidence.LOW
    evidence_suffix = "" if has_key else " (no OPENAI/ANTHROPIC_API_KEY set)"
    summary = f"CV at ~/{best_path.relative_to(env.home_dir)}"
    if len(summary) > 80:
        summary = summary[:77] + "..."
    return [Candidate(
        params={"cv_path": str(best_path)},
        path=str(best_path),
        confidence=confidence,
        evidence=f"file: {best_path.name}{evidence_suffix}",
        summary=summary,
        extra={
            "has_llm_api_key": has_key,
            "alternates_found": [str(p) for p, _ in found[1:3]],
        },
    )]


def _discover_zotero(env: DiscoveryEnv) -> list[Candidate]:
    """Zotero, local-first (add-academic-corpora-sources): the
    `~/Zotero` data dir (zotero.sqlite) plus Better-BibTeX
    auto-exported `.bib` files feed the existing bib_references
    source — no cloud OAuth. The opt-in localhost read API
    (127.0.0.1:23119) is detected as a richer-metadata route but
    its absence never degrades the file candidate."""
    zotero_dir = env.home_dir / "Zotero"
    if not env.dir_exists(zotero_dir):
        return []
    has_sqlite = env.file_exists(zotero_dir / "zotero.sqlite")
    if not has_sqlite:
        return []
    bib_files = env.list_files(
        zotero_dir, suffixes=(".bib",), max_depth=2,
    )
    local_api = _zotero_local_api_listening(env)
    extra: dict = {
        "zotero_data_dir": str(zotero_dir),
        "zotero_local_api": local_api,
        "bib_export_count": len(bib_files),
    }
    if not bib_files:
        return [Candidate(
            params={},
            path=str(zotero_dir),
            confidence=Confidence.MEDIUM,
            evidence=(
                "zotero.sqlite present; no Better-BibTeX .bib "
                "auto-export found — enable BBT auto-export to wire "
                "bib_references"
            ),
            summary="Zotero library found (needs BBT auto-export)",
            extra={**extra, "needs_operator_input": True},
        )]
    newest = max(bib_files, key=lambda p: p.stat().st_mtime)
    import datetime as _dt
    recency = _dt.datetime.fromtimestamp(
        newest.stat().st_mtime,
    ).strftime("%Y-%m-%d")
    api_note = " + localhost read API" if local_api else ""
    return [Candidate(
        params={"bib_paths": [str(zotero_dir / "*.bib")]},
        path=str(zotero_dir),
        confidence=Confidence.HIGH,
        evidence=(
            f"zotero.sqlite + {len(bib_files)} BBT .bib export(s), "
            f"newest {recency}{api_note}"
        ),
        summary="Zotero library (Better-BibTeX exports)",
        extra=extra,
    )]


def _zotero_local_api_listening(env: DiscoveryEnv) -> bool:
    """Probe the opt-in Zotero 7 localhost read API. Failure of
    any kind means 'not listening' — never an error."""
    import urllib.error
    import urllib.request

    port = env.env_var("OKG_ZOTERO_LOCAL_API_PORT", "23119")
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/",
            headers={"User-Agent": "okg-discovery/1.0"},
        )
        with urllib.request.urlopen(req, timeout=1):
            return True
    except urllib.error.HTTPError:
        # Listening but unhappy with the path — still a live API.
        return True
    except Exception:  # noqa: BLE001 — absence is a normal state, not an error
        return False


def _discover_bib_references(env: DiscoveryEnv) -> list[Candidate]:
    env.start_scan()
    try:
        hits = find_dirs_with_content(
            env,
            candidate_roots=COMMON_BIB_ROOTS,
            suffixes=(".bib",),
            min_files=1,
        )
    finally:
        env.end_scan()
    candidates: list[Candidate] = []
    for path, count, by_suffix in hits:
        confidence = (
            Confidence.HIGH if count >= 1 else Confidence.MEDIUM
        )
        # Build a `bib_paths` list of the actual .bib globs.
        glob_path = f"{path}/*.bib"
        summary = f"{count} .bib at ~/{path.relative_to(env.home_dir)}"
        if len(summary) > 80:
            summary = summary[:77] + "..."
        candidates.append(Candidate(
            params={"bib_paths": [glob_path]},
            path=str(path),
            confidence=confidence,
            evidence=f"{count} .bib file(s)",
            summary=summary,
            extra={"file_count": count, "extensions": by_suffix},
        ))
    # Zotero candidates rank first: a Zotero library is the
    # highest-signal reference corpus (add-academic-corpora-sources).
    return _discover_zotero(env) + candidates


def _discover_apple_calendar_local(env: DiscoveryEnv) -> list[Candidate]:
    if not env.platform_is("darwin"):
        return []
    # The macOS Calendar.app's DB lives under
    # ~/Library/Calendars/. Just check the dir exists; no
    # operator input required.
    cal_dir = env.home_dir / "Library" / "Calendars"
    if not env.dir_exists(cal_dir):
        return []
    return [Candidate(
        params={},
        path=str(cal_dir),
        confidence=Confidence.HIGH,
        evidence="macOS Calendar.app DB present",
        summary="Local macOS Calendar (no setup needed)",
    )]


def _csv_env(env: DiscoveryEnv, *names: str) -> set[str]:
    out: set[str] = set()
    for name in names:
        raw = env.env_var(name)
        for item in raw.replace(";", ",").split(","):
            normalized = item.strip().lower().replace("_", "-")
            if normalized:
                out.add(normalized)
    return out


def _host_connector_route(
    *,
    source_label: str,
    provider: str = "host",
    allowlist_fields: list[str],
    operator_action: str,
) -> ConnectorRoute:
    return ConnectorRoute(
        source=source_label,
        route_id=f"{source_label}:host_connector",
        route_kind=ConnectorRouteKind.HOST_CONNECTOR,
        provider=provider,
        label=f"{source_label} host connector",
        auth_owner="host",
        trust_tier=ConnectorTrustTier.HOST_MAINTAINED,
        data_path=(
            "agent host connector -> connector_ingest_batch "
            "normalized records"
        ),
        allowed_tools=["bounded_fetch", "connector_ingest_batch"],
        allowlist_fields=allowlist_fields,
        estimated_setup_effort="low",
        privacy_summary=(
            "Provider tokens stay in the host app; OKG receives "
            "only approved normalized records."
        ),
        requires_operator_action=operator_action,
        token_custody="host_app",
        refresh_model="host_managed",
        audit_surface=(
            "OKG logs route id, allowlists, batch counts, and "
            "record provenance; host owns connector audit logs."
        ),
        status="available",
        # Vetting facts (harden-connector-route-vetting): the host
        # app owns token storage + transport; the route itself is
        # in-process tool calls, no exposed HTTP listener.
        encryption_at_rest="host_managed",
        transport_auth="local_ipc",
        upstream_maintenance="active",
        security_policy="present",
    )


def _provider_mcp_route(
    *,
    source_label: str,
    provider: str,
    allowed_tools: list[str],
    allowlist_fields: list[str],
    operator_action: str,
) -> ConnectorRoute:
    return ConnectorRoute(
        source=source_label,
        route_id=f"{source_label}:provider_mcp:{provider}",
        route_kind=ConnectorRouteKind.PROVIDER_MCP,
        provider=provider,
        label=f"{provider} MCP route for {source_label}",
        auth_owner="provider",
        trust_tier=ConnectorTrustTier.PROVIDER_MAINTAINED,
        data_path="provider MCP -> McpComposedSource",
        allowed_tools=allowed_tools,
        allowlist_fields=allowlist_fields,
        estimated_setup_effort="medium",
        privacy_summary=(
            "Provider owns auth; OKG stores read-only route metadata "
            "and emitted facts."
        ),
        requires_operator_action=operator_action,
        token_custody="provider",
        refresh_model="provider_managed",
        audit_surface="OKG can log MCP calls, tools, counts, and errors.",
        status="available",
        # Vetting facts: provider-maintained MCP servers own auth
        # and publish a security policy.
        encryption_at_rest="provider_managed",
        transport_auth="oauth",
        upstream_maintenance="active",
        security_policy="present",
    )


def _gateway_mcp_route(
    *,
    source_label: str,
    provider: str,
    allowlist_fields: list[str],
) -> ConnectorRoute:
    return ConnectorRoute(
        source=source_label,
        route_id=f"{source_label}:gateway_mcp:{provider}",
        route_kind=ConnectorRouteKind.GATEWAY_MCP,
        provider=provider,
        label=f"{provider} gateway route for {source_label}",
        auth_owner="local_user",
        trust_tier=ConnectorTrustTier.LOCAL_USER,
        data_path=(
            "gateway MCP proxy search/describe/invoke -> "
            "connector_ingest_batch"
        ),
        allowed_tools=[
            "search_tools", "describe_tool", "invoke_tool", "route_status",
        ],
        allowlist_fields=allowlist_fields,
        estimated_setup_effort="medium",
        privacy_summary=(
            "Gateway owns connected-app tokens; OKG imports only "
            "allowlisted read-only records."
        ),
        requires_operator_action=(
            "approve platform and conversation allowlists"
        ),
        token_custody="file",
        refresh_model="local_refresh",
        audit_surface=(
            "OKG logs proxy tool ids, batch counts, and route errors."
        ),
        status="available",
        # Vetting facts: gateway hosts (Hermes/OpenClaw-style) keep
        # tokens in permission-guarded files (the 2026-06 audit's
        # finding), reached over local loopback transports.
        encryption_at_rest="file_permissions",
        transport_auth="local_loopback",
        upstream_maintenance="active",
        security_policy="present",
    )


def _discover_connector_source(
    source_label: str,
    *,
    host_names: tuple[str, ...],
    provider_mcp_names: tuple[str, ...] = (),
    gateway_names: tuple[str, ...] = (),
    allowlist_fields: list[str],
    operator_action: str,
    provider_allowed_tools: list[str] | None = None,
) -> Callable[[DiscoveryEnv], list[Candidate]]:
    """Report credentialed source route readiness.

    This deliberately does not look for provider client credentials or
    return `okg add ...` fallback instructions. If no safe route is
    visible, setup can still show the source as `needs_connection`.
    """
    def _discover(env: DiscoveryEnv) -> list[Candidate]:
        available_hosts = _csv_env(
            env, "OKG_HOST_CONNECTORS", "OKG_AVAILABLE_HOST_CONNECTORS",
        )
        provider_mcps = _csv_env(env, "OKG_PROVIDER_MCP_SOURCES")
        gateway_mcps = _csv_env(
            env, "OKG_GATEWAY_MCP_SOURCES", "OKG_MESSAGING_GATEWAYS",
        )
        routes: list[ConnectorRoute] = []
        if any(name in available_hosts for name in host_names):
            routes.append(_host_connector_route(
                source_label=source_label,
                allowlist_fields=allowlist_fields,
                operator_action=operator_action,
            ))
        if provider_mcp_names and any(
            name in provider_mcps for name in provider_mcp_names
        ):
            routes.append(_provider_mcp_route(
                source_label=source_label,
                provider=provider_mcp_names[0],
                allowed_tools=provider_allowed_tools or ["read"],
                allowlist_fields=allowlist_fields,
                operator_action=operator_action,
            ))
        if gateway_names and any(
            name in gateway_mcps for name in gateway_names
        ):
            routes.append(_gateway_mcp_route(
                source_label=source_label,
                provider=gateway_names[0],
                allowlist_fields=allowlist_fields,
            ))
        state = "needs_allowlist" if routes else "needs_connection"
        summary = f"{source_label}: connector route available" if routes else (
            f"{source_label}: connect app in host/provider"
        )
        return [Candidate(
            params={},
            path="",
            confidence=Confidence.NA,
            evidence=(
                f"{len(routes)} safe connector route(s) discovered"
                if routes else "no host/provider/gateway route discovered"
            ),
            summary=summary,
            extra={
                "setup_state": state,
                "credentialed_connector": True,
                "allowlist_fields": allowlist_fields,
            },
            connector_routes=routes,
        )]
    return _discover


def _discover_papers_dir_dependent(
    source_id: str, source_label: str,
) -> Callable[[DiscoveryEnv], list[Candidate]]:
    """Sources whose params template entirely from
    `${papers_dir}` (author_aliases, concept_aliases,
    external_repos, talks). They have nothing for the
    operator to provide — flip on iff papers_dir is set."""
    del source_id  # documentation-only

    def _discover(env: DiscoveryEnv) -> list[Candidate]:
        papers_dir = env.read_install_answer("papers_dir")
        if not papers_dir:
            return []
        return [Candidate(
            params={},
            path=str(papers_dir),
            confidence=Confidence.HIGH,
            evidence=f"templates from papers_dir={papers_dir}",
            summary=f"{source_label}: flip on (uses papers_dir)",
        )]
    return _discover


def _discover_mit_course_catalog(env: DiscoveryEnv) -> list[Candidate]:
    pi_name = _full_pi_name(env)
    if not pi_name:
        return []
    return [Candidate(
        params={"pi_name": pi_name},
        path="",
        confidence=Confidence.HIGH,
        evidence=f"pi_name={pi_name} + institution=MIT (source default)",
        summary=f"MIT course catalog for {pi_name}",
    )]


def _looks_like_full_person_name(value: object) -> bool:
    raw = str(value or "").strip()
    tokens = [
        tok.strip(".,")
        for tok in raw.split()
        if any(ch.isalpha() for ch in tok)
    ]
    return len(tokens) >= 2 and len(tokens[-1]) >= 2


def _full_pi_name(env: DiscoveryEnv) -> str:
    """Return `pi_name` only when it is precise enough for public lookup."""
    value = env.read_install_answer("pi_name")
    if value and _looks_like_full_person_name(value):
        return str(value)
    return ""


def _identity_values(env: DiscoveryEnv) -> dict[str, str]:
    """Strong public-identity values captured at install or setup."""
    return {
        "orcid": str(env.read_install_answer("orcid") or ""),
        "dblp_pid": str(env.read_install_answer("dblp_pid") or ""),
        "pi_name": _full_pi_name(env),
    }


def _discover_dblp_by_orcid(env: DiscoveryEnv) -> list[Candidate]:
    values = _identity_values(env)
    params: dict[str, str] = {}
    if values["orcid"]:
        params["orcid"] = values["orcid"]
    if values["dblp_pid"]:
        params["dblp_pid"] = values["dblp_pid"]
    if values["pi_name"]:
        params["author_name"] = values["pi_name"]
    if not params:
        return []
    hint = params.get("orcid") or params.get("dblp_pid") or params["author_name"]
    return [Candidate(
        params=params,
        path="",
        confidence=Confidence.HIGH,
        evidence="publication identity hint present",
        summary="Publications from DBLP identity lookup",
        extra={"identity_hint": hint},
    )]


def _discover_orcid_publications(env: DiscoveryEnv) -> list[Candidate]:
    """orcid_publications keys off the single `orcid` install answer.

    Unlike `dblp_by_orcid` (which resolves a DBLP author record from
    the ORCID), this source reads ORCID's own works + employments
    API, so it is the one that emits the PI's institutional
    `affiliated_with` edges."""
    orcid = str(env.read_install_answer("orcid") or "").strip()
    if not orcid:
        return []
    return [Candidate(
        params={"orcid": orcid},
        path="",
        confidence=Confidence.HIGH,
        evidence=f"orcid={orcid} (public ORCID works + employments)",
        summary="Publications + affiliations from ORCID",
        extra={"identity_hint": orcid},
    )]


def _discover_public_grant_source(
    source_label: str,
) -> Callable[[DiscoveryEnv], list[Candidate]]:
    def _discover(env: DiscoveryEnv) -> list[Candidate]:
        values = _identity_values(env)
        params: dict[str, str] = {}
        if source_label.startswith("NSF"):
            if values["pi_name"]:
                params["pi_name"] = values["pi_name"]
        else:
            if values["orcid"]:
                params["orcid"] = values["orcid"]
            if values["pi_name"]:
                params["pi_name"] = values["pi_name"]
        if not params:
            return []
        hint = params.get("orcid") or params.get("pi_name") or ""
        return [Candidate(
            params=params,
            path="",
            confidence=Confidence.HIGH,
            evidence=f"identity hint present for {source_label}",
            summary=f"{source_label} public award lookup",
            extra={"identity_hint": hint},
        )]
    return _discover


def _install_git_repos(env: DiscoveryEnv) -> list[Path]:
    raw = env.read_install_answer("git_repos")
    if not raw:
        return []
    if isinstance(raw, str):
        values = [p.strip() for p in raw.split(",") if p.strip()]
    elif isinstance(raw, list):
        values = [str(p).strip() for p in raw if str(p).strip()]
    else:
        values = [str(raw).strip()]
    paths: list[Path] = []
    for value in values:
        path = Path(value).expanduser()
        if env.dir_exists(path):
            paths.append(path)
    return paths


def _discover_explicit_git_repo_source(
    source_label: str,
) -> Callable[[DiscoveryEnv], list[Candidate]]:
    """Discover repo sources only from an explicit install answer.

    Default install no longer derives `git_repos` from the framework
    checkout, so any value here is operator-supplied project intent.
    """
    def _discover(env: DiscoveryEnv) -> list[Candidate]:
        candidates: list[Candidate] = []
        for repo in _install_git_repos(env):
            if not env.dir_exists(repo / ".git"):
                continue
            try:
                rel = repo.relative_to(env.home_dir)
                display = f"~/{rel}"
            except ValueError:
                display = str(repo)
            evidence = f"explicit git_repos install answer: {repo}"
            extra: dict = {"explicit_project_path": str(repo)}
            # Overleaf projects are git remotes
            # (add-academic-corpora-sources): an Overleaf remote
            # routes through this existing git source; syncing it
            # needs the operator's per-user Overleaf git token.
            if _has_overleaf_remote(env, repo):
                evidence += " (Overleaf project remote)"
                extra["overleaf_project"] = True
                extra["setup_state"] = "needs_token"
                extra["token_hint"] = (
                    "Overleaf git authentication token "
                    "(Account Settings -> Git integration)"
                )
            candidates.append(Candidate(
                params={},
                path=str(repo),
                confidence=Confidence.HIGH,
                evidence=evidence,
                summary=f"{source_label} for {display}",
                extra=extra,
            ))
        return candidates
    return _discover


def _has_overleaf_remote(env: DiscoveryEnv, repo: Path) -> bool:
    git_config = repo / ".git" / "config"
    if not env.file_exists(git_config):
        return False
    try:
        text = git_config.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return any(
        _is_overleaf_remote_url(line.partition("=")[2].strip())
        for line in text.splitlines()
        if line.strip().startswith("url")
    )


def _is_overleaf_remote_url(value: str) -> bool:
    if not value:
        return False
    if "://" in value:
        try:
            return urlsplit(value).hostname == "git.overleaf.com"
        except ValueError:
            return False
    if "@" in value:
        value = value.split("@", 1)[1]
    host = value.split(":", 1)[0].split("/", 1)[0]
    return host == "git.overleaf.com"


def _discover_explicit_openspec(env: DiscoveryEnv) -> list[Candidate]:
    candidates: list[Candidate] = []
    for repo in _install_git_repos(env):
        openspec_root = repo / "openspec"
        if not env.dir_exists(openspec_root):
            continue
        try:
            rel = openspec_root.relative_to(env.home_dir)
            display = f"~/{rel}"
        except ValueError:
            display = str(openspec_root)
        candidates.append(Candidate(
            params={},
            path=str(openspec_root),
            confidence=Confidence.HIGH,
            evidence=f"explicit git_repos install answer: {repo}",
            summary=f"OpenSpec proposals at {display}",
            extra={"explicit_project_path": str(repo)},
        ))
    return candidates


def _discover_external_url_source(
    source_label: str,
) -> Callable[[DiscoveryEnv], list[Candidate]]:
    """For sources that need an operator-provided URL
    (group-page, lab-talks, cal-ics). We can't discover URLs
    from the local filesystem; report as `needs_operator`."""
    def _discover(env: DiscoveryEnv) -> list[Candidate]:
        return [Candidate(
            params={},
            path="",
            confidence=Confidence.NA,
            evidence=f"needs operator-provided URL ({source_label})",
            summary=f"{source_label}: URL-based — wire later if needed",
            extra={"needs_operator_input": True},
        )]
    return _discover


# ---------- registry ----------


# Maps registry source_id → discoverer function. Source IDs
# absent from this dict get no discovery (the MCP tool reports
# them as "no automatic discovery").
DISCOVERERS: dict[str, Callable[[DiscoveryEnv], list[Candidate]]] = {
    "dblp_by_orcid": _discover_dblp_by_orcid,
    "orcid_publications": _discover_orcid_publications,
    "nsf_award_search": _discover_public_grant_source("NSF awards"),
    "doe_osti": _discover_public_grant_source("DOE OSTI"),
    "notes_dir": _discover_notes_dir,
    "paper_pdf_inbox": _discover_paper_pdf_inbox,
    "cv_ingest_llm": _discover_cv_ingest_llm,
    "bib_references": _discover_bib_references,
    "apple_calendar_local": _discover_apple_calendar_local,
    "gmail_api": _discover_connector_source(
        "gmail",
        host_names=("gmail", "google-mail"),
        allowlist_fields=["labels", "query", "time_range"],
        operator_action="connect Gmail in the host app and approve labels",
    ),
    "google_calendar_api": _discover_connector_source(
        "google-cal",
        host_names=("google-cal", "google-calendar", "gcal"),
        allowlist_fields=["calendars", "query", "time_range"],
        operator_action=(
            "connect Google Calendar in the host app and approve calendars"
        ),
    ),
    "outlook_mail_api": _discover_connector_source(
        "outlook-mail",
        host_names=("outlook", "ms-outlook", "microsoft-365", "m365"),
        allowlist_fields=["folders", "query", "time_range"],
        operator_action=(
            "consent the tenant Entra app and approve mail folders"
        ),
    ),
    "outlook_calendar_api": _discover_connector_source(
        "outlook-cal",
        host_names=("outlook-cal", "outlook-calendar", "microsoft-365", "m365"),
        allowlist_fields=["calendars", "query", "time_range"],
        operator_action=(
            "consent the tenant Entra app and approve calendars"
        ),
    ),
    "slack_api": _discover_connector_source(
        "slack",
        host_names=("slack",),
        provider_mcp_names=("slack",),
        gateway_names=("hermes", "openclaw"),
        allowlist_fields=["workspaces", "channels", "time_range"],
        operator_action=(
            "connect Slack in the host/provider route and approve channels"
        ),
        provider_allowed_tools=[
            "search", "fetch", "conversations.history",
            "conversations.info", "users.info",
        ],
    ),
    "author_aliases": _discover_papers_dir_dependent(
        "author_aliases", "author_aliases",
    ),
    "concept_aliases": _discover_papers_dir_dependent(
        "concept_aliases", "concept_aliases",
    ),
    "external_repos": _discover_papers_dir_dependent(
        "external_repos", "external_repos",
    ),
    "talks": _discover_papers_dir_dependent("talks", "talks"),
    "mit_course_catalog": _discover_mit_course_catalog,
    "git_files": _discover_explicit_git_repo_source("Git files"),
    "git_history": _discover_explicit_git_repo_source("Git history"),
    "code_structure": _discover_explicit_git_repo_source("Code structure"),
    "agent_sessions": _discover_explicit_git_repo_source("Agent sessions"),
    "openspec": _discover_explicit_openspec,
    "calendar_ics": _discover_external_url_source("cal-ics"),
    "canvas_api": _discover_external_url_source("canvas"),
    "group_webpage_scrape": _discover_external_url_source("group-page"),
    "lab_webpage_talks": _discover_external_url_source("lab-talks"),
}


def discover_for_source_id(
    source_id: str, env: DiscoveryEnv,
) -> list[Candidate]:
    """Dispatch to the per-source discoverer. Returns [] for
    sources without a discoverer entry."""
    fn = DISCOVERERS.get(source_id)
    if fn is None:
        return []
    return fn(env)
