"""External-live authority graph source.

The source turns deployment-declared MCP live tools into low-cardinality graph
facts. It records capability and authority metadata only; live call payloads
remain outside the generation-pinned graph.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml
from okg.substrate import fastyaml

from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceHealth,
    SourcePreflightResult,
    SourceRun,
)


_FULL_SCOPE_MODES = {"reconcile", "scope_complete", "release_new"}
_MAX_QUERY_EXAMPLES = 5
_MAX_QUERY_EXAMPLE_CHARS = 220
_MAX_SEARCH_TERMS = 16
_MAX_SEARCH_TERM_CHARS = 80
_MAX_FRESHNESS_NOTE_CHARS = 240


@dataclass
class _LoadedConfig:
    deployment_name: str
    agent_tools: dict[str, dict[str, Any]]
    authorities: dict[str, dict[str, Any]]
    services: dict[str, dict[str, Any]]
    endpoints: dict[str, dict[str, Any]]
    profiles: list[dict[str, Any]]
    content_hash: str


def validate_external_live_authority_profiles(
    deployment_yaml: str | Path,
    profiles_path: str | Path | None = None,
) -> dict[str, Any]:
    """Return deployment-authoring diagnostics for live authority profiles.

    Warnings are authoring gaps that leave a live tool non-applicable in
    benchmark traces. Errors are config states that would make the graph source
    fail closed.
    """
    deployment_path = Path(deployment_yaml).expanduser()
    profile_path = (
        Path(profiles_path).expanduser()
        if profiles_path is not None
        else deployment_path.parent / "live_authority_profiles.yaml"
    )
    report: dict[str, Any] = {
        "ok": True,
        "status": "OK",
        "deployment": None,
        "deployment_yaml": str(deployment_path),
        "profiles_path": str(profile_path),
        "external_live_tools": [],
        "profile_count": 0,
        "profiled_tools": [],
        "unprofiled_external_live_tools": [],
        "stale_profile_tools": [],
        "non_external_live_profile_tools": [],
        "unknown_authorities": [],
        "unknown_services": [],
        "unknown_endpoints": [],
        "errors": [],
        "warnings": [],
    }

    try:
        manifest = _load_yaml(deployment_path)
    except Exception as exc:
        _add_issue(
            report,
            "errors",
            "deployment_yaml_invalid",
            f"failed to load deployment.yaml: {exc}",
        )
        return _finalize_diagnostics(report)

    deployment_name = manifest.get("name")
    if isinstance(deployment_name, str) and deployment_name.strip():
        report["deployment"] = deployment_name.strip()

    agent_tools = manifest.get("agent_tools") or {}
    if not isinstance(agent_tools, dict):
        _add_issue(
            report,
            "errors",
            "agent_tools_not_mapping",
            "deployment.yaml agent_tools must be a mapping",
        )
        return _finalize_diagnostics(report)

    external_live_tools = {
        str(name): dict(raw)
        for name, raw in agent_tools.items()
        if isinstance(name, str)
        and isinstance(raw, dict)
        and raw.get("boundary") == "external_live"
    }
    report["external_live_tools"] = sorted(external_live_tools)

    if not profile_path.is_file():
        if external_live_tools:
            report["unprofiled_external_live_tools"] = sorted(external_live_tools)
            _add_issue(
                report,
                "warnings",
                "live_authority_profiles_missing",
                "external_live tools are declared but live_authority_profiles.yaml "
                "is missing",
                tools=sorted(external_live_tools),
            )
        return _finalize_diagnostics(report)

    try:
        profiles_doc = _load_yaml(profile_path)
    except Exception as exc:
        _add_issue(
            report,
            "errors",
            "profiles_yaml_invalid",
            f"failed to load live authority profiles: {exc}",
        )
        return _finalize_diagnostics(report)

    authorities = _diagnostic_keyed_records(
        profiles_doc.get("authorities") or [],
        "authorities",
        report,
    )
    services = _diagnostic_keyed_records(
        profiles_doc.get("services") or [],
        "services",
        report,
    )
    endpoints = _diagnostic_keyed_records(
        profiles_doc.get("endpoints") or [],
        "endpoints",
        report,
    )
    raw_profiles = profiles_doc.get("profiles") or []
    if not isinstance(raw_profiles, list):
        _add_issue(
            report,
            "errors",
            "profiles_not_list",
            "live authority profiles must be a list",
        )
        return _finalize_diagnostics(report)

    seen_profile_ids: set[str] = set()
    profiled_tools: set[str] = set()
    stale_tools: set[str] = set()
    non_live_tools: set[str] = set()
    unknown_authorities: set[str] = set()
    unknown_services: set[str] = set()
    unknown_endpoints: set[str] = set()

    for index, raw in enumerate(raw_profiles):
        if not isinstance(raw, dict):
            _add_issue(
                report,
                "errors",
                "profile_not_mapping",
                "each live authority profile must be a mapping",
                index=index,
            )
            continue
        profile_id = raw.get("id")
        context = (
            profile_id.strip()
            if isinstance(profile_id, str) and profile_id.strip()
            else f"profile[{index}]"
        )
        if not isinstance(profile_id, str) or not profile_id.strip():
            _add_issue(
                report,
                "errors",
                "profile_id_missing",
                "profile is missing required string 'id'",
                index=index,
            )
        elif profile_id in seen_profile_ids:
            _add_issue(
                report,
                "errors",
                "duplicate_profile_id",
                f"duplicate live authority profile id: {profile_id}",
                profile_id=profile_id,
            )
        else:
            seen_profile_ids.add(profile_id)

        tool_name = raw.get("tool")
        if not isinstance(tool_name, str) or not tool_name.strip():
            _add_issue(
                report,
                "errors",
                "profile_tool_missing",
                f"{context}: missing required string 'tool'",
                profile_id=profile_id,
            )
        else:
            tool_name = tool_name.strip()
            tool = agent_tools.get(tool_name)
            if not isinstance(tool, dict):
                stale_tools.add(tool_name)
                _add_issue(
                    report,
                    "errors",
                    "profile_references_unknown_tool",
                    f"profile {context!r} references unknown tool {tool_name!r}",
                    profile_id=profile_id,
                    tool=tool_name,
                )
            elif tool.get("boundary") != "external_live":
                non_live_tools.add(tool_name)
                _add_issue(
                    report,
                    "errors",
                    "profile_references_non_external_live_tool",
                    f"profile {context!r} references non-external_live "
                    f"tool {tool_name!r}",
                    profile_id=profile_id,
                    tool=tool_name,
                )
            else:
                profiled_tools.add(tool_name)

        authority_id = raw.get("authority")
        if not isinstance(authority_id, str) or not authority_id.strip():
            _add_issue(
                report,
                "errors",
                "profile_authority_missing",
                f"{context}: missing required string 'authority'",
                profile_id=profile_id,
            )
        elif authority_id not in authorities:
            unknown_authorities.add(authority_id)
            _add_issue(
                report,
                "errors",
                "profile_references_unknown_authority",
                f"profile {context!r} references unknown authority "
                f"{authority_id!r}",
                profile_id=profile_id,
                authority=authority_id,
            )

        for field, known, collector, code in (
            ("service_ids", services, unknown_services, "unknown_service"),
            ("endpoint_ids", endpoints, unknown_endpoints, "unknown_endpoint"),
        ):
            try:
                refs = _string_list(raw.get(field))
            except ValueError as exc:
                _add_issue(
                    report,
                    "errors",
                    f"profile_{field}_invalid",
                    f"{context}: {field} invalid: {exc}",
                    profile_id=profile_id,
                )
                continue
            for ref in refs:
                if ref not in known:
                    collector.add(ref)
                    _add_issue(
                        report,
                        "errors",
                        f"profile_references_{code}",
                        f"profile {context!r} references unknown "
                        f"{field[:-1]} {ref!r}",
                        profile_id=profile_id,
                        ref=ref,
                    )

        for field, max_items, max_chars in (
            ("query_examples", _MAX_QUERY_EXAMPLES, _MAX_QUERY_EXAMPLE_CHARS),
            ("search_terms", _MAX_SEARCH_TERMS, _MAX_SEARCH_TERM_CHARS),
        ):
            try:
                _bounded_string_list(
                    raw.get(field),
                    field=field,
                    context=context,
                    max_items=max_items,
                    max_chars=max_chars,
                )
            except ValueError as exc:
                _add_issue(
                    report,
                    "errors",
                    f"profile_{field}_invalid",
                    str(exc),
                    profile_id=profile_id,
                )
        try:
            _bounded_string(
                raw.get("freshness_note"),
                field="freshness_note",
                context=context,
                max_chars=_MAX_FRESHNESS_NOTE_CHARS,
            )
        except ValueError as exc:
            _add_issue(
                report,
                "errors",
                "profile_freshness_note_invalid",
                str(exc),
                profile_id=profile_id,
            )

    unprofiled = sorted(set(external_live_tools) - profiled_tools)
    if unprofiled:
        _add_issue(
            report,
            "warnings",
            "external_live_tool_without_authority_profile",
            "external_live tools have no ToolAuthorityProfile; they are not "
            "graph-applicable for benchmark live calls",
            tools=unprofiled,
        )

    report.update(
        {
            "profile_count": len(raw_profiles),
            "profiled_tools": sorted(profiled_tools),
            "unprofiled_external_live_tools": unprofiled,
            "stale_profile_tools": sorted(stale_tools),
            "non_external_live_profile_tools": sorted(non_live_tools),
            "unknown_authorities": sorted(unknown_authorities),
            "unknown_services": sorted(unknown_services),
            "unknown_endpoints": sorted(unknown_endpoints),
        }
    )
    return _finalize_diagnostics(report)


def scaffold_external_live_authority_profile(
    deployment_yaml: str | Path,
    *,
    tool_name: str,
    profiles_path: str | Path | None = None,
    profile_id: str | None = None,
    authority_id: str | None = None,
    service_ids: Iterable[str] | None = None,
    intent: str | None = None,
    source_family: str | None = None,
    result_kind: str = "search_results",
    default_time_window: str = "now-24h",
) -> dict[str, Any]:
    """Build a YAML stub for an external-live tool authority profile."""
    deployment_path = Path(deployment_yaml).expanduser()
    profile_path = (
        Path(profiles_path).expanduser()
        if profiles_path is not None
        else deployment_path.parent / "live_authority_profiles.yaml"
    )
    manifest = _load_yaml(deployment_path)
    deployment_name = _required_str(
        manifest,
        "name",
        context=str(deployment_path),
    )
    agent_tools = manifest.get("agent_tools") or {}
    if not isinstance(agent_tools, dict):
        raise ValueError("deployment.yaml agent_tools must be a mapping")
    existing_tool = agent_tools.get(tool_name)
    existing_agent_tool = isinstance(existing_tool, dict)
    if existing_agent_tool and existing_tool.get("boundary") != "external_live":
        raise ValueError(f"tool {tool_name!r} is not boundary: external_live")

    known_authorities: list[str] = []
    known_services: list[str] = []
    existing_profile_ids: list[str] = []
    if profile_path.is_file():
        profiles_doc = _load_yaml(profile_path)
        known_authorities = sorted(
            _diagnostic_ids(profiles_doc.get("authorities") or [])
        )
        known_services = sorted(
            _diagnostic_ids(profiles_doc.get("services") or [])
        )
        for raw_profile in profiles_doc.get("profiles") or []:
            if not isinstance(raw_profile, dict):
                continue
            if raw_profile.get("tool") != tool_name:
                continue
            raw_profile_id = raw_profile.get("id")
            if isinstance(raw_profile_id, str) and raw_profile_id.strip():
                existing_profile_ids.append(raw_profile_id.strip())

    selected_authority = (
        authority_id
        or (known_authorities[0] if len(known_authorities) == 1 else None)
        or "TODO_AUTHORITY_ID"
    )
    selected_services = list(service_ids or [])
    if not selected_services and len(known_services) == 1:
        selected_services = [known_services[0]]
    if not selected_services:
        selected_services = ["TODO_SERVICE_ID"]

    selected_profile_id = profile_id or _default_profile_id(
        deployment_name,
        tool_name,
    )
    selected_intent = intent or "TODO_CONCRETE_LIVE_TOOL_INTENT"
    selected_source_family = source_family or selected_authority

    if existing_profile_ids and profile_id is None:
        return {
            "ok": True,
            "deployment": deployment_name,
            "deployment_yaml": str(deployment_path),
            "profiles_path": str(profile_path),
            "tool": tool_name,
            "existing_agent_tool": existing_agent_tool,
            "profile_already_declared": True,
            "existing_profile_ids": sorted(existing_profile_ids),
            "known_authorities": known_authorities,
            "known_services": known_services,
            "agent_tool_yaml": "",
            "profile_yaml": "",
            "message": (
                "tool already has authority profile(s); pass --profile-id "
                "to scaffold an additional profile"
            ),
        }

    profile_doc = {
        "profiles": [
            {
                "id": selected_profile_id,
                "name": _humanize_profile_name(selected_profile_id),
                "tool": tool_name,
                "authority": selected_authority,
                "service_ids": selected_services,
                "endpoint_ids": [],
                "intent": selected_intent,
                "source_family": selected_source_family,
                "subject_subtypes": ["TODO_SUBTYPE"],
                "result_kind": result_kind,
                "default_time_window": default_time_window,
                "freshness_note": (
                    "Live external read observed at tool-call time; not "
                    "generation-pinned OKG evidence."
                ),
                "query_examples": ["TODO_QUERY_EXAMPLE"],
                "search_terms": ["TODO_SEARCH_TERM"],
            }
        ]
    }
    agent_tool_doc = None
    if not existing_agent_tool:
        agent_tool_doc = {
            "agent_tools": {
                tool_name: {
                    "module": "TODO_MODULE",
                    "function": "TODO_FUNCTION",
                    "boundary": "external_live",
                    "requires_env": ["TODO_ENV_VAR_NAME"],
                    "optional": True,
                    "description": (
                        "TODO: Describe this live external tool. Results are "
                        "not generation-pinned OKG facts; each response must "
                        "include observed_at, source, query, time_window, and "
                        "bounded evidence."
                    ),
                }
            }
        }

    return {
        "ok": True,
        "deployment": deployment_name,
        "deployment_yaml": str(deployment_path),
        "profiles_path": str(profile_path),
        "tool": tool_name,
        "existing_agent_tool": existing_agent_tool,
        "profile_already_declared": False,
        "existing_profile_ids": sorted(existing_profile_ids),
        "known_authorities": known_authorities,
        "known_services": known_services,
        "agent_tool_yaml": (
            ""
            if agent_tool_doc is None
            else yaml.safe_dump(agent_tool_doc, sort_keys=False)
        ),
        "profile_yaml": yaml.safe_dump(profile_doc, sort_keys=False),
    }


class ExternalLiveAuthoritySource:
    """Emit graph-native authority metadata for external-live tools."""

    name = "external_live_authority"
    profile = "reference_catalog"
    change_probe_kind = "content_hash"

    def __init__(
        self,
        *,
        deployment_yaml: str | Path,
        profiles_path: str | Path,
    ) -> None:
        self.deployment_yaml = Path(deployment_yaml).expanduser()
        self.profiles_path = Path(profiles_path).expanduser()
        self._config: _LoadedConfig | None = None
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        return ContentHashProbe(
            content_items=lambda: [
                (str(path), path)
                for path in (self.deployment_yaml, self.profiles_path)
                if path.is_file()
            ],
            config={
                "deployment_yaml": str(self.deployment_yaml),
                "profiles_path": str(self.profiles_path),
            },
            emit_targets=ExternalLiveAuthoritySource,
        )

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        missing = [
            str(path)
            for path in (self.deployment_yaml, self.profiles_path)
            if not path.is_file()
        ]
        if missing:
            return SourcePreflightResult(
                source_name=self.name,
                status="cache_missing",
                mode="config",
                required=True,
                cache_path=", ".join(missing),
                reason="authority graph configuration file missing",
                checked_at=_checked_at(),
            )
        config = self._load_config()
        return SourcePreflightResult(
            source_name=self.name,
            status="ok",
            mode="config",
            required=True,
            record_count=(
                len(config.authorities)
                + len(config.agent_tools)
                + len(config.profiles)
            ),
            content_hash=config.content_hash,
            reason="external-live authority config present",
            checked_at=_checked_at(),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, Any] | None = None,
        sync_scope: Mapping[str, Any] | None = None,
    ) -> SourceRun:
        del cursor, since_progress, sync_scope
        config = self._load_config()
        revision = {
            "run_id": run_id,
            "content_hash": config.content_hash,
            "profile_count": len(config.profiles),
            "tool_count": len(config.agent_tools),
        }
        facts = list(self._facts(config, revision=revision))
        completed_scope = mode in _FULL_SCOPE_MODES
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor={"content_hash": config.content_hash},
            run_mode=mode,
            record_set=(
                _record_set(facts, revision=revision)
                if completed_scope else None
            ),
            health=SourceHealth(
                status="ok",
                mode="config",
                record_count=len(facts),
                content_hash=config.content_hash,
                checked_at=_checked_at(),
            ),
        )

    def _load_config(self) -> _LoadedConfig:
        if self._config is not None:
            return self._config
        if not self.deployment_yaml.is_file():
            raise FileNotFoundError(f"deployment_yaml not found: {self.deployment_yaml}")
        if not self.profiles_path.is_file():
            raise FileNotFoundError(f"profiles_path not found: {self.profiles_path}")
        manifest = _load_yaml(self.deployment_yaml)
        profiles_doc = _load_yaml(self.profiles_path)
        deployment_name = _required_str(
            manifest, "name", context=str(self.deployment_yaml)
        )
        agent_tools = manifest.get("agent_tools") or {}
        if not isinstance(agent_tools, dict):
            raise ValueError("deployment.yaml agent_tools must be a mapping")
        authorities = _keyed_records(
            profiles_doc.get("authorities") or [],
            "authorities",
        )
        services = _keyed_records(
            profiles_doc.get("services") or [],
            "services",
        )
        endpoints = _keyed_records(
            profiles_doc.get("endpoints") or [],
            "endpoints",
        )
        raw_profiles = profiles_doc.get("profiles") or []
        if not isinstance(raw_profiles, list):
            raise ValueError("live authority profiles must be a list")
        profile_ids: set[str] = set()
        profiles: list[dict[str, Any]] = []
        for raw in raw_profiles:
            if not isinstance(raw, dict):
                raise ValueError("each live authority profile must be a mapping")
            profile_id = _required_str(raw, "id", context="profile")
            if profile_id in profile_ids:
                raise ValueError(f"duplicate live authority profile id: {profile_id}")
            profile_ids.add(profile_id)
            tool_name = _required_str(raw, "tool", context=profile_id)
            tool = agent_tools.get(tool_name)
            if not isinstance(tool, dict):
                raise ValueError(
                    f"profile {profile_id!r} references unknown tool {tool_name!r}"
                )
            if tool.get("boundary") != "external_live":
                raise ValueError(
                    f"profile {profile_id!r} references non-external_live "
                    f"tool {tool_name!r}"
                )
            authority_id = _required_str(raw, "authority", context=profile_id)
            if authority_id not in authorities:
                raise ValueError(
                    f"profile {profile_id!r} references unknown authority "
                    f"{authority_id!r}"
                )
            for service_id in _string_list(raw.get("service_ids")):
                if service_id not in services:
                    raise ValueError(
                        f"profile {profile_id!r} references unknown service "
                        f"{service_id!r}"
                    )
            for endpoint_id in _string_list(raw.get("endpoint_ids")):
                if endpoint_id not in endpoints:
                    raise ValueError(
                        f"profile {profile_id!r} references unknown endpoint "
                        f"{endpoint_id!r}"
                    )
            _bounded_string_list(
                raw.get("query_examples"),
                field="query_examples",
                context=profile_id,
                max_items=_MAX_QUERY_EXAMPLES,
                max_chars=_MAX_QUERY_EXAMPLE_CHARS,
            )
            _bounded_string_list(
                raw.get("search_terms"),
                field="search_terms",
                context=profile_id,
                max_items=_MAX_SEARCH_TERMS,
                max_chars=_MAX_SEARCH_TERM_CHARS,
            )
            _bounded_string(
                raw.get("freshness_note"),
                field="freshness_note",
                context=profile_id,
                max_chars=_MAX_FRESHNESS_NOTE_CHARS,
            )
            profiles.append(dict(raw))
        content_hash = _content_hash(self.deployment_yaml, self.profiles_path)
        self._config = _LoadedConfig(
            deployment_name=deployment_name,
            agent_tools={
                name: dict(value)
                for name, value in sorted(agent_tools.items())
                if isinstance(value, dict)
                and value.get("boundary") == "external_live"
            },
            authorities=authorities,
            services=services,
            endpoints=endpoints,
            profiles=profiles,
            content_hash=content_hash,
        )
        return self._config

    def _facts(
        self,
        config: _LoadedConfig,
        *,
        revision: dict[str, Any],
    ) -> Iterable[NodeFact | EdgeFact]:
        for authority_id, authority in sorted(config.authorities.items()):
            attrs = {
                "authority_id": authority_id,
                "name": _required_str(authority, "name", context=authority_id),
                "boundary": authority.get("boundary") or "external_live",
                "source_family": authority.get("source_family") or authority_id,
                "description": str(authority.get("description") or ""),
            }
            attrs["text"] = _text(
                attrs["name"],
                attrs["boundary"],
                attrs["source_family"],
                attrs["description"],
            )
            yield NodeFact(
                node_id=f"external_authority:{authority_id}",
                subtype="external_authority",
                attrs=attrs,
                source_record_id={
                    "deployment": config.deployment_name,
                    "record_type": "external_authority",
                    "record_id": authority_id,
                    "authority_id": authority_id,
                },
                source_revision=revision,
            )

        for service_id, service in sorted(config.services.items()):
            attrs = {
                "service_id": service_id,
                "name": _required_str(service, "name", context=service_id),
                "text": _text(
                    service.get("name"),
                    service_id,
                    service.get("description"),
                ),
            }
            if service.get("description"):
                attrs["description"] = str(service["description"])
            yield NodeFact(
                node_id=service_id,
                subtype="service",
                attrs=attrs,
                source_record_id={
                    "deployment": config.deployment_name,
                    "record_type": "service",
                    "record_id": service_id,
                    "service_id": service_id,
                },
                source_revision=revision,
            )

        for endpoint_id, endpoint in sorted(config.endpoints.items()):
            url = str(endpoint.get("url") or "")
            attrs = {
                "endpoint_id": endpoint_id,
                "name": _required_str(endpoint, "name", context=endpoint_id),
                "url": url,
                "text": _text(endpoint.get("name"), url, endpoint_id),
            }
            yield NodeFact(
                node_id=f"endpoint:{endpoint_id}",
                subtype="endpoint",
                attrs=attrs,
                source_record_id={
                    "deployment": config.deployment_name,
                    "record_type": "endpoint",
                    "record_id": endpoint_id,
                    "endpoint_id": endpoint_id,
                },
                source_revision=revision,
            )

        endpoints_by_tool: dict[str, set[str]] = {}
        authorities_by_tool: dict[str, set[str]] = {}
        for profile in config.profiles:
            tool_name = str(profile["tool"])
            authorities_by_tool.setdefault(tool_name, set()).add(
                str(profile["authority"])
            )
            endpoints_by_tool.setdefault(tool_name, set()).update(
                _string_list(profile.get("endpoint_ids"))
            )

        for tool_name, tool in sorted(config.agent_tools.items()):
            tool_id = f"external_live_tool:{config.deployment_name}:{tool_name}"
            requires_env_names = _string_list(tool.get("requires_env"))
            attrs = {
                "tool_id": tool_id,
                "tool_name": tool_name,
                "deployment": config.deployment_name,
                "boundary": "external_live",
                "requires_env_names": requires_env_names,
                "optional": bool(tool.get("optional", False)),
                "module": str(tool.get("module") or ""),
                "function": str(tool.get("function") or ""),
                "description": str(tool.get("description") or ""),
                "result_contract": (
                    "external_live result with observed_at, source, query, "
                    "time_window, and results or aggregation"
                ),
            }
            attrs["text"] = _text(
                tool_name,
                attrs["boundary"],
                attrs["description"],
                attrs["result_contract"],
                requires_env_names,
            )
            yield NodeFact(
                node_id=tool_id,
                subtype="external_live_tool",
                attrs=attrs,
                source_record_id={
                    "deployment": config.deployment_name,
                    "record_type": "external_live_tool",
                    "record_id": tool_name,
                    "tool_name": tool_name,
                },
                source_revision=revision,
            )
            for authority_id in sorted(authorities_by_tool.get(tool_name, ())):
                yield EdgeFact(
                    src=tool_id,
                    dst=f"external_authority:{authority_id}",
                    edge_type="depends_on",
                    attrs={"authority_id": authority_id},
                    source_record_id={
                        "deployment": config.deployment_name,
                        "record_type": "tool_authority_edge",
                        "tool_name": tool_name,
                        "record_id": f"{tool_name}:{authority_id}",
                        "authority_id": authority_id,
                    },
                    source_revision=revision,
                )
            for endpoint_id in sorted(endpoints_by_tool.get(tool_name, ())):
                yield EdgeFact(
                    src=tool_id,
                    dst=f"endpoint:{endpoint_id}",
                    edge_type="exposes",
                    attrs={"endpoint_id": endpoint_id},
                    source_record_id={
                        "deployment": config.deployment_name,
                        "record_type": "tool_endpoint_edge",
                        "tool_name": tool_name,
                        "record_id": f"{tool_name}:{endpoint_id}",
                        "endpoint_id": endpoint_id,
                    },
                    source_revision=revision,
                )

        for profile in sorted(config.profiles, key=lambda item: str(item["id"])):
            profile_id = str(profile["id"])
            tool_name = str(profile["tool"])
            authority_id = str(profile["authority"])
            profile_node_id = f"tool_authority_profile:{profile_id}"
            subject_subtypes = _string_list(profile.get("subject_subtypes"))
            query_examples = _bounded_string_list(
                profile.get("query_examples"),
                field="query_examples",
                context=profile_id,
                max_items=_MAX_QUERY_EXAMPLES,
                max_chars=_MAX_QUERY_EXAMPLE_CHARS,
            )
            search_terms = _bounded_string_list(
                profile.get("search_terms"),
                field="search_terms",
                context=profile_id,
                max_items=_MAX_SEARCH_TERMS,
                max_chars=_MAX_SEARCH_TERM_CHARS,
            )
            attrs = {
                "profile_id": profile_id,
                "name": _required_str(profile, "name", context=profile_id),
                "tool_name": tool_name,
                "authority_id": authority_id,
                "intent": _required_str(profile, "intent", context=profile_id),
                "source_family": str(profile.get("source_family") or authority_id),
                "subject_subtypes": subject_subtypes,
                "result_kind": str(profile.get("result_kind") or ""),
                "default_time_window": str(profile.get("default_time_window") or ""),
                "freshness_note": str(profile.get("freshness_note") or ""),
                "query_examples": query_examples,
                "search_terms": search_terms,
            }
            attrs["text"] = _text(
                attrs["name"],
                tool_name,
                attrs["intent"],
                attrs["source_family"],
                attrs["result_kind"],
                attrs["default_time_window"],
                attrs["freshness_note"],
                subject_subtypes,
                query_examples,
                search_terms,
            )
            yield NodeFact(
                node_id=profile_node_id,
                subtype="tool_authority_profile",
                attrs=attrs,
                source_record_id={
                    "deployment": config.deployment_name,
                    "record_type": "tool_authority_profile",
                    "record_id": profile_id,
                    "profile_id": profile_id,
                },
                source_revision=revision,
            )
            yield EdgeFact(
                src=f"external_live_tool:{config.deployment_name}:{tool_name}",
                dst=profile_node_id,
                edge_type="references",
                attrs={"profile_id": profile_id},
                source_record_id={
                    "deployment": config.deployment_name,
                    "record_type": "tool_profile_edge",
                    "tool_name": tool_name,
                    "record_id": f"{tool_name}:{profile_id}",
                    "profile_id": profile_id,
                },
                source_revision=revision,
            )
            yield EdgeFact(
                src=profile_node_id,
                dst=f"external_authority:{authority_id}",
                edge_type="references",
                attrs={"authority_id": authority_id},
                source_record_id={
                    "deployment": config.deployment_name,
                    "record_type": "profile_authority_edge",
                    "profile_id": profile_id,
                    "record_id": f"{profile_id}:{authority_id}",
                    "authority_id": authority_id,
                },
                source_revision=revision,
            )
            for service_id in _string_list(profile.get("service_ids")):
                yield EdgeFact(
                    src=profile_node_id,
                    dst=service_id,
                    edge_type="references",
                    attrs={"service_id": service_id},
                    source_record_id={
                        "deployment": config.deployment_name,
                        "record_type": "profile_service_edge",
                        "profile_id": profile_id,
                        "record_id": f"{profile_id}:{service_id}",
                        "service_id": service_id,
                    },
                    source_revision=revision,
                )


def _record_set(
    facts: list[NodeFact | EdgeFact],
    *,
    revision: dict[str, Any],
) -> RecordSet:
    nodes = [
        fact for fact in facts
        if isinstance(fact, NodeFact)
    ]
    edges = [
        fact for fact in facts
        if isinstance(fact, EdgeFact)
    ]
    descendants = [
        RecordNode(
            node_id=node.node_id,
            subtype=node.subtype,
            source_record_id=node.source_record_id,
            source_revision=revision,
        )
        for node in nodes
    ]
    record_edges = [
        EdgeKey(
            src=edge.src,
            dst=edge.dst,
            edge_type=edge.edge_type,
            attrs=edge.attrs,
            provenance=edge.provenance,
            confidence=edge.confidence,
            source_record_id=edge.source_record_id,
            source_revision=revision,
        )
        for edge in edges
    ]
    return RecordSet(records={
        "external_live_authority": RecordEmission(
            descendants=descendants,
            edges=record_edges,
        )
    })


def _keyed_records(records: Any, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError(f"{label} must be a list")
    out: dict[str, dict[str, Any]] = {}
    for raw in records:
        if not isinstance(raw, dict):
            raise ValueError(f"each {label} entry must be a mapping")
        record_id = _required_str(raw, "id", context=label)
        if record_id in out:
            raise ValueError(f"duplicate {label} id: {record_id}")
        out[record_id] = dict(raw)
    return out


def _diagnostic_keyed_records(
    records: Any,
    label: str,
    report: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    if not isinstance(records, list):
        _add_issue(
            report,
            "errors",
            f"{label}_not_list",
            f"{label} must be a list",
        )
        return {}
    out: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(records):
        if not isinstance(raw, dict):
            _add_issue(
                report,
                "errors",
                f"{label}_entry_not_mapping",
                f"each {label} entry must be a mapping",
                index=index,
            )
            continue
        record_id = raw.get("id")
        if not isinstance(record_id, str) or not record_id.strip():
            _add_issue(
                report,
                "errors",
                f"{label}_id_missing",
                f"{label} entry is missing required string 'id'",
                index=index,
            )
            continue
        record_id = record_id.strip()
        if record_id in out:
            _add_issue(
                report,
                "errors",
                f"duplicate_{label}_id",
                f"duplicate {label} id: {record_id}",
                record_id=record_id,
            )
            continue
        out[record_id] = dict(raw)
    return out


def _diagnostic_ids(records: Any) -> list[str]:
    if not isinstance(records, list):
        return []
    ids: list[str] = []
    for raw in records:
        if isinstance(raw, dict):
            record_id = raw.get("id")
            if isinstance(record_id, str) and record_id.strip():
                ids.append(record_id.strip())
    return ids


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r") as handle:
        raw = fastyaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return raw


def _required_str(raw: Mapping[str, Any], key: str, *, context: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}: missing required string {key!r}")
    return value.strip()


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list) or not all(
        isinstance(item, str) for item in value
    ):
        raise ValueError(f"expected string list, got {value!r}")
    return list(value)


def _bounded_string_list(
    value: Any,
    *,
    field: str,
    context: str,
    max_items: int,
    max_chars: int,
) -> list[str]:
    values = _string_list(value)
    if len(values) > max_items:
        raise ValueError(
            f"{context}: {field} has {len(values)} entries; max {max_items}"
        )
    for item in values:
        if len(item) > max_chars:
            raise ValueError(
                f"{context}: {field} entry exceeds {max_chars} characters"
            )
    return values


def _bounded_string(
    value: Any,
    *,
    field: str,
    context: str,
    max_chars: int,
) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{context}: {field} must be a string")
    if len(value) > max_chars:
        raise ValueError(
            f"{context}: {field} exceeds {max_chars} characters"
        )
    return value


def _text(*parts: Any) -> str:
    flat: list[str] = []
    for part in parts:
        if part is None:
            continue
        if isinstance(part, list):
            flat.extend(str(item) for item in part if str(item).strip())
            continue
        text = str(part).strip()
        if text:
            flat.append(text)
    return " ".join(flat)


def _content_hash(*paths: Path) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _checked_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def _add_issue(
    report: dict[str, Any],
    bucket: str,
    code: str,
    message: str,
    **data: Any,
) -> None:
    issue = {"code": code, "message": message}
    if data:
        issue.update(data)
    report[bucket].append(issue)


def _finalize_diagnostics(report: dict[str, Any]) -> dict[str, Any]:
    if report["errors"]:
        report["ok"] = False
        report["status"] = "ERROR"
    elif report["warnings"]:
        report["ok"] = True
        report["status"] = "WARN"
    else:
        report["ok"] = True
        report["status"] = "OK"
    return report


def _default_profile_id(deployment_name: str, tool_name: str) -> str:
    prefix = f"{deployment_name}_"
    suffix = tool_name[len(prefix):] if tool_name.startswith(prefix) else tool_name
    normalized = suffix.strip("_").replace("_", ".")
    return f"{deployment_name}.{normalized}" if normalized else deployment_name


def _humanize_profile_name(profile_id: str) -> str:
    return profile_id.replace(".", " ").replace("_", " ").title()


__all__ = [
    "ExternalLiveAuthoritySource",
    "scaffold_external_live_authority_profile",
    "validate_external_live_authority_profiles",
]
