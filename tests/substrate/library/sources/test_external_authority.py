from __future__ import annotations

import json
import textwrap

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.external_authority import (
    ExternalLiveAuthoritySource,
    scaffold_external_live_authority_profile,
    validate_external_live_authority_profiles,
)


def test_external_authority_source_emits_deterministic_low_cardinality_facts(
    tmp_path,
    monkeypatch,
) -> None:
    manifest, profiles = _write_fixture(tmp_path)
    monkeypatch.setenv("EXAMPLE_TOKEN", "secret-value")

    source = ExternalLiveAuthoritySource(
        deployment_yaml=manifest,
        profiles_path=profiles,
    )
    preflight = source.preflight()
    first = list(source.run("run-1", mode="scope_complete").facts)
    second = list(source.run("run-2", mode="scope_complete").facts)

    assert preflight.status == "ok"
    assert _node_ids(first) == _node_ids(second)
    assert _node_counts(first) == {
        "external_authority": 1,
        "external_live_tool": 1,
        "tool_authority_profile": 1,
        "service": 1,
        "endpoint": 1,
    }
    assert _edge_keys(first) == {
        (
            "external_live_tool:fixture:fixture_live_aggregate",
            "depends_on",
            "external_authority:example_authority",
        ),
        (
            "external_live_tool:fixture:fixture_live_aggregate",
            "references",
            "tool_authority_profile:fixture.live.aggregate",
        ),
        (
            "external_live_tool:fixture:fixture_live_aggregate",
            "exposes",
            "endpoint:fixture:live-endpoint",
        ),
        (
            "tool_authority_profile:fixture.live.aggregate",
            "references",
            "external_authority:example_authority",
        ),
        (
            "tool_authority_profile:fixture.live.aggregate",
            "references",
            "service:fixture:live-service",
        ),
    }
    tool = _node_by_id(
        first,
        "external_live_tool:fixture:fixture_live_aggregate",
    )
    assert tool.attrs["requires_env_names"] == ["EXAMPLE_TOKEN"]
    assert tool.attrs["optional"] is True
    profile = _node_by_id(first, "tool_authority_profile:fixture.live.aggregate")
    assert profile.attrs["search_terms"] == ["fixture live aggregate"]
    assert "secret-value" not in json.dumps(_fact_payload(first))


def test_external_authority_source_fails_unknown_tool(tmp_path) -> None:
    manifest, profiles = _write_fixture(
        tmp_path,
        profile_tool="fixture_missing_tool",
    )
    source = ExternalLiveAuthoritySource(
        deployment_yaml=manifest,
        profiles_path=profiles,
    )

    with pytest.raises(ValueError, match="unknown tool"):
        list(source.run("run-1").facts)


def test_external_authority_source_fails_non_external_live_tool(tmp_path) -> None:
    manifest, profiles = _write_fixture(
        tmp_path,
        tool_boundary="external_read",
    )
    source = ExternalLiveAuthoritySource(
        deployment_yaml=manifest,
        profiles_path=profiles,
    )

    with pytest.raises(ValueError, match="non-external_live"):
        list(source.run("run-1").facts)


def test_external_authority_source_bounds_prompt_like_profile_fields(tmp_path) -> None:
    manifest, profiles = _write_fixture(
        tmp_path,
        search_terms=["term"] * 17,
    )
    source = ExternalLiveAuthoritySource(
        deployment_yaml=manifest,
        profiles_path=profiles,
    )

    with pytest.raises(ValueError, match="search_terms has 17 entries"):
        list(source.run("run-1").facts)


def test_external_authority_diagnostics_warn_unprofiled_live_tool(tmp_path) -> None:
    manifest, profiles = _write_fixture(tmp_path)
    profiles.write_text(
        textwrap.dedent(
            """
            version: 1
            authorities:
              - id: example_authority
                name: Example Authority
            profiles: []
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    report = validate_external_live_authority_profiles(manifest, profiles)

    assert report["ok"] is True
    assert report["status"] == "WARN"
    assert report["unprofiled_external_live_tools"] == [
        "fixture_live_aggregate"
    ]
    assert report["warnings"][0]["code"] == (
        "external_live_tool_without_authority_profile"
    )


def test_external_authority_diagnostics_error_stale_profile_tool(tmp_path) -> None:
    manifest, profiles = _write_fixture(
        tmp_path,
        profile_tool="fixture_missing_tool",
    )

    report = validate_external_live_authority_profiles(manifest, profiles)

    assert report["ok"] is False
    assert report["status"] == "ERROR"
    assert report["stale_profile_tools"] == ["fixture_missing_tool"]
    assert any(
        issue["code"] == "profile_references_unknown_tool"
        for issue in report["errors"]
    )


def test_external_authority_scaffold_uses_existing_authority(tmp_path) -> None:
    manifest, profiles = _write_fixture(tmp_path)
    profiles.write_text(
        textwrap.dedent(
            """
            version: 1
            authorities:
              - id: example_authority
                name: Example Authority
            services:
              - id: service:fixture:live-service
                name: Live Service
            profiles: []
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    scaffold = scaffold_external_live_authority_profile(
        manifest,
        profiles_path=profiles,
        tool_name="fixture_live_aggregate",
        service_ids=["service:fixture:live-service"],
        intent="live_fixture_aggregation",
        result_kind="aggregation",
    )

    assert scaffold["ok"] is True
    assert scaffold["existing_agent_tool"] is True
    assert scaffold["profile_already_declared"] is False
    assert scaffold["agent_tool_yaml"] == ""
    assert "authority: example_authority" in scaffold["profile_yaml"]
    assert "tool: fixture_live_aggregate" in scaffold["profile_yaml"]
    assert "TODO_ENV_VAR_NAME" not in scaffold["profile_yaml"]


def test_external_authority_scaffold_reports_existing_profile(tmp_path) -> None:
    manifest, profiles = _write_fixture(tmp_path)

    scaffold = scaffold_external_live_authority_profile(
        manifest,
        profiles_path=profiles,
        tool_name="fixture_live_aggregate",
    )

    assert scaffold["profile_already_declared"] is True
    assert scaffold["existing_profile_ids"] == ["fixture.live.aggregate"]
    assert scaffold["profile_yaml"] == ""


def test_external_authority_scaffold_new_tool_includes_agent_tool_stub(
    tmp_path,
) -> None:
    manifest, profiles = _write_fixture(tmp_path)

    scaffold = scaffold_external_live_authority_profile(
        manifest,
        profiles_path=profiles,
        tool_name="fixture_live_search",
    )

    assert scaffold["existing_agent_tool"] is False
    assert "fixture_live_search" in scaffold["agent_tool_yaml"]
    assert "boundary: external_live" in scaffold["agent_tool_yaml"]
    assert "TODO_ENV_VAR_NAME" in scaffold["agent_tool_yaml"]
    assert "id: fixture.live.search" in scaffold["profile_yaml"]


def _write_fixture(
    tmp_path,
    *,
    tool_boundary: str = "external_live",
    profile_tool: str = "fixture_live_aggregate",
    search_terms: list[str] | None = None,
):
    manifest = tmp_path / "deployment.yaml"
    profiles = tmp_path / "live_authority_profiles.yaml"
    manifest.write_text(
        textwrap.dedent(
            f"""
            name: fixture
            description: Fixture deployment
            agent_tools:
              fixture_live_aggregate:
                module: fixture_tools
                function: aggregate
                boundary: {tool_boundary}
                requires_env: [EXAMPLE_TOKEN]
                optional: true
                description: Aggregate live external fixture records.
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    profiles.write_text(
        textwrap.dedent(
            f"""
            version: 1
            authorities:
              - id: example_authority
                name: Example Authority
                boundary: external_live
                source_family: example_live
                description: Example live source.
            services:
              - id: service:fixture:live-service
                name: Live Service
            endpoints:
              - id: fixture:live-endpoint
                name: Fixture live endpoint
                url: https://example.invalid/live
            profiles:
              - id: fixture.live.aggregate
                name: Fixture live aggregate profile
                tool: {profile_tool}
                authority: example_authority
                service_ids: [service:fixture:live-service]
                endpoint_ids: [fixture:live-endpoint]
                intent: live_fixture_aggregation
                source_family: example_live
                subject_subtypes: [fixture_record]
                result_kind: aggregation
                default_time_window: now-1h
                freshness_note: Observed at call time.
                query_examples:
                  - status:error
                search_terms: {search_terms or ["fixture live aggregate"]}
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return manifest, profiles


def _node_counts(facts: list[object]) -> dict[str, int]:
    out: dict[str, int] = {}
    for fact in facts:
        if isinstance(fact, NodeFact):
            out[fact.subtype] = out.get(fact.subtype, 0) + 1
    return out


def _node_ids(facts: list[object]) -> set[str]:
    return {
        fact.node_id for fact in facts
        if isinstance(fact, NodeFact)
    }


def _edge_keys(facts: list[object]) -> set[tuple[str, str, str]]:
    return {
        (fact.src, fact.edge_type, fact.dst)
        for fact in facts
        if isinstance(fact, EdgeFact)
    }


def _node_by_id(facts: list[object], node_id: str) -> NodeFact:
    for fact in facts:
        if isinstance(fact, NodeFact) and fact.node_id == node_id:
            return fact
    raise AssertionError(f"node not found: {node_id}")


def _fact_payload(facts: list[object]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for fact in facts:
        if isinstance(fact, NodeFact):
            payload.append({
                "node_id": fact.node_id,
                "attrs": fact.attrs,
                "source_record_id": fact.source_record_id,
                "source_revision": fact.source_revision,
            })
        elif isinstance(fact, EdgeFact):
            payload.append({
                "src": fact.src,
                "dst": fact.dst,
                "attrs": fact.attrs,
                "source_record_id": fact.source_record_id,
                "source_revision": fact.source_revision,
            })
    return payload
