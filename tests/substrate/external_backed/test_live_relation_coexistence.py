"""The old deployment agent-tool path is untouched by live relations.

Issue #2013, and the operator decision of 2026-09-20 that DEFERRED its
deletion to issue #2087. Live relations and the deployment `agent_tools` /
`external_live` path now coexist: this change adds the new mechanism and
changes nothing about the old one.

That coexistence needs a test, not a promise. The census behind the deferral
found deployments in the reference-deployments repository that still declare
`agent_tools` (one block has four tools, every one `boundary: external_live`
with `requires_env`) or enrol the `external_authority` ontology module (two of
them). A regression here
would not fail in this repository; it would fail on someone else's deployment
at load, which is exactly why the deletion was split out.

So the assertions below are deliberately about the OLD mechanism, in a file
owned by the new one: the manifest key still parses, an `external_live` tool
with `requires_env` still audits the way it always did, and the substrate
still emits the key by default. Nothing here is aspirational -- each one
would have broken under the deletion that is now parked on
`claude/s20-2013-delete-agent-tools-parked`, so together they are the
evidence that the parked work really is absent from this branch.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.deployment_contract import audit_deployment_portfolio
from okg.substrate.deployments import DeploymentManifest

#: Shaped after a real deployment block: a credentialed live tool. The deletion
#: would have refused this manifest outright at load.
CREDENTIALED_TOOL = {
    "module": "example_tools.live_search",
    "function": "example_search",
    "boundary": "external_live",
    "requires_env": ["EXAMPLE_SERVICE_TOKEN"],
    "optional": True,
    "description": (
        "Search live external example records. Results are live external "
        "data, not generation-pinned OKG reads."
    ),
}


def _write_manifest(root: Path, *, agent_tools: dict) -> Path:
    deployment_dir = root / "legacy-agent-tools"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n", encoding="utf-8",
    )
    manifest = {
        "name": "legacy-agent-tools",
        "description": "coexistence fixture",
        "postgres": {"dsn": "postgres://u:p@localhost:5432/demo"},
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
        "contract": {
            "version": 1,
            "lifecycle": "prototype",
            "owner": "okg-operators",
            "intended_usage": "agent_tools coexistence regression",
            "promotion_path": "migrate to live relations under issue #2087",
            "evidence": {
                "readiness": {
                    "status": "not_required",
                    "collected_at": None,
                    "ttl_seconds": None,
                    "provenance": "unit-test",
                },
            },
        },
        "agent_tools": agent_tools,
    }
    path = deployment_dir / "deployment.yaml"
    path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    return path


def test_a_manifest_declaring_external_live_agent_tools_still_loads(
    tmp_path: Path,
) -> None:
    """The credentialed-tool shape parses, and its tool survives the round trip.

    This is the assertion the four external deployments depend on. Under the
    parked deletion the key is refused by name at load, so this test is the
    difference between this branch and that one.
    """
    path = _write_manifest(tmp_path, agent_tools={"example_search": CREDENTIALED_TOOL})
    manifest = DeploymentManifest._from_yaml_uncached(path)

    assert manifest.agent_tools == {"example_search": CREDENTIALED_TOOL}
    assert manifest.agent_tools["example_search"]["boundary"] == "external_live"
    assert manifest.agent_tools["example_search"]["requires_env"] == [
        "EXAMPLE_SERVICE_TOKEN",
    ]


def test_the_substrate_still_emits_agent_tools_by_default(
    tmp_path: Path,
) -> None:
    """A manifest omitting the key still gets `{}`, as it always did.

    The default emission is why the golden manifest fixture carries six
    occurrences of a key no deployment author typed. Removing it was part of
    the parked deletion; this pins that it did not happen here.
    """
    path = _write_manifest(tmp_path, agent_tools={})
    text = yaml.safe_load(path.read_text(encoding="utf-8"))
    del text["agent_tools"]
    path.write_text(yaml.safe_dump(text, sort_keys=False), encoding="utf-8")

    manifest = DeploymentManifest._from_yaml_uncached(path)
    assert manifest.agent_tools == {}


def test_the_credentialed_live_tool_contract_check_still_runs(
    tmp_path: Path,
) -> None:
    """The AKMON boundary check is intact, in both directions.

    A credentialed tool WITHOUT the `external_live` boundary is still a
    finding; the same tool with it is still clean. Asserting only the clean
    side would pass with the check deleted.
    """
    missing = dict(CREDENTIALED_TOOL)
    del missing["boundary"]
    _write_manifest(tmp_path / "bad", agent_tools={"example_search": missing})
    report = audit_deployment_portfolio(root=tmp_path / "bad")
    codes = {
        finding["code"]
        for deployment in report["deployments"]
        for finding in deployment["findings"]
    }
    assert "deployment_contract.akmon.external_live_boundary_missing" in codes

    _write_manifest(
        tmp_path / "good", agent_tools={"example_search": CREDENTIALED_TOOL},
    )
    clean = audit_deployment_portfolio(root=tmp_path / "good")
    clean_codes = {
        finding["code"]
        for deployment in clean["deployments"]
        for finding in deployment["findings"]
    }
    assert (
        "deployment_contract.akmon.external_live_boundary_missing"
        not in clean_codes
    )


def test_the_external_authority_ontology_module_is_still_present() -> None:
    """Two external deployments enrol it; its subtypes must still compose.

    Deleting the directory is parked work. A composed catalog that lost
    `ExternalLiveTool` would break those two deployments at catalog build,
    which no test in this repository would otherwise notice.
    """
    from okg.substrate.library import ontologies

    module_dir = Path(ontologies.__file__).parent / "external_authority"
    assert (module_dir / "module.yaml").is_file()
    schema = yaml.safe_load(
        (module_dir / "schema.yaml").read_text(encoding="utf-8"),
    )
    assert "ExternalLiveTool" in schema["classes"]
    assert "ToolAuthorityProfile" in schema["classes"]


def test_live_relations_add_no_key_to_the_legacy_manifest_path(
    tmp_path: Path,
) -> None:
    """The new mechanism is opt-in and invisible to a manifest that omits it.

    A deployment that declares no `agent_surface` keeps exactly the behaviour
    it had, which is the freeze this change promises.
    """
    path = _write_manifest(tmp_path, agent_tools={"example_search": CREDENTIALED_TOOL})
    manifest = DeploymentManifest._from_yaml_uncached(path)
    assert getattr(manifest, "agent_surface", None) is None
