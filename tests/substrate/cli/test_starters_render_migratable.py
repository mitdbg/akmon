"""Every shipped starter renders a deployment that loads, migrates and ingests.

Issue #2816: ``okg init --template devops`` (and ``docs-corpus``, ``empty``)
rendered a ``deployment.yaml`` with no ``schema_dir`` or ``source_registry``
key and a fixed ``name`` instead of the name given to ``okg init``. Every
command that loads the deployment refused it (``okg migrate``:
``deployment.yaml missing required key: 'schema_dir'``), so the starter could
not be used without hand edits.

The starter list is read from the library at test time, so a starter added
later is checked too. Each starter is rendered through the real ``okg init``
entry point and used exactly as rendered:

* without a database: the strict manifest loader accepts it, the rendered
  name is the one ``okg init`` was given, and every ``fixture_path`` a source
  declares resolves to a shipped file;
* with PostgreSQL: ``okg migrate --apply`` and ``okg catalog load --apply``
  succeed; a starter that ships fixtures ingests them to a published
  generation with records from every source, and a starter with no sources
  publishes through ``okg run --once``.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.library import resolve
from okg.substrate.library.resolver import available_refs
from okg.substrate.sources.registry import load_source_registry
from tests.substrate._deployment_repo import commit_deployment_tree


def _starters() -> list[str]:
    names = sorted(available_refs("templates"))
    assert {"devops", "docs-corpus", "codebase-index", "empty"} <= set(names), names
    return names


def _ships_fixtures(template: str) -> bool:
    return (Path(resolve(f"okg.templates.{template}")) / "fixtures").is_dir()


def _deployment_name(template: str) -> str:
    return "s2816_" + template.replace("-", "_")


def _render(
    template: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """Run ``okg init --template <template> --no-publish`` into a temp target."""
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    claude_projects = tmp_path / "claude-projects"
    claude_projects.mkdir()
    target = tmp_path / "deployments"
    name = _deployment_name(template)
    rc = cli.main([
        "init",
        "--template", template,
        "--target", str(target),
        "--repo-root", str(repo),
        "--claude-projects-root", str(claude_projects),
        "--deployment-name", name,
        "--no-publish",
        "--non-interactive",
        "--yes",
    ])
    assert rc == 0, f"okg init --template {template} failed"
    deployment_dir = target / name
    assert (deployment_dir / "deployment.yaml").is_file()
    return deployment_dir


def _json_payload(capsys: pytest.CaptureFixture[str]) -> dict:
    """The command's ``--json`` envelope: the last line it wrote to stdout."""
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


@pytest.mark.parametrize("template", _starters())
def test_rendered_starter_loads_with_its_given_name(
    template: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment_dir = _render(template, tmp_path, monkeypatch)
    manifest = DeploymentManifest.from_yaml(deployment_dir / "deployment.yaml")
    assert manifest.name == _deployment_name(template)
    assert manifest.schema_dir.is_dir()

    registry = load_source_registry(deployment_dir / "source_registry.yaml")
    fixture_paths = {
        name: Path(str(entry.raw["params"]["fixture_path"]))
        for name, entry in registry.items()
        if isinstance(entry.raw.get("params"), dict)
        and "fixture_path" in entry.raw["params"]
    }
    # A fixture_path anywhere but params never reaches the reader.
    assert not [
        name for name, entry in registry.items() if "fixture_path" in entry.raw
    ], f"{template}: fixture_path outside params"
    if _ships_fixtures(template):
        assert set(fixture_paths) == set(registry), (
            f"{template} ships fixtures but these sources read none: "
            f"{sorted(set(registry) - set(fixture_paths))}"
        )
    for name, path in fixture_paths.items():
        assert path.is_absolute(), f"{template}/{name}: {path} not expanded"
        assert path.is_file(), f"{template}/{name}: {path} missing"
        assert deployment_dir.resolve() in path.resolve().parents


@pytest.mark.postgres
@pytest.mark.parametrize("template", _starters())
def test_rendered_starter_migrates_and_ingests_its_fixtures(
    template: str,
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deployment_dir = _render(template, tmp_path, monkeypatch)
    name = deployment_dir.name
    commit_deployment_tree(tmp_path / "repo", "starter code repository")
    commit_deployment_tree(deployment_dir.parent, f"okg init --template {template}")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployment_dir.parent))
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    capsys.readouterr()

    rc = cli.main(["--json", "migrate", "--deployment", name, "--apply"])
    out = capsys.readouterr()
    assert rc == 0, f"okg migrate on the rendered {template} starter: {out.out}{out.err}"

    # The catalog load claims catalog ownership for the deployment; a
    # publish refuses an unclaimed catalog.
    rc = cli.main([
        "--json", "catalog", "load", "--deployment", str(deployment_dir), "--apply",
    ])
    out = capsys.readouterr()
    assert rc == 0, f"okg catalog load on the rendered {template} starter: {out.out}{out.err}"

    registry = load_source_registry(deployment_dir / "source_registry.yaml")
    if not registry:
        # A starter with no sources publishes through the command okg init
        # prints after migrate.
        rc = cli.main(["--json", "run", "--once", "--deployment", name, "--apply"])
        run = _json_payload(capsys)
        assert rc == 0, run
        assert run["status"] == "published", run
        assert run["generation_id"].startswith(f"okg:{name}:"), run
        return
    if not _ships_fixtures(template):
        return
    rc = cli.main(["--json", "ingest", "--deployment", name, "--inline"])
    ingest = _json_payload(capsys)
    assert rc == 0, [
        (item.get("status"), item.get("error"), item.get("error_data"))
        for item in ingest.get("publishes", [])
    ]
    results = {item["name"]: item for item in ingest["results"]}
    assert set(results) == set(registry), ingest
    for source, item in results.items():
        assert item["status"] == "OK", item
        assert item["nodes_appended"] > 0, f"{template}/{source} read no record: {item}"
    published = [p for p in ingest["publishes"] if p.get("status") == "published"]
    assert published, ingest["publishes"]
    assert published[-1]["generation_id"].startswith(f"okg:{name}:"), published
