from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
QUICKSTART = ROOT / "docs" / "runbooks" / "workspace-graph" / "setup.md"
DEPLOYMENT_README = ROOT / "deployments" / "okg-workspace" / "README.md"
LLMS_TXT = ROOT / "okg-llms.txt"


def test_workspace_graph_quickstart_is_generic() -> None:
    text = QUICKSTART.read_text()

    assert "# Workspace Graph Quickstart" in text
    assert (
        "A Workspace Graph maps one git-backed software workspace "
        "per deployment"
    ) in text
    assert "okg-workspace" not in text
    assert "this repository's own graph" not in text
    assert "OKG checkout" not in text
    assert "a set of repositories" not in text
    assert "create a deployment from the reusable" in text
    assert "--profile workspace-graph" in text
    assert "--deployment-name <deployment>" in text
    assert "--repo-root <repo-root>" in text
    assert 'postgresql://postgres:okg@127.0.0.1:5433/<db-name>' in text
    assert "--mcp-port <mcp-port>" in text
    assert "<deployment-dir>" in text
    assert "A git repository at `<repo-root>`." in text
    setup_section = text.split("## 2. Set Up The Deployment", 1)[1].split(
        "## 3. Verify The Graph",
        1,
    )[0]
    primary_command = setup_section.split("```bash", 1)[1].split("```", 1)[0]
    assert "--no-publish" in primary_command
    assert "Commit those files before the first publish" in text
    assert "Publishing refuses an uncommitted deployment" in text.replace("\n", " ")
    assert "top-level directory of a git worktree" in text
    assert "okg provision" in text
    assert "--deployment <deployment>" in text
    assert "--publish-once" in text
    assert "installs the required PostgreSQL extensions" in text
    assert "uv run okg --json status --deployment <deployment>" in text
    assert "latest_published_status" in text
    assert "latest_published_node_count" in text
    assert "uv run okg generation describe <generation-id>" in text
    assert "OKG_NOMOS_SUBJECT=subject:human_operator" in text
    assert "subject:agent" in text
    assert "okg.v_nodes" not in text
    assert "okg deployment ready <deployment> --checks dogfood" in text
    assert "Dogfood readiness is stricter than the one-shot quickstart" in text
    assert "okg deployment up <deployment>" not in text
    assert "--profile dev" not in text


def test_quickstart_names_the_declared_newcomer_door() -> None:
    """The setup step must name the door, not the alias.

    Per PACT `define-newcomer-entry-path`
    (req.entry.migrate-the-other-doors): `okg install` is the one
    documented way in and `okg init` is an alias of it. The alias keeps
    working, so nothing stops this doc drifting back to it — except this.
    """
    from okg.substrate.cli import entry_roles

    door = entry_roles.newcomer_door()
    text = QUICKSTART.read_text()
    setup_section = text.split("## 2. Set Up The Deployment", 1)[1].split(
        "## 3. Verify The Graph", 1,
    )[0]
    primary_command = setup_section.split("```bash", 1)[1].split("```", 1)[0]

    assert f"okg {door}" in primary_command, (
        f"the quickstart's setup command does not name the declared door "
        f"{door!r}:\n{primary_command}"
    )
    for verb, role in entry_roles.ENTRY_ROLES.items():
        if role in {entry_roles.ALIAS, entry_roles.FROZEN}:
            assert f"okg {verb} " not in primary_command, (
                f"the setup command runs {verb!r}, which is a retired door"
            )


def test_generic_quickstart_has_no_repo_specific_carveout() -> None:
    text = QUICKSTART.read_text()

    forbidden = [
        "For this repository's own graph",
        "--deployment okg-workspace",
        "--deployment-name okg-workspace",
        "okg deployment env okg-workspace",
        "okg deployment up okg-workspace",
        "okg deployment ready okg-workspace",
        "dogfood status --deployment okg-workspace",
        "generation describe <generation-id> --deployment okg-workspace",
    ]

    for phrase in forbidden:
        assert phrase not in text


def test_profile_smoke_test_uses_sample_deployment() -> None:
    text = DEPLOYMENT_README.read_text()
    smoke_test = text.split("## Profile Smoke Test", 1)[1]

    assert "--deployment-name sample-workspace" in smoke_test
    assert "deployment ready sample-workspace" in smoke_test
    assert "--no-publish" not in smoke_test
    assert "--deployment-name okg-workspace" not in smoke_test
    assert "--deployment okg-workspace" not in smoke_test


def test_generated_llms_entry_describes_workspace_graph() -> None:
    text = LLMS_TXT.read_text()

    assert (
        "A Workspace Graph maps one git-backed software workspace "
        "per deployment"
    ) in text
    assert "A Workspace Graph can map one repository, a set of repositories" not in text
