"""The release shape `okg install` writes, checked by every downstream gate.

Issue #2127. `okg install` wrote a `deployment.yaml` with no `release:` block
at all. A deployment outside a Git repository -- the usual case, because the
XDG user-state deployments root is usually not a working tree -- then hit a
different refusal from each command the installer told the operator to run
next:

* `okg run --once --apply` and the publish gate refused with `deployment
  release authority is unavailable`, fixable only by hand-adding
  `release.kind: standalone`.
* `okg mcp-serve --deployment <name>` refused that hand-edited file in turn
  with `deployment release.channel must be dev or prod`, because it loads the
  full release contract and a declared `release` block with no channel is a
  hard refusal.

The systemic cause was that no test took the file the installer WROTE and put
it through the release checks the next commands APPLY. This file is that test.
It is deliberately a manifest-level contract test with no database and no
Docker, so it runs in the fast lane where a regression is caught in minutes.

What it must keep true:

1. The unedited installer output satisfies every release check that `okg run`,
   the publish gate and `okg mcp-serve` apply -- in one shape, with no hand
   edit.
2. Neither key is redundant. Dropping `channel` reproduces the mcp-serve
   refusal; dropping `kind` reproduces the publish refusal. This is what keeps
   the fix "the installer writes more" rather than "the checks were loosened".
3. A scaffold INSIDE a Git repository still gets `channel` -- the contract
   loader requires it wherever the deployment sits -- and still gets NO
   standalone declaration, so it keeps the ordinary checkout-tracking path.
   Only `kind` is bounded by the location probe; tying `channel` to it left
   the original refusal in place for an operator whose home directory is a
   Git repository, or who scaffolds into a project checkout.
4. A profile that ships a PARTIAL release block gets it completed rather than
   kept verbatim. `release: {kind: standalone}` -- the shape the old fix hint
   told operators to hand-write -- would otherwise survive the no-overwrite
   rule and produce a file every contract-loading command refuses.
"""
from __future__ import annotations

import json
import subprocess
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import okg.substrate.cli.main as cli
from okg.substrate import deployment_release as release
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.errors import SubstrateError


@pytest.fixture()
def profile_dir(tmp_path: Path) -> Path:
    """A minimal profile that reaches the real scaffold writer.

    `modules: []` keeps the composed catalog loadable, which matters: the
    standalone branch of `current_release_artifact` computes a catalog
    fingerprint, and a fixture whose modules do not compose would fail there
    for an unrelated reason and hide the release answer.
    """
    d = tmp_path / "profiles" / "fixture"
    d.mkdir(parents=True)
    (d / "profile.yaml").write_text(textwrap.dedent("""\
        name: fixture
        version: 0.1.0
        description: in-test profile
        init_questions:
          - id: deployment_name
            prompt: deployment name
            type: slug
            required: true
          - id: postgres_dsn
            prompt: postgres dsn
            type: string
            required: false
            default: postgresql://okg:okg@127.0.0.1:59999/unreachable
        modules: modules.yaml
        """))
    (d / "modules.yaml").write_text(yaml.safe_dump({"modules": []}))
    return d


def _install(
    *, deployments_root: Path, name: str, monkeypatch, capsys,
    profile_dir: Path,
) -> Path:
    """Run the real scaffold path and return the deployment directory."""
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    args = SimpleNamespace(
        command="install",
        profile_name="fixture",
        template=None,
        deployment_name=name,
        deployments_root=str(deployments_root),
        force=False,
        yes=False,
        # The first publish needs a database; this suite is about the file the
        # installer writes, so the publish stage is skipped and the release
        # checks are applied to that file directly.
        no_publish=True,
        non_interactive=True,
        dry_run=False,
        target=None,
        repo_root=None,
        openspec_root=None,
        claude_projects_root=None,
        notes_root=None,
        since=None,
    )
    rc = cli._cmd_init(args, as_json=True)
    out = capsys.readouterr().out
    assert rc == 0, out
    dep_dir = deployments_root / name
    assert (dep_dir / "deployment.yaml").is_file(), out
    return dep_dir


def _healthy_database() -> release.DatabaseReleaseState:
    """A database with nothing to report, so only release problems surface.

    `evaluate_release_contract` mixes schema-ledger findings into the same
    problem list as release findings. Feeding it a clean state is what makes
    a non-empty `problems` list mean "the release contract refused".
    """
    return release.DatabaseReleaseState(
        ledger_exists=True,
        schema_checksum=None,
        schema_ok=True,
        pending_migrations=(),
        blocking_pending_migrations=(),
        checksum_mismatches=(),
        checksum_compatibilities=(),
        latest_catalog_version_id=1,
        latest_catalog_fingerprint=None,
    )


def test_installer_writes_the_standalone_release_shape(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """The bytes on disk, before anyone edits them."""
    dep_dir = _install(
        deployments_root=tmp_path / "deployments", name="fresh",
        monkeypatch=monkeypatch, capsys=capsys, profile_dir=profile_dir,
    )

    written = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    assert written["release"] == {"channel": "dev", "kind": "standalone"}


def test_installer_output_passes_run_publish_and_mcp_serve_unedited(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """The class guard: one file, every release check, zero hand edits.

    Each assertion below names the command whose gate it stands for, so a
    future regression report says which operator command broke.
    """
    dep_dir = _install(
        deployments_root=tmp_path / "deployments", name="contract",
        monkeypatch=monkeypatch, capsys=capsys, profile_dir=profile_dir,
    )
    manifest = DeploymentManifest.from_yaml(dep_dir / "deployment.yaml")

    # The publish gate (`okg run --once --apply`, `okg publish`): the
    # publisher reads the declaration through this function when it decides
    # whether repository provenance may be recorded as unavailable.
    assert release.standalone_release_declared(dep_dir) is True

    # The release contract every enforcing command loads: `okg run`, the
    # publish gate, `okg runtime worker` and `okg mcp-serve` all reach
    # `load_release_contract` through `build_release_report`. This is the call
    # that refused the hand-edited file with "release.channel must be dev or
    # prod".
    contract = release.load_release_contract(manifest)
    assert contract.channel == "dev"
    assert contract.pin is None

    # The artifact side of the same report. This is where the F3 refusal
    # ("deployment release authority is unavailable") came from: without a
    # declaration the code fell through to Git authority resolution, which
    # cannot succeed outside a repository.
    current = release.current_release_artifact(manifest)
    assert current.release_kind == release.STANDALONE_RELEASE_KIND
    assert current.code_revision is None
    assert current.catalog_fingerprint is not None

    # The verdict those commands act on, with a database that has nothing to
    # say. No release problems means no refusal.
    report = release.evaluate_release_contract(
        deployment=manifest.name,
        contract=contract,
        current=current,
        database=_healthy_database(),
        process_role="mcp",
    )
    assert report["problems"] == []
    assert report["ok"] is True


def test_both_keys_are_load_bearing(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """Removing either key reproduces the refusal it exists to answer.

    This is the half that proves no check was weakened. `kind` alone is what
    the old fix hint told operators to add, and it is still refused by the
    contract loader; `channel` alone still leaves the publisher without a
    declaration and on the Git-authority path.
    """
    dep_dir = _install(
        deployments_root=tmp_path / "deployments", name="loadbearing",
        monkeypatch=monkeypatch, capsys=capsys, profile_dir=profile_dir,
    )
    manifest_path = dep_dir / "deployment.yaml"
    written = yaml.safe_load(manifest_path.read_text())

    kind_only = dict(written, release={"kind": "standalone"})
    manifest_path.write_text(yaml.safe_dump(kind_only, sort_keys=False))
    with pytest.raises(SubstrateError) as refused:
        release.load_release_contract(
            DeploymentManifest.from_yaml(manifest_path),
        )
    assert "release.channel must be dev or prod" in str(refused.value)

    channel_only = dict(written, release={"channel": "dev"})
    manifest_path.write_text(yaml.safe_dump(channel_only, sort_keys=False))
    assert release.standalone_release_declared(dep_dir) is False
    with pytest.raises(SubstrateError) as authority:
        release.current_release_artifact(
            DeploymentManifest.from_yaml(manifest_path),
        )
    assert "release authority is unavailable" in str(authority.value)


def test_scaffold_inside_a_repository_gets_no_standalone_declaration(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """The declaration is bounded by where the deployment actually is.

    A deployment scaffolded into a real working tree must keep the ordinary
    checkout-tracking path and every refusal that comes with it. If this test
    ever passes for the wrong reason -- a blanket standalone declaration --
    the installer would have silenced drift refusals in real checkouts.
    """
    repo = tmp_path / "checkout"
    (repo / "deployments").mkdir(parents=True)
    subprocess.run(
        ["git", "init", "-q"], cwd=repo, check=True, capture_output=True,
    )

    dep_dir = _install(
        deployments_root=repo / "deployments", name="inrepo",
        monkeypatch=monkeypatch, capsys=capsys, profile_dir=profile_dir,
    )

    written = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    assert "kind" not in written["release"]
    assert release.standalone_release_declared(dep_dir) is False


def test_scaffold_inside_a_repository_still_gets_a_channel(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """The channel is not conditional on being outside a repository.

    An operator whose `$HOME` is a Git repository (a common dotfiles
    pattern), or who runs `--deployments-root ./deployments` inside a
    project checkout, gets a scaffold target inside a working tree. Tying
    `channel` to the outside-a-repository probe left that operator with a
    manifest `okg deployment release` refuses outright ("deployment
    release.channel is required") and that `okg mcp-serve` and
    `okg runtime worker` SKIP rather than pass -- the release contract is
    only enforced when a block is declared, so no block means no schema-skew
    check at all.
    """
    repo = tmp_path / "checkout"
    (repo / "deployments").mkdir(parents=True)
    subprocess.run(
        ["git", "init", "-q"], cwd=repo, check=True, capture_output=True,
    )

    dep_dir = _install(
        deployments_root=repo / "deployments", name="inrepochannel",
        monkeypatch=monkeypatch, capsys=capsys, profile_dir=profile_dir,
    )

    written = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    assert written["release"] == {"channel": "dev"}

    # The refusal this key answers, gone: the same loader that every
    # enforcing command reaches now accepts the unedited file.
    contract = release.load_release_contract(
        DeploymentManifest.from_yaml(dep_dir / "deployment.yaml"),
    )
    assert contract.channel == "dev"
    assert contract.pin is None


def test_a_partial_profile_release_block_is_completed_not_kept(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """`release: {kind: standalone}` alone must not reach disk.

    The no-overwrite rule protects a profile deliberate declaration, but a
    block with no channel is not a declaration the loader can read: it is
    the original bug arriving by another route. The installer completes it
    and changes nothing else.
    """
    (profile_dir / "deployment-defaults.yaml").write_text(
        yaml.safe_dump({"release": {"kind": "standalone"}}),
    )
    (profile_dir / "profile.yaml").write_text(
        (profile_dir / "profile.yaml").read_text()
        + "deployment_defaults: deployment-defaults.yaml\n",
    )
    dep_dir = _install(
        deployments_root=tmp_path / "deployments", name="partial",
        monkeypatch=monkeypatch, capsys=capsys, profile_dir=profile_dir,
    )

    written = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    assert written["release"] == {"channel": "dev", "kind": "standalone"}

    contract = release.load_release_contract(
        DeploymentManifest.from_yaml(dep_dir / "deployment.yaml"),
    )
    assert contract.channel == "dev"


def test_profile_declared_release_block_is_not_overwritten(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """A profile that ships its own `release:` keeps it.

    The installer supplies the standalone shape where nothing else declared
    one. It does not silently replace a deliberate declaration -- a packaged
    profile's own release block would otherwise be rewritten to standalone by
    the mere fact of scaffolding outside a checkout.
    """
    (profile_dir / "deployment-defaults.yaml").write_text(
        yaml.safe_dump({"release": {"channel": "prod"}}),
    )
    (profile_dir / "profile.yaml").write_text(
        (profile_dir / "profile.yaml").read_text()
        + "deployment_defaults: deployment-defaults.yaml\n",
    )
    dep_dir = _install(
        deployments_root=tmp_path / "deployments", name="declared",
        monkeypatch=monkeypatch, capsys=capsys, profile_dir=profile_dir,
    )

    written = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    assert written["release"] == {"channel": "prod"}


# ---------------------------------------------------------------------------
# One existence rule for the deployments root (issue #2127, SHOULD-FIX 5)
# ---------------------------------------------------------------------------


def test_init_refuses_a_missing_environment_root_like_install_does(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """`okg init` used to create it; `okg install` refused it.

    Same variable, same typo, two different outcomes -- one of them a
    silent second deployments root. The resolver contract in
    `okg.substrate.deployments` is explicit that an env var set but not
    pointing at an existing directory raises rather than falling through,
    so both commands now refuse, with one shared envelope.
    """
    missing = tmp_path / "never-created"
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(missing))
    args = SimpleNamespace(
        command="init", profile_name="fixture", template=None,
        deployment_name="envrule", deployments_root=None, force=False,
        yes=False, no_publish=True, non_interactive=True, dry_run=False,
        target=None, repo_root=None, openspec_root=None,
        claude_projects_root=None, notes_root=None, since=None,
    )

    rc = cli._cmd_init(args, as_json=True)
    captured = capsys.readouterr()

    assert rc == 1
    payload = json.loads(captured.err)
    assert payload["error"]["code"] == "deployment_not_found"
    assert payload["error"]["data"]["env_var"] == "OKG_DEPLOYMENTS_DIR"
    assert str(missing) in payload["error"]["message"]
    assert not missing.exists()


def test_init_creates_an_explicitly_named_root(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """The other half of the rule: a named root is created, not refused.

    `--deployments-root` is the operator saying where this scaffold goes,
    so creating it is part of scaffolding. Only the ambient environment
    variable has to point at something that already exists.
    """
    root = tmp_path / "brand" / "new"
    assert not root.exists()

    dep_dir = _install(
        deployments_root=root, name="created",
        monkeypatch=monkeypatch, capsys=capsys, profile_dir=profile_dir,
    )

    assert dep_dir == root / "created"
