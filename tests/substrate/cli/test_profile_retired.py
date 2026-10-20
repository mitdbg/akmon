"""Retired install profiles fail loudly with `profile_retired`.

PACT remove-git-code-systems-research-profiles removed the `git-code` and
`systems-research` profiles with no compatibility alias. `discover_profile`
checks `RETIRED_PROFILES` before it searches any directory, so a retired
name is refused even when a stale copy of the profile still sits in
`OKG_PROFILES_DIR`, and the error names the replacement when there is one.
PACT merge-academic-researcher-profile retired `academic-pi` and
`cs-researcher` the same way, both replaced by `academic-researcher`.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli
import okg.substrate.library as library
from okg.substrate.catalog.profile import ProfileError
from okg.substrate.deployment_bootstrap.profile_init import (
    RETIRED_PROFILES,
    ProfileRetiredError,
    discover_profile,
)

LIBRARY_PROFILES = Path(library.__file__).parent / "profiles"


def _stale_copy(root: Path, name: str) -> None:
    profile_dir = root / name
    profile_dir.mkdir(parents=True)
    (profile_dir / "profile.yaml").write_text(
        f"name: {name}\nversion: 0.1.0\n",
    )


RETIRED_CASES = [
    ("git-code", "workspace-graph"),
    ("systems-research", None),
    ("academic-pi", "academic-researcher"),
    ("cs-researcher", "academic-researcher"),
]


@pytest.mark.parametrize(("name", "replacement"), RETIRED_CASES)
def test_retired_profile_is_refused_before_any_search(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    name: str, replacement: str | None,
) -> None:
    # A stale copy in every searched root must not resurrect the name.
    _stale_copy(tmp_path / "override", name)
    _stale_copy(tmp_path / "env", name)
    monkeypatch.setenv("OKG_PROFILES_DIR", str(tmp_path / "env"))

    with pytest.raises(ProfileRetiredError) as excinfo:
        discover_profile(name, profiles_root=tmp_path / "override")

    err = excinfo.value
    assert isinstance(err, ProfileError)
    assert err.code == "profile_retired"
    assert err.data["profile_name"] == name
    assert err.data["replacement"] == replacement
    assert err.data["reason"]
    if replacement is None:
        assert "no replacement" in str(err)
    else:
        assert f"--profile {replacement}" in str(err)
        assert f"--profile {replacement}" in (err.fix_hint or "")


@pytest.mark.parametrize(("name", "replacement"), RETIRED_CASES)
def test_okg_install_with_a_retired_profile_emits_profile_retired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
    name: str, replacement: str | None,
) -> None:
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    args = cli._build_parser(["install"]).parse_args(
        ["install", "--profile", name, "--name", "retired-probe"],
    )

    rc = cli._cmd_install(args, as_json=True)

    captured = capsys.readouterr()
    assert rc == 1
    envelope = json.loads(captured.err)["error"]
    assert envelope["code"] == "profile_retired", envelope
    assert envelope["data"]["replacement"] == replacement
    # Nothing was scaffolded.
    assert list(tmp_path.iterdir()) == []


def test_retired_map_names_live_replacements_and_no_shipped_folder() -> None:
    for name, retired in RETIRED_PROFILES.items():
        assert not (LIBRARY_PROFILES / name).exists(), (
            f"retired profile {name!r} still ships a library folder"
        )
        if retired.replacement is not None:
            assert retired.replacement not in RETIRED_PROFILES
            assert discover_profile(retired.replacement).name == (
                retired.replacement
            )


def test_a_live_profile_still_resolves() -> None:
    assert discover_profile("workspace-graph").name == "workspace-graph"


def test_profile_flag_runs_the_academic_researcher_installer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg install --profile academic-researcher` runs that profile's
    installer, the one registered under that name. Since issue #3414 a bare
    `okg install` builds the minimal starter instead."""
    ran: list[str] = []

    class _Recorder:
        def __init__(self, name: str) -> None:
            self.name = name

        def run(self, args, *, as_json: bool) -> int:  # noqa: ANN001
            ran.append(self.name)
            return 0

    import okg.substrate.cli.profile_cli_loader as loader

    real = loader.discover_install_extensions()
    assert "academic-researcher" in real
    assert "academic-pi" not in real
    args = cli._build_parser(["install"]).parse_args(
        ["install", "--profile", "academic-researcher"],
    )
    monkeypatch.setattr(
        loader, "discover_install_extensions",
        lambda: {name: _Recorder(name) for name in real},
    )

    assert cli._cmd_install(args, as_json=True) == 0
    assert ran == ["academic-researcher"]
    assert cli.DEFAULT_INSTALL_PROFILE == "minimal"


@pytest.mark.parametrize(("name", "replacement"), RETIRED_CASES)
def test_doctor_reports_a_retired_profile_as_profile_retired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    name: str, replacement: str | None,
) -> None:
    """`okg doctor` names the retired profile and its replacement instead
    of the generic `profile_unreadable` finding."""
    import yaml

    import okg.substrate.cli.doctor as cli_doctor

    dep_dir = tmp_path / "deployments" / "retired-dep"
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "retired-dep",
        "profile": {"name": name, "version": "0.1.0"},
        "modules": [],
    }))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda n: dep_dir if n == "retired-dep" else None,
    )

    diag = cli_doctor._doctor_profile_diagnostics("retired-dep")

    assert diag is not None
    [finding] = diag["yellow_findings"]
    assert finding["code"] == "profile_retired", finding
    assert finding["profile_name"] == name
    assert finding["replacement"] == replacement
    if replacement is not None:
        assert f"--profile {replacement}" in finding["fix_hint"]
