"""A misspelled or misplaced source-registry key fails loudly.

An unmarked (legacy) registry used to read the keys it knew and keep the rest
in ``raw`` unread, so ``enabled_by_defualt: true`` or a repository-level
``include_globs`` was silently ignored -- the second one would ingest a whole
repository. A strict_v1 registry already refused unknown keys through its
closed schema; the legacy loader now refuses them too.
"""
from __future__ import annotations

import json
import subprocess
import textwrap
from pathlib import Path

import pytest

from okg.substrate.sources.registry import (
    CODE_REPO_ENTRY_KEYS,
    CODE_REPOS_KEYS,
    SOURCE_ENTRY_CONTROL_KEYS,
    RetiredSourceRegistryKeyWarning,
    SourceRegistryUnknownKeyError,
    check_legacy_registry_keys,
    validate_legacy_document_keys,
    validate_legacy_entry_keys,
    expand_code_repos_sources,
    load_source_registry,
)
from okg.substrate import fastyaml

REPO_ROOT = Path(__file__).resolve().parents[2]

_ENTRY = (
    "sources:\n"
    "  jira:\n"
    "    module: okg.substrate.library.sources.jira\n"
    "    class: JiraSource\n"
    "    ownership_id: t.jira\n"
)


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "source_registry.yaml"
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


def _entry_registry(tmp_path: Path, extra: str) -> Path:
    """The jira entry plus ``extra``, a block at entry indentation."""
    path = tmp_path / "source_registry.yaml"
    path.write_text(_ENTRY + textwrap.indent(extra, "    "), encoding="utf-8")
    return path


def test_misspelled_entry_key_names_key_entry_and_closest_key(
    tmp_path: Path,
) -> None:
    reg = _entry_registry(tmp_path, "enabled_by_defualt: true\n")
    with pytest.raises(SourceRegistryUnknownKeyError) as caught:
        load_source_registry(reg)
    error = caught.value
    assert error.code == "source_registry_unknown_key"
    assert error.key == "enabled_by_defualt"
    assert error.location == "sources.jira"
    assert error.suggestion == "enabled_by_default"
    assert error.valid == SOURCE_ENTRY_CONTROL_KEYS
    message = str(error)
    assert "source_registry_unknown_key" in message
    assert str(reg) in message
    assert "'enabled_by_defualt'" in message
    assert "did you mean 'enabled_by_default'?" in message


def test_reader_setting_at_entry_level_says_move_it_under_params(
    tmp_path: Path,
) -> None:
    reg = _entry_registry(tmp_path, "fixture_path: fixtures/jira.json\n")
    with pytest.raises(SourceRegistryUnknownKeyError) as caught:
        load_source_registry(reg)
    assert caught.value.key == "fixture_path"
    assert caught.value.suggestion is None
    assert "sources.jira.params.fixture_path" in str(caught.value)


def test_the_same_setting_under_params_loads(tmp_path: Path) -> None:
    reg = _entry_registry(
        tmp_path, "params:\n  fixture_path: fixtures/jira.json\n",
    )
    entries = load_source_registry(reg)
    assert entries["jira"].raw["params"] == {
        "fixture_path": "fixtures/jira.json",
    }


def test_retired_schedule_block_still_loads_in_a_legacy_registry(
    tmp_path: Path,
) -> None:
    """`okg deployment bootstrap` drafted a per-source `schedule` before
    issue #2952. Nothing reads it, and promotion to strict_v1 removes it and
    says so; refusing it here would break every registry drafted before
    that fix."""
    reg = _entry_registry(tmp_path, "schedule:\n  reconcile_interval: 24h\n")
    with pytest.warns(RetiredSourceRegistryKeyWarning) as caught:
        entries = load_source_registry(reg)
    assert entries["jira"].raw["schedule"] == {"reconcile_interval": "24h"}
    # One warning for the load, naming the source, saying it is unread and
    # where a scheduled refresh lives.
    assert len(caught) == 1
    message = str(caught[0].message)
    assert str(reg) in message
    assert "sources jira carry a `schedule` block" in message
    assert "Nothing reads it" in message
    assert "`sync.poll`" in message
    # A misspelling of it is still refused. (Its own directory: the loader
    # caches by path and mtime, so rewriting the same file within one mtime
    # tick would return the cached admission.)
    (tmp_path / "misspelled").mkdir()
    misspelled = _entry_registry(
        tmp_path / "misspelled", "schedul:\n  reconcile_interval: 24h\n",
    )
    with pytest.raises(SourceRegistryUnknownKeyError):
        load_source_registry(misspelled)


def test_a_registry_without_schedule_loads_without_a_warning(
    tmp_path: Path,
) -> None:
    import warnings

    reg = _entry_registry(tmp_path, "enabled_by_default: true\n")
    with warnings.catch_warnings():
        warnings.simplefilter("error", RetiredSourceRegistryKeyWarning)
        load_source_registry(reg)


def test_raw_document_readers_get_the_same_key_check(tmp_path: Path) -> None:
    """Nomos baseline seeding and provision read the raw document; they call
    check_legacy_registry_keys so a bad key fails there too."""
    path = tmp_path / "source_registry.yaml"
    with pytest.raises(SourceRegistryUnknownKeyError) as caught:
        check_legacy_registry_keys(
            {"sources": {"jira": {"ownership_id": "t", "fixtur_path": "x"}}},
            path,
        )
    assert caught.value.location == "sources.jira"
    with pytest.raises(SourceRegistryUnknownKeyError):
        check_legacy_registry_keys({"sorces": {}}, path)
    # strict_v1 is left to its schema; the retired key passes.
    check_legacy_registry_keys(
        {"admission_contract": "strict_v1", "anything": 1}, path,
    )
    check_legacy_registry_keys(
        {"sources": {"jira": {"ownership_id": "t", "schedule": {}}}}, path,
    )


def test_nomos_baseline_refuses_an_unknown_key_without_a_database(
    tmp_path: Path, monkeypatch,
) -> None:
    from okg.substrate.errors import SubstrateError
    from okg.substrate.nomos.baseline import deployment_baseline_spec

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    (tmp_path / "deployment.yaml").write_text(
        "name: keyprobe\nsource_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    _entry_registry(tmp_path, "enabled_by_defualt: true\n")
    with pytest.raises(SubstrateError) as caught:
        deployment_baseline_spec(deployment="keyprobe", deployment_dir=tmp_path)
    assert caught.value.code == "source_registry_invalid"
    assert caught.value.data["detail_code"] == "source_registry_unknown_key"
    assert "enabled_by_defualt" in str(caught.value)


def test_cli_wrap_keeps_the_loader_code_as_detail_code(tmp_path: Path) -> None:
    from okg.substrate.cli.main import _source_registry_invalid_error

    reg = _entry_registry(tmp_path, "enabled_by_defualt: true\n")
    with pytest.raises(SourceRegistryUnknownKeyError) as caught:
        load_source_registry(reg)
    err = _source_registry_invalid_error(caught.value)
    assert err.code == "source_registry_invalid"
    assert err.data == {"detail_code": "source_registry_unknown_key"}
    plain = _source_registry_invalid_error(ValueError("bad yaml"))
    assert plain.data == {}


def test_academic_installer_refuses_an_unknown_key_before_docker(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    import okg.substrate.cli.main as cli
    from tests.substrate.cli.test_profile_duplicate_source_id import (
        _academic_researcher_override,
        _record_docker,
    )

    researcher_cli = _academic_researcher_override(
        tmp_path, monkeypatch, duplicate=False,
    )
    dblp = tmp_path / "profiles" / "academic-researcher" / "source-defaults"
    dblp = dblp / "dblp.yaml"
    dblp.write_text(
        dblp.read_text(encoding="utf-8") + "  enabled_by_defualt: true\n",
        encoding="utf-8",
    )
    docker_calls = _record_docker(researcher_cli, monkeypatch)
    root = tmp_path / "deployments"
    args = cli._build_parser(["install"]).parse_args([
        "install", "--profile", "academic-researcher", "--name", "keyprobe",
        "--deployments-root", str(root), "--no-claude-config", "--dry-run",
    ])
    rc = researcher_cli._cmd_install(args, as_json=True)
    captured = capsys.readouterr()
    assert rc == 1
    assert "Traceback" not in captured.err
    error = json.loads(captured.err)["error"]
    assert error["data"]["code"] == "source_registry_unknown_key"
    assert error["data"]["source"] == "dblp"
    assert error["data"]["key"] == "enabled_by_defualt"
    assert error["data"]["suggestion"] == "enabled_by_default"
    # The error names the file that declares the source, not its directory.
    assert error["data"]["source_default_file"] == str(dblp)
    assert f"registry={dblp} " in error["message"]
    assert docker_calls == []
    assert not (root / "keyprobe").exists()


def test_unknown_document_key_is_refused(tmp_path: Path) -> None:
    reg = _write(tmp_path, "sorces:\n  {}\n")
    with pytest.raises(SourceRegistryUnknownKeyError) as caught:
        load_source_registry(reg)
    assert caught.value.location == "<document>"
    assert caught.value.key == "sorces"
    assert caught.value.suggestion == "sources"


def test_extension_document_key_holds_anchors_and_is_accepted(
    tmp_path: Path,
) -> None:
    reg = _write(
        tmp_path,
        """
        x-defaults:
          jira: &jira
            module: okg.substrate.library.sources.jira
            class: JiraSource
        sources:
          jira:
            <<: *jira
            ownership_id: t.jira
        """,
    )
    entries = load_source_registry(reg)
    assert entries["jira"].raw["class"] == "JiraSource"


@pytest.mark.parametrize(
    ("code_repos", "location", "key", "valid"),
    [
        (
            """
            base: {base}
            include_globs: ["DataFormats/**"]
            repos:
              - {{slug: cmssw, url: https://x/cmssw.git}}
            """,
            "code_repos",
            "include_globs",
            CODE_REPOS_KEYS,
        ),
        (
            """
            base: {base}
            repos:
              - slug: cmssw
                url: https://x/cmssw.git
                include_globs: ["DataFormats/**"]
            """,
            "code_repos.repos[0]",
            "include_globs",
            CODE_REPO_ENTRY_KEYS,
        ),
        (
            """
            base: {base}
            repos:
              - {{slug: cmssw, url: https://x/cmssw.git, tracked_brach: main}}
            """,
            "code_repos.repos[0]",
            "tracked_brach",
            CODE_REPO_ENTRY_KEYS,
        ),
    ],
)
def test_unknown_code_repos_key_is_refused_before_expansion(
    tmp_path: Path,
    code_repos: str,
    location: str,
    key: str,
    valid: frozenset[str],
) -> None:
    body = textwrap.dedent(code_repos).format(base=tmp_path / "repos")
    reg = _write(
        tmp_path,
        "code_repos:\n" + textwrap.indent(body.lstrip(), "  ") + "sources: {}\n",
    )
    with pytest.raises(SourceRegistryUnknownKeyError) as caught:
        load_source_registry(reg)
    assert caught.value.location == location
    assert caught.value.key == key
    assert caught.value.valid == valid
    if key == "include_globs":
        assert "git-files" in str(caught.value)


def test_strict_registry_keeps_its_schema_refusal(tmp_path: Path) -> None:
    reg = _write(
        tmp_path,
        """
        admission_contract: strict_v1
        sources: {}
        sorces: {}
        """,
    )
    with pytest.raises(ValueError, match="source_registry_strict_schema"):
        load_source_registry(reg)


def _tracked_registries() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "*source_registry.yaml"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    return [REPO_ROOT / path for path in out]


def test_every_tracked_registry_carries_only_known_keys() -> None:
    """Every in-repo registry passes the key check the loader now applies."""
    registries = _tracked_registries()
    assert len(registries) >= 19
    for path in registries:
        document = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if "admission_contract" in document:
            continue  # strict_v1: the closed schema already refuses
        validate_legacy_document_keys(document, path)
        sources = expand_code_repos_sources(
            document, path, allow_unresolved_base=True,
        )
        for name, body in sources.items():
            if isinstance(body, dict):
                validate_legacy_entry_keys(name, body, path)


def test_every_shipped_profile_source_default_carries_only_known_keys() -> None:
    """Profile scaffolds render these into a legacy registry."""
    files = sorted(
        (REPO_ROOT / "src" / "okg" / "substrate" / "library" / "profiles")
        .glob("*/source-defaults/*.yaml")
    )
    assert len(files) >= 47  # 47 shipped at a8fcfca0fe
    for path in files:
        document = fastyaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(document, dict), path
        if len(document) == 1 and isinstance(
            next(iter(document.values())), dict,
        ):
            ((name, body),) = document.items()
        else:
            name, body = path.stem, document
        validate_legacy_entry_keys(str(name), body, path)


def test_legacy_code_repos_key_sets_match_the_strict_schema() -> None:
    """Both contracts accept the same code_repos keys, so moving a registry
    to strict_v1 never changes which of them are refused."""
    schema = json.loads(
        (REPO_ROOT / "schemas" / "source_registry.schema.json").read_text(
            encoding="utf-8",
        )
    )
    defs = schema["$defs"]
    assert set(defs["strict_code_repos"]["properties"]) == CODE_REPOS_KEYS
    assert set(defs["strict_code_repo"]["properties"]) == CODE_REPO_ENTRY_KEYS
    per_repo = dict(defs["strict_code_repo"]["properties"]["params"])
    every_repo = dict(defs["strict_code_repos"]["properties"]["params"])
    per_repo.pop("description")
    every_repo.pop("description")
    assert per_repo == every_repo
    # The legacy document schema names the keys the legacy loader accepts.
    legacy = defs["legacy_document"]
    assert set(legacy["properties"]) == {"sources", "code_repos"}
    assert list(legacy["patternProperties"]) == ["^x-"]
    code_repos = legacy["properties"]["code_repos"]
    assert set(code_repos["properties"]) == CODE_REPOS_KEYS
    assert (
        set(code_repos["properties"]["repos"]["items"]["properties"])
        == CODE_REPO_ENTRY_KEYS
    )


@pytest.mark.parametrize(
    ("template", "sources"),
    [
        ("devops", ("jira", "github")),
        ("docs-corpus", ("docsite", "confluence")),
    ],
)
def test_fixture_template_fixture_path_reaches_the_reader(
    template: str, sources: tuple[str, ...],
) -> None:
    """The fixture path used to sit at entry level, where nothing read it, so
    the reader got its default ``""``."""
    from okg.substrate.ingest.adapter_factory import source_adapter_init_params

    path = (
        REPO_ROOT / "src" / "okg" / "substrate" / "library" / "templates"
        / template / "source_registry.yaml"
    )
    entries = load_source_registry(path)
    for name in sources:
        params = source_adapter_init_params(entries[name], dsn="unused")
        fixture = Path(params["fixture_path"])
        assert fixture.is_absolute()
        assert fixture.is_file(), fixture
        assert fixture.parent == path.parent / "fixtures"


def test_status_reports_a_registry_that_does_not_load(tmp_path: Path) -> None:
    """`okg status` used to swallow the refusal into an empty scope."""
    from okg.substrate.cli.main import _collect_status

    _entry_registry(tmp_path, "enabled_by_defualt: true\n")
    snap = _collect_status(
        "postgresql://nobody@127.0.0.1:1/none",
        "keyprobe",
        deployment_dir=tmp_path,
    )
    registry_errors = [
        error for error in snap["errors"]
        if error.get("code") == "source_registry_unknown_key"
    ]
    assert len(registry_errors) == 1
    assert "enabled_by_defualt" in registry_errors[0]["message"]
