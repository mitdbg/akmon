"""Layer 2A — `okg init --profile`, `okg new-module`, retired `okg new-source`.

Coverage matrix:

1. Profile loader (`okg.substrate.catalog.profile`):
   - valid `profile.yaml` loads to a typed `Profile`
   - missing required keys raise `ProfileError` naming the key
   - duplicate question ids raise
   - unknown `init_question.type` raise
   - `interpolate` substitutes `${var}` from answers
   - `interpolate` raises on undefined variable references
   - `validate_answer` enforces type contracts per question

2. `okg init --profile` CLI (`_cmd_init_from_profile`):
   - Fixture profile scaffolds a deployment with the expected
     files; `--no-publish` short-circuits the migrate + first-
     publish steps.
   - The generated `deployment.yaml` carries the `profile:` block.
   - Re-running against an existing deployment refuses without
     `--force --yes`.
   - The composition pass surfaces `skipped_extends` in the
     summary payload AND emits YELLOW warnings.

3. `okg new-module` CLI (`_cmd_new_module`):
   - Default `--in-tree` placement scaffolds under a tmp ontologies
     root (no real in-tree writes during tests).
   - The scaffolded `module.yaml` parses through the manifest
     loader and `composition.resolve(...)` accepts the empty
     `schema.yaml`.
   - Refusal on existing dir unless `--force`.

4. `okg new-source` CLI (`_cmd_new_source`), retired by issue #2479:
   - Refuses with `command_retired`, names the example connector and
     `okg deployment add-source --registration`, and writes no file.
   - Hidden from `okg --help`; `okg new-module` no longer points at it.

5. Doctor profile-awareness (`_doctor_profile_diagnostics`):
   - Deployment without `profile:` block returns None
     (degrades silently).
   - Deployment with `profile:` block and a dropped module
     surfaces a YELLOW finding citing the profile name.
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import okg.substrate.cli.main as cli
from okg.substrate.cli import doctor as cli_doctor
from okg.substrate.catalog import module_scaffold
from okg.substrate.catalog.composition import resolve
from okg.substrate.catalog.module_manifest import load_manifest
from okg.substrate.catalog.profile import (
    INIT_QUESTION_TYPES,
    InitQuestion,
    ProfileError,
    RUNTIME_PLACEHOLDERS,
    interpolate,
    load_profile,
    validate_answer,
)
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    discover_profile,
    scaffold_deployment,
)


# ---------- 1. Profile loader ----------


def _write_profile(
    profile_dir: Path,
    *,
    body: str,
    modules: list[str] | None = None,
    source_defaults: dict[str, str] | None = None,
    invariants: str | None = None,
) -> Path:
    profile_dir.mkdir(parents=True, exist_ok=True)
    path = profile_dir / "profile.yaml"
    path.write_text(textwrap.dedent(body))
    if modules is not None:
        (profile_dir / "modules.yaml").write_text(
            yaml.safe_dump({"modules": modules}),
        )
    if source_defaults:
        sd_dir = profile_dir / "source-defaults"
        sd_dir.mkdir()
        for name, text in source_defaults.items():
            (sd_dir / f"{name}.yaml").write_text(text)
    if invariants is not None:
        (profile_dir / "invariants.yaml").write_text(invariants)
    return path


def test_load_valid_profile(tmp_path) -> None:
    path = _write_profile(
        tmp_path / "demo",
        body="""\
        name: demo
        version: 0.2.0
        description: A demo profile
        init_questions:
          - id: dep_name
            prompt: deployment name
            type: slug
            required: true
          - id: papers_dir
            prompt: papers dir
            type: path
            required: false
            default: /tmp/papers
        modules: modules.yaml
        invariants: invariants.yaml
        """,
        modules=["reasoning"],
        invariants="invariants: []\n",
    )
    profile = load_profile(path)
    assert profile.name == "demo"
    assert profile.version == "0.2.0"
    assert profile.description == "A demo profile"
    assert len(profile.init_questions) == 2
    assert profile.init_questions[0].id == "dep_name"
    assert profile.init_questions[0].type == "slug"
    assert profile.init_questions[0].required is True
    assert profile.init_questions[1].default == "/tmp/papers"
    assert profile.modules_file == (tmp_path / "demo" / "modules.yaml").resolve()
    assert profile.invariants_file == (
        tmp_path / "demo" / "invariants.yaml"
    ).resolve()
    assert profile.path == path.resolve()


def test_load_missing_required_keys(tmp_path) -> None:
    path = _write_profile(
        tmp_path / "noversion",
        body="""\
        name: noversion
        """,
    )
    with pytest.raises(ProfileError) as exc:
        load_profile(path)
    assert exc.value.data["key"] == "version"
    assert str(path) in str(exc.value)


def test_load_missing_name(tmp_path) -> None:
    path = _write_profile(
        tmp_path / "nameless",
        body="""\
        version: 1.0.0
        """,
    )
    with pytest.raises(ProfileError) as exc:
        load_profile(path)
    assert exc.value.data["key"] == "name"


def test_duplicate_question_id_rejected(tmp_path) -> None:
    path = _write_profile(
        tmp_path / "dup",
        body="""\
        name: dup
        version: 1.0.0
        init_questions:
          - id: x
            prompt: first
            type: string
          - id: x
            prompt: second
            type: string
        """,
    )
    with pytest.raises(ProfileError) as exc:
        load_profile(path)
    assert exc.value.data["duplicate_id"] == "x"


def test_unknown_question_type_rejected(tmp_path) -> None:
    path = _write_profile(
        tmp_path / "badtype",
        body="""\
        name: badtype
        version: 1.0.0
        init_questions:
          - id: foo
            prompt: foo
            type: not_a_real_type
        """,
    )
    with pytest.raises(ProfileError) as exc:
        load_profile(path)
    assert "not_a_real_type" in str(exc.value)


def test_init_question_types_present() -> None:
    """Sanity: the documented types are exposed."""
    for t in ("slug", "path", "path_list", "orcid", "string", "integer"):
        assert t in INIT_QUESTION_TYPES


# ---------- interpolation ----------


def test_interpolate_substitutes_simple_var() -> None:
    assert interpolate("hello ${name}", {"name": "world"}) == "hello world"


def test_interpolate_substitutes_path_list_joined() -> None:
    out = interpolate("dirs=${dirs}", {"dirs": ["/a", "/b"]})
    assert out == "dirs=/a, /b"


def test_interpolate_undefined_raises() -> None:
    with pytest.raises(ProfileError) as exc:
        interpolate("a ${missing} b", {})
    assert "missing" in str(exc.value)


def test_interpolate_no_recursion() -> None:
    # `${b}` in the answer for `a` is NOT re-interpolated.
    out = interpolate("${a}", {"a": "x${b}y", "b": "Z"})
    assert out == "x${b}y"


@pytest.mark.parametrize("name", sorted(RUNTIME_PLACEHOLDERS))
def test_interpolate_keeps_runtime_placeholder(name: str) -> None:
    text = f"path: ${{{name}}}/extractors"
    assert interpolate(text, {}, keep_runtime=True) == text


@pytest.mark.parametrize("name", sorted(RUNTIME_PLACEHOLDERS))
def test_interpolate_refuses_runtime_placeholder_by_default(name: str) -> None:
    with pytest.raises(ProfileError) as exc:
        interpolate(f"path: ${{{name}}}/x", {})
    assert name in str(exc.value)


def test_interpolate_answer_overrides_runtime_placeholder() -> None:
    assert interpolate("${repo_root}/x", {"repo_root": "/r"}, keep_runtime=True) == "/r/x"


def test_interpolate_unknown_placeholder_next_to_runtime_one_raises() -> None:
    with pytest.raises(ProfileError) as exc:
        interpolate("${deployment_dir}/${foo}", {}, keep_runtime=True)
    assert "foo" in str(exc.value)


def test_runtime_placeholders_are_all_expanded_by_registry_loader(tmp_path: Path) -> None:
    from okg.substrate.sources.registry import _substitute_registry_placeholders

    registry = tmp_path / "deployments" / "d" / "source_registry.yaml"
    registry.parent.mkdir(parents=True)
    for name in RUNTIME_PLACEHOLDERS:
        token = f"${{{name}}}"
        assert _substitute_registry_placeholders(token, registry) != token, name
    assert _substitute_registry_placeholders("${foo}", registry) == "${foo}"


def test_installed_registry_resolves_runtime_placeholder_at_load(
    tmp_path: Path,
) -> None:
    from okg.substrate.sources.registry import load_source_registry

    profile_dir = tmp_path / "anchor-profile"
    _write_profile(
        profile_dir,
        body="""\
        name: anchor-profile
        version: 0.1.0
        init_questions:
          - id: deployment_name
            prompt: deployment name
            type: slug
            required: true
        modules: modules.yaml
        source_defaults: source-defaults/
        """,
        modules=["reasoning"],
        source_defaults={
            "papers": (
                "class: example.PapersSource\n"
                "ownership_id: papers\n"
                "params:\n"
                "  extractors_dir: ${deployment_dir}/extractors\n"
            ),
        },
    )
    plan = build_plan(
        profile=load_profile(profile_dir / "profile.yaml"),
        deployment_name="anchored",
        deployments_root=tmp_path / "deployments",
        raw_answers={"deployment_name": "anchored"},
    )
    assert "${deployment_dir}/extractors" in plan.source_defaults["papers"]
    result = scaffold_deployment(plan)
    registry = result.deployment_dir / "source_registry.yaml"
    assert "${deployment_dir}/extractors" in registry.read_text()
    entries = load_source_registry(registry)
    resolved = entries["papers"].raw["params"]["extractors_dir"]
    assert resolved == f"{result.deployment_dir.resolve()}/extractors"


def _plan_with_deployment_defaults(tmp_path: Path, defaults: str):
    profile_dir = tmp_path / "dd-profile"
    _write_profile(
        profile_dir,
        body="""\
        name: dd-profile
        version: 0.1.0
        init_questions:
          - id: deployment_name
            prompt: deployment name
            type: slug
            required: true
        modules: modules.yaml
        deployment_defaults: deployment-defaults.yaml
        """,
        modules=["reasoning"],
    )
    (profile_dir / "deployment-defaults.yaml").write_text(defaults)
    return build_plan(
        profile=load_profile(profile_dir / "profile.yaml"),
        deployment_name="dd",
        deployments_root=tmp_path / "deployments",
        raw_answers={"deployment_name": "dd"},
    )


@pytest.mark.parametrize("name", sorted(RUNTIME_PLACEHOLDERS))
def test_deployment_defaults_still_refuse_runtime_placeholder(
    tmp_path: Path, name: str,
) -> None:
    with pytest.raises(ProfileError) as exc:
        _plan_with_deployment_defaults(
            tmp_path, f"taxonomy: ${{{name}}}/taxonomy.yaml\n",
        )
    assert name in str(exc.value)


# ---------- validate_answer ----------


def _q(id="x", type="string", required=False, default=None) -> InitQuestion:
    return InitQuestion(id=id, prompt="?", type=type, required=required,
                        default=default)


def test_validate_answer_slug_ok() -> None:
    assert validate_answer(_q(type="slug"), "my-dep_01") == "my-dep_01"


def test_validate_answer_slug_rejects_uppercase() -> None:
    with pytest.raises(ProfileError):
        validate_answer(_q(type="slug"), "BadSlug")


def test_validate_answer_path_list_string() -> None:
    out = validate_answer(_q(type="path_list"), "/a, /b, /c")
    assert out == ["/a", "/b", "/c"]


def test_validate_answer_orcid_ok() -> None:
    assert validate_answer(
        _q(type="orcid"), "0000-0001-2345-678X",
    ) == "0000-0001-2345-678X"


def test_validate_answer_orcid_rejects_short() -> None:
    with pytest.raises(ProfileError):
        validate_answer(_q(type="orcid"), "abc")


def test_validate_answer_integer_ok() -> None:
    assert validate_answer(_q(type="integer"), "42") == 42


def test_validate_answer_required_unanswered_raises() -> None:
    with pytest.raises(ProfileError) as exc:
        validate_answer(_q(required=True), None)
    assert "required" in str(exc.value)


def test_validate_answer_optional_returns_default() -> None:
    assert validate_answer(_q(default="z"), None) == "z"


# ---------- 2. okg init --profile (end-to-end against fixture) ----------


@pytest.fixture
def fixture_profile_dir(tmp_path) -> Path:
    """Minimal in-process profile fixture with one source-default
    so we exercise the interpolation + scaffold paths without
    relying on any real-source dependencies.

    The `deployment_name` init_question id matches the convention
    in `design.md`'s canonical example AND aligns with the CLI's
    `--deployment-name` flag (the gather step aliases the two)."""
    profile_dir = tmp_path / "fixture"
    _write_profile(
        profile_dir,
        body="""\
        name: fixture
        version: 0.1.0
        description: in-test profile
        init_questions:
          - id: deployment_name
            prompt: deployment name
            type: slug
            required: true
          - id: papers_dir
            prompt: papers dir
            type: path
            required: false
            default: /tmp/none
        modules: modules.yaml
        source_defaults: source-defaults/
        invariants: invariants.yaml
        """,
        modules=["reasoning"],
        source_defaults={
            "papers": (
                "class: example.PapersSource\n"
                "params:\n"
                "  paths:\n"
                "    - ${papers_dir}\n"
            ),
        },
        invariants="invariants: []\n",
    )
    return profile_dir


def test_build_plan_interpolates_source_defaults(
    fixture_profile_dir, tmp_path,
) -> None:
    profile = load_profile(fixture_profile_dir / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="my_dep",
        deployments_root=tmp_path / "deployments",
        raw_answers={"deployment_name": "my_dep", "papers_dir": "/home/x/papers"},
    )
    body = plan.source_defaults["papers"]
    assert "/home/x/papers" in body
    assert "${papers_dir}" not in body


def test_scaffold_deployment_writes_expected_files(
    fixture_profile_dir, tmp_path,
) -> None:
    profile = load_profile(fixture_profile_dir / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="alpha",
        deployments_root=tmp_path / "deployments",
        raw_answers={"deployment_name": "alpha", "papers_dir": "/tmp/p"},
    )
    result = scaffold_deployment(plan)
    deployment_yaml = (result.deployment_dir / "deployment.yaml").read_text()
    parsed = yaml.safe_load(deployment_yaml)
    assert parsed["name"] == "alpha"
    assert parsed["profile"]["name"] == "fixture"
    assert parsed["profile"]["version"] == "0.1.0"
    assert "initialized_at" in parsed["profile"]
    assert parsed["profile"]["init_answers"]["deployment_name"] == "alpha"
    assert parsed["modules"] == ["reasoning"]
    sr_yaml = (result.deployment_dir / "source_registry.yaml").read_text()
    sr_parsed = yaml.safe_load(sr_yaml)
    assert "papers" in sr_parsed["sources"]
    assert "/tmp/p" in sr_yaml
    assert (result.deployment_dir / "invariants.yaml").is_file()


def test_scaffold_deployment_redacts_secret_init_answer_provenance(
    tmp_path: Path,
) -> None:
    path = _write_profile(
        tmp_path / "secret-profile",
        body="""\
        name: secret-profile
        version: 0.1.0
        init_questions:
          - id: postgres_dsn
            prompt: postgres dsn
            type: string
            required: true
          - id: api_key
            prompt: api key
            type: string
            required: true
          - id: repo_root
            prompt: repo root
            type: path
            required: true
        modules: modules.yaml
        source_defaults: source-defaults/
        """,
        modules=["git_graph"],
        source_defaults={
            "git_files": """\
            git_files:
              module: okg.substrate.library.sources.git_graph_files
              class: GitGraphFilesSource
              params:
                repo_root: ${repo_root}
                endpoint: ${postgres_dsn}
            """,
        },
    )
    plan = build_plan(
        profile=load_profile(path),
        deployment_name="secret_demo",
        deployments_root=tmp_path / "deployments",
        raw_answers={
            "postgres_dsn": (
                "postgres://postgres:literal-secret@localhost:5432/okg"
            ),
            "api_key": "ghp_abcdefghijklmnopqrstuvwxyz123456",
            "repo_root": "/tmp/repo",
        },
    )

    result = scaffold_deployment(plan)
    deployment_yaml = (result.deployment_dir / "deployment.yaml").read_text()
    parsed = yaml.safe_load(deployment_yaml)

    assert parsed["postgres"]["dsn"] == (
        "postgres://postgres:literal-secret@localhost:5432/okg"
    )
    answers = parsed["profile"]["init_answers"]
    assert answers["postgres_dsn"] == (
        "postgres://postgres:***@localhost:5432/okg"
    )
    assert answers["api_key"] == "<redacted>"
    assert answers["repo_root"] == "/tmp/repo"
    assert parsed["profile"]["classification"] == "example"
    components = parsed["profile"]["components"]
    assert components["modules"][0]["name"] == "git_graph"
    assert components["source_packs"][0]["name"] == "git_files"
    assert "override_points" in components
    assert "literal-secret" in (
        result.deployment_dir / "source_registry.yaml"
    ).read_text()


def test_init_profile_json_redacts_secret_answers(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    profiles_root = tmp_path / "profiles"
    path = _write_profile(
        profiles_root / "secret-profile",
        body="""\
        name: secret-profile
        version: 0.1.0
        init_questions:
          - id: postgres_dsn
            prompt: postgres dsn
            type: string
            required: true
          - id: api_key
            prompt: api key
            type: string
            required: true
          - id: repo_root
            prompt: repo root
            type: path
            required: true
        modules: modules.yaml
        """,
        modules=["git_graph"],
    )
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profiles_root))
    args = SimpleNamespace(
        profile_name="secret-profile",
        deployment_name="secret_json",
        deployments_root=str(tmp_path / "deployments"),
        force=False,
        yes=False,
        no_publish=True,
        non_interactive=True,
        _profile_flag_map={
            "postgres_dsn": "answer_postgres_dsn",
            "api_key": "answer_api_key",
            "repo_root": "answer_repo_root",
        },
        answer_postgres_dsn=(
            "postgres://postgres:literal-secret@localhost:5432/okg"
        ),
        answer_api_key="ghp_abcdefghijklmnopqrstuvwxyz123456",
        answer_repo_root="/tmp/repo",
    )

    rc = cli._cmd_init(args, as_json=True)

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["answers"]["postgres_dsn"] == (
        "postgres://postgres:***@localhost:5432/okg"
    )
    assert payload["answers"]["api_key"] == "<redacted>"
    assert payload["answers"]["repo_root"] == "/tmp/repo"
    assert "literal-secret" not in json.dumps(payload)
    assert path.is_file()


def test_scaffold_deployment_restricts_config_permissions(
    fixture_profile_dir, tmp_path,
) -> None:
    """Per `harden-okg-network-and-secrets`: deployment.yaml carries
    the Postgres DSN+password, so the scaffold dir SHALL be 0700 and
    the credential-bearing files 0600 — not the world-readable
    umask default a local user/process could read the secret out of.
    """
    import os
    import stat

    profile = load_profile(fixture_profile_dir / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="perm",
        deployments_root=tmp_path / "deployments",
        raw_answers={"deployment_name": "perm", "papers_dir": "/tmp/p"},
    )
    result = scaffold_deployment(plan)
    dir_mode = stat.S_IMODE(os.stat(result.deployment_dir).st_mode)
    dep_mode = stat.S_IMODE(
        os.stat(result.deployment_dir / "deployment.yaml").st_mode
    )
    sr_mode = stat.S_IMODE(
        os.stat(result.deployment_dir / "source_registry.yaml").st_mode
    )
    assert dir_mode == 0o700, oct(dir_mode)
    assert dep_mode == 0o600, oct(dep_mode)
    assert sr_mode == 0o600, oct(sr_mode)


def test_scaffold_deployment_refuses_existing_dir(
    fixture_profile_dir, tmp_path,
) -> None:
    profile = load_profile(fixture_profile_dir / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="beta",
        deployments_root=tmp_path / "deployments",
        raw_answers={"deployment_name": "beta", "papers_dir": "/tmp/p"},
    )
    scaffold_deployment(plan)
    # second call without --force raises
    from okg.substrate.errors import SubstrateError
    with pytest.raises(SubstrateError) as exc:
        scaffold_deployment(plan)
    assert "already exists" in str(exc.value)


def test_scaffold_deployment_force_overwrites(
    fixture_profile_dir, tmp_path,
) -> None:
    profile = load_profile(fixture_profile_dir / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="gamma",
        deployments_root=tmp_path / "deployments",
        raw_answers={"deployment_name": "gamma", "papers_dir": "/tmp/p"},
    )
    scaffold_deployment(plan)
    # Drop a stray file to confirm rmtree happens.
    stray = plan.deployment_dir / "stray.txt"
    stray.write_text("delete me")
    result = scaffold_deployment(plan, force=True)
    assert not stray.exists()
    assert (result.deployment_dir / "deployment.yaml").is_file()


def test_discover_profile_via_explicit_root(
    fixture_profile_dir, tmp_path,
) -> None:
    profile = discover_profile(
        "fixture", profiles_root=fixture_profile_dir.parent,
    )
    assert profile.name == "fixture"


def test_discover_profile_finds_shipped_test_profile() -> None:
    """The `_test` profile shipped with the substrate library is
    discoverable without any env var override (in-tree fallback)."""
    profile = discover_profile("_test")
    assert profile.name == "_test"
    assert profile.version == "0.1.0"


def test_discover_profile_missing_raises(tmp_path) -> None:
    # discover_profile raises ProfileFetchError when the fetch cache
    # is empty/absent (the CI and clean-machine default) and
    # ProfileError when the cache is populated but lacks the name;
    # both are SubstrateError subclasses. Assert the common base so
    # the test is robust to fetch-cache state.
    from okg.substrate.errors import SubstrateError
    with pytest.raises(SubstrateError):
        discover_profile(
            "_definitely_not_a_profile", profiles_root=tmp_path,
        )


def test_cmd_init_from_profile_no_publish(
    fixture_profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """End-to-end via the CLI dispatcher: --profile + --no-publish
    writes the deployment files and prints a JSON envelope."""
    monkeypatch.setenv("OKG_PROFILES_DIR", str(fixture_profile_dir.parent))
    args = SimpleNamespace(
        command="init",
        profile_name="fixture",
        template=None,
        deployment_name="cli_dep",
        deployments_root=str(tmp_path / "deployments"),
        force=False,
        yes=False,
        no_publish=True,
        non_interactive=True,
        target=None,
        repo_root=None,
        openspec_root=None,
        claude_projects_root=None,
        notes_root=None,
        since=None,
    )
    rc = cli._cmd_init(args, as_json=True)
    assert rc == 0
    payload = capsys.readouterr().out
    assert '"deployment"' in payload
    assert "cli_dep" in payload
    dep_dir = tmp_path / "deployments" / "cli_dep"
    assert (dep_dir / "deployment.yaml").is_file()
    assert (dep_dir / "source_registry.yaml").is_file()


def test_cmd_init_from_profile_refuses_without_yes(
    fixture_profile_dir, tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_PROFILES_DIR", str(fixture_profile_dir.parent))
    args = SimpleNamespace(
        command="init",
        profile_name="fixture",
        template=None,
        deployment_name="cli_dep_no_yes",
        deployments_root=str(tmp_path / "deployments"),
        force=True,
        yes=False,
        no_publish=True,
        non_interactive=True,
        target=None, repo_root=None, openspec_root=None,
        claude_projects_root=None, notes_root=None, since=None,
    )
    rc = cli._cmd_init(args, as_json=True)
    assert rc != 0


def test_cmd_init_from_profile_refuses_overwrite(
    fixture_profile_dir, tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_PROFILES_DIR", str(fixture_profile_dir.parent))
    base_args = dict(
        command="init",
        profile_name="fixture",
        template=None,
        deployment_name="cli_dep_overwrite",
        deployments_root=str(tmp_path / "deployments"),
        force=False, yes=False, no_publish=True, non_interactive=True,
        target=None, repo_root=None, openspec_root=None,
        claude_projects_root=None, notes_root=None, since=None,
    )
    rc1 = cli._cmd_init(SimpleNamespace(**base_args), as_json=True)
    assert rc1 == 0
    rc2 = cli._cmd_init(SimpleNamespace(**base_args), as_json=True)
    assert rc2 != 0  # second invocation refuses


# ---------- 3. okg new-module ----------


def test_scaffold_module_writes_expected_files(tmp_path) -> None:
    target = tmp_path / "wet_lab"
    result = module_scaffold.scaffold_module(
        "wet_lab",
        module_dir=target,
        package_path="example.modules.wet_lab",
    )
    names = {p.name for p in result.files_written}
    assert "module.yaml" in names
    assert "schema.yaml" in names
    assert "__init__.py" in names
    assert "test_schema.py" in names
    assert "README.md" in names


def test_scaffold_module_yields_valid_manifest(tmp_path) -> None:
    target = tmp_path / "wet_lab"
    module_scaffold.scaffold_module(
        "wet_lab",
        module_dir=target,
        package_path="example.modules.wet_lab",
    )
    manifest = load_manifest(target / "module.yaml")
    assert manifest.name == "wet_lab"
    assert manifest.version == "0.1.0"
    assert manifest.provides.subtypes_file == "schema.yaml"
    # The composition resolver MUST accept the empty schema
    # (verified in design.md "Scaffold minimalism").
    resolved = resolve(["wet_lab"], modules_root=tmp_path)
    assert "wet_lab" in resolved.loaded_modules


def test_scaffold_module_refuses_existing_dir(tmp_path) -> None:
    target = tmp_path / "wet_lab"
    module_scaffold.scaffold_module(
        "wet_lab", module_dir=target, package_path="example.x",
    )
    from okg.substrate.errors import SubstrateError
    with pytest.raises(SubstrateError):
        module_scaffold.scaffold_module(
            "wet_lab", module_dir=target, package_path="example.x",
        )


def test_scaffold_module_force_overwrites(tmp_path) -> None:
    target = tmp_path / "wet_lab"
    module_scaffold.scaffold_module(
        "wet_lab", module_dir=target, package_path="example.x",
    )
    module_scaffold.scaffold_module(
        "wet_lab", module_dir=target, package_path="example.x", force=True,
    )
    # Sanity: still valid after force-overwrite.
    manifest = load_manifest(target / "module.yaml")
    assert manifest.name == "wet_lab"


def _deployment(root, name: str = "my_dep"):
    """A minimal deployment folder: `new-module --deployment` needs one."""
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "deployment.yaml").write_text(f"name: {name}\n")
    return path


def test_cmd_new_module_in_deployment_mode(tmp_path, capsys) -> None:
    _deployment(tmp_path / "deployments")
    args = SimpleNamespace(
        command="new-module",
        name="wet_lab",
        deployment="my_dep",
        deployments_root=str(tmp_path / "deployments"),
        in_tree=False,
        force=False,
    )
    rc = cli._cmd_new_module(args, as_json=True)
    assert rc == 0
    target = tmp_path / "deployments" / "my_dep" / "modules" / "wet_lab"
    assert (target / "module.yaml").is_file()


def test_cmd_new_module_writes_into_the_deployments_own_folder(
    tmp_path, capsys, monkeypatch,
) -> None:
    """#3417: `--deployment <name>` finds the deployment the way other
    commands do, not `./deployments/` under the current directory."""
    installed = tmp_path / "installed"
    dep = _deployment(installed)
    elsewhere = tmp_path / "checkout"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(installed))
    args = SimpleNamespace(
        command="new-module", name="wet_lab", deployment="my_dep",
        deployments_root=None, in_tree=False, force=False,
    )
    rc = cli._cmd_new_module(args, as_json=True)
    assert rc == 0, capsys.readouterr()
    assert (dep / "modules" / "wet_lab" / "module.yaml").is_file()
    assert not (elsewhere / "deployments").exists()


def test_cmd_new_module_refuses_an_unknown_deployment(tmp_path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "none"))
    args = SimpleNamespace(
        command="new-module", name="wet_lab", deployment="no_such_dep",
        deployments_root=None, in_tree=False, force=False,
    )
    rc = cli._cmd_new_module(args, as_json=True)
    assert rc != 0
    captured = capsys.readouterr()
    assert "no_such_dep" in captured.out + captured.err
    assert not (tmp_path / "deployments").exists()


def test_cmd_new_module_refuses_a_file_that_is_not_a_deployment(tmp_path, capsys, monkeypatch) -> None:
    """Any existing file resolves; only a deployment.yaml names a deployment."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "README.md").write_text("hello\n")
    args = SimpleNamespace(
        command="new-module", name="wet_lab", deployment="README.md",
        deployments_root=None, in_tree=False, force=False,
    )
    rc = cli._cmd_new_module(args, as_json=True)
    assert rc != 0
    assert not (tmp_path / "modules").exists()


def test_cmd_new_module_rejects_invalid_name(tmp_path, capsys) -> None:
    args = SimpleNamespace(
        command="new-module",
        name="not a name!",
        deployment="my_dep",
        deployments_root=str(tmp_path / "deployments"),
        in_tree=False,
        force=False,
    )
    rc = cli._cmd_new_module(args, as_json=True)
    assert rc != 0


# ---------- 4. okg new-source (retired, issue #2479) ----------


REPO_ROOT = Path(__file__).resolve().parents[3]


def _tree(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _scaffold_wet_lab(tmp_path: Path) -> Path:
    deployments_root = tmp_path / "deployments"
    mod_dir = deployments_root / "my_dep" / "modules" / "wet_lab"
    module_scaffold.scaffold_module(
        "wet_lab",
        module_dir=mod_dir,
        package_path="my_dep.modules.wet_lab",
    )
    return deployments_root


def test_new_source_refuses_with_replacement_and_writes_nothing(
    tmp_path, monkeypatch, capsys,
) -> None:
    """Failure case: the full old invocation now refuses and writes nothing.

    Before issue #2479 this call wrote `sources/lims_export.py`, a strict
    xfail test, a fixture and a `module.yaml` entry for a reader that did
    not work.
    """
    deployments_root = _scaffold_wet_lab(tmp_path)
    monkeypatch.chdir(tmp_path)
    before = _tree(tmp_path)

    rc = cli.main([
        "new-source", "lims_export",
        "--module", "wet_lab",
        "--kind", "record_set",
        "--deployment", "my_dep",
        "--deployments-root", str(deployments_root),
        "--json",
    ])

    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    error = json.loads(captured.err.strip().splitlines()[-1])["error"]
    assert error["code"] == "command_retired"
    assert "Nothing was written" in error["message"]
    assert error["data"]["command"] == "new-source"
    assert error["data"]["example"] == "examples/json-record-connector"
    replacement = error["data"]["replacement"]
    assert error["fix_hint"] == replacement
    assert "examples/json-record-connector/" in replacement
    assert "okg deployment add-source" in replacement
    assert "--registration" in replacement
    assert _tree(tmp_path) == before
    assert not (
        deployments_root / "my_dep" / "modules" / "wet_lab" / "sources"
    ).exists()


def test_bare_new_source_refuses_in_text_mode(
    tmp_path, monkeypatch, capsys,
) -> None:
    """A bare call gets the same typed refusal, not an argparse usage error."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OKG_AGENT", raising=False)

    rc = cli.main(["new-source"])

    assert rc == 1
    err = capsys.readouterr().err
    assert "error [command_retired]:" in err
    assert "examples/json-record-connector/" in err
    assert "--registration" in err
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "argv",
    [
        ["new-source", "a", "b"],
        ["new-source", "x", "--typo"],
        ["new-source", "x", "--module"],
        ["new-source", "--kind", "not_a_kind", "--json"],
    ],
    ids=["extra-positional", "unknown-flag", "flag-missing-value", "bad-kind"],
)
def test_new_source_refuses_any_argument_list(
    argv, tmp_path, monkeypatch, capsys,
) -> None:
    """Failure case: argparse never exits 2 first; every call refuses."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OKG_AGENT", raising=False)

    rc = cli.main(argv)

    assert rc == 1
    err = capsys.readouterr().err
    if "--json" in argv:
        assert json.loads(err.strip().splitlines()[-1])["error"]["code"] == (
            "command_retired"
        )
    else:
        assert "error [command_retired]:" in err
    assert list(tmp_path.iterdir()) == []


def test_new_source_replacement_names_real_paths(capsys) -> None:
    """The refusal points at an example and a flag that exist."""
    from okg.substrate.cli import init as cli_init

    example = REPO_ROOT / cli_init.NEW_SOURCE_EXAMPLE
    assert (example / "README.md").is_file()
    assert (example / "pyproject.toml").is_file()
    capsys.readouterr()
    with pytest.raises(SystemExit) as exc:
        cli.main(["deployment", "add-source", "--help"])
    assert exc.value.code == 0
    assert "--registration" in capsys.readouterr().out


def test_new_source_is_hidden_from_help_but_answers_help(capsys) -> None:
    parser = cli._build_parser()
    listing = parser.format_help()
    assert "new-source" not in listing
    assert "new-source" not in (parser.epilog or "")

    with pytest.raises(SystemExit) as exc:
        cli.main(["new-source", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "Retired" in out
    assert "examples/json-record-connector/" in out


def test_new_module_no_longer_points_at_new_source(tmp_path, capsys) -> None:
    deployments_root = tmp_path / "deployments"
    _deployment(deployments_root)
    args = SimpleNamespace(
        command="new-module",
        name="wet_lab",
        deployment="my_dep",
        deployments_root=str(deployments_root),
        in_tree=False,
        force=False,
    )
    rc = cli._cmd_new_module(args, as_json=True)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert not any("new-source" in step for step in payload["next"])
    readme = (
        deployments_root / "my_dep" / "modules" / "wet_lab" / "README.md"
    ).read_text()
    assert "new-source" not in readme
    assert "examples/json-record-connector/" in readme
    assert not hasattr(module_scaffold, "scaffold_source")


# ---------- 5. Doctor profile-awareness ----------


def test_doctor_diag_returns_none_without_profile_block(
    tmp_path, monkeypatch,
) -> None:
    """Deployment.yaml without a `profile:` block -> degrades silently."""
    dep_dir = tmp_path / "deployments" / "no_profile_dep"
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "no_profile_dep",
            "description": "manual deployment",
            "modules": ["reasoning"],
            "source_registry": "source_registry.yaml",
        }),
    )

    def fake_find(name):
        if name == "no_profile_dep":
            return dep_dir
        return None

    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir", fake_find,
    )
    diag = cli_doctor._doctor_profile_diagnostics("no_profile_dep")
    assert diag is None


def test_doctor_diag_flags_dropped_module(
    tmp_path, monkeypatch, fixture_profile_dir,
) -> None:
    """`profile:` block + missing module -> YELLOW finding."""
    dep_dir = tmp_path / "deployments" / "test_init"
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "test_init",
            "description": "scaffolded",
            "profile": {
                "name": "fixture",
                "version": "0.1.0",
                "initialized_at": "2026-01-01T00:00:00+00:00",
                "init_answers": {"deployment_name": "test_init"},
            },
            # NB: profile expects ['reasoning'] but deployment
            # has none — that's the YELLOW finding under test.
            "modules": [],
            "source_registry": "source_registry.yaml",
        }),
    )
    monkeypatch.setenv(
        "OKG_PROFILES_DIR", str(fixture_profile_dir.parent),
    )

    def fake_find(name):
        if name == "test_init":
            return dep_dir
        return None

    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir", fake_find,
    )

    diag = cli_doctor._doctor_profile_diagnostics("test_init")
    assert diag is not None
    assert diag["profile"]["name"] == "fixture"
    assert "reasoning" in diag["modules"]["dropped"]
    yellows = diag["yellow_findings"]
    assert any(
        y["code"] == "profile_dropped_module" and y["module"] == "reasoning"
        for y in yellows
    )


def test_doctor_diag_handles_missing_profile_gracefully(
    tmp_path, monkeypatch,
) -> None:
    """If the profile referenced by deployment.yaml is unreadable,
    surface a YELLOW finding (don't crash)."""
    dep_dir = tmp_path / "deployments" / "dangling"
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "dangling",
            "profile": {
                "name": "ghost_profile",
                "version": "0.0.1",
            },
            "modules": [],
        }),
    )
    monkeypatch.setenv("OKG_PROFILES_DIR", str(tmp_path / "no_profiles_dir"))

    def fake_find(name):
        return dep_dir if name == "dangling" else None

    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir", fake_find,
    )
    diag = cli_doctor._doctor_profile_diagnostics("dangling")
    assert diag is not None
    assert diag["profile"]["name"] == "ghost_profile"
    assert any(
        y["code"] == "profile_unreadable"
        for y in diag["yellow_findings"]
    )
