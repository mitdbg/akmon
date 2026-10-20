"""Runtime origins must describe the verified installation, not an unused copy."""
from importlib.util import module_from_spec, spec_from_file_location
from dataclasses import replace

import pytest

from okg.substrate.distributions.packaged_release import (
    PackagedReleaseIdentity, verify_loaded_package,
)
from okg.substrate.errors import SubstrateError
from tests.substrate._fake_credentials import fake_credential
from tests.substrate.distributions.test_installed_wheel import installation  # noqa: F401  (pytest fixture)


def _raise(exc: BaseException, cause: BaseException | None = None) -> None:
    """Raise inside a ``with`` body through a call, so static analysis does not
    read the statements after the block as unreachable. ``cause`` preserves
    explicit exception chaining."""
    if cause is not None:
        raise exc from cause
    raise exc


@pytest.fixture(autouse=True)
def supported_imports(monkeypatch):
    import sys
    from importlib.machinery import BuiltinImporter, FrozenImporter, PathFinder
    from okg.substrate.distributions.packaged_release import _FILE_HOOK
    monkeypatch.setattr(sys, "meta_path", [BuiltinImporter, FrozenImporter, PathFinder])
    monkeypatch.setattr(sys, "path_hooks", [_FILE_HOOK])
    monkeypatch.setattr(sys, "path_importer_cache", {})


def test_identity_roundtrip_and_drift_refusal():
    identity = PackagedReleaseIdentity(*(["sha256:" + "a" * 64] * 4), "archi-profile/v1")
    assert PackagedReleaseIdentity.from_metadata(identity.to_dict()) == identity
    identity.require_same(identity)
    with pytest.raises(SubstrateError, match="verification refused"):
        identity.require_same(replace(identity, configuration="sha256:" + "b" * 64))
    for bad in ({}, {**identity.to_dict(), "core_revision": "a" * 40},
                {**identity.to_dict(), "framework_wheel": "a" * 40}):
        with pytest.raises(SubstrateError):
            PackagedReleaseIdentity.from_metadata(bad)


def test_loaded_origin_must_match_verified_wheel(installation, tmp_path):  # noqa: F811
    wheel, pin, root = installation
    path = root / "archi" / "__init__.py"
    module = module_from_spec(spec_from_file_location("archi", path))
    kwargs = dict(wheel_path=wheel, expected_wheel_digest=pin, installed_root=root)
    assert verify_loaded_package("archi", modules={"archi": module}, **kwargs).wheel_digest == pin
    shadow = tmp_path / "shadow" / "archi"
    shadow.mkdir(parents=True)
    (shadow / "__init__.py").write_text("raise AssertionError('must not execute')")
    other = module_from_spec(spec_from_file_location("archi", shadow / "__init__.py"))
    with pytest.raises(SubstrateError):
        verify_loaded_package("archi", modules={"archi": other}, **kwargs)
    module.__path__.append(str(shadow))
    with pytest.raises(SubstrateError):
        verify_loaded_package("archi", modules={"archi": module}, **kwargs)


def test_prospective_package_does_not_execute_initializer(installation, monkeypatch):  # noqa: F811
    import sys
    from okg.substrate.distributions.packaged_release import verify_prospective_package

    wheel, pin, root = installation
    monkeypatch.delitem(sys.modules, "archi", raising=False)
    from importlib.machinery import BuiltinImporter, FrozenImporter, PathFinder
    monkeypatch.setattr(sys, "meta_path", [BuiltinImporter, FrozenImporter, PathFinder])
    monkeypatch.setattr(sys, "path", [str(root)])
    result = verify_prospective_package(
        "archi", wheel_path=wheel, expected_wheel_digest=pin, installed_root=root,
    )
    assert result.wheel_digest == pin
    assert "archi" not in sys.modules
    monkeypatch.setattr(sys, "path", [])
    with pytest.raises(SubstrateError):
        verify_prospective_package(
            "archi", wheel_path=wheel, expected_wheel_digest=pin, installed_root=root,
        )


def test_plan_and_bundle_are_transitively_bound():
    from okg.substrate.distributions.install_plan import (
        INSTALL_PLAN_SCHEMA, INSTALL_LOCK_SCHEMA, _digest_without,
    )
    from okg.substrate.distributions.packaged_release import verify_plan_binding
    plan = {"schema": INSTALL_PLAN_SCHEMA, "bundle": {"bundle_id": "cern-team"}}
    plan["plan_digest"] = _digest_without(plan, "plan_digest")
    lock = {"schema": INSTALL_LOCK_SCHEMA, "bundle_id": "cern-team", "plan_digest": plan["plan_digest"]}
    lock["lock_digest"] = _digest_without(lock, "lock_digest")
    identity = PackagedReleaseIdentity(
        "sha256:" + "a" * 64, "sha256:" + "b" * 64,
        "sha256:" + "c" * 64, lock["lock_digest"], "archi-profile/v1",
    )
    verify_plan_binding(identity, plan, lock)
    plan["bundle"]["bundle_id"] = "different"
    with pytest.raises(SubstrateError):
        verify_plan_binding(identity, plan, lock)


def test_loaded_name_cannot_claim_another_verified_member(installation):  # noqa: F811
    wheel, pin, root = installation
    path = root / "archi" / "__init__.py"
    modules = {
        name: module_from_spec(spec_from_file_location(name, path))
        for name in ("archi", "archi.unrelated")
    }
    with pytest.raises(SubstrateError):
        verify_loaded_package("archi", wheel_path=wheel, expected_wheel_digest=pin,
                              installed_root=root, modules=modules)


def test_custom_finder_refuses_before_execution_in_fresh_process(installation, tmp_path):  # noqa: F811
    import subprocess
    import sys
    wheel, pin, root = installation
    marker = tmp_path / "executed"
    program = '''
import sys
from pathlib import Path
from okg.substrate.distributions.packaged_release import configure_packaged_imports, verify_prospective_package
from okg.substrate.errors import SubstrateError
configure_packaged_imports()
class Finder:
    def find_spec(self, *args):
        Path(sys.argv[4]).write_text("executed")
sys.meta_path.insert(0, Finder())
sys.path.insert(0, sys.argv[3])
try:
    verify_prospective_package("archi", wheel_path=Path(sys.argv[1]), expected_wheel_digest=sys.argv[2], installed_root=Path(sys.argv[3]))
except SubstrateError:
    assert not Path(sys.argv[4]).exists()
else:
    raise AssertionError("custom finder accepted")
'''
    result = subprocess.run([sys.executable, "-I", "-B", "-c", program,
                             str(wheel), pin, str(root), str(marker)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert not marker.exists()


def test_release_artifact_preserves_tagged_metadata():
    from okg.substrate.deployment_release import ReleaseArtifact
    identity = PackagedReleaseIdentity(*(["sha256:" + "a" * 64] * 4), "archi-profile/v1")
    artifact = ReleaseArtifact(None, "schema", "catalog", "skills", packaged_release=identity.to_dict())
    assert ReleaseArtifact.from_mapping(artifact.to_dict()) == artifact
    with pytest.raises(SubstrateError):
        ReleaseArtifact.from_mapping({**artifact.to_dict(), "packaged_release": {}})
    with pytest.raises(ValueError):
        ReleaseArtifact.from_mapping({**artifact.to_dict(), "code_revision": "a" * 40})


def test_delayed_publisher_cannot_outlive_verification_scope(tmp_path):
    from okg.substrate.publisher.publisher import Publisher
    from okg.substrate.distributions.packaged_release import _VerifiedRelease
    identity = PackagedReleaseIdentity(*(["sha256:" + "a" * 64] * 4), "archi-profile/v1")
    publisher = object.__new__(Publisher)
    publisher._packaged_release_context = _VerifiedRelease(tmp_path, identity, lambda: None)
    with pytest.raises(ValueError, match="scope expired"):
        publisher.cycle()


def test_factory_revalidates_before_stack_or_catalog(tmp_path, monkeypatch):
    from okg.substrate.publisher import factory
    from okg.substrate.distributions import packaged_release as release
    identity = PackagedReleaseIdentity(*(["sha256:" + "a" * 64] * 4), "archi-profile/v1")
    def refuse():
        raise SubstrateError("configuration changed")
    verified = release._VerifiedRelease(tmp_path, identity, refuse)
    token = release._ACTIVE_RELEASE.set(verified)
    calls = []
    monkeypatch.setattr(factory._deployment_stack, "build_deployment_publisher_stack", lambda **kw: calls.append("stack"))
    monkeypatch.setattr(factory._deployment_stack, "build_runtime_catalog_identity", lambda **kw: calls.append("catalog"))
    try:
        with pytest.raises(SubstrateError, match="configuration changed"):
            factory.build_deployment_publisher(dsn="unused", deployment_name="demo", command="test", deployment_dir=tmp_path)
        assert calls == []
    finally:
        release._ACTIVE_RELEASE.reset(token)


def test_launcher_retains_verified_bytes_and_refuses_ancestor_symlink(tmp_path):
    import hashlib
    from okg.substrate.distributions.install_launcher import _retain
    root = tmp_path.resolve()
    original = root / "release.whl"
    original.write_bytes(b"pinned release")
    pin = "sha256:" + hashlib.sha256(original.read_bytes()).hexdigest()
    retained = root / "retained"
    retained.mkdir()
    result = _retain(original, pin, retained)
    original.write_bytes(b"changed after verification")
    assert result.read_bytes() == b"pinned release"
    assert result.stat().st_mode & 0o222 == 0
    alias = root / "alias"
    alias.symlink_to(root, target_is_directory=True)
    other = root / "other"
    other.mkdir()
    with pytest.raises(OSError):
        _retain(alias / "release.whl", pin, other)


def test_launcher_foreign_environment_refuses_without_invoking_uv(tmp_path, monkeypatch):
    import hashlib
    from okg.substrate.distributions import install_launcher
    root = tmp_path.resolve()
    wheel = root / "release.whl"
    wheel.write_bytes(b"pinned release")
    pin = "sha256:" + hashlib.sha256(wheel.read_bytes()).hexdigest()
    environment = root / "foreign"
    environment.mkdir()
    valuable = environment / "existing"
    valuable.write_text("preserve")
    calls = []
    monkeypatch.setattr(install_launcher.subprocess, "run", lambda *args, **kwargs: calls.append(args))
    assert install_launcher.main([
        "--framework-wheel", str(wheel), "--framework-digest", pin,
        "--consumer-wheel", str(wheel), "--consumer-digest", pin,
        "--dependency-lock", str(wheel), "--dependency-digest", pin,
        "--installer-wheel", str(wheel), "--installer-digest", pin,
        "--environment", str(environment),
    ]) == 1
    assert calls == []
    assert valuable.read_text() == "preserve"


def test_launcher_uses_wheels_only_and_sets_flags_before_framework_import(tmp_path, monkeypatch):
    import hashlib
    from types import SimpleNamespace
    from okg.substrate.distributions import install_launcher
    root = tmp_path.resolve()
    wheel = root / "release.whl"
    wheel.write_bytes(b"pinned release")
    pin = "sha256:" + hashlib.sha256(wheel.read_bytes()).hexdigest()
    environment = root / "owned"
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        if "venv" in command:
            (environment / "bin").mkdir()
            (environment / "bin/python").write_text("interpreter placeholder")
        if "--no-deps" in command and command[0] != "uv":
            # The install reads copies, not the mutable original wheel path.
            assert str(wheel) not in command
            assert all(__import__('pathlib').Path(path).read_bytes() == b"pinned release" for path in command[-2:])
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(install_launcher.subprocess, "run", run)
    arguments = ["--framework-wheel", str(wheel), "--framework-digest", pin,
                 "--consumer-wheel", str(wheel), "--consumer-digest", pin,
                 "--dependency-lock", str(wheel), "--dependency-digest", pin,
        "--installer-wheel", str(wheel), "--installer-digest", pin,
                 "--environment", str(environment), "--profile", "selected"]
    import json
    manifest = root / "wheelhouse.json"
    manifest.write_text(json.dumps({"requirements_digest": pin, "entries": [{"wheel": wheel.name, "wheel_digest": pin}]}))
    manifest_pin = "sha256:" + hashlib.sha256(manifest.read_bytes()).hexdigest()
    arguments.extend(["--dependency-wheelhouse", str(root), "--wheelhouse-manifest", str(manifest), "--wheelhouse-digest", manifest_pin])
    assert install_launcher.main(arguments) == 0
    assert "--require-hashes" in commands[2]
    assert "--no-index" in commands[2] and "--find-links" in commands[2]
    assert commands[2][commands[2].index("--only-binary") + 1] == ":all:"
    assert "--no-deps" in commands[3]
    assert commands[4][1:4] == ["-I", "-B", "-c"]
    assert commands[4][-2:] == ["--profile", "selected"]
    commands.clear()
    assert install_launcher.main(arguments) == 0
    assert len(commands) == 1  # Same pins reuse the environment without reinstalling.
    commands.clear()
    for selector in ("--update-source", "--update-sour", "--u"):
        commands.clear()
        assert install_launcher.main([*arguments, selector, "documents"]) == 0
        assert len(commands) == 1
        assert "--update-source=documents" in commands[0]
    for selector in ("--search-query", "--search-q"):
        commands.clear()
        assert install_launcher.main([*arguments, selector, "needle", "--search-g", "published"]) == 0
        assert len(commands) == 1
        assert "--search-query=needle" in commands[0]
        assert "--search-generation=published" in commands[0]
    import argparse
    child = argparse.ArgumentParser(add_help=False)
    child.add_argument("--search-query")
    commands.clear()
    assert install_launcher.main([*arguments, "--search-query=--literal"]) == 0
    parsed, _ = child.parse_known_args(commands[0][4:])
    assert parsed.search_query == "--literal"
    commands.clear()
    marker = environment / ".okg-install-owner.json"
    ownership = json.loads(marker.read_text())
    ownership["state"] = "preparing"
    marker.write_text(json.dumps(ownership))
    for selector in ("--update-source", "--update-sour", "--u", "--search-query", "--search-q"):
        assert install_launcher.main([*arguments, selector, "documents"]) == 1
        assert not commands
    missing = root / "never-created"
    arguments[arguments.index("--environment") + 1] = str(missing)
    for selector in ("--update-source", "--update-sour", "--u", "--search-query", "--search-q"):
        assert install_launcher.main([*arguments, selector, "documents"]) == 1
        assert not missing.exists() and not commands


    for invalid in (["--search-generation", "generation"], ["--search-query", " "],
                    ["--search-query", "needle", "--update-source", "documents"],
                    ["--search-query", "needle", "--dry-run"],
                    ["--search-query", "needle", "--scaffold-only"],
                    ["--search-query", "needle", "--no-publish"],
                    ["--search-query", "needle", "--search-generation", " "]):
        assert install_launcher.main([*arguments, *invalid]) == 1
        assert not missing.exists() and not commands


def test_launcher_retry_ignores_abandoned_marker_temporary_file(tmp_path):
    import json
    from okg.substrate.distributions.install_launcher import _record
    marker = tmp_path / "owner.json"
    marker.write_text(json.dumps({"ownership": {}, "state": "preparing"}))
    marker.with_suffix(".new").write_text("interrupted old write")
    _record(marker, {}, "installed")
    assert json.loads(marker.read_text())["state"] == "installed"
    assert not list(tmp_path.glob(".okg-owner-*"))


def test_launcher_concurrent_owner_refused(tmp_path):
    from okg.substrate.distributions.install_launcher import _environment_lock
    environment = tmp_path.resolve() / "owned"
    with _environment_lock(environment):
        with pytest.raises(ValueError, match="busy"):
            with _environment_lock(environment):
                pytest.fail("concurrent owner acquired the environment")


def test_install_stage_preserves_failure_reason_without_credential(tmp_path):
    import json
    from okg.substrate.distributions.install_execution import _capture

    def failed_stage(args, *, as_json):
        print(json.dumps({"ok": False, "error": {"code": "missing_extension",
                         "fix_hint": "check postgresql://operator:private-password@localhost/db"}}))
        return 1

    with pytest.raises(SubstrateError) as caught:
        _capture(failed_stage, None)
    assert caught.value.data["reason"] == "missing_extension"
    assert "private-password" not in caught.value.fix_hint


def test_launcher_parent_traversal_cannot_bypass_owner_lock(tmp_path):
    from okg.substrate.distributions.install_launcher import _environment_lock
    root = tmp_path.resolve()
    (root / "sub").mkdir()
    with _environment_lock(root / "environment"):
        with pytest.raises(ValueError, match="parent traversal"):
            with _environment_lock(root / "sub" / ".." / "environment"):
                pytest.fail("lexical alias acquired a second lock")


def test_existing_install_handler_routes_packaged_request_without_legacy_discovery(monkeypatch):
    import argparse
    from okg.substrate.cli.main import _cmd_install
    from okg.substrate.distributions import install_execution
    from okg.substrate.cli import profile_cli_loader
    calls = []
    monkeypatch.setattr(install_execution, "active_bootstrap", object)
    monkeypatch.setattr(install_execution, "run_install", lambda args, **kwargs: calls.append(args) or 7)
    monkeypatch.setattr(profile_cli_loader, "discover_install_extensions", lambda: pytest.fail("legacy extension discovery ran"))
    args = argparse.Namespace(distribution_package="selected")
    assert _cmd_install(args, as_json=True) == 7
    assert calls == [args]


def test_stage_preserves_redacted_cause_and_retry_hint():
    from okg.substrate.distributions.install_execution import _stage
    with pytest.raises(SubstrateError) as caught:
        with _stage("runtime_bootstrap"):
            _raise(SubstrateError("connection refused postgresql://user:private-password@localhost/db",
                                  data={"reason": "missing_role", "password": "private-password"},
                                  fix_hint="Provide the authorized role; PASSWORD=private-password"))
    error = caught.value
    assert error.data["stage"] == "runtime_bootstrap"
    assert error.data["reason"] == "missing_role"
    assert "private-password" not in str(error.to_json())
    assert "authorized role" in error.fix_hint
    with pytest.raises(SubstrateError) as value:
        with _stage("runtime_bootstrap"):
            _raise(ValueError("unsupported runtime schema"))
    assert value.value.message == "unsupported runtime schema"
    assert value.value.data["exception"] == "ValueError"


def test_packaged_manifest_name_resolves_only_bound_directory(tmp_path):
    from okg.substrate.deployments import find_deployment_dir, resolve_deployment
    from okg.substrate.distributions import packaged_release as release
    from okg.substrate.nomos.store import _bootstrap_policy_activation_outcome
    directory = tmp_path.resolve() / "configuration"
    directory.mkdir()
    (directory / "schema").mkdir()
    (directory / "source_registry.yaml").write_text("sources: {}\n")
    (directory / "deployment.yaml").write_text("name: packaged-demo\nschema_dir: schema\nsource_registry: source_registry.yaml\n")
    identity = PackagedReleaseIdentity(*(["sha256:" + "a" * 64] * 4), "test/v1")
    calls = []
    token = release._ACTIVE_RELEASE.set(release._VerifiedRelease(directory, identity, lambda: calls.append("verified")))
    try:
        assert find_deployment_dir("packaged-demo") == directory
        manifest = resolve_deployment("packaged-demo")
        assert manifest.name == "packaged-demo"
        outcome = _bootstrap_policy_activation_outcome(
            deployment_name=manifest.name, scope="deployment:" + manifest.name,
            surface="policy.activate", subject="subject:nomos-rollout", resource={})
        assert outcome.allowed
        for name in (None, "foreign", str(tmp_path / "other")):
            with pytest.raises(SubstrateError):
                resolve_deployment(name)
        assert len(calls) == 5
    finally:
        release._ACTIVE_RELEASE.reset(token)


def test_capture_retains_nonzero_provision_summary_and_error_envelope():
    import argparse
    import json
    from okg.substrate.distributions.install_execution import _capture
    for payload in (
        {"payload": {"ok": False, "publish_proof": {"ok": False, "reasons": ["no_published_generation"]},
                     "failed_sources": [{"source": "fixture", "error": "source_refused"}]}},
        {"error": {"code": "source_refused", "fix_hint": "Correct policy", "data": {"password": "private-value"}}},
    ):
        def command(args, *, as_json):
            print(json.dumps(payload))
            return 1
        with pytest.raises(SubstrateError) as caught:
            _capture(command, argparse.Namespace())
        result = caught.value.data["stage_result"]
        if "payload" in payload:
            assert result["publish_proof"]["reasons"] == ["no_published_generation"]
            assert result["failed_sources"][0]["source"] == "fixture"
        else:
            assert caught.value.data["reason"] == "source_refused"
            assert "private-value" not in str(caught.value.to_json())


def test_capture_proceeds_on_the_provision_capacity_warning_exit_only():
    """Decision 23a: provision's exit 8 (committed with a capacity warning)
    lets the installer proceed; exit 8 from any other stage, and exit 1 from
    provision, still refuse."""
    import argparse
    import json
    from okg.substrate.cli.run_summary import EXIT_CAPACITY_WARNING
    from okg.substrate.distributions.install_execution import _capture

    payload = {"ok": True, "status": "capacity_warning",
               "publish_proof": {"ok": True, "capacity_warning": {"hold_ms": 800400.0}}}

    def make(name, rc):
        def command(args, *, as_json):
            print(json.dumps(payload))
            return rc
        command.__name__ = name
        return command

    assert EXIT_CAPACITY_WARNING == 8
    result = _capture(make("_cmd_provision", EXIT_CAPACITY_WARNING), argparse.Namespace())
    assert result["publish_proof"]["capacity_warning"]["hold_ms"] == 800400.0
    for name, rc in (("_cmd_provision", 1), ("cmd_search", EXIT_CAPACITY_WARNING)):
        with pytest.raises(SubstrateError):
            _capture(make(name, rc), argparse.Namespace())


def test_six_moves_hook_cannot_resolve_owned_or_similarly_named_modules(monkeypatch):
    import sys
    import six
    from okg.substrate.distributions import packaged_release as release
    monkeypatch.setattr(release, "_SIX_FINDER", None)
    release.configure_packaged_imports()
    release._require_import_policy()
    finder = release._SIX_FINDER
    assert finder._delegate is six._importer
    assert finder.find_spec("six.moves") is not None
    assert finder.find_spec("six.moves.urllib_parse") is not None
    monkeypatch.setitem(six._importer.known_modules, "okg", six.moves)
    monkeypatch.setitem(six._importer.known_modules, "archi", six.moves)
    monkeypatch.setitem(six._importer.known_modules, "six.movesevil", six.moves)
    for name in ("okg", "okg.substrate", "archi", "archi.sources", "six.movesevil"):
        assert finder.find_spec(name) is None
    sys.meta_path.append(six._importer)
    with pytest.raises(SubstrateError):
        release._require_import_policy()


def test_import_policy_refuses_equality_impostor(monkeypatch):
    import sys
    from okg.substrate.distributions import packaged_release as release
    monkeypatch.setattr(release, "_SIX_FINDER", None)
    class Impostor:
        def __eq__(self, other):
            return True
    release.configure_packaged_imports()
    original = list(sys.meta_path)
    for index in range(len(original)):
        sys.meta_path[:] = original
        sys.meta_path[index] = Impostor()
        with pytest.raises(SubstrateError):
            release._require_import_policy()
    sys.meta_path[:] = original
    sys.path_hooks[:] = [Impostor()]
    with pytest.raises(SubstrateError):
        release._require_import_policy()


def test_six_moves_lazy_import_in_fresh_process():
    import subprocess
    import sys
    result = subprocess.run([sys.executable, "-B", "-c", """
from okg.substrate.distributions.packaged_release import configure_packaged_imports, _require_import_policy
configure_packaged_imports()
from six.moves.urllib_parse import urlparse
assert urlparse('https://example.test/document').hostname == 'example.test'
_require_import_policy()
"""], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_cached_loader_equality_impostor_refuses_before_execution(tmp_path):
    import sys
    from okg.substrate.distributions import packaged_release as release
    calls = []
    class Impostor:
        def __eq__(self, other):
            return True
        def __call__(self, *args):
            calls.append(args)
            raise AssertionError("unverified loader executed")
    finder = release._FILE_HOOK(str(tmp_path))
    finder._loaders = [(suffix, Impostor()) for suffix, loader in finder._loaders]
    sys.path_importer_cache[str(tmp_path)] = finder
    with pytest.raises(SubstrateError):
        release._require_import_policy()
    assert calls == []


def test_runtime_receipt_keeps_measurements_and_refuses_nonfinite_replacement(tmp_path):
    import json
    from okg.substrate.distributions.install_execution import _write_install_receipt
    path = tmp_path / "install.json"
    receipt = {"state": "published", "provision": {"ok": True, "timings_ms": {"total": 2194.231}}}
    _write_install_receipt(path, receipt)
    assert json.loads(path.read_text()) == receipt
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(ValueError):
        _write_install_receipt(path, {"timing": float("nan")})
    assert json.loads(path.read_text()) == receipt
    assert sorted(item.name for item in tmp_path.iterdir()) == ["install.json"]


def test_reader_verification_refuses_empty_foreign_or_denied_results(monkeypatch):
    from contextlib import nullcontext
    from okg.substrate.distributions import install_execution as execution
    from okg.substrate.cli import _read_side, provision
    from okg.substrate.db import connection
    from okg.substrate.nomos import doctor
    monkeypatch.setattr(_read_side, "_resolve_cli_read_subject", lambda **kw: ("subject:human_operator", {"source": "manifest_read_principal"}))
    monkeypatch.setattr(provision, "_generation_row_counts", lambda *a, **kw: ("generation:1", 2, 1))
    monkeypatch.setattr(connection, "connect", lambda *a, **kw: nullcontext(object()))
    monkeypatch.setattr(doctor, "source_lineage_readiness_report", lambda *a, **kw: {"ok": True, "runtime_enforcement_mode": "enforce", "active_policy_count": 1})
    good = {"generation_id": "generation:1", "nomos_effect": "allow", "results": [{"node_id": "doc:1", "provenance": {"source": "fixture"}, "source_record_id": {"id": "doc:1"}, "attrs": {"text": "bootstrap technical text"}}]}
    for result in ({**good, "results": []}, {**good, "results": [{"node_id": "doc:1", "provenance": {"source": "fixture"}}]}, {**good, "results": [{**good["results"][0], "source_record_id": {}}]}, {**good, "generation_id": "foreign"}, {**good, "nomos_effect": "deny"}):
        monkeypatch.setattr(execution, "_capture", lambda *a, **kw: result)
        with pytest.raises(SubstrateError):
            execution._verify_installed_reader(dsn="unused", deployment="demo", generation="generation:1", query="bootstrap")
    monkeypatch.setattr(execution, "_capture", lambda *a, **kw: good)
    receipt = execution._verify_installed_reader(dsn="unused", deployment="demo", generation="generation:1", query="bootstrap")
    assert receipt["results"][0]["source_record_id"] == {"id": "doc:1"}
    assert receipt["results"][0]["returned_text_sha256"] and "attrs" not in receipt["results"][0]


def test_known_install_reason_survives_redaction_without_exempting_external_text():
    from okg.substrate.distributions.install_execution import _stage, _refuse
    reason = "verification_query_source_evidence_missing"
    with pytest.raises(SubstrateError) as caught:
        with _stage("reader_verification"):
            _refuse(reason)
    assert caught.value.data["reason"] == reason
    with pytest.raises(SubstrateError) as external:
        with _stage("reader_verification"):
            _raise(SubstrateError("external refusal", data={"reason": fake_credential("glpat-", "abcdefghijklmnopqrstuvwxyz")}))
    assert "glpat-" not in str(external.value.to_json())


def test_packaged_constructor_and_readback_refuse_mixed_git_identity():
    from okg.substrate.cli.generation import _checkpoint_release_identities
    from okg.substrate.publisher.publisher import Publisher
    from okg.substrate.publisher.repository_provenance import RepositoryProvenance
    git = RepositoryProvenance.exact(core_revision="a" * 40, deployment_revision="b" * 40, deployment_tree_sha="c" * 40)
    packaged = PackagedReleaseIdentity(*(["sha256:" + "a" * 64] * 4), "test/v1")
    with pytest.raises(ValueError, match="cannot be combined"):
        Publisher(dsn="unused", deployment_name="demo", repository_provenance=git, packaged_release=object())
    assert _checkpoint_release_identities({"repository_provenance": git.to_dict()}) == (git, None)
    assert _checkpoint_release_identities({"packaged_release": packaged.to_dict()})[1] == packaged
    for metadata in (
        {"repository_provenance": git.to_dict()},
        {"repository_provenance": {"status": "exact", "core_revision": "malformed"}},
        {"core_revision": "a" * 40},
    ):
        with pytest.raises(ValueError, match="cannot be combined"):
            _checkpoint_release_identities({**metadata, "packaged_release": packaged.to_dict()})


def test_install_stage_receipt_refuses_malformed_fields():
    from okg.substrate.distributions.install_stage import InstallStage
    stage = InstallStage("test", "default", None, "token", ("admission",), "a" * 64)
    assert InstallStage.from_dict(stage.to_dict()) == stage
    for invalid in (None, {}, {**stage.to_dict(), "extra": True},
                    {**stage.to_dict(), "admission_ids": None},
                    {**stage.to_dict(), "admission_ids": ["a", "a"]},
                    {**stage.to_dict(), "deployment": {}},
                    {**stage.to_dict(), "latest_checkpoint_id": 123},
                    {**stage.to_dict(), "admission_digest": "z" * 64}):
        with pytest.raises(SubstrateError):
            InstallStage.from_dict(invalid)


@pytest.mark.parametrize("change", [
    {"state": "unknown"}, {"schema": "other"}, {"state": "staged", "published": False},
    {"state": "published", "published": True},
])
def test_invalid_install_receipt_refuses_before_database_mutation(tmp_path, monkeypatch, change):
    import json
    from types import SimpleNamespace
    from okg.substrate.distributions import install_execution as execution
    from okg.substrate.distributions import packaged_release
    digest = "sha256:" + "a" * 64
    files = {"deployment.yaml": b"name: test\nrelease: {kind: packaged}\n", "source_registry.yaml": b"{}\n"}
    identity = PackagedReleaseIdentity(digest, digest, execution._configuration_digest(files), digest, "test/v1")
    marker = tmp_path / "install.json"
    marker.write_text(json.dumps({"schema": "okg.install-receipt/v1", "state": "prepared",
        "release": identity.to_dict(), "database": {}, **change}))
    original = marker.read_bytes()
    bootstrap = SimpleNamespace(verify=lambda: None, framework=SimpleNamespace(digest=digest), consumer=SimpleNamespace(digest=digest))
    monkeypatch.setattr(packaged_release, "verify_plan_binding", lambda *args: None)
    def forbidden(*args, **kwargs):
        pytest.fail("database must not be touched for invalid receipt")
    monkeypatch.setattr(execution, "_database_identity", forbidden)
    with pytest.raises(SubstrateError):
        execution.execute_prepared_install(bootstrap=bootstrap, root=tmp_path, files=files,
            plan={"catalog": {"module_refs": [], "subtype_origin": {}}, "modules": []}, lock={"lock_digest": digest}, translation="test/v1", dsn="unused",
            authorize_cluster_bootstrap=True, verification_query="test")
    assert marker.read_bytes() == original


@pytest.mark.parametrize("manifest_body", ["null", "[]", '{"requirements_digest":"PIN","entries":[null]}'])
def test_launcher_malformed_pinned_wheelhouse_refuses_before_environment(tmp_path, monkeypatch, manifest_body):
    import hashlib
    from okg.substrate.distributions import install_launcher
    root = tmp_path.resolve()
    wheel = root / "input.whl"
    wheel.write_bytes(b"test")
    pin = "sha256:" + hashlib.sha256(wheel.read_bytes()).hexdigest()
    manifest = root / "manifest.json"
    manifest.write_text(manifest_body.replace("PIN", pin))
    manifest_pin = "sha256:" + hashlib.sha256(manifest.read_bytes()).hexdigest()
    def forbidden(*args, **kwargs):
        pytest.fail("malformed manifest must not invoke installation")
    monkeypatch.setattr(install_launcher.subprocess, "run", forbidden)
    environment = root / "environment"
    assert install_launcher.main(["--framework-wheel", str(wheel), "--framework-digest", pin,
        "--consumer-wheel", str(wheel), "--consumer-digest", pin,
        "--installer-wheel", str(wheel), "--installer-digest", pin,
        "--dependency-lock", str(wheel), "--dependency-digest", pin,
        "--dependency-wheelhouse", str(root), "--wheelhouse-manifest", str(manifest),
        "--wheelhouse-digest", manifest_pin, "--environment", str(environment)]) == 1
    assert not environment.exists()


def test_owned_pseudonym_key_persists_and_restores_ambient_environment(tmp_path, monkeypatch):
    import os
    from okg.substrate.distributions.install_execution import _pseudonym_secret, _pseudonym_environment
    from okg.substrate.alias.pseudonymize import load_pseudonym_service
    root = tmp_path.resolve()
    key, binding = _pseudonym_secret(root, create=True, expected=None)
    assert (root / "pseudonym.key").stat().st_mode & 0o777 == 0o600
    monkeypatch.setenv("OKG_PSEUDONYM_HMAC_KEY", "ambient-value-is-not-the-instance-key")
    with _pseudonym_environment(key):
        first = load_pseudonym_service().pseudonym(canonical="test-person")
    assert os.environ["OKG_PSEUDONYM_HMAC_KEY"] == "ambient-value-is-not-the-instance-key"
    repeated, repeated_binding = _pseudonym_secret(root, create=False, expected=binding)
    with _pseudonym_environment(repeated):
        assert load_pseudonym_service().pseudonym(canonical="test-person") == first
    assert repeated_binding == binding
    assert key not in repr(binding)


@pytest.mark.parametrize("damage", ["missing", "changed", "permissions", "symlink", "hardlink"])
def test_owned_pseudonym_key_never_rotates_on_drift(tmp_path, damage):
    from okg.substrate.distributions.install_execution import _pseudonym_secret
    root = tmp_path.resolve()
    _, binding = _pseudonym_secret(root, create=True, expected=None)
    path = root / "pseudonym.key"
    if damage == "missing":
        path.unlink()
    elif damage == "changed":
        path.write_text("a" * 64)
    elif damage == "permissions":
        path.chmod(0o644)
    elif damage == "hardlink":
        import os
        os.link(path, root / "other")
    else:
        path.rename(root / "original")
        path.symlink_to(root / "original")
    with pytest.raises(SubstrateError, match="missing or changed"):
        _pseudonym_secret(root, create=False, expected=binding)


def test_reader_failure_retains_completed_publication_and_reader_diagnostics(tmp_path):
    import json
    from okg.substrate.distributions.install_execution import _installation_receipts, _stage, _capture
    def provision(args, *, as_json):
        print(json.dumps({"generation_id": "generation:actual", "publish_proof": {
            "ok": True, "nodes_added": 2, "edges_added": 1}}))
        return 0
    def cmd_search(args, *, as_json):
        print(json.dumps({"generation_id": "generation:actual", "nomos_effect": "deny", "results": []}))
        return 0
    with pytest.raises(SubstrateError):
        with _installation_receipts(tmp_path.resolve()):
            with _stage("provision"):
                _capture(provision, None)
            with _stage("reader_verification"):
                _capture(cmd_search, None)
                _raise(SubstrateError("reader refused", data={"reason": "reader_denied"}))
    path, = tmp_path.glob("install-stages-*.json")
    assert path.stat().st_mode & 0o777 == 0o600
    events = json.loads(path.read_text())["stages"]
    publication = next(e for e in events if e["stage"] == "provision" and e["status"] == "returned")
    assert publication["result"]["generation_id"] == "generation:actual"
    assert publication["result"]["publish_proof"]["nodes_added"] == "2"
    search = next(e for e in events if e["stage"] == "cmd_search")
    assert search["result"]["nomos_effect"] == "deny" and search["result"]["result_count"] == "0"
    assert events[-1]["stage"] == "reader_verification" and events[-1]["status"] == "failed"


def test_release_bundle_emits_one_pinned_operator_invocation(tmp_path):
    import hashlib
    import json
    import subprocess
    import sys
    import zipfile
    from okg.substrate.distributions.install_bundle import build_bundle
    root = tmp_path.resolve()
    framework = root / "framework.whl"
    with zipfile.ZipFile(framework, "w") as wheel:
        wheel.writestr("okg/substrate/distributions/install_launcher.py", "import json,sys;print(json.dumps({'argv':sys.argv[1:],'isolated':sys.flags.isolated,'bytecode_disabled':sys.dont_write_bytecode}))")
    consumer = root / "consumer.whl"
    consumer.write_bytes(b"consumer")
    installer = root / "pip.whl"
    installer.write_bytes(b"trusted-pip")
    wheelhouse = root / "wheelhouse"
    (wheelhouse / "wheels").mkdir(parents=True)
    dependency = wheelhouse / "wheels/dep.whl"
    dependency.write_bytes(b"dependency")
    requirements = wheelhouse / "runtime-wheels.txt"
    requirements.write_text("dep==1 --hash=sha256:test\n")

    def pin(p):
        return "sha256:" + hashlib.sha256(p.read_bytes()).hexdigest()
    manifest = wheelhouse / "manifest.json"
    manifest.write_text(json.dumps({"requirements_digest": pin(requirements), "entries": [{"wheel": dependency.name,"wheel_digest": pin(dependency)}]}))
    output = root / "release"
    entry = build_bundle(framework=framework, framework_digest=pin(framework), consumer=consumer,
        consumer_digest=pin(consumer), consumer_package="archi", wheelhouse=wheelhouse,
        wheelhouse_digest=pin(manifest), installer=installer, installer_digest=pin(installer), output=output)
    command = [sys.executable, "-I", "-B", str(entry), "--target", str(root / "instance"), "--profile", "cern-team"]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    observed = json.loads(result.stdout)
    assert observed["isolated"] and observed["bytecode_disabled"]
    args = observed["argv"]
    assert args.count("--consumer-digest") == 1 and pin(consumer) in args
    assert args[args.index("--environment") + 1] == str(root / "instance.environment")
    assert args[-2:] == ["--profile", "cern-team"]
    refused = subprocess.run(command + ["--consumer-digest", "replacement"], capture_output=True, text=True)
    assert refused.returncode != 0 and not refused.stdout
    refused = subprocess.run(command + ["--", "--consumer-digest", "replacement"], capture_output=True, text=True)
    assert refused.returncode != 0 and not refused.stdout
    for option in ("--framework-d=other", "--environ=foreign", "--consumer-p=other"):
        refused = subprocess.run(command + [option], capture_output=True, text=True)
        assert refused.returncode != 0 and not refused.stdout
    (output / "install_launcher.py").write_text("raise AssertionError('must not execute')")
    refused = subprocess.run(command, capture_output=True, text=True)
    assert refused.returncode != 0 and "launcher bytes changed" in refused.stderr


def test_stage_retains_package_errno_without_private_path():
    import errno
    from okg.substrate.distributions.install_execution import _stage
    from okg.substrate.distributions.package import DistributionPackageError
    with pytest.raises(SubstrateError) as caught:
        with _stage("provision"):
            try:
                raise OSError(errno.EMFILE, "private diagnostic", "/private/sensitive/source")
            except OSError as cause:
                _raise(DistributionPackageError("installed_wheel_path_unsafe", "private-path", "unavailable or unsafe input path"), cause)
    assert caught.value.data["filesystem_errno"] == errno.EMFILE
    assert caught.value.data["package_code"] == "installed_wheel_path_unsafe"
    assert "sensitive" not in repr(caught.value.data) and "private-path" not in repr(caught.value.data)


# Issue #1914: a connector that declares records only must be able to finish a
# packaged install. The published-generation readback consults the deployment's
# source registry before it insists on relationships.

_RECORDS_ONLY_POLICY = {
    "trust_label": "review-trusted",
    "allowed_record_kinds": ["node"],
    "output_signature": {
        "nodes": [{"subtype": "document", "attrs": {"source": "proof"}}],
    },
    "policy_clauses": [
        {"id": "proof.document", "record_kind": "node", "subtype": "document"},
    ],
}

_RELATIONSHIP_POLICY = {
    "trust_label": "review-trusted",
    "allowed_record_kinds": ["edge"],
    "output_signature": {
        "edges": [{
            "src_subtype": "document",
            "edge_type": "references",
            "dst_subtype": "source_file",
        }],
    },
}

_READER_RESULT = {
    "generation_id": "generation:1",
    "nomos_effect": "allow",
    "results": [{
        "node_id": "doc:1",
        "provenance": {"source": "fixture"},
        "source_record_id": {"id": "doc:1"},
        "attrs": {"text": "bootstrap technical text"},
    }],
}


def _registry_entries(admission_policy, extra_policy=None):
    """Admit a registry document and return its typed entries.

    ``extra_policy`` adds a second source the install did not select, which the
    rule still reads: the declaration is deployment-wide, not selection-wide.
    """
    from pathlib import Path
    from okg.substrate.sources.registry import admit_source_registry_document
    source = {
        "module": "tests.strict_sources",
        "class": "ReferenceAdapter",
        "ownership_id": "proof.reference",
        "source_class": "reference_catalog",
        "record_identity_kind": "domain_key",
        "record_identity_fields": ["id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "params": {"fixture_path": "/tmp/proof.json"},
    }
    if admission_policy is not None:
        source["admission_policy"] = admission_policy
    sources = {"reference": source}
    if extra_policy is not None:
        other = dict(source)
        other["ownership_id"] = "proof.unselected"
        other["admission_policy"] = extra_policy
        sources["unselected"] = other
    return admit_source_registry_document(
        {"sources": sources},
        registry_path=Path("source_registry.yaml"),
    ).entries


def _drive_prepared_install(tmp_path, monkeypatch, *, existing_receipt=None, observation=(None, None, None),
                            no_publish=False, update_source=None, search_query=None, search_generation=None,
                            registry_policy=_RELATIONSHIP_POLICY, stage_proofs=None,
                            registry_error=None):
    """Run execute_prepared_install past identity and bootstrap with stubs, and
    return (provision Namespace seen by the installer, observation calls, receipt)."""
    import json
    from contextlib import nullcontext
    from types import SimpleNamespace
    from okg.substrate.distributions import install_execution as execution
    from okg.substrate.distributions import packaged_release
    from okg.substrate.cli import provision as provision_mod
    digest = "sha256:" + "b" * 64
    files = {"deployment.yaml": b"name: test\nrelease: {kind: packaged}\n", "source_registry.yaml": b"sources: {}\n"}
    identity = PackagedReleaseIdentity(digest, digest, execution._configuration_digest(files), digest, "test/v1")
    database = {"database": "s14_packaged_test", "oid": 1, "system_identifier": 2}
    root = tmp_path / "instance"
    if existing_receipt is not None:
        root.mkdir()
        (root / "install.json").write_text(json.dumps({"schema": "okg.install-receipt/v1",
            "release": identity.to_dict(), "database": database, **existing_receipt}))
    bootstrap = SimpleNamespace(verify=lambda: None, framework=SimpleNamespace(digest=digest), consumer=SimpleNamespace(digest=digest))
    monkeypatch.setattr(packaged_release, "verify_plan_binding", lambda *args: None)
    monkeypatch.setattr(execution, "_database_identity", lambda *a, **kw: database)
    monkeypatch.setattr(execution, "_require_redaction_runtime", lambda registry: None)
    monkeypatch.setattr(execution, "verified_release_scope", lambda **kw: nullcontext())
    monkeypatch.setattr("okg.substrate.db.migrate.ensure_required_extensions", lambda dsn: None)
    monkeypatch.setattr("okg.substrate.db.migrate.migrate", lambda dsn: None)
    monkeypatch.setattr("okg.substrate.runtime.bootstrap_dbos_schema", lambda **kw: None)
    monkeypatch.setattr("okg.substrate.deployments.resolve_deployment",
                        lambda path: SimpleNamespace(name="test", runtime=None,
                                                     source_registry=tmp_path / "source_registry.yaml"))
    # Issue #1914: the installer reads the deployment's declared output
    # signature once, before it observes a replay predecessor or provisions.
    def load_registry(*a, **kw):
        if registry_error is not None:
            raise registry_error
        return _registry_entries(registry_policy)
    monkeypatch.setattr("okg.substrate.sources.registry.load_source_registry", load_registry)
    if stage_proofs is not None:
        # Drive the staged resume (`install --no-publish`, then `install`):
        # its publication and its own publish proof run before the ordinary
        # provision, from a captured stage rather than a fresh source run.
        from okg.substrate.distributions import install_stage as stage_mod
        from okg.substrate.ingest import service as ingest_service
        monkeypatch.setattr(stage_mod, "capture_stage", lambda dsn, **kw: kw.get("expected"))
        monkeypatch.setattr(ingest_service, "_publish",
                            lambda *a, **kw: {"status": "published", "generation_id": "staged-gen"})
        def resumed_proof(dsn, payload, **kwargs):
            stage_proofs.append(kwargs)
            return {"ok": True, "nodes_added": 3, "edges_added": 0}
        monkeypatch.setattr(provision_mod, "_provision_publish_proof", resumed_proof)
    observations = []
    def observe(dsn, *, deployment_name):
        observations.append(deployment_name)
        return observation
    monkeypatch.setattr(provision_mod, "_published_generation_row_counts", observe)
    seen = {}
    def capture(command, args):
        seen["args"] = args
        if getattr(args, "_packaged_stage_only", False):
            return {"ok": True, "state": "staged", "published": False, "admission_ids": []}
        return {"generation_id": "gen-next", "publish_proof": {"ok": True, "nodes_added": 1, "edges_added": 1}}
    if update_source is not None or search_query is not None:
        def forbidden(*args, **kwargs):
            raise AssertionError("update must not bootstrap or provision")
        monkeypatch.setattr("okg.substrate.db.migrate.ensure_required_extensions", forbidden)
        monkeypatch.setattr("okg.substrate.db.migrate.migrate", forbidden)
        monkeypatch.setattr("okg.substrate.runtime.bootstrap_dbos_schema", forbidden)
        monkeypatch.setattr(provision_mod, "_cmd_provision", forbidden)
        def update(**kwargs):
            seen["args"] = kwargs
            return {"published": True, "generation_id": "updated"}
        monkeypatch.setattr(execution, "_execute_update", update)
        monkeypatch.setattr(execution, "_execute_search", update)
    monkeypatch.setattr(execution, "_capture", capture)
    monkeypatch.setattr(execution, "_verify_installed_reader", lambda **kw: {"result_count": 1})
    if no_publish:
        monkeypatch.setattr("okg.substrate.distributions.install_stage.capture_stage",
                            lambda dsn, **kw: SimpleNamespace(to_dict=lambda: {"deployment": "test"}))
    receipt = execution.execute_prepared_install(bootstrap=bootstrap, root=root, files=files,
        plan={"catalog": {"module_refs": [], "subtype_origin": {}}, "modules": []},
        lock={"lock_digest": digest}, translation="test/v1", dsn="postgresql://unused",
        authorize_cluster_bootstrap=True, verification_query="test", no_publish=no_publish,
        update_source=update_source, search_query=search_query, search_generation=search_generation)
    return seen["args"], observations, receipt


def test_daily_update_enters_verified_execution_without_bootstrap(tmp_path, monkeypatch):
    import json
    existing = {"state": "published", "published": True, "reader": {}, "provision": {}}
    args, observations, receipt = _drive_prepared_install(tmp_path, monkeypatch,
        existing_receipt=existing, update_source="documents")
    assert args["source"] == "documents"
    assert args["deployment"] == tmp_path / "instance" / "configuration"
    assert observations == []
    assert receipt["generation_id"] == "updated"
    stored = json.loads((tmp_path / "instance" / "install.json").read_text())
    assert stored["state"] == "published" and "generation_id" not in stored


@pytest.mark.parametrize("existing", [None, {"state": "prepared"}])
def test_daily_update_requires_completed_installation_without_creating_target(tmp_path, monkeypatch, existing):
    with pytest.raises(SubstrateError) as refused:
        _drive_prepared_install(tmp_path, monkeypatch, existing_receipt=existing, update_source="documents")
    assert refused.value.data["reason"] == "update_requires_published_installation"
    assert not (tmp_path / "instance" / "configuration").exists()


@pytest.mark.parametrize("source", ["", "one,two", " documents "])
def test_daily_update_rejects_ambiguous_selector_before_creating_target(tmp_path, monkeypatch, source):
    with pytest.raises(SubstrateError) as refused:
        _drive_prepared_install(tmp_path, monkeypatch, update_source=source)
    assert refused.value.data["reason"] == "update_source_invalid"
    assert not (tmp_path / "instance").exists()


def test_daily_update_cannot_stage_without_publication(tmp_path, monkeypatch):
    with pytest.raises(SubstrateError) as refused:
        _drive_prepared_install(tmp_path, monkeypatch, update_source="documents", no_publish=True)
    assert refused.value.data["reason"] == "update_requires_publication"


@pytest.mark.parametrize("flag", ["dry_run", "scaffold_only", "no_publish"])
def test_daily_update_refuses_unsupported_install_modes_before_preparation(monkeypatch, capsys, flag):
    from argparse import Namespace
    from okg.substrate.distributions import install_execution as execution
    monkeypatch.setattr(execution, "active_bootstrap", lambda: None)
    assert execution.run_install(Namespace(update_source="documents", **{flag: True}), as_json=True) == 1
    assert "update_mode_unsupported" in capsys.readouterr().err


def test_personal_private_without_a_source_policy_refuses_before_preparation(monkeypatch, capsys):
    import json
    from argparse import Namespace
    from types import SimpleNamespace
    from okg.substrate.distributions import install_execution as execution
    from okg.substrate.distributions import package

    def unreachable(*args, **kwargs):
        raise AssertionError("the refusal must come before any package is read")

    monkeypatch.setattr(execution, "active_bootstrap", lambda: SimpleNamespace())
    monkeypatch.setattr(package, "verify_distribution_package", unreachable)
    monkeypatch.setattr(execution, "render_verified_profile", unreachable)
    monkeypatch.setattr(execution, "execute_prepared_install", unreachable)
    monkeypatch.setenv("OKG_PERSONAL_PRIVATE_DSN", "postgresql://unused")
    args = Namespace(
        deployment_name="sample", target="/unused/instance", install_dsn_ref="env:OKG_PERSONAL_PRIVATE_DSN",
        verify_query="needle", personal_private=True, source_policy=None,
        distribution_package="/unused/package.zip", bundle="sample",
    )
    assert execution.run_install(args, as_json=True) == 1
    error = json.loads(capsys.readouterr().err.strip().splitlines()[-1])["error"]
    assert error["data"]["reason"] == "personal_private_requires_source_policy"


def _update_execution(monkeypatch, tmp_path, *, publications, status=0, modes=None):
    import json
    from types import SimpleNamespace
    from okg.substrate.distributions import install_execution as execution
    from okg.substrate.cli import main as cli
    selected = SimpleNamespace(ordered=("predecessor", "documents"), runnable={
        name: SimpleNamespace(sync=None) for name in ("predecessor", "documents")})
    monkeypatch.setattr("okg.substrate.deployments.resolve_deployment",
        lambda path: SimpleNamespace(name="test", source_registry=tmp_path / "source_registry.yaml"))
    monkeypatch.setattr("okg.substrate.sources.registry.load_source_registry", lambda path: {})
    def plan(registry, *, include):
        assert include == {"documents"}
        return selected
    monkeypatch.setattr("okg.substrate.ingest.planning.plan_ingest_sources", plan)
    observed = {}
    def ingest(args, *, as_json):
        observed["ingest"] = vars(args)
        print(json.dumps({"publishes": publications, "error": {"code": "source_failed"}}))
        return status
    monkeypatch.setattr(cli, "_cmd_ingest", ingest)
    def reader(**kwargs):
        observed["reader"] = kwargs
        return {"result_count": 1}
    monkeypatch.setattr(execution, "_verify_installed_reader", reader)
    identity = PackagedReleaseIdentity(*(["sha256:" + "a" * 64] * 4), "installed-profile/v1")
    return lambda: execution._execute_update(deployment=tmp_path, source="documents", dsn="unused",
        query="changed record", source_modes=modes or {}, identity=identity, database={}), observed


def test_daily_update_reader_uses_last_returned_publish_and_keeps_predecessors(monkeypatch, tmp_path):
    run, observed = _update_execution(monkeypatch, tmp_path, publications=[
        {"status": "published", "generation_id": "predecessor-gen"},
        {"status": "published", "generation_id": "changed-gen"}])
    result = run()
    assert observed["ingest"]["include"] == "documents"
    assert observed["ingest"]["inline"] is True
    assert observed["reader"]["generation"] == "changed-gen"
    assert observed["reader"]["query"] == "changed record"
    assert result["sources"] == ["predecessor", "documents"]


@pytest.mark.parametrize("publications,status", [([], 0), ([{"status": "blocked", "generation_id": "bad"}], 0),
    ([{"status": "published", "generation_id": "old"}], 1), ([{"status": "published"}], 0)])
def test_daily_update_never_substitutes_old_read_for_missing_or_failed_publish(monkeypatch, tmp_path, publications, status):
    run, observed = _update_execution(monkeypatch, tmp_path, publications=publications, status=status)
    with pytest.raises(SubstrateError):
        run()
    assert "reader" not in observed


def test_daily_update_refuses_incompatible_predecessor_modes_before_ingest(monkeypatch, tmp_path):
    run, observed = _update_execution(monkeypatch, tmp_path, publications=[],
        modes={"predecessor": "release_new", "documents": "scope_complete"})
    with pytest.raises(SubstrateError) as refused:
        run()
    assert refused.value.data["reason"] == "update_source_modes_incompatible"
    assert not observed


def test_first_install_observes_no_replay_predecessor(tmp_path, monkeypatch):
    args, observations, receipt = _drive_prepared_install(tmp_path, monkeypatch)
    assert observations == []
    assert args._packaged_replay_predecessor is None
    assert receipt["state"] == "published" and receipt["replay_predecessor"] is None


@pytest.mark.parametrize("state", [
    {"state": "prepared"},
    {"state": "published", "published": True, "reader": {}, "provision": {}},
])
def test_repeat_install_binds_observed_populated_predecessor(tmp_path, monkeypatch, state):
    args, observations, receipt = _drive_prepared_install(
        tmp_path, monkeypatch, existing_receipt=state, observation=("gen-prior", 12, 7))
    assert observations == ["test"]
    expected = {"generation_id": "gen-prior", "node_count": 12, "edge_count": 7}
    assert args._packaged_replay_predecessor == expected
    assert receipt["replay_predecessor"] == expected


@pytest.mark.parametrize("observation", [(None, None, None), ("gen-prior", None, None), ("gen-prior", 0, 0),
                                         ("gen-prior", 5, 0), ("gen-prior", True, True)])
def test_repeat_install_passes_no_context_without_populated_generation(tmp_path, monkeypatch, observation):
    args, observations, receipt = _drive_prepared_install(
        tmp_path, monkeypatch, existing_receipt={"state": "prepared"}, observation=observation)
    assert observations == ["test"]
    assert args._packaged_replay_predecessor is None
    assert receipt["replay_predecessor"] is None


def test_staged_repeat_does_not_observe_replay_predecessor(tmp_path, monkeypatch):
    args, observations, receipt = _drive_prepared_install(
        tmp_path, monkeypatch, existing_receipt={"state": "prepared"}, observation=("gen-prior", 12, 7),
        no_publish=True)
    assert observations == []
    assert args._packaged_replay_predecessor is None
    assert receipt["state"] == "staged" and receipt["published"] is False


def test_replay_predecessor_refused_outside_verified_packaged_install(tmp_path, monkeypatch, capsys):
    import argparse
    import json
    from okg.substrate.cli import provision as provision_mod
    monkeypatch.setattr("okg.substrate.deployments.find_deployment_dir", lambda name: str(tmp_path))
    monkeypatch.setattr("okg.substrate.distributions.packaged_release.current_verified_release", lambda path: None)
    def forbidden(*args, **kwargs):
        pytest.fail("deployment path must not be injected for a refused request")
    monkeypatch.setattr("okg.substrate.deployments.inject_deployment_path", forbidden)
    rc = provision_mod._cmd_provision(argparse.Namespace(
        deployment="test", _packaged_replay_predecessor={"generation_id": "gen-prior", "node_count": 1, "edge_count": 1}),
        as_json=True)
    assert rc != 0
    captured = capsys.readouterr()
    assert "verified packaged install" in json.loads(captured.out or captured.err)["error"]["message"]


@pytest.mark.parametrize("generation", [None, "published-generation"])
def test_retained_search_uses_existing_scope_without_bootstrap(tmp_path, monkeypatch, generation):
    existing = {"state": "published", "published": True, "reader": {}, "provision": {}}
    args, observations, result = _drive_prepared_install(tmp_path, monkeypatch,
        existing_receipt=existing, search_query="needle", search_generation=generation)
    assert args["query"] == "needle" and args["generation"] == generation
    assert args["deployment"] == tmp_path / "instance/configuration"
    assert observations == []


@pytest.mark.parametrize("existing", [None, {"state": "prepared"}])
def test_retained_search_requires_completed_installation(tmp_path, monkeypatch, existing):
    with pytest.raises(SubstrateError) as refused:
        _drive_prepared_install(tmp_path, monkeypatch, existing_receipt=existing, search_query="needle")
    assert refused.value.data["reason"] == "search_requires_published_installation"
    assert not (tmp_path / "instance/configuration").exists()


@pytest.mark.parametrize("selection", [
    {"search_generation": "old"}, {"search_query": " "},
    {"search_query": "needle", "search_generation": " "},
    {"search_query": "needle", "update_source": "documents"},
    {"search_query": "needle", "no_publish": True},
])
def test_retained_search_rejects_invalid_selection_before_target(tmp_path, monkeypatch, selection):
    with pytest.raises(SubstrateError):
        _drive_prepared_install(tmp_path, monkeypatch, **selection)
    assert not (tmp_path / "instance").exists()


@pytest.mark.parametrize("flag", ["dry_run", "scaffold_only", "no_publish"])
def test_retained_search_refuses_staging_before_preparation(monkeypatch, capsys, flag):
    from argparse import Namespace
    from okg.substrate.distributions import install_execution as execution
    monkeypatch.setattr(execution, "active_bootstrap", lambda: None)
    assert execution.run_install(Namespace(search_query="needle", **{flag: True}), as_json=True) == 1
    assert "search_mode_unsupported" in capsys.readouterr().err


def _search_execution(monkeypatch, tmp_path, *, result, mode="enforce", manifest_mode="enforce", status=0):
    import json
    from types import SimpleNamespace
    from okg.substrate.distributions import install_execution as execution
    from okg.substrate.cli import _read_side
    monkeypatch.setattr("okg.substrate.deployments.resolve_deployment", lambda path: SimpleNamespace(name="test"))
    monkeypatch.setattr("okg.substrate.nomos.runtime_enforcement.deployment_nomos_enforcement_mode", lambda name: mode)
    calls, journals = [], []
    def cmd_search(args, *, as_json):
        calls.append(vars(args))
        print(json.dumps(result))
        return status
    monkeypatch.setattr(_read_side, "cmd_search", cmd_search)
    monkeypatch.setattr(execution, "_stage_receipt", lambda *args: journals.append(args))
    def run(generation=None):
        return execution._execute_search(
            deployment=tmp_path, dsn="selected", query="private query",
            generation=generation, manifest={"nomos": {"runtime_enforcement": manifest_mode}},
        )
    return run, calls, journals


@pytest.mark.parametrize("rows", [[], [{"node_id": "doc", "attrs": {"body": "private content"}, "source_revision": {"v": 2}}]])
@pytest.mark.parametrize("generation", [None, "published"])
def test_retained_search_preserves_allowed_results_and_metadata_only_journals(monkeypatch, tmp_path, rows, generation):
    result = {"nomos_effect": "allow", "nomos_decision_id": "decision", "generation_id": "published", "results": rows}
    run, calls, journals = _search_execution(monkeypatch, tmp_path, result=result)
    assert run(generation) == result
    assert calls[0]["deployment"] == "test" and calls[0]["dsn"] == "selected"
    assert calls[0]["generation_id"] == generation
    assert "source_revision" in calls[0]["select"][0]
    assert "private query" not in repr(journals) and "private content" not in repr(journals)
    assert journals[0][2]["result_count"] == len(rows)


@pytest.mark.parametrize("mode,manifest_mode", [("off", "enforce"), ("audit", "enforce"), ("enforce", "off"), ("enforce", "audit")])
def test_retained_search_refuses_unenforced_policy_before_reader(monkeypatch, tmp_path, mode, manifest_mode):
    run, calls, _ = _search_execution(monkeypatch, tmp_path, result={}, mode=mode, manifest_mode=manifest_mode)
    with pytest.raises(SubstrateError) as refused:
        run()
    assert refused.value.data["reason"] == "search_requires_enforced_policy"
    assert not calls


@pytest.mark.parametrize("result,status", [
    ({"results": []}, 0), ({"nomos_effect": "allow", "results": []}, 0),
    ({"nomos_effect": "deny", "nomos_decision_id": "decision", "results": []}, 0),
    ({"error": {"code": "nomos_denied"}}, 1),
    ({"error": {"code": "generation_not_found"}}, 1),
])
def test_retained_search_never_accepts_missing_authority_or_failed_reader(monkeypatch, tmp_path, result, status):
    run, _, _ = _search_execution(monkeypatch, tmp_path, result=result, status=status)
    with pytest.raises(SubstrateError):
        run("foreign-or-blocked")


def test_retained_search_environment_cannot_replace_unenforced_manifest(monkeypatch, tmp_path):
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    run, calls, _ = _search_execution(monkeypatch, tmp_path, result={}, manifest_mode="off")
    with pytest.raises(SubstrateError) as refused:
        run()
    assert refused.value.data["reason"] == "search_requires_enforced_policy"
    assert not calls


@pytest.fixture
def readback(monkeypatch, tmp_path):
    """Run `_verify_installed_reader` against stubbed collaborators.

    Everything the readback touches other than the count comparison and the
    registry consultation is held healthy, so a refusal can only come from the
    record/relationship rule under test.
    """
    from contextlib import nullcontext
    from types import SimpleNamespace
    from okg.substrate import deployments
    from okg.substrate.cli import _read_side, provision
    from okg.substrate.db import connection
    from okg.substrate.distributions import install_execution as execution
    from okg.substrate.nomos import doctor
    from okg.substrate.sources import registry as registry_module
    monkeypatch.setattr(_read_side, "_resolve_cli_read_subject",
                        lambda **kw: ("subject:human_operator", {"source": "manifest_read_principal"}))
    monkeypatch.setattr(connection, "connect", lambda *a, **kw: nullcontext(object()))
    monkeypatch.setattr(doctor, "source_lineage_readiness_report", lambda *a, **kw: {
        "ok": True, "runtime_enforcement_mode": "enforce", "active_policy_count": 1})
    monkeypatch.setattr(execution, "_capture", lambda *a, **kw: _READER_RESULT)
    monkeypatch.setattr(deployments, "resolve_deployment", lambda name: SimpleNamespace(
        name=str(name), source_registry=tmp_path / "source_registry.yaml"))

    def run(*, counts, policy, declared=None, extra_policy=None, registry_error=None):
        monkeypatch.setattr(provision, "_generation_row_counts", lambda *a, **kw: counts)
        def load_registry(*a, **kw):
            if registry_error is not None:
                raise registry_error
            return _registry_entries(policy, extra_policy)
        monkeypatch.setattr(registry_module, "load_source_registry", load_registry)
        return execution._verify_installed_reader(
            dsn="unused", deployment="demo", generation="generation:1", query="bootstrap",
            declares_relationships=declared)

    return run


def test_records_only_registry_completes_reader_verification(readback):
    receipt = readback(counts=("generation:1", 2, 0), policy=_RECORDS_ONLY_POLICY)
    assert receipt["results"][0]["source_record_id"] == {"id": "doc:1"}


@pytest.mark.parametrize("counts,policy,case", [
    (("generation:1", 0, 0), _RECORDS_ONLY_POLICY, "records_absent"),
    (("generation:1", 0, 3), _RELATIONSHIP_POLICY, "records_absent"),
    (("generation:1", 2, 0), _RELATIONSHIP_POLICY, "relationships_absent"),
    (("generation:1", 2, 0), None, "relationships_absent"),
    (("foreign", 2, 1), _RECORDS_ONLY_POLICY, "generation_identity_mismatch"),
    (("generation:1", None, None), _RECORDS_ONLY_POLICY, "counts_unavailable"),
])
def test_readback_refuses_absent_records_or_declared_relationships(readback, counts, policy, case):
    with pytest.raises(SubstrateError) as caught:
        readback(counts=counts, policy=policy)
    assert caught.value.data["reason"] == "published_generation_readback_failed"
    assert caught.value.data["case"] == case


# A published node-only payload that satisfies every part of the uniform gate
# except the row-delta rule under test, lock receipt included.
_PUBLISHED_NODE_ONLY_PAYLOAD = {
    "status": "published",
    "generation_id": "gen-1",
    "nodes_added": 5,
    "edges_added": 0,
    "timing_details": {"lock": {"hold_ms": 10, "timeout_ms": 60000}},
}


def test_unreadable_source_registry_refuses_with_its_own_reason(readback, monkeypatch):
    """An unreadable registry cannot decide the rule, so it refuses by name."""
    with pytest.raises(SubstrateError) as caught:
        readback(counts=("generation:1", 2, 0), policy=None, declared=None,
                 registry_error=OSError("source_registry.yaml is missing"))
    assert caught.value.data["reason"] == "deployment_source_registry_unreadable"


@pytest.mark.parametrize("policy,records_only", [
    (_RECORDS_ONLY_POLICY, True),
    (_RELATIONSHIP_POLICY, False),
    (None, False),
])
def test_installer_hands_provision_the_registry_declaration(tmp_path, monkeypatch, policy, records_only):
    args, _observations, _receipt = _drive_prepared_install(
        tmp_path, monkeypatch, registry_policy=policy)
    assert args._packaged_records_only is records_only


@pytest.mark.parametrize("policy,predecessor", [
    (_RECORDS_ONLY_POLICY, {"generation_id": "gen-prior", "node_count": 5, "edge_count": 0}),
    (_RELATIONSHIP_POLICY, None),
])
def test_records_only_repeat_binds_a_predecessor_that_holds_no_edges(tmp_path, monkeypatch, policy, predecessor):
    """P1: without this, a records-only deployment never reaches the
    zero-addition replay arm, so its second install refuses."""
    args, observations, _receipt = _drive_prepared_install(
        tmp_path, monkeypatch, registry_policy=policy,
        existing_receipt={"state": "published", "published": True, "reader": {}, "provision": {}},
        observation=("gen-prior", 5, 0))
    assert observations == ["test"]
    assert args._packaged_replay_predecessor == predecessor


@pytest.mark.parametrize("records_only,expected_ok", [(True, True), (False, False)])
def test_provision_gate_accepts_a_node_only_delta_only_when_declared(monkeypatch, records_only, expected_ok):
    """The gate that actually refuses a records-only install, at its own level."""
    from okg.substrate import publish_proof
    from okg.substrate.cli.provision import _provision_publish_proof
    monkeypatch.setattr(publish_proof, "_publish_dlq_count", lambda *a, **kw: (0, {}))
    verdict = _provision_publish_proof(
        "postgresql://unused",
        {**_PUBLISHED_NODE_ONLY_PAYLOAD},
        deployment_name="test",
        records_only=records_only,
    )
    assert verdict["ok"] is expected_ok
    if not expected_ok:
        assert "did not add live node and edge rows" in verdict["reason"]


def test_provision_gate_still_refuses_a_zero_total_delta_when_records_only(monkeypatch):
    from okg.substrate import publish_proof
    from okg.substrate.cli.provision import _provision_publish_proof
    monkeypatch.setattr(publish_proof, "_publish_dlq_count", lambda *a, **kw: (0, {}))
    verdict = _provision_publish_proof(
        "postgresql://unused",
        {**_PUBLISHED_NODE_ONLY_PAYLOAD, "nodes_added": 0},
        deployment_name="test",
        records_only=True,
    )
    assert verdict["ok"] is False
    assert "did not add any live node or edge rows" in verdict["reason"]


@pytest.mark.parametrize("policy,records_only", [
    (_RECORDS_ONLY_POLICY, True),
    (_RELATIONSHIP_POLICY, False),
])
def test_staged_resume_judges_its_publish_by_the_same_declaration(tmp_path, monkeypatch, policy, records_only):
    """`install --no-publish` then `install` runs its own publish proof before
    the ordinary provision. Without the declaration it applies the strict rule,
    so the documented staging workaround still refused a records-only bundle."""
    from okg.substrate.distributions.install_stage import InstallStage
    stage = InstallStage("test", "default", None, "token", ("admission",), "a" * 64)
    proofs = []
    _drive_prepared_install(
        tmp_path, monkeypatch, registry_policy=policy, stage_proofs=proofs,
        existing_receipt={"state": "staged", "published": False, "provision": {},
                          "stage": stage.to_dict()})
    assert len(proofs) == 1
    assert proofs[0]["records_only"] is records_only


def test_an_unselected_edge_declaring_source_keeps_the_strict_rule(readback):
    """Scope choice (a): the rule reads every registry entry, not only the
    sources this install selected."""
    with pytest.raises(SubstrateError) as caught:
        readback(counts=("generation:1", 2, 0), policy=_RECORDS_ONLY_POLICY,
                 extra_policy=_RELATIONSHIP_POLICY)
    assert caught.value.data["case"] == "relationships_absent"


@pytest.mark.parametrize("records_only,hint_names_declaration", [(False, True), (True, False)])
def test_publish_gate_verdict_says_which_rule_judged_it(monkeypatch, records_only, hint_names_declaration):
    from okg.substrate import publish_proof
    from okg.substrate.cli.provision import _provision_publish_proof
    monkeypatch.setattr(publish_proof, "_publish_dlq_count", lambda *a, **kw: (0, {}))
    verdict = _provision_publish_proof(
        "postgresql://unused",
        {**_PUBLISHED_NODE_ONLY_PAYLOAD},
        deployment_name="test",
        records_only=records_only,
    )
    assert verdict["records_only"] is records_only
    assert verdict["declares_relationships"] is not records_only
    names = "declares relationship output" in (verdict.get("fix_hint") or "")
    assert names is hint_names_declaration


@pytest.mark.parametrize("failure", ["DeploymentConfigError", "DeploymentResolutionError"])
def test_unreadable_deployment_manifest_names_the_manifest_not_the_registry(readback, monkeypatch, failure):
    """A broken deployment.yaml and a broken source_registry.yaml are different
    files, so they refuse with different reasons. The two deployment errors are
    siblings, not subclasses, so both are named."""
    from okg.substrate import deployments
    error = getattr(deployments, failure)
    def unreadable(name):
        raise error("deployment.yaml is malformed or unresolvable")
    monkeypatch.setattr(deployments, "resolve_deployment", unreadable)
    with pytest.raises(SubstrateError) as caught:
        readback(counts=("generation:1", 2, 0), policy=None, declared=None)
    assert caught.value.data["reason"] == "deployment_manifest_unreadable"


def test_declaration_stage_runs_before_any_database_change(tmp_path, monkeypatch):
    """The stage journal pins where the declaration is read: before the schema
    migration, and so before the staged resume that publishes from a stage."""
    import json
    from okg.substrate.distributions import install_execution as execution
    _drive_prepared_install(tmp_path, monkeypatch, registry_policy=_RECORDS_ONLY_POLICY)
    journals = sorted((tmp_path / "instance").glob("install-stages-*.json"))
    assert journals, "the installer wrote no stage journal"
    journal = json.loads(journals[-1].read_text())
    assert journal["schema"] == "okg.install-stage-receipts/v1"
    stages = [entry["stage"] for entry in journal["stages"]]
    assert "output_declaration" in stages
    assert stages.index("output_declaration") < stages.index("database_migrations")
    assert stages.index("output_declaration") < stages.index("provision")
    assert execution is not None


def test_unreadable_registry_refuses_the_install_before_it_publishes(tmp_path, monkeypatch):
    """At install level, not just at readback: the declaration is read before
    the database is touched, so an unreadable registry stops the whole run."""
    with pytest.raises(SubstrateError) as caught:
        _drive_prepared_install(tmp_path, monkeypatch, registry_policy=_RECORDS_ONLY_POLICY,
                                registry_error=OSError("source_registry.yaml is missing"))
    assert caught.value.data["reason"] == "deployment_source_registry_unreadable"
    assert caught.value.data["stage"] == "output_declaration"


_RELEASE_IN_SUBPROCESS = """
import hashlib, json, sys
from pathlib import Path
from okg.distributions import build_release_entry

root, output = Path(sys.argv[1]), Path(sys.argv[2])
house = root / "wheelhouse"


def pin(path):
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


entry = build_release_entry(
    framework=root / "framework.whl", framework_digest=pin(root / "framework.whl"),
    consumer=root / "consumer.whl", consumer_digest=pin(root / "consumer.whl"),
    consumer_package="okg_example_records",
    wheelhouse=house, wheelhouse_digest=pin(house / "manifest.json"),
    installer=root / "pip.whl", installer_digest=pin(root / "pip.whl"),
    output=output)
print(json.loads((entry.parent / "release.json").read_text())["install_entry_sha256"])
"""


def test_release_entry_is_byte_identical_across_processes(tmp_path):
    """Two producer runs under different hash seeds write the same entry.

    The generated entry carries the set of pin names that are release-relative
    paths. Written as an unsorted ``repr`` of a set, it moved with Python's
    string-hash randomization, so identical inputs produced a different
    ``install_entry_sha256`` on every run and a rebuilt release could not be
    compared to the one it replaced.
    """
    import hashlib
    import json
    import os
    import subprocess
    import sys
    import zipfile
    root = tmp_path.resolve()
    framework = root / "framework.whl"
    with zipfile.ZipFile(framework, "w") as wheel:
        wheel.writestr("okg/substrate/distributions/install_launcher.py", "raise SystemExit(0)")
    (root / "consumer.whl").write_bytes(b"consumer")
    (root / "pip.whl").write_bytes(b"trusted-pip")
    wheelhouse = root / "wheelhouse"
    (wheelhouse / "wheels").mkdir(parents=True)
    dependency = wheelhouse / "wheels" / "dep.whl"
    dependency.write_bytes(b"dependency")
    requirements = wheelhouse / "runtime-wheels.txt"
    requirements.write_text("dep==1 \\\n    --hash=sha256:test\n")

    def pin(path):
        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    (wheelhouse / "manifest.json").write_text(json.dumps(
        {"requirements_digest": pin(requirements),
         "entries": [{"wheel": dependency.name, "wheel_digest": pin(dependency)}]}))

    digests, bodies = [], []
    for seed in ("0", "424242"):
        output = root / ("release-" + seed)
        environment = dict(os.environ)
        environment["PYTHONHASHSEED"] = seed
        completed = subprocess.run(
            [sys.executable, "-c", _RELEASE_IN_SUBPROCESS, str(root), str(output)],
            capture_output=True, text=True, check=True, env=environment)
        digests.append(completed.stdout.strip())
        bodies.append((output / "install.py").read_bytes())
    assert digests[0] == digests[1]
    assert bodies[0] == bodies[1]
    assert hashlib.sha256(bodies[0]).hexdigest() == digests[0]
