"""Hermetic package/installed-runtime proof for #1185; no conformance pass.

Nothing the isolated interpreter runs is checkout-backed. OKG itself is
built into a wheel and installed non-editably, its dependencies resolved
from `uv.lock`'s exported hash-pinned requirement set; the connector runner
is copied out of the checkout and executed by absolute path from a working
directory outside it, and no entry of that interpreter's `sys.path` lies
inside the checkout. A separate neutral public-SDK wheel is built,
explicitly installed in the same interpreter, and imported through the
admitted strict registry after its temporary build tree is deleted.
The v2 distribution contains registration JSON, not executable Python. The
install plan neither installs nor binds the runtime wheel; we record the
wheel and archive identities separately.

The package's ontology, connector registration, and graph-backed playbook
are exercised through real composition, SourceRunner, and publication.
Live-tool, action, automation and enricher assets have packaging coverage
only: they are verified declarations, never executed here. The two bundles
share one ontology; each is planned independently, not applied together.

The exact published shape is two lab_unit nodes, three lab_run nodes and
three contains edges. A fresh adapter resumes its measured checkpoint and
appends no duplicate facts. Test-owned catalog writer authority supports
this substrate test; it is not a sealed external deployment release.

Current conformance wire identities and the package authority producer
refuse v2. The test asserts that refusal instead of relabeling v2 bytes as
v1, inventing policy admission digests, or issuing a passing receipt. Real
conformance, sealed-release lifecycle/recovery, health, legacy inventory and
external acknowledgement remain outside this proof (#1185/#1794). Installing
OKG from a wheel does not add them: the arm still seals no release and opens
no MCP read surface.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import sysconfig
import uuid
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote, urlencode

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from okg.substrate.catalog.module_providers import (
    CrossOwnerSubtypeCollision,
    builtin_module_providers,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.package_module_provider import package_module_providers
from okg.substrate.contracts.deployment_product import (
    CompatibilityMetadata,
    DistributionRelease,
    OwnerQualifiedReference,
)
from okg.deployment import (
    ConnectorRegistration,
    SdkVersionRange,
    registration_registry_entry,
)
from okg.substrate.distributions.conformance import (
    DistributionConformanceAuthorityError,
    derive_distribution_conformance_authority,
)
from okg.substrate.contracts.deployment_product.typed_assets import TypedAsset
from okg.substrate.distributions import (
    AssetLocator,
    DistributionObject,
    DistributionPackageManifest,
    DistributionPackageProvenance,
    DistributionPackageSource,
    ModuleDeclaration,
    OkgCompatibility,
    PackagedAsset,
    build_distribution_package,
    canonical_release_digest,
    verify_distribution_package,
)
from okg.substrate.distributions import package as package_impl
from okg.substrate.distributions.install_plan import (
    InstallBundleUnknown,
    InstallLockDrift,
    InstallOkgIncompatible,
    InstallPackageUnverified,
    resolve_install_plan,
    write_install_plan,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer


pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[3]
_OWNER = "archi-test"
_MODULE = "lab_units"
_MODULE_ID = f"{_OWNER}:{_MODULE}"
_VERSION = "1.0.0"
_OPS_BUNDLE = "lab-ops"
_ANALYSIS_BUNDLE = "lab-analysis"
_INSTANCE = "lab-hermetic"
_SOURCE_NAME = "lab-hermetic.lab_units"
_DEPLOYMENT = "default"
_SECRET_ENV = "ARCHI_TEST_LAB_TOKEN"
_ZERO_DIGEST = "sha256:" + "0" * 64
_ASSET_SCHEMA = "okg.deployment_product.typed_asset/v1"
_REVISION = "0" * 40

# The fixture's expected shape, asserted end to end rather than counted
# from whatever the connector happened to emit.
_EXPECTED_LAB_UNITS = 2
_EXPECTED_LAB_RUNS = 3
_EXPECTED_CONTAINS_EDGES = 3
_EXPECTED_NODES = _EXPECTED_LAB_UNITS + _EXPECTED_LAB_RUNS


# --------------------------------------------------------------------------
# Packaged asset bytes
# --------------------------------------------------------------------------

_SCHEMA_YAML = b"""id: https://example.invalid/okg/modules/lab_units
name: lab_units
title: Lab units
description: Neutral hermetic fixture ontology for the #1185 conformance arm.
prefixes:
  linkml: https://w3id.org/linkml/
  lab: https://example.invalid/okg/modules/lab_units/
default_range: string
imports:
- linkml:types
classes:
  LabUnit:
    description: A bench that runs measurements.
    class_uri: lab:LabUnit
    attributes:
      lab_unit_id:
        required: true
      label:
    annotations:
      archetype: entity
      transient: 'false'
  LabRun:
    description: One measurement run on a bench.
    class_uri: lab:LabRun
    attributes:
      lab_run_id:
        required: true
      label:
    annotations:
      archetype: event
      transient: 'false'
  EdgeNarrowings:
    description: Intra-module narrowings for lab_units.
    annotations:
      okg_edge_narrowings: 'true'
    attributes:
      lab_unit_contains_lab_run:
        annotations:
          edge_archetype: contains
          src_subtypes: LabUnit
          dst_subtypes: LabRun
"""

_RUNTIME_FIXTURE = (
    _REPO_ROOT / "tests/fixtures/deployment_products/lab_connector_runtime"
)
_REGISTRATION = ConnectorRegistration(
    connector_type="lab_units",
    version="0.1.0",
    module="archi_test_lab_runtime.connector",
    class_name="LabUnitsSource",
    ownership_id=_SOURCE_NAME,
    source_class="mutable_api",
    trust_label="curated",
    sdk_range=SdkVersionRange("1.0.0", "2.0.0"),
    auth_modes=("none",),
    capabilities=("checkpoint_resume",),
    required_modules=(_MODULE_ID,),
    output_signature={
        "nodes": [{"subtype": "lab_unit"}, {"subtype": "lab_run"}],
        "edges": [
            {
                "src_subtype": "lab_unit",
                "edge_type": "contains",
                "dst_subtype": "lab_run",
            }
        ],
    },
    record_identity_kind="remote_id",
    record_identity_fields=("id",),
    source_revision_kind="version_token",
    deletion_semantics="explicit_delete",
    publication_mode="published_generation",
)
_CONNECTOR_JSON = json.dumps(_REGISTRATION.as_asset_payload(), sort_keys=True).encode()

_PLAYBOOK_JSON = json.dumps(
    {
        "schema": "archi-test.playbook/v1",
        "playbook_id": "lab-unit-census",
        "title": "Lab unit census",
        "read_only": True,
        "query": (
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE subtype = 'lab_unit' ORDER BY node_id"
        ),
        "minimum_rows": _EXPECTED_LAB_UNITS,
    },
    sort_keys=True,
).encode()

_LIVE_TOOL_JSON = json.dumps(
    {
        "schema": "archi-test.live_tool/v1",
        "tool_id": "lab-bench-status",
        "mode": "read_only",
        "side_effecting": False,
        "connection_ref": f"{_OWNER}:lab-bench-connection",
        "credential_reference": f"env:{_SECRET_ENV}",
    },
    sort_keys=True,
).encode()

_ACTION_TYPE_JSON = json.dumps(
    {
        "schema": "archi-test.action_type/v1",
        "action_type_id": "lab-bench-recalibrate",
        "side_effecting": True,
        "requires_approval": True,
        "payload_fields": ["lab_unit_id", "target_offset"],
    },
    sort_keys=True,
).encode()

_AUTOMATION_JSON = json.dumps(
    {
        "schema": "archi-test.automation/v1",
        "automation_id": "lab-bench-sweep",
        "heartbeat_seconds": 60,
        "budget_units": 10,
        "principal_ref": f"{_OWNER}:lab-sweep-principal",
    },
    sort_keys=True,
).encode()

_ENRICHER_RULES_YAML = b"""rules:
- id: lab_run_label_references_lab_unit
  src:
    subtype: lab_run
    field: label
  dst:
    subtype: lab_unit
    field: lab_unit_id
  matcher: word_boundary
  edge: contains
  bidirectional: false
"""


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _asset(asset_id: str, payload: bytes, *, kind: str) -> TypedAsset:
    return TypedAsset(
        owner=_OWNER,
        asset_id=asset_id,
        kind=kind,
        plane="semantic-catalog",
        version="v1",
        digest=_digest(payload),
        schema=_ASSET_SCHEMA,
        merge_identity=asset_id,
        merge_rule="refuse",
        compatibility={"contract_versions": ["v1"], "capabilities": ["public"]},
        provenance={
            "source": "package",
            "repository_url": "https://github.com/example/lab-units",
            "revision": _REVISION,
            "artifact_ref": f"{_OWNER}:{asset_id}",
        },
    )


def _ref(asset: TypedAsset) -> OwnerQualifiedReference:
    return OwnerQualifiedReference(
        owner=asset.owner,
        id=asset.asset_id,
        kind=asset.kind,
        version=asset.version,
        digest=asset.digest,
    )


def _bundle(bundle_id: str, refs: tuple[OwnerQualifiedReference, ...]) -> Any:
    provisional = package_impl.ProductBundle(
        bundle_id=bundle_id,
        version="v1",
        distribution_id=_OWNER,
        distribution_version=_VERSION,
        bundle_digest=_ZERO_DIGEST,
        assets=refs,
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("public",)
        ),
    )
    return provisional.model_copy(
        update={"bundle_digest": package_impl.canonical_bundle_digest(provisional)}
    )


#: Every packaged asset: id -> (kind, bytes). The ontology module is shared
#: by both bundles, which is the "two bundles, one schema" shape #1185 asks
#: for; the other assets split across them.
_ASSET_BYTES: dict[str, tuple[str, bytes]] = {
    "lab-units-schema": ("ontology_schema", _SCHEMA_YAML),
    "lab-units-connector": ("connector", _CONNECTOR_JSON),
    "lab-unit-census": ("playbook", _PLAYBOOK_JSON),
    "lab-bench-status": ("tool", _LIVE_TOOL_JSON),
    "lab-bench-recalibrate": ("policy", _ACTION_TYPE_JSON),
    "lab-bench-sweep": ("service", _AUTOMATION_JSON),
    "lab-run-links": ("enricher", _ENRICHER_RULES_YAML),
}
_OPS_ASSETS = (
    "lab-units-schema",
    "lab-units-connector",
    "lab-bench-recalibrate",
    "lab-bench-sweep",
)
_ANALYSIS_ASSETS = (
    "lab-units-schema",
    "lab-unit-census",
    "lab-bench-status",
    "lab-run-links",
)


def build_archi_test_package(
    root: Path,
    *,
    schema_payload: bytes = _SCHEMA_YAML,
    okg_compatibility: tuple[str, str] = ("0.1.0", "9.0.0"),
    ghost_bundle_asset: bool = False,
) -> Path:
    """Build the `archi-test` v2 distribution archive under `root`.

    `root` is the authoring tree; it is deleted in one test to prove the
    plan does not need it. `ghost_bundle_asset` makes a bundle name an
    asset the package does not carry, for the missing-declared-asset arm.
    """

    authoring = root / "authoring"
    authoring.mkdir(parents=True)
    output = root / "packages"
    output.mkdir()

    payloads = dict(_ASSET_BYTES)
    payloads["lab-units-schema"] = ("ontology_schema", schema_payload)

    sources: dict[str, DistributionPackageSource] = {}
    assets: list[TypedAsset] = []
    packaged: list[PackagedAsset] = []
    objects: list[DistributionObject] = []
    for asset_id, (kind, payload) in payloads.items():
        path = authoring / asset_id
        path.write_bytes(payload)
        asset = _asset(asset_id, payload, kind=kind)
        assets.append(asset)
        packaged.append(
            PackagedAsset(
                asset=asset,
                locator=AssetLocator(object_digest=_digest(payload)),
                uncompressed_size=len(payload),
            )
        )
        objects.append(
            DistributionObject(
                digest=_digest(payload),
                size=len(payload),
                media_type="application/octet-stream",
            )
        )
        sources[_digest(payload)] = DistributionPackageSource(root=authoring, path=path)

    ref_by_id = {asset.asset_id: _ref(asset) for asset in assets}
    ops_refs = tuple(ref_by_id[name] for name in _OPS_ASSETS)
    analysis_refs = tuple(ref_by_id[name] for name in _ANALYSIS_ASSETS)
    if ghost_bundle_asset:
        analysis_refs = (
            *analysis_refs,
            OwnerQualifiedReference(
                owner=_OWNER,
                id="lab-ghost",
                kind="playbook",
                version="v1",
                digest=_digest(b"lab-ghost"),
            ),
        )
    bundles = (_bundle(_OPS_BUNDLE, ops_refs), _bundle(_ANALYSIS_BUNDLE, analysis_refs))

    release_refs = (ref_by_id["lab-units-schema"],)
    release_provisional = DistributionRelease(
        distribution_id=_OWNER,
        version=_VERSION,
        origin="external",
        release_digest=_ZERO_DIGEST,
        assets=release_refs,
        bundle_ids=tuple(item.bundle_id for item in bundles),
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("public",)
        ),
    )
    provisional = DistributionPackageManifest(
        schema_version=package_impl.SCHEMA_V2,
        distribution_release=release_provisional,
        bundles=bundles,
        assets=tuple(packaged),
        objects=tuple(objects),
        provenance=DistributionPackageProvenance(
            repository_url="https://github.com/example/lab-units",
            revision=_REVISION,
            builder="okg-1185-hermetic-arm",
        ),
        okg_compatibility=OkgCompatibility(
            min_version=okg_compatibility[0],
            max_version_exclusive=okg_compatibility[1],
        ),
        module_declarations=(
            ModuleDeclaration(
                module_id=_MODULE_ID,
                asset_refs=(ref_by_id["lab-units-schema"],),
            ),
        ),
    )
    manifest = cast(
        DistributionPackageManifest,
        provisional.model_copy(
            update={
                "distribution_release": release_provisional.model_copy(
                    update={"release_digest": canonical_release_digest(provisional)}
                )
            }
        ),
    )
    return build_distribution_package(manifest, sources, output)


def _write_instance(path: Path) -> Path:
    """The shared instance declares no credential.

    Both bundles plan from this one file, and a deferred credential may
    only name a declaration the planned bundle selects. The one tool
    declaration this package carries is in the analysis bundle alone, so
    a shared secret here would refuse the ops plan. The credential arm
    uses `_write_credential_instance` against that bundle instead.
    """
    path.write_text(
        json.dumps({"name": _INSTANCE, "modules": []}, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _write_credential_instance(path: Path) -> Path:
    """An instance whose secret names the packaged live-tool declaration."""
    path.write_text(
        json.dumps(
            {
                "name": _INSTANCE,
                "modules": [],
                "secrets": {
                    "lab_token": {
                        "reference": f"env:{_SECRET_ENV}",
                        "declaration": f"{_OWNER}:lab-bench-status",
                    },
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _interpreter_library_env() -> dict[str, str]:
    """Preserve only this Linux interpreter's own shared-library directory."""
    if sys.platform != "linux" or not sysconfig.get_config_var("Py_ENABLE_SHARED"):
        return {}
    prefix = Path(sys.base_prefix).resolve()
    library = sysconfig.get_config_var("LDLIBRARY")
    assert isinstance(library, str) and Path(library).name == library, library
    directory = prefix / "lib"
    binary = directory / library
    assert binary.is_file(), binary
    assert binary.resolve().is_relative_to(prefix), binary
    return {"LD_LIBRARY_PATH": str(directory)}


def _checkout_free_path() -> str:
    """The ambient `PATH` with every entry inside this checkout removed.

    `PATH` does not feed `sys.path`, but a `.venv/bin` inside the checkout is
    still a route back to it for anything the subprocess execs. Dropping
    those entries makes "no variable names the checkout" a statement about
    the whole environment rather than about `PYTHONPATH` alone.
    """

    ambient = os.environ.get("PATH", "/usr/bin:/bin").split(os.pathsep)
    kept = [
        entry
        for entry in ambient
        if entry and not Path(entry).resolve().is_relative_to(_REPO_ROOT)
    ]
    return os.pathsep.join(kept) or "/usr/bin:/bin"


def _scrubbed_env(cwd: Path) -> dict[str, str]:
    """An environment with no route back to the authoring tree or checkout.

    The explicitly provisioned interpreter supplies wheel-installed OKG and
    its dependencies. No Python path is injected, and no `PATH` entry lies
    inside this checkout. Every `OKG_*` variable is dropped so no ambient
    profile, deployments root or DSN supplies a module. Native library lookup
    uses only the selected interpreter’s own directory; arbitrary ambient
    loader paths and preload settings are never inherited.
    """

    env = {
        "PATH": _checkout_free_path(),
        "HOME": str(cwd),
        "TMPDIR": str(cwd),
        "PYTHONDONTWRITEBYTECODE": "1",
        _SECRET_ENV: "not-a-real-token",
    }
    env.update(_interpreter_library_env())
    return env


def _install_plan_subprocess(
    *,
    python: Path,
    package: Path,
    bundle: str,
    instance: Path,
    output: Path,
    cwd: Path,
    replace_lock: bool = False,
) -> subprocess.CompletedProcess[str]:
    argv = [
        str(python),
        # -I for the same reason as the connector runner: no ambient PYTHON*
        # variable, no user site directory, and no working directory on the
        # import path.
        "-I",
        "-m",
        "okg.substrate.cli.main",
        "--json",
        "deployment",
        "install-plan",
        "--package",
        str(package),
        "--bundle",
        bundle,
        "--instance",
        str(instance),
        "--output",
        str(output),
    ]
    if replace_lock:
        argv.append("--replace-lock")
    return subprocess.run(
        argv,
        cwd=cwd,
        env=_scrubbed_env(cwd),
        capture_output=True,
        text=True,
        timeout=600,
    )


def _last_json(text: str) -> dict[str, Any]:
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise AssertionError(f"no JSON envelope in output: {text!r}")


def _checked_run(
    command: Sequence[str],
    *,
    timeout: int,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> str:
    completed = subprocess.run(
        list(command),
        cwd=None if cwd is None else str(cwd),
        env=None if env is None else dict(env),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert completed.returncode == 0, (
        list(command),
        completed.stdout,
        completed.stderr,
    )
    return completed.stdout


#: The isolation probe run from the copied runner's own directory. It is the
#: same shape as the real connector runner below -- a file on disk, executed
#: by absolute path with `-I` from a working directory outside the checkout --
#: so what it reports about `sys.path` is what the runner gets. `-I` keeps the
#: script directory and the working directory off `sys.path` entirely, which
#: is why the recorded path holds only the interpreter's stdlib entries and
#: its site-packages. The runner reports its own `sys.path` too; this one lets
#: the environment tests read it without a database.
_ISOLATION_PROBE_PROGRAM = """
import json
import sys
from pathlib import Path

import okg

print(
    json.dumps(
        {
            "sys_path": list(sys.path),
            "prefix": sys.prefix,
            "okg": str(Path(next(iter(okg.__path__))).resolve()),
        }
    )
)
"""


@pytest.fixture(scope="session")
def okg_wheel(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[dict[str, Any]]:
    """Build OKG's own wheel and export its locked requirements, once.

    The arm installs OKG the way an operator does, from a built wheel, so
    nothing it runs reads `src/okg` out of this checkout. Two harness-side
    facts are worth stating plainly. `uv build` writes its intermediate
    `build/` and `src/okg.egg-info` trees into the checkout while it runs, as
    it always does; the isolated interpreter never sees either, both are
    cleared before the build so a file the working tree has since deleted
    cannot survive an earlier build into this wheel, and both are cleared
    again on teardown so the checkout is left as it was found. And resolving
    the exported requirements reads uv's cache, falling back to the network
    for any locked wheel the cache lacks -- this arm is hermetic with respect
    to the checkout, not with respect to the package index.

    The build and the export run once per session and nothing serializes
    them, so a second pytest session sharing this checkout would race on
    those two intermediate trees. The lane that runs this arm runs it without
    `-n`, and the repository's rule is one pytest session at a time.
    """

    import shutil

    root = tmp_path_factory.mktemp("okg-wheelhouse")
    wheelhouse = root / "wheels"
    requirements = root / "requirements.txt"
    intermediates = (_REPO_ROOT / "build", _REPO_ROOT / "src/okg.egg-info")

    def _clear_intermediates() -> None:
        for path in intermediates:
            shutil.rmtree(path, ignore_errors=True)

    _clear_intermediates()
    _checked_run(
        ["uv", "build", "--wheel", "--out-dir", str(wheelhouse), str(_REPO_ROOT)],
        timeout=900,
    )
    # --locked verifies uv.lock against pyproject.toml rather than accepting a
    # stale lock; --no-emit-project leaves OKG itself out, since the wheel is
    # what supplies it.
    _checked_run(
        [
            "uv",
            "export",
            "--locked",
            "--no-dev",
            "--no-emit-project",
            "--format",
            "requirements-txt",
            "--output-file",
            str(requirements),
        ],
        cwd=_REPO_ROOT,
        timeout=300,
    )
    (wheel,) = wheelhouse.glob("okg-*.whl")
    payload = wheel.read_bytes()
    distribution, version, _ = wheel.name.split("-", 2)
    assert distribution == "okg", wheel.name
    try:
        # The digest is this build's, not a fixed value: the setuptools wheel
        # embeds timestamps, so it is not byte-reproducible across runs. It is
        # recorded per run in the `hermetic_publish_proof` JUnit property.
        yield {
            "wheel": wheel,
            "requirements": requirements,
            "name": wheel.name,
            "version": version,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    finally:
        _clear_intermediates()


@pytest.fixture(scope="module")
def installed_runtime(
    tmp_path_factory: pytest.TempPathFactory, okg_wheel: dict[str, Any]
) -> dict[str, Any]:
    """Install OKG and a separate fixture wheel; neither is checkout-backed."""
    import shutil

    root = tmp_path_factory.mktemp("lab-installed-runtime")
    source = root / "authoring"
    shutil.copytree(_RUNTIME_FIXTURE, source)
    wheelhouse = root / "wheels"
    runtime = root / "venv"
    python = runtime / "bin/python"
    preinstall_probe_program = (
        "import importlib.util; "
        "assert importlib.util.find_spec('yaml') is None; "
        "assert importlib.util.find_spec('okg') is None"
    )
    commands = [
        ["uv", "build", "--wheel", "--out-dir", str(wheelhouse), str(source)],
        ["uv", "venv", "--python", sys.executable, str(runtime)],
        [
            str(python),
            "-I",
            "-c",
            preinstall_probe_program,
        ],
        # Dependencies come from the exported lock and are hash-checked;
        # --require-hashes refuses anything the lock did not pin.
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--require-hashes",
            "-r",
            str(okg_wheel["requirements"]),
        ],
        # OKG arrives as wheel bytes, with no dependency resolution of its
        # own and no link back to `src/okg`.
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--no-deps",
            str(okg_wheel["wheel"]),
        ],
    ]
    for command in commands:
        _checked_run(command, timeout=900)
    (wheel,) = wheelhouse.glob("*.whl")
    _checked_run(
        ["uv", "pip", "install", "--python", str(python), "--no-deps", str(wheel)],
        timeout=60,
    )
    shutil.rmtree(source)

    # The connector runner is executed from outside the checkout, by absolute
    # path, under `-I`. Isolated mode never prepends the script's directory or
    # the working directory, so the interpreter's only import roots are its own
    # stdlib and site-packages -- neither inside this repository.
    runner_dir = root / "runner"
    runner_dir.mkdir()
    runner = runner_dir / "_hermetic_connector_runner.py"
    shutil.copy2(Path(__file__).with_name(runner.name), runner)
    probe_script = runner_dir / "_hermetic_isolation_probe.py"
    probe_script.write_text(_ISOLATION_PROBE_PROGRAM, encoding="utf-8")
    run_cwd = root / "run-cwd"
    run_cwd.mkdir()
    isolation_probe = json.loads(
        _checked_run(
            [str(python), "-I", str(probe_script)],
            timeout=120,
            cwd=run_cwd,
            env=_scrubbed_env(run_cwd),
        )
    )
    # Changing HOME must not hide dependencies that happened to be installed
    # only for the parent interpreter. Both imports have explicit authorities.
    import_origin_probe_program = (
        "import importlib.metadata, json, sys, ctypes, _ctypes, yaml, okg; "
        "from pathlib import Path; "
        "mapped = sorted({line.split()[-1] for line in "
        "Path('/proc/self/maps').read_text().splitlines() if '/libpython' in line}) "
        "if sys.platform == 'linux' else []; "
        "print(json.dumps({'prefix': sys.prefix, 'base_prefix': sys.base_prefix, "
        "'ctypes': _ctypes.__spec__.origin, "
        "'ctypes_builtin': '_ctypes' in sys.builtin_module_names, "
        "'libpython': mapped, 'yaml': yaml.__file__, "
        "'okg_version': importlib.metadata.version('okg'), "
        "'okg': str(Path(next(iter(okg.__path__))))}))"
    )
    result = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            import_origin_probe_program,
        ],
        cwd=root,
        env=_scrubbed_env(root),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    origins = json.loads(result.stdout)
    assert Path(origins["prefix"]).resolve() == runtime.resolve()
    assert Path(origins["yaml"]).resolve().is_relative_to(runtime.resolve())
    assert "site-packages" in Path(origins["yaml"]).parts
    # OKG is wheel-installed, exactly like PyYAML above: inside the isolated
    # interpreter's site-packages and nowhere under this checkout.
    okg_origin = Path(origins["okg"]).resolve()
    assert okg_origin.is_relative_to(runtime.resolve())
    assert "site-packages" in okg_origin.parts
    assert not okg_origin.is_relative_to(_REPO_ROOT)
    # An editable install leaves a `__editable__*` finder or path file behind;
    # a wheel install leaves none.
    assert not list((runtime / "lib").rglob("__editable__*"))
    assert origins["okg_version"] == okg_wheel["version"]
    # The probe and the runner share a directory outside the checkout, so
    # neither can reach `src/okg` through `sys.path[0]`.
    assert not [
        entry
        for entry in isolation_probe["sys_path"]
        if entry and Path(entry).resolve().is_relative_to(_REPO_ROOT)
    ]
    assert Path(isolation_probe["okg"]).resolve() == okg_origin
    base_prefix = Path(origins["base_prefix"]).resolve()
    assert base_prefix == Path(sys.base_prefix).resolve()
    if origins["ctypes_builtin"]:
        assert origins["ctypes"] == "built-in"
        ctypes_origin = "<interpreter>/built-in _ctypes"
    else:
        assert Path(origins["ctypes"]).resolve().is_relative_to(base_prefix)
        ctypes_origin = "<interpreter>/" + str(
            Path(origins["ctypes"]).resolve().relative_to(base_prefix)
        )
    libraries = origins["libpython"]
    if _interpreter_library_env():
        assert libraries, "the shared interpreter must expose its loaded libpython"
        assert all(
            Path(path).resolve().is_relative_to(base_prefix) for path in libraries
        )
    return {
        "python": python,
        "root": root,
        "runner": runner,
        "run_cwd": run_cwd,
        "isolation_probe": isolation_probe,
        "okg_wheel": {
            "wheel": okg_wheel["name"],
            "version": okg_wheel["version"],
            "sha256": okg_wheel["sha256"],
        },
        "wheel_digest": _digest(wheel.read_bytes()),
        "dependency_origins": {
            "fresh_environment_had_no_yaml_or_okg": True,
            "yaml": "<isolated-runtime>/"
            + str(Path(origins["yaml"]).relative_to(root)),
            "okg": {
                "path": "<isolated-runtime>/" + str(okg_origin.relative_to(root)),
                "wheel": okg_wheel["name"],
                "version": okg_wheel["version"],
                "sha256": okg_wheel["sha256"],
                "editable": False,
            },
            "planning_python": "<isolated-runtime>/venv/bin/python",
            "planning_pythonpath": None,
            "ctypes_imported": True,
            "ctypes": ctypes_origin,
            "loader_path": "<interpreter>/lib" if _interpreter_library_env() else None,
            "loaded_libpython": [
                "<interpreter>/" + str(Path(path).resolve().relative_to(base_prefix))
                for path in libraries
            ],
        },
    }


# --------------------------------------------------------------------------
# The dedicated database
# --------------------------------------------------------------------------


def _database_name() -> str:
    """A fresh name for this invocation; no pre-existing database is removed."""
    return f"okg_test_dist1185_{uuid.uuid4().hex}"


def _with_dbname(dsn: str, dbname: str) -> str:
    """Resolve libpq options, replace the database, and retain a Chronos URI."""
    try:
        options = conninfo_to_dict(dsn)
    except psycopg.ProgrammingError:
        raise ValueError("cannot parse test database connection options") from None
    options.pop("dbname", None)
    target = "postgresql:///" + quote(dbname, safe="")
    if options:
        target += "?" + urlencode(options, quote_via=quote)
    try:
        resolved = conninfo_to_dict(target)
    except psycopg.ProgrammingError:
        raise ValueError("cannot serialize test database connection options") from None
    if resolved != dict(options, dbname=dbname):
        raise ValueError("test database rewrite changed connection options")
    return target


def _assert_connected_database(conn: Any, expected: str) -> None:
    """Check live identity before schema writes or database create/drop."""
    (actual,) = conn.execute("SELECT current_database()").fetchone()
    assert actual == expected, "connection resolved to an unexpected database"


@pytest.fixture(scope="module")
def hermetic_dsn(okg_dsn: str) -> Any:
    """Create, migrate, and drop a database owned by this module alone.

    The shared `okg_test` runtime is never used: this arm applies its own
    catalog and counts every live row, so another test's rows would make
    the counts meaningless.
    """

    dbname = _database_name()
    admin = _with_dbname(okg_dsn, "postgres")
    target = _with_dbname(okg_dsn, dbname)
    with psycopg.connect(admin, autocommit=True, connect_timeout=15) as conn:
        _assert_connected_database(conn, "postgres")
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))

    from okg.substrate.db import migrate as migrate_module

    original = dict(migrate_module.REQUIRED_EXTENSIONS)
    migrate_module.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        # A fresh database carries none of the extensions the substrate
        # requires; the shared test database has them only because an
        # earlier fixture installed them there.
        with psycopg.connect(target, autocommit=True, connect_timeout=15) as conn:
            _assert_connected_database(conn, dbname)
            for extension in migrate_module.REQUIRED_EXTENSIONS:
                conn.execute(
                    sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                        sql.Identifier(extension)
                    )
                )
        migrate_module.migrate(target)
        yield target
    finally:
        migrate_module.REQUIRED_EXTENSIONS.clear()
        migrate_module.REQUIRED_EXTENSIONS.update(original)
        from okg.substrate.db.pool import close_all

        close_all()
        with psycopg.connect(admin, autocommit=True, connect_timeout=15) as conn:
            _assert_connected_database(conn, "postgres")
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(dbname)
                )
            )


@pytest.fixture(scope="module")
def installed(
    tmp_path_factory: pytest.TempPathFactory, installed_runtime: dict[str, Any]
) -> dict[str, Any]:
    """Build the package and plan an install with no path injection."""

    # Two disjoint trees on purpose: the operator's working directory is
    # not under the distribution's, so `HOME`, `TMPDIR` and `cwd` cannot
    # name the authoring tree even by being its parent.
    root = tmp_path_factory.mktemp("archi-test-dist")
    workdir = tmp_path_factory.mktemp("archi-test-operator")
    archive = build_archi_test_package(root)
    instance = _write_instance(workdir / "instance.json")
    output = workdir / "install"
    completed = _install_plan_subprocess(
        python=installed_runtime["python"],
        package=archive,
        bundle=_OPS_BUNDLE,
        instance=instance,
        output=output,
        cwd=workdir,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    envelope = _last_json(completed.stdout)
    plan = json.loads((output / "plan.json").read_text())
    lock = json.loads((output / "lock.json").read_text())
    return {
        "root": root,
        "python": installed_runtime["python"],
        "dependency_origins": installed_runtime["dependency_origins"],
        "isolation_probe": installed_runtime["isolation_probe"],
        "archive": archive,
        "instance": instance,
        "workdir": workdir,
        "output": output,
        "envelope": envelope,
        "plan": plan,
        "lock": lock,
    }


# --------------------------------------------------------------------------
# (b) install-plan from a clean environment
# --------------------------------------------------------------------------


def test_the_plan_resolves_its_modules_out_of_the_package(
    installed: dict[str, Any],
) -> None:
    """Every planned module came from the package's own object store."""

    plan = installed["plan"]
    assert plan["schema"] == "okg.deployment_product.install_plan/v1"
    assert [item["module_id"] for item in plan["modules"]] == [_MODULE_ID]
    assert {item["provider_kind"] for item in plan["modules"]} == {"package"}
    assert plan["catalog"]["subtype_origin"]["lab_unit"] == _MODULE_ID
    assert plan["catalog"]["subtype_origin"]["lab_run"] == _MODULE_ID
    assert plan["okg"]["satisfied"] is True
    # This bundle selects no source or tool declaration, so it carries no
    # deferred credential. The credential arm is below, on the analysis
    # bundle, which does.
    assert plan["secret_references"] == []
    assert "not-a-real-token" not in json.dumps(plan)
    assert installed["lock"]["modules"] == {
        _MODULE_ID: plan["modules"][0]["object_digests"]
    }


def test_the_subprocess_had_no_route_to_the_authoring_tree(
    installed: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The environment the plan ran in names the distribution nowhere."""

    env = _scrubbed_env(installed["workdir"])
    distribution = str(installed["root"])
    assert (installed["root"] / "authoring").is_dir()
    assert not [name for name in env if name.startswith("OKG_")]
    assert "PYTHONPATH" not in env
    assert installed["dependency_origins"]["fresh_environment_had_no_yaml_or_okg"]
    assert installed["dependency_origins"]["planning_pythonpath"] is None
    assert installed["dependency_origins"]["ctypes_imported"]

    # Nor to the checkout. OKG is wheel bytes in the isolated interpreter, no
    # `sys.path` entry of that interpreter lies inside this repository, and no
    # variable -- `PATH` included -- names it either.
    okg_origin = installed["dependency_origins"]["okg"]
    assert okg_origin["editable"] is False
    assert okg_origin["wheel"].startswith("okg-") and okg_origin["wheel"].endswith(
        ".whl"
    )
    assert len(okg_origin["sha256"]) == 64
    probe = installed["isolation_probe"]
    assert not [
        entry
        for entry in probe["sys_path"]
        if entry and Path(entry).resolve().is_relative_to(_REPO_ROOT)
    ]
    assert Path(probe["okg"]).resolve().is_relative_to(Path(probe["prefix"]).resolve())
    assert "site-packages" in Path(probe["okg"]).parts
    assert not [name for name, value in env.items() if str(_REPO_ROOT) in value]
    # `_checkout_free_path` drops checkout entries rather than assuming the
    # ambient PATH has none, so the guarantee holds however pytest was started.
    with monkeypatch.context() as guard:
        guard.setenv(
            "PATH", os.pathsep.join([str(_REPO_ROOT / ".venv/bin"), "/usr/bin"])
        )
        assert _checkout_free_path() == "/usr/bin"
    # Loader configuration is derived from the interpreter, not caller input.
    prefix = tmp_path / "interpreter"
    libdir = prefix / "lib"
    libdir.mkdir(parents=True)
    binary = libdir / "libpython-fixture.so"
    binary.write_bytes(b"bounded path-validation fixture")
    with monkeypatch.context() as guard:
        guard.setattr(sys, "platform", "linux")
        guard.setattr(sys, "base_prefix", str(prefix))
        guard.setattr(
            sysconfig,
            "get_config_var",
            lambda key: {"Py_ENABLE_SHARED": 1, "LDLIBRARY": binary.name}[key],
        )
        for key in ("LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH", "LD_PRELOAD", "PYTHONPATH"):
            guard.setenv(key, "/hostile/authoring")
        isolated = _scrubbed_env(installed["workdir"])
        assert isolated["LD_LIBRARY_PATH"] == str(libdir.resolve())
        assert not any("/hostile/authoring" in value for value in isolated.values())
        assert all(
            key not in isolated
            for key in ("DYLD_LIBRARY_PATH", "LD_PRELOAD", "PYTHONPATH")
        )
        binary.unlink()
        with pytest.raises(AssertionError):
            _interpreter_library_env()
        foreign = tmp_path / "foreign.so"
        foreign.write_bytes(b"not the selected interpreter")
        binary.symlink_to(foreign)
        with pytest.raises(AssertionError):
            _interpreter_library_env()
    # No variable at all names the distribution tree: not PYTHONPATH, and
    # not HOME, TMPDIR or the working directory either.
    assert not [name for name, value in env.items() if distribution in value]
    assert distribution not in str(installed["workdir"])
    # The harness process itself never gained the distribution either.
    assert not [entry for entry in sys.path if entry and distribution in entry]

    # libpq query parameters override URI paths. Both accepted DSN forms
    # must target our database, retaining every other connection option.
    for original in (
        "postgresql://postgres@localhost:5433/okg_test?dbname=other_test",
        "postgresql://localhost/okg_test?host=%2Ftmp&application_name=lab%20fixture",
        "host=localhost port=5433 user=postgres dbname=other_test "
        "password='fixture-only /?&' options='-c statement_timeout=1000'",
    ):
        parsed = conninfo_to_dict(original)
        for name in ("postgres", "okg_test_dist1185_fixture"):
            rewritten = _with_dbname(original, name)
            assert rewritten.startswith("postgresql://")
            assert conninfo_to_dict(rewritten) == dict(parsed, dbname=name)


def test_the_plan_is_identical_without_the_authoring_tree(
    tmp_path: Path,
) -> None:
    """Delete the authoring checkout; the package alone re-plans the same.

    The same plan and lock resolve after the distribution source tree is
    deleted. This is package data independence, not a runtime install.
    """

    import shutil

    root = tmp_path / "dist"
    archive = build_archi_test_package(root)
    instance = _write_instance(tmp_path / "instance.json")
    first_plan, first_lock = resolve_install_plan(
        archive,
        _OPS_BUNDLE,
        json.loads(instance.read_text()),
        environ={_SECRET_ENV: "not-a-real-token"},
    )
    shutil.rmtree(root / "authoring")
    assert not (root / "authoring").exists()
    second_plan, second_lock = resolve_install_plan(
        archive,
        _OPS_BUNDLE,
        json.loads(instance.read_text()),
        environ={_SECRET_ENV: "not-a-real-token"},
    )
    assert second_plan["plan_digest"] == first_plan["plan_digest"]
    assert second_lock["lock_digest"] == first_lock["lock_digest"]


def test_both_bundles_select_the_one_shared_ontology_module(
    installed: dict[str, Any],
) -> None:
    """Two bundles, one schema: the analysis bundle plans the same module."""

    other_output = installed["workdir"] / "install-analysis"
    completed = _install_plan_subprocess(
        python=installed["python"],
        package=installed["archive"],
        bundle=_ANALYSIS_BUNDLE,
        instance=installed["instance"],
        output=other_output,
        cwd=installed["workdir"],
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    analysis_plan = json.loads((other_output / "plan.json").read_text())
    assert analysis_plan["modules"] == installed["plan"]["modules"]
    assert analysis_plan["bundle"]["bundle_id"] == _ANALYSIS_BUNDLE
    # Different selections, so different plans; same module, same digests.
    assert analysis_plan["plan_digest"] != installed["plan"]["plan_digest"]


def test_a_deferred_credential_records_an_opaque_reference(
    tmp_path: Path,
) -> None:
    """The plan names the declaration, never the variable or its value.

    The packaged live tool is a `tool` declaration the analysis bundle
    selects, so it may carry a deferred credential. The ops bundle does
    not select it, and the identical instance refuses there -- the
    producer half of the validator rule that a shipped-but-unselected
    asset carries no binding.
    """

    from okg.substrate.distributions.install_plan import (
        InstallSecretDeclarationUnselected,
        secret_reference_token,
    )

    archive = build_archi_test_package(tmp_path / "dist")
    instance = json.loads(
        _write_credential_instance(tmp_path / "credential.json").read_text()
    )
    environ = {_SECRET_ENV: "not-a-real-token"}
    plan, _lock = resolve_install_plan(
        archive, _ANALYSIS_BUNDLE, instance, environ=environ
    )
    declaration = next(
        item
        for item in plan["assets"]
        if item["id"] == "lab-bench-status"
    )
    assert plan["secret_references"] == [
        {
            "name": "lab_token",
            "declaration_ref": f"{_OWNER}:lab-bench-status",
            "declaration_digest": declaration["asset_digest"],
            "reference": secret_reference_token(
                release_digest=plan["package"]["release"]["release_digest"],
                instance_name=_INSTANCE,
                declaration_ref=f"{_OWNER}:lab-bench-status",
                variable=_SECRET_ENV,
            ),
            "resolved": True,
        }
    ]
    serialised = json.dumps(plan)
    assert "not-a-real-token" not in serialised
    assert _SECRET_ENV not in serialised

    with pytest.raises(InstallSecretDeclarationUnselected) as caught:
        resolve_install_plan(archive, _OPS_BUNDLE, instance, environ=environ)
    assert caught.value.data["reason"] == "declaration_unselected"


# --------------------------------------------------------------------------
# (d) refusals, end to end
# --------------------------------------------------------------------------


def test_an_out_of_range_okg_version_refuses_before_any_write(
    tmp_path: Path,
) -> None:
    archive = build_archi_test_package(
        tmp_path / "dist", okg_compatibility=("0.0.1", "0.0.2")
    )
    output = tmp_path / "install"
    with pytest.raises(InstallOkgIncompatible) as excinfo:
        write_install_plan(
            archive,
            _OPS_BUNDLE,
            json.loads(_write_instance(tmp_path / "instance.json").read_text()),
            output,
            environ={_SECRET_ENV: "x"},
        )
    assert excinfo.value.code == "install_okg_incompatible"
    assert excinfo.value.data["declared"]["max_version_exclusive"] == "0.0.2"
    assert not output.exists()


def test_a_mutated_package_digest_refuses(tmp_path: Path) -> None:
    archive = build_archi_test_package(tmp_path / "dist")
    payload = bytearray(archive.read_bytes())
    # Flip one byte well inside the archive body, past the first header.
    payload[len(payload) // 2] ^= 0xFF
    archive.write_bytes(bytes(payload))
    with pytest.raises(InstallPackageUnverified) as excinfo:
        resolve_install_plan(
            archive,
            _OPS_BUNDLE,
            json.loads(_write_instance(tmp_path / "instance.json").read_text()),
            environ={_SECRET_ENV: "x"},
        )
    assert excinfo.value.code == "install_package_unverified"


def test_an_undeclared_cross_owner_subtype_collision_refuses(
    tmp_path: Path,
) -> None:
    """A packaged module that redeclares a built-in subtype is refused.

    The collision is caught while the plan composes in memory, so no lock,
    no catalog and no generation exists when it fires.
    """

    colliding = (
        _SCHEMA_YAML.replace(b"LabUnit:", b"Table:")
        .replace(b"lab_unit_id", b"table_id")
        .replace(b"src_subtypes: LabUnit", b"src_subtypes: Table")
    )
    archive = build_archi_test_package(tmp_path / "dist", schema_payload=colliding)
    instance = json.loads(_write_instance(tmp_path / "instance.json").read_text())
    instance["modules"] = ["dataset"]
    output = tmp_path / "install"
    with pytest.raises(CrossOwnerSubtypeCollision) as excinfo:
        write_install_plan(
            archive,
            _OPS_BUNDLE,
            instance,
            output,
            environ={_SECRET_ENV: "x"},
        )
    assert excinfo.value.code == "duplicate_semantic_ownership"
    assert excinfo.value.data["phase"] == "pre_mutation"
    assert excinfo.value.data["subtype"] == "table"
    assert not output.exists()


def test_a_bundle_naming_a_missing_asset_never_seals(tmp_path: Path) -> None:
    """The missing-declared-asset refusal fires at build, not at install.

    `install_declared_asset_missing` exists on the install path, but the
    v2 manifest binds every bundle reference to a packaged asset, so a
    validated archive can never reach it. The refusal is real; it is just
    earlier than the install verb.
    """

    with pytest.raises((ValueError, TypeError)) as excinfo:
        build_archi_test_package(tmp_path / "dist", ghost_bundle_asset=True)
    assert "does not resolve" in str(excinfo.value)


def test_an_unknown_bundle_refuses_and_names_the_declared_ones(
    installed: dict[str, Any],
) -> None:
    with pytest.raises(InstallBundleUnknown) as excinfo:
        resolve_install_plan(
            installed["archive"],
            "lab-nonexistent",
            json.loads(installed["instance"].read_text()),
            environ={_SECRET_ENV: "x"},
        )
    assert excinfo.value.code == "install_bundle_unknown"
    assert excinfo.value.data["declared_bundles"] == sorted(
        (_ANALYSIS_BUNDLE, _OPS_BUNDLE)
    )


def test_lock_drift_refuses_and_replace_lock_accepts(tmp_path: Path) -> None:
    """Mutate, re-plan, refuse; then take the new resolution explicitly."""

    archive = build_archi_test_package(tmp_path / "dist")
    instance_path = _write_instance(tmp_path / "instance.json")
    instance = json.loads(instance_path.read_text())
    output = tmp_path / "install"
    first = write_install_plan(
        archive,
        _OPS_BUNDLE,
        instance,
        output,
        environ={_SECRET_ENV: "x"},
    )
    # Re-running the identical resolution is idempotent, not drift.
    repeat = write_install_plan(
        archive,
        _OPS_BUNDLE,
        instance,
        output,
        environ={_SECRET_ENV: "x"},
    )
    assert repeat["lock_digest"] == first["lock_digest"]
    assert repeat["replaced_lock"] is False

    drifted = dict(instance, modules=["dataset"])
    with pytest.raises(InstallLockDrift) as excinfo:
        write_install_plan(
            archive,
            _OPS_BUNDLE,
            drifted,
            output,
            environ={_SECRET_ENV: "x"},
        )
    assert excinfo.value.code == "install_lock_drift"
    assert excinfo.value.data["recorded_lock_digest"] == first["lock_digest"]
    # The refused run left the recorded lock exactly as it was.
    assert (
        json.loads((output / "lock.json").read_text())["lock_digest"]
        == (first["lock_digest"])
    )

    replaced = write_install_plan(
        archive,
        _OPS_BUNDLE,
        drifted,
        output,
        replace_lock=True,
        environ={_SECRET_ENV: "x"},
    )
    assert replaced["replaced_lock"] is True
    assert replaced["lock_digest"] != first["lock_digest"]


# --------------------------------------------------------------------------
# The four extra packaged assets
# --------------------------------------------------------------------------


def test_the_connector_registration_and_v2_refusal_are_explicit(
    installed: dict[str, Any],
) -> None:
    package = verify_distribution_package(installed["archive"])
    try:
        declared = ConnectorRegistration.from_asset_payload(
            json.loads(package.read_asset(f"{_OWNER}:lab-units-connector"))
        )
        assert declared == _REGISTRATION
        assert package.manifest.schema_version == package_impl.SCHEMA_V2
        with pytest.raises(DistributionConformanceAuthorityError) as excinfo:
            derive_distribution_conformance_authority(
                package, bundle_ids=(_OPS_BUNDLE,)
            )
        assert (
            excinfo.value.code == "distribution_conformance_package_schema_unsupported"
        )
        assert excinfo.value.observed == package_impl.SCHEMA_V2
    finally:
        package.close_runtime_authority()
    source = (_RUNTIME_FIXTURE / "src/archi_test_lab_runtime/connector.py").read_text()
    assert "okg.substrate" not in source
    assert "from okg.deployment import" in source
    connector = installed["plan"]["connectors"][0]
    assert connector["connector_type"] == declared.connector_type
    assert connector["sdk_satisfied"] is True
    assert connector["object_digest"] == _digest(_CONNECTOR_JSON)


@pytest.mark.parametrize(
    ("asset_id", "expected"),
    [
        ("lab-bench-status", {"mode": "read_only", "side_effecting": False}),
        (
            "lab-bench-recalibrate",
            {"side_effecting": True, "requires_approval": True},
        ),
        ("lab-bench-sweep", {"heartbeat_seconds": 60, "budget_units": 10}),
    ],
)
def test_the_declaration_only_assets_verify_and_carry_their_shape(
    installed: dict[str, Any],
    asset_id: str,
    expected: dict[str, Any],
) -> None:
    """Packaging coverage for the live tool, action type and automation.

    Each is read back through the package's own verifying reader and
    checked against its declared shape. None of the three is executed; the
    module docstring's table says so and this test is what that row means.
    """

    package = verify_distribution_package(installed["archive"])
    try:
        payload = package.read_asset(f"{_OWNER}:{asset_id}")
    finally:
        package.close_runtime_authority()
    assert _digest(payload) == _digest(_ASSET_BYTES[asset_id][1])
    document = json.loads(payload)
    for key, value in expected.items():
        assert document[key] == value


def test_the_enricher_rules_are_packaged_and_deferred(
    installed: dict[str, Any],
) -> None:
    """#1181 slice 5 owns running an enricher from a package.

    The rules ship and verify; the publish below gets its edge delta from
    the connector's own edges instead, so nothing silently claims a
    derived edge that was never derived.
    """

    package = verify_distribution_package(installed["archive"])
    try:
        payload = package.read_asset(f"{_OWNER}:lab-run-links")
    finally:
        package.close_runtime_authority()
    assert payload == _ENRICHER_RULES_YAML
    assert b"matcher: word_boundary" in payload


# --------------------------------------------------------------------------
# Real substrate publication, without a conformance receipt
# --------------------------------------------------------------------------


def _live_count(dsn: str, query: str, params: dict[str, Any] | None = None) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        rows = store.checkout().query(query, params or {})
    return int(rows[0]["n"])


def _live_rows(dsn: str, query: str) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return store.checkout().query(query, {})


# --------------------------------------------------------------------------
# Measurements the conformance receipt is built from
#
# Every value below is read out of the hermetic database, the verified
# package, or a process this test ran. Nothing here is a constant standing in
# for something the arm did not observe; `conformance_evidence` refuses an
# absent measurement rather than accepting a default.
# --------------------------------------------------------------------------


def _rows(dsn: str, query: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    """Read operational tables directly; these are not graph-store views."""

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def _graph_counts(dsn: str) -> dict[str, int]:
    """Live node counts by packaged subtype and edge counts by type."""

    counts = {
        name: _live_count(
            dsn,
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE subtype = :st",
            {"st": name},
        )
        for name in ("lab_unit", "lab_run")
    }
    counts["contains"] = _live_count(
        dsn,
        "SELECT count(*) AS n FROM okg.graph_edges WHERE edge_type = :et",
        {"et": "contains"},
    )
    return counts


def _source_node_counts(dsn: str) -> dict[str, int]:
    """Nodes appended per source, as `okg.v_source_runs` reports them."""

    rows = _rows(
        dsn,
        "SELECT source_name, coalesce(sum(nodes_appended), 0) "
        "FROM okg.v_source_runs GROUP BY source_name",
    )
    return {str(name): int(total) for name, total in rows}


def _generation_state(dsn: str) -> tuple[int, int | None]:
    """How many generations exist, and which published one is newest."""

    (total,), = _rows(dsn, "SELECT count(*) FROM okg.graph_generations")
    newest = _rows(
        dsn,
        "SELECT max(generation_id) FROM okg.graph_generations "
        "WHERE status = 'published'",
    )[0][0]
    return int(total), None if newest is None else int(newest)


def _queue_depths_from_rows(
    rows: Sequence[tuple[Any, ...]],
) -> tuple[int | None, int | None, str | None]:
    """Reduce queue-health rows to depths, or to why there are none.

    Each `okg.v_runtime_queue_health` row carries `availability`. When the DBOS
    workflow history is absent the view still returns a row, with
    `availability = 'unavailable'`, a `reason`, and a zero in every count.
    Those zeros are the shape of a report that could not read a queue, not a
    measured empty queue, so this returns `(None, None, reason)` and the
    readiness producer refuses `measurement_missing`. Depths come back only
    when every row reports itself available.
    """

    if not rows:
        return None, None, "no runtime queue health row"
    unavailable = [row for row in rows if row[0] != "available"]
    if unavailable:
        reason = next(
            (str(row[3]) for row in unavailable if row[3]),
            "queue authority unavailable",
        )
        return None, None, reason
    return (
        sum(int(row[1] or 0) for row in rows),
        sum(int(row[2] or 0) for row in rows),
        None,
    )


def _queue_depths(dsn: str) -> tuple[int | None, int | None, str | None]:
    """Active and dead-letter depth, only when the queue authority is readable."""

    return _queue_depths_from_rows(
        _rows(
            dsn,
            "SELECT availability, active_count, dlq_count, reason "
            "FROM okg.v_runtime_queue_health",
        )
    )


def _composition_fingerprint(dsn: str) -> str:
    """Digest the applied catalog's own fingerprint, not some other quantity."""

    rows = _rows(
        dsn,
        "SELECT catalog_fingerprint FROM okg.catalog_versions "
        "WHERE status = 'applied' AND catalog_fingerprint IS NOT NULL "
        "ORDER BY catalog_version_id DESC LIMIT 1",
    )
    assert rows, "the applied catalog must record its fingerprint"
    fingerprint = str(rows[0][0])
    if fingerprint.startswith("sha256:") and len(fingerprint) == 71:
        return fingerprint
    return _digest(fingerprint.encode())


def test_an_unavailable_queue_authority_yields_no_depth() -> None:
    """The view's zeros-with-a-reason row is not a measured empty queue."""

    assert _queue_depths_from_rows(
        [("unavailable", 0, 0, "dbos.workflow_status.status missing")]
    ) == (None, None, "dbos.workflow_status.status missing")
    assert _queue_depths_from_rows([]) == (None, None, "no runtime queue health row")
    # One unavailable row poisons the reduction, however many others read.
    assert _queue_depths_from_rows(
        [("available", 4, 1, None), ("unavailable", 0, 0, "history missing")]
    ) == (None, None, "history missing")


def test_an_available_queue_authority_yields_its_counts() -> None:
    assert _queue_depths_from_rows(
        [("available", 4, 1, None), ("available", 2, 0, None)]
    ) == (6, 1, None)
    assert _queue_depths_from_rows([("available", 0, 0, None)]) == (0, 0, None)


def _admission_digests(
    package: Any, bundles: tuple[Any, ...], registration: Any
) -> dict[str, str]:
    """Derive this fixture consumer's four admission digests from real bytes.

    The wire contract requires the request's admission digests to equal the
    receipt authority's and gives them no other meaning: they are the
    consumer's declaration of what it admitted. This fixture consumer declares
    exactly four things it can point at, so no digest here is a constant:

    * the extension boundary the packaged connector declares (its SDK window,
      auth modes, capabilities and required modules);
    * the package verification receipt the verifier produced for these bytes;
    * the capabilities and contract versions the selected release and bundles
      declare;
    * the resource limits the package verifier enforced while reading them.
    """

    from okg.substrate.distributions import package as impl

    boundary = {
        "connector_type": registration.connector_type,
        "sdk_range": [
            registration.sdk_range.min_version,
            registration.sdk_range.max_version_exclusive,
        ],
        "auth_modes": sorted(registration.auth_modes),
        "capabilities": sorted(registration.capabilities),
        "required_modules": sorted(registration.required_modules),
    }
    release = package.manifest.distribution_release
    capabilities = {
        "release": {
            "capabilities": sorted(release.compatibility.capabilities),
            "contract_versions": sorted(release.compatibility.contract_versions),
        },
        "bundles": {
            bundle.bundle_id: {
                "capabilities": sorted(bundle.compatibility.capabilities),
                "contract_versions": sorted(bundle.compatibility.contract_versions),
            }
            for bundle in bundles
        },
    }
    limits = {name: int(value) for name, value in sorted(impl.LIMITS.items())}
    assert limits, "the package verifier must declare the limits it enforced"
    return {
        "extension_boundary_digest": _digest(
            json.dumps(boundary, sort_keys=True).encode()
        ),
        "package_admission_digest": package.verification_receipt().digest,
        "capability_digest": _digest(
            json.dumps(capabilities, sort_keys=True).encode()
        ),
        "resource_policy_digest": _digest(
            json.dumps(limits, sort_keys=True).encode()
        ),
    }


def _materialized_asset_digests(archive: Path, destination: Path) -> dict[str, str]:
    """Materialize the package and digest every selected file it wrote."""

    package = verify_distribution_package(archive)
    try:
        package.materialize(destination)
        packaged = {item.asset.qualified_id: item for item in package.manifest.assets}
    finally:
        package.close_runtime_authority()
    observed: dict[str, str] = {}
    for qualified_id, item in packaged.items():
        assert item.locator.member_path is None, qualified_id
        digest = item.locator.object_digest
        path = destination / "objects/sha256" / digest.split(":", 1)[1]
        observed[qualified_id] = _digest(path.read_bytes())
    return observed


def _okg_revision() -> str:
    completed = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    return completed.stdout.strip()


# --------------------------------------------------------------------------
# The v2 conformance request and receipt this arm emits
# --------------------------------------------------------------------------

#: The `ci_fixture` profile's mandatory arms: everything but `legacy_authority`.
_CONFORMANCE_ARMS: tuple[str, ...] = (
    "contract",
    "artifact_materialization",
    "first_publish",
    "readback_health",
    "idempotent_reapply",
    "update_rollback_recovery",
)
#: What this arm cannot execute, and the reference each block names.
#:
#: `first_publish` and `readback_health` block because the deployment-product
#: lifecycle that produces a sealed release and an MCP read surface does not
#: run here: the arm publishes through `Publisher`, so it has no sealed-release
#: digest and opens no MCP session. `update_rollback_recovery` blocks because
#: an executed interruption and restore needs a real host; an inert plan is
#: refused, never recorded as recovery proof.
_BLOCKED_ARMS: dict[str, tuple[str, str]] = {
    "first_publish": ("apply", "sealed-release-lifecycle"),
    "readback_health": ("readback", "mcp-graph-read-surface"),
    "update_rollback_recovery": ("recovery", "recovery-host"),
}
_REQUEST_ID = "lab-hermetic-conformance"
_CASE_ID = "lab-hermetic-ci-fixture"


def _expected_signals() -> tuple[Any, ...]:
    from okg.substrate.contracts.deployment_product.conformance import (
        ConformanceExpectedSignal,
    )

    return tuple(
        ConformanceExpectedSignal(
            signal_id=signal_id, kind=kind, target=target, minimum_count=minimum
        )
        for signal_id, kind, target, minimum in (
            ("lab-unit-nodes", "node_count", "lab_unit", _EXPECTED_LAB_UNITS),
            ("lab-run-nodes", "node_count", "lab_run", _EXPECTED_LAB_RUNS),
            ("contains-edges", "edge_count", "contains", _EXPECTED_CONTAINS_EDGES),
            ("packaged-subtypes", "subtype_count", _MODULE_ID, 2),
        )
    )


def _sealed_release_inputs(
    *,
    release_digest: str,
    definition_digest: str,
    archive_digest: str,
    candidate_resolution: Mapping[str, Any],
    baseline_resolution: Mapping[str, Any],
) -> tuple[Any, ...]:
    """A PLACEHOLDER baseline/candidate pair. No sealed release exists here.

    The `ci_fixture` profile makes `update_rollback_recovery` mandatory, and
    the request schema requires a baseline and a candidate sealed release for
    it, so a well-formed request cannot omit them. This arm produces no sealed
    release at all. Rather than fabricate two, these ids are digests over the
    two real, distinct install resolutions the run computes from the same
    package -- present solely to satisfy the profile's request shape.

    They prove nothing about recovery. `validate_conformance_exchange` binds
    them only when the recovery arm passes, and that arm is blocked; passing it
    would additionally require typed `ConformanceRecoveryEvidence` binding
    these exact artifact ids to a restored generation, which no code here
    produces.
    """

    from okg.substrate.contracts.deployment_product.conformance import (
        ConformanceSealedReleaseInput,
    )

    def _artifact_id(resolution: Mapping[str, Any]) -> str:
        return _digest(json.dumps(dict(resolution), sort_keys=True).encode())

    candidate = _artifact_id(candidate_resolution)
    baseline = _artifact_id(baseline_resolution)
    assert candidate != baseline, "the declared resolutions must be distinct"
    return tuple(
        ConformanceSealedReleaseInput(
            role=role,
            schema_version="okg.sealed_release/v1",
            artifact_id=artifact_id,
            input_id=archive_digest,
            definition_digest=definition_digest,
            distribution_release_digest=release_digest,
        )
        for role, artifact_id in (("baseline", baseline), ("candidate", candidate))
    )


def _build_conformance_request(
    *,
    authority: Any,
    definition_digest: str,
    archive_digest: str,
    admission: Mapping[str, str],
    sealed_releases: tuple[Any, ...],
) -> Any:
    from okg.substrate.contracts.deployment_product import API_VERSION
    from okg.substrate.contracts.deployment_product.conformance import (
        CONFORMANCE_PROTOCOL_V2,
        CONFORMANCE_REQUEST_SCHEMA_V2,
        ConformanceAdmission,
        ConformanceCase,
        ConformanceCompatibilityV2,
        ConformanceDefinitionReference,
        ConformanceExpectations,
        ConformanceInputReference,
        ConformanceInputs,
        ExternalDistributionConformanceRequestV2,
    )

    definition = ConformanceDefinitionReference(
        definition_id=_DEPLOYMENT, definition_digest=definition_digest
    )
    return ExternalDistributionConformanceRequestV2(
        schema_version=CONFORMANCE_REQUEST_SCHEMA_V2,
        request_id=_REQUEST_ID,
        subject=authority.subject("hermetic_fixture"),
        selection=authority.selection(definition),
        case=ConformanceCase(
            case_id=_CASE_ID,
            claim_profile="ci_fixture",
            requested_arms=_CONFORMANCE_ARMS,
        ),
        expectations=ConformanceExpectations(signals=_expected_signals()),
        inputs=ConformanceInputs(
            inputs=(
                ConformanceInputReference(
                    input_id="lab-units-package",
                    kind="fixture",
                    digest=archive_digest,
                    media_type="application/octet-stream",
                ),
            ),
            sealed_releases=sealed_releases,
        ),
        admission=ConformanceAdmission(**dict(admission)),
        compatibility=ConformanceCompatibilityV2(
            product_contract_api=API_VERSION,
            harness_protocol=CONFORMANCE_PROTOCOL_V2,
        ),
    )


def _build_conformance_receipt(
    *,
    request: Any,
    authority: Any,
    admission: Mapping[str, str],
    lock_digest: str,
    plan_digest: str,
    definition_digest: str,
    applied_instance_id: str,
    evidence: Any,
    arms: tuple[Any, ...],
    diagnostics: tuple[Any, ...],
) -> Any:
    from importlib.metadata import version

    from okg.substrate.contracts.deployment_product import API_VERSION
    from okg.substrate.contracts.deployment_product.conformance import (
        CONFORMANCE_PROTOCOL_V2,
        CONFORMANCE_RECEIPT_SCHEMA_V2,
        ConformanceAuthority,
        ConformanceClaim,
        ConformanceHarnessIdentityV2,
        ConformanceObservedBundleIdentity,
        ExternalDistributionConformanceReceiptV2,
        aggregate_conformance_verdict,
        conformance_receipt_digest,
        conformance_request_digest,
        load_conformance_receipt,
    )

    verdict = aggregate_conformance_verdict(arms)
    completed = tuple(item.arm for item in arms if item.status != "blocked")
    receipt_authority = ConformanceAuthority(
        observed_artifact_digest=request.subject.artifact.digest,
        release_manifest_digest=request.subject.release_digest,
        release_digest=request.subject.release_digest,
        bundle_identities=tuple(
            ConformanceObservedBundleIdentity(
                bundle_id=bundle.bundle_id,
                version=bundle.version,
                digest=bundle.bundle_digest,
            )
            for bundle in authority.bundles
        ),
        definition_digest=definition_digest,
        lock_digest=lock_digest,
        plan_digest=plan_digest,
        applied_instance_id=applied_instance_id,
        **dict(admission),
    )
    draft = ExternalDistributionConformanceReceiptV2(
        schema_version=CONFORMANCE_RECEIPT_SCHEMA_V2,
        receipt_digest=_ZERO_DIGEST,
        request_digest=conformance_request_digest(request),
        verdict=verdict,
        harness=ConformanceHarnessIdentityV2(
            okg_revision=_okg_revision(),
            package_version=version("okg"),
            harness_protocol=CONFORMANCE_PROTOCOL_V2,
            product_contract_api=API_VERSION,
        ),
        subject=request.subject,
        authority=receipt_authority,
        arms=arms,
        claim=ConformanceClaim(
            profile="ci_fixture",
            subject_kind="hermetic_fixture",
            requested_arms=_CONFORMANCE_ARMS,
            completed_arms=completed,
            satisfied=verdict == "pass",
            scope="ci_fixture",
        ),
        evidence=evidence,
        diagnostics=diagnostics,
    )
    payload = draft.model_dump(mode="json", by_alias=True)
    payload["receipt_digest"] = conformance_receipt_digest(draft)
    return load_conformance_receipt(payload)


def _blocked_arms_and_diagnostics() -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    """The three arms this hermetic environment cannot execute."""

    from okg.substrate.contracts.deployment_product.conformance import (
        ConformanceArmResult,
        ConformanceDiagnostic,
    )
    from okg.substrate.distributions.conformance_diagnostics import (
        RUNTIME_ENVIRONMENT_CODE,
        blocked_arm_result,
        runtime_environment_unavailable_diagnostic,
    )

    arms = tuple(
        ConformanceArmResult.model_validate(
            blocked_arm_result(arm, (RUNTIME_ENVIRONMENT_CODE,))
        )
        for arm in sorted(_BLOCKED_ARMS)
    )
    diagnostics = tuple(
        ConformanceDiagnostic.model_validate(
            runtime_environment_unavailable_diagnostic(
                arm=arm,
                phase=phase,
                reference=reference,
                gap="runtime-unavailable",
                owner="harness",
            )
        )
        for arm, (phase, reference) in sorted(_BLOCKED_ARMS.items())
    )
    return arms, diagnostics


def _passing_arms(*names: str) -> tuple[Any, ...]:
    from okg.substrate.contracts.deployment_product.conformance import (
        CONFORMANCE_PASS_CLASSIFICATIONS,
        ConformanceArmResult,
    )

    return tuple(
        ConformanceArmResult(
            arm=name,
            status="pass",
            mutation=CONFORMANCE_PASS_CLASSIFICATIONS[name][0],
            retry_classification=CONFORMANCE_PASS_CLASSIFICATIONS[name][1],
        )
        for name in names
    )


@pytest.mark.postgres
def test_the_packaged_distribution_publishes_its_declared_counts(
    hermetic_dsn: str,
    installed_runtime: dict[str, Any],
    record_property: Any,
    installed: dict[str, Any],
    tmp_path: Path,
) -> None:
    """Compose package data, run the installed connector, and publish.

    Every count asserted here is the fixture's declared shape, not a
    measurement copied back from the run.
    """

    plan = installed["plan"]
    lock = installed["lock"]
    package = verify_distribution_package(installed["archive"])
    try:
        manifest = package.manifest
        declared = ConnectorRegistration.from_asset_payload(
            json.loads(package.read_asset(f"{_OWNER}:lab-units-connector"))
        )

        # --- compose the catalog from the plan's providers ---------------
        providers = dict(builtin_module_providers())
        providers.update(package_module_providers(package, tmp_path / "modules"))
        deployment_dir = tmp_path / "deployment"
        deployment_dir.mkdir()
        definition = json.dumps(
            {"name": _DEPLOYMENT, "modules": [_MODULE_ID]}, sort_keys=True
        )
        (deployment_dir / "deployment.yaml").write_text(definition)
        composed = compose_catalog(deployment_dir, providers=providers)
    finally:
        package.close_runtime_authority()

    assert composed.module_origins[_MODULE_ID] == {
        "owner": _OWNER,
        "provider_kind": "package",
        "package_release_digest": (manifest.distribution_release.release_digest),
        "object_digests": plan["modules"][0]["object_digests"],
    }
    assert {"lab_unit", "lab_run"} <= composed.subtypes

    with psycopg.connect(hermetic_dsn) as conn:
        apply_composed_catalog(conn, composed)

    # Read the empty graph before the connector runs. These are measurements,
    # not assumptions: the conformance evidence producers refuse a delta whose
    # "before" side was never read.
    baseline_counts = _graph_counts(hermetic_dsn)
    baseline_sources = _source_node_counts(hermetic_dsn)
    baseline_generations, baseline_pointer = _generation_state(hermetic_dsn)
    assert baseline_counts == {"lab_unit": 0, "lab_run": 0, "contains": 0}

    # The packaged subtypes really are on the applied schema, and no
    # built-in module supplied them.
    with psycopg.connect(hermetic_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM okg.subtypes WHERE name IN ('lab_unit','lab_run')"
        )
        assert {row[0] for row in cur.fetchall()} == {"lab_unit", "lab_run"}
    # No built-in module declares these names, so the origin is the
    # package's and only the package's.
    assert not [
        name
        for name, origin in composed.subtype_origin.items()
        if name in {"lab_unit", "lab_run"} and origin != _MODULE_ID
    ]

    # The selected registration enters the production strict registry path.
    registry_path = tmp_path / "source_registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "admission_contract": "strict_v1",
                "sources": {
                    declared.connector_type: registration_registry_entry(declared)
                },
            }
        )
    )
    result_path = tmp_path / "runtime-proof.json"
    # The runner is a copy outside the checkout, run by absolute path from a
    # working directory outside it, under the same scrubbed environment the
    # install-plan subprocesses get. The only addition is the fixture DSN.
    run_cwd = installed_runtime["run_cwd"]
    env = dict(_scrubbed_env(run_cwd), LAB_TEST_DSN=hermetic_dsn)
    assert not Path(installed_runtime["runner"]).is_relative_to(_REPO_ROOT)
    completed = subprocess.run(
        [
            str(installed_runtime["python"]),
            "-I",
            str(installed_runtime["runner"]),
            str(registry_path),
            str(result_path),
            _DEPLOYMENT,
        ],
        cwd=run_cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    runtime_proof = json.loads(result_path.read_text())
    # What actually ran imported OKG out of the isolated interpreter's
    # site-packages, and saw no checkout path at all.
    assert not [
        entry
        for entry in runtime_proof["sys_path"]
        if entry and Path(entry).resolve().is_relative_to(_REPO_ROOT)
    ]
    okg_module = Path(runtime_proof["okg_module_path"])
    assert okg_module.is_relative_to(installed_runtime["root"])
    assert "site-packages" in okg_module.parts
    assert not okg_module.is_relative_to(_REPO_ROOT)
    assert runtime_proof["first_nodes"] == _EXPECTED_NODES
    assert runtime_proof["first_edges"] == _EXPECTED_CONTAINS_EDGES
    assert runtime_proof["second_nodes"] == runtime_proof["second_edges"] == 0
    assert runtime_proof["checkpoint_before"] == runtime_proof["checkpoint_after"]
    assert runtime_proof["caught_up"] == 1.0
    assert Path(runtime_proof["module_file"]).is_relative_to(installed_runtime["root"])
    assert "site-packages" in Path(runtime_proof["module_file"]).parts
    assert runtime_proof["ownership_id"] == declared.ownership_id
    assert runtime_proof["source_class"] == declared.source_class
    assert runtime_proof["registry_binding_fingerprint"]
    assert runtime_proof["record_identity_fields"] == list(
        declared.record_identity_fields
    )
    assert runtime_proof["identity_facts_checked"] == 8

    # --- publish -------------------------------------------------------
    published = Publisher(
        hermetic_dsn,
        _DEPLOYMENT,
        catalog_identity=bind_test_catalog_writer(hermetic_dsn, _DEPLOYMENT),
    ).cycle()
    assert published["status"] == "published", published
    generation_id = str(published["compat_generation_id"])

    unit_rows = _live_count(
        hermetic_dsn,
        "SELECT count(*) AS n FROM okg.graph_nodes WHERE subtype = :st",
        {"st": "lab_unit"},
    )
    run_rows = _live_count(
        hermetic_dsn,
        "SELECT count(*) AS n FROM okg.graph_nodes WHERE subtype = :st",
        {"st": "lab_run"},
    )
    edge_rows = _live_count(
        hermetic_dsn,
        "SELECT count(*) AS n FROM okg.graph_edges WHERE edge_type = :et",
        {"et": "contains"},
    )
    assert (unit_rows, run_rows, edge_rows) == (
        _EXPECTED_LAB_UNITS,
        _EXPECTED_LAB_RUNS,
        _EXPECTED_CONTAINS_EDGES,
    )

    # --- the packaged playbook, actually run ---------------------------
    package = verify_distribution_package(installed["archive"])
    try:
        playbook = json.loads(package.read_asset(f"{_OWNER}:lab-unit-census"))
    finally:
        package.close_runtime_authority()
    query = playbook["query"]
    assert playbook["read_only"] is True
    assert query.lstrip().upper().startswith("SELECT") and ";" not in query
    playbook_rows = _live_rows(hermetic_dsn, query)
    assert [row["node_id"] for row in playbook_rows] == [
        "lab_unit:alpha",
        "lab_unit:beta",
    ]
    assert len(playbook_rows) >= playbook["minimum_rows"]

    published_counts = _graph_counts(hermetic_dsn)
    published_sources = _source_node_counts(hermetic_dsn)
    published_generations, published_pointer = _generation_state(hermetic_dsn)
    subtype_count = _rows(
        hermetic_dsn,
        "SELECT count(DISTINCT name) FROM okg.subtypes "
        "WHERE name IN ('lab_unit','lab_run')",
    )[0][0]

    # Independently re-plan data and measure its database no-op. This does
    # not apply a deployment or prove a sealed-release lifecycle arm.
    before = (unit_rows, run_rows, edge_rows)
    reapplied = write_install_plan(
        installed["archive"],
        _OPS_BUNDLE,
        json.loads(installed["instance"].read_text()),
        installed["output"],
        environ={_SECRET_ENV: "not-a-real-token"},
    )
    assert reapplied["plan_digest"] == plan["plan_digest"]
    assert reapplied["lock_digest"] == lock["lock_digest"]
    assert reapplied["replaced_lock"] is False
    after = (
        _live_count(
            hermetic_dsn,
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE subtype = :st",
            {"st": "lab_unit"},
        ),
        _live_count(
            hermetic_dsn,
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE subtype = :st",
            {"st": "lab_run"},
        ),
        _live_count(
            hermetic_dsn,
            "SELECT count(*) AS n FROM okg.graph_edges WHERE edge_type = :et",
            {"et": "contains"},
        ),
    )
    assert after == before
    reapplied_counts = _graph_counts(hermetic_dsn)
    reapplied_sources = _source_node_counts(hermetic_dsn)
    reapplied_generations, reapplied_pointer = _generation_state(hermetic_dsn)
    queue_depth, dlq_depth, queue_reason = _queue_depths(hermetic_dsn)

    # A JUnit property records identities and measured outcomes, not a
    # fabricated conformance receipt or a runtime-enforced wheel binding.
    proof = dict(
        runtime_proof,
        wheel_digest=installed_runtime["wheel_digest"],
        okg_wheel=installed_runtime["okg_wheel"],
        dependency_origins=installed_runtime["dependency_origins"],
        archive_digest=_digest(installed["archive"].read_bytes()),
        generation_id=generation_id,
        published_counts=list(after[:3]),
        conformance="v2 conformance receipt emitted; see hermetic_conformance_receipt",
    )
    root = installed_runtime["root"]

    def _relabel(path: str) -> str:
        resolved = Path(path).resolve()
        if resolved.is_relative_to(root):
            return "<isolated-runtime>/" + str(resolved.relative_to(root))
        return path

    proof["module_file"] = _relabel(runtime_proof["module_file"])
    proof["okg_module_path"] = _relabel(runtime_proof["okg_module_path"])
    proof["sys_path"] = [
        _relabel(entry) for entry in runtime_proof["sys_path"] if entry
    ]
    (tmp_path / "published-proof.json").write_text(json.dumps(proof, sort_keys=True))
    record_property("hermetic_publish_proof", json.dumps(proof, sort_keys=True))

    _emit_conformance_receipt(
        installed=installed,
        tmp_path=tmp_path,
        record_property=record_property,
        definition_digest=_digest(definition.encode()),
        generation_id=generation_id,
        runtime_proof=runtime_proof,
        measurements={
            "baseline_counts": baseline_counts,
            "baseline_sources": baseline_sources,
            "baseline_generations": baseline_generations,
            "baseline_pointer": baseline_pointer,
            "published_counts": published_counts,
            "published_sources": published_sources,
            "published_generations": published_generations,
            "published_pointer": published_pointer,
            "reapplied_counts": reapplied_counts,
            "reapplied_sources": reapplied_sources,
            "reapplied_generations": reapplied_generations,
            "reapplied_pointer": reapplied_pointer,
            "subtype_count": int(subtype_count),
            "queue_depth": queue_depth,
            "dlq_depth": dlq_depth,
            "queue_reason": queue_reason,
            "composition_digest": _composition_fingerprint(hermetic_dsn),
        },
    )


# --------------------------------------------------------------------------
# The receipt
# --------------------------------------------------------------------------


def _emit_conformance_receipt(
    *,
    installed: dict[str, Any],
    tmp_path: Path,
    record_property: Any,
    definition_digest: str,
    generation_id: str,
    runtime_proof: Mapping[str, Any],
    measurements: Mapping[str, Any],
) -> None:
    """Build, validate and record this run's v2 conformance exchange.

    Every value in the receipt comes from `measurements` -- rows this test read
    out of the hermetic database -- or from the verified package. The
    `conformance_evidence` producers refuse an absent measurement, so an arm
    this run did not observe cannot reach the receipt as a pass. Three do not:
    `first_publish` and `readback_health` need the deployment-product
    lifecycle's sealed release and an MCP read surface, and
    `update_rollback_recovery` needs a host that can interrupt and restore one.
    They are recorded blocked with the settled environment profile.
    """

    import shutil

    from okg.substrate.contracts.deployment_product.conformance import (
        ConformanceEvidence,
        conformance_receipt_digest,
        validate_conformance_exchange,
    )
    from okg.substrate.distributions.conformance_diagnostics import (
        check_conformance_diagnostic_profiles,
    )
    from okg.substrate.distributions.conformance_evidence import (
        ConformanceEvidenceError,
        MeasuredCount,
        build_count_signal_evidence,
        build_materialization_evidence,
        build_publish_evidence,
        build_readiness_evidence,
        build_reapply_evidence,
    )

    archive = installed["archive"]
    archive_digest = _digest(archive.read_bytes())
    plan = installed["plan"]
    lock = installed["lock"]
    instance = json.loads(installed["instance"].read_text())

    package = verify_distribution_package(archive)
    try:
        authority = derive_distribution_conformance_authority(
            package, bundle_ids=(_OPS_BUNDLE,), protocol_version="v2"
        )
        registration = ConnectorRegistration.from_asset_payload(
            json.loads(package.read_asset(f"{_OWNER}:lab-units-connector"))
        )
        admission = _admission_digests(package, authority.bundles, registration)
        declared_asset_digests = {
            item.asset.qualified_id: item.asset.digest
            for item in package.manifest.assets
        }
        release_digest = package.manifest.distribution_release.release_digest
    finally:
        package.close_runtime_authority()

    # --- artifact_materialization: digests read off materialized files ----
    #
    # The subject archive is rebuilt from a fresh authoring tree, that tree is
    # deleted, and the package is materialized from its own bytes alone. The
    # rebuilt transport digest equals the subject's, so what is measured here
    # is the subject package's materialization, measured with no checkout to
    # fall back on.
    independent_root = tmp_path / "independent-dist"
    independent_archive = build_archi_test_package(independent_root)
    assert independent_archive.name == archive.name
    shutil.rmtree(independent_root / "authoring")
    authoring_present = (independent_root / "authoring").exists()
    materialized = _materialized_asset_digests(
        independent_archive, tmp_path / "materialized"
    )
    materialization_digest = _digest(
        json.dumps(materialized, sort_keys=True).encode()
    )
    materialization = build_materialization_evidence(
        materialization_digest=materialization_digest,
        declared_asset_digests=declared_asset_digests,
        materialized_asset_digests=materialized,
        authoring_tree_present=authoring_present,
    )

    # --- idempotent_reapply: the graph either side of the re-plan ---------
    def _counts(before_key: str, after_key: str, names: tuple[str, ...]) -> dict:
        return {
            name: MeasuredCount(
                name,
                measurements[before_key][name],
                measurements[after_key][name],
            )
            for name in names
        }

    def _source_counts(before_key: str, after_key: str) -> dict:
        """Per-source appended-node counts either side of a measured step.

        `okg.v_source_runs` documents its own absence: "A source with no
        per-run evidence projects ZERO rows ('no runs recorded') -- never a
        run-shaped placeholder with NULL run_id/counts." So a source missing
        from one side of the span is that view reporting no run for it, which
        is a measured zero appended rows, not an unread value. The read itself
        succeeded; only the row is absent. Every source present on both sides
        uses both read-back numbers.
        """

        names = set(measurements[before_key]) | set(measurements[after_key])
        return {
            name: MeasuredCount(
                name,
                measurements[before_key].get(name, 0),
                measurements[after_key].get(name, 0),
            )
            for name in names
        }

    # The active generation is the published pointer `okg.graph_generations`
    # reports, read either side of the re-plan; `compat_generation_id` is the
    # publisher's own compatibility id and is recorded separately.
    published_pointer = measurements["published_pointer"]
    assert published_pointer is not None, "the publish must leave a pointer"
    reapply_sources = _source_counts("published_sources", "reapplied_sources")
    reapply = build_reapply_evidence(
        generation_id_before=str(published_pointer),
        generation_id_after=str(measurements["reapplied_pointer"]),
        lock_digest=lock["lock_digest"],
        plan_digest=plan["plan_digest"],
        node_counts=_counts(
            "published_counts", "reapplied_counts", ("lab_unit", "lab_run")
        ),
        edge_counts=_counts("published_counts", "reapplied_counts", ("contains",)),
        generations_created=MeasuredCount(
            "generations",
            measurements["published_generations"],
            measurements["reapplied_generations"],
        ),
        active_generation_pointer_changes=int(
            measurements["published_pointer"] != measurements["reapplied_pointer"]
        ),
        claimed_source_deltas={name: 0 for name in reapply_sources},
        source_counts=reapply_sources,
    )
    # The connector's own second run agrees with the database: no new fact.
    assert runtime_proof["second_nodes"] == runtime_proof["second_edges"] == 0

    # --- readiness: refused, so the receipt carries no readiness block ----
    #
    # This deployment has no DBOS runtime, so the queue-health view reports
    # itself unavailable and no depth is measured; the arm also opens no MCP
    # session. The producer refuses rather than record the report's zeros, and
    # the receipt omits readiness entirely, which is consistent with
    # readback_health being blocked. Nothing here asserts a ready deployment.
    assert measurements["queue_depth"] is None, measurements["queue_reason"]
    assert measurements["dlq_depth"] is None
    assert measurements["queue_reason"]
    with pytest.raises(ConformanceEvidenceError) as readiness_refusal:
        build_readiness_evidence(
            ready=False,
            doctor="unavailable",
            queue_depth=measurements["queue_depth"],
            dlq_depth=measurements["dlq_depth"],
            mcp_graph_read=False,
        )
    assert readiness_refusal.value.data == {
        "block": "readiness",
        "path": "queue_depth",
        "reason": "measurement_missing",
    }

    # --- signals: every requested count, bound to its read-back ----------
    published = measurements["published_counts"]
    signals = tuple(
        build_count_signal_evidence(
            signal_id=signal_id,
            kind=kind,
            target=target,
            claimed_count=expected,
            measured_count=measured,
        )
        for signal_id, kind, target, expected, measured in (
            (
                "lab-unit-nodes",
                "node_count",
                "lab_unit",
                _EXPECTED_LAB_UNITS,
                published["lab_unit"],
            ),
            (
                "lab-run-nodes",
                "node_count",
                "lab_run",
                _EXPECTED_LAB_RUNS,
                published["lab_run"],
            ),
            (
                "contains-edges",
                "edge_count",
                "contains",
                _EXPECTED_CONTAINS_EDGES,
                published["contains"],
            ),
            (
                "packaged-subtypes",
                "subtype_count",
                _MODULE_ID,
                2,
                measurements["subtype_count"],
            ),
        )
    )

    # --- first_publish: refused, because no sealed release was produced ---
    publish_arguments: dict[str, Any] = dict(
        generation_id=str(published_pointer),
        generation_status="published",
        definition_digest=definition_digest,
        lock_digest=lock["lock_digest"],
        composition_digest=measurements["composition_digest"],
        plan_digest=plan["plan_digest"],
        sealed_release_digest=None,
        applied_instance_id=_INSTANCE,
        node_counts=_counts(
            "baseline_counts", "published_counts", ("lab_unit", "lab_run")
        ),
        edge_counts=_counts("baseline_counts", "published_counts", ("contains",)),
        claimed_nodes_live_delta=_EXPECTED_NODES,
        claimed_edges_live_delta=_EXPECTED_CONTAINS_EDGES,
        claimed_source_deltas={_SOURCE_NAME: _EXPECTED_NODES},
        source_counts=_source_counts("baseline_sources", "published_sources"),
    )
    with pytest.raises(ConformanceEvidenceError) as publish_refusal:
        build_publish_evidence(**publish_arguments)
    assert publish_refusal.value.data == {
        "block": "publish",
        "path": "sealed_release_digest",
        "reason": "measurement_missing",
    }
    # Every other measurement in that call is real, and the producer accepts
    # them once the missing one is supplied. What is supplied below is a
    # digest-shaped PLACEHOLDER, not a sealed release: no sealed release
    # exists in this arm. The resulting block is never put in the receipt;
    # this line shows the refusal above is about that one input and nothing
    # else.
    placeholder_sealed_release = _digest(b"not-a-sealed-release/placeholder")
    proved = build_publish_evidence(
        **dict(publish_arguments, sealed_release_digest=placeholder_sealed_release)
    )
    assert (proved.nodes_live_delta, proved.edges_live_delta) == (
        _EXPECTED_NODES,
        _EXPECTED_CONTAINS_EDGES,
    )
    assert proved.composition_digest == measurements["composition_digest"]

    # --- the exchange ----------------------------------------------------
    request = _build_conformance_request(
        authority=authority,
        definition_digest=definition_digest,
        archive_digest=archive_digest,
        admission=admission,
        sealed_releases=_sealed_release_inputs(
            release_digest=release_digest,
            definition_digest=definition_digest,
            archive_digest=archive_digest,
            candidate_resolution={
                "bundle": _OPS_BUNDLE,
                "plan_digest": plan["plan_digest"],
                "lock_digest": lock["lock_digest"],
            },
            baseline_resolution=_baseline_resolution(archive, instance),
        ),
    )
    blocked_arms, blocked_diagnostics = _blocked_arms_and_diagnostics()
    evidence = ConformanceEvidence(
        signals=signals,
        materialization=materialization,
        reapply=reapply,
    )
    receipt = _build_conformance_receipt(
        request=request,
        authority=authority,
        admission=admission,
        lock_digest=lock["lock_digest"],
        plan_digest=plan["plan_digest"],
        definition_digest=definition_digest,
        applied_instance_id=_INSTANCE,
        evidence=evidence,
        arms=_passing_arms("contract", "artifact_materialization", "idempotent_reapply")
        + blocked_arms,
        diagnostics=blocked_diagnostics,
    )
    assert receipt.verdict == "blocked"
    assert receipt.claim.satisfied is False
    assert receipt.claim.completed_arms == (
        "contract",
        "artifact_materialization",
        "idempotent_reapply",
    )
    validate_conformance_exchange(request, receipt)
    check_conformance_diagnostic_profiles(request, receipt)

    request_path = tmp_path / "conformance-request.json"
    receipt_path = tmp_path / "conformance-receipt.json"
    request_path.write_text(request.model_dump_json(by_alias=True))
    receipt_path.write_text(receipt.model_dump_json(by_alias=True))

    # The CLI is the operator's surface: run it from the isolated
    # interpreter's environment, with no DSN and no OKG_* variable at all.
    completed = subprocess.run(
        [
            str(installed["python"]),
            "-I",
            "-m",
            "okg.substrate.cli.main",
            "--json",
            "deployment",
            "conformance",
            "validate",
            "--request",
            str(request_path),
            "--receipt",
            str(receipt_path),
        ],
        cwd=installed["workdir"],
        env=_scrubbed_env(installed["workdir"]),
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    verdict = _last_json(completed.stdout)
    assert verdict["valid"] is True, verdict
    assert verdict["protocol"] == "okg.external_distribution_conformance/v2"
    assert verdict["receipt_verdict"] == "blocked"
    assert verdict["receipt_digest"] == receipt.receipt_digest

    _refuse_a_pass_on_an_unmeasured_arm(request, receipt)
    _refuse_a_mis_owned_environment_block(request, receipt)
    _fail_the_materialization_arm_on_a_mutated_asset(
        request=request,
        authority=authority,
        admission=admission,
        lock_digest=lock["lock_digest"],
        plan_digest=plan["plan_digest"],
        definition_digest=definition_digest,
        declared_asset_digests=declared_asset_digests,
        materialized=materialized,
        materialization_digest=materialization_digest,
        evidence=evidence,
        blocked_arms=blocked_arms,
        blocked_diagnostics=blocked_diagnostics,
    )

    record_property(
        "hermetic_conformance_request", request.model_dump_json(by_alias=True)
    )
    record_property(
        "hermetic_conformance_receipt", receipt.model_dump_json(by_alias=True)
    )
    assert conformance_receipt_digest(receipt) == receipt.receipt_digest


def _baseline_resolution(archive: Path, instance: dict[str, Any]) -> dict[str, str]:
    """A second, real resolution of the same package, for the recovery input."""

    plan, lock = resolve_install_plan(
        archive,
        _ANALYSIS_BUNDLE,
        instance,
        environ={_SECRET_ENV: "not-a-real-token"},
    )
    return {
        "bundle": _ANALYSIS_BUNDLE,
        "plan_digest": plan["plan_digest"],
        "lock_digest": lock["lock_digest"],
    }


def _resealed(receipt: Any, changes: Mapping[str, Any]) -> dict[str, Any]:
    """Re-digest a hand-edited receipt so only the edited claim is under test."""

    from okg.substrate.contracts.deployment_product.conformance import (
        conformance_receipt_digest,
        load_conformance_receipt,
    )

    payload = dict(receipt.model_dump(mode="json", by_alias=True), **dict(changes))
    payload["receipt_digest"] = _ZERO_DIGEST
    payload["receipt_digest"] = conformance_receipt_digest(
        load_conformance_receipt(payload)
    )
    return payload


def _refuse_a_pass_on_an_unmeasured_arm(request: Any, receipt: Any) -> None:
    """Claiming a pass for an arm this run never executed is refused."""

    from okg.substrate.contracts.deployment_product.conformance import (
        ConformanceContractError,
        load_conformance_receipt,
    )

    payload = receipt.model_dump(mode="json", by_alias=True)
    arms = [dict(item) for item in payload["arms"]]
    for arm in arms:
        if arm["arm"] == "update_rollback_recovery":
            arm.update(
                status="pass",
                mutation="mutating",
                retry_classification="safe",
                diagnostic_codes=[],
            )
    payload["arms"] = arms
    payload["diagnostics"] = [
        item
        for item in payload["diagnostics"]
        if item["arm"] != "update_rollback_recovery"
    ]
    payload["claim"] = dict(
        payload["claim"],
        completed_arms=sorted(
            {*payload["claim"]["completed_arms"], "update_rollback_recovery"}
        ),
    )
    with pytest.raises(ConformanceContractError) as excinfo:
        load_conformance_receipt(payload)
    assert excinfo.value.code == "conformance_receipt_invalid"
    assert request.case.claim_profile == "ci_fixture"


def _refuse_a_mis_owned_environment_block(request: Any, receipt: Any) -> None:
    """A contract-valid receipt that breaks the settled profile is refused.

    Blaming the distribution for an environment this harness does not provide
    passes every wire rule; the issue-1794 profile check is what refuses it,
    with `conformance_profile_refused`.
    """

    from okg.substrate.contracts.deployment_product.conformance import (
        load_conformance_receipt,
        validate_conformance_exchange,
    )
    from okg.substrate.distributions.conformance_diagnostics import (
        ConformanceProfileError,
        check_conformance_diagnostic_profiles,
        runtime_environment_unavailable_diagnostic,
    )

    # The producer refuses to write the block in the first place.
    with pytest.raises(ConformanceProfileError) as produced:
        runtime_environment_unavailable_diagnostic(
            arm="readback_health",
            phase="readback",
            reference="mcp-graph-read-surface",
            gap="runtime-unavailable",
            owner="distribution",
        )
    assert produced.value.data["reason"] == "owner_not_environmental"

    diagnostics = [dict(item) for item in receipt.model_dump(mode="json")["diagnostics"]]
    for item in diagnostics:
        if item["arm"] == "readback_health":
            item["owner"] = "distribution"
    mutated = load_conformance_receipt(_resealed(receipt, {"diagnostics": diagnostics}))
    validate_conformance_exchange(request, mutated)
    with pytest.raises(ConformanceProfileError) as excinfo:
        check_conformance_diagnostic_profiles(request, mutated)
    assert excinfo.value.code == "conformance_profile_refused"
    assert excinfo.value.data["reason"] == "profile_fields_mismatch"
    assert excinfo.value.data["code"] == "conformance_runtime_environment_unavailable"


def _fail_the_materialization_arm_on_a_mutated_asset(
    *,
    request: Any,
    authority: Any,
    admission: Mapping[str, str],
    lock_digest: str,
    plan_digest: str,
    definition_digest: str,
    declared_asset_digests: Mapping[str, str],
    materialized: Mapping[str, str],
    materialization_digest: str,
    evidence: Any,
    blocked_arms: tuple[Any, ...],
    blocked_diagnostics: tuple[Any, ...],
) -> None:
    """A mutated or absent materialized asset fails the arm, and says why."""

    from okg.substrate.contracts.deployment_product.conformance import (
        ConformanceArmResult,
        ConformanceDiagnostic,
        ConformanceEvidence,
        validate_conformance_exchange,
    )
    from okg.substrate.distributions.conformance_diagnostics import (
        DECLARED_ASSET_CODE,
        check_conformance_diagnostic_profiles,
        declared_asset_missing_diagnostic,
        failed_arm_result,
    )
    from okg.substrate.distributions.conformance_evidence import (
        ConformanceEvidenceError,
        build_materialization_evidence,
    )

    target = f"{_OWNER}:lab-units-schema"
    arguments: dict[str, Any] = dict(
        materialization_digest=materialization_digest,
        declared_asset_digests=dict(declared_asset_digests),
        authoring_tree_present=False,
    )
    with pytest.raises(ConformanceEvidenceError) as mutated:
        build_materialization_evidence(
            **arguments,
            materialized_asset_digests=dict(materialized, **{target: _ZERO_DIGEST}),
        )
    assert mutated.value.data["reason"] == "asset_digest_mismatch"
    assert mutated.value.data["path"] == f"materialized_asset_digests.{target}"

    absent = {key: value for key, value in materialized.items() if key != target}
    with pytest.raises(ConformanceEvidenceError) as missing:
        build_materialization_evidence(
            **arguments, materialized_asset_digests=absent
        )
    assert missing.value.data["reason"] == "asset_not_materialized"

    # The refusal becomes a failed arm the contract and the profile accept.
    failed = ConformanceArmResult.model_validate(
        failed_arm_result(
            "artifact_materialization",
            (DECLARED_ASSET_CODE,),
            mutation="none",
            retry_classification="never",
        )
    )
    diagnostic = ConformanceDiagnostic.model_validate(
        declared_asset_missing_diagnostic(
            asset_ref=target, asset_digest=declared_asset_digests[target]
        )
    )
    receipt = _build_conformance_receipt(
        request=request,
        authority=authority,
        admission=admission,
        lock_digest=lock_digest,
        plan_digest=plan_digest,
        definition_digest=definition_digest,
        applied_instance_id=_INSTANCE,
        evidence=ConformanceEvidence(
            signals=evidence.signals,
            reapply=evidence.reapply,
        ),
        arms=_passing_arms("contract", "idempotent_reapply")
        + (failed,)
        + blocked_arms,
        diagnostics=(diagnostic, *blocked_diagnostics),
    )
    assert receipt.verdict == "fail"
    assert receipt.claim.satisfied is False
    validate_conformance_exchange(request, receipt)
    check_conformance_diagnostic_profiles(request, receipt)
