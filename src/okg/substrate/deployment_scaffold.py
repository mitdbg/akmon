"""Deployment skeleton writer for ``okg deployment new``.

This module owns the file-system side of deployment scaffold creation.
CLI wiring should stay thin: resolve arguments, call
``create_deployment_scaffold()``, and emit ``result.json_payload()``.
"""
from __future__ import annotations

import json
import re
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.deployments import find_deployments_root
from okg.substrate.errors import SubstrateError
from okg.substrate.support.user_hints import okg_prefix


_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,78}[a-z0-9])?$")
_SLUG_CHAR_RE = re.compile(r"[^a-z0-9]+")
_PLACEHOLDER_FILE = ".gitkeep"


__all__ = [
    "DeploymentScaffoldPlan",
    "DeploymentScaffoldResult",
    "build_deployment_scaffold_plan",
    "create_deployment_scaffold",
    "safe_deployment_slug",
    "scaffold_deployment",
]


@dataclass(frozen=True)
class DeploymentScaffoldPlan:
    """Resolved deployment scaffold target before any writes happen."""

    requested_name: str
    deployment_name: str
    deployments_root: Path
    deployment_dir: Path


@dataclass(frozen=True)
class DeploymentScaffoldResult:
    """Files and follow-up commands emitted by the scaffold writer."""

    plan: DeploymentScaffoldPlan
    files_written: list[Path]
    directories_created: list[Path]
    clobbered: bool
    next_commands: list[str]

    @property
    def deployment_name(self) -> str:
        return self.plan.deployment_name

    @property
    def deployment_dir(self) -> Path:
        return self.plan.deployment_dir

    def json_payload(self) -> dict[str, Any]:
        """Return the CLI-ready JSON shape for ``okg deployment new``."""

        return {
            "ok": True,
            "deployment": {
                "name": self.plan.deployment_name,
                "dir": str(self.plan.deployment_dir),
                "deployments_root": str(self.plan.deployments_root),
            },
            "files_written": [str(p) for p in self.files_written],
            "directories_created": [
                str(p) for p in self.directories_created
            ],
            "clobbered": self.clobbered,
            "next_commands": list(self.next_commands),
        }


def safe_deployment_slug(name: str) -> str:
    """Normalize a human deployment name to a safe directory slug.

    Path-like input is rejected before slugification. That keeps
    ``foo/../bar`` from becoming a harmless-looking ``foo-bar`` and
    makes traversal attempts fail loudly.
    """

    raw = str(name).strip()
    if not raw:
        raise _invalid_name(name, "deployment name is empty")
    if "\x00" in raw:
        raise _invalid_name(name, "deployment name contains a NUL byte")
    if Path(raw).is_absolute():
        raise _invalid_name(name, "deployment name must not be absolute")
    if any(sep in raw for sep in ("/", "\\")):
        raise _invalid_name(name, "deployment name must not contain path separators")
    if ":" in raw:
        raise _invalid_name(name, "deployment name must not contain ':'")
    if raw in {".", ".."} or raw.startswith(".") or ".." in raw:
        raise _invalid_name(name, "deployment name must not contain dot segments")

    slug = _SLUG_CHAR_RE.sub("-", raw.lower()).strip("-")
    slug = re.sub(r"-{2,}", "-", slug)
    if not slug or not _SLUG_RE.fullmatch(slug):
        raise _invalid_name(
            name,
            "deployment name must resolve to a slug matching "
            "[a-z0-9][a-z0-9-]*",
        )
    return slug


def build_deployment_scaffold_plan(
    name: str,
    *,
    deployments_root: str | Path | None = None,
) -> DeploymentScaffoldPlan:
    """Resolve the default scaffold target under the deployments root."""

    deployment_name = safe_deployment_slug(name)
    root = _resolve_deployments_root(deployments_root)
    target = root / deployment_name
    _assert_child_path(root, target, requested_name=name)
    return DeploymentScaffoldPlan(
        requested_name=str(name),
        deployment_name=deployment_name,
        deployments_root=root,
        deployment_dir=target,
    )


def create_deployment_scaffold(
    name: str,
    *,
    deployments_root: str | Path | None = None,
    force: bool = False,
    yes: bool = False,
) -> DeploymentScaffoldResult:
    """Build and write a new deployment scaffold."""

    plan = build_deployment_scaffold_plan(
        name,
        deployments_root=deployments_root,
    )
    return scaffold_deployment(plan, force=force, yes=yes)


def scaffold_deployment(
    plan: DeploymentScaffoldPlan,
    *,
    force: bool = False,
    yes: bool = False,
) -> DeploymentScaffoldResult:
    """Write a loader-valid deployment directory from ``plan``.

    Existing non-empty directories are preserved unless both ``force``
    and ``yes`` are true. The function never writes a real DSN, secrets,
    ``.env``, or ``.okg/local.env``.
    """

    target = plan.deployment_dir
    clobbered = False
    if force and not yes:
        raise SubstrateError(
            "--force requires --yes to overwrite an existing deployment directory",
            data={
                "deployment_name": plan.deployment_name,
                "deployment_dir": str(target),
            },
            fix_hint="re-run with --force --yes",
        )
    if target.exists() and not target.is_dir():
        raise SubstrateError(
            f"deployment target {target} exists and is not a directory",
            data={
                "deployment_name": plan.deployment_name,
                "deployment_dir": str(target),
            },
            fix_hint="choose a different deployment name or remove the file",
        )
    if target.is_dir() and any(target.iterdir()):
        if not force:
            raise SubstrateError(
                f"deployment directory {target} already exists and is non-empty",
                data={
                    "deployment_name": plan.deployment_name,
                    "deployment_dir": str(target),
                },
                fix_hint="choose a different name or re-run with --force --yes",
            )
        shutil.rmtree(target)
        clobbered = True

    directories = _create_directories(target)
    files = _write_scaffold_files(target, plan)
    return DeploymentScaffoldResult(
        plan=plan,
        files_written=files,
        directories_created=directories,
        clobbered=clobbered,
        next_commands=_next_commands(plan),
    )


def _resolve_deployments_root(
    deployments_root: str | Path | None,
) -> Path:
    if deployments_root is not None:
        return Path(deployments_root).expanduser().resolve()
    discovered = find_deployments_root()
    if discovered is not None:
        return discovered.resolve()
    return (Path.cwd() / "deployments").resolve()


def _assert_child_path(
    root: Path,
    target: Path,
    *,
    requested_name: str,
) -> None:
    try:
        target.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise _invalid_name(
            requested_name,
            "deployment target must stay under the deployments root",
        ) from exc


def _create_directories(target: Path) -> list[Path]:
    dirs = [
        target,
        target / "schemas",
        target / "schemas" / "bridges",
        target / "fixtures",
        target / "workflows",
        target / "enrichers",
        target / "views",
    ]
    for path in dirs:
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def _write_scaffold_files(
    target: Path,
    plan: DeploymentScaffoldPlan,
) -> list[Path]:
    files: list[Path] = []

    for rel_path, text in _file_bodies(plan).items():
        path = target / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        files.append(path)

    for rel_dir in (
        "schemas",
        "schemas/bridges",
        "workflows",
        "enrichers",
        "views",
    ):
        path = target / rel_dir / _PLACEHOLDER_FILE
        path.write_text("", encoding="utf-8")
        files.append(path)

    return files


def _file_bodies(plan: DeploymentScaffoldPlan) -> dict[str, str]:
    name = plan.deployment_name
    deployment_ref = _command_ref(plan)
    fixture_path = plan.deployment_dir / "fixtures" / "bootstrap_fixture.json"
    return {
        "deployment.yaml": (
            f"name: {name}\n"
            "description: Describe this deployment before first ingest.\n"
            "postgres:\n"
            "  dsn: ${OKG_DSN}\n"
            "schema_dir: schemas\n"
            "source_registry: source_registry.yaml\n"
            "modules:\n"
            "  - document_starter\n"
            "runtime:\n"
            "  enabled: false\n"
            "nomos:\n"
            "  # Privacy declarations do not activate runtime enforcement.\n"
            "  # Deployment readiness still checks operational policy separately.\n"
            "  deployment_posture:\n"
            "    deployment_class: personal_okg\n"
            "    read_principals: [human_operator, okg_service]\n"
            "    write_principals: [human_operator, source_adapter, okg_service]\n"
            "    mcp_write_default: deny\n"
            "    export_policy: no_export\n"
            "    secret_handling: secret_ref_required\n"
            "    live_call_default: deny\n"
            "    allowed_source_sensitivities: [local_private]\n"
            "    allowed_data_classifications: [private]\n"
            "    allowed_exportability: [no_export]\n"
            "    privacy_obligations: [audit, no_export]\n"
            "    readiness_profiles: [personal]\n"
            "chronos:\n"
            "  # How long superseded graph history is kept. Without this block\n"
            "  # every superseded node and edge version accumulates forever:\n"
            "  # one large deployment reached 88% dead history and 136 GB\n"
            "  # before it was declared. 14d keeps time travel across a\n"
            "  # normal working window; audit_tail_generations keeps the last\n"
            "  # 50 generations reachable regardless of age.\n"
            "  #\n"
            "  # Declaring this block also turns OFF the publisher per-cycle\n"
            "  # prune of the attribute-effective interval sidecar: a declared\n"
            "  # policy makes the confirmed `okg generation compact` plan the\n"
            "  # sole deletion authority, because the per-cycle prune cannot\n"
            "  # tell full/horizon/latest-only classifiers apart. Run\n"
            "  # `okg generation compact` on a schedule, or `okg doctor` will\n"
            "  # warn from the first row of dead history.\n"
            "  history_retention:\n"
            "    default:\n"
            "      horizon: 14d\n"
            "    audit_tail_generations: 50\n"
            "# Free-disk floors, in percent available: `okg doctor` warns below\n"
            "# the first and treats the second as a blocker. Without this block\n"
            "# `okg doctor` reports its own disk check as misconfigured.\n"
            "host_storage:\n"
            "  warn_available_pct: 15\n"
            "  blocker_available_pct: 6\n"
        ),
        "source_registry.yaml": (
            "sources:\n"
            "  bootstrap-fixture:\n"
            f"    ownership_id: {name}.bootstrap_fixture\n"
            "    source_class: discovery_crawl\n"
            "    record_identity_kind: scoped_locator\n"
            "    record_identity_fields:\n"
            "      - id\n"
            "    source_revision_kind: content_hash\n"
            "    deletion_semantics: missing_from_completed_scope\n"
            "    publication_mode: published_generation\n"
            "    # Raw storage is authorized only for this synthetic fixture.\n"
            "    # Review this policy before replacing it with real data.\n"
            "    source_policy:\n"
            "      sensitivity: local_private\n"
            "      data_classification: private\n"
            "      credential_ref_policy: none\n"
            "      live_call_allowed: false\n"
            "      exportability: no_export\n"
            "      retention: normal\n"
            "      provenance: local_filesystem\n"
            "      privacy_obligations: [audit, no_export]\n"
            "      store_raw: true\n"
            "    module: okg.substrate.library.sources.bootstrap_fixture\n"
            "    class: BootstrapFixtureSource\n"
            "    params:\n"
            f"      fixture_path: {json.dumps(str(fixture_path))}\n"
            "    admission_policy:\n"
            f"      producer_id: {name}.bootstrap_fixture\n"
            "      producer_kind: source\n"
            "      trust_label: fixture-trusted\n"
            "      admission_mode: fast_track\n"
            "      output_scope_summary:\n"
            "        summary: scaffold bootstrap fixture facts\n"
            "    sync:\n"
            "      triggers:\n"
            "        - manual\n"
            "        - reconcile\n"
            "      default_event_mode: scope_complete\n"
            "      reconcile_mode: scope_complete\n"
        ),
        "fixtures/bootstrap_fixture.json": (
            json.dumps(
                {
                    "nodes": [
                        {
                            "node_id": f"{name}:doc:1",
                            "subtype": "document",
                            "attrs": {
                                "document_id": "doc:1",
                                "title": "Bootstrap fixture",
                                "body": (
                                    "# Getting started\n\nReplace "
                                    "this with a deployment-owned source."
                                ),
                            },
                            "source_record_id": {"id": "doc:1"},
                            "source_revision": {"v": 1},
                        },
                        {
                            "node_id": f"{name}:doc:1:section:1",
                            "subtype": "section",
                            "attrs": {
                                "section_id": "doc:1:section:1",
                                "heading": "Getting started",
                                "body": "Replace this with a deployment-owned source.",
                            },
                            "source_record_id": {"id": "doc:1:section:1"},
                            "source_revision": {"v": 1},
                        },
                    ],
                    "edges": [{
                        "src": f"{name}:doc:1",
                        "dst": f"{name}:doc:1:section:1",
                        "edge_type": "contains",
                        "source_record_id": {"id": "doc:1:contains:section:1"},
                        "source_revision": {"v": 1},
                    }],
                },
                indent=2,
            )
            + "\n"
        ),
        "invariants.yaml": "invariants: []\n",
        "views_registry.yaml": (
            f"deployment: {name}\n"
            "enabled: true\n"
            "views: []\n"
        ),
        ".env.example": (
            "# Local runtime state only. Do not commit a real .env file.\n"
            "# Set OKG_DSN in your shell or copy this file outside versioned\n"
            "# deployment config before running migrate, ingest, or publish.\n"
            "OKG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DATABASE\n"
        ),
        "README.md": (
            f"# {name}\n\n"
            "This deployment scaffold contains committed configuration only.\n"
            "Keep real DSNs, API tokens, OAuth material, and other secrets in\n"
            "local runtime state such as your shell environment or an ignored\n"
            ".env file. The scaffold does not create `.env` or `.okg/local.env`.\n\n"
            "Before first ingest:\n\n"
            "- Edit `deployment.yaml` with the deployment description and\n"
            "  any additional substrate ontology modules you want to load.\n"
            "- Add deployment-owned schemas under `schemas/` and bridge\n"
            "  narrowings under `schemas/bridges/`.\n"
            "- Replace the bootstrap fixture source in `source_registry.yaml`\n"
            "  once a real source adapter and ownership policy are ready.\n"
            "- Put optional workflows, enrichers, and read-side view code in\n"
            "  `workflows/`, `enrichers/`, and `views/`.\n\n"
            "Smoke commands:\n\n"
            "```bash\n"
            f"{okg_prefix()} deployment env {deployment_ref} --json\n"
            f"{okg_prefix()} catalog load --deployment {deployment_ref} --json\n"
            f"{okg_prefix()} ingest --deployment {deployment_ref} --json\n"
            f"{okg_prefix()} deployment ready {deployment_ref} --checks general --json\n"
            "```\n"
        ),
    }


def _next_commands(plan: DeploymentScaffoldPlan) -> list[str]:
    deployment_ref = _command_ref(plan)
    return [
        f"{okg_prefix()} deployment env {deployment_ref} --json",
        f"{okg_prefix()} catalog load --deployment {deployment_ref} --json",
        f"{okg_prefix()} ingest --deployment {deployment_ref} --json",
        f"{okg_prefix()} deployment ready {deployment_ref} --checks general --json",
    ]


def _command_ref(plan: DeploymentScaffoldPlan) -> str:
    path = plan.deployment_dir.resolve()
    try:
        ref = str(path.relative_to(Path.cwd().resolve()))
    except ValueError:
        ref = str(path)
    return shlex.quote(ref)


def _invalid_name(value: object, reason: str) -> SubstrateError:
    return SubstrateError(
        f"invalid deployment name {value!r}: {reason}",
        data={"deployment_name": str(value), "reason": reason},
        fix_hint=(
            "use a plain deployment name such as `my-lab-okg`; "
            "do not pass a path"
        ),
    )
