"""Academic-researcher profile CLI.

Discovered by the substrate's `cli/profile_cli_loader.py` (per
`retire-pi-prefix-commands`). Operator-facing verbs:

  - `okg install`        — one-command deployment bootstrap
                            (Docker + scaffold + catalog-ready
                            Postgres + agent-runtime MCP
                            registration). Source ingestion starts
                            only after the setup wizard is approved.
  - `okg add <source>`   — low-level recovery hook for local/path/URL
                            sources on an existing deployment.
  - `okg remove <source>` — drop a source from the registry;
                            existing graph facts remain.
  - `okg reset`          — destroy Postgres volume + scaffold.

Implementation lives in this file (`install.py`) + sibling
`wizards.py` (per-source config wizards). The substrate-wide
`okg doctor --deployment <name>` covers health checks; the
substrate-wide `okg admission` covers extracted-candidates
review. No academic-researcher-specific verbs survive outside this
profile's CLI directory.
"""
from __future__ import annotations
from okg.substrate import fastyaml

import argparse
import json
import os
import re
import secrets
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

from okg.substrate.cli._output import command_output_tail
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.cli.command_reference import Masked, okg_command
from okg.substrate.cli.init import (
    SETUP_STATUS_AWAITING_APPROVAL,
    add_source_next_step,
)
from okg.substrate.deployments import DeploymentResolutionError
from okg.substrate.errors import SubstrateError


def _agent_runtime_registration_extra_env() -> dict[str, str]:
    """Explicit runtime config env for academic-researcher MCP registrations."""
    extra_env = {"OKG_MCP_ALLOW_WRITES": "1"}
    for key in ("OKG_MCP_RO_DSN", "OKG_MCP_RW_DSN"):
        value = os.environ.get(key)
        if value:
            extra_env[key] = value
    return extra_env


def _child_interpreter_report() -> str:
    """One line naming the interpreter, the okg package and its version.

    A PATH collision is invisible in the child's own usage text, so the
    failure has to carry the identity of what ran. A version that cannot be
    resolved is reported as such rather than omitted.
    """
    try:
        import okg

        package = getattr(okg, "__file__", None) or "<namespace package>"
    except Exception as exc:  # pragma: no cover - import of self
        package = f"<unimportable: {exc}>"
    try:
        from importlib.metadata import PackageNotFoundError, version

        try:
            release = version("okg")
        except PackageNotFoundError:
            release = "unavailable (okg distribution not installed)"
    except Exception as exc:
        release = f"unavailable ({exc})"
    return (
        f"ran: {shlex.join(running_okg_argv())} "
        f"(okg package: {package}, version: {release})"
    )


_DSN_FLAGS = ("--postgres-dsn", "--dsn")
_DSN_FLAG_VALUE_RE = re.compile(r"(--(?:postgres-dsn|dsn))([=\s]+)(\S+)")


def _redacted_text(text: str) -> str:
    """``text`` with DSN flag values and DSN passwords masked.

    Issue #2211. The failure report echoes the child's own stdout and
    stderr, and an argparse child echoes back the arguments it refused --
    the DSN among them. Redacting only the parent argv line would print the
    password one line further down. Both the ``--flag value`` and the
    ``--flag=value`` spelling are covered, as is a DSN that appears in the
    tail on its own: its secrets go through the shared
    ``okg.substrate.db.dsn.redact_text``, which also masks a URL password
    with an empty user name (``postgresql://:pw@db``), a ``?password=``
    value and a keyword-form password.
    """
    from okg.substrate.db.dsn import redact_text

    masked = _DSN_FLAG_VALUE_RE.sub(r"\1\2<redacted-dsn>", text)
    return redact_text(masked, dsn_flags=())


def _redacted_argv(argv: list[str]) -> list[str]:
    """``argv`` with the value of every DSN-bearing flag masked."""
    out: list[str] = []
    mask_next = False
    for item in argv:
        if mask_next:
            out.append("<redacted-dsn>")
            mask_next = False
            continue
        if any(item.startswith(f"{flag}=") for flag in _DSN_FLAGS):
            out.append(f"{item.split('=', 1)[0]}=<redacted-dsn>")
            continue
        out.append(_redacted_text(item))
        mask_next = item in _DSN_FLAGS
    return out


# ---------- internal: Postgres + scaffold provisioning ----------


def _provision_postgres_and_scaffold(args: argparse.Namespace, *, as_json: bool) -> int:
    """Thin deterministic provisioning: preflight + Postgres +
    okg init.

    Per `align-substrate-cli-docs-and-runbooks` Phase 4: this verb
    is deprecated in favor of `okg install --orcid <id>`, which
    wraps this plus agent-runtime MCP registration and the
    conversational setup handoff.
    """
    repo_root = _find_repo_root()
    deployment_name = args.deployment_name
    orcid = args.orcid
    # Issue #2127 F9: carried from `_cmd_install` so the scaffold child
    # process, the password reuse probe and the post-scaffold manifest edits
    # all read the SAME root the operator named.
    deployments_root = getattr(args, "deployments_root_resolved", None)
    use_local_pg = bool(args.use_local_postgres)
    # Per-deployment Postgres password (reused on re-run, fresh on a
    # new install) — replaces the universal hardcoded `okg`. Unused
    # on the --use-local-postgres path (operator owns that DSN).
    # Per `harden-okg-network-and-secrets`.
    pg_password = _resolve_pg_password(
        deployment_name, deployments_root=deployments_root,
    )
    # Per `dead-simple-install`: when --postgres-port is omitted,
    # defer to Docker's ephemeral-port assignment (race-free).
    # `args.postgres_port` may be None at this point; the port
    # is read back from Docker after `compose up --wait` and
    # written back onto args (line below the compose call).
    # When the operator passes --postgres-port explicitly we
    # honor it; when they pass `--use-local-postgres` we use
    # their DSN as-is.
    if use_local_pg:
        postgres_dsn = args.postgres_dsn
    elif args.postgres_port is not None:
        postgres_dsn = args.postgres_dsn or _default_dsn(
            deployment_name, args.postgres_port, pg_password,
        )
    else:
        # Will be resolved post-`compose up`.
        postgres_dsn = None

    if args.reset:
        return _provision_postgres_and_scaffold_reset(
            deployment_name=deployment_name,
            repo_root=repo_root,
            as_json=as_json,
            deployments_root=deployments_root,
        )

    # Preflight.
    if not use_local_pg:
        if shutil.which("docker") is None:
            return _fail(
                # Issue #2210 N3: --use-local-postgres is not a flag
                # `okg install` has; --postgres-dsn is the one that now
                # skips Docker provisioning.
                "docker not on PATH; pass --postgres-dsn <dsn> "
                "to skip Docker provisioning",
                as_json=as_json,
            )
        try:
            subprocess.run(
                ["docker", "info"],
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            stderr = e.stderr.decode("utf-8", errors="replace").strip()
            # Triage instead of blaming the daemon for everything.
            # On Linux the daemon is typically RUNNING while the
            # operator simply isn't in the `docker` group — `docker
            # info` fails with "permission denied" on the socket,
            # and telling them to "start the daemon" sends them in
            # circles (observed live on a PI's first install,
            # 2026-06-09).
            if "permission denied" in stderr.lower():
                return _fail(
                    "docker is running but this user can't reach it "
                    f"(permission denied on the Docker socket): {stderr}\n"
                    "Fix: sudo usermod -aG docker $USER\n"
                    "then log out and back in (or run `newgrp docker`) "
                    "and re-run the install.",
                    as_json=as_json,
                )
            if sys.platform == "linux":
                hint = "start it with: sudo systemctl enable --now docker"
            else:
                hint = "start Docker Desktop and wait for it to finish booting"
            return _fail(
                f"docker info failed (daemon not reachable — {hint}): "
                f"{stderr}",
                as_json=as_json,
            )
        # Compose v2 ships separately from the docker CLI on most
        # Linux distros. Without it, the image build / compose-up
        # steps below die mid-install with the bare CLI's cryptic
        # "unknown shorthand flag: 'd' in -d" (observed live on a
        # PI's first install, 2026-06-09). Fail at preflight with
        # the package to install instead.
        compose_probe = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
        )
        if compose_probe.returncode != 0:
            return _fail(
                "docker is running, but the Compose v2 plugin is "
                "missing ('docker compose' is not available).\n"
                "Ubuntu/Debian: sudo apt-get install docker-compose-v2\n"
                "Fedora/RHEL:   sudo dnf install docker-compose-plugin\n"
                "macOS:         update Docker Desktop (Compose is bundled)\n"
                "then re-run the install.",
                as_json=as_json,
            )
        # The compose.yaml references the substrate's `okg-pg17:local`
        # image. If absent, build it ourselves rather than refusing
        # — first-time install should be one command from the
        # operator's perspective. The build is idempotent + cached;
        # subsequent installs skip past this branch in milliseconds.
        result = subprocess.run(
            ["docker", "image", "inspect", "okg-pg17:local"],
            capture_output=True,
        )
        if result.returncode != 0:
            build_failure = _build_okg_pg17_image(repo_root)
            if build_failure is not None:
                return _fail(build_failure, as_json=as_json)
    if sys.version_info < (3, 12):
        return _fail(
            f"Python 3.12+ required (got {sys.version_info[:2]})",
            as_json=as_json,
        )

    # Postgres provisioning via shipped compose.yaml.
    # `--wait` blocks until the container's healthcheck passes —
    # without it, `okg init`'s first publish fires while Postgres
    # is still running its init scripts and gets "server closed
    # connection" / "connection refused" errors. The healthcheck
    # uses pg_isready, so a successful wait means the DB is
    # actually accepting connections.
    if not use_local_pg:
        compose_path = _find_compose_yaml()
        env = os.environ.copy()
        env["DEPLOYMENT_NAME"] = deployment_name
        # Drives compose's POSTGRES_PASSWORD (applied at DB init on a
        # fresh volume; ignored on an existing one, where we've
        # reused the baked-in value). Per
        # `harden-okg-network-and-secrets`.
        env["POSTGRES_PASSWORD"] = pg_password
        # Per `dead-simple-install`: `POSTGRES_PORT=0` is the
        # compose-recognized sentinel for "let Docker pick an
        # ephemeral host port." When the operator passes
        # --postgres-port explicitly we honor it; otherwise we
        # ask Docker, then read the assignment back below.
        env["POSTGRES_PORT"] = str(args.postgres_port or 0)
        try:
            subprocess.run(
                ["docker", "compose", "-f", str(compose_path),
                 "-p", f"okg-{deployment_name}", "up", "-d",
                 "--wait"],
                check=True,
                env=env,
            )
        except subprocess.CalledProcessError as e:
            return _fail(
                f"docker compose up failed: rc={e.returncode}",
                as_json=as_json,
            )

        # If the port was Docker-assigned, query it back and
        # write to args so downstream code (DSN derivation,
        # agent-runtime registration) sees the real value.
        if args.postgres_port is None:
            try:
                port_proc = subprocess.run(
                    ["docker", "compose", "-f", str(compose_path),
                     "-p", f"okg-{deployment_name}", "port",
                     "postgres", "5432"],
                    check=True, capture_output=True, env=env,
                )
            except subprocess.CalledProcessError as e:
                return _fail(
                    f"failed to read Docker-assigned host port: "
                    f"rc={e.returncode}",
                    as_json=as_json,
                )
            assigned = _parse_compose_port(
                port_proc.stdout.decode("utf-8", errors="replace"),
            )
            if assigned is None:
                return _fail(
                    f"could not parse Docker-assigned port from "
                    f"`docker compose port` output: "
                    f"{port_proc.stdout!r}",
                    as_json=as_json,
                )
            args.postgres_port = assigned
            postgres_dsn = _default_dsn(
                deployment_name, assigned, pg_password,
            )
            # Docker stores an ephemeral HostPort request as
            # HostPort=0, which can be reassigned on a later
            # container start. Once Docker tells us the chosen port,
            # immediately reconcile the compose stack with that fixed
            # port so deployment.yaml's DSN remains valid across
            # stop/start lifecycle operations.
            env["POSTGRES_PORT"] = str(assigned)
            try:
                subprocess.run(
                    ["docker", "compose", "-f", str(compose_path),
                     "-p", f"okg-{deployment_name}", "up", "-d",
                     "--wait"],
                    check=True,
                    env=env,
                )
                pinned_proc = subprocess.run(
                    ["docker", "compose", "-f", str(compose_path),
                     "-p", f"okg-{deployment_name}", "port",
                     "postgres", "5432"],
                    check=True, capture_output=True, env=env,
                )
            except subprocess.CalledProcessError as e:
                return _fail(
                    f"failed to pin Docker-assigned host port "
                    f"{assigned}: rc={e.returncode}",
                    as_json=as_json,
                )
            pinned = _parse_compose_port(
                pinned_proc.stdout.decode("utf-8", errors="replace"),
            )
            if pinned != assigned:
                return _fail(
                    f"Docker port pin mismatch: expected {assigned}, "
                    f"got {pinned!r}",
                    as_json=as_json,
                )

    # Scaffold this profile, in a child process, with answers passed
    # through. It calls the scaffold path DIRECTLY rather than through
    # `okg init`: since `define-newcomer-entry-path` made `init` an alias of
    # `install`, going through it would mean this installer re-entering
    # `okg install` — a loop that terminates only by accident of branch
    # order, and whose failure text would name a command the operator never
    # typed.
    init_argv = [
        *running_okg_argv(),
        "install", "--scaffold-only",
        "--profile", "academic-researcher",
        "--deployment-name", deployment_name,
        "--postgres-dsn", postgres_dsn,
    ]
    # Issue #2127 F9. The generic scaffold path (`cli/init.py`) already
    # resolves and creates this root; passing it through is what makes
    # `okg install --deployments-root <dir>` land in <dir> instead of
    # reporting one target and writing another.
    if deployments_root is not None:
        init_argv += ["--deployments-root", str(deployments_root)]
    # Per `dead-simple-install`: ORCID is optional. Only pass
    # --orcid through to `okg init` when the operator provided
    # one. Without it, the dblp_by_orcid source falls back to
    # name-based search (per
    # `extend-dblp-by-orcid-with-name-fallback`).
    if orcid:
        init_argv += ["--orcid", orcid]
    if args.papers_dir:
        init_argv += ["--papers-dir", args.papers_dir]
    if args.cv_path:
        init_argv += ["--cv-path", args.cv_path]
    if args.group_webpage:
        init_argv += ["--group-webpage", args.group_webpage]
    if args.pi_name:
        init_argv += ["--pi-name", args.pi_name]
    if getattr(args, "dblp_pid", None):
        init_argv += ["--dblp-pid", args.dblp_pid]
    if args.mcp_port:
        init_argv += ["--mcp-port", str(args.mcp_port)]
    if getattr(args, "git_repos", None):
        init_argv += ["--git-repos", args.git_repos]
    if args.yes:
        init_argv += ["--yes"]
    if args.force:
        init_argv += ["--force"]

    try:
        subprocess.run(
            init_argv,
            check=True,
            # None outside a checkout (issue #2313): the child inherits the
            # operator's working directory, never one inside the package.
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        recorded = (
            None if use_local_pg else _record_docker_project_after_failed_scaffold(
                deployment_name,
                repo_root=repo_root,
                deployments_root=deployments_root,
                host_port=args.postgres_port,
            )
        )
        # Carry BOTH streams: `okg init` prints its structured failure to
        # STDOUT and logs warnings to STDERR, so a stderr-first message would
        # hide the report whenever a warning was logged.
        # Issue #2211. Name the executable that actually ran and the argv it
        # ran with. Without them a child that refuses an argument reads as an
        # okg bug; with them a stale second okg on PATH is one line of text.
        return _fail(
            f"scaffold step failed: rc={e.returncode}\n"
            f"{_child_interpreter_report()}\n"
            f"argv: {shlex.join(_redacted_argv(init_argv))}\n"
            f"{_redacted_text(command_output_tail(e))}"
            + (f"\n{recorded}" if recorded else ""),
            as_json=as_json,
        )
    except BaseException:
        # Interrupted mid-scaffold (Ctrl-C): the Postgres container is up
        # all the same, so record it before the interrupt propagates.
        if not use_local_pg:
            _record_docker_project_after_failed_scaffold(
                deployment_name,
                repo_root=repo_root,
                deployments_root=deployments_root,
                host_port=args.postgres_port,
            )
        raise

    _emit(
        {
            "status": "bootstrap_complete",
            "deployment_name": deployment_name,
            # Issue #2210: this payload goes to the operator's terminal and
            # into `okg install`'s captured child output. It used to print
            # the DSN whole, password included -- the generated one before,
            # and now the operator's own.
            "postgres_dsn": (
                _redacted_dsn(postgres_dsn) if postgres_dsn else postgres_dsn
            ),
            # Issue #2331: the same first step the final report names.
            "next_steps": [
                add_source_next_step(deployment_name),
                (
                    "or open your registered agent and say: "
                    f"set up my OKG for {deployment_name}"
                ),
                f"okg doctor --deployment {deployment_name}",
            ],
        },
        as_json=as_json,
    )
    return 0


def _record_docker_project_after_failed_scaffold(
    deployment_name: str,
    *,
    repo_root: Optional[Path],
    deployments_root: Optional[Path],
    host_port: Optional[int],
) -> Optional[str]:
    """Stamp the `docker:` block on a deployment whose scaffold failed.

    Issue #3414 review H2. `compose up` has already started the
    `okg-<name>` container and volume by the time the scaffold runs, but
    the block naming them used to be written only after a successful
    scaffold. A scaffold that wrote deployment.yaml and then failed (its
    first publish, say) left a manifest with no block, so `okg reset` read
    it as a database the operator owns, reported `not_touched` and left
    the container; a reinstall then met the old volume's password. The
    block cannot be written before the scaffold, which refuses a non-empty
    deployment directory, so it is written here, on the failure path.

    With no deployment.yaml, reset already falls back to the installer's
    default `okg-<name>` project. Returns a line for the failure message,
    or None when nothing was written.
    """
    try:
        dep_dir = _resolve_existing_dep_dir(
            deployment_name, repo_root=repo_root,
            deployments_root=deployments_root,
        )
    except FileNotFoundError:
        return None
    if not (dep_dir / "deployment.yaml").is_file():
        return None
    try:
        _write_deployment_docker_block(
            dep_dir,
            compose_path=_find_compose_yaml(),
            project_name=f"okg-{deployment_name}",
            host_port=host_port,
        )
    except OSError as exc:
        return (
            f"warning: could not record Docker project okg-{deployment_name} "
            f"in {dep_dir / 'deployment.yaml'}: {exc}; `okg reset` will not "
            f"remove it -- run `docker compose -p okg-{deployment_name} "
            f"down -v` yourself"
        )
    return (
        f"recorded Docker project okg-{deployment_name} in "
        f"{dep_dir / 'deployment.yaml'}; `okg reset --deployment "
        f"{deployment_name}` removes it with the deployment"
    )


def _provision_postgres_and_scaffold_reset(
    *,
    deployment_name: str,
    repo_root: Optional[Path],
    as_json: bool,
    yes: bool = False,
    deployments_root: Path | None = None,
) -> int:
    """`okg reset` internal: remove the deployment directory and, when
    this profile's installer provisioned the deployment's Postgres in
    Docker, that project and its volume. Confirms before destruction
    unless `yes=True` (the `--yes` flag is the scripted / CI path).
    """
    # Per `separate-framework-and-deployments` (2026-05-22):
    # try the user-state location first, then fall back to the
    # legacy in-repo location. The multi-root resolver in
    # `find_deployment_dir` handles both transparently — but
    # we also need to support the case where reset runs against
    # an in-repo deployment from before the migration.
    # Issue #2127 F9. This was the one remaining default-root read in the
    # install path: `--deployments-root` / `OKG_DEPLOYMENTS_DIR` reached the
    # install and the plan, but `okg reset` resolved through the default
    # cascade and so reported "not found" for a deployment the operator had
    # just installed into a named root.
    dep_dir = _find_install_deployment_dir(
        deployment_name, deployments_root=deployments_root,
    )
    if dep_dir is None:
        # Last-ditch: legacy in-repo path (pre-migration).
        dep_dir = _legacy_in_repo_deployment_dir(repo_root, deployment_name)
    if dep_dir is None or not dep_dir.is_dir():
        return _fail(
            f"deployment {deployment_name!r} not found in any "
            f"known location (user-state, in-repo, or "
            f"OKG_DEPLOYMENTS_DIR override)",
            as_json=as_json,
        )

    # Issue #3414: `okg reset` serves every installed deployment, not only
    # this profile's. Only a Postgres this profile's installer provisioned
    # in Docker is torn down, as named by the `docker:` block it stamps.
    # Any other deployment -- the minimal starter, workspace-graph, an
    # install given `--postgres-dsn` -- uses a database the operator owns;
    # reset leaves it and every graph version in it alone, and says so.
    docker_project = _reset_docker_project(dep_dir, deployment_name)
    database_note = (
        None if docker_project is not None else (
            "the deployment's PostgreSQL database is not touched; its "
            "published graph versions stay there until you drop it"
        )
    )
    lines = [f"About to destroy:\n  - deployment dir:  {dep_dir}"]
    if docker_project is not None:
        lines.append(
            f"  - docker volume:   {docker_project['project_name']}_postgres-data"
        )
        lines.append(f"  - docker project:  {docker_project['project_name']}")
    else:
        lines.append(f"Not touched: {database_note}.")
    print("\n".join(lines), file=sys.stderr)
    if not yes:
        confirm = input("Type 'destroy' to confirm: ")
        if confirm.strip().lower() != "destroy":
            print("aborted", file=sys.stderr)
            return 1

    # Tear down docker project (volume removal via -v). MUST pass
    # DEPLOYMENT_NAME in env so the compose file's `${DEPLOYMENT_NAME}`
    # interpolation produces the correct volume name. Without it,
    # `docker compose down -v` tries to remove `okg-_postgres-data`
    # (empty deployment slug) and leaves the real volume stranded.
    docker_teardown, teardown_detail = _reset_teardown_docker(
        docker_project, deployment_name,
    )
    if docker_teardown == "failed":
        # Keep the deployment directory: its `docker:` block is the only
        # record of which project still holds the database, and a retry of
        # `okg reset` needs it. Removing it here would turn the next reset
        # into `database: not_touched` and strand the volume.
        _emit(
            {
                "status": "reset_failed",
                "deployment_name": deployment_name,
                "docker_project": docker_project["project_name"],
                "docker_teardown": "failed",
                "docker_teardown_detail": teardown_detail,
                "deployment_dir_kept": str(dep_dir),
                "fix_hint": (
                    "fix Docker (is the daemon running? is `docker` on PATH?), "
                    "then re-run `okg reset`"
                ),
            },
            as_json=as_json,
        )
        return 1
    # Remove deployment dir.
    shutil.rmtree(dep_dir)

    # Per `dead-simple-install`: also unregister from every
    # agent runtime so the operator's Claude Desktop / codex
    # / Cursor configs don't keep stale entries pointing at a
    # destroyed deployment.
    from okg.substrate.library.agent_runtimes.registry import (
        unregister_from_all,
    )
    removed_runtimes = unregister_from_all(deployment=deployment_name)

    _emit(
        {
            "status": "reset_complete",
            "deployment_name": deployment_name,
            "unregistered_runtimes": removed_runtimes,
            "docker_project_removed": (
                docker_project["project_name"] if docker_project else None
            ),
            "docker_project_basis": (
                docker_project["basis"] if docker_project else None
            ),
            "docker_teardown": docker_teardown,
            **(
                {"docker_teardown_detail": teardown_detail}
                if teardown_detail else {}
            ),
            "database": {
                "removed": "removed_with_docker_project",
                "skipped_no_docker": "not_removed_docker_missing",
                "not_applicable": "not_touched",
            }[docker_teardown],
            **({"database_note": database_note} if database_note else {}),
        },
        as_json=as_json,
    )
    return 0


def _reset_teardown_docker(
    docker_project: Optional[dict[str, str]], deployment_name: str,
) -> tuple[str, Optional[str]]:
    """Run `docker compose down -v` for the project reset removes.

    Returns `(outcome, detail)`. The outcome is `not_applicable` (no
    installer-owned project), `removed`, `skipped_no_docker` or `failed`.
    A project the installer recorded in deployment.yaml provably existed,
    so a missing `docker` there is a failure; the guessed `okg-<name>`
    project of a half-written install may never have been created, so a
    missing `docker` there is only skipped -- and reported, not hidden.
    """
    if docker_project is None:
        return "not_applicable", None
    recorded = docker_project["basis"] == "deployment.yaml docker block"
    if shutil.which("docker") is None:
        detail = "`docker` is not on PATH, so the Docker project was not removed"
        return ("failed" if recorded else "skipped_no_docker"), detail
    # MUST pass DEPLOYMENT_NAME in env so the compose file's
    # `${DEPLOYMENT_NAME}` interpolation produces the correct volume name.
    # Without it, `docker compose down -v` tries to remove
    # `okg-_postgres-data` (empty deployment slug) and leaves the real
    # volume stranded.
    env = os.environ.copy()
    env["DEPLOYMENT_NAME"] = deployment_name
    cmd = [
        "docker", "compose", "-f", docker_project["compose_path"],
        "-p", docker_project["project_name"], "down", "-v",
    ]
    try:
        result = subprocess.run(
            cmd, check=False, env=env, capture_output=True, text=True,
        )
    except OSError as exc:
        return "failed", f"{' '.join(cmd)} could not run: {exc}"
    if result.returncode != 0:
        return "failed", (
            f"{' '.join(cmd)} exited {result.returncode}: "
            f"{_redacted_text(command_output_tail(result, limit=500))}"
        )
    return "removed", None


def _reset_docker_project(
    dep_dir: Path, deployment_name: str,
) -> Optional[dict[str, str]]:
    """The Docker Postgres project `okg reset` tears down, or None.

    Read from the `docker:` block this profile's installer stamps
    (`_write_deployment_docker_block`) whenever it starts Postgres in
    Docker. A deployment without that block -- any other profile, or an
    academic-researcher install given `--postgres-dsn` -- has no
    installer-owned database. A directory without a deployment.yaml is a
    half-written install: only the installer's default `okg-<name>`
    project can belong to it.
    """
    import yaml

    manifest_path = dep_dir / "deployment.yaml"
    if not manifest_path.is_file():
        # A half-written install (the scaffold failed before writing its
        # manifest) is a common reason to reset. Nothing says which database
        # it used, so reset removes what only this profile's installer
        # creates: the `okg-<name>` project, a no-op when it does not exist.
        return {
            "project_name": f"okg-{deployment_name}",
            "compose_path": str(_find_compose_yaml()),
            "basis": "no deployment.yaml; the installer's default project name",
        }
    try:
        raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise SubstrateError(
            f"cannot read {manifest_path} to decide what `okg reset` removes: {exc}",
            fix_hint="fix or restore deployment.yaml, then re-run okg reset",
        ) from exc
    docker = raw.get("docker") if isinstance(raw, dict) else None
    if isinstance(docker, dict) and docker.get("project_name"):
        # The recorded compose file can be gone: an install from a since-
        # removed checkout or an older wheel. `down -v` addressed by project
        # name only needs a parseable compose file with the same volume
        # declaration, which the shipped one is.
        recorded_compose = docker.get("compose_path")
        compose_path = (
            Path(str(recorded_compose))
            if recorded_compose and Path(str(recorded_compose)).is_file()
            else _find_compose_yaml()
        )
        return {
            "project_name": str(docker["project_name"]),
            "compose_path": str(compose_path),
            "basis": "deployment.yaml docker block",
        }
    return None


# ---------- install (one-command wrapper) ----------


def _cmd_install(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg install` — one-command academic-researcher bootstrap.

    Wraps the Postgres-provision + scaffold helper, then registers MCP
    entries with detected agent runtimes unless `--no-claude-config`.
    Operator data sources are intentionally not ingested here; the
    conversational setup wizard is the first source-approval boundary.

    Default `--name` is a random scientist/philosopher slug on
    first install. No-name reruns reuse a single existing user-state
    academic-researcher deployment. Postgres uses a Docker-assigned host
    port; MCP uses the lowest free local port at or above the
    default.

    `--dry-run` short-circuits to a read-only plan emitter — no
    Docker build, no container start, no file writes — see
    `_dry_run_install`.
    """
    repo_root = _find_repo_root()
    # Issue #2127 F9: resolved ONCE, here, and threaded through every path
    # below. Reading `--deployments-root` in some paths and
    # `user_state_deployments_root()` in others is what made the dry-run plan
    # report a target the real install did not use.
    try:
        deployments_root = _install_deployments_root(args)
    except DeploymentResolutionError as e:
        # `OKG_DEPLOYMENTS_DIR` pointing at a directory that does not exist
        # used to escape as a traceback from deep inside the deployment
        # resolver -- even under `--json`, which the external-deployments
        # runbook tells operators to parse. Refuse in the CLI envelope with a
        # code instead.
        return _fail_typed(
            _deployments_root_unresolved_error(e), as_json=as_json,
        )
    # Per `dead-simple-install`: --orcid is optional. Per
    # `fix-academic-pi-install-e2e`, no-name reruns first inspect
    # user-state academic-researcher deployments so `./install.sh` can
    # converge on the one deployment it created instead of picking
    # a fresh random slug. Resolution chain: explicit --name >
    # exactly-one existing user-state academic-researcher deployment >
    # ORCID-derived (when --orcid is set) > randomly picked from
    # the scientist roster.
    #
    # Rationale for random over hostname: a hostname-derived slug
    # (e.g. a laptop hostname) is ugly and exposes the
    # operator's machine name in the MCP server key. A randomly-
    # picked scientist last name (e.g. `einstein`) is short,
    # memorable, identity-neutral, and ships ~50 distinct
    # options.
    #
    # Issue #2608: `okg install --deployment-name <n>` is the spelling the
    # general installer and `docs/operate/install.md` use for every profile.
    # This installer reads `--name`, so `--deployment-name` used to be
    # dropped without a word and a random roster name used instead. Treat it
    # as `--name`; refuse when the two disagree rather than pick one.
    general_name = getattr(args, "deployment_name", None)
    if general_name:
        if args.name and args.name != general_name:
            # Same shape as the router's `--profile`/`--template` conflict:
            # a coded envelope with the two values and the fix.
            return _fail_typed(
                SubstrateError(
                    "--name and --deployment-name name different deployments",
                    data={
                        "name": args.name,
                        "deployment_name": general_name,
                    },
                    fix_hint=(
                        "pass one: `--deployment-name <name>` (or `--name`, "
                        "the same flag for this profile)"
                    ),
                ),
                as_json=as_json,
            )
        args.name = general_name
    try:
        existing_user_state_academic_researcher = (
            [] if args.name
            else _user_state_academic_researcher_deployments(
                deployments_root=deployments_root,
            )
        )
    except SubstrateError as e:
        return _fail_typed(e, as_json=as_json)
    if args.name:
        deployment_name = args.name
    elif len(existing_user_state_academic_researcher) == 1:
        deployment_name = existing_user_state_academic_researcher[0].name
        print(
            f"Found existing academic-researcher deployment "
            f"{deployment_name!r}; refreshing agent-runtime "
            f"registrations.\n"
            f"   To create another deployment: pass --name "
            f"\"...\" explicitly.",
            file=sys.stderr,
        )
    elif len(existing_user_state_academic_researcher) > 1:
        names = ", ".join(
            sorted(dep.name for dep in existing_user_state_academic_researcher)
        )
        return _fail(
            f"multiple academic-researcher deployments exist in user state: "
            f"{names}. Pass --name <deployment> to choose one.",
            as_json=as_json,
        )
    elif args.orcid:
        deployment_name = _derive_name_from_orcid(args.orcid)
    else:
        deployment_name = _derive_name_from_random()
        print(
            f"Using --name {deployment_name!r} (randomly picked "
            f"from the scientist roster).\n"
            f"   To override: pass --name \"...\" explicitly.\n"
            f"   To refresh an existing deployment: pass --name "
            f"<that-deployment>.",
            file=sys.stderr,
        )

    # Per `dead-simple-install`: smart defaults derived from
    # the OS environment are observable — we print the derived
    # value back to stderr so the operator knows where it came
    # from and how to override.
    if not getattr(args, "pi_name", None):
        derived = _derive_pi_name_from_git()
        if derived:
            args.pi_name = derived
            print(
                f"Using --pi-name {derived!r} from "
                f"`git config --global user.name`.\n"
                f"   To override: pass --pi-name \"...\" "
                f"explicitly.",
                file=sys.stderr,
            )

    # Per `dead-simple-install` Phase 6: install is idempotent.
    # When the deployment directory already exists, short-circuit
    # to a re-registration of the agent runtimes (the
    # `--claude-config-only` semantics) instead of erroring out
    # of `okg init`. This means rerunning `okg install --orcid X
    # --name Y` after a successful install is a clean no-op +
    # MCP-refresh. To rebuild from scratch, use `okg reset`
    # followed by `okg install`.
    #
    # Per `separate-framework-and-deployments` (2026-05-22): use
    # the multi-root cascade so we detect existing deployments
    # in either user-state OR the legacy in-repo location.
    dep_dir_existing = _find_install_deployment_dir(
        deployment_name, deployments_root=deployments_root,
    )
    if dep_dir_existing is None:
        # Fall back to legacy in-repo check during migration.
        dep_dir_existing = _legacy_in_repo_deployment_dir(
            repo_root, deployment_name,
        )
    if dep_dir_existing is not None:
        try:
            _refuse_retired_profile_record(dep_dir_existing)
        except SubstrateError as e:
            return _fail_typed(e, as_json=as_json)
    if (
        dep_dir_existing is not None
        and not getattr(args, "claude_config_only", False)
        and not getattr(args, "dry_run", False)
    ):
        print(
            f"Deployment {deployment_name!r} already exists; "
            f"refreshing agent-runtime registrations. To rebuild "
            f"from scratch, run `okg reset --deployment "
            f"{deployment_name} --yes` first.",
            file=sys.stderr,
        )
        args.claude_config_only = True

    # Per `dead-simple-install`: don't probe-and-claim a host
    # port here — that's a TOCTOU race. Pass `None` through to
    # the bootstrap, which sets `POSTGRES_PORT=0` in the compose
    # env and reads the Docker-assigned port back via
    # `docker compose port`. The MCP port stays probed-locally
    # because it's not a Docker resource.
    postgres_port = args.postgres_port  # may be None
    mcp_port = (
        args.mcp_port if args.mcp_port is not None
        else _pick_free_port(starting_at=5431)
    )

    resolved_git_repos = _resolve_git_repos(
        explicit=getattr(args, "git_repos", None),
    )

    # Issue #2210. Resolved ONCE, here, and threaded through every path
    # below, the way `deployments_root` is. When it is set the install
    # provisions nothing: no Docker network, volume or container, no
    # `docker:` block, and the operator's DSN is what the deployment and
    # the agent-runtime registration carry.
    operator_postgres_dsn = _operator_postgres_dsn(args)

    # Issue #2070. Two source-default files that declare one source id are
    # refused here, before the dry-run plan and before Docker Postgres is
    # provisioned, with the same check and typed error as the scaffold.
    # The registration refresh below renders no sources, so a real refresh
    # is exempt. A refresh DRY RUN still takes the dry-run branch, which
    # lists the sources, so it is checked like every other dry run.
    if not (
        getattr(args, "claude_config_only", False)
        and not getattr(args, "dry_run", False)
    ):
        try:
            _load_source_defaults(repo_root)
        except SubstrateError as e:
            return _fail_typed(e, as_json=as_json)

    if getattr(args, "dry_run", False):
        return _dry_run_install(
            args=args,
            deployment_name=deployment_name,
            postgres_port=postgres_port,
            mcp_port=mcp_port,
            resolved_git_repos=resolved_git_repos,
            repo_root=repo_root,
            deployments_root=deployments_root,
            operator_postgres_dsn=operator_postgres_dsn,
            as_json=as_json,
        )

    # Short-circuit: `--claude-config-only` refreshes agent-runtime
    # MCP registration for an existing deployment. No bootstrap, no
    # publish. The flag name is retained for back-compat with the
    # old Claude-only path.
    if getattr(args, "claude_config_only", False):
        repo_root = _find_repo_root()
        # Per `separate-framework-and-deployments`: resolve via
        # multi-root cascade so we find user-state deployments
        # AND legacy in-repo ones.
        dep_dir = _find_install_deployment_dir(
            deployment_name, deployments_root=deployments_root,
        )
        if dep_dir is None:
            dep_dir = _legacy_in_repo_deployment_dir(
                repo_root, deployment_name,
            )
        if dep_dir is None or not dep_dir.is_dir():
            return _fail(
                f"deployment not found: {dep_dir}. Run `okg install "
                f"--profile academic-researcher --orcid <id> --name "
                f"{deployment_name}` first.",
                as_json=as_json,
            )
        # Per `fix-okg-install-claude-config`: read the canonical
        # DSN from deployment.yaml's init_answers rather than
        # rebuilding from --postgres-port (which the operator
        # would have to re-specify).
        postgres_dsn = _read_deployment_postgres_dsn(dep_dir)
        if not postgres_dsn:
            return _fail(
                f"could not read postgres_dsn from "
                f"{dep_dir}/deployment.yaml. The file may be "
                f"missing or malformed; re-run `okg install "
                f"--profile academic-researcher --orcid <id> --name "
                f"{deployment_name}` to "
                f"re-scaffold.",
                as_json=as_json,
            )
        # Per `dead-simple-install`: re-register with every
        # detected runtime, not just Claude Desktop. The verb
        # name `--claude-config-only` is preserved for
        # back-compat; semantics are broader.
        from okg.substrate.library.agent_runtimes.registry import (
            register_with_all_detected,
        )
        # academic-researcher setup is agent-driven through the MCP write tools
        # (propose_setup / commit_setup / add_source). Launch the
        # registered server write-enabled so "set up my OKG" works; reads
        # still run as the least-privilege okg_mcp role and writes as a
        # separate app_rw pool (PACT least-privilege-mcp). The operator's
        # install IS the write-mode enablement; the runtime registration
        # translates this capability into launcher/MCP command shape without
        # persisting raw DSNs or OKG_MCP_ALLOW_WRITES in agent config env.
        register_report = register_with_all_detected(
            deployment=deployment_name, dsn=postgres_dsn,
            extra_env=_agent_runtime_registration_extra_env(),
        )
        _emit(
            {
                "status": "agent_runtimes_re_registered",
                "deployment_name": deployment_name,
                "agent_runtimes": [
                    {
                        "runtime": r.runtime,
                        "status": r.status,
                        "config_path": (
                            str(r.config_path) if r.config_path else None
                        ),
                        "error": r.error,
                    }
                    for r in register_report.results
                ],
                "primary_agent": register_report.primary_agent(),
            },
            as_json=as_json,
        )
        return 0 if not register_report.failed else 1

    # 1. Bootstrap the deployment. `okg init` still migrates,
    # composes the catalog, and may publish an empty generation so
    # doctor/read-side tooling has a generation to inspect. It does
    # not run sources; source ingestion begins only after the
    # conversational setup wizard is approved.
    # Issue #2210. Prove the operator's database answers BEFORE anything
    # is written. The scaffold child process, the migration and the first
    # publish all assume the database is there; refusing here costs one
    # connection, while refusing later leaves a half-written deployment
    # pointing at a database that never answered.
    if operator_postgres_dsn is not None:
        # The documented answer is the placeholder `${OKG_DSN}`, which the
        # default profile expands with this same helper (issue #1948). The
        # probe needs the expanded value; the scaffold child still gets the
        # literal text, expands it itself, and writes the placeholder, not
        # the password, into deployment.yaml. An unset variable refuses here.
        try:
            _require_reachable_postgres(_expanded_dsn(operator_postgres_dsn))
        except SubstrateError as e:
            return _fail_typed(e, as_json=as_json)

    bootstrap_args = argparse.Namespace(
        deployment_name=deployment_name,
        orcid=args.orcid,
        postgres_dsn=operator_postgres_dsn,
        postgres_port=postgres_port,
        use_local_postgres=operator_postgres_dsn is not None,
        mcp_port=mcp_port,
        papers_dir=None,
        cv_path=None,
        group_webpage=None,
        pi_name=getattr(args, "pi_name", None),
        dblp_pid=getattr(args, "dblp_pid", None),
        git_repos=resolved_git_repos,
        reset=False,
        yes=True,
        force=False,
        # Issue #2127 F9: the resolved install root, forwarded to the
        # scaffold child process as `--deployments-root`.
        deployments_root_resolved=deployments_root,
        # Suppress the deprecation warning when called internally.
        _invoked_from_install=True,
    )
    if _provision_postgres_and_scaffold(
        bootstrap_args, as_json=as_json,
    ) != 0:
        return 1
    # Per `dead-simple-install`: when --postgres-port was
    # omitted, the bootstrap reads the Docker-assigned host
    # port and writes it back onto bootstrap_args. Hoist it to
    # outer `postgres_port` so the DSN building + agent-runtime
    # registration use the real value.
    postgres_port = bootstrap_args.postgres_port

    # Per `dead-simple-install`: stamp deployment.yaml with the
    # `docker:` block so lifecycle verbs (okg start/stop/restart)
    # can locate the compose stack.
    dep_dir = _resolve_existing_dep_dir(
        deployment_name, repo_root=repo_root,
        deployments_root=deployments_root,
    )
    # Issue #2210: with an operator-supplied DSN there is no compose
    # stack, so a `docker:` block would point `okg start/stop/restart` at
    # a project that does not exist. A deployment without the block is
    # already the supported shape (see `_write_deployment_docker_block`).
    if operator_postgres_dsn is None:
        try:
            _write_deployment_docker_block(
                dep_dir,
                compose_path=_find_compose_yaml(),
                project_name=f"okg-{deployment_name}",
                host_port=postgres_port,
            )
        except OSError as e:
            print(
                f"warning: failed to write `docker:` block to "
                f"{dep_dir}/deployment.yaml: {e}",
                file=sys.stderr,
            )

    # 2. Agent-runtime MCP registration.
    #
    # Per `dead-simple-install`: this used to write only Claude
    # Desktop. The new registry registers with every detected
    # runtime (Claude Desktop, codex CLI, Cursor). Each runtime's
    # outcome is captured in `register_report` and surfaced in
    # the install payload.
    from okg.substrate.library.agent_runtimes.registry import (
        register_with_all_detected,
    )
    register_report = None
    if not args.no_claude_config:
        # Issue #2210: the operator's DSN is the deployment's database, so
        # it is what the registered MCP server must be handed. The
        # Docker-path branch below is unchanged.
        dsn = operator_postgres_dsn or (
            f"postgres://postgres:okg@localhost:{postgres_port}/"
            f"okg_{deployment_name}"
        )
        try:
            # Write-enable the registered MCP server: academic-researcher setup is
            # agent-driven via the MCP write tools (PACT
            # least-privilege-mcp). Reads still run as okg_mcp; writes use a
            # separate app_rw pool. The registration translates the flag into
            # launcher/MCP command shape instead of persisting it as env.
            register_report = register_with_all_detected(
                deployment=deployment_name, dsn=dsn,
                extra_env=_agent_runtime_registration_extra_env(),
            )
        except Exception as e:  # noqa: BLE001
            print(
                f"warning: agent-runtime registration failed: {e}. "
                f"Run `okg install --profile academic-researcher "
                f"--claude-config-only --name {deployment_name}` to retry.",
                file=sys.stderr,
            )

    # Back-compat scalar for the `claude_desktop_configured`
    # payload field (consumers may read it). True iff Claude
    # Desktop specifically was registered.
    claude_desktop_configured = bool(
        register_report
        and any(
            r.runtime == "claude-desktop" and r.status == "registered"
            for r in register_report.results
        )
    )

    # 3. Wizard-first source boundary. Load the registry for reporting,
    # but do NOT ingest any source here. The first operator-data row
    # delta must come from MCP propose_setup -> operator approval ->
    # commit_setup.
    dep_dir = _resolve_existing_dep_dir(
        deployment_name, repo_root=repo_root,
        deployments_root=deployments_root,
    )
    try:
        from okg.substrate.sources.registry import (
            load_source_registry,
        )
        registry = load_source_registry(
            dep_dir / "source_registry.yaml",
        )
    except Exception as e:  # noqa: BLE001
        return _fail(
            f"failed to load source_registry.yaml: {e}",
            as_json=as_json,
        )
    enabled = sorted(
        name for name, entry in registry.items()
        if entry.enabled_by_default
    )
    ingest_outcome = {
        "ran": False,
        "rc": None,
        "reason": "wizard_first_setup_requires_operator_approval",
    }
    publish_outcome = {
        "ran": False,
        "rc": None,
        "reason": "no_operator_data_ingested_by_install",
    }

    chronos_cleanup = {"ran": False, "status": "not_needed"}
    if ingest_outcome.get("rc") in (None, 0) and publish_outcome.get("rc") in (None, 0):
        chronos_cleanup = _retire_legacy_storage_and_migrate(
            dep_dir, deployment_name=deployment_name,
        )
        if chronos_cleanup["status"] == "failed":
            publish_outcome = {
                **publish_outcome,
                "cleanup_error": chronos_cleanup["error"],
                "rc": publish_outcome.get("rc") or 1,
            }

    # 3.5. Optional: start the DBOS runtime worker so
    # `search method=semantic` can populate embeddings after install.
    # OFF by default — the operator chooses an embedding backend rather
    # than the install path silently picking one. Opt in with the
    # legacy-named `--embed-worker` flag.
    embed_worker = _maybe_start_embed_worker(
        deployment_name=deployment_name,
        repo_root=repo_root,
        enabled=bool(getattr(args, "embed_worker", False)),
        deployments_root=deployments_root,
    )

    # 4. Final report.
    aggregates = _try_aggregate_counts(
        deployment_name, deployments_root=deployments_root,
    ) or {}
    # Issue #2331: every academic-researcher source waits for setup approval, so a
    # fresh install publishes an empty graph. Name both ways to fill it: the
    # exact command that adds a first source, and the agent setup proposal.
    add_source_command = add_source_next_step(deployment_name)
    setup_message = (
        "No data sources have been ingested yet. Add a first source with "
        f"`{add_source_command}`, or open the registered agent and approve "
        "the setup proposal to populate this OKG."
    )
    # The cleanup-and-migrate step is the only database work install itself
    # does; when it fails the deployment cannot publish, so the install
    # failed and says so in its status and exit code.
    install_failed = chronos_cleanup.get("status") == "failed"
    payload = {
        "status": "install_failed" if install_failed else "install_complete",
        **(
            {
                "failed_stage": "chronos_native_graph_cleanup",
                "error": chronos_cleanup.get("error"),
            }
            if install_failed else {}
        ),
        "deployment_name": deployment_name,
        # Issue #2127 F9: where the deployment actually landed. A caller that
        # passed `--deployments-root` needs this to reach the deployment
        # again, and it is the field that would have caught the flag being
        # ignored.
        "deployment_dir": str(dep_dir),
        "postgres_port": (
            None if operator_postgres_dsn is not None else postgres_port
        ),
        # Issue #2210: the DSN the deployment actually carries, with the
        # password masked by the substrate's own redactor. This is the
        # field that makes an ignored --postgres-dsn observable.
        "postgres_dsn": (
            _redacted_dsn(operator_postgres_dsn)
            if operator_postgres_dsn is not None else None
        ),
        "postgres_provisioning": (
            "operator_supplied_dsn" if operator_postgres_dsn is not None
            else "docker"
        ),
        "mcp_port": mcp_port,
        "resolved_git_repos": resolved_git_repos,
        "first_generation_node_counts": dict(
            sorted(
                aggregates.items(),
                key=lambda kv: -kv[1],
            )[:10]
        ),
        "first_publish_filtered_ingest": ingest_outcome,
        "first_publish_run_once": publish_outcome,
        "chronos_native_graph_cleanup": chronos_cleanup,
        "enabled_sources": enabled,
        "data_sources_ingested": False,
        "setup_status": SETUP_STATUS_AWAITING_APPROVAL,
        "setup_message": setup_message,
        "next_steps": (
            # A deployment outside the default root is reachable by name only
            # once the resolver is told where to look, so the export comes
            # FIRST -- the same order `okg install --scaffold-only` uses.
            ([
                f"export OKG_DEPLOYMENTS_DIR={shlex.quote(str(dep_dir.parent))}"
            ] if deployments_root is not None else [])
            + [
                add_source_command,
                (
                    "or open your registered agent and say: "
                    f"set up my OKG for {deployment_name}"
                ),
                f"okg doctor --deployment {deployment_name}",
            ]
        ),
        "claude_desktop_configured": claude_desktop_configured,
        "agent_runtimes": (
            [
                {
                    "runtime": r.runtime,
                    "status": r.status,
                    "config_path": (
                        str(r.config_path) if r.config_path else None
                    ),
                    "error": r.error,
                }
                for r in register_report.results
            ]
            if register_report else []
        ),
        "primary_agent": (
            register_report.primary_agent() if register_report else None
        ),
        "embed_worker": embed_worker,
        "conversational_setup": (
            _build_handoff_line(
                deployment_name=deployment_name,
                register_report=register_report,
            )
        ),
    }
    _emit(payload, as_json=as_json)
    if install_failed:
        print(
            "error: install_failed: migrating the deployment database "
            f"failed: {chronos_cleanup.get('error')}",
            file=sys.stderr,
        )
        return 1
    # Per `dead-simple-install` Phase 5: also print the
    # handoff as a single line to stderr so the operator sees
    # it even when the JSON payload goes to a script.
    print(
        setup_message + "\n" +
        _build_handoff_line(
            deployment_name=deployment_name,
            register_report=register_report,
        ),
        file=sys.stderr,
    )
    return 0


def _retire_legacy_storage_and_migrate(
    dep_dir: Path, *, deployment_name: str,
) -> dict:
    """Retire legacy graph storage and migrate the deployment database.

    The only database work install does itself. Returns the
    ``chronos_native_graph_cleanup`` report; ``status`` is ``failed`` with
    the error when any step raises.
    """
    try:
        from okg.substrate.db.connection import connect as db_connect
        from okg.substrate.db.management import (
            legacy_graph_storage_retirement_plan,
            retire_legacy_graph_storage,
        )
        from okg.substrate.db.migrate import migrate

        dsn = _read_deployment_postgres_dsn(dep_dir)
        if not dsn:
            raise RuntimeError("deployment.yaml has no postgres DSN")
        dsn = _expanded_dsn(dsn)
        with db_connect(dsn) as conn:
            retirement_plan = legacy_graph_storage_retirement_plan(conn)
            retire_legacy_graph_storage(conn)
            conn.commit()
        migrate(dsn, deployment_name=deployment_name)
    except Exception as e:  # noqa: BLE001 - reported as the install's failed stage.
        return {"ran": True, "status": "failed", "error": str(e)}
    return {"ran": True, "status": "ok", "retirement_plan": retirement_plan}


def _maybe_start_embed_worker(
    *,
    deployment_name: str,
    repo_root: Optional[Path],
    enabled: bool,
    deployments_root: Path | None = None,
) -> dict:
    """Background-spawn the DBOS runtime worker so semantic search works
    immediately after install.

    Returns a status dict for the final install report:
      - `{started: False, reason: "not_requested"}` when
        `--embed-worker` is NOT passed (the default)
      - `{started: True, pid: <int>, logfile: <path>}` on success
      - `{started: False, reason: "spawn_failed", error: <str>}` on
        spawn failure (does NOT fail the install; the operator can
        still start `okg runtime worker` manually).
      - `{started: False, reason: "deployment_not_found", error: <str>}`
        when the deployment cannot be resolved under the install's
        deployments root.

    The deployment's canonical `embeddings.backend` setting selects the embedder.
    This repository ships no supported 2560-dimensional production backend
    recipe. ``hash`` is deterministic test/development support; any other
    backend must be supported by the parser and pass capability preflight
    before the runtime worker starts.
    """
    if not enabled:
        return {"started": False, "reason": "not_requested"}
    # Issue #2338: resolve with the operator's `--deployments-root`, the
    # root the rest of the install used. Without it a deployment under a
    # custom root was not found and the FileNotFoundError escaped the
    # install as a traceback.
    try:
        dep_dir = _resolve_existing_dep_dir(
            deployment_name, repo_root=repo_root,
            deployments_root=deployments_root,
        )
    except FileNotFoundError as e:
        return {
            "started": False,
            "reason": "deployment_not_found",
            "error": str(e),
        }
    # The worker resolves `--deployment <name>` itself, so it must look
    # under the same root.
    child_env = dict(os.environ)
    if deployments_root is not None:
        child_env["OKG_DEPLOYMENTS_DIR"] = str(dep_dir.parent)
    log_dir = dep_dir / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return {
            "started": False,
            "reason": "log_dir_unwritable",
            "error": str(e),
        }
    logfile = log_dir / "runtime-worker.log"
    try:
        # Detach from this process so the worker survives the
        # install command's exit. stdin closed; stdout/stderr to
        # the deployment-scoped logfile.
        with open(logfile, "ab", buffering=0) as f:
            proc = subprocess.Popen(
                [
                    *running_okg_argv(),
                    "runtime", "worker",
                    "--deployment", deployment_name,
                ],
                cwd=repo_root,
                env=child_env,
                stdin=subprocess.DEVNULL,
                stdout=f, stderr=f,
                start_new_session=True,
            )
    except OSError as e:
        return {
            "started": False,
            "reason": "spawn_failed",
            "error": str(e),
        }
    return {
        "started": True,
        "pid": proc.pid,
        "logfile": str(logfile),
        "worker": "runtime",
    }


# ---------- dry-run plan emitter ----------


# Connector-backed source IDs. These need a trusted app connector and
# allowlists, so dry-run reports them distinctly from local filesystem /
# URL sources.
_CONNECTOR_SOURCE_IDS = frozenset({
    "gmail_api",
    "google_calendar_api",
    "slack_api",
})


def _scaffold_profile(repo_root: Optional[Path]):
    """The academic-researcher profile the scaffold child will render.

    The child (`okg install --scaffold-only --profile academic-researcher`) runs
    with `cwd=repo_root` inside a checkout, and outside one (`repo_root`
    None, issue #2313) it inherits this process's working directory. It
    uses this process's environment and resolves the profile through
    `discover_profile`, so `OKG_PROFILES_DIR` or a `./profiles/` directory
    can replace the packaged copy. This calls the same resolver from the
    same working directory, so the pre-Docker check reads the files the
    child will render, not the packaged ones.
    """
    import contextlib

    from okg.substrate.deployment_bootstrap.profile_init import (
        discover_profile,
    )

    if repo_root is None:
        # Issue #2331. Outside a checkout `_find_repo_root` returns None
        # (#2313) and the child inherits this process's working directory,
        # so the profile resolves from here as well.
        return discover_profile("academic-researcher")
    with contextlib.chdir(repo_root):
        return discover_profile("academic-researcher")


def _load_source_defaults(repo_root: Optional[Path]) -> dict[str, dict]:
    """Walk the scaffold profile's source-defaults/*.yaml.

    Returns {source_id: parsed_yaml}.

    Reads through `source_default_entries`, the same id rule and the same
    duplicate check the scaffold renders with. Before issue #2070 this
    loader kept its own dict keyed by id, so two files declaring one id
    merged last-wins here while the scaffold child refused them, and the
    refusal arrived only after Docker Postgres had started. A repeated id
    now raises DuplicateSourceIdError from this call. A key the legacy
    source registry does not read raises ProfileError here too, before
    Docker Postgres starts, instead of at the first ingest.
    """
    from okg.substrate.deployment_bootstrap.profile_init import (
        refuse_unknown_source_default_keys,
        source_default_entries,
        source_entries_from_default,
    )

    profile = _scaffold_profile(repo_root)
    directory = profile.source_defaults_dir
    if directory is None or not directory.is_dir():
        # The child renders no sources from a profile without the
        # directory, so there is nothing to list or refuse.
        return {}
    paths = sorted(directory.glob("*.yaml"))
    texts = {path.stem: path.read_text(encoding="utf-8") for path in paths}
    entries = source_default_entries(profile, texts)
    if profile.source_admission_contract is None:
        # Name the file that declares each source, not the directory.
        source_files = {
            str(source_id): path
            for path in paths
            for source_id in source_entries_from_default(
                path.stem, texts[path.stem],
            )
        }
        refuse_unknown_source_default_keys(
            profile,
            entries,
            source_files=source_files,
        )
    return {
        source_id: body
        for source_id, body in entries.items()
        if isinstance(body, dict)
    }


def _docker_state() -> dict:
    """Read-only probe of the Docker daemon + okg-pg17:local image."""
    if shutil.which("docker") is None:
        return {
            "docker_on_path": False,
            "daemon_reachable": False,
            "image_present": False,
            "image_build_needed": True,
        }
    daemon_ok = (
        subprocess.run(
            ["docker", "info"], capture_output=True,
        ).returncode == 0
    )
    image_present = (
        subprocess.run(
            ["docker", "image", "inspect", "okg-pg17:local"],
            capture_output=True,
        ).returncode == 0
    )
    return {
        "docker_on_path": True,
        "daemon_reachable": daemon_ok,
        "image_present": image_present,
        "image_build_needed": not image_present,
    }


def _resolve_git_repos(*, explicit: Optional[str]) -> Optional[str]:
    """Return the value to pass as `--git-repos` to `okg init`.

    Wizard-first install deliberately does NOT infer this from
    `git rev-parse --show-toplevel`: `./install.sh` runs from the OKG
    framework checkout, and treating that checkout as the operator's
    project is the bug this flow avoids. Only an explicit operator value
    is preserved as setup context.
    """
    if explicit:
        return explicit
    return None


def _dry_run_install(
    *,
    args: argparse.Namespace,
    deployment_name: str,
    postgres_port: int,
    mcp_port: int,
    resolved_git_repos: Optional[str],
    repo_root: Optional[Path],
    deployments_root: Optional[Path],
    operator_postgres_dsn: Optional[str],
    as_json: bool,
) -> int:
    """Emit a read-only install plan.

    Probes:
      - Docker daemon + okg-pg17:local image (would a build run?)
      - chosen ports + free-or-not
      - scaffold target dir + collision
      - profile source-defaults + their setup/approval flags
      - agent-runtime config paths + key collisions

    Returns 0 if the plan is executable, 1 if a blocker is present
    (e.g., scaffold dir already exists). NEVER writes files, builds
    images, starts containers, or talks to Postgres.
    """
    blockers: list[str] = []

    # Per `separate-framework-and-deployments`: new scaffolds
    # target the user-state XDG dir, not the in-repo
    # `deployments/`. Detect collisions across BOTH locations
    # (user-state + legacy in-repo) since either is a real
    # conflict.
    # Issue #2127 F9: the plan reports the target the REAL install will use,
    # including the root the operator named. It used to report the user-state
    # default unconditionally while the install wrote somewhere else.
    scaffold_target = _install_scaffold_target(
        deployment_name, deployments_root=deployments_root,
    )
    existing = _find_install_deployment_dir(
        deployment_name, deployments_root=deployments_root,
    ) or (
        _legacy_in_repo_deployment_dir(repo_root, deployment_name)
        if deployments_root is None
        else None
    )
    scaffold_collision = existing is not None
    if scaffold_collision:
        blockers.append(
            f"scaffold target already exists: {scaffold_target} — "
            f"run `okg reset --deployment {deployment_name}` first."
        )

    docker_state = _docker_state()
    # Issue #2210: an unreachable Docker daemon blocks nothing when the
    # operator named their own database -- the real install would start
    # no container. Reporting it as a blocker would refuse the one path
    # that does not need Docker. The dry run opens no database
    # connection: the plan's `postgres_dsn` is the address as named, not
    # a checked one, and the Docker state read here stays in the plan.
    if (
        operator_postgres_dsn is None
        and not docker_state["daemon_reachable"]
    ):
        blockers.append(
            "Docker daemon unreachable (run Docker Desktop / "
            "`colima start`)."
        )

    try:
        source_defaults = _load_source_defaults(repo_root)
    except SubstrateError as e:
        # DuplicateSourceIdError and every other ProfileError are
        # SubstrateErrors: emit the typed envelope, never a traceback.
        return _fail_typed(e, as_json=as_json)
    enabled: list[dict] = []
    disabled: list[dict] = []
    for source_id, body in sorted(source_defaults.items()):
        needs_connector = source_id in _CONNECTOR_SOURCE_IDS
        info = {
            "source_id": source_id,
            "needs_connector": needs_connector,
        }
        if bool(body.get("enabled_by_default", False)):
            enabled.append(info)
        else:
            disabled.append(info)

    # Per `dead-simple-install`: use the agent-runtime registry
    # for Claude Desktop path resolution instead of the deleted
    # `_claude_desktop_config_path` helper.
    from okg.substrate.library.agent_runtimes.claude_desktop import (
        ClaudeDesktopRuntime,
    )
    claude_cfg_path = ClaudeDesktopRuntime().config_path()
    claude_server_key = f"okg-{deployment_name}"
    claude_collision = False
    claude_existing_keys: list[str] = []
    if (
        not args.no_claude_config
        and claude_cfg_path is not None
        and claude_cfg_path.is_file()
    ):
        try:
            existing = json.loads(claude_cfg_path.read_text(encoding="utf-8"))
            servers = (existing or {}).get("mcpServers") or {}
            claude_existing_keys = sorted(servers.keys())
            claude_collision = claude_server_key in servers
        except (json.JSONDecodeError, OSError):
            # A malformed/unreadable Claude config is reported later as non-blocking context.
            pass

    plan = {
        "status": "dry_run_blocked" if blockers else "dry_run_ok",
        # Every dry-run plan carries this, whichever route produced it, so
        # a consumer has one discriminator for "this plan describes the
        # run" vs "this command could not get far enough to describe it".
        # The generic scaffold path sets it too. Per issue #946.
        "plan_complete": True,
        "deployment_name": deployment_name,
        "orcid": args.orcid,
        "scaffold_target": str(scaffold_target),
        # Named so a consumer can tell an operator-chosen root from the
        # user-state default without re-deriving either.
        "deployments_root": str(scaffold_target.parent),
        "deployments_root_source": (
            "explicit" if getattr(args, "deployments_root", None)
            else "environment" if deployments_root is not None
            else "user_state_default"
        ),
        "scaffold_collision": scaffold_collision,
        "ports": {
            "postgres": (
                None if operator_postgres_dsn is not None else postgres_port
            ),
            "mcp": mcp_port,
        },
        # Issue #2210: the plan says WHICH database the install would use.
        # The DSN is redacted -- a plan is printed and piped, and the
        # password the operator typed must not travel with it. A plan
        # opens no connection, so this is the named DSN, not a probed one.
        "postgres_dsn": (
            _redacted_dsn(operator_postgres_dsn)
            if operator_postgres_dsn is not None else None
        ),
        "postgres_provisioning": (
            "operator_supplied_dsn" if operator_postgres_dsn is not None
            else "docker"
        ),
        "resolved_git_repos": resolved_git_repos,
        "docker": docker_state,
        "sources": {
            "total": len(source_defaults),
            "will_run_during_install": [],
            "requires_setup_approval": enabled + disabled,
            "connector_sources_in_either_bucket": sorted(
                s["source_id"]
                for s in enabled + disabled
                if s["needs_connector"]
            ),
        },
        "claude_desktop": {
            "enabled": not args.no_claude_config,
            # Issue #2127 F21: the plan named the operator's Claude Desktop
            # config file and the keys it would touch, but never said the
            # step could be declined -- so the only way to find that out was
            # to read `--help`. The plan now says so where it says what it
            # would edit.
            "optional": True,
            "skip_with": "--no-claude-config",
            "note": (
                "optional: agent-runtime registration writes this file. "
                "Re-run with --no-claude-config to install without "
                "touching any agent configuration; you can register later "
                "with `okg install --profile academic-researcher "
                f"--claude-config-only --name {deployment_name}`."
            ),
            "config_path": (
                str(claude_cfg_path) if claude_cfg_path else None
            ),
            "config_exists": (
                claude_cfg_path.is_file() if claude_cfg_path else False
            ),
            "server_key": claude_server_key,
            "would_overwrite_existing_key": claude_collision,
            "existing_server_keys": claude_existing_keys,
        },
        "blockers": blockers,
        # Issue #2210: the line dropped BOTH flags that decide what gets
        # built -- `--profile`, which selects this installer at all, and
        # `--postgres-dsn`, which decides whether anything is provisioned.
        # Running the printed line therefore did something other than what
        # the plan above described. The DSN is redacted for the same
        # reason it is redacted in the plan body; an operator restores
        # their own password. Issue #2277: the line is built from the
        # parser (#2575), so an unset ORCID is left out instead of printing
        # `--orcid None`, and a flag the parser does not define refuses.
        "next_step": (
            okg_command(
                "install",
                profile="academic-researcher",
                orcid=args.orcid,
                name=deployment_name,
                postgres_dsn=(
                    Masked(
                        operator_postgres_dsn,
                        _redacted_dsn(operator_postgres_dsn),
                    )
                    if operator_postgres_dsn is not None else None
                ),
                deployments_root=(
                    str(scaffold_target.parent)
                    if getattr(args, "deployments_root", None) else None
                ),
            ).display()
            if not blockers else
            "resolve blockers above, then re-run with --dry-run"
        ),
    }
    _emit(plan, as_json=as_json)
    return 0 if not blockers else 1


# ---------- shared install helpers ----------


def _build_handoff_line(
    *, deployment_name: str, register_report,
) -> str:
    """Per `dead-simple-install` Phase 5: the final install
    message is one line of "open <agent> and say <phrase>",
    not a 4-item CLI next_steps list.

    The primary agent is the runtime whose config file was
    most recently modified among those registered (recency =
    "the agent the operator probably uses"). When two or more
    runtimes registered, name the primary + an optional
    `(or <secondary>)` clause.
    """
    if register_report is None:
        # No registry attempt (e.g. --no-claude-config).
        return (
            f"Open your agent of choice (Claude Code, Claude "
            f"Desktop, codex, Cursor) and say: \"set up my OKG "
            f"for {deployment_name}\""
        )
    registered = register_report.registered
    if not registered:
        # The re-run is a config-only refresh; it reuses the
        # deployment's saved answers, so don't print an `--orcid
        # <id>` placeholder the operator has to puzzle over.
        return (
            f"No agent runtimes were detected to register with. "
            f"Install one — on Linux: Claude Code "
            f"(`curl -fsSL https://claude.ai/install.sh | bash`) "
            f"or codex CLI; on macOS also Claude Desktop or "
            f"Cursor — open it once, then re-run `okg install "
            f"--profile academic-researcher --name {deployment_name} "
            f"--claude-config-only`."
        )
    primary = register_report.primary_agent() or registered[0].runtime
    secondary = register_report.secondary_agents()
    if secondary:
        # Show only the first secondary to keep the line short.
        line = (
            f"Open {primary} (or {secondary[0]}) and say: "
            f"\"set up my OKG for {deployment_name}\""
        )
    else:
        line = (
            f"Open {primary} and say: "
            f"\"set up my OKG for {deployment_name}\""
        )
    return line


def _derive_pi_name_from_git() -> Optional[str]:
    """Per `dead-simple-install`: derive `--pi-name` from the
    operator's git config when not explicitly provided.
    Returns None if git isn't available, the value is unset, or
    the value does not look like a full human name.

    Observable derivation — the install logs the source and
    the override instruction so the operator knows where the
    value came from.
    """
    if shutil.which("git") is None:
        return None
    try:
        result = subprocess.run(
            ["git", "config", "--global", "user.name"],
            check=True, capture_output=True, timeout=2,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None
    value = result.stdout.decode("utf-8", errors="replace").strip()
    if not _looks_like_full_person_name(value):
        return None
    return value


def _looks_like_full_person_name(value: str) -> bool:
    """Return True for values usable as public identity lookup names.

    Git's `user.name` is often a handle or first name. Treating
    `Riley` as a scholarly/grant lookup key causes bad public-source
    proposals, so install only auto-derives `pi_name` from values that
    look like at least first + last name.
    """
    tokens = [
        tok.strip(".,")
        for tok in value.split()
        if any(ch.isalpha() for ch in tok)
    ]
    if len(tokens) < 2:
        return False
    return len(tokens[-1]) >= 2


def _derive_name_from_orcid(orcid: Optional[str]) -> str:
    """Derive a deployment slug from the ORCID's last 8 digits."""
    if not orcid:
        return "pi_default"
    digits = "".join(ch for ch in orcid if ch.isdigit())
    if len(digits) < 8:
        return "pi_default"
    return f"pi_{digits[-8:-4]}_{digits[-4:]}"


def _derive_name_from_hostname() -> str:
    """Per `dead-simple-install`: derive a deployment slug from
    the machine hostname. Sanitize: lowercase, dots → hyphens,
    drop `.local` / `.lan` / `.home` suffixes, replace any
    remaining non-[a-z0-9-_] with hyphens.

    Used as a fallback when the random-roster pick somehow
    fails (defensive; the roster is non-empty and
    `random.SystemRandom().choice` doesn't raise).
    """
    import socket
    raw = socket.gethostname() or "okg"
    # Strip mDNS-style suffixes.
    for suffix in (".local", ".lan", ".home"):
        if raw.endswith(suffix):
            raw = raw[: -len(suffix)]
            break
    cleaned = []
    for ch in raw.lower():
        if ch.isalnum() or ch in "-_":
            cleaned.append(ch)
        else:
            cleaned.append("-")
    result = "".join(cleaned).strip("-_") or "okg"
    return result


# Curated last-names roster used by `_derive_name_from_random`.
# Chosen for diversity (scientists across eras, computer
# scientists, mathematicians, philosophers — diversity of
# geography, gender, discipline). 50 names → effectively
# zero collision risk on a single machine with a handful of
# deployments.
#
# Slugified: lowercase, ASCII-only, no hyphens. Each is safe
# as a Docker project name, MCP server key suffix, and
# directory name.
SCIENTIST_ROSTER: tuple[str, ...] = (
    # Physics + cosmology
    "einstein", "curie", "feynman", "bohr", "schrodinger",
    "heisenberg", "hawking", "sagan", "hubble", "oppenheimer",
    # Biology + chemistry
    "darwin", "mendel", "pasteur", "watson", "crick",
    "franklin", "mendeleev",
    # Engineering + early science
    "newton", "galileo", "tesla", "faraday", "lovelace",
    # Computer science
    "turing", "hopper", "knuth", "dijkstra", "hoare",
    "liskov", "lamport", "codd", "ritchie", "thompson",
    "kernighan",
    # Mathematics
    "gauss", "euler", "noether", "ramanujan", "godel",
    "cantor", "riemann", "hilbert",
    # Philosophy
    "plato", "aristotle", "socrates", "kant", "hume",
    "descartes", "russell", "wittgenstein", "spinoza",
)


def _derive_name_from_random() -> str:
    """Per `dead-simple-install` (2026-05-21): when --name is
    omitted, pick a random last name from `SCIENTIST_ROSTER`.

    Uses `random.SystemRandom` so the pick draws from OS entropy,
    not the seeded `random` module that might be set elsewhere
    in the process. Returns a string slug.

    The operator sees the chosen name + the override hint at
    install start. This helper is only reached when no existing
    user-state academic-researcher deployment can be reused.
    """
    import random
    if not SCIENTIST_ROSTER:
        # Defensive fallback; the roster is hard-coded non-empty.
        return _derive_name_from_hostname()
    return random.SystemRandom().choice(SCIENTIST_ROSTER)


def _parse_compose_port(output: str) -> Optional[int]:
    """Parse the `docker compose port <service> <container-port>`
    output, which is one or two lines like:

        0.0.0.0:32773
        [::]:32773

    Returns the port as an int. Returns None if no parseable
    port found.

    Per `dead-simple-install`: this replaces the TOCTOU-prone
    `_pick_free_port` strategy. Docker atomically picks a free
    ephemeral host port when we pass `0:5432` in the compose
    port mapping; we then ask Docker which port it picked.
    """
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        # Take the substring after the last ':' on the line.
        # Handles both `0.0.0.0:32773` and `[::]:32773`.
        tail = line.rsplit(":", 1)[-1]
        try:
            return int(tail)
        except ValueError:
            continue
    return None


def _pick_free_port(*, starting_at: int) -> int:
    """Return the lowest free TCP port at or above `starting_at`.

    Probes by binding to 127.0.0.1; bind succeeds → port free. Not
    TOCTOU-safe; the immediate next `docker compose up` would
    surface a real collision.
    """
    import socket
    for port in range(starting_at, starting_at + 200):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    # Fell off the search range — return the default; bootstrap
    # will surface the collision when it tries to actually bind.
    return starting_at


# ---------- add / remove / doctor / reset ----------


def _cmd_add(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg add <source>` — low-level recovery add for local/path
    sources. Normal operator setup uses the planner/committer.
    """
    from . import wizards as _wiz

    source_short = args.source
    if source_short not in _wiz.WIZARDS:
        return _fail(
            f"unknown source `{source_short}`. Supported: "
            f"{', '.join(_wiz.supported_short_names())}",
            as_json=as_json,
        )
    source_id, kind, wizard_fn = _wiz.WIZARDS[source_short]
    if kind == "connector":
        return _fail(
            f"`okg add {source_short}` is no longer an academic-researcher "
            "setup path. Open the registered agent, say `set up my "
            "OKG`, and let the connector route planner report "
            "connection and allowlist status.",
            as_json=as_json,
        )

    repo_root = _find_repo_root()
    deployment_name = args.deployment
    dep_dir = _resolve_existing_dep_dir(
        deployment_name, repo_root=repo_root,
    )
    if not dep_dir.is_dir():
        return _fail(
            f"deployment not found: {dep_dir}. Run `okg install "
            f"--profile academic-researcher` first.",
            as_json=as_json,
        )

    # Wizard returns a dict of config to merge.
    try:
        config = wizard_fn(deployment_name)
    except (KeyboardInterrupt, EOFError):
        return _fail("cancelled by operator", as_json=as_json)
    except RuntimeError as e:
        return _fail(str(e), as_json=as_json)

    # Build the new entry and hand off to the rollback-aware
    # helper in source_admin. The helper writes the registry,
    # runs scoped ingest, and restores the pre-call entry shape
    # on ingest failure. Per
    # `fix-add-source-rollback-on-ingest-failure`.
    #
    # Per `fix-add-source-envelope-accuracy`: open a psycopg
    # connection so the helper can do its pre/post row-count
    # check and distinguish `ingest_complete` (rows added) from
    # `ingest_complete_no_rows` (zero rows). Best-effort: if
    # the connection fails, we fall through to the legacy
    # rc-only envelope so the CLI still works against a broken
    # Postgres.
    # Canonical home is source_lifecycle (the source_admin shim
    # re-exports only the public tools, not this private helper —
    # importing it from source_admin broke `okg add` with an
    # ImportError after `extract-mcp-source-lifecycle`).
    from okg.substrate.mcp.source_lifecycle import (
        _apply_source_with_ingest,
    )

    registry_path = dep_dir / "source_registry.yaml"
    raw = fastyaml.safe_load(registry_path.read_text()) or {}
    sources = raw.setdefault("sources", {})
    existing = sources.get(source_id) or {}
    new_entry = dict(existing)
    new_params = dict(new_entry.get("params") or {})
    new_params.update(config)
    new_entry["params"] = new_params
    new_entry["enabled_by_default"] = True
    ownership_id = (
        new_entry.get("ownership_id")
        or f"{deployment_name}.{source_id}"
    )

    postgres_dsn = _read_deployment_postgres_dsn(dep_dir)
    envelope: dict
    conn = None
    if postgres_dsn:
        try:
            from okg.substrate.db.connection import connect as db_connect
            conn = db_connect(_expanded_dsn(postgres_dsn), autocommit=True)
        except Exception as e:  # noqa: BLE001
            print(
                f"warning: could not open Postgres for row-count "
                f"verification ({e}); falling back to rc-only "
                f"envelope.",
                file=sys.stderr,
            )
            conn = None
    try:
        envelope = _apply_source_with_ingest(
            deployment=deployment_name,
            source_id=source_id,
            registry_path=registry_path,
            raw=raw,
            new_entry=new_entry,
            conn=conn,
            ownership_id=ownership_id,
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    if envelope["status"] == "ingest_complete":
        aggregates = _try_aggregate_counts(deployment_name) or {}
        payload = {
            "status": "source_added",
            "deployment_name": deployment_name,
            "source_id": source_id,
            "params": config,
            "node_counts_total": aggregates,
        }
        # Surface per-add row delta when the helper computed it.
        if "nodes_added" in envelope:
            payload["nodes_added"] = envelope["nodes_added"]
            payload["edges_added"] = envelope["edges_added"]
        _emit(payload, as_json=as_json)
        return 0
    if envelope["status"] == "ingest_complete_no_rows":
        # Source IS registered, but produced zero rows. Surface
        # the operator-actionable hint + the cli_fallback so
        # automation can branch.
        _emit(
            {
                "status": "source_added_no_rows",
                "deployment_name": deployment_name,
                "source_id": source_id,
                "params": config,
                "nodes_added": 0,
                "edges_added": 0,
                "agent_hint": envelope.get("agent_hint"),
                "cli_fallback": envelope.get("cli_fallback"),
            },
            as_json=as_json,
        )
        return 0
    # ingest_failed (with rollback=True) or rollback_failed —
    # surface the envelope verbatim so automation gets the same
    # shape the MCP path returns. Exit non-zero so existing
    # scripts that check rc keep working.
    _emit(
        {
            "status": "source_add_failed",
            "deployment_name": deployment_name,
            **envelope,
        },
        as_json=as_json,
    )
    return 1


def _cmd_remove(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg remove <source>` — drop a source from the registry.
    Existing nodes from the source are NOT deleted; future publishes
    won't emit new ones.
    """
    from . import wizards as _wiz

    source_short = args.source
    if source_short not in _wiz.WIZARDS:
        return _fail(
            f"unknown source `{source_short}`. Supported: "
            f"{', '.join(_wiz.supported_short_names())}",
            as_json=as_json,
        )
    source_id, _kind, _fn = _wiz.WIZARDS[source_short]

    repo_root = _find_repo_root()
    deployment_name = args.deployment
    dep_dir = _resolve_existing_dep_dir(
        deployment_name, repo_root=repo_root,
    )

    import yaml
    registry_path = dep_dir / "source_registry.yaml"
    raw = fastyaml.safe_load(registry_path.read_text()) or {}
    sources = raw.get("sources") or {}
    if source_id not in sources:
        return _fail(
            f"source `{source_id}` not in registry; nothing to "
            f"remove",
            as_json=as_json,
        )
    sources.pop(source_id, None)
    raw["sources"] = sources
    registry_path.write_text(yaml.safe_dump(raw, sort_keys=False))

    _emit(
        {
            "status": "source_removed",
            "deployment_name": deployment_name,
            "source_id": source_id,
            "note": (
                "Existing nodes from this source remain in the "
                "graph. Run `okg reset` if you want full teardown."
            ),
        },
        as_json=as_json,
    )
    return 0


def _cmd_reset(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg reset` — remove any installed deployment (issue #3414).

    The verb is registered for every deployment, whichever profile built
    it; only the Docker teardown is specific to this profile's installer.
    """
    repo_root = _find_repo_root()
    try:
        deployments_root = _install_deployments_root(args)
    except DeploymentResolutionError as e:
        return _fail_typed(
            _deployments_root_unresolved_error(e), as_json=as_json,
        )
    return _provision_postgres_and_scaffold_reset(
        deployment_name=args.deployment,
        repo_root=repo_root,
        as_json=as_json,
        yes=bool(getattr(args, "yes", False)),
        deployments_root=deployments_root,
    )


# ---------- internal: deployment.yaml docker metadata writer ----------


def _write_deployment_docker_block(
    dep_dir: Path,
    *,
    compose_path: Path,
    project_name: str,
    host_port: Optional[int] = None,
) -> None:
    """Per `dead-simple-install`: stamp `deployment.yaml` with a
    `docker:` block recording the compose path + project name +
    Docker-assigned host port. Lifecycle verbs (`okg start/stop/
    restart`) read these fields to locate the deployment's
    container stack. A deployment without a `docker:` block (e.g.
    one installed via `--use-local-postgres`) is left
    untouched; lifecycle verbs error cleanly when called against
    it.
    """
    import yaml
    dy = dep_dir / "deployment.yaml"
    if not dy.is_file():
        return
    try:
        raw = fastyaml.safe_load(dy.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return
    block: dict = {
        "compose_path": str(compose_path),
        "project_name": project_name,
    }
    if host_port is not None:
        block["host_port"] = int(host_port)
    raw["docker"] = block
    dy.write_text(yaml.safe_dump(raw, sort_keys=False))


def _read_deployment_postgres_dsn(dep_dir: Path) -> Optional[str]:
    """Pull the persisted Postgres DSN from deployment.yaml.

    Prefer the canonical `postgres.dsn` block; the profile
    provenance copy may redact secret answers. Fall back to the
    legacy `profile.init_answers.postgres_dsn` location for older
    deployments. Returns None if the file is missing or the field
    is absent.

    Per `fix-okg-install-claude-config`: the
    `--claude-config-only` path reads the canonical DSN from
    the deployment scaffold rather than reconstructing it,
    which lets operators who hand-edited the YAML (e.g. to
    point at a remote Postgres) refresh their Claude entry
    correctly.
    """
    import yaml
    dy = dep_dir / "deployment.yaml"
    if not dy.is_file():
        return None
    try:
        raw = fastyaml.safe_load(dy.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None
    pg_block = raw.get("postgres") or {}
    profile = raw.get("profile") or {}
    answers = profile.get("init_answers") or {}
    dsn = pg_block.get("dsn") or answers.get("postgres_dsn")
    return str(dsn) if dsn else None


def _expanded_dsn(dsn: str) -> str:
    """``dsn`` with its ``${VAR}`` references expanded, for a connection.

    deployment.yaml keeps the operator's placeholder (``${OKG_DSN}``) so the
    password never lands on disk; a connection needs the expanded value. The
    default profile's helper refuses an unset variable by name.
    """
    from okg.substrate.cli.init import _resolve_install_answer_dsn

    return _resolve_install_answer_dsn(dsn)


# ---------- helpers ----------


def _deployment_profile_name(dep_dir: Path) -> Optional[str]:
    """Return deployment.yaml's profile name, if present."""
    import yaml
    dy = dep_dir / "deployment.yaml"
    if not dy.is_file():
        return None
    try:
        raw = fastyaml.safe_load(dy.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None
    profile = raw.get("profile") or {}
    name = profile.get("name")
    return str(name) if name else None


def _install_deployments_root(args: argparse.Namespace) -> Path | None:
    """The root this install must write into, or None for the default cascade.

    Issue #2127 F9: `--deployments-root` is declared on the `install` parser
    and the generic scaffold path honours it, but this profile installer read
    neither it nor `OKG_DEPLOYMENTS_DIR` -- it computed every path from
    `user_state_deployments_root()`, so an operator who named a root got a
    plan and a deployment somewhere else with no warning.

    Resolution order AND the existence rule now match `cli/init.py`:

    * explicit `--deployments-root`: expanded, resolved, and NOT required to
      exist. The operator named this root for this scaffold, so creating it
      is part of scaffolding.
    * `OKG_DEPLOYMENTS_DIR`: expanded, and required to EXIST. It is an
      ambient pointer at a portfolio the operator already has; a misspelled
      value must be heard about rather than quietly turned into a second
      root. This is the resolver contract in `okg.substrate.deployments`
      ("set but not pointing at an existing directory raises rather than
      silently falling through"). `cli/init.py` used to create it instead;
      issue #2127 settled the disagreement in favour of the contract, and
      both commands raise through one shared builder.
    * neither set: the default cascade.
    """
    explicit = getattr(args, "deployments_root", None)
    if explicit:
        return Path(explicit).expanduser().resolve()
    env_root = os.environ.get("OKG_DEPLOYMENTS_DIR")
    if env_root:
        root = Path(env_root).expanduser()
        if not root.is_dir():
            raise DeploymentResolutionError(
                f"OKG_DEPLOYMENTS_DIR={str(root)!r} is not an existing "
                f"directory"
            )
        return root.resolve()
    return None


def _install_scaffold_target(
    deployment_name: str, *, deployments_root: Path | None,
) -> Path:
    """Where this install would put a NEW deployment directory."""
    from okg.substrate.deployments import default_deployment_target

    if deployments_root is not None:
        return deployments_root / deployment_name
    return default_deployment_target(deployment_name)


def _find_install_deployment_dir(
    deployment_name: str, *, deployments_root: Path | None,
) -> Path | None:
    """Locate an existing deployment, honouring an operator-named root.

    With a root named, the search is that root and nothing else: the point of
    naming one is that deployments outside it are not this install business,
    and letting the cascade answer would report a same-named user-state
    deployment as an existing install of THIS one.
    """
    from okg.substrate.deployments import find_deployment_dir

    if deployments_root is not None:
        candidate = deployments_root / deployment_name
        if (candidate / "deployment.yaml").is_file():
            return candidate
        return None
    return find_deployment_dir(deployment_name)


def _user_state_academic_researcher_deployments(
    *, deployments_root: Path | None = None,
) -> list[Path]:
    """List academic-researcher deployments in the root this install writes into.

    Only that root participates in no-name install idempotency. In-repo
    deployments such as okg-workspace are framework fixtures, not operator
    installs, and should not make a fresh install ambiguous.

    A deployment that still records a retired name of this profile
    (`academic-pi`, `cs-researcher`) raises ProfileRetiredError instead of
    being skipped: skipping it would make a no-name rerun create a second
    deployment beside the operator's existing one.
    """
    from okg.substrate.deployments import user_state_deployments_root

    root = (
        deployments_root if deployments_root is not None
        else user_state_deployments_root()
    )
    if not root.is_dir():
        return []
    dep_dirs = sorted(p for p in root.iterdir() if p.is_dir())
    for dep_dir in dep_dirs:
        _refuse_retired_profile_record(dep_dir)
    return [
        dep_dir for dep_dir in dep_dirs
        if _deployment_profile_name(dep_dir) == PROFILE_NAME
    ]


#: This profile's name, as scaffolded deployments record it in
#: `deployment.yaml` under `profile.name`.
PROFILE_NAME = "academic-researcher"


def _refuse_retired_profile_record(dep_dir: Path) -> None:
    """Refuse a deployment that records a retired name of this profile.

    On 2026-09-23 `academic-pi` was renamed `academic-researcher` (its
    ontology module `academic_pi` became `academic_researcher`) and
    `cs-researcher` was merged into it. A deployment scaffolded before
    then still records the old name, and an `academic-pi` one also names
    the old module and source package. Raises ProfileRetiredError
    (`profile_retired`) whose fix hint rewrites the deployment's files in
    place, re-claims catalog ownership, reloads the catalog, and re-runs
    the install.
    """
    from okg.substrate.deployment_bootstrap.profile_init import (
        RETIRED_PROFILES,
        ProfileRetiredError,
    )

    recorded = _deployment_profile_name(dep_dir)
    retired = RETIRED_PROFILES.get(recorded) if recorded else None
    if retired is None or retired.replacement != PROFILE_NAME:
        return
    substitutions = (
        ACADEMIC_PI_REWRITE if recorded == "academic-pi"
        else f"s/{recorded}/{PROFILE_NAME}/g"
    )
    rewrite = (
        f"perl -pi -e '{substitutions}' "
        + " ".join(
            shlex.quote(str(path))
            for path in _retired_record_files(dep_dir)
        )
    )
    # The stored catalog and the ownership row both carry the old catalog
    # fingerprint. `okg migrate` changes neither: the publish refuses
    # catalog_ownership_foreign_writer until the claim, then
    # catalog_applied_version_absent until the catalog load.
    name = shlex.quote(dep_dir.name)
    claim = f"okg catalog ownership claim --deployment {name}"
    load = f"okg catalog load --deployment {name} --apply"
    reinstall = f"okg install --profile {PROFILE_NAME} --name {name}"
    fix = f"{rewrite} && {claim} && {load} && {reinstall}"
    raise ProfileRetiredError(
        f"deployment {dep_dir.name!r} at {dep_dir} records retired profile "
        f"{recorded!r}, now {PROFILE_NAME!r}; rewrite the record with "
        f"`{rewrite}`, then run `{claim}`, then `{load}`, then re-run "
        f"`{reinstall}`",
        data={
            "profile_name": recorded,
            "replacement": PROFILE_NAME,
            "reason": retired.reason,
            "deployment_dir": str(dep_dir),
            "rewrite_command": rewrite,
            "claim_command": claim,
            "catalog_load_command": load,
            "reinstall_command": reinstall,
        },
        fix_hint=fix,
    )


#: Perl substitutions that move an `academic-pi` deployment's files to the
#: new names: the profile name, the module name and the source and linker
#: module paths, the linker class name, and the `academic_pi_*` health-check
#: names. Three stored graph identifiers keep the old spelling, because
#: published rows carry them: the linker name `academic_pi_collaboration`
#: (only its module path `linkers.academic_pi_collaboration` moves), the
#: review source `_review:academic_pi_candidate_review`, and the
#: `okg.academic_pi.candidate_link*.v1` schema strings.
ACADEMIC_PI_REWRITE = (
    r"s/academic-pi/academic-researcher/g; "
    r"s/AcademicPi/AcademicResearcher/g; "
    r"s/linkers\.academic_pi_collaboration\b/"
    r"linkers.academic_researcher_collaboration/g; "
    r"s/(?<!okg\.)academic_pi(?!_collaboration\b|_candidate_review\b)/"
    r"academic_researcher/g"
)


def _retired_record_files(dep_dir: Path) -> list[Path]:
    """The deployment's own files that can name the retired profile:
    deployment.yaml, source_registry.yaml, invariants.yaml,
    dashboards/*.yaml and every file under skills/."""
    files = [
        dep_dir / name
        for name in ("deployment.yaml", "source_registry.yaml", "invariants.yaml")
        if (dep_dir / name).is_file()
    ]
    dashboards = dep_dir / "dashboards"
    if dashboards.is_dir():
        files += sorted(p for p in dashboards.glob("*.yaml") if p.is_file())
    skills = dep_dir / "skills"
    if skills.is_dir():
        files += sorted(p for p in skills.rglob("*") if p.is_file())
    return files


def _resolve_existing_dep_dir(
    deployment_name: str, *, repo_root: Optional[Path],
    deployments_root: Path | None = None,
) -> Path:
    """Resolve an existing deployment directory.

    Per `separate-framework-and-deployments` (2026-05-22): walks
    the substrate's multi-root cascade (user-state XDG dir →
    in-repo `deployments/` → in-tree fallback) with one extra
    fallback for the legacy in-repo path during the migration
    window. Raises `FileNotFoundError` if not found anywhere.

    Callers expect a real directory; tolerate-absent is the
    `find_deployment_dir(...) is None` pattern at the call site
    rather than this helper.
    """
    dep_dir = _find_install_deployment_dir(
        deployment_name, deployments_root=deployments_root,
    )
    if dep_dir is None:
        legacy = _legacy_in_repo_deployment_dir(repo_root, deployment_name)
        if legacy is not None:
            return legacy
        raise FileNotFoundError(
            f"deployment {deployment_name!r} not found in "
            f"user-state, in-repo, or OKG_DEPLOYMENTS_DIR "
            f"override"
        )
    return dep_dir


def _find_repo_root() -> Optional[Path]:
    """The okg source checkout containing cwd, or None outside one.

    Walks up from cwd looking for a `src/okg/` directory. A checkout is
    where the legacy in-repo `deployments/` and the `ops/pg` image-build
    context live, and where child processes used to be started.

    Issue #2313. Outside a checkout this used to fall back to the directory
    this module lives in (`.../okg/substrate`, inside the installed
    package). Children started there with `python -m` got that directory
    first on `sys.path`, so the package's own `okg/substrate/mcp` and
    `okg/substrate/types.py` shadowed the `mcp` library and the standard
    library `types`, and the first ingest failed as a circular import. No
    directory inside the installed package is a repo root: callers treat
    None as "no checkout" -- they skip the legacy in-repo lookups, refuse
    the image build by name, and let children inherit the operator's
    working directory, the same one this process resolved its deployments
    root from.
    """
    here = Path.cwd()
    for parent in [here, *here.parents]:
        if (parent / "src" / "okg").is_dir():
            return parent
    return None


def _legacy_in_repo_deployment_dir(
    repo_root: Optional[Path], deployment_name: str,
) -> Optional[Path]:
    """`<checkout>/deployments/<name>` when it is a directory, else None."""
    if repo_root is None:
        return None
    legacy = repo_root / "deployments" / deployment_name
    return legacy if legacy.is_dir() else None


def _find_compose_yaml() -> Path:
    """Locate the academic-researcher profile's shipped compose.yaml.

    This file lives at `library/profiles/academic-researcher/cli/install.py`;
    `compose.yaml` is a sibling of the `cli/` directory.
    """
    here = Path(__file__).resolve()
    return here.parent.parent / "compose.yaml"


def _default_dsn(
    deployment_name: str, port: int, password: str = "okg",
) -> str:
    """Default Postgres DSN for an academic-researcher deployment.

    `password` is per-deployment-randomized by the install flow
    (`_resolve_pg_password`); the `okg` default exists only for
    direct callers/tests. Combined with the 127.0.0.1 port bind in
    compose.yaml, the DB is unreachable off-host.
    """
    return (
        f"postgres://postgres:{password}@localhost:{port}/"
        f"okg_{deployment_name}"
    )


def _operator_postgres_dsn(args: argparse.Namespace) -> Optional[str]:
    """The existing-database DSN the operator named, or None.

    Issue #2210. Two slots can carry it: the native `--postgres-dsn`
    declared by `add_install_arguments` (dest `postgres_dsn`) and the
    profile-question injection in `cli/main.py` (dest
    `answer_postgres_dsn`), which still fires for the standalone
    `_build_install_parser` and for `okg init --profile academic-researcher`.
    Reading both is what makes the flag mean the same thing whichever
    parser assembled it.
    """
    for attr in ("postgres_dsn", "answer_postgres_dsn"):
        value = getattr(args, attr, None)
        if value:
            return str(value)
    return None


def _redacted_dsn(dsn: str) -> str:
    """Mask the password in `dsn` with the substrate's own redactor.

    Imported lazily: `okg.substrate.db.connection` imports psycopg, and
    this profile module is loaded to build `okg install --help`.
    """
    from okg.substrate.db.connection import redact

    return redact(dsn)


# The probe's own name in `pg_stat_activity`, so an operator watching the
# server sees which okg step opened the connection that is asking about
# privileges and extensions.
PROBE_APPLICATION_NAME = "okg-install-postgres-probe"

# Shorter than the substrate default (10s). The operator is sitting at a
# terminal that has printed nothing yet, and a DSN nobody answers should
# come back as a refusal quickly rather than look like a hang. Still far
# larger than any real connect, local or on a campus network.
PROBE_CONNECT_TIMEOUT_SECONDS = 5


def _require_reachable_postgres(dsn: str) -> None:
    """Open one connection to `dsn` and ask whether this install can use it.

    Issue #2210. The operator-supplied database is not provisioned by
    this installer, so nothing else proves it exists before the scaffold
    child process, the migration and the first publish all assume it
    does. A refusal here costs one connection; the alternative is a
    half-written deployment pointing at a database that never answered.

    Reachability is not usability. The migration a few steps later issues
    `CREATE SCHEMA`, `CREATE EXTENSION`, `CREATE TABLE`, `CREATE ROLE`
    and `GRANT <role> TO <role>`; a role that can connect but may not do
    those gets past a bare `SELECT 1`, and the install then fails AFTER
    the deployment directory, the schemas and the source registry are on
    disk. So the same open connection asks five more questions, all of
    them read-only catalog reads:

    1. `CREATE` on the database -- the migration creates the `okg` and
       `okg_derived` schemas.
    2. Whether every name in `REQUIRED_EXTENSIONS` is listed in
       `pg_available_extensions`, and which of them `pg_extension`
       already carries.
    3. `CREATE` on schema `public`, but ONLY when some required
       extension is not installed yet. Every `CREATE TABLE` in
       `schema.sql` is qualified into `okg` or `okg_derived`; the one
       statement that touches `public` is the unqualified `CREATE
       EXTENSION` in `ensure_required_extensions`, which that function
       skips for extensions already present. Asking unconditionally
       refused the ordinary managed-PostgreSQL shape -- a DBA installs
       the extensions and grants the role `CREATE` on the database -- on
       which the migration in fact runs, proved live on PostgreSQL 17.
    4. Whether schema `public` exists at all, asked BEFORE
       `has_schema_privilege`, which raises `InvalidSchemaName` on a
       database whose `public` was dropped and would otherwise be
       reported as "not reachable" on a database that answered.
    5. The schema roles. `_ensure_schema_roles` and `views.sql` create
       the names in `migrate.SCHEMA_ROLES` and then GRANT or ALTER them;
       on PostgreSQL 16 and later CREATEROLE does not carry admin over a
       role the connecting role did not create. So: CREATEROLE (or
       superuser) when any of those roles is absent, and ADMIN OPTION on
       each one that already exists. Without this the probe passed a
       database whose migration then died with `permission denied to
       grant role "app_rw"`, reproduced live.

    What this probe does NOT prove: that the whole migration succeeds.
    It asks the five questions above on one connection; a privilege it
    does not ask about can still stop `okg migrate` later.

    Two codes, because the operator fixes them differently:
    `install_postgres_dsn_unreachable` means nobody answered, and
    `install_postgres_dsn_unusable` means the database answered and
    cannot be used, naming the missing privilege, extension or schema.

    The connection comes from the substrate factory
    (`okg.substrate.db.connection.connect`), not from a raw
    `psycopg.connect`, so the probe carries the same TCP keepalives, the
    finite connect timeout, the pinned `search_path` and the
    idle-in-transaction bound every other substrate session carries, and
    names itself in `pg_stat_activity`. The factory needs no installed
    schema and no deployment object, which matters here: this probe runs
    BEFORE the deployment directory exists, so `deployment=` is left
    unset and the bound falls back to the process env or the built-in
    default. `autocommit=True` because every query is a read-only
    catalog read that needs no transaction -- and an operator-supplied
    server must not see a stray idle transaction from a probe.

    Raises a coded :class:`SubstrateError` naming the REDACTED DSN --
    the password the operator typed must not reach a log, a terminal or
    a JSON payload.
    """
    # Lazy, and from the migrator itself: a second hand-maintained list
    # would drift from the one the migration actually runs.
    from okg.substrate.db import connection
    from okg.substrate.db.migrate import (
        REQUIRED_EXTENSIONS,
        SCHEMA_ROLES,
        SCHEMA_ROLES_NEEDING_ADMIN_OPTION,
    )

    wanted = sorted(REQUIRED_EXTENSIONS)
    roles_wanted = sorted(SCHEMA_ROLES)
    redacted = _redacted_dsn(dsn)
    probe: dict[str, object] = {}
    row = None
    try:
        with connection.connect(
            dsn,
            autocommit=True,
            application_name=PROBE_APPLICATION_NAME,
            connect_timeout=PROBE_CONNECT_TIMEOUT_SECONDS,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                if row == (1,):
                    # Database-level CREATE authorizes CREATE SCHEMA, so
                    # the migration needs it on every install. `public`
                    # is asked about separately and conditionally below;
                    # its EXISTS term rides along here so a dropped
                    # `public` cannot raise inside this block.
                    cur.execute(
                        "SELECT has_database_privilege("
                        "current_user, current_database(), 'CREATE'), "
                        "EXISTS (SELECT 1 FROM pg_namespace "
                        "WHERE nspname = 'public'), "
                        "(SELECT rolsuper FROM pg_roles "
                        "WHERE rolname = current_user), "
                        "(SELECT rolcreaterole FROM pg_roles "
                        "WHERE rolname = current_user)"
                    )
                    probe["database"] = cur.fetchone()
                    # Installed AND installable in one read: an already
                    # installed extension is listed by
                    # pg_available_extensions too, so the second column
                    # is what decides whether CREATE EXTENSION runs.
                    cur.execute(
                        "SELECT a.name, EXISTS (SELECT 1 FROM pg_extension e "
                        "WHERE e.extname = a.name) "
                        "FROM pg_available_extensions a "
                        "WHERE a.name = ANY(%s)",
                        (wanted,),
                    )
                    extensions = [
                        (str(r[0]), bool(r[1])) for r in cur.fetchall()
                    ]
                    probe["available"] = sorted(n for n, _ in extensions)
                    probe["installed"] = sorted(
                        n for n, present in extensions if present
                    )
                    # Which schema roles exist, and which of those this
                    # role may grant on. `MEMBER WITH ADMIN OPTION` is
                    # true for a superuser and for a role holding admin,
                    # whether granted directly or through a chain.
                    cur.execute(
                        "SELECT r.rolname, pg_has_role("
                        "current_user, r.oid, 'MEMBER WITH ADMIN OPTION') "
                        "FROM pg_roles r WHERE r.rolname = ANY(%s)",
                        (roles_wanted,),
                    )
                    probe["roles"] = {
                        str(r[0]): bool(r[1]) for r in cur.fetchall()
                    }
                    database_row = tuple(probe.get("database") or ())
                    public_exists = bool(
                        (database_row + (None, None, None, None))[1]
                    )
                    to_create = [
                        name
                        for name in wanted
                        if name not in set(probe.get("installed") or ())
                    ]
                    if to_create and public_exists:
                        # Only now is `public` load-bearing: the
                        # unqualified CREATE EXTENSION lands there.
                        cur.execute(
                            "SELECT has_schema_privilege("
                            "current_user, 'public', 'CREATE')"
                        )
                        probe["public_create"] = cur.fetchone()
    except Exception as e:  # noqa: BLE001 -- any driver failure is the same refusal.
        err = SubstrateError(
            f"--postgres-dsn is not reachable: {redacted}: {e}",
            data={"postgres_dsn": redacted, "stage": "connect"},
            fix_hint=(
                "start the database and create the target database, or "
                "drop --postgres-dsn to let the installer provision "
                "Docker Postgres"
            ),
        )
        err.code = "install_postgres_dsn_unreachable"
        raise err from e
    if row != (1,):
        err = SubstrateError(
            f"--postgres-dsn answered, but `SELECT 1` returned {row!r}: "
            f"{redacted}",
            data={"postgres_dsn": redacted, "stage": "select"},
            fix_hint="point --postgres-dsn at a real PostgreSQL database",
        )
        err.code = "install_postgres_dsn_unreachable"
        raise err
    database_row = (tuple(probe.get("database") or ()) + (None,) * 4)[:4]
    database_create, public_exists, is_superuser, can_create_role = (
        database_row[0] is True,
        database_row[1] is True,
        database_row[2] is True,
        database_row[3] is True,
    )
    if not database_create:
        raise _unusable_dsn(
            redacted,
            "its role may not create in that database "
            "(missing: CREATE on the database)",
            stage="privileges",
            data={"missing_privileges": ["CREATE on the database"]},
            fix_hint=(
                "grant CREATE on the database to the role in "
                "--postgres-dsn, or point the flag at a database that "
                "role owns"
            ),
        )
    available = set(probe.get("available") or ())
    missing_extensions = [name for name in wanted if name not in available]
    if missing_extensions:
        raise _unusable_dsn(
            redacted,
            "this server cannot install the extensions the schema "
            f"requires (missing: {', '.join(missing_extensions)})",
            stage="extensions",
            data={"missing_extensions": missing_extensions},
            fix_hint=(
                "install these PostgreSQL extensions on the server, or drop "
                "--postgres-dsn to let the installer provision Docker "
                "Postgres, whose image already carries them"
            ),
        )
    installed = set(probe.get("installed") or ())
    to_create = [name for name in wanted if name not in installed]
    if to_create and not public_exists:
        raise _unusable_dsn(
            redacted,
            "schema public does not exist, and CREATE EXTENSION needs it "
            f"to install {', '.join(to_create)}",
            stage="schema",
            data={"missing_schema": "public", "missing_extensions": to_create},
            fix_hint=(
                "create schema public on that database, or install these "
                "extensions there yourself so the migration does not have "
                "to"
            ),
        )
    if to_create and (probe.get("public_create") or (None,))[0] is not True:
        raise _unusable_dsn(
            redacted,
            "its role may not create in schema public, where CREATE "
            f"EXTENSION installs {', '.join(to_create)} "
            "(missing: CREATE on schema public)",
            stage="privileges",
            data={
                "missing_privileges": ["CREATE on schema public"],
                "missing_extensions": to_create,
            },
            fix_hint=(
                "grant CREATE on schema public to the role in "
                "--postgres-dsn, or install these extensions on that "
                "database yourself"
            ),
        )
    if not is_superuser:
        present_roles = dict(probe.get("roles") or {})
        absent = [name for name in roles_wanted if name not in present_roles]
        if absent and not can_create_role:
            raise _unusable_dsn(
                redacted,
                "its role may not create the roles the migration needs "
                f"({', '.join(absent)}) (missing: CREATEROLE)",
                stage="roles",
                data={
                    "missing_privileges": ["CREATEROLE"],
                    "missing_roles": absent,
                },
                fix_hint=(
                    "grant CREATEROLE to the role in --postgres-dsn, or "
                    "create these roles on the server yourself and grant "
                    "that role ADMIN OPTION on each"
                ),
            )
        not_administered = [
            name
            for name in sorted(SCHEMA_ROLES_NEEDING_ADMIN_OPTION)
            if name in present_roles and not present_roles[name]
        ]
        if not_administered:
            missing = [
                f"ADMIN OPTION on {name}" for name in not_administered
            ]
            raise _unusable_dsn(
                redacted,
                "its role may not grant the roles the migration grants "
                f"(missing: {', '.join(missing)})",
                stage="roles",
                data={
                    "missing_privileges": missing,
                    "missing_roles": not_administered,
                },
                fix_hint=(
                    "run GRANT "
                    + ", ".join(not_administered)
                    + " TO <the role in --postgres-dsn> WITH ADMIN OPTION "
                    "as a superuser"
                ),
            )
    return None


def _unusable_dsn(
    redacted: str,
    detail: str,
    *,
    stage: str,
    data: dict[str, object],
    fix_hint: str,
) -> SubstrateError:
    """Build the one refusal shape every usability check raises.

    One helper because the code, the `--postgres-dsn answered, but`
    opening and the REDACTED DSN in both the message and the payload
    have to be identical across the checks; a hand-copied fifth block is
    where the password leaks back in.
    """
    err = SubstrateError(
        f"--postgres-dsn answered, but {detail}: {redacted}",
        data={"postgres_dsn": redacted, "stage": stage, **data},
        fix_hint=fix_hint,
    )
    err.code = "install_postgres_dsn_unusable"
    return err


def _password_from_dsn(dsn: str) -> Optional[str]:
    """Extract the password from a `scheme://user:password@host/...`
    DSN. Returns None if the DSN has no password segment."""
    m = re.match(r"^[^:]+://[^:/@]+:([^@]*)@", dsn or "")
    return m.group(1) if m else None


def _resolve_pg_password(
    deployment_name: str, *, deployments_root: Path | None = None,
) -> str:
    """Per-deployment Postgres password.

    A re-run of `okg install` against an EXISTING deployment must
    reuse the password already baked into that deployment's Postgres
    volume at init — minting a new one would make the persisted DSN
    fail to authenticate. So: read the existing deployment's
    persisted DSN and reuse its password; only when none exists
    (a genuinely fresh install) generate a fresh random secret.

    Per `harden-okg-network-and-secrets` — replaces the universal
    hardcoded `okg` that was identical on every install worldwide.
    """
    try:
        existing = _find_install_deployment_dir(
            deployment_name, deployments_root=deployments_root,
        )
        if existing is not None:
            dep_yaml = Path(existing) / "deployment.yaml"
            if dep_yaml.is_file():
                raw = fastyaml.safe_load(dep_yaml.read_text()) or {}
                dsn = (
                    (raw.get("postgres") or {}).get("dsn")
                    or (
                        (raw.get("profile") or {}).get("init_answers")
                        or {}
                    ).get("postgres_dsn")
                )
                pw = _password_from_dsn(dsn) if dsn else None
                if pw:
                    return pw
    except Exception:  # noqa: BLE001
        # Reuse is best-effort; on any read/parse failure fall through
        # to a fresh secret. (A fresh secret on a pre-existing volume
        # would fail loudly at the first connect — never silently.)
        pass
    return secrets.token_urlsafe(24)


def _build_okg_pg17_image(repo_root: Optional[Path]) -> Optional[str]:
    """Build the substrate's `okg-pg17:local` Postgres image.

    Idempotent + cached: Docker re-uses layers from prior builds, so
    only the first-ever install pays the full ~5 min cost. Returns
    None on success, a failure message string on error.

    The build context `ops/pg` exists only in a source checkout. Outside
    one (`repo_root is None`, issue #2313) this refuses by name instead of
    looking for it inside the installed package.
    """
    if repo_root is None:
        return (
            "can't auto-build okg-pg17:local: the image's build context "
            "(ops/pg) ships only in an okg source checkout, and this "
            "install is not running inside one. Build or pull "
            "okg-pg17:local first, or pass --postgres-dsn <dsn> to use an "
            "existing PostgreSQL database."
        )
    ops_pg = repo_root / "ops/pg"
    # `docker compose` accepts either filename; the substrate ships
    # `ops/pg/docker-compose.yaml`. Accept either so a fresh
    # first-install (no pre-built okg-pg17:local image) finds the
    # build context instead of aborting on the wrong name.
    if not any(
        (ops_pg / name).is_file()
        for name in ("docker-compose.yaml", "compose.yaml")
    ):
        return (
            f"can't auto-build okg-pg17:local: no compose file "
            f"(docker-compose.yaml or compose.yaml) in {ops_pg}"
        )
    print(
        "first-time: building okg-pg17:local "
        "(~5 min, one-time, layers cache for next install)..."
    )
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--build"],
            cwd=ops_pg,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        return (
            f"failed to build okg-pg17:local (rc={e.returncode}). "
            f"Try `cd {ops_pg} && docker compose up -d --build` to "
            f"see the full build output."
        )
    print("okg-pg17:local built successfully.")
    return None


def _try_aggregate_counts(
    deployment_name: str, *, deployments_root: Path | None = None,
) -> Optional[dict[str, int]]:
    """Read live subtype counts for the latest published generation.

    `okg metrics --kind aggregate_by_type` was removed from the CLI,
    so the install report must query the deployment DB directly.
    Best-effort by design: failures return None and the install still
    reports the rest of the completed flow.
    """
    try:
        repo_root = _find_repo_root()
        dep_dir = _resolve_existing_dep_dir(
            deployment_name, repo_root=repo_root,
            deployments_root=deployments_root,
        )
        postgres_dsn = _read_deployment_postgres_dsn(dep_dir)
        if not postgres_dsn:
            return None
        from okg.substrate.db.connection import connect as db_connect

        from okg.substrate.graph_store import ChronosGraphStore
        with db_connect(_expanded_dsn(postgres_dsn)) as conn:
            # Resolve the latest published generation from
            # okg.graph_generations (generation metadata -- stays on
            # the raw cursor). The per-subtype node counts then resolve
            # through a Chronos checkout pinned to that generation over
            # okg.graph_nodes; the legacy valid_from_gen/valid_to_gen
            # pin predicate is dropped (the checkpoint session injects
            # visibility).
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT max(generation_id) AS generation_id
                    FROM okg.graph_generations
                    WHERE deployment_name = %s
                      AND status = 'published'
                    """,
                    (deployment_name,),
                )
                gen_row = cur.fetchone()
            generation_id = gen_row[0] if gen_row else None
            if generation_id is None:
                return {}
            graph = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).checkout_generation(generation_id)
            rows = graph.query(
                """
                SELECT subtype, count(*)::bigint AS live_count
                FROM okg.graph_nodes
                GROUP BY subtype
                ORDER BY live_count DESC, subtype
                """
            )
            return {
                str(row["subtype"]): int(row["live_count"])
                for row in rows
            }
    except Exception:  # noqa: BLE001
        return None


def _emit(payload: dict, *, as_json: bool) -> None:
    if as_json:
        json.dump(payload, sys.stdout, indent=2, default=str)
        print()
    else:
        # Pretty print for human consumption.
        for k, v in payload.items():
            if isinstance(v, (dict, list)):
                print(f"{k}:")
                print(json.dumps(v, indent=2, default=str))
            else:
                # Issue #2277: an unset value reads "(not set)", not the
                # Python literal "None", which looks like a value.
                print(f"{k}: {'(not set)' if v is None else v}")


def _deployments_root_unresolved_error(
    exc: DeploymentResolutionError,
) -> SubstrateError:
    """Translate an unresolvable deployments root into a coded refusal.

    Issue #2127 F9. `OKG_DEPLOYMENTS_DIR` set to a directory that does not
    exist is refused by the deployment resolver with a
    :class:`DeploymentResolutionError`, which is not a `SubstrateError` and so
    walked straight out of `main()` as a traceback -- under `--json` too,
    which `docs/operate/deployments/external.md` tells operators to parse.
    The refusal itself is right and stays: an operator who exports that
    variable for an external portfolio and misspells the path must hear about
    it rather than have `okg install` create a second root behind their back.
    Only the shape changes.

    `deployment_not_found` is the documented code the substrate already
    attaches when this same exception is translated for `okg mcp-serve`, and
    its own documented fix hint names this variable. The original message
    travels verbatim in both the message and `data`, so the path it names
    survives translation.

    The envelope itself is built by `cli/init.py`, which applies the same
    existence rule to the same variable: one builder, so the code, the data
    shape and the fix hint cannot drift between the two scaffold commands.
    """
    from okg.substrate.cli.init import deployments_env_root_error

    return deployments_env_root_error(str(exc), command="okg install")


def _fail_typed(err: SubstrateError, *, as_json: bool) -> int:
    """Emit a coded error through the CANONICAL substrate CLI error path.

    Issue #2127. The first version of this helper printed its own envelope:
    JSON on STDOUT, `indent=2`, keys in insertion order, no redaction. The
    documented `--json` contract (`cli/main.py`) is the opposite on every
    count -- stderr is the structured channel, keys are sorted, and the
    payload goes through `redact_value` -- so a consumer parsing stderr saw
    nothing and one parsing stdout found an error where the success payload
    belongs. Text mode dropped the `[code]` prefix and the `data` block that
    `_text_error` prints. Delegating is what keeps one contract instead of a
    second one that drifts.

    Imported inside the function: `cli.main` imports this profile module
    through the install extension, so a module-level import would be
    circular.

    The pre-existing untyped `_fail` below still writes the old shape. It has
    25 call sites and at least one test parses its stdout envelope, so
    converting it is a separate change, not part of this one.
    """
    from okg.substrate.cli.main import _json_error, _text_error

    return _json_error(err) if as_json else _text_error(err)


def _fail(msg: str, *, as_json: bool) -> int:
    err = SubstrateError(msg, fix_hint="see `okg install --help`")
    if as_json:
        json.dump(
            {"error": str(err), "code": err.code or "pi_error"},
            sys.stdout,
            indent=2,
        )
        print()
    else:
        print(f"error: {msg}", file=sys.stderr)
    return 1


# ---------- argparse + dispatch ----------


def _build_install_parser(
    sub: argparse._SubParsersAction,
) -> None:
    """Register a standalone `install` subparser.

    Retained for direct callers and tests. The MAIN CLI no longer routes
    through this: per PACT `define-newcomer-entry-path`, `install` is the
    one documented way in, so the substrate owns the verb and reaches this
    profile through `INSTALL_EXTENSION` instead. See `add_install_arguments`.
    """
    p = sub.add_parser(
        "install",
        help=(
            "One-command academic-researcher install: Docker + Postgres "
            "+ scaffold + agent-runtime MCP registration. "
            "Data sources are wired by the setup wizard."
        ),
    )
    add_install_arguments(p)
    # Standalone parser, so it owns `--json` and `--dry-run` itself. On
    # the shared substrate parser both flags belong to the substrate,
    # because every installable profile contributes to one parser and the
    # second contributor would collide — and because the generic scaffold
    # path has to honour `--dry-run` too (issue #946).
    p.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS,
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Print the install plan and exit without writing anything. "
            "Probes Docker daemon, image presence, port availability, "
            "scaffold-dir collisions, the profile's source-defaults "
            "(which will require setup approval), and agent-runtime "
            "config collisions."
        ),
    )


def add_install_arguments(p: argparse.ArgumentParser) -> None:
    """Contribute academic-researcher's install flags to a parser.

    Split out of `_build_install_parser` so the substrate-owned `install`
    verb can offer the same flag surface it always had, without the
    substrate importing this profile by name.
    """
    p.add_argument(
        "--orcid",
        help=(
            "Operator ORCID (optional). When provided, used by the "
            "dblp_by_orcid source for clean publication matching. "
            "When omitted, "
            "the install proceeds via name-based fallback for "
            "publication sources. Find yours at https://orcid.org/."
        ),
    )
    p.add_argument(
        "--name",
        help=(
            "Deployment slug. Default: reuse the single existing "
            "academic-researcher deployment, or pick a random scientist/"
            "philosopher slug on first install."
        ),
    )
    p.add_argument(
        "--postgres-port", type=int, default=None,
        help="Docker Postgres host port (default: Docker-assigned)",
    )
    # Issue #2210. This flag used to exist ONLY as a profile-question
    # injection: `_maybe_inject_profile_flags` in cli/main.py turns the
    # profile's `postgres_dsn` init_question into `--postgres-dsn` (dest
    # `answer_postgres_dsn`), and only when `--profile` appears in argv.
    # `okg install --help` carries no `--profile`, so the flag was absent
    # from help while still being ACCEPTED on a real install -- and
    # `_cmd_install` read neither slot. Declaring it here gives it one
    # dest, one help entry, and one owner; the injector sees the name
    # collision and maps the question onto this native dest instead of
    # adding a second one.
    p.add_argument(
        "--postgres-dsn", default=None,
        help=(
            "Use an existing PostgreSQL database instead of provisioning "
            "one with Docker. The DSN is checked for reachability before "
            "anything is written, then stored in the deployment. With this "
            "flag the install starts no container, creates no Docker "
            "volume or network, and ignores --postgres-port."
        ),
    )
    p.add_argument(
        "--mcp-port", type=int, default=None,
        help="MCP server port (default: auto-pick from 5431)",
    )
    p.add_argument(
        "--git-repos",
        help=(
            "Explicit project repo path to store as setup context "
            "for later git/code/OpenSpec/session source approval. "
            "Default: none; install never infers the current OKG "
            "framework checkout as operator data."
        ),
    )
    p.add_argument(
        "--pi-name",
        help=(
            "Operator's full name as DBLP / NSF / DOE index it "
            "(e.g. 'Ada Lovelace'). Templated into ${pi_name} for "
            "the doe_osti + nsf_award_search sources AND for "
            "`dblp_by_orcid`'s name-based fallback when DBLP's "
            "ORCID endpoint doesn't cover this author. Optional; "
            "when omitted, install tries `git config --global "
            "user.name`."
        ),
    )
    p.add_argument(
        "--dblp-pid",
        help=(
            "Explicit DBLP author PID (e.g. '000/0001'). Templated "
            "into ${dblp_pid} for `dblp_by_orcid`'s resolution "
            "chain; bypasses both ORCID URL and name search. Use "
            "for operators with common names where name search "
            "would be ambiguous. Look up at "
            "https://dblp.org/search?q=<your-name>."
        ),
    )
    p.add_argument(
        "--no-claude-config", action="store_true",
        help="Skip agent-runtime MCP registration",
    )
    p.add_argument(
        "--claude-config-only", action="store_true",
        help=(
            "Skip Postgres provisioning + source setup; only "
            "refresh agent-runtime MCP registrations for an "
            "existing deployment. Requires --name."
        ),
    )
    # `--dry-run` is deliberately NOT declared here — same reason as
    # `--json` below. It is a shared flag on the substrate-owned
    # `install` parser (`_add_scaffold_arguments` in cli/main.py),
    # because the generic scaffold path has to honour it too and its
    # presence must not depend on this profile being discovered. This
    # installer still reads `args.dry_run`; only the declaration moved.
    # Per issue #946.
    p.add_argument(
        "--embed-worker", action="store_true",
        help=(
            "Start the DBOS runtime worker at the end of the install "
            "so embedding-enabled deployments can populate semantic "
            "search vectors "
            "immediately. OFF by default — the operator chooses an "
            "embedding backend rather than the install path silently "
            "running one. Run later with `okg runtime worker "
            "--deployment <name>`."
        ),
    )
    # `--json` is deliberately NOT declared here. It is a shared flag on the
    # substrate-owned `install` parser: every installable profile contributes
    # arguments to ONE parser, so a flag more than one profile would want has
    # to be owned once, centrally, or the second contributor collides.


def _build_add_parser(sub: argparse._SubParsersAction) -> None:
    from . import wizards as _wiz
    p = sub.add_parser(
        "add",
        help=(
            "Low-level recovery add for a local/path/URL source on "
            "an existing deployment."
        ),
    )
    p.add_argument(
        "source",
        choices=_wiz.supported_recovery_short_names(),
        help="Source short-name (`notes`, `cal-ics`, ...)",
    )
    p.add_argument(
        "--deployment", required=True, help="Deployment slug",
    )
    p.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS,
    )


def _build_remove_parser(sub: argparse._SubParsersAction) -> None:
    from . import wizards as _wiz
    p = sub.add_parser(
        "remove",
        help=(
            "Remove one source from a deployment registry. Existing "
            "nodes from the source remain."
        ),
    )
    p.add_argument(
        "source",
        choices=_wiz.supported_short_names(),
        help="Source short-name",
    )
    p.add_argument(
        "--deployment", required=True, help="Deployment slug",
    )
    p.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS,
    )


def _build_set_token_parser(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser(
        "set-token",
        help=(
            "Store a static per-user API token (e.g. Canvas) in the "
            "OS keychain instead of an env var."
        ),
    )
    p.add_argument(
        "--deployment", required=True, help="Deployment slug",
    )
    p.add_argument(
        "--provider", required=True,
        help="Provider key (e.g. `canvas`)",
    )
    p.add_argument(
        "--token", default=None,
        help=(
            "The token value. Omit to read from stdin (avoids the "
            "secret landing in shell history)."
        ),
    )
    p.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS,
    )


def _cmd_set_token(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.library.oauth.static_token import set_static_token
    from okg.substrate.library.oauth.token_store import KeyringUnavailable

    token = args.token
    if not token:
        import sys
        token = sys.stdin.readline().strip()
    if not token:
        return _fail("no token provided (arg or stdin)", as_json=as_json)
    try:
        set_static_token(
            deployment=args.deployment,
            provider=args.provider,
            token=token,
        )
    except KeyringUnavailable as exc:
        return _fail(str(exc), as_json=as_json)
    _emit(
        {
            "status": "token_stored",
            "deployment": args.deployment,
            "provider": args.provider,
            "custody": "keychain",
        },
        as_json=as_json,
    )
    return 0


def _build_reset_parser(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser(
        "reset",
        help=(
            "Remove one installed deployment, whatever its profile: its "
            "directory and agent-runtime registrations, plus the Docker "
            "Postgres an academic-researcher install started. A database "
            "you supplied is not touched. Confirms first."
        ),
    )
    p.add_argument(
        "--deployment", required=True, help="Deployment slug",
    )
    p.add_argument(
        "--yes", action="store_true",
        help="Skip confirmation prompt",
    )
    p.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS,
    )


def build_parsers(sub: argparse._SubParsersAction) -> None:
    """Add academic-researcher subcommands to the main parser.

    `install` is deliberately absent: the substrate owns that verb (PACT
    `define-newcomer-entry-path`) and reaches this profile through
    `INSTALL_EXTENSION`. Registering it here would also be refused by the
    profile loader, which rejects a profile claiming a substrate verb.
    """
    _build_add_parser(sub)
    _build_remove_parser(sub)
    _build_set_token_parser(sub)
    _build_reset_parser(sub)


class _AcademicResearcherInstall:
    """This profile's contribution to the substrate `install` verb."""

    #: Shown by `okg install --help` when listing installable profiles.
    description = (
        "A private research assistant for a PI or lab: Docker + Postgres "
        "+ scaffold + agent registration, with data sources wired by a "
        "setup wizard."
    )

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        add_install_arguments(parser)

    def run(self, args: argparse.Namespace, *, as_json: bool = False) -> int:
        return _cmd_install(args, as_json=as_json)


INSTALL_EXTENSION = _AcademicResearcherInstall()


DISPATCH = {
    "add": _cmd_add,
    "remove": _cmd_remove,
    "set-token": _cmd_set_token,
    "reset": _cmd_reset,
}
