"""Does this deployment directory work with the okg that is running?

`okg deployment compat` answers that before an upgrade, with no database and
no network. It is the general form of the contract checks external
deployments wrote for themselves (archi-crab-okg's `scripts/contract-check.py`):

1. `deployment lint`, the static authoring checks.
2. Every source-registry entry, `code_repos` expanded, imports, names a class
   that exists, and passes params its constructor accepts. This imports the
   deployment's own adapter modules, as ingest would; it constructs nothing.
3. The Nomos deployment policy audit with `code_repos` expanded, which
   `deployment lint` alone does not cover.
4. Optionally, every `okg` command line in a file still parses against this
   okg's command tree (`--commands`).
5. Optionally, the breaking changes between an older release contract and
   this okg's (`--against`), with the ones touching what this deployment
   uses listed first. Without `--against`, `release.tested_with` names the
   older release, whose contract a release wheel carries.

Exit 0: compatible. 1: a blocker was found. 2: a check could not run, so
compatibility is unknown; it is never reported as compatible.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import shlex
from collections.abc import Mapping
from pathlib import Path
from typing import Any

COMPAT_SCHEMA = "okg.deployment_compat.v1"

BLOCKER = "blocker"
WARNING = "warning"
UNKNOWN = "unknown"
INFO = "info"


def check_deployment_compat(
    deployment: str | Path,
    *,
    deployments_root: str | Path | None = None,
    commands_file: str | Path | None = None,
    against: str | Path | None = None,
) -> dict[str, Any]:
    """Return the compatibility report for one deployment."""
    from okg.substrate.deployment_lint import (
        lint_deployment,
        resolve_deployment_manifest_path,
    )

    findings: list[dict[str, Any]] = []
    manifest_path = resolve_deployment_manifest_path(
        deployment, deployments_root=deployments_root,
    )
    if manifest_path is None:
        # Nothing to judge: unknown, never incompatible or compatible.
        findings.append(_finding(
            "deployment.not_found",
            UNKNOWN,
            f"deployment {str(deployment)!r} was not found",
            check="lint",
            next_action="pass a deployment directory, or set OKG_DEPLOYMENTS_DIR",
        ))
        return _report(deployment, None, findings, None)
    lint = lint_deployment(deployment, deployments_root=deployments_root)
    for finding in lint.get("findings") or ():
        severity = finding.get("severity")
        if severity not in {"blocker", "warning"}:
            continue
        findings.append(_finding(
            f"lint.{finding.get('code')}",
            BLOCKER if severity == "blocker" else WARNING,
            str(finding.get("message")),
            next_action=finding.get("next_action"),
            check="lint",
        ))

    usage: dict[str, set[str]] = {
        "deployment_keys": set(),
        "source_classes": set(),
        "ontology_modules": set(),
        "cli": set(),
    }
    _run_check(findings, "sources", lambda: _check_sources(
        manifest_path, findings, usage,
    ))
    if commands_file is not None:
        _run_check(findings, "commands", lambda: _check_commands(
            Path(commands_file), findings, usage,
        ))
    breaking_since: dict[str, Any] | None = None
    if against is not None:
        breaking_since = _run_check(findings, "against", lambda: _diff_against(
            Path(against), usage,
        ))
    else:
        breaking_since = _run_check(findings, "tested_with", lambda: _diff_tested_with(
            manifest_path, findings, usage,
        ))
    if breaking_since:
        _block_touching_changes(
            findings, breaking_since, "against" if against is not None else "tested_with",
        )

    return _report(
        lint.get("deployment"), lint.get("deployment_dir"), findings,
        breaking_since,
    )


def _report(
    deployment: Any,
    deployment_dir: Any,
    findings: list[dict[str, Any]],
    breaking_since: dict[str, Any] | None,
) -> dict[str, Any]:
    import okg

    severities = {f["severity"] for f in findings}
    if BLOCKER in severities:
        verdict, exit_code = "incompatible", 1
    elif UNKNOWN in severities:
        verdict, exit_code = "unknown", 2
    else:
        verdict, exit_code = "compatible", 0
    return {
        "schema": COMPAT_SCHEMA,
        "deployment": str(deployment) if deployment is not None else None,
        "deployment_dir": deployment_dir,
        "okg_version": okg.__version__,
        "verdict": verdict,
        "ok": exit_code == 0,
        "exit_code": exit_code,
        "findings": findings,
        **({"breaking_since": breaking_since} if breaking_since is not None else {}),
    }


def _finding(
    code: str,
    severity: str,
    message: str,
    *,
    check: str,
    next_action: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "code": f"compat.{code}",
        "severity": severity,
        "check": check,
        "message": message,
    }
    if next_action:
        row["next_action"] = next_action
    if details:
        row["details"] = dict(details)
    return row


def _run_check(findings: list[dict[str, Any]], check: str, body: Any) -> Any:
    """Run one check; a check that raises makes the verdict unknown."""
    try:
        return body()
    except Exception as exc:  # noqa: BLE001 - reported as an unknown verdict.
        findings.append(_finding(
            f"{check}.could_not_run",
            UNKNOWN,
            f"the {check} check could not run: {type(exc).__name__}: {exc}",
            check=check,
        ))
        return None


# ----------------------------------------------------------------- sources


def _check_sources(
    manifest_path: Path,
    findings: list[dict[str, Any]],
    usage: dict[str, set[str]],
) -> None:
    from okg.substrate import fastyaml
    from okg.substrate.deployments import (
        inject_deployment_path,
        resolve_source_registry_path,
    )
    from okg.substrate.nomos import audit_deployment_policy
    from okg.substrate.nomos.runtime_enforcement import (
        manifest_nomos_enforcement_mode,
    )
    from okg.substrate.sources.registry import (
        STRICT_ADMISSION_CONTRACT,
        expand_code_repos_sources,
        load_source_registry,
    )

    manifest = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    usage["deployment_keys"].update(_key_paths(manifest))
    for module in manifest.get("modules") or ():
        usage["ontology_modules"].add(str(module).split(":")[-1])
    deployment_dir = manifest_path.parent
    registry_path = resolve_source_registry_path(deployment_dir, manifest)
    raw_registry: Any = (
        fastyaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
        if registry_path.is_file() else {}
    )
    if isinstance(raw_registry, dict) and raw_registry.get("code_repos"):
        raw_registry = {
            **raw_registry,
            "sources": expand_code_repos_sources(
                raw_registry, registry_path, allow_unresolved_base=True,
            ),
        }

    # The registry's own validation is reported once; each entry is still
    # imported and bound on its own, so one invalid row does not hide what
    # an upgrade broke in the others.
    try:
        if registry_path.is_file():
            load_source_registry(registry_path)
    except ValueError as exc:
        findings.append(_finding(
            "sources.registry_invalid",
            BLOCKER,
            str(exc),
            check="sources",
            next_action=f"fix {registry_path.name}",
        ))

    strict = (
        isinstance(raw_registry, Mapping)
        and raw_registry.get("admission_contract") == STRICT_ADMISSION_CONTRACT
    )
    raw_sources = (
        raw_registry.get("sources") if isinstance(raw_registry, Mapping) else None
    ) or {}
    inject_deployment_path(deployment_dir)
    for name, raw_entry in sorted(raw_sources.items()):
        if not isinstance(raw_entry, Mapping):
            continue
        if raw_entry.get("source_class"):
            usage["source_classes"].add(str(raw_entry["source_class"]))
        _check_source_entry(str(name), raw_entry, findings, strict=strict)

    # Provision audits only when the deployment runs Nomos (enforce or
    # audit); a deployment with enforcement off is not refused for a posture
    # it never evaluates.
    if manifest_nomos_enforcement_mode(manifest) == "off":
        return
    audit = audit_deployment_policy(
        manifest,
        source_registry=raw_registry,
        deployment=str(manifest.get("name") or deployment_dir.name),
    )
    for item in audit.get("findings") or ():
        if not isinstance(item, Mapping):
            continue
        if str(item.get("severity", "error")) not in {"error", "blocker"}:
            continue
        code = str(item.get("code") or "finding")
        findings.append(_finding(
            code if code.startswith("nomos.") else f"nomos.{code}",
            BLOCKER,
            str(item.get("message") or item),
            check="nomos",
            next_action=item.get("fix_hint") or item.get("next_action"),
        ))


def _check_source_entry(
    name: str,
    raw_entry: Mapping[str, Any],
    findings: list[dict[str, Any]],
    *,
    strict: bool,
) -> None:
    from okg.substrate.ingest.adapter_factory import import_source_module
    from okg.substrate.ingest.source_params import (
        ConstructorParameters,
        check_source_params,
    )

    module_name = raw_entry.get("module")
    class_name = raw_entry.get("class")
    if not module_name or not class_name:
        return
    try:
        module = import_source_module(module_name)
    except Exception as exc:  # noqa: BLE001 - any import failure is the finding.
        findings.append(_finding(
            "source.import_failed",
            BLOCKER,
            f"source {name!r}: module {module_name!r} does not import: "
            f"{type(exc).__name__}: {exc}",
            check="sources",
            next_action="install the module or fix `module:` in the registry",
        ))
        return
    adapter_class = getattr(module, str(class_name), None)
    if adapter_class is None:
        public = _public_classes(module)
        findings.append(_finding(
            "source.class_missing",
            BLOCKER,
            f"source {name!r}: {module_name}.{class_name} does not exist",
            check="sources",
            next_action="rename `class:` to one the module defines",
            details={"classes_in_module": public},
        ))
        return
    try:
        parameters = ConstructorParameters.from_class(adapter_class)
    except (TypeError, ValueError) as exc:
        parameters = None
        findings.append(_finding(
            "source.params_not_checked",
            WARNING,
            f"source {name!r}: the constructor of {module_name}.{class_name} "
            f"cannot be inspected ({exc}), so unknown and missing params "
            "were not checked",
            check="sources",
        ))
    params = raw_entry.get("params") or {}
    if not isinstance(params, Mapping):
        params = {}
    for problem in check_source_params(params, parameters):
        refused = strict or problem.refused_by_legacy
        findings.append(_finding(
            f"source.{problem.code}",
            BLOCKER if refused else WARNING,
            problem.render(name),
            check="sources",
            details=(
                {"accepted_params": list(parameters.accepted)}
                if parameters is not None else None
            ),
        ))


def _public_classes(module: Any) -> list[str]:
    """Classes a module offers: its `__all__`, else those it defines."""
    exported = getattr(module, "__all__", None)
    names = (
        exported if isinstance(exported, (list, tuple))
        else [
            attr for attr, value in vars(module).items()
            if getattr(value, "__module__", None) == module.__name__
        ]
    )
    return sorted(
        str(name) for name in names
        if not str(name).startswith("_")
        and isinstance(getattr(module, str(name), None), type)
    )


def _key_paths(node: Any, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if isinstance(node, Mapping):
        for key, value in node.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            paths.add(path)
            paths.update(_key_paths(value, path))
    return paths


# ---------------------------------------------------------------- commands


def _check_commands(
    commands_file: Path,
    findings: list[dict[str, Any]],
    usage: dict[str, set[str]],
) -> None:
    from okg.substrate.cli.main import (
        _build_parser,
        _maybe_inject_profile_flags,
        _maybe_rewrite_schema_show,
    )

    shared_parser = _build_parser(argv=[])
    for line_number, raw_line in enumerate(
        commands_file.read_text(encoding="utf-8").splitlines(), start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            tokens = shlex.split(line)
        except ValueError as exc:
            findings.append(_finding(
                "cli.line_unreadable",
                UNKNOWN,
                f"line {line_number}: cannot split the command line: {exc}",
                check="commands",
            ))
            continue
        if tokens[:2] == ["uv", "run"]:
            tokens = tokens[2:]
        if tokens[:1] == [shared_parser.prog]:
            tokens = tokens[1:]
        if not tokens:
            continue
        # The same rewrites `okg` applies before it parses (cli/main.py main).
        tokens = _maybe_rewrite_schema_show(tokens)
        verb_index = next(
            (i for i, token in enumerate(tokens) if not token.startswith("-")),
            None,
        )
        if verb_index is not None and tokens[verb_index] == "pact":
            findings.append(_finding(
                "cli.not_checked",
                INFO,
                f"line {line_number}: `okg pact` commands are not part of the "
                "deployment surface and are not checked",
                check="commands",
            ))
            continue
        if any(token in {"-h", "--help", "--version"} for token in tokens):
            findings.append(_finding(
                "cli.not_checked",
                INFO,
                f"line {line_number}: a --help or --version line stops before "
                "the rest is parsed, so it is not checked",
                check="commands",
            ))
            continue
        parser = shared_parser
        if "--profile" in tokens:
            # init/install accept per-profile answer flags only after the
            # profile is loaded, so they get a parser of their own.
            parser = _build_parser(argv=[])
            try:
                _maybe_inject_profile_flags(parser, tokens)
            except Exception as exc:  # noqa: BLE001 - this line only is unknown.
                findings.append(_finding(
                    "cli.line_unreadable",
                    UNKNOWN,
                    f"line {line_number}: the profile flags could not be "
                    f"loaded: {type(exc).__name__}: {exc}",
                    check="commands",
                ))
                continue
        usage["cli"].update(_command_items(parser, tokens))
        captured = io.StringIO()
        try:
            with contextlib.redirect_stderr(captured), contextlib.redirect_stdout(captured):
                parser.parse_args(tokens)
        except SystemExit as exc:
            message = captured.getvalue().strip().splitlines()
            findings.append(_finding(
                "cli.command_refused",
                BLOCKER,
                f"line {line_number}: `okg {' '.join(tokens)}` no longer "
                f"parses: {message[-1] if message else f'exit {exc.code}'}",
                check="commands",
                next_action="see `okg <verb> --help` for this okg",
            ))


def _command_items(
    parser: argparse.ArgumentParser, tokens: list[str],
) -> set[str]:
    """Contract CLI item ids a command line touches: its command and flags."""
    path = [parser.prog]
    node = parser
    index = 0
    while index < len(tokens) and tokens[index].startswith("-"):
        index += 1  # leading globals such as --json
    while index < len(tokens):
        sub = next(
            (a for a in node._actions if isinstance(a, argparse._SubParsersAction)),
            None,
        )
        if sub is None or tokens[index] not in sub.choices:
            break
        node = sub.choices[tokens[index]]
        path.append(tokens[index])
        index += 1
    command = " ".join(path)
    items = {command} if len(path) > 1 else set()
    for token in tokens[index:]:
        if token.startswith("--"):
            items.add(f"{command} {token.split('=', 1)[0]}")
    return items


# ------------------------------------------------------------------ against


def _diff_against(contract_path: Path, usage: dict[str, set[str]]) -> dict[str, Any]:
    old = json.loads(contract_path.read_text(encoding="utf-8"))
    return _diff_contract(old, usage)


def _diff_tested_with(
    manifest_path: Path,
    findings: list[dict[str, Any]],
    usage: dict[str, set[str]],
) -> dict[str, Any] | None:
    """Diff against the release named by `release.tested_with`, offline.

    The release wheel carries every earlier release's contract (decision
    312). Two limits, stated where they bite: v0.2.0 is the first release
    with a contract, so nothing older can be compared; and a wheel built
    outside the release cut carries only the contracts its build found.
    """
    import okg
    from okg.substrate import fastyaml
    from okg.substrate.release_contract import (
        bundled_contract,
        bundled_contract_versions,
    )

    manifest = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    release = manifest.get("release") if isinstance(manifest, Mapping) else None
    tested = release.get("tested_with") if isinstance(release, Mapping) else None
    if tested is None:
        return None
    if not isinstance(tested, str):
        # An unquoted 0.10 is the number 0.1: refuse rather than compare
        # against the wrong release.
        findings.append(_finding(
            "release.tested_with_not_a_string",
            UNKNOWN,
            f"release.tested_with is the {type(tested).__name__} {tested!r}, not a "
            "version string, so the release it names is unknown",
            check="tested_with",
            next_action='quote the version: tested_with: "0.10.0"',
        ))
        return None
    tested = str(tested).removeprefix("v")
    if tested == okg.__version__:
        return None
    old = bundled_contract(tested)
    if old is None:
        bundled = bundled_contract_versions()
        findings.append(_finding(
            "release.tested_with_not_bundled",
            WARNING,
            f"release.tested_with is {tested}, but this okg "
            f"({okg.__version__}) carries no contract for it, so the changes "
            "since then cannot be listed offline. Release wheels carry every "
            "release's contract from v0.2.0 on; a wheel built outside the "
            "release cut carries only what its build found "
            f"(bundled: {', '.join(bundled) or 'none'}).",
            check="tested_with",
            next_action=(
                "pass --against with that release's okg-contract-v<version>.json "
                "asset from its GitHub release"
            ),
        ))
        return None
    return _diff_contract(old, usage)


def _block_touching_changes(
    findings: list[dict[str, Any]], since: Mapping[str, Any], check: str,
) -> None:
    """A breaking change touching what this deployment uses makes it
    incompatible (decision compatexit A, design D3); each is one blocker."""
    for row in since.get("affecting_this_deployment") or ():
        findings.append(_finding(
            "breaking.touches_deployment",
            BLOCKER,
            f"{row['section']}: {row['item']} {row['change']} between okg "
            f"{since['from_version']} and {since['to_version']}, and this "
            "deployment uses it",
            check=check,
            next_action=(
                "apply the migration note in the Breaking changes section of "
                f"the okg release notes after {since['from_version']}, then "
                "rerun compat"
            ),
            details={
                "section": row["section"],
                "item": row["item"],
                "change": row["change"],
            },
        ))


def _diff_contract(old: Mapping[str, Any], usage: dict[str, set[str]]) -> dict[str, Any]:
    from okg.substrate.release_contract import build_contract, diff_contracts

    diff = diff_contracts(old, build_contract())
    affecting: list[dict[str, Any]] = []
    for row in diff["breaking"]:
        used = usage.get(row["section"])
        if used is None:
            continue
        item = row["item"]
        if item in used or (
            row["section"] == "deployment_keys"
            and any(_key_matches(item, path) for path in used)
        ):
            affecting.append(row)
    return {
        "from_version": diff["from_version"],
        "to_version": diff["to_version"],
        "breaking_count": len(diff["breaking"]),
        "affecting_this_deployment": affecting,
        "breaking": diff["breaking"],
    }


def _key_matches(schema_path: str, used_path: str) -> bool:
    schema_parts = schema_path.split(".")
    used_parts = used_path.split(".")
    if len(schema_parts) != len(used_parts):
        return False
    return all(s in {"*", u} for s, u in zip(schema_parts, used_parts))


def format_compat_summary(report: Mapping[str, Any]) -> str:
    """The human-readable form of one report."""
    lines = [
        f"deployment {report.get('deployment')}: {report.get('verdict')} "
        f"with okg {report.get('okg_version')}",
    ]
    for finding in report.get("findings") or ():
        lines.append(
            f"  {finding['severity'].upper()} [{finding['code']}]: {finding['message']}"
        )
        if finding.get("next_action"):
            lines.append(f"    fix: {finding['next_action']}")
    since = report.get("breaking_since")
    if since:
        lines.append(
            f"  {since['breaking_count']} breaking change(s) from okg "
            f"{since['from_version']} to {since['to_version']}; "
            f"{len(since['affecting_this_deployment'])} touch this deployment:"
        )
        for row in since["affecting_this_deployment"]:
            lines.append(f"    {row['section']}: {row['item']} ({row['change']})")
    return "\n".join(lines)


def add_compat_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "deployment",
        help="Deployment name, directory, or deployment.yaml path",
    )
    parser.add_argument(
        "--deployments-root",
        help="Deployment root used when resolving a deployment name",
    )
    parser.add_argument(
        "--commands",
        metavar="FILE",
        help=(
            "File of okg command lines this deployment's scripts run, one per "
            "line; each must still parse against this okg"
        ),
    )
    parser.add_argument(
        "--against",
        metavar="CONTRACT",
        help=(
            "An older release contract JSON; list the breaking changes since "
            "it, those touching this deployment first"
        ),
    )
