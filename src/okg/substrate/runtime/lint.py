"""`okg lint deployments` — boundary enforcement (Phase 2 §6.3).

Walks every deployment's manifest, resolves each ``runtime.workflows``
Python module to a source file, and AST-walks the file (plus its
transitive same-deployment imports) to find any import touching
``okg.substrate.*``. Each match is flagged as ``DEPLOYMENT-001`` with
a file:line citation and a pointer to the SDK helper that should be
used instead.

The boundary contract is **deny-all** on the substrate prefix:
deployment-author code uses ``okg.deployment.*`` only. The SDK
package re-exports everything authors need; the substrate is internal
to OKG itself.

Why a *transitive* check: a deployment workflow module
``deployments/af_pubs/workflows/verifier.py`` might be clean on its
own, but if it imports a sibling helper module
``deployments/af_pubs/workflows/_helpers.py`` which itself imports
from ``okg.substrate.publisher``, the boundary is still broken — the
deployment process loads substrate code. The lint follows imports
within the deployment's package tree.

Limitations (documented; the lint is one defense, not the only one):

  * Dynamic imports (``importlib.import_module(name)`` with a variable)
    are not followed.
  * Imports inside function bodies are detected.
  * Star imports (``from okg.substrate.publisher import *``) are
    detected at the import line.
"""
from __future__ import annotations
from okg.substrate import fastyaml

import ast
import logging
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterator

import okg


__all__ = [
    "BANNED_PREFIX",
    "BOUNDARY_IN_TREE",
    "BOUNDARY_LEGACY_ENRICHER",
    "BOUNDARY_SCANNED",
    "BOUNDARY_UNRESOLVED",
    "CHAT_CHECKED",
    "CHAT_NOT_CHECKABLE",
    "CHAT_NOT_DECLARED",
    "CHAT_VIOLATION",
    "ChatCoverage",
    "DEPLOYMENT_001",
    "DEPLOYMENT_002",
    "DEPLOYMENT_003",
    "EntryCoverage",
    "LEGACY_ENRICHER_SURFACE",
    "LintFinding",
    "LintReport",
    "boundary_entry_coverage",
    "chat_block_coverage",
    "lint_deployments",
    "lint_deployments_report",
]


log = logging.getLogger(__name__)


BANNED_PREFIX = "okg.substrate"
DEPLOYMENT_001 = "DEPLOYMENT-001"
#: A declared workflow / connector / enricher entry whose module could not be
#: resolved to a source file. Stated as a refusal, never a silent skip: an
#: entry lint could not read is an entry lint cannot vouch for, and a clean
#: findings list must not absorb it.
DEPLOYMENT_002 = "DEPLOYMENT-002"
#: A deployment declares ``source_registry:`` but the file is missing or does
#: not parse, so NONE of its connector entries could be enumerated.
DEPLOYMENT_003 = "DEPLOYMENT-003"

#: The installed ``okg`` package directory. A declared module that resolves to
#: a file under it IS the substrate -- the in-tree library connectors and
#: linkers are substrate code, not deployment-author code -- so the deny-all
#: rule does not apply to it. The test is the resolved FILE LOCATION, not the
#: module name: a deployment that ships its own ``okg.`` -named package would
#: pass a name-prefix test while being external code.
_OKG_PACKAGE_ROOT = Path(okg.__file__).resolve().parent

_ENTRY_WORKFLOW = "workflow"
_ENTRY_CONNECTOR = "connector"
_ENTRY_ENRICHER = "enricher"
_ENTRY_REGISTRY = "registry"

#: The entry was resolved outside the okg package and AST-scanned.
BOUNDARY_SCANNED = "scanned"
#: The entry resolved inside the okg package: it IS the substrate, exempt.
BOUNDARY_IN_TREE = "in-tree-exempt"
#: The entry's module could not be resolved to a file. Blocks (DEPLOYMENT-002
#: / DEPLOYMENT-003).
BOUNDARY_UNRESOLVED = "unresolved"
#: An external ``enrichers:`` entry whose substrate imports are confined to
#: :data:`LEGACY_ENRICHER_SURFACE`. Stated, not a finding: see the surface.
BOUNDARY_LEGACY_ENRICHER = "legacy-enricher-surface"

#: The substrate modules an existing database-backed enricher may still import
#: (issue #1793). The connector SDK has no enricher read API yet (#1181 slice
#: 5 owns it), and every enricher the external Archi distribution ships reads
#: the live graph through exactly these modules -- the enricher protocol, the
#: deterministic derived-edge writer, the private ``_chronos`` checkout read,
#: the declarative cross-link linker, and the alias-backend protocol. Until
#: that read API exists, an ``enrichers:`` entry importing only these is
#: recorded in entry coverage as :data:`BOUNDARY_LEGACY_ENRICHER` rather than
#: reported as DEPLOYMENT-001, so the boundary lint does not reject an
#: enricher that has nowhere else to go. The disposition is deliberately
#: narrow: it applies to modules reached ONLY from enricher entries (a module a
#: workflow or connector entry also pulls in stays deny-all), and any other
#: ``okg.substrate.*`` import from an enricher is still a finding. Extending
#: this set is a PACT decision; the migration record lives in
#: ``docs/develop/sdk/index.md``.
LEGACY_ENRICHER_SURFACE: frozenset[str] = frozenset({
    "okg.substrate.alias.protocol",
    "okg.substrate.enrichers.base",
    "okg.substrate.enrichers.derived_edges",
    "okg.substrate.library.linkers._chronos",
    "okg.substrate.library.linkers.declarative",
})


# A short suggestion table for the most common substrate imports.
# Keys are exact module prefixes; values are the SDK helper / surface
# the author should reach for instead. Anything not in this table
# gets a generic message.
_SUGGESTIONS: dict[str, str] = {
    "okg.substrate.publisher": (
        "use `from okg.deployment import emit_node_fact, "
        "emit_edge_fact, update_node_attrs` for mutation; "
        "publisher cycles are substrate-internal"
    ),
    "okg.substrate.runtime.escalation": (
        "use `from okg.deployment import request_escalation`"
    ),
    "okg.substrate.runtime.generation": (
        "use `from okg.deployment import worker_write_generation`"
    ),
    "okg.substrate.runtime.hooks": (
        "use `from okg.deployment import on_generation_published, "
        "on_generation_blocked, on_escalation_decided`"
    ),
    "okg.substrate.catalog": (
        "use `from okg.deployment import declare_narrowings, "
        "declare_sdk_version`"
    ),
    "okg.substrate.library.sources.base": (
        "use `from okg.deployment import NodeFact, EdgeFact, "
        "ProgressMarker, RecordSet, RecordNode, RecordEmission, EdgeKey, "
        "PreflightResult`"
    ),
    "okg.substrate.sources.preflight": (
        "use `from okg.deployment import file_preflight, "
        "credential_preflight, http_preflight, dependency_preflight`"
    ),
    "okg.substrate.sources.redaction": (
        "use `from okg.deployment import redact, redact_endpoint, "
        "redact_mapping`"
    ),
    "okg.substrate.db": (
        "raw DB access is substrate-internal; use the SDK's read "
        "helpers (`read_node`, `query_at_generation`) or mutation "
        "helpers (`emit_node_fact`, etc.)"
    ),
}


@dataclass(frozen=True)
class LintFinding:
    """One boundary violation."""

    code: str
    path: Path
    line: int
    importing_symbol: str
    message: str
    #: The manifest entry that brought the module into the scan -- the
    #: registry key, the enricher class path, or the workflow module.
    entry: str = ""
    #: For ``from X import a, b`` the names imported from ``X``; empty for
    #: ``import X``. Lets a reader tell ``from okg.substrate.library.linkers
    #: import _chronos`` from an import of the package itself.
    imported_names: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LintReport:
    """Findings plus the per-entry coverage the scan actually produced.

    ``entry_coverage`` is the post-scan state: an enricher entry whose
    substrate imports were confined to :data:`LEGACY_ENRICHER_SURFACE` reads
    :data:`BOUNDARY_LEGACY_ENRICHER` here, which the pre-scan
    :func:`boundary_entry_coverage` cannot know.
    """

    findings: list[LintFinding]
    entry_coverage: list[EntryCoverage]


def lint_deployments(
    *,
    deployments_root: Path | str = "deployments",
    repo_root: Path | None = None,
) -> list[LintFinding]:
    """Check every boundary-governed module each deployment declares.

    Three declaration sites, one deny-all rule: ``runtime.workflows``, the
    source registry's connector entries (``module``/``class``), and
    ``enrichers:``. Every entry is resolved to a source file and AST-walked;
    an entry that cannot be resolved is REFUSED (``DEPLOYMENT-002``) rather
    than skipped, and a declared-but-unreadable source registry is refused as
    ``DEPLOYMENT-003``. The one stated exception is
    :data:`LEGACY_ENRICHER_SURFACE`.

    Returns a flat list of :class:`LintFinding` instances. An empty list means
    clean. Callers (CLI) decide on the exit code. Use
    :func:`lint_deployments_report` to read what the scan did with each
    entry, including the in-tree entries it exempted and the enricher entries
    it recorded on the legacy surface.

    Parameters
    ----------
    deployments_root : Path | str, default "deployments"
        Directory containing ``<deployment_name>/deployment.yaml``
        manifests.
    repo_root : Path | None
        Repo root, used to resolve declared Python module paths. Defaults to
        the current working directory.
    """

    return lint_deployments_report(
        deployments_root=deployments_root, repo_root=repo_root,
    ).findings


def lint_deployments_report(
    *,
    deployments_root: Path | str = "deployments",
    repo_root: Path | None = None,
) -> LintReport:
    """:func:`lint_deployments` plus the post-scan entry coverage."""

    findings: list[LintFinding] = []
    coverage = boundary_entry_coverage(
        deployments_root=deployments_root, repo_root=repo_root,
    )
    # Several registry entries routinely share one connector module (a
    # generic web-seed adapter parameterised per source). Walk each distinct
    # module once and name every entry that brought it in, so one violating
    # module is one report rather than forty identical ones.
    scanned: dict[tuple[str, Path], list[EntryCoverage]] = {}
    for cov in coverage:
        if cov.state == BOUNDARY_UNRESOLVED:
            findings.append(_unresolved_finding(cov))
        elif cov.state == BOUNDARY_SCANNED and cov.source_path is not None:
            scanned.setdefault(
                (cov.deployment, cov.source_path), [],
            ).append(cov)

    legacy_by_module: dict[tuple[str, Path], tuple[str, ...]] = {}
    for (deployment, source_path), covs in scanned.items():
        walked = list(
            _walk_module(
                source_path,
                deployment=deployment,
                label=_combined_label(covs),
                module_path=covs[0].module_path,
            )
        )
        # The legacy enricher surface applies to a module reached ONLY through
        # enricher entries. One workflow or connector entry naming the same
        # module keeps every import in it deny-all.
        enricher_only = all(c.kind == _ENTRY_ENRICHER for c in covs)
        if not enricher_only:
            findings.extend(walked)
            continue
        legacy: set[str] = set()
        for finding in walked:
            modules = _legacy_surface_modules(finding)
            if modules:
                legacy.update(modules)
            else:
                findings.append(finding)
        if legacy:
            legacy_by_module[(deployment, source_path)] = tuple(sorted(legacy))

    refined: list[EntryCoverage] = []
    for cov in coverage:
        modules = (
            legacy_by_module.get((cov.deployment, cov.source_path))
            if cov.state == BOUNDARY_SCANNED and cov.source_path is not None
            else None
        )
        if modules:
            cov = replace(
                cov,
                state=BOUNDARY_LEGACY_ENRICHER,
                detail=(
                    "substrate imports confined to the legacy enricher read "
                    "surface, accepted until the enricher SDK read API "
                    f"exists: {', '.join(modules)}"
                ),
            )
        refined.append(cov)
    return LintReport(findings=findings, entry_coverage=refined)


def _combined_label(covs: list[EntryCoverage]) -> str:
    """Name every entry that pulled one module into the scan."""
    labels = sorted({c.label for c in covs})
    if len(labels) <= 3:
        return ", ".join(labels)
    return f"{', '.join(labels[:3])} and {len(labels) - 3} more"


# ---------------------------------------------------------------------------
# Entry coverage: what the boundary scan did with every declared module
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntryCoverage:
    """What the boundary scan could say about one declared entry."""

    deployment: str
    manifest_path: Path
    kind: str
    key: str
    module_path: str
    state: str
    source_path: Path | None = None
    detail: str = ""

    @property
    def label(self) -> str:
        """The operator-facing name of the entry, for finding readback."""
        return f"{self.kind} `{self.key}`"

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "deployment": self.deployment,
            "manifest_path": str(self.manifest_path),
            "kind": self.kind,
            "key": self.key,
            "module_path": self.module_path,
            "state": self.state,
        }
        if self.source_path is not None:
            payload["source_path"] = str(self.source_path)
        if self.detail:
            payload["detail"] = self.detail
        return payload


def boundary_entry_coverage(
    *,
    deployments_root: Path | str = "deployments",
    repo_root: Path | None = None,
) -> list[EntryCoverage]:
    """State, per declared entry, what the boundary scan did with it.

    One record per workflow module, connector registry entry and enricher
    class -- scanned, exempted as in-tree substrate code, or unresolved.
    Coverage is STATED rather than inferred from an empty findings list,
    because "no findings" is produced equally by a clean deployment, a
    deployment whose entries are all exempt, and a deployment whose entries
    lint could not locate.
    """
    root = Path(deployments_root)
    resolved_repo_root = repo_root or Path.cwd()
    if not root.is_absolute():
        root = resolved_repo_root / root

    out: list[EntryCoverage] = []
    for manifest_path in sorted(root.glob("*/deployment.yaml")):
        try:
            with manifest_path.open() as fh:
                manifest = fastyaml.safe_load(fh)
        except Exception as e:  # noqa: BLE001 - reported by chat coverage.
            log.warning(
                "skipping %s: failed to parse manifest (%s)",
                manifest_path, e,
            )
            continue
        if not isinstance(manifest, dict):
            continue
        out.extend(
            _deployment_coverage(
                manifest_path, manifest, repo_root=resolved_repo_root,
            )
        )
    return out


def _deployment_coverage(
    manifest_path: Path, manifest: dict, *, repo_root: Path,
) -> Iterator[EntryCoverage]:
    """Enumerate and resolve one deployment's boundary-governed entries."""
    deployment = manifest_path.parent.name
    deployment_dir = manifest_path.parent

    def _record(
        kind: str, key: str, module_path: str,
    ) -> EntryCoverage:
        source_path = _resolve_module_source(
            module_path,
            repo_root=repo_root,
            deployment_dir=deployment_dir,
        )
        if source_path is None:
            state, detail = BOUNDARY_UNRESOLVED, (
                f"no source file found for module `{module_path}`"
                if module_path else
                "the entry names no importable module"
            )
        elif _is_in_tree(source_path):
            state, detail = BOUNDARY_IN_TREE, (
                "resolves inside the okg package; this IS substrate code"
            )
        else:
            state, detail = BOUNDARY_SCANNED, ""
        return EntryCoverage(
            deployment=deployment,
            manifest_path=manifest_path,
            kind=kind,
            key=key,
            module_path=module_path,
            state=state,
            source_path=source_path,
            detail=detail,
        )

    runtime = manifest.get("runtime") or {}
    for raw in runtime.get("workflows") or []:
        module_path = str(raw).strip()
        yield _record(_ENTRY_WORKFLOW, module_path or "<unnamed>", module_path)

    registry_ref = manifest.get("source_registry")
    if registry_ref:
        registry_path = deployment_dir / str(registry_ref)
        try:
            with registry_path.open() as fh:
                registry = fastyaml.safe_load(fh) or {}
        except Exception as exc:  # noqa: BLE001 - refuse, never skip.
            yield EntryCoverage(
                deployment=deployment,
                manifest_path=manifest_path,
                kind=_ENTRY_REGISTRY,
                key=str(registry_ref),
                module_path="",
                state=BOUNDARY_UNRESOLVED,
                source_path=registry_path,
                detail=(
                    f"{type(exc).__name__}: {exc}"
                ),
            )
            registry = {}
        if not isinstance(registry, dict):
            registry = {}
        sources = registry.get("sources")
        for key, source in sorted((sources or {}).items()):
            if not isinstance(source, dict):
                continue
            module_path = str(source.get("module") or "").strip()
            if not module_path:
                # A `registry_only` entry declares scope, not a runnable
                # factory: the schema forbids module/class on it, so there
                # is no author code to scan and nothing is being skipped.
                continue
            yield _record(_ENTRY_CONNECTOR, str(key), module_path)

    for raw in manifest.get("enrichers") or []:
        cls = raw.get("class") if isinstance(raw, dict) else raw
        key = str(cls or "").strip()
        module_path = key.rsplit(".", 1)[0] if "." in key else ""
        yield _record(_ENTRY_ENRICHER, key or "<unnamed>", module_path)


def _is_in_tree(source_path: Path) -> bool:
    """Whether a resolved module lives inside the installed okg package."""
    try:
        source_path.relative_to(_OKG_PACKAGE_ROOT)
    except ValueError:
        return False
    return True


def _unresolved_finding(coverage: EntryCoverage) -> LintFinding:
    """Refuse an entry the boundary scan could not read."""
    if coverage.kind == _ENTRY_REGISTRY:
        return LintFinding(
            code=DEPLOYMENT_003,
            path=coverage.source_path or coverage.manifest_path,
            line=0,
            importing_symbol=coverage.key,
            entry=coverage.label,
            message=(
                f"deployment `{coverage.deployment}` declares "
                f"source_registry `{coverage.key}` but it could not be read "
                f"({coverage.detail}); its connector entries cannot be "
                "checked against the substrate boundary"
            ),
        )
    return LintFinding(
        code=DEPLOYMENT_002,
        path=coverage.manifest_path,
        line=0,
        importing_symbol=coverage.module_path,
        entry=coverage.label,
        message=(
            f"deployment `{coverage.deployment}` declares {coverage.label} "
            f"but {coverage.detail}, so its imports cannot be checked "
            "against the substrate boundary"
        ),
    )


# ---------------------------------------------------------------------------
# chat: block coverage (PACT chat-manifest-block)
# ---------------------------------------------------------------------------

#: A chat block was validated and is clean.
CHAT_CHECKED = "checked"
#: A chat block was validated and BROKE a rule. The `checked` state's failure
#: outcome, kept a distinct value so a portfolio's passing count can never
#: absorb a violation.
CHAT_VIOLATION = "violation"
#: The deployment declares no chat block. NOT a passing chat check — stating
#: this distinctly is the whole point: a portfolio in which nobody declares a
#: chat block must never report as a portfolio of passing chat checks.
CHAT_NOT_DECLARED = "not-declared"
#: The manifest could not be parsed, or the validation machinery could not be
#: resolved, so nothing about its chat block is known. Exits non-zero and is
#: never counted toward passing chat checks: `lint_deployments` historically
#: warned and CONTINUED on a parse error, which reported a broken portfolio as
#: clean.
CHAT_NOT_CHECKABLE = "not-checkable"


@dataclass(frozen=True)
class ChatCoverage:
    """What lint could say about one deployment's chat block."""

    deployment: str
    manifest_path: Path
    state: str
    rule: str | None = None
    message: str | None = None
    offending_value: object = None

    @property
    def blocks_exit(self) -> bool:
        """True when this state must make `okg lint deployments` exit non-zero."""
        return self.state in (CHAT_VIOLATION, CHAT_NOT_CHECKABLE)

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "deployment": self.deployment,
            "manifest_path": str(self.manifest_path),
            "state": self.state,
        }
        if self.rule:
            payload["rule"] = self.rule
        if self.message:
            payload["message"] = self.message
        if self.offending_value is not None:
            payload["offending_value"] = self.offending_value
        return payload


def chat_block_coverage(
    *,
    deployments_root: Path | str = "deployments",
    repo_root: Path | None = None,
) -> list[ChatCoverage]:
    """State the chat-block coverage for every deployment under the root.

    One record per deployment, always — coverage is STATED, never assumed.
    Callers read `state` rather than inferring health from an empty findings
    list, because "no chat findings" is produced equally by a clean block, an
    absent block, and a manifest lint could not read.
    """
    root = Path(deployments_root)
    if not root.is_absolute():
        root = (repo_root or Path.cwd()) / root

    out: list[ChatCoverage] = []
    for manifest_path in sorted(root.glob("*/deployment.yaml")):
        out.append(
            _chat_coverage_for(
                manifest_path, deployment=manifest_path.parent.name,
            )
        )
    return out


def _chat_coverage_for(
    manifest_path: Path, *, deployment: str,
) -> ChatCoverage:
    try:
        from okg.substrate.deployments.chat_block import (
            ChatManifestError,
            parse_chat_block,
            resolve_theme_accent,
        )
    except Exception as exc:  # noqa: BLE001 - unresolvable module: say so.
        return ChatCoverage(
            deployment=deployment,
            manifest_path=manifest_path,
            state=CHAT_NOT_CHECKABLE,
            rule="chat.module_unresolved",
            message=(
                "the chat-block validation module could not be imported: "
                f"{type(exc).__name__}: {exc}"
            ),
        )

    try:
        with manifest_path.open() as fh:
            raw = fastyaml.safe_load(fh)
    except Exception as exc:  # noqa: BLE001 - unparseable: not-checkable.
        return ChatCoverage(
            deployment=deployment,
            manifest_path=manifest_path,
            state=CHAT_NOT_CHECKABLE,
            rule="chat.manifest_unparseable",
            message=(
                f"deployment.yaml could not be parsed: "
                f"{type(exc).__name__}: {exc}"
            ),
        )
    if not isinstance(raw, dict):
        return ChatCoverage(
            deployment=deployment,
            manifest_path=manifest_path,
            state=CHAT_NOT_CHECKABLE,
            rule="chat.manifest_not_mapping",
            message=(
                "deployment.yaml is not a mapping, so its chat block cannot "
                f"be read (observed {type(raw).__name__})"
            ),
        )

    try:
        block = parse_chat_block(raw, manifest_path=manifest_path)
        resolve_theme_accent(raw, manifest_path=manifest_path)
    except ChatManifestError as exc:
        data = getattr(exc, "data", {}) or {}
        return ChatCoverage(
            deployment=deployment,
            manifest_path=manifest_path,
            state=CHAT_VIOLATION,
            rule=f"{exc.code}:{data.get('path', 'chat')}",
            message=str(exc),
            offending_value=_offending_value(data),
        )
    except Exception as exc:  # noqa: BLE001 - unexpected: never a clean pass.
        return ChatCoverage(
            deployment=deployment,
            manifest_path=manifest_path,
            state=CHAT_NOT_CHECKABLE,
            rule="chat.unexpected_error",
            message=f"{type(exc).__name__}: {exc}",
        )

    if block is None:
        return ChatCoverage(
            deployment=deployment,
            manifest_path=manifest_path,
            state=CHAT_NOT_DECLARED,
            message="deployment.yaml declares no chat: block",
        )
    return ChatCoverage(
        deployment=deployment,
        manifest_path=manifest_path,
        state=CHAT_CHECKED,
        message="chat: block validated with no findings",
    )


def _offending_value(data: dict) -> object:
    """The value that broke the rule, for the operator-facing report."""
    for key in (
        "requested", "unknown", "observed", "missing", "extra", "census_bound",
    ):
        if data.get(key) is not None:
            return data[key]
    if data.get("chat_accent") is not None:
        return {
            "viz_accent": data.get("viz_accent"),
            "chat_accent": data.get("chat_accent"),
        }
    return None


# ---------------------------------------------------------------------------
# Module walker
# ---------------------------------------------------------------------------


def _walk_module(
    source_path: Path,
    *,
    deployment: str,
    label: str,
    module_path: str,
) -> Iterator[LintFinding]:
    """Visit a module's source plus its same-package transitive imports.

    Yields one :class:`LintFinding` per banned import line. The scan is
    static: the file is parsed with :mod:`ast` and never imported, so a
    module whose import-time code would fail (or run) is still checked.
    """
    visited: set[Path] = set()
    queue: list[Path] = [source_path]

    while queue:
        current = queue.pop()
        if current in visited:
            continue
        visited.add(current)

        try:
            tree = _parse_source(current)
        except (SyntaxError, OSError, UnicodeDecodeError) as e:
            # Unreadable source is a refusal, not a skip: lint cannot vouch
            # for a file it could not parse.
            yield LintFinding(
                code=DEPLOYMENT_002,
                path=current,
                line=getattr(e, "lineno", 0) or 0,
                importing_symbol=module_path,
                entry=label,
                message=(
                    f"{label} in deployment "
                    f"`{deployment}` could not be parsed "
                    f"({type(e).__name__}: {e}), so its imports cannot be "
                    "checked against the substrate boundary"
                ),
            )
            continue

        for finding in _find_banned_imports(
            current, tree, deployment=deployment, label=label,
        ):
            yield finding

        # Follow same-deployment sibling imports for transitive analysis.
        # Anything outside the deployment's tree we don't follow (it's
        # outside the deployment author's control). The okg package is
        # never followed: when an external distribution is installed as a
        # wheel, its package root is site-packages, which also holds okg,
        # and okg's own internal imports are not the deployment's (#2819).
        # The deployment's import of okg.substrate is still reported at the
        # importing line above.
        for sibling_path in _resolve_sibling_imports(tree, current):
            if sibling_path not in visited and not _is_in_tree(
                sibling_path.resolve(),
            ):
                queue.append(sibling_path)


def _resolve_module_source(
    module_path: str,
    *,
    repo_root: Path,
    deployment_dir: Path | None = None,
) -> Path | None:
    """Resolve a dotted module path to a ``.py`` file WITHOUT importing it.

    ``importlib.util.find_spec`` is deliberately NOT used: it executes every
    parent package's ``__init__.py``, and a lint that runs deployment-author
    code in order to decide whether that code is allowed to run is not a
    static check. Resolution is filesystem-only, in this order:

      1. the deployment's own directory -- the production layout, where
         ``deployments/beaver-okg/deployment.yaml`` names
         ``beaver_okg.source`` at
         ``deployments/beaver-okg/beaver_okg/source.py``;
      2. ``<repo_root>``, ``<repo_root>/src`` and ``<repo_root>/deployments``;
      3. the directory holding the installed ``okg`` package, so the in-tree
         library connectors resolve;
      4. ``sys.path``, covering an installed deployment package.
    """
    if not module_path:
        return None
    rel = module_path.replace(".", "/")
    roots: list[Path] = []
    if deployment_dir is not None:
        roots.append(deployment_dir)
    roots.extend((
        repo_root,
        repo_root / "src",
        repo_root / "deployments",
        _OKG_PACKAGE_ROOT.parent,
    ))
    roots.extend(Path(entry) for entry in sys.path if entry)
    for root in roots:
        for candidate in (
            root / f"{rel}.py",
            root / rel / "__init__.py",
        ):
            if candidate.is_file():
                return candidate.resolve()
    return None


def _parse_source(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _find_banned_imports(
    path: Path,
    tree: ast.Module,
    *,
    deployment: str,
    label: str,
) -> Iterator[LintFinding]:
    """Walk the AST yielding one finding per import that touches
    ``okg.substrate.*``. Handles both ``import X`` and ``from X
    import Y``."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_banned(alias.name):
                    yield _build_finding(
                        path, node.lineno, alias.name, deployment, label,
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if _is_banned(module):
                yield _build_finding(
                    path, node.lineno, module, deployment, label,
                    imported_names=tuple(alias.name for alias in node.names),
                )


def _is_banned(module_name: str) -> bool:
    return (
        module_name == BANNED_PREFIX
        or module_name.startswith(BANNED_PREFIX + ".")
    )


def _build_finding(
    path: Path, line: int, module_name: str, deployment: str, label: str,
    *, imported_names: tuple[str, ...] = (),
) -> LintFinding:
    # Find the most-specific matching suggestion.
    suggestion = None
    for prefix, hint in _SUGGESTIONS.items():
        if module_name.startswith(prefix):
            suggestion = hint
            break
    if suggestion is None:
        suggestion = (
            "deployment code must use only `okg.deployment.*`; the "
            "substrate is internal"
        )
    return LintFinding(
        code=DEPLOYMENT_001,
        path=path,
        line=line,
        importing_symbol=module_name,
        entry=label,
        message=(
            f"{label} in deployment `{deployment}` "
            f"imports `{module_name}`; {suggestion}"
        ),
        imported_names=imported_names,
    )


def _legacy_surface_modules(finding: LintFinding) -> tuple[str, ...] | None:
    """The surface modules one DEPLOYMENT-001 import resolves to, or None.

    ``from okg.substrate.enrichers.base import EnrichResult`` is the surface
    module itself; ``from okg.substrate.library.linkers import _chronos``
    names the parent package and reaches the surface through the imported
    name, so every imported name must resolve into the surface for the
    import to count. Anything else -- including a bare import of the parent
    package -- is not on the surface.
    """
    if finding.code != DEPLOYMENT_001:
        return None
    if finding.importing_symbol in LEGACY_ENRICHER_SURFACE:
        return (finding.importing_symbol,)
    if not finding.imported_names:
        return None
    modules = tuple(
        f"{finding.importing_symbol}.{name}" for name in finding.imported_names
    )
    if all(module in LEGACY_ENRICHER_SURFACE for module in modules):
        return modules
    return None


def _resolve_sibling_imports(
    tree: ast.Module, source_path: Path,
) -> Iterator[Path]:
    """Yield same-deployment sibling .py paths the source imports.

    A "sibling" is any module under the source's package tree. The
    lint follows these so transitive substrate imports through a
    helper module still flag.

    Handles three forms:

      * ``import a.b.c`` — yield <package_root>/a/b/c.py if it exists.
      * ``from a.b import c`` — yield <package_root>/a/b/c.py.
      * ``from .helper import x`` / ``from . import helper`` —
        relative; resolve relative to source_path's directory.
    """
    package_root = _detect_package_root(source_path)
    if package_root is None:
        return

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                # Relative import: walk up `level` directories from
                # source_path, then descend into the module path.
                base = source_path.parent
                for _ in range(node.level - 1):
                    base = base.parent
                if node.module:
                    # `from .x.y import z` -> x/y.py + x/y/z.py
                    submodule = base / node.module.replace(".", "/")
                    for candidate in (
                        submodule.with_suffix(".py"),
                        submodule / "__init__.py",
                    ):
                        if candidate.is_file():
                            yield candidate
                    # Also follow each imported name as a possible
                    # submodule (e.g. `from .x import y` where y is a
                    # module).
                    for alias in node.names:
                        c = (submodule / f"{alias.name}.py")
                        if c.is_file():
                            yield c
                else:
                    # `from . import helper`: each alias is a submodule.
                    for alias in node.names:
                        c = base / f"{alias.name}.py"
                        if c.is_file():
                            yield c
            else:
                module = node.module or ""
                sibling = _module_to_path(module, package_root)
                if sibling is not None and sibling.is_file():
                    yield sibling
        elif isinstance(node, ast.Import):
            for alias in node.names:
                sibling = _module_to_path(alias.name, package_root)
                if sibling is not None and sibling.is_file():
                    yield sibling


def _detect_package_root(source_path: Path) -> Path | None:
    """Walk up from source_path looking for the package root.

    A package root is the directory containing the first __init__.py
    that has no parent __init__.py — i.e. the topmost __init__.py in
    a chain.
    """
    cur = source_path.parent
    if not (cur / "__init__.py").is_file():
        return None
    # Walk up while the parent also has an __init__.py.
    while (cur.parent / "__init__.py").is_file():
        cur = cur.parent
    return cur.parent  # the directory holding the topmost package


def _module_to_path(
    module: str, package_root: Path,
) -> Path | None:
    """Map a dotted module to a .py path under package_root, or None
    if it doesn't fall under that root."""
    if not module:
        return None
    rel = module.replace(".", "/") + ".py"
    candidate = package_root / rel
    return candidate if candidate.exists() else None
