"""Security scanner artifact import for quality evidence."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._path_scope import (
    glob_matches_path,
    path_scope_requested,
    resolve_scoped_paths,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceHealth,
    SourceRun,
)


__all__ = ["SecurityAnalysisSource"]

CODE_QUALITY_SCHEMA = "okg.code_quality.findings.v1"

SECRET_FINDING_ALLOWED_ATTRS = frozenset({
    "finding_id",
    "repo",
    "tool",
    "rule_id",
    "path",
    "start_line",
    "end_line",
    "fingerprint",
    "status",
    "baseline_state",
    "suppression",
    "entropy",
    "redacted_context",
    "commit",
    "ref",
    "artifact",
    "artifact_hash",
    "observed_at",
    "text",
})

_FINDING_SUBTYPES = frozenset({
    "code_quality_finding",
    "security_finding",
    "secret_finding",
    "dependency_vulnerability",
})


class _ArtifactParseError(ValueError):
    """A configured artifact was readable but not syntactically valid."""


@dataclass(frozen=True)
class _SuppressionRule:
    rule_id: str
    reason: str
    subtypes: tuple[str, ...] = ()
    tools: tuple[str, ...] = ()
    scanner_rule_ids: tuple[str, ...] = ()
    path_globs: tuple[str, ...] = ()
    start_lines: tuple[int, ...] = ()
    fingerprints: tuple[str, ...] = ()
    artifact_globs: tuple[str, ...] = ()

    def has_matcher(self) -> bool:
        return bool(
            self.subtypes
            or self.tools
            or self.scanner_rule_ids
            or self.path_globs
            or self.start_lines
            or self.fingerprints
            or self.artifact_globs
        )


def _sha(value: str, n: int = 16) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:n]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


@dataclass
class SecurityAnalysisSource:
    """Import local security-analysis artifacts.

    Scanner execution is intentionally absent from this adapter. It
    imports artifacts produced by external tools and emits evidence
    nodes that later deterministic linkers can attach to files,
    symbols, manifests, dependencies, sessions, or criteria.
    """

    repo: str
    repo_root: Path | str
    artifact_paths: Sequence[Path | str] = ()
    artifact_globs: Sequence[str] = ()
    finding_exclude_globs: Sequence[str] = ()
    suppression_paths: Sequence[Path | str] = ()
    suppression_globs: Sequence[str] = ()
    max_artifacts: int = 200

    name: str = "security_analysis"
    # The registry contract classifies this filesystem walk as a
    # discovery_crawl. Keeping the adapter on an ad-hoc profile prevented the
    # runner from selecting its authoritative scope-complete replay mode.
    profile: str = "discovery_crawl"
    probe_short_circuit_safe = True
    # Incremental-source contract (issue #323): content-hash floor over the
    # imported (CI-generated, not git-tracked) artifacts; the finding policy
    # (suppression rules + exclude globs) folds into the token config exactly as
    # the adapter folds finding_policy_hash into each fact's revision.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self._suppression_rules = _load_suppression_rules(
            self.repo_root,
            self.suppression_paths,
            self.suppression_globs,
        )
        self._finding_policy_hash = _finding_policy_hash(
            self.finding_exclude_globs,
            self._suppression_rules,
        )
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        return ContentHashProbe(
            content_items=lambda: [
                (_rel_or_name(Path(self.repo_root), p), p)
                for p in self._artifact_files()
            ],
            config={
                "repo": self.repo,
                "finding_policy_hash": self._finding_policy_hash,
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        prior = dict(cursor or {})
        facts: list[NodeFact | EdgeFact] = []
        applied_item_hashes: dict[str, str] = {}
        enumeration_failure_count = 0
        scoped = path_scope_requested(sync_scope)
        paths = self._artifact_files(sync_scope if scoped else None)
        for path in paths:
            try:
                raw_bytes = path.read_bytes()
                applied_item_hashes[
                    _rel_or_name(Path(self.repo_root), path)
                ] = hashlib.sha256(raw_bytes).hexdigest()
                facts.extend(
                    self._facts_for_artifact(path, raw_bytes=raw_bytes)
                )
            except (OSError, _ArtifactParseError):
                enumeration_failure_count += 1
        completed_scope = (
            not scoped and mode in {"reconcile", "scope_complete"}
            and enumeration_failure_count == 0
        )
        self.last_run_stats = {
            "artifact_count": len(paths),
            "enumeration_failure_count": enumeration_failure_count,
        }
        deduped_facts = _dedupe_facts(facts)
        record_set = (
            _record_set_for_artifact_facts(deduped_facts)
            if completed_scope else None
        )
        return SourceRun(
            facts=iter(deduped_facts),
            completed_scope=completed_scope,
            next_cursor=(
                {"run_id": run_id}
                if enumeration_failure_count == 0 else prior
            ),
            applied_raw_token=(
                self.change_probe.token_from_item_hashes(applied_item_hashes)
                if not scoped and enumeration_failure_count == 0
                else None
            ),
            applied_token_scope=(
                "global"
                if not scoped and enumeration_failure_count == 0
                else "unavailable"
            ),
            applied_token_authority=(
                "exact"
                if not scoped and enumeration_failure_count == 0
                else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set,
            health=(
                None
                if enumeration_failure_count == 0
                else SourceHealth(
                    status="acquire_failed",
                    mode="local",
                    reason=(
                        "Security artifact enumeration was incomplete: "
                        f"{enumeration_failure_count} configured artifact(s) "
                        "unreadable or malformed"
                    ),
                )
            ),
        )

    def _artifact_files(
        self,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> list[Path]:
        if sync_scope is None:
            return _configured_artifacts(
                Path(self.repo_root),
                self.artifact_paths,
                self.artifact_globs,
                self.max_artifacts,
            )
        candidates = resolve_scoped_paths(
            sync_scope,
            roots=self._scope_roots(),
            suffixes=[".json", ".sarif", ".xml"],
        )
        return sorted({
            path for path in candidates
            if _configured_artifact_matches(
                Path(self.repo_root),
                path,
                self.artifact_paths,
                self.artifact_globs,
            )
        })

    def _scope_roots(self) -> tuple[Path, ...]:
        roots = [Path(self.repo_root)]
        for raw in self.artifact_paths:
            roots.append(_resolve_configured_path(Path(self.repo_root), raw).parent)
        return tuple(roots)

    def _facts_for_artifact(
        self,
        path: Path,
        *,
        raw_bytes: Optional[bytes] = None,
    ) -> list[NodeFact | EdgeFact]:
        if _is_quality_sarif_with_normalized_sibling(path):
            return []
        rel_artifact = _rel_or_name(Path(self.repo_root), path)
        if raw_bytes is None:
            raw_bytes = path.read_bytes()
        text = raw_bytes.decode("utf-8", errors="replace")
        revision = {
            "artifact": rel_artifact,
            "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "finding_policy_hash": self._finding_policy_hash,
            **_artifact_lineage(Path(self.repo_root), path),
        }
        if path.suffix.lower() == ".xml":
            return _apply_finding_policy(
                _xml_artifact_facts(
                    self.repo,
                    Path(self.repo_root),
                    path,
                    rel_artifact,
                    revision,
                    raw_bytes=raw_bytes,
                ),
                finding_exclude_globs=self.finding_exclude_globs,
                suppression_rules=self._suppression_rules,
            )
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as exc:
            raise _ArtifactParseError(
                f"malformed JSON security artifact: {path}"
            ) from exc
        return _apply_finding_policy(
            _json_artifact_facts(
                self.repo,
                Path(self.repo_root),
                path,
                rel_artifact,
                doc,
                revision,
            ),
            finding_exclude_globs=self.finding_exclude_globs,
            suppression_rules=self._suppression_rules,
        )


def _json_artifact_facts(
    repo: str,
    repo_root: Path,
    path: Path,
    artifact: str,
    doc: Any,
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    if not isinstance(doc, dict):
        if isinstance(doc, list):
            return _gitleaks_facts(repo, repo_root, artifact, doc, revision)
        return []
    if doc.get("schema") == CODE_QUALITY_SCHEMA:
        return _code_quality_json_facts(repo, repo_root, artifact, doc, revision)
    if _is_sarif(doc):
        return _sarif_facts(repo, repo_root, artifact, doc, revision)
    if doc.get("bomFormat") == "CycloneDX" or "components" in doc:
        return _cyclonedx_json_facts(repo, artifact, doc, revision)
    if "spdxVersion" in doc or "packages" in doc and "SPDXID" in doc:
        return _spdx_json_facts(repo, artifact, doc, revision)
    if "results" in doc and _looks_like_bandit(doc):
        return _bandit_facts(repo, repo_root, artifact, doc, revision)
    if "results" in doc and _looks_like_semgrep(doc):
        return _semgrep_facts(repo, repo_root, artifact, doc, revision)
    if "findings" in doc and _looks_like_gitleaks(doc):
        return _gitleaks_facts(repo, repo_root, artifact, doc.get("findings") or [], revision)
    if "dependencies" in doc and _looks_like_pip_audit(doc):
        return _pip_audit_facts(repo, artifact, doc, revision)
    if "results" in doc and _looks_like_osv(doc):
        return _osv_facts(repo, artifact, doc, revision)
    if "Results" in doc and _looks_like_trivy(doc):
        return _trivy_facts(repo, repo_root, artifact, doc, revision)
    return []


def _xml_artifact_facts(
    repo: str,
    repo_root: Path,
    path: Path,
    artifact: str,
    revision: dict[str, Any],
    *,
    raw_bytes: Optional[bytes] = None,
) -> list[NodeFact | EdgeFact]:
    try:
        from defusedxml import ElementTree as ET
    except ImportError as exc:
        raise SubstrateError(
            "SecurityAnalysisSource requires defusedxml for SBOM XML import",
            fix_hint="install okg[quality] or configure only JSON artifacts",
        ) from exc
    try:
        root = (
            ET.fromstring(raw_bytes)
            if raw_bytes is not None
            else ET.parse(path).getroot()
        )
    except Exception as exc:  # noqa: BLE001 — normalize parser failures.
        raise _ArtifactParseError(
            f"malformed XML security artifact: {path}"
        ) from exc
    if _local_tag(root.tag).lower() != "bom":
        return []
    components = []
    for component in root.findall(".//{*}component"):
        name = component.findtext("{*}name") or ""
        version = component.findtext("{*}version") or ""
        purl = component.findtext("{*}purl") or ""
        if name or purl:
            components.append({
                "name": name,
                "version": version,
                "purl": purl,
            })
    generated_at = (
        root.findtext(".//{*}metadata/{*}timestamp")
        or _artifact_lineage_timestamp(revision)
    )
    return [_sbom_node(
        repo=repo,
        artifact=artifact,
        fmt="cyclonedx_xml",
        generated_at=generated_at,
        tool="cyclonedx",
        tool_version="",
        component_count=len(components),
        components=components[:100],
        revision=revision,
    )]


def _sarif_facts(
    repo: str,
    repo_root: Path,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    try:
        from sarif_pydantic import Sarif

        doc = Sarif.model_validate(doc).model_dump(by_alias=True, exclude_none=True)
    except Exception:  # noqa: BLE001 — sarif_pydantic is optional; fall through to the raw-dict parse path below.
        if "runs" not in doc:
            return []
    out: list[NodeFact | EdgeFact] = []
    for run in doc.get("runs", []) or []:
        tool = _sarif_tool_name(run)
        driver = ((run.get("tool") or {}).get("driver") or {})
        tool_version = str(driver.get("semanticVersion") or driver.get("version") or "")
        rules = _sarif_rules(driver)
        code_quality_run = _is_code_quality_sarif(artifact, run)
        for result in run.get("results", []) or []:
            rule_id = str(result.get("ruleId") or (result.get("rule") or {}).get("id") or "")
            loc = _first_sarif_location(result.get("locations"), repo_root)
            if not loc.get("path"):
                continue
            rule = rules.get(rule_id, {})
            message = _sarif_message(result.get("message")) or str(rule.get("name") or "")
            fingerprint = _sarif_fingerprint(result) or _safe_fingerprint(
                "",
                secret="",
                fallback_parts=[artifact, rule_id, loc.get("path"), loc.get("start_line"), message],
            )
            if _sarif_result_is_code_quality(code_quality_run, result, rule):
                attrs = _code_quality_attrs(
                    repo=repo,
                    tool=tool,
                    tool_version=tool_version,
                    rule_id=rule_id,
                    rule_name=_sarif_rule_name(rule, rule_id),
                    language=_sarif_language(artifact, run),
                    category=_sarif_quality_category(rule),
                    severity=_sarif_quality_severity(result, rule),
                    level=str(result.get("level") or ""),
                    status=_sarif_status(result),
                    path=str(loc.get("path")),
                    start_line=_int_or_one(loc.get("start_line")),
                    end_line=_int_or_one(loc.get("end_line") or loc.get("start_line")),
                    start_column=_int_or_zero(loc.get("start_column")) or None,
                    end_column=_int_or_zero(loc.get("end_column")) or None,
                    message=message,
                    fingerprint=fingerprint,
                    baseline_state=str(result.get("baselineState") or ""),
                    help_uri=str(rule.get("helpUri") or ""),
                    sha="",
                    ref="",
                    branch="",
                    workflow_name="",
                    workflow_run_id="",
                    run_attempt="",
                    source_sarif=artifact,
                    artifact=artifact,
                )
                node = _code_quality_node(attrs, revision)
                out.append(node)
                out.extend(_code_quality_edges(repo, artifact, attrs, revision))
                continue
            attrs = _security_attrs(
                repo=repo,
                tool=tool,
                tool_version=tool_version,
                rule_id=rule_id,
                rule_version=str(rule.get("version") or ""),
                severity=_sarif_severity(result, rule),
                confidence=str(result.get("level") or "medium"),
                path=str(loc.get("path")),
                start_line=_int_or_one(loc.get("start_line")),
                end_line=_int_or_one(loc.get("end_line") or loc.get("start_line")),
                status=_sarif_status(result),
                message=message,
                cwe=_sarif_cwe(rule),
                owasp=_sarif_owasp(rule),
                help_uri=str(rule.get("helpUri") or ""),
                fingerprint=fingerprint,
                baseline_state=str(result.get("baselineState") or ""),
                suppression=_sarif_suppression(result),
                artifact=artifact,
            )
            if _is_secret_tool_or_rule(tool, rule_id, attrs):
                out.append(_secret_node_from_security_attrs(attrs, revision))
            else:
                out.append(NodeFact(
                    attrs["finding_id"],
                    "security_finding",
                    _with_artifact_metadata(attrs, revision),
                    {"repo": repo, "artifact": artifact, "finding_id": attrs["finding_id"]},
                    revision,
                ))
    return out


def _semgrep_facts(
    repo: str,
    repo_root: Path,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    tool_version = str(doc.get("version") or doc.get("semgrep_version") or "")
    out: list[NodeFact] = []
    for result in doc.get("results", []) or []:
        rel_path = _rel_from_scanner_path(repo_root, result.get("path") or "")
        if not rel_path:
            continue
        extra = result.get("extra") or {}
        start = result.get("start") or {}
        end = result.get("end") or {}
        rule_id = str(result.get("check_id") or extra.get("fingerprint") or "")
        attrs = _security_attrs(
            repo=repo,
            tool="semgrep",
            tool_version=tool_version,
            rule_id=rule_id,
            rule_version=str((extra.get("metadata") or {}).get("rule-version") or ""),
            severity=_normalize_severity(extra.get("severity")),
            confidence=str((extra.get("metadata") or {}).get("confidence") or "medium"),
            path=rel_path,
            start_line=_int_or_one(start.get("line")),
            end_line=_int_or_one(end.get("line") or start.get("line")),
            status=str(extra.get("validation_state") or "open"),
            message=str(extra.get("message") or ""),
            cwe=_list_or_str((extra.get("metadata") or {}).get("cwe")),
            owasp=_list_or_str((extra.get("metadata") or {}).get("owasp")),
            help_uri=str((extra.get("metadata") or {}).get("source") or ""),
            fingerprint=str(extra.get("fingerprint") or ""),
            baseline_state=str(extra.get("is_ignored") or ""),
            suppression=str(extra.get("ignore_reason") or ""),
            artifact=artifact,
        )
        out.append(NodeFact(
            attrs["finding_id"],
            "security_finding",
            _with_artifact_metadata(attrs, revision),
            {"repo": repo, "artifact": artifact, "finding_id": attrs["finding_id"]},
            revision,
        ))
    return out


def _bandit_facts(
    repo: str,
    repo_root: Path,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact] = []
    tool_version = str(((doc.get("generated_at") or "") and doc.get("bandit_version")) or "")
    for result in doc.get("results", []) or []:
        rel_path = _rel_from_scanner_path(repo_root, result.get("filename") or "")
        if not rel_path:
            continue
        line = _int_or_one(result.get("line_number"))
        attrs = _security_attrs(
            repo=repo,
            tool="bandit",
            tool_version=tool_version,
            rule_id=str(result.get("test_id") or result.get("test_name") or ""),
            rule_version="",
            severity=_normalize_severity(result.get("issue_severity")),
            confidence=str(result.get("issue_confidence") or "medium").lower(),
            path=rel_path,
            start_line=line,
            end_line=line,
            status="open",
            message=str(result.get("issue_text") or ""),
            cwe=_bandit_cwe(result),
            owasp="",
            help_uri=str(result.get("more_info") or ""),
            fingerprint=str(result.get("fingerprint") or ""),
            baseline_state="",
            suppression="",
            artifact=artifact,
        )
        out.append(NodeFact(
            attrs["finding_id"],
            "security_finding",
            _with_artifact_metadata(attrs, revision),
            {"repo": repo, "artifact": artifact, "finding_id": attrs["finding_id"]},
            revision,
        ))
    return out


def _gitleaks_facts(
    repo: str,
    repo_root: Path,
    artifact: str,
    findings: Any,
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    if not isinstance(findings, list):
        return []
    out: list[NodeFact] = []
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        rel_path = _rel_from_scanner_path(
            repo_root,
            finding.get("File") or finding.get("file") or finding.get("Path") or "",
        )
        if not rel_path:
            continue
        rule_id = str(finding.get("RuleID") or finding.get("rule") or finding.get("Description") or "secret")
        fingerprint = str(finding.get("Fingerprint") or finding.get("fingerprint") or "")
        line = _int_or_one(finding.get("StartLine") or finding.get("line"))
        secret_value = str(finding.get("Secret") or finding.get("secret") or "")
        match = str(finding.get("Match") or finding.get("match") or "")
        attrs = _secret_attrs(
            repo=repo,
            tool="gitleaks",
            rule_id=rule_id,
            path=rel_path,
            start_line=line,
            end_line=_int_or_one(finding.get("EndLine") or line),
            fingerprint=_safe_fingerprint(
                fingerprint,
                secret=secret_value,
                fallback_parts=[artifact, rel_path, line, rule_id],
            ),
            status=str(finding.get("Status") or "open"),
            entropy=_float_or_zero(finding.get("Entropy") or finding.get("entropy")),
            redacted_context=_redact_context(match, secret_value),
            commit=str(finding.get("Commit") or finding.get("commit") or ""),
            ref=str(finding.get("Branch") or finding.get("branch") or finding.get("Ref") or finding.get("ref") or ""),
            artifact=artifact,
        )
        out.append(NodeFact(
            attrs["finding_id"],
            "secret_finding",
            _with_artifact_metadata(attrs, revision),
            {"repo": repo, "artifact": artifact, "finding_id": attrs["finding_id"]},
            revision,
        ))
    return out


def _pip_audit_facts(
    repo: str,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    for dep in doc.get("dependencies", []) or []:
        if not isinstance(dep, dict):
            continue
        name = str(dep.get("name") or "")
        version = str(dep.get("version") or dep.get("installed_version") or "")
        for vuln in dep.get("vulns", []) or dep.get("vulnerabilities", []) or []:
            if not isinstance(vuln, dict):
                continue
            attrs = _vulnerability_attrs(
                vuln_id=str(vuln.get("id") or vuln.get("vuln_id") or ""),
                package_name=name,
                ecosystem="pypi",
                installed_version=version,
                severity=_normalize_severity(vuln.get("severity") or vuln.get("cvss_severity") or "unknown"),
                source="pip-audit",
                status=str(vuln.get("status") or "open"),
                fix_versions=_list(vuln.get("fix_versions") or vuln.get("fixed_versions")),
                aliases=_list(vuln.get("aliases")),
                cvss=_cvss_value(vuln),
                published_at=str(vuln.get("published_at") or vuln.get("published") or ""),
                modified_at=str(vuln.get("modified_at") or vuln.get("modified") or ""),
                summary=str(vuln.get("description") or vuln.get("summary") or ""),
                artifact=artifact,
            )
            out.append(NodeFact(
                attrs["vuln_node_id"],
                "dependency_vulnerability",
                _with_artifact_metadata(_without_private(attrs), revision),
                {"repo": repo, "artifact": artifact, "vuln_id": attrs["vuln_id"]},
                revision,
            ))
            out.append(_dependency_edge(repo, artifact, attrs, revision))
    return out


def _osv_facts(
    repo: str,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    for result in doc.get("results", []) or []:
        packages = result.get("packages") or []
        if isinstance(packages, dict):
            packages = [packages]
        for package_entry in packages:
            pkg = package_entry.get("package") or package_entry
            name = str(pkg.get("name") or "")
            ecosystem = _ecosystem(str(pkg.get("ecosystem") or ""))
            version = str(pkg.get("version") or package_entry.get("version") or "")
            for vuln in package_entry.get("vulnerabilities", []) or []:
                attrs = _vulnerability_attrs(
                    vuln_id=str(vuln.get("id") or ""),
                    package_name=name,
                    ecosystem=ecosystem,
                    installed_version=version,
                    severity=_severity_from_osv(vuln),
                    source="osv-scanner",
                    status="open",
                    fix_versions=_osv_fix_versions(vuln),
                    aliases=_list(vuln.get("aliases")),
                    cvss=_cvss_value(vuln),
                    published_at=str(vuln.get("published") or ""),
                    modified_at=str(vuln.get("modified") or ""),
                    summary=str(vuln.get("summary") or ""),
                    artifact=artifact,
                )
                out.append(NodeFact(
                    attrs["vuln_node_id"],
                    "dependency_vulnerability",
                    _with_artifact_metadata(_without_private(attrs), revision),
                    {"repo": repo, "artifact": artifact, "vuln_id": attrs["vuln_id"]},
                    revision,
                ))
                out.append(_dependency_edge(repo, artifact, attrs, revision))
    return out


def _trivy_facts(
    repo: str,
    repo_root: Path,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    tool_version = str((doc.get("Metadata") or {}).get("Version") or "")
    for result in doc.get("Results", []) or []:
        target = str(result.get("Target") or "")
        result_type = str(result.get("Type") or "")
        for vuln in result.get("Vulnerabilities", []) or []:
            attrs = _vulnerability_attrs(
                vuln_id=str(vuln.get("VulnerabilityID") or vuln.get("VulnerabilityId") or vuln.get("ID") or ""),
                package_name=str(vuln.get("PkgName") or ""),
                ecosystem=_ecosystem(result_type),
                installed_version=str(vuln.get("InstalledVersion") or ""),
                severity=_normalize_severity(vuln.get("Severity")),
                source="trivy",
                status=str(vuln.get("Status") or "open"),
                fix_versions=_list(vuln.get("FixedVersion")),
                aliases=[],
                cvss=_cvss_value(vuln),
                published_at=str(vuln.get("PublishedDate") or ""),
                modified_at=str(vuln.get("LastModifiedDate") or ""),
                summary=str(vuln.get("Title") or vuln.get("Description") or ""),
                artifact=artifact,
            )
            attrs["tool_version"] = tool_version
            attrs["target"] = target
            out.append(NodeFact(
                attrs["vuln_node_id"],
                "dependency_vulnerability",
                _with_artifact_metadata(_without_private(attrs), revision),
                {"repo": repo, "artifact": artifact, "vuln_id": attrs["vuln_id"]},
                revision,
            ))
            out.append(_dependency_edge(repo, artifact, attrs, revision))
        for misconfig in result.get("Misconfigurations", []) or []:
            rel_path = _rel_from_scanner_path(repo_root, misconfig.get("FilePath") or target)
            line = _int_or_one((misconfig.get("CauseMetadata") or {}).get("StartLine") or misconfig.get("Line"))
            attrs = _security_attrs(
                repo=repo,
                tool="trivy",
                tool_version=tool_version,
                rule_id=str(misconfig.get("ID") or misconfig.get("AVDID") or ""),
                rule_version="",
                severity=_normalize_severity(misconfig.get("Severity")),
                confidence="medium",
                path=rel_path,
                start_line=line,
                end_line=line,
                status=str(misconfig.get("Status") or "open"),
                message=str(misconfig.get("Message") or misconfig.get("Title") or ""),
                cwe="",
                owasp="",
                help_uri=str(misconfig.get("PrimaryURL") or ""),
                fingerprint=str(misconfig.get("ID") or ""),
                baseline_state="",
                suppression="",
                artifact=artifact,
            )
            out.append(NodeFact(
                attrs["finding_id"],
                "security_finding",
                _with_artifact_metadata(attrs, revision),
                {"repo": repo, "artifact": artifact, "finding_id": attrs["finding_id"]},
                revision,
            ))
        for secret in result.get("Secrets", []) or []:
            rel_path = _rel_from_scanner_path(repo_root, secret.get("FilePath") or target)
            line = _int_or_one(secret.get("StartLine") or secret.get("Line"))
            attrs = _secret_attrs(
                repo=repo,
                tool="trivy",
                rule_id=str(secret.get("RuleID") or secret.get("Category") or "secret"),
                path=rel_path,
                start_line=line,
                end_line=_int_or_one(secret.get("EndLine") or line),
                fingerprint=_safe_fingerprint(
                    secret.get("Fingerprint") or secret.get("RuleID") or "",
                    secret=str(secret.get("Match") or ""),
                    fallback_parts=[artifact, rel_path, line, secret.get("RuleID")],
                ),
                status=str(secret.get("Status") or "open"),
                entropy=0.0,
                redacted_context=_redact_context(str(secret.get("Match") or ""), ""),
                commit="",
                ref="",
                artifact=artifact,
            )
            out.append(NodeFact(
                attrs["finding_id"],
                "secret_finding",
                _with_artifact_metadata(attrs, revision),
                {"repo": repo, "artifact": artifact, "finding_id": attrs["finding_id"]},
                revision,
            ))
    return out


def _cyclonedx_json_facts(
    repo: str,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    metadata = doc.get("metadata") or {}
    tools = metadata.get("tools") or []
    tool = ""
    tool_version = ""
    if isinstance(tools, list) and tools:
        first = tools[0]
        tool = str(first.get("name") or first.get("vendor") or "")
        tool_version = str(first.get("version") or "")
    components = doc.get("components") or []
    node = _sbom_node(
        repo=repo,
        artifact=artifact,
        fmt="cyclonedx_json",
        generated_at=str(
            metadata.get("timestamp")
            or _artifact_lineage_timestamp(revision)
        ),
        tool=tool or "cyclonedx",
        tool_version=tool_version,
        component_count=len(components) if isinstance(components, list) else 0,
        components=components[:100] if isinstance(components, list) else [],
        revision=revision,
    )
    edges = _sbom_dependency_edges(
        repo,
        artifact,
        node.node_id,
        components if isinstance(components, list) else [],
        revision,
    )
    return [node, *edges]


def _spdx_json_facts(
    repo: str,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    packages = doc.get("packages") or []
    node = _sbom_node(
        repo=repo,
        artifact=artifact,
        fmt="spdx_json",
        generated_at=str(
            doc.get("creationInfo", {}).get("created")
            or _artifact_lineage_timestamp(revision)
        ),
        tool="spdx",
        tool_version=str(doc.get("spdxVersion") or ""),
        component_count=len(packages) if isinstance(packages, list) else 0,
        components=packages[:100] if isinstance(packages, list) else [],
        revision=revision,
    )
    edges = _sbom_dependency_edges(
        repo,
        artifact,
        node.node_id,
        packages if isinstance(packages, list) else [],
        revision,
    )
    return [node, *edges]


def _code_quality_json_facts(
    repo: str,
    repo_root: Path,
    artifact: str,
    doc: dict[str, Any],
    revision: dict[str, Any],
) -> list[NodeFact | EdgeFact]:
    findings = doc.get("findings") or []
    if not isinstance(findings, list):
        return []
    out: list[NodeFact | EdgeFact] = []
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        rel_path = _rel_from_scanner_path(repo_root, finding.get("path") or "")
        if not rel_path:
            continue
        attrs = _code_quality_attrs(
            repo=repo,
            tool=str(finding.get("tool") or "codeql"),
            tool_version=str(finding.get("tool_version") or ""),
            rule_id=str(finding.get("rule_id") or "unknown"),
            rule_name=str(finding.get("rule_name") or finding.get("rule_id") or "unknown"),
            language=str(finding.get("language") or ""),
            category=str(finding.get("category") or "maintainability"),
            severity=str(finding.get("severity") or "note"),
            level=str(finding.get("level") or ""),
            status=str(finding.get("status") or "open"),
            path=rel_path,
            start_line=_int_or_one(finding.get("start_line")),
            end_line=_int_or_one(finding.get("end_line") or finding.get("start_line")),
            start_column=_int_or_zero(finding.get("start_column")) or None,
            end_column=_int_or_zero(finding.get("end_column")) or None,
            message=str(finding.get("message") or ""),
            fingerprint=str(finding.get("fingerprint") or ""),
            baseline_state=str(finding.get("baseline_state") or ""),
            help_uri=str(finding.get("help_uri") or ""),
            sha=str(finding.get("sha") or doc.get("sha") or ""),
            ref=str(finding.get("ref") or doc.get("ref") or ""),
            branch=str(finding.get("branch") or doc.get("branch") or ""),
            workflow_name=str(finding.get("workflow_name") or doc.get("workflow_name") or ""),
            workflow_run_id=str(finding.get("workflow_run_id") or doc.get("workflow_run_id") or ""),
            run_attempt=str(finding.get("run_attempt") or doc.get("run_attempt") or ""),
            source_sarif=str(finding.get("source_sarif") or ""),
            artifact=artifact,
            finding_id=str(finding.get("finding_id") or ""),
        )
        node = _code_quality_node(attrs, revision)
        out.append(node)
        out.extend(_code_quality_edges(repo, artifact, attrs, revision))
    return out


def _code_quality_attrs(
    *,
    repo: str,
    tool: str,
    tool_version: str,
    rule_id: str,
    rule_name: str,
    language: str,
    category: str,
    severity: str,
    level: str,
    status: str,
    path: str,
    start_line: int,
    end_line: int,
    start_column: int | None,
    end_column: int | None,
    message: str,
    fingerprint: str,
    baseline_state: str,
    help_uri: str,
    sha: str,
    ref: str,
    branch: str,
    workflow_name: str,
    workflow_run_id: str,
    run_attempt: str,
    source_sarif: str,
    artifact: str,
    finding_id: str = "",
) -> dict[str, Any]:
    tool_key = _id_slug(tool)
    fingerprint = fingerprint or _safe_fingerprint(
        "",
        secret="",
        fallback_parts=[repo, artifact, rule_id, path, start_line, message],
    )
    if not finding_id.startswith("code-quality-finding:"):
        finding_id = (
            f"code-quality-finding:{tool_key}:"
            f"{_sha(json.dumps([repo, artifact, rule_id, path, start_line, fingerprint], sort_keys=True, default=str), 24)}"
        )
    return {
        "finding_id": finding_id,
        "repo": repo,
        "tool": tool_key,
        "tool_version": tool_version,
        "rule_id": rule_id or "unknown",
        "rule_name": rule_name or rule_id or "unknown",
        "language": language or "unknown",
        "category": (category or "maintainability").lower(),
        "severity": (severity or "note").lower(),
        "level": level,
        "status": status or "open",
        "path": path,
        "start_line": start_line,
        "end_line": end_line,
        "start_column": start_column,
        "end_column": end_column,
        "message": message,
        "fingerprint": fingerprint,
        "baseline_state": baseline_state,
        "help_uri": help_uri,
        "sha": sha,
        "ref": ref,
        "branch": branch,
        "workflow_name": workflow_name,
        "workflow_run_id": workflow_run_id,
        "run_attempt": run_attempt,
        "source_sarif": source_sarif,
        "artifact": artifact,
        "text": f"{tool_key} {rule_id} {category} {path}:{start_line} {message}".strip(),
    }


def _code_quality_node(
    attrs: dict[str, Any],
    revision: dict[str, Any],
) -> NodeFact:
    merged = _with_artifact_metadata(attrs, revision)
    return NodeFact(
        attrs["finding_id"],
        "code_quality_finding",
        {key: value for key, value in merged.items() if value is not None},
        {
            "repo": attrs["repo"],
            "artifact": attrs["artifact"],
            "finding_id": attrs["finding_id"],
        },
        revision,
    )


def _code_quality_edges(
    repo: str,
    artifact: str,
    attrs: dict[str, Any],
    revision: dict[str, Any],
) -> list[EdgeFact]:
    edges: list[EdgeFact] = []
    path = str(attrs.get("path") or "")
    if path:
        edges.append(EdgeFact(
            src=str(attrs["finding_id"]),
            dst=f"file:{repo}:{path}",
            edge_type="affects",
            attrs={"resolution": "artifact_path", "path": path},
            source_record_id={
                "repo": repo,
                "artifact": artifact,
                "finding_id": attrs["finding_id"],
                "target": path,
            },
            source_revision=revision,
        ))
    sha = str(attrs.get("sha") or "").lower()
    if re.fullmatch(r"[0-9a-f]{40}", sha):
        edges.append(EdgeFact(
            src=str(attrs["finding_id"]),
            dst=f"commit:{repo}:{sha}",
            edge_type="affects",
            attrs={"resolution": "artifact_sha", "sha": sha},
            source_record_id={
                "repo": repo,
                "artifact": artifact,
                "finding_id": attrs["finding_id"],
                "sha": sha,
            },
            source_revision=revision,
        ))
    return edges


def _security_attrs(
    *,
    repo: str,
    tool: str,
    tool_version: str,
    rule_id: str,
    rule_version: str,
    severity: str,
    confidence: str,
    path: str,
    start_line: int,
    end_line: int,
    status: str,
    message: str,
    cwe: str,
    owasp: str,
    help_uri: str,
    fingerprint: str,
    baseline_state: str,
    suppression: str,
    artifact: str,
) -> dict[str, Any]:
    finding_id = f"security-finding:{tool}:{_sha(json.dumps([repo, artifact, rule_id, path, start_line, end_line, message], sort_keys=True))}"
    attrs = {
        "finding_id": finding_id,
        "repo": repo,
        "tool": tool,
        "rule_id": rule_id or "unknown",
        "severity": _normalize_severity(severity),
        "confidence": confidence or "medium",
        "path": path,
        "start_line": start_line,
        "end_line": end_line,
        "status": status or "open",
        "tool_version": tool_version,
        "rule_version": rule_version,
        "message": message,
        "cwe": cwe,
        "owasp": owasp,
        "help_uri": help_uri,
        "fingerprint": fingerprint,
        "baseline_state": baseline_state,
        "suppression": suppression,
        "artifact": artifact,
        "text": f"{tool} {rule_id} {path}:{start_line} {severity} {message}".strip(),
    }
    return attrs


def _secret_attrs(
    *,
    repo: str,
    tool: str,
    rule_id: str,
    path: str,
    start_line: int,
    end_line: int,
    fingerprint: str,
    status: str,
    entropy: float,
    redacted_context: str,
    commit: str,
    ref: str,
    artifact: str,
) -> dict[str, Any]:
    finding_id = f"secret-finding:{tool}:{_sha(json.dumps([repo, artifact, rule_id, path, start_line, fingerprint], sort_keys=True))}"
    attrs = {
        "finding_id": finding_id,
        "repo": repo,
        "tool": tool,
        "rule_id": rule_id or "secret",
        "path": path,
        "start_line": start_line,
        "end_line": end_line,
        "fingerprint": fingerprint,
        "status": status or "open",
        "entropy": entropy,
        "redacted_context": redacted_context,
        "commit": commit,
        "ref": ref,
        "artifact": artifact,
        "text": f"{tool} {rule_id} {path}:{start_line} {redacted_context}".strip(),
    }
    return _secret_allowlist(attrs)


def _secret_node_from_security_attrs(
    attrs: dict[str, Any],
    revision: dict[str, Any],
) -> NodeFact:
    secret_attrs = _secret_attrs(
        repo=str(attrs.get("repo") or ""),
        tool=str(attrs.get("tool") or ""),
        rule_id=str(attrs.get("rule_id") or "secret"),
        path=str(attrs.get("path") or ""),
        start_line=_int_or_one(attrs.get("start_line")),
        end_line=_int_or_one(attrs.get("end_line") or attrs.get("start_line")),
        fingerprint=_safe_fingerprint(
            attrs.get("fingerprint") or "",
            secret=str(attrs.get("message") or ""),
            fallback_parts=[
                attrs.get("artifact"),
                attrs.get("path"),
                attrs.get("start_line"),
                attrs.get("rule_id"),
            ],
        ),
        status=str(attrs.get("status") or "open"),
        entropy=0.0,
        redacted_context=_redact_context(str(attrs.get("message") or ""), ""),
        commit="",
        ref="",
        artifact=str(attrs.get("artifact") or ""),
    )
    return NodeFact(
        secret_attrs["finding_id"],
        "secret_finding",
        _with_artifact_metadata(secret_attrs, revision),
        {
            "repo": secret_attrs["repo"],
            "artifact": secret_attrs["artifact"],
            "finding_id": secret_attrs["finding_id"],
        },
        revision,
    )


def _vulnerability_attrs(
    *,
    vuln_id: str,
    package_name: str,
    ecosystem: str,
    installed_version: str,
    severity: str,
    source: str,
    status: str,
    fix_versions: list[str],
    aliases: list[str],
    cvss: float,
    published_at: str,
    modified_at: str,
    summary: str,
    artifact: str,
) -> dict[str, Any]:
    node_id = f"dependency-vuln:{source}:{_sha(json.dumps([vuln_id, package_name, ecosystem, installed_version], sort_keys=True))}"
    normalized_ecosystem = _ecosystem(ecosystem)
    return {
        "vuln_node_id": node_id,
        "vuln_id": vuln_id or "unknown",
        "package_name": package_name or "unknown",
        "ecosystem": normalized_ecosystem,
        "package_id": _dep_id(normalized_ecosystem, package_name),
        "installed_version": installed_version or "unknown",
        "severity": _normalize_severity(severity),
        "source": source,
        "status": status or "open",
        "fix_versions": fix_versions,
        "aliases": aliases,
        "cvss": cvss,
        "published_at": published_at or None,
        "modified_at": modified_at or None,
        "artifact": artifact,
        "text": f"{source} {vuln_id} {package_name} {installed_version} {severity} {summary}".strip(),
    }


def _dependency_edge(
    repo: str,
    artifact: str,
    attrs: dict[str, Any],
    revision: dict[str, Any],
) -> EdgeFact:
    return EdgeFact(
        src=str(attrs["vuln_node_id"]),
        dst=str(attrs["package_id"]),
        edge_type="affects",
        attrs={
            "package_name": attrs["package_name"],
            "ecosystem": attrs["ecosystem"],
            "installed_version": attrs["installed_version"],
            "source": attrs["source"],
        },
        source_record_id={
            "repo": repo,
            "artifact": artifact,
            "vuln_id": attrs["vuln_id"],
            "package_id": attrs["package_id"],
        },
        source_revision=revision,
    )


def _with_artifact_metadata(
    attrs: dict[str, Any],
    revision: dict[str, Any],
) -> dict[str, Any]:
    lineage = {
        key: revision[key]
        for key in (
            "ci_artifact_id",
            "ci_artifact_node_id",
            "ci_artifact_name",
            "ci_run_id",
            "ci_run_attempt",
            "ci_workflow_name",
            "ci_head_branch",
            "ci_lane",
        )
        if revision.get(key) not in (None, "")
    }
    out = {
        **attrs,
        **lineage,
        "artifact_hash": str(revision.get("text_hash") or ""),
    }
    observed_at = _artifact_lineage_timestamp(revision)
    if observed_at:
        out["observed_at"] = observed_at
    return out


def _sbom_dependency_edges(
    repo: str,
    artifact: str,
    sbom_id: str,
    components: list[Any],
    revision: dict[str, Any],
) -> list[EdgeFact]:
    edges: list[EdgeFact] = []
    for component in components:
        if not isinstance(component, dict):
            continue
        name = str(component.get("name") or component.get("Name") or "")
        ecosystem = _component_ecosystem(component)
        if not name or not ecosystem:
            continue
        package_id = _dep_id(ecosystem, name)
        edges.append(EdgeFact(
            src=sbom_id,
            dst=package_id,
            edge_type="references",
            attrs={
                "package_name": name,
                "ecosystem": ecosystem,
                "version": str(component.get("version") or component.get("Version") or ""),
                "purl": str(component.get("purl") or component.get("externalRefs") or ""),
            },
            source_record_id={
                "repo": repo,
                "artifact": artifact,
                "sbom_id": sbom_id,
                "package_id": package_id,
            },
            source_revision=revision,
        ))
    return edges


def _component_ecosystem(component: dict[str, Any]) -> str:
    purl = str(component.get("purl") or component.get("PURL") or "")
    if purl.startswith("pkg:"):
        purl_type = purl.removeprefix("pkg:").split("/", 1)[0].split("@", 1)[0]
        return _ecosystem(purl_type)
    raw = (
        component.get("ecosystem")
        or component.get("type")
        or component.get("Type")
        or component.get("PackageSupplier")
        or ""
    )
    return _ecosystem(str(raw))


def _dep_id(ecosystem: str, package_name: str) -> str:
    return f"dep:{_ecosystem(ecosystem)}:{package_name.lower()}"


def _sbom_node(
    *,
    repo: str,
    artifact: str,
    fmt: str,
    generated_at: str,
    tool: str,
    tool_version: str,
    component_count: int,
    components: list[Any],
    revision: dict[str, Any],
) -> NodeFact:
    sbom_id = f"sbom:{repo}:{_sha(artifact)}"
    attrs = {
        "sbom_id": sbom_id,
        "repo": repo,
        "path": artifact,
        "format": fmt,
        "tool": tool,
        "tool_version": tool_version,
        "component_count": component_count,
        "components": components,
        "artifact_hash": str(revision.get("text_hash") or ""),
        "text": f"{fmt} {artifact} components={component_count}",
    }
    if generated_at:
        attrs["generated_at"] = generated_at
        attrs["observed_at"] = generated_at
    return NodeFact(
        sbom_id,
        "sbom_artifact",
        attrs,
        {"repo": repo, "artifact": artifact, "sbom_id": sbom_id},
        revision,
    )


def _apply_finding_policy(
    facts: list[NodeFact | EdgeFact],
    *,
    finding_exclude_globs: Sequence[str],
    suppression_rules: Sequence[_SuppressionRule],
) -> list[NodeFact | EdgeFact]:
    if not finding_exclude_globs and not suppression_rules:
        return facts
    out: list[NodeFact | EdgeFact] = []
    dropped_node_ids: set[str] = set()
    for fact in facts:
        if not isinstance(fact, NodeFact) or fact.subtype not in _FINDING_SUBTYPES:
            out.append(fact)
            continue
        path = str(fact.attrs.get("path") or "")
        if _path_matches_any(path, finding_exclude_globs):
            dropped_node_ids.add(fact.node_id)
            continue
        suppression = _matching_suppression_rule(fact.subtype, fact.attrs, suppression_rules)
        if suppression is None:
            out.append(fact)
            continue
        attrs = dict(fact.attrs)
        attrs["status"] = "suppressed"
        attrs["baseline_state"] = "accepted"
        attrs["suppression"] = (
            f"{suppression.rule_id}: {suppression.reason}"
            if suppression.reason else suppression.rule_id
        )
        out.append(NodeFact(
            fact.node_id,
            fact.subtype,
            attrs,
            fact.source_record_id,
            fact.source_revision,
            fact.op,
        ))
    if not dropped_node_ids:
        return out
    return [
        fact for fact in out
        if not (
            isinstance(fact, EdgeFact)
            and (fact.src in dropped_node_ids or fact.dst in dropped_node_ids)
        )
    ]


def _matching_suppression_rule(
    subtype: str,
    attrs: dict[str, Any],
    rules: Sequence[_SuppressionRule],
) -> _SuppressionRule | None:
    for rule in rules:
        if _suppression_rule_matches(subtype, attrs, rule):
            return rule
    return None


def _suppression_rule_matches(
    subtype: str,
    attrs: dict[str, Any],
    rule: _SuppressionRule,
) -> bool:
    if not rule.has_matcher():
        return False
    if rule.subtypes and subtype not in rule.subtypes:
        return False
    tool = str(attrs.get("tool") or "").lower()
    if rule.tools and tool not in {item.lower() for item in rule.tools}:
        return False
    scanner_rule_id = str(attrs.get("rule_id") or "")
    if rule.scanner_rule_ids and scanner_rule_id not in rule.scanner_rule_ids:
        return False
    fingerprint = str(attrs.get("fingerprint") or "")
    if rule.fingerprints and fingerprint not in rule.fingerprints:
        return False
    path = str(attrs.get("path") or "")
    if rule.path_globs and not _path_matches_any(path, rule.path_globs):
        return False
    if rule.start_lines:
        try:
            start_line = int(attrs.get("start_line") or 0)
        except (TypeError, ValueError):
            return False
        if start_line not in rule.start_lines:
            return False
    artifact = str(attrs.get("artifact") or "")
    if rule.artifact_globs and not _path_matches_any(artifact, rule.artifact_globs):
        return False
    return True


def _path_matches_any(path: str, patterns: Sequence[str]) -> bool:
    if not path:
        return False
    return any(glob_matches_path(path, pattern) for pattern in patterns)


def _load_suppression_rules(
    repo_root: Path,
    suppression_paths: Sequence[Path | str],
    suppression_globs: Sequence[str],
) -> tuple[_SuppressionRule, ...]:
    rules: list[_SuppressionRule] = []
    for path in _configured_suppression_files(repo_root, suppression_paths, suppression_globs):
        try:
            raw = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError):
            continue
        items = raw.get("suppressions") if isinstance(raw, dict) else raw
        if not isinstance(items, list):
            continue
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            rule_id = str(item.get("id") or f"{path.name}:{index + 1}")
            rule = _SuppressionRule(
                rule_id=rule_id,
                reason=str(item.get("reason") or ""),
                subtypes=_string_tuple(item.get("subtypes")),
                tools=_string_tuple(item.get("tools")),
                scanner_rule_ids=_string_tuple(item.get("rule_ids")),
                path_globs=_string_tuple(item.get("path_globs")),
                start_lines=_int_tuple(item.get("start_lines")),
                fingerprints=_string_tuple(item.get("fingerprints")),
                artifact_globs=_string_tuple(item.get("artifact_globs")),
            )
            if rule.has_matcher():
                rules.append(rule)
    return tuple(rules)


def _configured_suppression_files(
    repo_root: Path,
    suppression_paths: Sequence[Path | str],
    suppression_globs: Sequence[str],
) -> tuple[Path, ...]:
    paths: list[Path] = []
    for raw in suppression_paths:
        path = _resolve_configured_path(repo_root, raw)
        if path.is_file():
            paths.append(path.resolve())
    for pattern in suppression_globs:
        paths.extend(path.resolve() for path in repo_root.glob(pattern) if path.is_file())
    return tuple(sorted(set(paths)))


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value if str(item))
    return (str(value),)


def _int_tuple(value: Any) -> tuple[int, ...]:
    if value is None:
        return ()
    raw_values = value if isinstance(value, Sequence) and not isinstance(value, str) else [value]
    out: list[int] = []
    for item in raw_values:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return tuple(out)


def _finding_policy_hash(
    finding_exclude_globs: Sequence[str],
    suppression_rules: Sequence[_SuppressionRule],
) -> str:
    payload = {
        "finding_exclude_globs": tuple(finding_exclude_globs),
        "suppressions": tuple(
            {
                "id": rule.rule_id,
                "reason": rule.reason,
                "subtypes": rule.subtypes,
                "tools": rule.tools,
                "rule_ids": rule.scanner_rule_ids,
                "path_globs": rule.path_globs,
                "start_lines": rule.start_lines,
                "fingerprints": rule.fingerprints,
                "artifact_globs": rule.artifact_globs,
            }
            for rule in suppression_rules
        ),
    }
    return _sha(json.dumps(payload, sort_keys=True, default=str), 24)


def _configured_artifacts(
    repo_root: Path,
    artifact_paths: Sequence[Path | str],
    artifact_globs: Sequence[str],
    max_artifacts: int,
) -> list[Path]:
    paths: list[Path] = []
    for raw in artifact_paths:
        path = _resolve_configured_path(repo_root, raw)
        if path.is_file():
            paths.append(path.resolve())
    for pattern in artifact_globs:
        paths.extend(path.resolve() for path in repo_root.glob(pattern) if path.is_file())
    deduped = sorted(set(paths))
    if len(deduped) > max_artifacts:
        raise ValueError(
            f"security analysis source exceeded max_artifacts={max_artifacts}; "
            "tighten artifact_globs or raise the cap"
        )
    return deduped


def _resolve_configured_path(repo_root: Path, raw: Path | str) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _configured_artifact_matches(
    repo_root: Path,
    path: Path,
    artifact_paths: Sequence[Path | str],
    artifact_globs: Sequence[str],
) -> bool:
    resolved = path.resolve()
    for raw in artifact_paths:
        if resolved == _resolve_configured_path(repo_root, raw):
            return True
    try:
        rel = resolved.relative_to(repo_root).as_posix()
    except ValueError:
        return False
    return any(glob_matches_path(rel, pattern) for pattern in artifact_globs)


def _rel_or_name(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.name


def _is_quality_sarif_with_normalized_sibling(path: Path) -> bool:
    if path.suffix.lower() != ".sarif":
        return False
    name = path.name.lower()
    artifact_text = path.as_posix().lower()
    if "quality" not in name and "code-quality" not in artifact_text:
        return False
    return (path.parent / "code-quality-findings.json").is_file()


def _rel_from_scanner_path(repo_root: Path, raw: Any) -> str:
    text = str(raw or "")
    if not text:
        return ""
    path = Path(text)
    try:
        if path.is_absolute():
            return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return ""
    return path.as_posix()


def _is_sarif(doc: dict[str, Any]) -> bool:
    return str(doc.get("version") or "") == "2.1.0" and isinstance(doc.get("runs"), list)


def _looks_like_semgrep(doc: dict[str, Any]) -> bool:
    return bool(doc.get("results")) and any(
        isinstance(row, dict) and ("check_id" in row or "extra" in row)
        for row in doc.get("results", []) or []
    )


def _looks_like_bandit(doc: dict[str, Any]) -> bool:
    return "metrics" in doc or any(
        isinstance(row, dict) and ("test_id" in row or "issue_text" in row)
        for row in doc.get("results", []) or []
    )


def _looks_like_gitleaks(doc: dict[str, Any]) -> bool:
    return any(
        isinstance(row, dict) and ("RuleID" in row or "Secret" in row or "Fingerprint" in row)
        for row in doc.get("findings", []) or []
    )


def _looks_like_pip_audit(doc: dict[str, Any]) -> bool:
    return any(
        isinstance(row, dict) and ("vulns" in row or "vulnerabilities" in row)
        for row in doc.get("dependencies", []) or []
    )


def _looks_like_osv(doc: dict[str, Any]) -> bool:
    return any(
        isinstance(row, dict) and ("packages" in row or "vulnerabilities" in row)
        for row in doc.get("results", []) or []
    )


def _looks_like_trivy(doc: dict[str, Any]) -> bool:
    return any(
        isinstance(row, dict)
        and any(key in row for key in ("Vulnerabilities", "Misconfigurations", "Secrets"))
        for row in doc.get("Results", []) or []
    )


def _sarif_tool_name(run: dict[str, Any]) -> str:
    driver = ((run.get("tool") or {}).get("driver") or {})
    raw = str(driver.get("name") or "sarif").lower()
    if "codeql" in raw:
        return "codeql"
    if "semgrep" in raw:
        return "semgrep"
    if "bandit" in raw:
        return "bandit"
    if "gitleaks" in raw:
        return "gitleaks"
    if "trivy" in raw:
        return "trivy"
    return raw or "sarif"


def _sarif_rules(driver: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for rule in driver.get("rules", []) or []:
        if isinstance(rule, dict):
            out[str(rule.get("id") or "")] = rule
    return out


def _is_code_quality_sarif(artifact: str, run: dict[str, Any]) -> bool:
    artifact_text = artifact.lower()
    if "code-quality" in artifact_text or ".quality." in artifact_text:
        return True
    props = json.dumps(run.get("properties") or {}, sort_keys=True).lower()
    automation = json.dumps(run.get("automationDetails") or {}, sort_keys=True).lower()
    if "code-quality" in props or "code-quality" in automation:
        return True
    driver = ((run.get("tool") or {}).get("driver") or {})
    for rule in driver.get("rules", []) or []:
        tags = {
            str(tag).lower()
            for tag in ((rule.get("properties") or {}).get("tags") or [])
        }
        if tags & {"maintainability", "reliability", "readability", "correctness"}:
            return True
    return False


def _sarif_result_is_code_quality(
    artifact_or_run_hint: bool,
    result: dict[str, Any],
    rule: dict[str, Any],
) -> bool:
    if _sarif_rule_has_security_markers(rule):
        return False
    if _sarif_rule_has_quality_markers(rule):
        return True
    props = rule.get("properties") or {}
    if str(result.get("level") or "").lower() in {"note", "warning"} and (
        props.get("problem.severity") or props.get("precision")
    ):
        return bool(artifact_or_run_hint)
    return artifact_or_run_hint


def _sarif_rule_has_security_markers(rule: dict[str, Any]) -> bool:
    props = rule.get("properties") or {}
    tags = {str(tag).lower() for tag in props.get("tags", []) or []}
    return (
        "security" in tags
        or any(tag.startswith("cwe") or "owasp" in tag for tag in tags)
        or props.get("security-severity") not in (None, "")
    )


def _sarif_rule_has_quality_markers(rule: dict[str, Any]) -> bool:
    props = rule.get("properties") or {}
    tags = {str(tag).lower() for tag in props.get("tags", []) or []}
    quality_tags = {
        "maintainability",
        "reliability",
        "readability",
        "correctness",
    }
    return bool(tags & quality_tags)


def _sarif_rule_name(rule: dict[str, Any], rule_id: str) -> str:
    short = rule.get("shortDescription") or {}
    full = rule.get("fullDescription") or {}
    return str(
        rule.get("name")
        or short.get("text")
        or full.get("text")
        or rule_id
        or "unknown"
    )


def _sarif_language(artifact: str, run: dict[str, Any]) -> str:
    haystack = " ".join([
        artifact.lower(),
        json.dumps(run.get("automationDetails") or {}, sort_keys=True).lower(),
        json.dumps(run.get("properties") or {}, sort_keys=True).lower(),
    ])
    if "javascript-typescript" in haystack:
        return "javascript-typescript"
    if "javascript" in haystack or "typescript" in haystack:
        return "javascript-typescript"
    if "python" in haystack:
        return "python"
    if "java" in haystack:
        return "java"
    if "go" in haystack or "golang" in haystack:
        return "go"
    return "unknown"


def _sarif_quality_category(rule: dict[str, Any]) -> str:
    props = rule.get("properties") or {}
    tags = {str(tag).lower() for tag in props.get("tags", []) or []}
    if "reliability" in tags or "correctness" in tags:
        return "reliability"
    if "maintainability" in tags or "readability" in tags:
        return "maintainability"
    return str(props.get("category") or "maintainability")


def _sarif_quality_severity(result: dict[str, Any], rule: dict[str, Any]) -> str:
    raw = str(
        result.get("level")
        or (rule.get("properties") or {}).get("problem.severity")
        or "note"
    ).lower()
    if raw in {"error", "warning", "note"}:
        return raw
    if raw in {"high", "critical"}:
        return "error"
    if raw in {"medium", "moderate"}:
        return "warning"
    return "note"


def _first_sarif_location(
    locations: Any,
    repo_root: Path,
) -> dict[str, Any]:
    if not isinstance(locations, list) or not locations:
        return {}
    physical = (locations[0].get("physicalLocation") or {})
    artifact = physical.get("artifactLocation") or {}
    region = physical.get("region") or {}
    rel_path = _rel_from_scanner_path(repo_root, artifact.get("uri") or "")
    start = _int_or_zero(region.get("startLine"))
    if not rel_path or not start:
        return {}
    return {
        "path": rel_path,
        "start_line": start,
        "end_line": _int_or_zero(region.get("endLine")) or start,
        "start_column": _int_or_zero(region.get("startColumn")),
        "end_column": _int_or_zero(region.get("endColumn")),
    }


def _sarif_message(message: Any) -> str:
    if not isinstance(message, dict):
        return ""
    return str(message.get("text") or message.get("markdown") or "")


def _sarif_severity(result: dict[str, Any], rule: dict[str, Any]) -> str:
    props = rule.get("properties") or {}
    return _normalize_severity(
        result.get("level")
        or props.get("security-severity")
        or props.get("problem.severity")
        or props.get("precision")
    )


def _sarif_status(result: dict[str, Any]) -> str:
    if result.get("suppressions"):
        return "suppressed"
    baseline = str(result.get("baselineState") or "")
    if baseline:
        return baseline
    return "open"


def _sarif_fingerprint(result: dict[str, Any]) -> str:
    fingerprints = result.get("fingerprints") or result.get("partialFingerprints") or {}
    if isinstance(fingerprints, dict):
        for value in fingerprints.values():
            if value:
                return str(value)
    return ""


def _sarif_suppression(result: dict[str, Any]) -> str:
    suppressions = result.get("suppressions") or []
    if not suppressions:
        return ""
    return json.dumps(suppressions, sort_keys=True)


def _sarif_cwe(rule: dict[str, Any]) -> str:
    tags = _list((rule.get("properties") or {}).get("tags"))
    return ",".join(tag for tag in tags if str(tag).lower().startswith("cwe"))


def _sarif_owasp(rule: dict[str, Any]) -> str:
    tags = _list((rule.get("properties") or {}).get("tags"))
    return ",".join(tag for tag in tags if "owasp" in str(tag).lower())


def _is_secret_tool_or_rule(tool: str, rule_id: str, attrs: dict[str, Any]) -> bool:
    haystack = " ".join([
        tool,
        rule_id,
        str(attrs.get("message") or ""),
        str(attrs.get("cwe") or ""),
    ]).lower()
    return "gitleaks" in haystack or "secret" in haystack or "credential" in haystack


def _artifact_lineage(repo_root: Path, path: Path) -> dict[str, Any]:
    try:
        resolved_root = repo_root.resolve()
        current = path.resolve().parent
    except OSError:
        return {}
    lineage: dict[str, Any] = {}
    for parent in [current, *current.parents]:
        if parent == resolved_root.parent:
            break
        manifest = parent / ".okg-ci-artifact.json"
        if manifest.is_file():
            try:
                doc = json.loads(manifest.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return {}
            for source_key, target_key in (
                ("artifact_id", "ci_artifact_id"),
                ("artifact_node_id", "ci_artifact_node_id"),
                ("artifact_name", "ci_artifact_name"),
                ("run_id", "ci_run_id"),
                ("run_attempt", "ci_run_attempt"),
                ("workflow_name", "ci_workflow_name"),
                ("head_branch", "ci_head_branch"),
                ("lane", "ci_lane"),
                ("created_at", "ci_workflow_created_at"),
                ("updated_at", "ci_workflow_updated_at"),
                ("artifact_created_at", "ci_artifact_created_at"),
                ("artifact_updated_at", "ci_artifact_updated_at"),
            ):
                value = doc.get(source_key)
                if value not in (None, ""):
                    lineage[target_key] = value
            return lineage
        if parent == resolved_root:
            break
    return {}


def _artifact_lineage_timestamp(revision: dict[str, Any]) -> str:
    """Return only time supplied by the artifact's upstream lineage."""
    return str(
        revision.get("ci_artifact_updated_at")
        or revision.get("ci_artifact_created_at")
        or revision.get("ci_workflow_updated_at")
        or revision.get("ci_workflow_created_at")
        or ""
    )


def _id_slug(value: Any) -> str:
    text = str(value or "").lower()
    slug = re.sub(r"[^a-z0-9._-]+", "-", text).strip("-")
    return slug or "unknown"


def _bandit_cwe(result: dict[str, Any]) -> str:
    cwe = result.get("issue_cwe")
    if isinstance(cwe, dict):
        return str(cwe.get("id") or cwe.get("name") or "")
    return str(cwe or "")


def _severity_from_osv(vuln: dict[str, Any]) -> str:
    severities = vuln.get("severity") or []
    if isinstance(severities, list) and severities:
        return _normalize_severity(severities[0].get("score") or severities[0].get("type"))
    return "unknown"


def _osv_fix_versions(vuln: dict[str, Any]) -> list[str]:
    versions: list[str] = []
    for affected in vuln.get("affected", []) or []:
        for rng in affected.get("ranges", []) or []:
            for event in rng.get("events", []) or []:
                fixed = event.get("fixed")
                if fixed:
                    versions.append(str(fixed))
    return sorted(set(versions))


def _cvss_value(vuln: dict[str, Any]) -> float:
    for key in ("cvss", "CVSS", "CVSSScore", "score"):
        value = vuln.get(key)
        if isinstance(value, (int, float, str)):
            return _float_or_zero(value)
        if isinstance(value, dict):
            for nested in value.values():
                if isinstance(nested, dict):
                    score = nested.get("V3Score") or nested.get("score")
                    if score is not None:
                        return _float_or_zero(score)
    for severity in vuln.get("severity", []) if isinstance(vuln.get("severity"), list) else []:
        score = severity.get("score")
        if score is not None:
            return _float_or_zero(score)
    return 0.0


def _redact_context(context: str, secret: str) -> str:
    text = context or ""
    if secret:
        text = text.replace(secret, "[REDACTED]")
    # Redact long high-entropy-ish tokens even if the scanner did not
    # split out a Secret field.
    text = re.sub(r"[A-Za-z0-9_+/=-]{12,}", "[REDACTED]", text)
    return text[:500]


def _safe_fingerprint(
    raw: Any,
    *,
    secret: str,
    fallback_parts: list[Any],
) -> str:
    text = str(raw or "")
    if text and secret and secret in text:
        text = ""
    return text or _sha(json.dumps(fallback_parts, sort_keys=True, default=str), 24)


def _secret_allowlist(attrs: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in attrs.items()
        if key in SECRET_FINDING_ALLOWED_ATTRS
    }


def _normalize_severity(value: Any) -> str:
    text = str(value or "unknown").lower()
    if text in {"error", "critical", "crit"}:
        return "critical"
    if text in {"warning", "warn", "high"}:
        return "high"
    if text in {"note", "medium", "moderate"}:
        return "medium"
    if text in {"low", "info", "informational", "none"}:
        return "low"
    try:
        score = float(text)
    except ValueError:
        return text or "unknown"
    if score >= 9:
        return "critical"
    if score >= 7:
        return "high"
    if score >= 4:
        return "medium"
    return "low"


def _ecosystem(value: str) -> str:
    text = str(value or "").lower()
    mapping = {
        "python": "python",
        "pip": "python",
        "pypi": "python",
        "python-pkg": "python",
        "npm": "npm",
        "node": "npm",
        "node-pkg": "npm",
        "npm-pkg": "npm",
        "gomod": "go",
        "go": "go",
        "golang": "go",
        "cargo": "rust",
        "crates": "rust",
        "crates.io": "rust",
        "rust": "rust",
    }
    return mapping.get(text, text or "unknown")


def _list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    if isinstance(value, str) and "," in value:
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(value)]


def _list_or_str(value: Any) -> str:
    return ",".join(_list(value))


def _without_private(attrs: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in attrs.items() if not k.endswith("_node_id")}


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _int_or_one(value: Any) -> int:
    return _int_or_zero(value) or 1


def _float_or_zero(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _local_tag(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _record_set_for_artifact_facts(
    facts: list[NodeFact | EdgeFact],
) -> RecordSet:
    """Bind a complete artifact walk to the exact graph footprint it emitted."""
    records: dict[str, RecordEmission] = {}
    for fact in facts:
        source_record_id = dict(fact.source_record_id)
        artifact = str(source_record_id.get("artifact") or "")
        record_key = artifact or json.dumps(
            source_record_id,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        emission = records.setdefault(record_key, RecordEmission())
        if isinstance(fact, NodeFact):
            emission.descendants.append(
                RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=source_record_id,
                    source_revision=dict(fact.source_revision),
                )
            )
        else:
            emission.edges.append(
                EdgeKey(
                    src=fact.src,
                    dst=fact.dst,
                    edge_type=fact.edge_type,
                    attrs=dict(fact.attrs),
                    provenance=fact.provenance,
                    confidence=fact.confidence,
                    source_record_id=source_record_id,
                    source_revision=dict(fact.source_revision),
                )
            )
    return RecordSet(records=records)


def _dedupe_facts(facts: list[NodeFact | EdgeFact]) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    seen: set[tuple[str, ...]] = set()
    for fact in facts:
        if isinstance(fact, NodeFact):
            key = (
                "node",
                fact.node_id,
                fact.op,
                json.dumps(fact.source_revision, sort_keys=True, default=str),
            )
        else:
            key = (
                "edge",
                fact.src,
                fact.dst,
                fact.edge_type,
                fact.op,
                json.dumps(fact.source_revision, sort_keys=True, default=str),
            )
        if key in seen:
            continue
        seen.add(key)
        out.append(fact)
    return out
