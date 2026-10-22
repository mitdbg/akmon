"""Supported source types for `okg deployment add-source` (issue #1802).

Every supported type is one `okg.deployment.ConnectorRegistration` -- the
same declaration a packaged connector ships as a package-v2 `connector`
asset (issue #1181). The registration supplies what the user must never
author: the reader module and class, the identity / revision / deletion
contract, the settings schema, the access mode, the ontology modules the
reader's records need, and the refresh triggers. `registration_registry_entry`
turns it into the strict source-registry body; the guided flow only adds the
per-instance parts (ownership id, settings, privacy policy).

A type carried here is a built-in reader; `--registration <asset.json>` feeds
a packaged connector's asset body through the same path.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

from okg.deployment import ConnectorRegistration, SdkVersionRange


#: Every declared type must fall inside this window; the running SDK is 1.x.
_SDK_RANGE = SdkVersionRange(min_version="1.0", max_version_exclusive="2.0")


@dataclass(frozen=True)
class SupportedSourceType:
    """One supported reader as the guided flow presents it."""

    type_id: str
    title: str
    summary: str
    registration: ConnectorRegistration
    #: "none" | "env" | "gh_cli": how access is checked and reused.
    access_mode: str
    #: Nomos `source_policy` the flow declares for this type.
    source_policy: Mapping[str, Any]
    #: Ontology modules (bare built-in names) the reader's records need.
    ontology_modules: tuple[str, ...]
    #: Plain-language refresh description.
    refresh: str
    #: Extra fixed constructor params not chosen by the user.
    fixed_params: Mapping[str, Any] = field(default_factory=dict)

    @property
    def settings_schema(self) -> Mapping[str, Any]:
        return self.registration.config_schema

    def describe(self) -> dict[str, Any]:
        schema = dict(self.settings_schema)
        properties = schema.get("properties") or {}
        required = set(schema.get("required") or ())
        return {
            "type": self.type_id,
            "title": self.title,
            "summary": self.summary,
            "settings": [
                {
                    "name": name,
                    "required": name in required,
                    "description": str(spec.get("description") or ""),
                    "default": spec.get("default"),
                }
                for name, spec in properties.items()
            ],
            "access": _ACCESS_TEXT[self.access_mode],
            "records": sorted(
                node["subtype"]
                for node in (
                    self.registration.output_signature.get("nodes") or ()
                )
            ),
            "update_behaviour": update_behaviour_text(self.registration),
            "removal_behaviour": removal_behaviour_text(self.registration),
            "refresh": self.refresh,
        }


_ACCESS_TEXT = {
    "none": "No credential needed.",
    "env": (
        "A credential is read from an environment variable; only the "
        "variable name is saved."
    ),
    "gh_cli": (
        "Reuses your existing GitHub CLI login (`gh auth login`); no token "
        "is copied or saved."
    ),
}


def update_behaviour_text(registration: ConnectorRegistration) -> str:
    kind = registration.source_revision_kind
    if kind == "content_hash":
        return (
            "An edited record is detected by its content and updates the "
            "same graph record; unchanged records are left alone."
        )
    if kind in ("updated_at", "version_token"):
        return (
            "An edited record is detected by the service's update marker "
            "and updates the same graph record."
        )
    return "An edited record updates the same graph record."


def removal_behaviour_text(registration: ConnectorRegistration) -> str:
    semantics = registration.deletion_semantics
    if semantics == "missing_from_completed_scope":
        return (
            "A record that is missing after a complete, successful scan is "
            "removed from the next published version. A scan that stops "
            "early or fails removes nothing."
        )
    if semantics == "explicit_delete":
        return "A record is removed only when the service reports its deletion."
    if semantics == "append_only":
        return "Records are never removed; this source only appends."
    return "Removal follows the reader's declared deletion rule."


_LOCAL_PRIVATE_POLICY: Mapping[str, Any] = {
    "sensitivity": "local_private",
    "data_classification": "private",
    "credential_ref_policy": "none",
    "live_call_allowed": False,
    "exportability": "no_export",
    "retention": "normal",
    "provenance": "local_filesystem",
    "privacy_obligations": ["audit", "no_export"],
    "store_raw": True,
}

_CREDENTIALED_SERVICE_POLICY: Mapping[str, Any] = {
    "sensitivity": "credentialed_external",
    "data_classification": "private",
    "credential_ref_policy": "optional",
    "live_call_allowed": True,
    "exportability": "no_export",
    "retention": "normal",
    "provenance": "remote_api",
    "privacy_obligations": ["audit", "no_export", "review"],
    "store_raw": True,
}


LOCAL_FOLDER = SupportedSourceType(
    type_id="local-folder",
    title="Local folder of documents",
    summary=(
        "Reads Markdown, text, JSON and YAML files under one folder, and "
        "PDF text when include names PDF files, and makes each document and "
        "its sections searchable."
    ),
    registration=ConnectorRegistration(
        connector_type="local-folder",
        version="1.0.0",
        module="okg.substrate.library.sources.doc_corpus",
        class_name="DocCorpusSource",
        ownership_id="local-folder",
        source_class="discovery_crawl",
        trust_label="repo-local-trusted",
        sdk_range=_SDK_RANGE,
        config_schema={
            "type": "object",
            "additionalProperties": False,
            "required": ["root"],
            "properties": {
                "root": {
                    "type": "string",
                    "description": "Folder to read (absolute path).",
                },
                "include": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "File patterns to read.",
                    "default": ["**/*.md", "**/*.txt", "**/*.json", "**/*.yaml"],
                },
                "exclude": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "File patterns to skip.",
                    "default": ["**/.git/**", "**/node_modules/**"],
                },
            },
        },
        auth_modes=("none",),
        capabilities=("change_probe", "record_set_authority"),
        required_modules=("okg:extraction",),
        output_signature={
            "nodes": [
                {"subtype": "document"},
                {"subtype": "document_chunk"},
                # Emitted when extractors.yaml enables entity extraction
                # (a TODO, a DOI, an arXiv id); same shapes as the upload
                # source, which runs the same reader.
                {"subtype": "entity_mention"},
            ],
            "edges": [
                {
                    "src_subtype": "document",
                    "edge_type": "contains",
                    "dst_subtype": "document_chunk",
                },
                {
                    "src_subtype": "document_chunk",
                    "edge_type": "member_of",
                    "dst_subtype": "document",
                },
                # A Markdown link from one file to another in the folder
                # (#3006). Without it admission refuses the whole import.
                {
                    "src_subtype": "document_chunk",
                    "edge_type": "references",
                    "dst_subtype": "document",
                },
                {
                    "src_subtype": "entity_mention",
                    "edge_type": "member_of",
                    "dst_subtype": "document_chunk",
                },
                # `document derived_from source_file` is NOT declared. The
                # reader emits it only toward an existing node
                # `file:<source name>:<path>`, and a deployment whose catalog
                # has `source_file` but no extraction-to-git bridge (the
                # workspace-graph install) refuses every import at the
                # catalog check when the signature declares it.
            ],
        },
        sync={
            "triggers": ["manual", "reconcile"],
            "default_event_mode": "scope_complete",
            "reconcile_mode": "scope_complete",
            "deletes_require_reconcile": True,
            "cheap_probe": "stat_tree",
        },
        record_identity_kind="scoped_locator",
        record_identity_fields=("repo", "path"),
        source_revision_kind="content_hash",
        deletion_semantics="missing_from_completed_scope",
        publication_mode="published_generation",
        admission_mode="fast_track",
        description="Local folder of documents",
    ),
    access_mode="none",
    source_policy=_LOCAL_PRIVATE_POLICY,
    ontology_modules=("extraction",),
    refresh=(
        "Refreshes when you run `okg ingest --deployment <name>` or when the "
        "deployment's scheduled refresh runs; every refresh is a complete scan."
    ),
    fixed_params={"max_files": 5000},
)


GITHUB = SupportedSourceType(
    type_id="github",
    title="GitHub repository issues and pull requests",
    summary=(
        "Reads the issues and pull requests of one repository through your "
        "GitHub CLI login."
    ),
    registration=ConnectorRegistration(
        connector_type="github",
        version="1.0.0",
        module="okg.substrate.library.sources.github",
        class_name="GitHubSource",
        ownership_id="github",
        source_class="mutable_api",
        trust_label="untrusted",
        sdk_range=_SDK_RANGE,
        config_schema={
            "type": "object",
            "additionalProperties": False,
            "required": ["repository"],
            "properties": {
                "repository": {
                    "type": "string",
                    "pattern": "^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
                    "description": "Repository as owner/name.",
                },
                "include_discussion": {
                    "type": "boolean",
                    "description": "Also read issue and review comments.",
                    "default": False,
                },
                "commit_repo": {
                    "type": "string",
                    "pattern": "^[A-Za-z0-9_.-]*$",
                    "description": (
                        "Optional. Name of the git repository in your graph; "
                        "with discussion on, links merged pull requests to "
                        "their merge commits. Empty: no merge link."
                    ),
                },
            },
        },
        auth_modes=("keychain_token",),
        capabilities=("change_probe", "record_set_authority"),
        required_modules=("okg:forge",),
        output_signature={
            "nodes": [
                {"subtype": "repo"},
                {"subtype": "issue"},
                {"subtype": "pull_request"},
                {"subtype": "forge_comment"},
            ],
            "edges": [
                {
                    "src_subtype": "issue",
                    "edge_type": "has_comment",
                    "dst_subtype": "forge_comment",
                },
                {
                    "src_subtype": "pull_request",
                    "edge_type": "has_comment",
                    "dst_subtype": "forge_comment",
                },
                {
                    "src_subtype": "repo",
                    "edge_type": "contains",
                    "dst_subtype": "issue",
                },
                {
                    "src_subtype": "repo",
                    "edge_type": "contains",
                    "dst_subtype": "pull_request",
                },
            ],
        },
        sync={
            "triggers": ["manual", "reconcile"],
            "default_event_mode": "reconcile",
            "reconcile_mode": "reconcile",
            "deletes_require_reconcile": True,
            "cheap_probe": "ttl",
        },
        record_identity_kind="remote_id",
        record_identity_fields=("repo", "number"),
        source_revision_kind="version_token",
        deletion_semantics="missing_from_completed_scope",
        publication_mode="published_generation",
        admission_mode="fast_track",
        description="GitHub repository issues and pull requests",
    ),
    access_mode="gh_cli",
    source_policy=_CREDENTIALED_SERVICE_POLICY,
    ontology_modules=("forge",),
    refresh=(
        "Refreshes when you run `okg ingest --deployment <name>` or when the "
        "deployment's scheduled refresh runs; every refresh re-reads the "
        "whole repository."
    ),
    fixed_params={"forge_host": "github"},
)


SUPPORTED_SOURCE_TYPES: Mapping[str, SupportedSourceType] = {
    LOCAL_FOLDER.type_id: LOCAL_FOLDER,
    GITHUB.type_id: GITHUB,
}


def supported_type(type_id: str) -> Optional[SupportedSourceType]:
    return SUPPORTED_SOURCE_TYPES.get(type_id)


def packaged_type(
    registration: ConnectorRegistration,
    *,
    ontology_modules: tuple[str, ...] = (),
) -> SupportedSourceType:
    """Present a packaged connector registration as a supported type.

    A packaged connector declares its own access mode: an `env_credential`
    connector names its variables in `credential_refs`; anything else needs
    no credential from the flow. Its records are treated as private and
    non-exportable, the safest policy a personal deployment allows.
    """
    env_backed = "env_credential" in registration.auth_modes
    if env_backed:
        policy = dict(_CREDENTIALED_SERVICE_POLICY)
        policy["credential_ref_policy"] = "required"
        policy["privacy_obligations"] = [
            "audit", "no_export", "review", "secret_ref_required",
        ]
    else:
        policy = dict(_LOCAL_PRIVATE_POLICY)
        policy["provenance"] = "declared"
    modules = ontology_modules or tuple(
        name.split(":", 1)[1] if name.startswith("okg:") else name
        for name in registration.required_modules
    )
    return SupportedSourceType(
        type_id=registration.connector_type,
        title=registration.description or registration.connector_type,
        summary=registration.description or "Packaged connector.",
        registration=registration,
        access_mode="env" if env_backed else "none",
        source_policy=policy,
        ontology_modules=modules,
        refresh=(
            "Refreshes when you run `okg ingest --deployment <name>` or when "
            "the deployment's scheduled refresh runs."
        ),
    )


def manifest_type(
    registration: ConnectorRegistration, *, summary: str, reads_http: bool = False,
) -> SupportedSourceType:
    """Present a compiled source manifest (`--manifest`) as a supported type.

    A manifest that reads only local files gets the same local, private,
    non-exportable policy as a local folder; one that reads a JSON API gets
    the remote-service policy (live calls allowed). Either way the records'
    provenance is what the manifest declares.
    """
    if reads_http:
        policy = dict(_CREDENTIALED_SERVICE_POLICY)
        title = "Source manifest (JSON APIs and local files)"
        refresh = (
            "Refreshes when you run `okg ingest --deployment <name>`: every read "
            "is fetched again; nothing is written for unchanged records, and the "
            "run is skipped when the API answers a conditional request with "
            "304 Not Modified for every page."
        )
    else:
        policy = dict(_LOCAL_PRIVATE_POLICY)
        title = "Source manifest (local JSON, JSONL or CSV files)"
        refresh = (
            "Refreshes when you run `okg ingest --deployment <name>`: every "
            "file the manifest's glob matches is read again; nothing is read "
            "when the files and the manifest are unchanged."
        )
    policy["provenance"] = "declared"
    return SupportedSourceType(
        type_id="manifest",
        title=title,
        summary=summary,
        registration=registration,
        access_mode="none",
        source_policy=policy,
        ontology_modules=(),
        refresh=refresh,
    )


__all__ = [
    "GITHUB",
    "LOCAL_FOLDER",
    "SUPPORTED_SOURCE_TYPES",
    "SupportedSourceType",
    "manifest_type",
    "packaged_type",
    "removal_behaviour_text",
    "supported_type",
    "update_behaviour_text",
]
