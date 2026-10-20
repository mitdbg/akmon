"""Runtime MCP surface fingerprinting.

The report is computed from the running server process, especially the
effective instructions passed to FastMCP, so a stale resident process cannot
pretend to be current by reading newer files from its install root.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import os
import subprocess
from pathlib import Path
from typing import Any

from okg.substrate.mcp.provenance import UNTRUSTED_DATA_CONTRACT


def mcp_surface_report(
    *,
    deployment_name: str,
    instructions: str,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    """Return the running MCP server's code/instruction surface report."""
    package_version = _package_version()
    if os.environ.get("OKG_MCP_GIT_REVISION"):
        git_revision = os.environ["OKG_MCP_GIT_REVISION"]
        revision_source = "OKG_MCP_GIT_REVISION"
    elif repo_root is not None:
        git_revision = _git_revision(repo_root)
        revision_source = "git rev-parse HEAD"
    else:
        installed = _installed_revision()
        git_revision = installed.revision
        revision_source = {
            "checkout": "git rev-parse HEAD",
            "build_info": "okg/_build_info.json source_revision",
        }.get(installed.source or "")
    report = {
        "kind": "mcp_surface",
        "schema": "okg.mcp.surface_freshness.v1",
        "source": "running_process",
        "deployment_name": deployment_name,
        "package_name": "okg",
        "package_version": package_version,
        "git_revision": git_revision,
        **served_instructions_fields(instructions),
        "untrusted_data_contract_hash": _sha256_text(
            UNTRUSTED_DATA_CONTRACT
        ),
        "computed_from": [
            "effective_instructions",
            "importlib.metadata.version('okg')",
            *([revision_source] if revision_source else []),
        ],
        "git_revision_source": revision_source,
    }
    release_keys = (
        "OKG_ENVIRONMENT_ID",
        "OKG_RELEASE_ARTIFACT_ID",
        "OKG_RELEASE_ROOT",
        "OKG_ACTIVE_RELEASE_POINTER",
    )
    present = {key: os.environ.get(key) for key in release_keys}
    if any(present.values()):
        missing = [key for key, value in present.items() if not value]
        report["release_authority"] = {
            "status": "complete" if not missing else "incomplete",
            "environment_id": present["OKG_ENVIRONMENT_ID"],
            "artifact_id": present["OKG_RELEASE_ARTIFACT_ID"],
            "artifact_root": present["OKG_RELEASE_ROOT"],
            "active_pointer": present["OKG_ACTIVE_RELEASE_POINTER"],
            "missing": missing,
        }
        report["computed_from"].append("sealed release environment")
    return report


def served_instructions_fields(instructions: str) -> dict[str, Any]:
    """The report fields computed from the served instruction text.

    Split out so the server can refresh them after it appends the generated
    capability lines (#3151) without re-reading the package and revision.
    """
    return {
        "instructions_hash": _sha256_text(instructions),
        "instructions_bytes": len(instructions.encode("utf-8")),
        "untrusted_data_contract_present": (
            UNTRUSTED_DATA_CONTRACT in instructions
        ),
    }


#: Build-metadata tag marking okg's own version in the MCP initialize reply.
#: Without it the reply carried the MCP SDK's version (FastMCP's default), and
#: a launch that withholds `inspect(kind=surface)` -- every read-only agent
#: launch -- gave the freshness doctor nothing of okg's to compare.
SERVER_INFO_TAG = "okg"


def server_info_version(
    package_version: str | None, git_revision: str | None,
) -> str:
    """okg's version and revision as one MCP `serverInfo.version` string.

    Semver build metadata: `0.1.0+okg.g<revision>`, or `0.1.0+okg` with no
    revision. The tag tells an okg self-report from an SDK default.
    """
    meta = SERVER_INFO_TAG + (f".g{git_revision}" if git_revision else "")
    return f"{package_version or 'unknown'}+{meta}"


def parse_server_info_version(version: object) -> dict[str, str | None] | None:
    """The package version and revision from `server_info_version`, or None
    when `version` is not an okg self-report.

    Split at the LAST `+okg`, so a package version with its own local part
    (`0.1.0+local`) or a revision holding `.` or `+` still reads back.
    """
    if not isinstance(version, str):
        return None
    marker = "+" + SERVER_INFO_TAG
    index = version.rfind(marker)
    if index < 0:
        return None
    package_version, rest = version[:index], version[index + len(marker):]
    if rest and (not rest.startswith(".g") or len(rest) < 3):
        return None
    return {
        "package_version": (
            None if package_version in {"", "unknown"} else package_version
        ),
        "git_revision": rest[2:] or None,
    }


def _sha256_text(text: str) -> str:
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def _package_version() -> str | None:
    try:
        return importlib.metadata.version("okg")
    except importlib.metadata.PackageNotFoundError:
        return None


def _installed_revision():  # noqa: ANN202 - InstalledCodeRevision, imported lazily
    """The commit the running okg was built from: the checkout's HEAD, or a
    wheel's recorded build revision, which `git rev-parse` cannot see (#3585)."""
    from okg.substrate.runtime.code_identity import installed_code_revision

    return installed_code_revision()


def _git_revision(repo_root: Path | None) -> str | None:
    if repo_root is None:
        return None
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    revision = result.stdout.strip()
    return revision or None
