"""The short text `okg install` prints when it succeeds (issues #3891, #3848).

The scaffold payload carries everything the install did: every file written,
the composition probe, the first-publish proof with its queue and lock
numbers, and the commands for each next step. `--json` prints all of it.
For a person, `render_install_summary` keeps what they act on: the graph's
name, how much was published, where the database is, one search to try, and
how to connect an agent. `okg install --verbose` prints the full payload.

Pure functions of the payload, so a test can pin the text without a database.
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

#: Warning codes that never call for action, so the short summary leaves
#: them out. `skipped_extends`: a module declares optional cross-link rules
#: for a module this profile does not load (for example `code_graph`
#: extending `concepts`). The rules apply only when both modules are loaded;
#: without the second one the graph is complete for what the profile asks
#: for. Adding the module later activates the rules, and nothing needs to be
#: fixed first. `--verbose` prints them.
INFORMATIONAL_WARNING_CODES = frozenset({"skipped_extends"})

_CONNECT_NEEDS_ROLE_PASSWORDS = "needs_role_passwords"


def actionable_warnings(
    warnings: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """The warnings a person must act on; the rest are informational."""
    return [w for w in warnings if w.get("code") not in INFORMATIONAL_WARNING_CODES]


def warning_line(warning: Mapping[str, Any]) -> str:
    """One warning as the text that goes to stderr (`WARN [code]: ...`)."""
    message = warning.get("message") or (
        f"{warning.get('source_module', '')} -> "
        f"{warning.get('target_module', '')}: {warning.get('reason', '')}"
    )
    return f"WARN [{warning['code']}]: {message}"


def warning_fix(warning: Mapping[str, Any], deployment: str) -> str | None:
    """The line that tells the reader what to do about ``warning``.

    Every warning the install prints carries one, except the stage failures
    that stop the install: those print their own fix after the warning
    (`_print_fatal_install_summary`), so they return None here.
    """
    code = warning.get("code")
    if code == "skipped_extends":
        target = warning.get("target_module") or "that"
        return (
            f"  Fix: none needed. The links turn on if you add the {target} "
            "module to the deployment."
        )
    if code == "composition_probe_failed":
        from okg.substrate.cli.command_reference import okg_command
        from okg.substrate.support.user_hints import hint_command

        doctor = hint_command(okg_command("doctor", deployment=deployment))
        return (
            "  Fix: the install went on without checking the optional module "
            f"links. Run `{doctor}` to see the cause."
        )
    return None


#: Shown when no word of the scanned repository can be named.
SEARCH_PLACEHOLDER = "<a word from a file>"


def _git_files_excludes(deployment_dir: str | None) -> tuple[str, ...] | None:
    """The `git_files` source's exclude globs, as the install wrote them.

    None when the deployment has no readable `git_files` source: nothing says
    which files the graph holds, so no word is named.
    """
    from okg.substrate import fastyaml
    from okg.substrate.library.sources._git_helpers import DEFAULT_EXCLUDE_GLOBS

    if not deployment_dir:
        return None
    try:
        registry = fastyaml.safe_load(
            (Path(deployment_dir) / "source_registry.yaml").read_text(encoding="utf-8"),
        )
        params = registry["sources"]["git_files"].get("params") or {}
    except (OSError, KeyError, TypeError, AttributeError, ValueError):
        return None
    globs = params.get("exclude_globs")
    return tuple(globs) if globs is not None else tuple(DEFAULT_EXCLUDE_GLOBS)


def _first_indexed_word(repo_root: str, excludes: tuple[str, ...]) -> str | None:
    """The name (without extension) of the first file the graph holds.

    The graph indexes the paths git tracks, minus the `git_files` source's
    exclude globs, so that word is in it. The folder's own name is not:
    searching `Hello-World` in the Hello-World repository found nothing
    (#3896 review).
    """
    import subprocess

    from okg.substrate.library.sources._git_helpers import path_included_by_globs

    try:
        listing = subprocess.run(
            ["git", "-C", repo_root, "ls-files"],
            capture_output=True, text=True, timeout=10, check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    for path in listing.splitlines():
        if not path_included_by_globs(path, exclude_globs=excludes):
            continue
        stem = Path(path).stem
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{2,}", stem):
            return stem
    return None


def _search_term(payload: Mapping[str, Any]) -> str:
    """A word that is in the graph, else a placeholder the reader fills in."""
    repo_root = (payload.get("answers") or {}).get("repo_root")
    excludes = _git_files_excludes((payload.get("deployment") or {}).get("dir"))
    word = (
        _first_indexed_word(str(repo_root), excludes)
        if repo_root and excludes is not None else None
    )
    return word or SEARCH_PLACEHOLDER


def _published(payload: Mapping[str, Any]) -> tuple[bool, Mapping[str, Any]]:
    """Whether this install published a graph, and the proof that says so."""
    publish = payload.get("publish")
    if not isinstance(publish, Mapping) or not publish.get("ok"):
        return False, {}
    details = publish.get("details") or {}
    if details.get("skipped"):
        return False, {}
    proof = details.get("publish_proof")
    return True, proof if isinstance(proof, Mapping) else {}


def _count(proof: Mapping[str, Any], *keys: str) -> int | None:
    """The first of ``keys`` the proof holds as a real integer."""
    for key in keys:
        value = proof.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def render_install_summary(
    payload: Mapping[str, Any],
    *,
    agents_step: Mapping[str, Any] | None,
    env_prefix: str = "",
) -> str:
    """The short summary of a successful scaffold install.

    ``env_prefix`` is the `OKG_DEPLOYMENTS_DIR=... ` words a command needs
    when the install used a deployments folder okg would not find by itself.
    Warnings are not rendered here: the caller prints the actionable ones to
    stderr as they happen (`actionable_warnings`).
    """
    from okg.substrate.cli.command_reference import okg_command
    from okg.substrate.support.user_hints import hint_command


    deployment = payload.get("deployment") or {}
    name = str(deployment.get("name") or "")
    profile = (payload.get("profile") or {}).get("name")
    published, proof = _published(payload)
    lines: list[str] = []

    title = f"Installed graph {name!r}"
    if profile:
        title += f" from the {profile} profile"
    lines.append(title + ".")
    if deployment.get("dir"):
        lines.append(f"  Deployment folder: {deployment['dir']}")
    dsn = (payload.get("answers") or {}).get("postgres_dsn")
    if dsn:
        from okg.substrate.db.dsn import placeholders

        names = placeholders(str(dsn))
        whole = len(names) == 1 and str(dsn).strip() in (
            "${" + names[0] + "}", "$" + names[0],
        )
        lines.append(
            f"  Database: from ${names[0]}" if whole else f"  Database: {dsn}"
        )

    if not published:
        lines.append(
            "  Nothing is published yet. Next, in order:"
        )
        lines.extend(f"    {command}" for command in payload.get("next") or [])
        return "\n".join(lines)

    nodes = _count(proof, "nodes_added", "node_count")
    edges = _count(proof, "edges_added", "edge_count")
    if nodes is not None and edges is not None:
        lines.append(f"  The graph now holds {nodes} records and {edges} links.")
    else:
        lines.append("  Published the first version of the graph.")
    waiting = ((payload.get("publish") or {}).get("details") or {}).get(
        "ingest_selection", {},
    ).get("awaiting_setup")
    if waiting:
        lines.append(
            "  Sources waiting for your setup approval: "
            + ", ".join(str(item) for item in waiting) + "."
        )

    if (payload.get("publish") or {}).get("no_runnable_sources"):
        # The graph is empty by design until a source is added.
        add_source = next(
            (c for c in payload.get("next") or [] if "add-source" in c), None,
        )
        lines.append("  No source is runnable yet, so the graph is empty.")
        if add_source:
            lines.append(f"  Add one: {env_prefix}{add_source}")
    else:
        query = _search_term(payload)
        lines.append(
            "  Try a search: "
            + env_prefix
            + hint_command(okg_command("search", deployment=name, query=query))
        )

    step = agents_step or {}
    status = step.get("status")
    # An agent that is installed but was not connected gets its own line and
    # command. An agent that is not on this machine gets nothing.
    refused = [
        item for item in step.get("not_connected") or []
        if item.get("reason") != _AGENT_NOT_FOUND
    ]
    if status == _CONNECT_NEEDS_ROLE_PASSWORDS:
        from okg.substrate.local_database_roles import REMOTE_ROLES_DOC
        from okg.substrate.support.user_hints import doc_reference

        labels = [_agent_label(item) for item in refused]
        if not labels:
            who, verb = "Connecting an agent", "waits"
        elif len(labels) == 1:
            who, verb = labels[0], "waits"
        else:
            who = ", ".join(labels[:-1]) + f" and {labels[-1]}"
            verb = "wait"
        lines.append(
            f"  {who} {verb} for the database roles to get passwords. In order:"
        )
        lines.extend(f"    {command}" for command in step.get("next_commands") or [])
        lines.append(
            "  On a database on another host, set the roles up by hand "
            f"instead: {doc_reference(REMOTE_ROLES_DOC)}, step 4, 'A "
            "database on another host'."
        )
        # No per-agent lines: the header says every installed agent waits for
        # the passwords, and the commands above connect them all.
    elif status == "connected":
        first = str(step.get("message") or "").split("\n", 1)[0]
        lines.append(f"  {first}")
        lines.extend(_refused_lines(refused))
    elif status == "skipped":
        lines.append(
            f"  Agent connection skipped ({step.get('skip_reason')}). "
            "To connect: "
            + env_prefix
            + hint_command(okg_command("agent connect", deployment=name))
        )
    elif refused:
        lines.extend(_refused_lines(refused))
    else:
        lines.append(
            "  Connect an agent: "
            + env_prefix
            + hint_command(okg_command("agent connect", deployment=name))
        )
    return "\n".join(lines)


#: The reason `connect_after_install` gives an agent that is not installed.
_AGENT_NOT_FOUND = "not found on this machine"


def _agent_label(item: Mapping[str, Any]) -> str:
    from okg.substrate.library.agent_runtimes.connect import AGENT_LABELS

    return AGENT_LABELS.get(item["agent"], str(item["agent"]))


def _refused_lines(items: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        f"  {_agent_label(item)} is not connected "
        f"({str(item['reason']).removeprefix('not connected: ')}). "
        f"To connect it: {item['command']}"
        for item in items
    ]


def deployments_env_prefix(
    deployments_root: os.PathLike[str] | str, *, explicit: bool,
) -> str:
    """`OKG_DEPLOYMENTS_DIR=<root> ` when the install chose its own folder."""
    import shlex

    if not explicit:
        return ""
    return f"OKG_DEPLOYMENTS_DIR={shlex.quote(str(deployments_root))} "


def agent_failure_line(agents_step: Mapping[str, Any]) -> str | None:
    """The one line for an agent connection that failed, with its fix.

    A missing agent or a wait for role passwords is not a failure and has
    its command in the summary; only a refusal needs this line.
    """
    error = agents_step.get("error")
    if agents_step.get("status") != "failed" or not isinstance(error, Mapping):
        return None
    line = f"Agent connection failed ({error.get('code')}): {error.get('message')}"
    if error.get("fix_hint"):
        line += f" Fix: {error['fix_hint']}"
    return line
