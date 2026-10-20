"""The one Nomos subject a single-operator MCP server presents (issue #2343).

Every MCP path that asks Nomos a question in the server's own name -- the
read boundary (``mcp.tool.call`` / ``graph.read.mcp``), ``record``
(``mcp.record``), source visibility for the source tools
(``mcp.source_admin``), and the ``run_sql`` audit bypass
(``graph.read.sql_admin``) -- resolves its subject here, so all of them
present the same one. Before this module the read boundary and ``record``
shared a resolver in ``mcp/server.py`` (issue #2157) while the source tools
and ``run_sql`` used ``read_filter.nomos_subject_from_env``, which skips the
baseline default and so presented the login name on a baseline deployment.

It lives in ``okg.substrate.nomos`` rather than ``okg.substrate.mcp.server``
because ``server.py`` imports ``run_sql`` and ``source_lifecycle``; those
modules importing ``server.py`` back would close an import cycle. Its only
top-level import is ``okg.substrate.errors``, so importing it never loads
``okg.substrate.mcp``.
"""
from __future__ import annotations

import getpass
import os
from typing import Any

from okg.substrate.errors import SubstrateError

#: Named refusal for an MCP call whose Nomos subject resolves to nothing
#: (issue #2157). An identity nobody declared is not a policy question, so
#: the call is refused before any decision is asked for or any row written.
MCP_PRINCIPAL_UNRESOLVED = "mcp_principal_unresolved"

MCP_PRINCIPAL_UNRESOLVED_HINT = (
    "set OKG_NOMOS_SUBJECT to a subject the deployment's active policy grants"
)
MCP_PRINCIPAL_UNRESOLVED_MESSAGE = (
    "the MCP server's Nomos subject did not resolve: no OKG_NOMOS_SUBJECT / "
    "OKG_MCP_NOMOS_SUBJECT / OKG_PRINCIPAL, no baseline default subject, "
    "and no process login name; refusing the call"
)


def as_subject(raw: str) -> str:
    """``raw`` with the ``subject:`` prefix, added when missing."""
    subject = raw.strip()
    return subject if subject.startswith("subject:") else f"subject:{subject}"


def os_login_subject() -> str | None:
    """``subject:<login>`` for the process's login name, or None.

    None when neither ``$USER`` nor ``getpass.getuser()`` yields a name.
    The old chain (``$USER or getpass.getuser() or <kind>``) never reached
    its literal default: ``getpass.getuser()`` raises when there is no login
    name, so a server with none failed to START. Returning None instead lets
    it start and refuse each call by name (``mcp_principal_unresolved``).
    """
    login = (os.getenv("USER") or "").strip()
    if not login:
        try:
            login = (getpass.getuser() or "").strip()
        except (OSError, KeyError, ImportError):
            # getpass raises OSError (3.12+; KeyError before) when there is
            # no login name and no passwd entry. That is "no login", and the
            # caller refuses on it by name -- nothing is substituted.
            login = ""
    return as_subject(login) if login else None


def resolve_mcp_subject(conn: Any, *, deployment_name: str) -> str | None:
    """The ONE subject a single-operator MCP server presents to Nomos.

    Resolved per call so every path sees the same active policy. Per
    ``fix-nomos-read-path-ux`` (req.readux.mcp-subject-default; GitHub #640):

    1. An explicit ``OKG_NOMOS_SUBJECT`` / ``OKG_MCP_NOMOS_SUBJECT`` /
       ``OKG_PRINCIPAL`` wins unchanged.
    2. Otherwise ``subject:mcp_client`` exactly when the ACTIVE bundle is the
       recognized deployment rollout baseline AND that baseline grants
       ``subject:mcp_client`` the ``mcp.tool.call`` boundary
       (``nomos.read_grants.baseline_default_mcp_subject``). A failure to
       READ the active policy propagates: it is an authority-evaluation
       failure, not a reason to present a different subject (issue #2343).
    3. Otherwise the process login (``$USER`` / ``getpass.getuser()``), which
       stays denied under an enforce-mode default-deny baseline.
    4. Otherwise None: the subject is unresolved and the caller refuses with
       ``mcp_principal_unresolved``.
    """
    explicit = (
        os.getenv("OKG_NOMOS_SUBJECT")
        or os.getenv("OKG_MCP_NOMOS_SUBJECT")
        or os.getenv("OKG_PRINCIPAL")
        or ""
    ).strip()
    if explicit:
        return as_subject(explicit)
    # Looked up through the package at call time so a test can substitute
    # the baseline lookup in one place for every MCP path.
    from okg.substrate.nomos import baseline_default_mcp_subject

    baseline_subject = baseline_default_mcp_subject(
        conn,
        deployment_name=deployment_name,
        scope=f"deployment:{deployment_name}",
    )
    if baseline_subject is not None:
        return baseline_subject
    return os_login_subject()


def mcp_principal_unresolved_error(surface: str) -> SubstrateError:
    """The named refusal for an unresolved subject on ``surface``."""
    error = SubstrateError(
        MCP_PRINCIPAL_UNRESOLVED_MESSAGE,
        data={"surface": surface},
        fix_hint=MCP_PRINCIPAL_UNRESOLVED_HINT,
    )
    error.code = MCP_PRINCIPAL_UNRESOLVED
    return error


#: Codes for a subject lookup that could not read the active policy. They are
#: the read boundary's authority-evaluation codes: a database failure is
#: ``authority_unavailable``, anything else ``authority_evaluation_failed``.
AUTHORITY_UNAVAILABLE = "authority_unavailable"
AUTHORITY_EVALUATION_FAILED = "authority_evaluation_failed"


def mcp_subject_lookup_error(surface: str, err: BaseException) -> SubstrateError:
    """The named refusal for a subject lookup whose policy read failed.

    The message names only the exception TYPE. A driver error's text can
    carry the database host and user, and this message reaches the MCP
    client; the full cause stays chained on the exception in-process.
    """
    import psycopg

    infra = isinstance(err, (psycopg.OperationalError, psycopg.InterfaceError))
    code = AUTHORITY_UNAVAILABLE if infra else AUTHORITY_EVALUATION_FAILED
    reason = (
        "the deployment database connection failed"
        if infra
        else "the active Nomos policy could not be read"
    )
    error = SubstrateError(
        f"the MCP server's Nomos subject could not be resolved because {reason} "
        f"({type(err).__name__}); no policy verdict was rendered and no other "
        "subject was substituted",
        data={"surface": surface, "cause_type": type(err).__name__},
        fix_hint=(
            "retry once the deployment database is reachable"
            if infra
            else "inspect the deployment's active Nomos policy; no verdict exists"
        ),
    )
    error.code = code
    return error


def require_mcp_subject(
    conn: Any,
    *,
    deployment_name: str,
    surface: str,
) -> str:
    """:func:`resolve_mcp_subject`, or a named refusal.

    ``mcp_principal_unresolved`` when no step yields a subject;
    ``authority_unavailable`` / ``authority_evaluation_failed`` (see
    :func:`mcp_subject_lookup_error`) when reading the active policy for the
    baseline step fails. A :class:`SubstrateError` from the lookup keeps its
    own code.
    """
    try:
        subject = resolve_mcp_subject(conn, deployment_name=deployment_name)
    except SubstrateError:
        raise
    except Exception as exc:
        raise mcp_subject_lookup_error(surface, exc) from exc
    if subject is None:
        raise mcp_principal_unresolved_error(surface)
    return subject
