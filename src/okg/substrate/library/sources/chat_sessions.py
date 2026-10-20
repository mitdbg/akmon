"""Open WebUI chat conversations as a governed graph source (PACT chat-transcript-source).

What this reads, and why that upstream
--------------------------------------
The Open WebUI **app database**, READ-ONLY. The HTTP-API arm was rejected:
the design's own platform census (design.md section 2) enumerates the API as
chat completions, model sync, files, and users. Conversations, ratings, and
arena preferences are not in that census, so an API reader would rest on
vendor claims nobody in this program has measured. The app database is the
instance's own store of record, and it is the surface the census does name.

The DB arm's fragility is accepted EXPLICITLY rather than hidden. Reading
another application's schema couples this source to a vendor version, so:

* the version is PINNED (:data:`PINNED_OPENWEBUI_VERSION`) together with the
  app-database migration revision (:data:`PINNED_APP_DB_REVISION`), read from
  ``alembic_version.version_num``;
* every table and column read is named in :data:`READ_SURFACE` — nothing else
  is opened, and in particular no table holding credentials (``auth``,
  ``api_key``, ``oauth_session``, ``config``) and not the ``user`` table, so
  no email, display name, or OAuth subject reaches a fact THROUGH THE APP
  DATABASE. The principal stored from that path is the instance's own opaque
  user uuid. The audit artifact is a SECOND input whose ``principal`` /
  ``model`` / ``provider`` are caller-supplied and unvalidated, so those are
  scrubbed on the way in rather than trusted;
* an observed revision that is not the pinned one is a REFUSAL
  (:class:`ChatSourceRefused`), never a best-effort parse of an unknown
  schema.

What it does NOT do
-------------------
It opens no connection to a deployment graph database, reads and writes no
graph table, and calls no publisher. It takes no ``dsn`` parameter, so the
adapter factory structurally cannot hand it one. It emits ``NodeFact`` and
``EdgeFact`` values and the ordinary ingest, admission, and publish path does
the rest.

What reaches the graph
----------------------
Not message text. Following the agent_sessions precedent, each message node
carries a BOUNDED, credential-redacted excerpt plus a sha256 of the full
(redacted) text. The context-audit artifact's payload bytes stay in operator
state as the byte authority; the graph holds ``payload_sha256``, the
artifact's injective ``record_id``, and a bounded excerpt.

Short-circuit contract
----------------------
``probe_short_circuit_safe = False``. This source resolves
``(chat_message, references, X)`` edges into OTHER sources' nodes, so it has
an input its own content probe cannot see (a target retracted elsewhere
changes this source's correct output without changing its file). That is the
runner's own rule for cross-source-target sources
(``SourceRunner._adapter_probe_short_circuit_safe``). A declared probe kind
alone does not prevent re-emission, and short-circuit acknowledgement would
additionally require the ``applied_raw_token`` / ``applied_token_scope`` /
``applied_token_authority`` triple, which this source does not claim.

Deletion
--------
Decided, using the substrate's named mechanism. A full-scope or reconcile run
returns ``SourceRun(record_set=RecordSet(...))`` declaring every conversation
the source owns; the runner diffs it against the prior cursor's set and emits
deterministic retractions for conversations the user deleted upstream.
Reporting ``completed_scope`` while silently keeping vanished conversations is
the failure that mechanism exists to exclude.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

from okg.substrate.chat.audit_record import (
    AUDIT_SCHEMA_VERSION,
    PAYLOAD_DIGEST_FIELD,
    PAYLOAD_FIELD,
    SOURCE_REVISION as AUDIT_SOURCE_REVISION,
    UnknownSchemaVersion,
    payload_sha256,
    read_records,
    resolve_audit_dir,
    schema_major,
)
from okg.substrate.chat.audit_filter import (
    CITATION_HEADER,
    CITATION_SEPARATOR,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._transcript_redaction import (
    scrub_transcript_text,
    transcript_secret_classes,
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
    source_state_without_runner_keys,
)


__all__ = [
    "CHAT_SESSIONS_SOURCE_CLASS",
    "CHAT_SESSIONS_SOURCE_MODULE",
    "CITED_SUBTYPE_ALLOWLIST",
    "EMISSION_SCHEMA_VERSION",
    "FORBIDDEN_TABLES",
    "PINNED_APP_DB_REVISION",
    "PINNED_OPENWEBUI_VERSION",
    "READ_SURFACE",
    "ChatSessionsSource",
    "ChatSourceRefused",
    "chat_sessions_source_registry_entry",
    "cited_subtype",
]


CHAT_SESSIONS_SOURCE_MODULE = "okg.substrate.library.sources.chat_sessions"
CHAT_SESSIONS_SOURCE_CLASS = "ChatSessionsSource"

#: The Open WebUI release this reader was written against and measured on.
#: Written against 0.11.0 (app-database revision ``f0bd01a18a3d``); moved to
#: 0.11.4 on 2026-09-29 after re-reading the schema. The three migrations in
#: between (``1ce6ade7d93b`` a group_member index, ``6d09d1bf1f23`` a user
#: oauth repair, ``d4c1a8e37b62`` a nullable ``chat.timer_at`` column and
#: chat indexes) touch no column in :data:`READ_SURFACE`; the only column
#: change on a table read here is the added ``chat.timer_at``, which this
#: reader does not open (``models/chats.py``, ``models/chat_messages.py`` and
#: ``models/feedbacks.py`` diffed between the two tags).
PINNED_OPENWEBUI_VERSION = "0.11.4"

#: ``alembic_version.version_num`` observed on that release. The reader
#: refuses any other value rather than parsing an unknown schema. The v0.11.4
#: head is ``d4c1a8e37b62`` (``migrations/versions/d4c1a8e37b62_add_chat_
#: timer_at_and_chat_indexes.py``; nothing names it as ``down_revision``).
PINNED_APP_DB_REVISION = "d4c1a8e37b62"

#: Bumped whenever emission changes shape, so the change probe re-emits.
EMISSION_SCHEMA_VERSION = "1.0"

#: EVERY table and column this source reads. Named as data so a reviewer can
#: check the claim against the code instead of taking it on faith, and so the
#: startup gate can verify each column actually exists on the pinned schema.
READ_SURFACE: dict[str, tuple[str, ...]] = {
    "alembic_version": ("version_num",),
    "chat": (
        "id",
        "user_id",
        "title",
        "created_at",
        "updated_at",
        "archived",
    ),
    "chat_message": (
        "id",
        "chat_id",
        "user_id",
        "role",
        "parent_id",
        "content",
        "model_id",
        "created_at",
        "updated_at",
    ),
    "feedback": (
        "id",
        "user_id",
        "type",
        "data",
        "meta",
        "created_at",
        "updated_at",
    ),
}

#: Tables this source must never open. Asserted by test, not by comment: the
#: app database also holds password hashes, API keys, and OAuth sessions.
FORBIDDEN_TABLES: frozenset[str] = frozenset({
    "auth",
    "api_key",
    "oauth_session",
    "config",
    "config_old",
    "user",
})

#: The CLOSED cited-subtype allowlist, longest prefix first.
#:
#: A narrowing catalog admits NAMED triples only -- there is no wildcard --
#: so "each cited subtype the deployment admits" is not an enumerable set for
#: a deployment-independent library module. This table is the enumeration,
#: and it is the same list the chat_sessions ontology module declares one
#: ``(chat_message, references, X)`` narrowing for. A citation whose id
#: matches no entry is DROPPED with the named reason
#: ``citation-subtype-not-allowlisted`` and counted in ``last_run_stats``,
#: so an out-of-allowlist citation is a visible adapter refusal rather than a
#: silent narrowing_violation at insert.
#:
#: The prefixes are the substrate's own ``output_signature.id_prefix``
#: convention, which every source registry entry already declares; this
#: source never invents a subtype for an id it does not recognize.
CITED_SUBTYPE_ALLOWLIST: tuple[tuple[str, str], ...] = (
    ("pact-change:", "pact_change"),
    ("pact-task:", "pact_task"),
    ("subsession:", "sub_agent_session"),
    ("test-case:", "test_case"),
    ("pr:github:", "pull_request"),
    ("issue:github:", "issue"),
    ("session:", "agent_session"),
    ("commit:", "git_commit"),
    ("change:", "open_spec_change"),
    ("chunk:", "document_chunk"),
    ("file:", "source_file"),
    ("doc:", "document"),
)

#: Named refusal reasons. Every one of them is a distinct state; none of them
#: is a value that compares equal to "there was nothing there".
REFUSAL_APP_DB_MISSING = "chat-app-db-missing"
REFUSAL_APP_DB_UNREADABLE = "chat-app-db-unreadable"
REFUSAL_APP_DB_REVISION_UNPINNED = "chat-app-db-revision-unpinned"
REFUSAL_APP_DB_SURFACE_MISSING = "chat-app-db-read-surface-missing"
REFUSAL_AUDIT_ARTIFACT_MISSING = "chat-audit-artifact-missing"
REFUSAL_AUDIT_ARTIFACT_UNREADABLE = "chat-audit-artifact-unreadable"
REFUSAL_AUDIT_SCHEMA_VERSION_UNKNOWN = "chat-audit-schema-version-unknown"
REFUSAL_AUDIT_PAYLOAD_DIGEST_MISMATCH = "chat-audit-payload-digest-mismatch"
REFUSAL_CITATION_NOT_ALLOWLISTED = "citation-subtype-not-allowlisted"
REFUSAL_FEEDBACK_META_UNPARSEABLE = "feedback-meta-unparseable"
REFUSAL_FEEDBACK_CHAT_UNKNOWN = "feedback-chat-id-unknown"
REFUSAL_RATING_MESSAGE_UNRESOLVED = "rating-message-unresolved"
REFUSAL_INSTANCE_NOT_INJECTIVE = "chat-instance-label-not-injective"
REFUSAL_AUDIT_TURN_SUPERSEDED = "audit-turn-superseded-by-later-record"
REFUSAL_RETRACTION_SUPPRESSED = "audit-refusal-without-readable-prior-record-set"

#: The audit filter's citation footer is the ONLY place a citation is read
#: from, and its grammar is IMPORTED from the producer rather than
#: re-spelled here. A node id typed by a person into a message is not a
#: citation and is never turned into a graph edge -- deriving one from free
#: text would put an assertion in the graph that nobody made.
_MARKDOWN_LINK_RE = re.compile(r"^\[(?P<text>[^\]]+)\]\((?P<url>[^)]*)\)$")

_ARTIFACT_GLOB = "records-*.jsonl"

_ID_PREFIXES = {
    "chat_session": "chat:",
    "chat_message": "chat-msg:",
    "chat_rating": "chat-rating:",
    "arena_preference": "chat-arena:",
    "chat_context_record": "chat-ctx:",
}


class ChatSourceRefused(SubstrateError):
    """The source refused to read, with a machine-readable reason.

    A refusal is not an empty upstream. Raising here is what keeps
    "the app database moved to a schema this reader does not know" from
    reaching the graph as "the operator held no conversations".
    """

    code = "chat_source_refused"

    def __init__(self, reason: str, message: str, **data: Any) -> None:
        super().__init__(
            message,
            data={"reason": reason, **data},
            fix_hint=(
                "if this deployment's chat instance still runs an older Open "
                "WebUI image, recreate it on the pinned image (okg "
                "chat-instance down --deployment <d>, then up) so its app "
                "database migrates to the Open WebUI "
                f"{PINNED_OPENWEBUI_VERSION} schema; otherwise point "
                "params.app_db_path at a readable copy of that instance's "
                "app database"
            ),
        )
        self.reason = reason


def cited_subtype(node_id: str) -> Optional[str]:
    """The allowlisted subtype for a cited node id, or None.

    None is the DROP signal, and the caller names it; it is never turned into
    a default subtype.
    """
    for prefix, subtype in CITED_SUBTYPE_ALLOWLIST:
        if node_id.startswith(prefix):
            return subtype
    return None


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _epoch_to_iso(value: Any) -> Optional[str]:
    """Open WebUI stores seconds (and occasionally milliseconds) as BIGINT."""
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    if seconds > 10_000_000_000:
        seconds = seconds // 1000
    from datetime import datetime, timezone

    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()


def _message_text(raw: Any) -> str:
    """Flatten Open WebUI's ``chat_message.content`` into text.

    The column is JSON and holds either a string or a list of content blocks
    (``{"type": "text", "text": ...}``). Both shapes are handled; an
    unrecognized shape is serialized rather than dropped, because dropping it
    would understate what the person actually sent.
    """
    value = raw
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return value
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for block in value:
            if isinstance(block, Mapping):
                text = block.get("text")
                parts.append(text if isinstance(text, str) else json.dumps(block, sort_keys=True))
            elif isinstance(block, str):
                parts.append(block)
            else:
                parts.append(json.dumps(block, sort_keys=True))
        return "\n".join(parts)
    return json.dumps(value, sort_keys=True)


def _json_value(raw: Any) -> Any:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None
    return raw


def _scrub_optional(value: Any) -> Any:
    """Redact a string in place, leaving a non-string (including None) alone.

    None must stay None: an absent field is a distinct state from an empty
    one, and turning it into ``""`` would make "the artifact recorded no
    provider" read as "the provider was blank".
    """
    return scrub_transcript_text(value) if isinstance(value, str) else value


def _prior_record_set(cursor: Any) -> tuple[Optional[RecordSet], bool]:
    """The prior run's declared footprint, and whether it is unreadable here.

    Three states, kept apart because collapsing them is how a read failure
    turns into a retraction: an INLINE prior set (readable), an EXTERNAL one
    held by reference (present but unreadable from inside the adapter), and no
    prior authority at all (a first run, with nothing published to lose).
    """
    if not isinstance(cursor, Mapping):
        return None, False
    raw = cursor.get("record_set")
    if raw is not None:
        try:
            return RecordSet.from_cursor(raw), False
        except ValueError:
            return None, True
    return None, cursor.get("record_set_ref") is not None


def _bounded(text: str, limit: int) -> tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    return text[:limit], True


def parse_citations(text: str) -> list[str]:
    """Node ids cited in the audit filter's own provenance footer.

    The grammar is the one ``okg.substrate.chat.audit_filter.render_citations``
    writes: a ``Sources:`` line, then one ``- <id or [id](url)> — <lineage>``
    bullet per cited row. Parsing our own footer is a contract between two
    children of the same umbrella; scanning free text for id-shaped tokens
    would manufacture citations nobody made.
    """
    lines = text.splitlines()
    try:
        start = next(
            index
            for index, line in enumerate(lines)
            if line.strip() == CITATION_HEADER
        )
    except StopIteration:
        return []
    found: list[str] = []
    for line in lines[start + 1:]:
        stripped = line.strip()
        if not stripped.startswith("- "):
            break
        body = stripped[2:]
        head = body.split(CITATION_SEPARATOR, 1)[0].strip()
        match = _MARKDOWN_LINK_RE.match(head)
        if match is not None:
            head = match.group("text").strip()
        if not head or head.startswith("("):
            # `(empty identifier)` is the renderer's marker for an empty id.
            continue
        if head not in found:
            found.append(head)
    return found


@dataclass
class ChatSessionsSource:
    """Substrate adapter for Open WebUI chat conversations.

    Constructed from the registry entry's ``params``. Note the absent ``dsn``
    parameter: the adapter factory injects ``dsn`` only into constructors that
    declare it, so this source structurally cannot be handed a graph
    connection.
    """

    #: Absolute path to a readable copy or mount of the instance's app
    #: database. A containerized instance keeps the file inside its volume, so
    #: the operator exposes it (bind mount, or a snapshot copy) and names the
    #: path here. Absence is a refusal, not an empty read.
    app_db_path: Path | str
    #: Label for the chat instance. Part of every node id, because two
    #: deployments' instances mint ids in disjoint databases.
    instance: str = "default"
    #: Deployment + preset, used only to locate the context-audit artifact.
    deployment: Optional[str] = None
    preset: Optional[str] = None
    audit_state_home: Path | str | None = None
    #: Bound on every excerpt written to the graph.
    excerpt_chars: int = 400
    #: The pinned app-database migration revision. Overridable in the registry
    #: so an operator can move the pin DELIBERATELY after re-reading the vendor
    #: schema; there is no "accept anything" value.
    app_db_revision: str = PINNED_APP_DB_REVISION
    openwebui_version: str = PINNED_OPENWEBUI_VERSION
    audit_schema_version: str = AUDIT_SCHEMA_VERSION
    source_name: str = "chat-sessions"

    name: str = "chat_sessions"
    profile: str = "discovery_crawl"

    #: The app database file plus the audit artifacts are the whole read
    #: scope, so a content digest over them covers this source exactly.
    change_probe_kind: str = "content_hash"

    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        # Node ids are `<prefix><instance>:<remote id>`. A colon inside the
        # instance label makes that composition NON-INJECTIVE -- instance
        # "a:b" with chat "c" and instance "a" with chat "b:c" both mint
        # `chat:a:b:c` -- and the instance segment exists precisely to keep
        # two deployments' ids apart. Refused at construction rather than
        # producing a collision nobody would notice.
        if ":" in str(self.instance) or not str(self.instance).strip():
            raise ChatSourceRefused(
                REFUSAL_INSTANCE_NOT_INJECTIVE,
                f"the chat instance label {self.instance!r} is empty or "
                "contains ':'; node ids compose as <prefix><instance>:<remote "
                "id>, so a colon there makes the composition non-injective",
                instance=str(self.instance),
            )
        self.app_db_path = Path(self.app_db_path).expanduser()
        if self.audit_state_home is not None:
            self.audit_state_home = Path(self.audit_state_home).expanduser()
        self.excerpt_chars = int(self.excerpt_chars)
        self.change_probe = self._build_change_probe()

    # -- short-circuit contract -----------------------------------------

    @property
    def probe_short_circuit_safe(self) -> bool:
        """Always False; see the module docstring.

        The content probe hashes this source's OWN inputs. The
        ``(chat_message, references, X)`` edges resolve into other sources'
        nodes, an input the probe cannot see, so the runner must not
        short-circuit the reader on an unchanged verdict.
        """
        return False

    # -- probe ----------------------------------------------------------

    def _probe_items(self) -> list[tuple[str, Path]]:
        items: list[tuple[str, Path]] = []
        db = Path(self.app_db_path)
        if db.is_file():
            items.append((f"app_db::{db.name}", db))
        # WAL and shared-memory sidecars carry committed rows the main file
        # does not yet hold. Leaving them out of the digest would let a run
        # that only appended to the WAL read as "unchanged upstream".
        for suffix in ("-wal", "-shm"):
            sidecar = db.with_name(db.name + suffix)
            if sidecar.is_file():
                items.append((f"app_db{suffix}::{sidecar.name}", sidecar))
        audit_dir = self._audit_dir()
        if audit_dir is not None and audit_dir.is_dir():
            for artifact in sorted(audit_dir.glob(_ARTIFACT_GLOB)):
                if artifact.is_file():
                    items.append((f"audit::{artifact.name}", artifact))
        return items

    def _build_change_probe(self) -> Any:
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        return ContentHashProbe(
            content_items=self._probe_items,
            config={
                "instance": self.instance,
                "app_db_path": str(self.app_db_path),
                "app_db_revision": self.app_db_revision,
                "openwebui_version": self.openwebui_version,
                "audit_schema_version": self.audit_schema_version,
                "excerpt_chars": self.excerpt_chars,
                "emission_schema_version": EMISSION_SCHEMA_VERSION,
                "cited_subtype_allowlist": [
                    subtype for _prefix, subtype in CITED_SUBTYPE_ALLOWLIST
                ],
            },
            emit_targets=[sys.modules[__name__]],
        )

    # -- upstream gate --------------------------------------------------

    def _audit_dir(self) -> Optional[Path]:
        if not self.deployment or not self.preset:
            return None
        return resolve_audit_dir(
            self.deployment,
            self.preset,
            state_home=self.audit_state_home,
        )

    def _connect(self) -> sqlite3.Connection:
        """Open the app database READ-ONLY, or refuse with a named reason."""
        path = Path(self.app_db_path)
        if not path.is_file():
            raise ChatSourceRefused(
                REFUSAL_APP_DB_MISSING,
                f"the Open WebUI app database {path} does not exist; a "
                "missing upstream is a refusal, never an empty conversation "
                "list",
                app_db_path=str(path),
            )
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        except sqlite3.Error as exc:
            raise ChatSourceRefused(
                REFUSAL_APP_DB_UNREADABLE,
                f"the Open WebUI app database {path} could not be opened "
                f"read-only: {exc}",
                app_db_path=str(path),
            ) from exc
        conn.row_factory = sqlite3.Row
        try:
            self._verify_pinned_schema(conn)
        except Exception:
            conn.close()
            raise
        return conn

    def _verify_pinned_schema(self, conn: sqlite3.Connection) -> str:
        """Refuse unless the observed revision is exactly the pinned one."""
        try:
            rows = conn.execute(
                "SELECT version_num FROM alembic_version"
            ).fetchall()
        except sqlite3.Error as exc:
            raise ChatSourceRefused(
                REFUSAL_APP_DB_REVISION_UNPINNED,
                "the app database declares no alembic_version table, so its "
                "migration revision cannot be compared against the pinned "
                f"{self.app_db_revision!r}; refusing rather than parsing an "
                f"unknown schema ({exc})",
                expected_revision=self.app_db_revision,
            ) from exc
        observed = [str(row[0]) for row in rows]
        if observed != [self.app_db_revision]:
            raise ChatSourceRefused(
                REFUSAL_APP_DB_REVISION_UNPINNED,
                "the app database migration revision "
                f"{observed!r} is not the pinned "
                f"{[self.app_db_revision]!r} for Open WebUI "
                f"{self.openwebui_version}; refusing rather than "
                "best-effort-parsing an unknown schema",
                expected_revision=self.app_db_revision,
                observed_revision=observed,
            )
        self._verify_read_surface(conn)
        return self.app_db_revision

    def _verify_read_surface(self, conn: sqlite3.Connection) -> None:
        """Every column in :data:`READ_SURFACE` must exist on this schema.

        A pinned revision whose tables have drifted is still an unknown
        schema. Checking the columns is what makes the pin mean something
        beyond a string comparison.
        """
        for table, columns in READ_SURFACE.items():
            try:
                present = {
                    str(row[1])
                    for row in conn.execute(f"PRAGMA table_info('{table}')")
                }
            except sqlite3.Error as exc:  # pragma: no cover - PRAGMA is total
                raise ChatSourceRefused(
                    REFUSAL_APP_DB_SURFACE_MISSING,
                    f"the app database table {table!r} could not be "
                    f"inspected: {exc}",
                    table=table,
                ) from exc
            missing = sorted(set(columns) - present)
            if missing:
                raise ChatSourceRefused(
                    REFUSAL_APP_DB_SURFACE_MISSING,
                    f"the app database table {table!r} is missing column(s) "
                    f"{missing} that this reader requires; refusing rather "
                    "than emitting partial rows",
                    table=table,
                    missing_columns=missing,
                )

    def preflight(self) -> SourceHealth:
        """Open the upstream and report health without emitting facts."""
        try:
            conn = self._connect()
        except ChatSourceRefused as exc:
            return SourceHealth(
                status="endpoint_failed",
                mode="live",
                reason=f"{exc.reason}: {exc}",
                endpoint=str(self.app_db_path),
            )
        try:
            count = int(
                conn.execute("SELECT count(*) FROM chat").fetchone()[0] or 0
            )
        finally:
            conn.close()
        return SourceHealth(
            status="ok",
            mode="live",
            record_count=count,
            endpoint=str(self.app_db_path),
            cache_path=str(self.app_db_path),
        )

    # -- audit artifact --------------------------------------------------

    def _load_audit_records(
        self, stats: dict[str, Any],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        """Join the artifact's inlet + completion records, or refuse.

        Returns a mapping from ``(conversation_id, message_id)`` to the merged
        record. A refusal yields an EMPTY mapping AND a named reason in
        ``stats``: no ``chat_context_record`` fact is emitted for any message,
        and no defaulted context is emitted in its place.
        """
        audit_dir = self._audit_dir()
        if audit_dir is None:
            stats["audit_refusal"] = {
                "reason": REFUSAL_AUDIT_ARTIFACT_MISSING,
                "detail": (
                    "the source declares no deployment/preset, so the audit "
                    "artifact cannot be located"
                ),
            }
            return {}
        artifacts = (
            sorted(audit_dir.glob(_ARTIFACT_GLOB)) if audit_dir.is_dir() else []
        )
        if not artifacts:
            stats["audit_refusal"] = {
                "reason": REFUSAL_AUDIT_ARTIFACT_MISSING,
                "detail": f"no audit artifact under {audit_dir}",
            }
            return {}

        inlets: dict[str, dict[str, Any]] = {}
        completions: dict[str, dict[str, Any]] = {}
        for artifact in artifacts:
            try:
                records = read_records(
                    artifact, schema_version=self.audit_schema_version
                )
            except FileNotFoundError:
                stats["audit_refusal"] = {
                    "reason": REFUSAL_AUDIT_ARTIFACT_MISSING,
                    "detail": f"{artifact} vanished between listing and read",
                }
                return {}
            except UnknownSchemaVersion as exc:
                stats["audit_refusal"] = {
                    "reason": REFUSAL_AUDIT_SCHEMA_VERSION_UNKNOWN,
                    "detail": (
                        f"{exc}; this reader knows audit schema major "
                        f"{schema_major(self.audit_schema_version)!r}"
                    ),
                }
                return {}
            except OSError as exc:
                stats["audit_refusal"] = {
                    "reason": REFUSAL_AUDIT_ARTIFACT_UNREADABLE,
                    "detail": f"{artifact}: {exc}",
                }
                return {}
            for record in records:
                record_id = record.get("record_id")
                if not isinstance(record_id, str) or not record_id:
                    continue
                if record.get("record_kind") == "completion":
                    completions[record_id] = record
                else:
                    inlets[record_id] = record

        joined: dict[tuple[str, str], dict[str, Any]] = {}
        digest_mismatches = 0
        superseded_turns = 0
        for record_id, inlet in sorted(
            inlets.items(), key=lambda kv: int(kv[1].get("record_seq") or 0),
        ):
            payload = inlet.get(PAYLOAD_FIELD)
            digest = inlet.get(PAYLOAD_DIGEST_FIELD)
            if not isinstance(payload, str) or not isinstance(digest, str):
                continue
            if payload_sha256(payload) != digest:
                # The artifact's own contract is
                # `payload_sha256 == sha256(payload)`. A record that breaks it
                # is not a record this source republishes a digest for.
                digest_mismatches += 1
                continue
            conversation = inlet.get("conversation_id")
            message = inlet.get("message_id")
            if not isinstance(conversation, str) or not isinstance(message, str):
                continue
            merged = dict(inlet)
            completion = completions.get(record_id)
            if completion is not None:
                merged["graph_read"] = completion.get("graph_read")
                merged["generation_agreement"] = completion.get(
                    "generation_agreement"
                )
                merged["observed_generation_id"] = completion.get(
                    "observed_generation_id"
                )
            # A regenerated answer appends a SECOND inlet for the same message
            # id. Records are ordered by `record_seq`, so the newest wins --
            # and the displacement is COUNTED rather than silent, because "one
            # turn per message" is an assumption about the vendor, not a fact.
            if (conversation, message) in joined:
                superseded_turns += 1
            joined[(conversation, message)] = merged
        if digest_mismatches:
            stats.setdefault("audit_warnings", []).append({
                "reason": REFUSAL_AUDIT_PAYLOAD_DIGEST_MISMATCH,
                "count": digest_mismatches,
            })
        if superseded_turns:
            stats.setdefault("audit_warnings", []).append({
                "reason": REFUSAL_AUDIT_TURN_SUPERSEDED,
                "count": superseded_turns,
            })
        stats["audit_records"] = len(joined)
        return joined

    @staticmethod
    def _audit_fingerprints(
        audit: Mapping[tuple[str, str], Mapping[str, Any]],
    ) -> dict[str, list[Any]]:
        """Per-conversation audit state, folded into the change digest.

        Without this the conversation's content hash is blind to the artifact:
        a turn whose completion record is appended AFTER the conversation was
        first emitted would leave the digest unchanged, and the
        ``chat_context_record`` fact would never be emitted at all -- the node
        would be declared in the record set and never created. The completion
        record is appended after the inlet by construction, so this is the
        normal case, not an edge case.
        """
        fingerprints: dict[str, list[Any]] = {}
        for (conversation, message), record in sorted(audit.items()):
            fingerprints.setdefault(conversation, []).append([
                message,
                record.get("record_id"),
                record.get(PAYLOAD_DIGEST_FIELD),
                record.get("graph_read"),
                record.get("generation_agreement"),
                record.get("observed_generation_id"),
            ])
        return fingerprints

    # -- run -------------------------------------------------------------

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[Mapping[str, Any]] = None,
        change_probe_verdict: Any = None,
        authoritative_replay: bool = False,
        **_ignored: Any,
    ) -> SourceRun:
        completed_scope = mode in {"reconcile", "scope_complete"}
        prior = source_state_without_runner_keys(cursor)
        prior_hashes = prior.get("conversation_hashes")
        prior_hashes = prior_hashes if isinstance(prior_hashes, Mapping) else {}
        prior_emission = prior.get("emission_schema_version")

        stats: dict[str, Any] = {
            "run_mode": mode,
            "instance": self.instance,
            "app_db_revision": self.app_db_revision,
            "openwebui_version": self.openwebui_version,
            "conversations_upstream": 0,
            "conversations_emitted": 0,
            "conversations_unchanged": 0,
            "messages_emitted": 0,
            "ratings_emitted": 0,
            "arena_preferences_emitted": 0,
            "context_records_emitted": 0,
            "citations_emitted": 0,
            "citations_dropped": 0,
            "redacted_messages": 0,
            "ratings_dropped": 0,
            "feedback_dropped": 0,
        }

        conn = self._connect()
        try:
            conversations = self._read_conversations(conn, stats)
            audit = self._load_audit_records(stats)
            audit_fingerprints = self._audit_fingerprints(audit)
            prior_record_set, prior_authority_unreadable = (
                _prior_record_set(cursor)
            )
            emissions: dict[str, RecordEmission] = {}
            facts: list[Any] = []
            hashes: dict[str, str] = {}
            stats["conversations_upstream"] = len(conversations)
            reemit_all = prior_emission != EMISSION_SCHEMA_VERSION
            for conversation in conversations:
                chat_id = conversation["id"]
                digest = self._conversation_digest(
                    conversation, audit_fingerprints.get(chat_id, []),
                )
                hashes[chat_id] = digest
                unchanged = (
                    not reemit_all
                    and prior_hashes.get(chat_id) == digest
                )
                emission = self._emit_conversation(
                    conversation,
                    digest=digest,
                    audit=audit,
                    stats=stats,
                    facts=facts if not unchanged else None,
                )
                emissions[chat_id] = emission
                if unchanged:
                    stats["conversations_unchanged"] += 1
                else:
                    stats["conversations_emitted"] += 1
        finally:
            conn.close()

        # An audit REFUSAL must not retract context this source already
        # published. Without this, a missing or unreadable artifact would take
        # the `chat_context_record` nodes out of the record set and the runner
        # would read their disappearance as a deletion -- a read failure
        # ERASING recorded context, which is worse than the "an absent context
        # MUST NOT read as no context was sent" clause it would violate.
        declare_record_set = completed_scope
        if completed_scope and stats.get("audit_refusal"):
            if prior_authority_unreadable:
                # The prior footprint is held by REFERENCE and cannot be read
                # here, so no complete set can be declared honestly. Declining
                # completed_scope suppresses retraction entirely, which is the
                # fail-safe direction.
                declare_record_set = False
                completed_scope = False
                stats["retraction_suppressed"] = {
                    "reason": REFUSAL_RETRACTION_SUPPRESSED,
                    "detail": (
                        "the audit artifact refused and the prior record set "
                        "is external, so this run declines completed_scope "
                        "rather than risk retracting recorded context"
                    ),
                }
            elif prior_record_set is not None:
                stats["context_records_carried"] = (
                    self._carry_prior_context_records(
                        emissions, prior_record_set,
                    )
                )

        next_cursor: dict[str, Any] = {
            "instance": self.instance,
            "app_db_revision": self.app_db_revision,
            "emission_schema_version": EMISSION_SCHEMA_VERSION,
            "conversation_hashes": hashes,
        }
        self.last_run_stats = stats
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor=next_cursor,
            run_mode=mode,
            # An EMPTY record set (not None) is what makes the last
            # conversation's deletion retract: the runner gates retraction on
            # `record_set is not None`, so returning None here would leave a
            # deleted conversation's rows published forever.
            record_set=(
                RecordSet(records=emissions) if declare_record_set else None
            ),
            health=SourceHealth(
                status="ok",
                mode="live",
                record_count=len(conversations),
                endpoint=str(self.app_db_path),
            ),
        )

    def _carry_prior_context_records(
        self,
        emissions: dict[str, RecordEmission],
        prior: RecordSet,
    ) -> int:
        """Re-declare context records this run could not read.

        Returns how many were carried forward.
        """
        carried = 0
        for chat_id, emission in list(emissions.items()):
            previous = prior.records.get(chat_id)
            if previous is None:
                continue
            known = emission.nodes_by_id()
            extra_nodes = [
                node for node in previous.descendants
                if node.subtype == "chat_context_record"
                and node.node_id not in known
            ]
            if not extra_nodes:
                continue
            keep_ids = {node.node_id for node in extra_nodes}
            existing_edges = emission.edges_by_identity()
            extra_edges = [
                edge for edge in previous.edges
                if edge.dst in keep_ids
                and edge.identity() not in existing_edges
            ]
            emissions[chat_id] = RecordEmission(
                primary=emission.primary,
                descendants=[*emission.descendants, *extra_nodes],
                edges=[*emission.edges, *extra_edges],
            )
            carried += len(extra_nodes)
        return carried

    # -- reading ---------------------------------------------------------

    def _read_conversations(
        self, conn: sqlite3.Connection, stats: dict[str, Any],
    ) -> list[dict[str, Any]]:
        chats = [
            dict(row)
            for row in conn.execute(
                "SELECT id, user_id, title, created_at, updated_at, archived "
                "FROM chat ORDER BY id"
            )
        ]
        by_id = {chat["id"]: chat for chat in chats}
        for chat in chats:
            chat["messages"] = []
            chat["ratings"] = []
            chat["arena"] = []
        for row in conn.execute(
            "SELECT id, chat_id, user_id, role, parent_id, content, model_id, "
            "created_at, updated_at FROM chat_message ORDER BY created_at, id"
        ):
            chat = by_id.get(row["chat_id"])
            if chat is not None:
                chat["messages"].append(dict(row))
        for row in conn.execute(
            "SELECT id, user_id, type, data, meta, created_at, updated_at "
            "FROM feedback ORDER BY created_at, id"
        ):
            record = dict(row)
            meta = _json_value(record.get("meta"))
            data = _json_value(record.get("data"))
            record["meta_parsed"] = meta if isinstance(meta, Mapping) else {}
            record["data_parsed"] = data if isinstance(data, Mapping) else {}
            chat_id = record["meta_parsed"].get("chat_id")
            chat = by_id.get(chat_id) if isinstance(chat_id, str) else None
            if chat is None:
                # A preference the graph will not carry. COUNTED with a named
                # reason, because "the operator left no feedback" and "the
                # feedback did not resolve to a conversation" are different
                # facts and this source must not report the second as the
                # first.
                stats["feedback_dropped"] += 1
                stats.setdefault("feedback_drops", []).append({
                    "reason": (
                        REFUSAL_FEEDBACK_META_UNPARSEABLE
                        if not isinstance(meta, Mapping)
                        else REFUSAL_FEEDBACK_CHAT_UNKNOWN
                    ),
                    "feedback_id": str(record.get("id")),
                })
                continue
            siblings = record["data_parsed"].get("sibling_model_ids")
            is_arena = bool(record["meta_parsed"].get("arena")) or bool(siblings)
            (chat["arena"] if is_arena else chat["ratings"]).append(record)
        return chats

    def _conversation_digest(
        self,
        conversation: Mapping[str, Any],
        audit_fingerprint: Any = (),
    ) -> str:
        """A content hash over everything this source emits for a conversation.

        Keyed on content rather than on ``chat.updated_at``: a rating lives in
        a DIFFERENT table and does NOT move ``chat.updated_at``, so an
        updated_at cursor would silently miss exactly the preference facts
        this source exists to carry.

        ``audit_fingerprint`` is part of the digest for the same reason. The
        completion record is appended AFTER the inlet by construction, and the
        artifact may be written long after the conversation was first
        ingested; without it the digest would say "unchanged" and the
        ``chat_context_record`` fact would never be emitted at all.
        """
        payload = {
            "audit": list(audit_fingerprint),
            "chat": {
                key: conversation.get(key)
                for key in ("id", "user_id", "title", "created_at",
                            "updated_at", "archived")
            },
            "messages": [
                {
                    key: message.get(key)
                    for key in ("id", "role", "parent_id", "content",
                                "model_id", "created_at", "updated_at")
                }
                for message in conversation["messages"]
            ],
            "ratings": [
                {key: record.get(key) for key in ("id", "updated_at")}
                for record in conversation["ratings"]
            ],
            "arena": [
                {key: record.get(key) for key in ("id", "updated_at")}
                for record in conversation["arena"]
            ],
        }
        return _sha256(json.dumps(payload, sort_keys=True, default=str))

    # -- emission --------------------------------------------------------

    def _node_id(self, subtype: str, remote_id: str) -> str:
        return f"{_ID_PREFIXES[subtype]}{self.instance}:{remote_id}"

    def _record_id(self, chat_id: str) -> dict[str, Any]:
        return {"instance": self.instance, "chat_id": chat_id}

    def _revision(self, digest: str) -> dict[str, Any]:
        return {"content_hash": digest}

    def _emit_conversation(
        self,
        conversation: Mapping[str, Any],
        *,
        digest: str,
        audit: Mapping[tuple[str, str], Mapping[str, Any]],
        stats: dict[str, Any],
        facts: Optional[list[Any]],
    ) -> RecordEmission:
        """Build the conversation's footprint; append facts when emitting.

        ``facts=None`` means the conversation is unchanged since the prior
        cursor: its footprint is still declared IN FULL (so the record set
        stays complete and a deletion elsewhere still retracts) but nothing is
        re-emitted. Declaring a PARTIAL footprint here would make an ordinary
        no-change reconcile retract every message, rating, preference, and
        context record under the conversation.
        """
        chat_id = str(conversation["id"])
        record_id = self._record_id(chat_id)
        revision = self._revision(digest)
        session_node_id = self._node_id("chat_session", chat_id)
        descendants: list[RecordNode] = []
        edges: list[EdgeKey] = []

        title_raw = conversation.get("title") or ""
        title_clean, _ = _bounded(
            scrub_transcript_text(str(title_raw)), self.excerpt_chars
        )
        models = sorted({
            str(message.get("model_id"))
            for message in conversation["messages"]
            if message.get("model_id")
        })
        session_attrs = {
            "chat_session_id": session_node_id,
            "instance": self.instance,
            "remote_chat_id": chat_id,
            "title": title_clean,
            "principal": conversation.get("user_id"),
            "archived": bool(conversation.get("archived")),
            "message_count": len(conversation["messages"]),
            "models": ",".join(models),
            "openwebui_version": self.openwebui_version,
            "app_db_revision": self.app_db_revision,
            "started_at": _epoch_to_iso(conversation.get("created_at")),
            "updated_at": _epoch_to_iso(conversation.get("updated_at")),
            "text": title_clean,
        }
        if facts is not None:
            facts.append(NodeFact(
                node_id=session_node_id,
                subtype="chat_session",
                attrs=session_attrs,
                source_record_id=record_id,
                source_revision=revision,
            ))

        # Resolved ONCE, before the message loop, so an unresolvable rating is
        # counted once with a named reason rather than either silently skipped
        # or counted once per message.
        rating_targets: dict[str, Optional[str]] = {}
        for rating in conversation["ratings"]:
            target = self._rating_target(rating, conversation)
            rating_targets[str(rating["id"])] = target
            if target is None:
                stats["ratings_dropped"] += 1
                stats.setdefault("rating_drops", []).append({
                    "reason": REFUSAL_RATING_MESSAGE_UNRESOLVED,
                    "feedback_id": str(rating.get("id")),
                })

        for message in conversation["messages"]:
            message_node_id, message_nodes, message_edges = self._emit_message(
                conversation=conversation,
                message=message,
                session_node_id=session_node_id,
                record_id=record_id,
                revision=revision,
                audit=audit,
                stats=stats,
                facts=facts,
            )
            descendants.extend(message_nodes)
            edges.extend(message_edges)
            for rating in conversation["ratings"]:
                if rating_targets[str(rating["id"])] != message_node_id:
                    continue
                rating_node, rating_edge = self._emit_rating(
                    rating=rating,
                    conversation=conversation,
                    message_node_id=message_node_id,
                    record_id=record_id,
                    revision=revision,
                    stats=stats,
                    facts=facts,
                )
                descendants.append(rating_node)
                edges.append(rating_edge)

        for preference in conversation["arena"]:
            arena_node, arena_edge = self._emit_arena_preference(
                preference=preference,
                conversation=conversation,
                session_node_id=session_node_id,
                record_id=record_id,
                revision=revision,
                stats=stats,
                facts=facts,
            )
            descendants.append(arena_node)
            edges.append(arena_edge)

        return RecordEmission(
            primary=RecordNode(
                node_id=session_node_id,
                subtype="chat_session",
                source_record_id=record_id,
                source_revision=revision,
            ),
            descendants=descendants,
            edges=edges,
        )

    def _emit_message(
        self,
        *,
        conversation: Mapping[str, Any],
        message: Mapping[str, Any],
        session_node_id: str,
        record_id: Mapping[str, Any],
        revision: Mapping[str, Any],
        audit: Mapping[tuple[str, str], Mapping[str, Any]],
        stats: dict[str, Any],
        facts: Optional[list[Any]],
    ) -> tuple[str, list[RecordNode], list[EdgeKey]]:
        chat_id = str(conversation["id"])
        remote_id = str(message["id"])
        node_id = self._node_id("chat_message", remote_id)
        raw_text = _message_text(message.get("content"))
        secret_classes = transcript_secret_classes(raw_text)
        clean_text = scrub_transcript_text(raw_text)
        if secret_classes:
            stats["redacted_messages"] += 1
        excerpt, truncated = _bounded(clean_text, self.excerpt_chars)
        attrs = {
            "chat_message_id": node_id,
            "instance": self.instance,
            "remote_message_id": remote_id,
            "remote_chat_id": chat_id,
            "role": message.get("role"),
            "model_id": message.get("model_id"),
            "principal": message.get("user_id"),
            "excerpt": excerpt,
            "excerpt_truncated": truncated,
            # Computed over the REDACTED text, so the digest never certifies a
            # secret the graph does not hold.
            "content_sha256": _sha256(clean_text),
            "content_chars": len(clean_text),
            "redacted_secret_classes": ",".join(secret_classes),
            "sent_at": _epoch_to_iso(message.get("created_at")),
            "text": excerpt,
        }
        nodes = [RecordNode(
            node_id=node_id,
            subtype="chat_message",
            source_record_id=dict(record_id),
            source_revision=dict(revision),
        )]
        edges = [EdgeKey(
            src=session_node_id,
            dst=node_id,
            edge_type="contains",
            source_record_id=dict(record_id),
            source_revision=dict(revision),
        )]
        if facts is not None:
            stats["messages_emitted"] += 1
            facts.append(NodeFact(
                node_id=node_id,
                subtype="chat_message",
                attrs=attrs,
                source_record_id=dict(record_id),
                source_revision=dict(revision),
            ))
            facts.append(EdgeFact(
                src=session_node_id,
                dst=node_id,
                edge_type="contains",
                source_record_id=dict(record_id),
                source_revision=dict(revision),
            ))

        for cited in parse_citations(clean_text):
            subtype = cited_subtype(cited)
            if subtype is None:
                stats["citations_dropped"] += 1
                stats.setdefault("citation_drops", []).append({
                    "reason": REFUSAL_CITATION_NOT_ALLOWLISTED,
                    "node_id": cited,
                })
                continue
            edges.append(EdgeKey(
                src=node_id,
                dst=cited,
                edge_type="references",
                source_record_id=dict(record_id),
                source_revision=dict(revision),
            ))
            if facts is not None:
                stats["citations_emitted"] += 1
                facts.append(EdgeFact(
                    src=node_id,
                    dst=cited,
                    edge_type="references",
                    attrs={"cited_subtype": subtype},
                    source_record_id=dict(record_id),
                    source_revision=dict(revision),
                ))

        context = self._audit_record_for(audit, chat_id, remote_id)
        if context is not None:
            context_node, context_edge = self._emit_context_record(
                context=context,
                message_node_id=node_id,
                record_id=record_id,
                stats=stats,
                facts=facts,
            )
            nodes.append(context_node)
            edges.append(context_edge)
        return node_id, nodes, edges

    @staticmethod
    def _audit_record_for(
        audit: Mapping[tuple[str, str], Mapping[str, Any]],
        chat_id: str,
        remote_message_id: str,
    ) -> Optional[Mapping[str, Any]]:
        """Join an artifact record to a stored message.

        Open WebUI's ``chat_message.id`` is ``<chat id>-<message uuid>`` while
        the metadata the filter sees carries the BARE uuid. Both spellings are
        tried; nothing else is guessed.
        """
        candidates = [remote_message_id]
        prefix = f"{chat_id}-"
        if remote_message_id.startswith(prefix):
            candidates.append(remote_message_id[len(prefix):])
        for candidate in candidates:
            found = audit.get((chat_id, candidate))
            if found is not None:
                return found
        return None

    def _emit_context_record(
        self,
        *,
        context: Mapping[str, Any],
        message_node_id: str,
        record_id: Mapping[str, Any],
        stats: dict[str, Any],
        facts: Optional[list[Any]],
    ) -> tuple[RecordNode, EdgeKey]:
        audit_record_id = str(context["record_id"])
        node_id = self._node_id("chat_context_record", audit_record_id)
        payload = str(context.get(PAYLOAD_FIELD) or "")
        excerpt, _ = _bounded(
            scrub_transcript_text(payload), self.excerpt_chars
        )
        # The artifact record_id is INJECTIVE (`<artifact uuid4>:<seq>`), so it
        # addresses exactly one record; `source_revision` is fixed because
        # records are immutable.
        context_record_id = {**record_id, "audit_record_id": audit_record_id}
        context_revision = {"audit_source_revision": AUDIT_SOURCE_REVISION}
        attrs = {
            "chat_context_record_id": node_id,
            "instance": self.instance,
            "audit_record_id": audit_record_id,
            "audit_schema_version": context.get("schema_version"),
            "payload_sha256": context.get(PAYLOAD_DIGEST_FIELD),
            "payload_chars": len(payload),
            "payload_excerpt": excerpt,
            "task_kind": context.get("task_kind"),
            # Redacted like any other artifact-supplied string. These three
            # come from the chat instance's own request metadata, which is
            # caller-supplied and unvalidated: the "no credential reaches a
            # fact" claim cannot rest on the app-database read surface alone.
            "model": _scrub_optional(context.get("model")),
            "provider": _scrub_optional(context.get("provider")),
            "principal": _scrub_optional(context.get("principal")),
            "compaction_enabled": context.get("compaction_enabled"),
            "compaction_observed": context.get("compaction_observed"),
            "graph_read": context.get("graph_read"),
            "generation_agreement": context.get("generation_agreement"),
            "observed_generation_id": context.get("observed_generation_id"),
            "recorded_at": context.get("timestamp"),
            "text": excerpt,
        }
        node = RecordNode(
            node_id=node_id,
            subtype="chat_context_record",
            source_record_id=context_record_id,
            source_revision=context_revision,
        )
        edge = EdgeKey(
            src=message_node_id,
            dst=node_id,
            edge_type="contains",
            source_record_id=dict(record_id),
            source_revision=dict(context_revision),
        )
        if facts is not None:
            stats["context_records_emitted"] += 1
            facts.append(NodeFact(
                node_id=node_id,
                subtype="chat_context_record",
                attrs=attrs,
                source_record_id=context_record_id,
                source_revision=context_revision,
            ))
            facts.append(EdgeFact(
                src=message_node_id,
                dst=node_id,
                edge_type="contains",
                source_record_id=dict(record_id),
                source_revision=dict(context_revision),
            ))
        return node, edge

    def _rating_target(
        self, rating: Mapping[str, Any], conversation: Mapping[str, Any],
    ) -> Optional[str]:
        meta = rating.get("meta_parsed") or {}
        message_id = meta.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            return None
        chat_id = str(conversation["id"])
        composite = (
            message_id
            if message_id.startswith(f"{chat_id}-")
            else f"{chat_id}-{message_id}"
        )
        known = {str(message["id"]) for message in conversation["messages"]}
        for candidate in (composite, message_id):
            if candidate in known:
                return self._node_id("chat_message", candidate)
        return None

    def _emit_rating(
        self,
        *,
        rating: Mapping[str, Any],
        conversation: Mapping[str, Any],
        message_node_id: str,
        record_id: Mapping[str, Any],
        revision: Mapping[str, Any],
        stats: dict[str, Any],
        facts: Optional[list[Any]],
    ) -> tuple[RecordNode, EdgeKey]:
        remote_id = str(rating["id"])
        node_id = self._node_id("chat_rating", remote_id)
        data = rating.get("data_parsed") or {}
        comment = scrub_transcript_text(str(data.get("comment") or ""))
        comment_excerpt, _ = _bounded(comment, self.excerpt_chars)
        attrs = {
            "chat_rating_id": node_id,
            "instance": self.instance,
            "rated_message_id": message_node_id,
            "remote_chat_id": str(conversation["id"]),
            "principal": rating.get("user_id"),
            "rating": (
                str(data.get("rating"))
                if data.get("rating") is not None else None
            ),
            "model_id": data.get("model_id"),
            "reason": data.get("reason"),
            "comment_excerpt": comment_excerpt,
            "rated_at": _epoch_to_iso(rating.get("created_at")),
            "text": f"{data.get('rating')} {data.get('reason') or ''}".strip(),
        }
        node = RecordNode(
            node_id=node_id,
            subtype="chat_rating",
            source_record_id=dict(record_id),
            source_revision=dict(revision),
        )
        edge = EdgeKey(
            src=message_node_id,
            dst=node_id,
            edge_type="contains",
            source_record_id=dict(record_id),
            source_revision=dict(revision),
        )
        if facts is not None:
            stats["ratings_emitted"] += 1
            facts.append(NodeFact(
                node_id=node_id,
                subtype="chat_rating",
                attrs=attrs,
                source_record_id=dict(record_id),
                source_revision=dict(revision),
            ))
            facts.append(EdgeFact(
                src=message_node_id,
                dst=node_id,
                edge_type="contains",
                source_record_id=dict(record_id),
                source_revision=dict(revision),
            ))
        return node, edge

    def _emit_arena_preference(
        self,
        *,
        preference: Mapping[str, Any],
        conversation: Mapping[str, Any],
        session_node_id: str,
        record_id: Mapping[str, Any],
        revision: Mapping[str, Any],
        stats: dict[str, Any],
        facts: Optional[list[Any]],
    ) -> tuple[RecordNode, EdgeKey]:
        remote_id = str(preference["id"])
        node_id = self._node_id("arena_preference", remote_id)
        data = preference.get("data_parsed") or {}
        chosen = data.get("model_id")
        siblings = data.get("sibling_model_ids")
        sibling_ids = sorted({
            str(value) for value in siblings or () if value
        })
        compared = sorted(set(sibling_ids) | ({str(chosen)} if chosen else set()))
        attrs = {
            "arena_preference_id": node_id,
            "instance": self.instance,
            "remote_chat_id": str(conversation["id"]),
            "compared_message_id": self._rating_target(preference, conversation),
            "principal": preference.get("user_id"),
            "chosen_model_id": str(chosen) if chosen else None,
            "compared_model_ids": ",".join(compared),
            "sibling_model_ids": ",".join(sibling_ids),
            "rating": (
                str(data.get("rating"))
                if data.get("rating") is not None else None
            ),
            "preferred_at": _epoch_to_iso(preference.get("created_at")),
            "text": " ".join(compared),
        }
        node = RecordNode(
            node_id=node_id,
            subtype="arena_preference",
            source_record_id=dict(record_id),
            source_revision=dict(revision),
        )
        edge = EdgeKey(
            src=session_node_id,
            dst=node_id,
            edge_type="contains",
            source_record_id=dict(record_id),
            source_revision=dict(revision),
        )
        if facts is not None:
            stats["arena_preferences_emitted"] += 1
            facts.append(NodeFact(
                node_id=node_id,
                subtype="arena_preference",
                attrs=attrs,
                source_record_id=dict(record_id),
                source_revision=dict(revision),
            ))
            facts.append(EdgeFact(
                src=session_node_id,
                dst=node_id,
                edge_type="contains",
                source_record_id=dict(record_id),
                source_revision=dict(revision),
            ))
        return node, edge


def _emitted_node_signature(ownership_id: str) -> list[dict[str, Any]]:
    return [
        {
            "subtype": subtype,
            "id_prefix": prefix,
            "clause_id": f"{ownership_id}.{subtype}",
        }
        for subtype, prefix in sorted(_ID_PREFIXES.items())
    ]


def _emitted_edge_signature(ownership_id: str) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = [
        {
            "src_subtype": "chat_session",
            "edge_type": "contains",
            "dst_subtype": "chat_message",
            "src_id_prefix": _ID_PREFIXES["chat_session"],
            "dst_id_prefix": _ID_PREFIXES["chat_message"],
            "clause_id": f"{ownership_id}.chat_session_contains_chat_message",
        },
        {
            "src_subtype": "chat_message",
            "edge_type": "contains",
            "dst_subtype": "chat_rating",
            "src_id_prefix": _ID_PREFIXES["chat_message"],
            "dst_id_prefix": _ID_PREFIXES["chat_rating"],
            "clause_id": f"{ownership_id}.chat_message_contains_chat_rating",
        },
        {
            "src_subtype": "chat_session",
            "edge_type": "contains",
            "dst_subtype": "arena_preference",
            "src_id_prefix": _ID_PREFIXES["chat_session"],
            "dst_id_prefix": _ID_PREFIXES["arena_preference"],
            "clause_id": f"{ownership_id}.chat_session_contains_arena_preference",
        },
        {
            "src_subtype": "chat_message",
            "edge_type": "contains",
            "dst_subtype": "chat_context_record",
            "src_id_prefix": _ID_PREFIXES["chat_message"],
            "dst_id_prefix": _ID_PREFIXES["chat_context_record"],
            "clause_id": (
                f"{ownership_id}.chat_message_contains_chat_context_record"
            ),
        },
    ]
    for prefix, subtype in CITED_SUBTYPE_ALLOWLIST:
        edges.append({
            "src_subtype": "chat_message",
            "edge_type": "references",
            "dst_subtype": subtype,
            "src_id_prefix": _ID_PREFIXES["chat_message"],
            "dst_id_prefix": prefix,
            # The cited node is owned by ANOTHER source. It is optional here
            # because a deployment that does not load that module never mints
            # one, and admission must not reject the whole payload for it.
            "dst_subtype_optional": True,
            "clause_id": f"{ownership_id}.chat_message_references_{subtype}",
        })
    return edges


def chat_sessions_source_registry_entry(
    *,
    ownership_id: str,
    app_db_path: str = "${deployment_dir}/state/chat/app.db",
    instance: str = "default",
    preset: Optional[str] = None,
    excerpt_chars: int = 400,
    # `agent-derived-trusted` (lattice rank 3), matching what the agent-session
    # and codex-session transcript sources use: identical shape -- turns of
    # operator input plus model output, captured locally from a trusted
    # principal.
    #
    # This was `operator-chat-instance`, which is NOT a member of
    # producer_authority.TRUST_LABEL_RANKS, so normalize_trust_label() raised
    # and every run was rejected at admission -- this source had NEVER ingested
    # on any deployment. Minting the missing member instead was considered and
    # rejected: the lattice is hashed into every producer's profile_fingerprint,
    # so an additive label re-materializes ALL producer profiles. See #1316.
    trust_label: str = "agent-derived-trusted",
) -> dict[str, Any]:
    """The canonical ``source_registry.yaml`` entry for this source.

    Kept in code, like ``upload_source_registry_entry``, so the shape is
    exercised by the same loader, the same Nomos audit, and the same
    admission-signature evaluation a deployment uses.

    The profile tuple is the ``discovery_crawl`` row of
    ``okg.substrate.sources.profiles.PROFILES``:
    ``(discovery_crawl, scoped_locator, content_hash,
    missing_from_completed_scope, published_generation)`` -- the same row
    agent_sessions occupies. ``content_hash`` rather than ``updated_at``
    because a rating lives in a different table and does not move
    ``chat.updated_at``, so an updated_at revision would silently miss the
    preference facts this source exists to carry.

    ``source_policy`` is declared INLINE and therefore wins over the
    deployment's ``nomos.source_policy_defaults`` (the audit's precedence:
    ``source_registry.source_policy`` first). It is stricter than the
    ``discovery_crawl`` class default on the dimension that matters for chat
    content -- ``privacy_obligations`` adds ``redaction``, which is the
    obligation this source actually discharges by scrubbing credentials and
    bounding every excerpt before emission.
    """
    return {
        "module": CHAT_SESSIONS_SOURCE_MODULE,
        "class": CHAT_SESSIONS_SOURCE_CLASS,
        "ownership_id": ownership_id,
        "params": {
            "app_db_path": app_db_path,
            "instance": instance,
            "preset": preset,
            "excerpt_chars": excerpt_chars,
            "app_db_revision": PINNED_APP_DB_REVISION,
            "openwebui_version": PINNED_OPENWEBUI_VERSION,
        },
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["instance", "chat_id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "source_policy": {
            "store_raw": True,
            "sensitivity": "local_private",
            "data_classification": "private",
            "credential_ref_policy": "none",
            "live_call_allowed": False,
            "exportability": "no_export",
            "retention": "source_defined",
            "provenance": "local_filesystem",
            "privacy_obligations": ["audit", "no_export", "redaction"],
            # Declared even under store_raw, so a deployment that flips the
            # posture classifies these attributes by DECLARATION rather than
            # by the fail-safe mask-everything default.
            "pii_classes": {
                # `free_text`, not `sensitive_attribute`. The latter is what
                # an UNDECLARED attribute already resolves to under the
                # fail-safe classifier, so declaring it would add nothing;
                # `free_text` is the class that actually describes user prose
                # and routes it to the detector rather than a blanket mask.
                "excerpt": "free_text",
                "payload_excerpt": "free_text",
                "comment_excerpt": "free_text",
                "title": "free_text",
                "text": "free_text",
                "principal": "quasi_identifier",
                "model_id": "quasi_identifier",
                "model": "quasi_identifier",
                "provider": "quasi_identifier",
                "chosen_model_id": "quasi_identifier",
                "compared_model_ids": "quasi_identifier",
                "sibling_model_ids": "quasi_identifier",
                "content_sha256": "non_pii",
                "payload_sha256": "non_pii",
                "instance": "non_pii",
                "role": "non_pii",
                "rating": "non_pii",
                "reason": "non_pii",
            },
        },
        "admission_policy": {
            "producer_id": ownership_id,
            "producer_kind": "source",
            "trust_label": trust_label,
            "admission_mode": "fast_track",
            "authority_scope": {
                "source_family": "openwebui_chat_instance",
                "path_globs": ["**/webui.db", "**/app.db", "**/records-*.jsonl"],
            },
            "allowed_record_kinds": ["node", "edge"],
            "output_signature": {
                "nodes": _emitted_node_signature(ownership_id),
                "edges": _emitted_edge_signature(ownership_id),
            },
            "policy_clauses": [
                *[
                    {
                        "id": f"{ownership_id}.{subtype}",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": subtype,
                    }
                    for subtype in sorted(_ID_PREFIXES)
                ],
                *[
                    {
                        "id": edge["clause_id"],
                        "effect": "allow",
                        "record_kind": "edge",
                        "triple": [
                            edge["src_subtype"],
                            edge["edge_type"],
                            edge["dst_subtype"],
                        ],
                    }
                    for edge in _emitted_edge_signature(ownership_id)
                ],
            ],
            "output_scope_summary": {
                "nodes": sorted(_ID_PREFIXES),
                "edges": [
                    f"{edge['src_subtype']} {edge['edge_type']} "
                    f"{edge['dst_subtype']}"
                    for edge in _emitted_edge_signature(ownership_id)
                ],
                "scope": (
                    "Conversations, messages, ratings, arena preferences, and "
                    "per-turn context-audit pointers from one Open WebUI "
                    f"{PINNED_OPENWEBUI_VERSION} instance"
                ),
            },
        },
        "sync": {
            # No `poll` trigger and no `fs_event` watch: the app database is
            # operator state outside the repo, and the live worker must not
            # start reading a chat instance because a source entry appeared.
            "triggers": ["manual", "reconcile"],
            "reconcile_mode": "scope_complete",
            "deletes_require_reconcile": True,
            "cheap_probe": "change_probe",
        },
    }


def emitted_edge_triples() -> list[tuple[str, str, str]]:
    """Every ``(parent_subtype, edge_type, child_subtype)`` this source emits.

    Exposed so a test can cross-check the whole set against the composed
    narrowing catalog rather than against a hand-maintained list.
    """
    return [
        (edge["src_subtype"], edge["edge_type"], edge["dst_subtype"])
        for edge in _emitted_edge_signature("x")
    ]


def emitted_node_subtypes() -> list[str]:
    return sorted(_ID_PREFIXES)
