"""The versioned, append-only audit-record artifact (PACT chat-context-audit-filter).

What this file is
-----------------
One turn of an Open WebUI chat produces TWO records here, joined by
``record_id``:

``inlet``
    Written before the model runs, from inside the Filter inlet. It carries
    the exact payload the model is about to receive, in the explicitly named
    field ``payload``, plus its digest in ``payload_sha256``.

``completion``
    Written at turn end, once the turn's MCP envelopes exist. It carries the
    generation id the turn's graph reads actually observed.

The split is not decoration. The inlet fires BEFORE any MCP call for that
turn, so an inlet record cannot hold an observed generation id; a single
record carrying both would either be rewritten in place (destroying
append-only) or hold a field that was unknowable at its own write time. Two
immutable records joined by an id is the only shape where every field was
knowable when it was written.

Why the payload is a canonical string, not the raw body
------------------------------------------------------
``payload`` holds the canonical serialization of the IN-SCOPE SUBTREE — the
message array and the model identifier — and nothing else. That choice is
load-bearing in three ways:

* ``payload_sha256 == sha256(payload.encode("utf-8"))`` exactly, so
  ``chat-transcript-source`` can verify the digest it stores without
  re-deriving anyone's serialization rules;
* the post-compaction equality proof compares this same subtree on both
  sides, so the artifact stores the bytes the proof is about rather than a
  superset the proof has to re-extract; and
* Open WebUI's request body also carries instance internals (chat ids,
  session ids, feature flags, file handles) that the model never sees and
  that would be recorded as if it had.

The discarded alternative — store the whole request body — was rejected for
the third reason: it would make ``payload`` a superset of what reached the
model, which is precisely the confusion this artifact exists to remove.

Schema version comparison
-------------------------
EXACT MATCH ON THE MAJOR COMPONENT, stated once for producer and consumer:

* a reader that meets an unrecognized major REFUSES
  (:class:`UnknownSchemaVersion`) rather than parsing best-effort, and
* a producer whose major changed REFUSES to append into an artifact whose
  existing records carry the old major (:class:`SchemaVersionConflict`) and
  starts a new artifact file instead of mixing versions in one file.

Absence is never agreement. A missing artifact, an unreadable one, and one
carrying an unknown major are three distinct refusals; none of them is a
value that compares equal to a recorded turn.

Where the file lives
--------------------
Operator state for the deployment's chat instance, NEVER a deployment graph
database. The resolution rule is stated in :func:`resolve_audit_dir` so a
consumer can locate the file without this module's context.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from okg.substrate.deployments import user_state_deployments_root

__all__ = [
    "AUDIT_SCHEMA_NAME",
    "AUDIT_SCHEMA_VERSION",
    "PAYLOAD_FIELD",
    "PAYLOAD_DIGEST_FIELD",
    "PAYLOAD_CANONICALIZATION",
    "STATE_HOME_ENV",
    "AuditWriteError",
    "SchemaVersionConflict",
    "UnknownSchemaVersion",
    "GenerationAgreement",
    "GenerationObservation",
    "RecordKind",
    "SOURCE_REVISION",
    "TaskKind",
    "AuditArtifact",
    "canonical_payload",
    "canonical_subtree",
    "observe_generation",
    "payload_sha256",
    "read_records",
    "resolve_artifact_path",
    "resolve_audit_dir",
    "resolve_chat_state_dir",
    "schema_major",
    "task_kind_from_metadata",
]


#: Identifies the artifact KIND. A reader that opens the wrong JSONL file
#: refuses on this rather than discovering it row by row.
AUDIT_SCHEMA_NAME = "okg.chat.audit_record"

#: MAJOR.MINOR. Only the major participates in compatibility comparison.
AUDIT_SCHEMA_VERSION = "1.0"

#: Fixed by the cross-child interface: chat-transcript-source reads the
#: prompt text through this exact name so the deployment's PII posture
#: applies to a named field rather than to an opaque blob.
PAYLOAD_FIELD = "payload"
PAYLOAD_DIGEST_FIELD = "payload_sha256"

#: The canonicalization rule, recorded in every inlet record so a consumer
#: never has to infer it. Stated as data, not as prose only.
PAYLOAD_CANONICALIZATION = (
    "json.dumps({'messages': ..., 'model': ...}, sort_keys=True, "
    "separators=(',', ':'), ensure_ascii=False)"
)

#: The system message Open WebUI prepends when it compacts a conversation
#: (``utils/middleware.py``, v0.11.0; ``:2508`` at v0.11.4). Its presence in a recorded payload is
#: the one compaction fact knowable at inlet-record write time.
COMPACTION_SUMMARY_MARKER = "[CONVERSATION SUMMARY]"

#: Explicit operator input for the state root. Not a fallback: when unset,
#: the XDG operator-state root is used, which is a DERIVATION, not a default
#: standing in for a missing value.
STATE_HOME_ENV = "OKG_CHAT_STATE_HOME"

_ARTIFACT_STEM = "records"
_ARTIFACT_RE = re.compile(r"^records-(\d{3})\.jsonl$")

#: ``source_revision`` is fixed at 1 for every record: records are immutable,
#: so a record id never addresses more than one revision. Named here because
#: chat-transcript-source maps ``record_id`` -> ``source_record_id`` and needs
#: the revision to come from somewhere stated.
SOURCE_REVISION = 1


class AuditWriteError(RuntimeError):
    """An audit record could not be appended. The turn fails.

    Carries a machine-readable ``reason`` so the refusal that reaches the
    user names itself instead of being a bare traceback.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class SchemaVersionConflict(AuditWriteError):
    """The artifact's existing records carry a different schema major.

    The producer refuses rather than mixing versions in one file; the caller
    resolves a NEW artifact path (:func:`resolve_artifact_path` does this
    automatically) instead of forcing the append.
    """

    def __init__(self, message: str) -> None:
        super().__init__("audit-schema-version-conflict", message)


class UnknownSchemaVersion(ValueError):
    """A reader met a record whose schema major it does not recognize.

    Refusing here is the point: a best-effort parse of an unknown version
    would emit a record with defaulted fields, which reads downstream as a
    turn that happened rather than as one that could not be read.
    """


class RecordKind(str, Enum):
    INLET = "inlet"
    COMPLETION = "completion"


class TaskKind(str, Enum):
    """What Open WebUI was actually asking the model to do.

    Only ``USER_TURN`` is a TURN. Open WebUI issues auxiliary completions —
    title, tags, follow-ups, and the compaction summarization call itself —
    against the same filter, and counting one of those as the user's turn
    would compare the wrong capture in the equality proof.
    """

    USER_TURN = "user_turn"
    TITLE_GENERATION = "title_generation"
    TAGS_GENERATION = "tags_generation"
    FOLLOW_UP_GENERATION = "follow_up_generation"
    CONTEXT_COMPACTION = "context_compaction"
    QUERY_GENERATION = "query_generation"
    AUTOCOMPLETE_GENERATION = "autocomplete_generation"
    OTHER_AUXILIARY = "other_auxiliary"


#: Open WebUI v0.11.0 spells its auxiliary task names in
#: ``constants.py::TASKS`` and stamps them at ``metadata["task"]``; the
#: compaction summarizer stamps ``"context_compaction"`` literally
#: (``utils/context_compaction.py``). Anything named but unrecognized is
#: auxiliary — never silently promoted to a user turn. Re-read at v0.11.4
#: (revision ``8bd8b4fa``): the same six names, at ``constants.py:140-146``
#: and ``utils/context_compaction.py:380``.
_OPENWEBUI_TASK_KINDS: dict[str, TaskKind] = {
    "title_generation": TaskKind.TITLE_GENERATION,
    "tags_generation": TaskKind.TAGS_GENERATION,
    "follow_up_generation": TaskKind.FOLLOW_UP_GENERATION,
    "context_compaction": TaskKind.CONTEXT_COMPACTION,
    "query_generation": TaskKind.QUERY_GENERATION,
    "autocomplete_generation": TaskKind.AUTOCOMPLETE_GENERATION,
}


def schema_major(version: str) -> str:
    """The major component of a MAJOR.MINOR version string.

    A version with no dot is its own major; an empty or non-string version
    has no major and callers treat that as unrecognized.
    """
    if not isinstance(version, str) or not version.strip():
        return ""
    return version.strip().split(".", 1)[0]


def task_kind_from_metadata(metadata: Mapping[str, Any] | None) -> TaskKind:
    """Label a call by Open WebUI's own task metadata.

    An ABSENT ``task`` key is how Open WebUI marks a user chat completion,
    so absence maps to ``USER_TURN`` here — the one place in this module
    where absence means something rather than nothing. A PRESENT but
    unrecognized task is auxiliary, never a user turn: a new auxiliary task
    in a future Open WebUI version must not silently be audited and compared
    as if the user had sent it.
    """
    if not isinstance(metadata, Mapping):
        return TaskKind.USER_TURN
    raw = metadata.get("task")
    if raw is None:
        return TaskKind.USER_TURN
    name = str(raw).strip()
    if not name:
        return TaskKind.USER_TURN
    return _OPENWEBUI_TASK_KINDS.get(name, TaskKind.OTHER_AUXILIARY)


def canonical_subtree(body: Mapping[str, Any]) -> dict[str, Any]:
    """The in-scope subtree: the message array plus the model identifier.

    Nothing else. This is the definition the equality proof compares, so it
    must not quietly widen: a field added here changes what "the bytes the
    model received" means on both sides at once.
    """
    return {
        "messages": body.get("messages"),
        "model": body.get("model"),
    }


def canonical_payload(body: Mapping[str, Any]) -> str:
    """Serialize the in-scope subtree under the pinned canonical form.

    Canonicalization is the DEFINITION of the comparison, not an escape
    hatch: a difference confined to serialization form still has to fail,
    which it does, because both sides are re-serialized from parsed values
    rather than compared as received.
    """
    return json.dumps(
        canonical_subtree(body),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def payload_sha256(payload: str) -> str:
    """The digest of the canonical payload string, over its UTF-8 bytes."""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def resolve_chat_state_dir(
    deployment: str,
    *,
    state_home: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Operator state for a deployment's chat instance.

    THE RULE, stated once for every consumer:

    * ``state_home`` argument, when given, wins — that is an explicit
      operator input;
    * otherwise ``$OKG_CHAT_STATE_HOME`` when set;
    * otherwise ``user_state_deployments_root()/<deployment>/chat``, the XDG
      operator-state root the rest of the substrate already uses.

    In every branch the result is operator state on the host filesystem. No
    branch resolves into a deployment graph database, and there is no
    same-directory-as-the-repo guess.
    """
    env = os.environ if environ is None else environ
    if state_home is not None:
        return Path(state_home) / deployment
    configured = env.get(STATE_HOME_ENV)
    if configured and configured.strip():
        return Path(configured.strip()) / deployment
    return user_state_deployments_root() / deployment / "chat"


def resolve_audit_dir(
    deployment: str,
    preset: str,
    *,
    state_home: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """``<chat state dir>/audit/<preset>`` — one artifact series per preset."""
    return (
        resolve_chat_state_dir(deployment, state_home=state_home, environ=environ)
        / "audit"
        / preset
    )


def _artifact_head_version(path: Path) -> str | None:
    """The schema version of an artifact's first record, or None if empty.

    Also checks ``schema``: a JSONL file that is not this artifact kind has
    no version we can reason about, and treating its absent version as
    "empty, matches anything" would append our records into someone else's
    file.
    """
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise UnknownSchemaVersion(
                        f"{path}: first record is not valid JSON: {exc}"
                    ) from exc
                if record.get("schema") != AUDIT_SCHEMA_NAME:
                    raise UnknownSchemaVersion(
                        f"{path}: not an {AUDIT_SCHEMA_NAME} artifact "
                        f"(schema={record.get('schema')!r})"
                    )
                version = record.get("schema_version")
                return version if isinstance(version, str) else None
    except FileNotFoundError:
        return None
    return None


def resolve_artifact_path(
    deployment: str,
    preset: str,
    *,
    schema_version: str = AUDIT_SCHEMA_VERSION,
    state_home: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """The artifact file to append to for this producer version.

    Files are ``records-001.jsonl``, ``records-002.jsonl``, ... The NEWEST
    file whose records carry the same schema MAJOR is reused; when no
    existing file matches, the next index is returned. So a version change
    starts a new artifact by construction rather than by a scheduled
    migration someone has to remember to run, and a rollback to a previous
    major resumes that major's own file instead of stranding it or mixing
    versions into the newest one.
    """
    directory = resolve_audit_dir(
        deployment, preset, state_home=state_home, environ=environ
    )
    wanted = schema_major(schema_version)
    existing: list[tuple[int, Path]] = []
    if directory.is_dir():
        for candidate in directory.iterdir():
            match = _ARTIFACT_RE.match(candidate.name)
            if match is not None:
                existing.append((int(match.group(1)), candidate))
    if not existing:
        return directory / f"{_ARTIFACT_STEM}-001.jsonl"

    existing.sort()
    # A file whose records carry the wanted major is the answer. An EMPTY
    # candidate is only the answer when nothing else matches: an empty file
    # matches every major, so preferring it would hand major 7 the same
    # file as major 1 and make the "a rollback resumes its own major's
    # file" rule false whenever an empty rotation target exists.
    empty: Path | None = None
    for _index, candidate in reversed(existing):
        head = _artifact_head_version(candidate)
        if head is None:
            if empty is None:
                empty = candidate
            continue
        if schema_major(head) == wanted:
            return candidate
    if empty is not None:
        return empty
    return directory / f"{_ARTIFACT_STEM}-{existing[-1][0] + 1:03d}.jsonl"


def read_records(
    path: str | os.PathLike[str],
    *,
    schema_version: str = AUDIT_SCHEMA_VERSION,
) -> list[dict[str, Any]]:
    """Read an artifact, refusing anything whose major is not recognized.

    Raises :class:`FileNotFoundError` when the artifact is absent and
    :class:`UnknownSchemaVersion` when any record carries an unrecognized
    major or the file is not this artifact kind. Both are refusals; neither
    yields an empty list, because an empty list would read downstream as
    "no turns happened".
    """
    wanted = schema_major(schema_version)
    records: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise UnknownSchemaVersion(
                    f"{path}:{number}: record is not valid JSON: {exc}"
                ) from exc
            if record.get("schema") != AUDIT_SCHEMA_NAME:
                raise UnknownSchemaVersion(
                    f"{path}:{number}: not an {AUDIT_SCHEMA_NAME} artifact "
                    f"(schema={record.get('schema')!r})"
                )
            found = record.get("schema_version")
            if not isinstance(found, str) or schema_major(found) != wanted:
                raise UnknownSchemaVersion(
                    f"{path}:{number}: unrecognized schema_version {found!r}; "
                    f"this reader knows major {wanted!r}"
                )
            records.append(record)
    return records


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


@dataclass(frozen=True)
class AuditArtifact:
    """An append-only JSONL artifact for one (deployment, preset).

    Every method either appends one whole record or raises. There is no
    partial-record path: a failed append truncates back to the byte length
    the file had before the attempt, so the artifact never holds half a
    record and never holds a record with an empty payload.
    """

    path: Path
    schema_version: str = AUDIT_SCHEMA_VERSION

    @classmethod
    def open(
        cls,
        deployment: str,
        preset: str,
        *,
        schema_version: str = AUDIT_SCHEMA_VERSION,
        state_home: str | os.PathLike[str] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> AuditArtifact:
        return cls(
            path=resolve_artifact_path(
                deployment,
                preset,
                schema_version=schema_version,
                state_home=state_home,
                environ=environ,
            ),
            schema_version=schema_version,
        )

    # -- append state ----------------------------------------------------

    def _scan(self) -> tuple[str | None, int]:
        """Recover ``(artifact_id, next inlet sequence)`` from the file.

        Recovered from the artifact itself rather than held in memory, so a
        restarted process cannot re-mint a sequence number that an earlier
        process already used.

        Reads the HEAD line (artifact identity and version) and then scans
        BACKWARD for the newest inlet record. A full forward scan on every
        append is O(n) per turn and O(n^2) over the life of an artifact,
        which on a long-lived preset degrades every turn a little more.

        A torn FINAL line is repaired, not fatal. A line at the end with no
        terminating newline is by definition an append that never committed
        — a process died between ``write`` and ``flush`` — so it is
        truncated away. A corrupt line ANYWHERE ELSE is a real corruption
        and still refuses: it was committed once and something else damaged
        it.
        """
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                lines = handle.readlines()
        except FileNotFoundError:
            return None, 1
        except OSError as exc:
            raise AuditWriteError(
                "audit-artifact-unreadable",
                f"the audit artifact {self.path} could not be read: {exc}",
            ) from exc

        if lines and not lines[-1].endswith("\n"):
            self._truncate_torn_tail(len(lines[-1]))
            lines = lines[:-1]

        head: dict[str, Any] | None = None
        for number, line in enumerate(lines, start=1):
            if line.strip():
                head = self._parse_line(line, number)
                break
        if head is None:
            return None, 1

        version = head.get("schema_version")
        if not isinstance(version, str) or schema_major(version) != schema_major(
            self.schema_version
        ):
            raise SchemaVersionConflict(
                f"{self.path}:1 carries schema_version {version!r}; this "
                f"producer writes {self.schema_version!r} and refuses to mix "
                f"versions in one artifact"
            )
        artifact_id = head.get("artifact_id")

        for offset, line in enumerate(reversed(lines)):
            if not line.strip():
                continue
            record = self._parse_line(line, len(lines) - offset)
            if record.get("record_kind") == RecordKind.INLET.value:
                return artifact_id, int(record.get("record_seq") or 0) + 1
        return artifact_id, 1

    def _parse_line(self, line: str, number: int) -> dict[str, Any]:
        try:
            return json.loads(line)
        except json.JSONDecodeError as exc:
            raise AuditWriteError(
                "audit-artifact-corrupt",
                f"{self.path}:{number}: existing record is not valid JSON: "
                f"{exc}",
            ) from exc

    def _truncate_torn_tail(self, tail_bytes: int) -> None:
        """Drop an uncommitted final line so the artifact stays appendable."""
        try:
            size = self.path.stat().st_size
            with self.path.open("a", encoding="utf-8") as handle:
                handle.truncate(max(0, size - tail_bytes))
        except OSError as exc:
            raise AuditWriteError(
                "audit-artifact-unwritable",
                f"the audit artifact {self.path} carries an uncommitted "
                f"partial record that could not be repaired: {exc}",
            ) from exc

    @contextmanager
    def _exclusive(self) -> Iterator[None]:
        """Hold an exclusive advisory lock across read-then-append.

        ``_scan`` reads the next sequence and ``_append`` writes it; between
        those two, a second writer that read the same sequence mints the
        SAME ``record_id``. Two Open WebUI workers, or two replicas over a
        shared volume, are enough. A uuid4 artifact id does not save it:
        each racing writer that finds an empty file mints its OWN artifact
        id, so one file ends up holding several identities.

        The lock file sits beside the artifact, so the lock survives the
        artifact being created inside the critical section.
        """
        lock_path = self.path.with_name(self.path.name + ".lock")
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle = lock_path.open("a+")
        except OSError as exc:
            raise AuditWriteError(
                "audit-artifact-unwritable",
                f"the audit lock {lock_path} could not be opened: {exc}",
            ) from exc
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def _append(self, record: Mapping[str, Any]) -> None:
        """Append one record, or leave the file exactly as it was."""
        try:
            line = json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        except (TypeError, ValueError) as exc:
            raise AuditWriteError(
                "audit-record-unserializable",
                f"the audit record could not be serialized: {exc}",
            ) from exc

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise AuditWriteError(
                "audit-artifact-unwritable",
                f"the audit artifact directory {self.path.parent} could not "
                f"be created: {exc}",
            ) from exc

        try:
            size_before = self.path.stat().st_size
        except FileNotFoundError:
            size_before = 0
        except OSError as exc:
            raise AuditWriteError(
                "audit-artifact-unwritable",
                f"the audit artifact {self.path} could not be inspected: {exc}",
            ) from exc

        try:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            # A partial line is worse than no line: it would read as a
            # corrupt artifact for every later consumer. Roll our own
            # append back — never an earlier record.
            try:
                if self.path.exists() and self.path.stat().st_size > size_before:
                    with self.path.open("a", encoding="utf-8") as handle:
                        handle.truncate(size_before)
            except OSError:
                # Best effort only: preserve the original append failure below.
                pass
            raise AuditWriteError(
                "audit-artifact-unwritable",
                f"the audit artifact {self.path} could not be appended to: {exc}",
            ) from exc

    # -- records ---------------------------------------------------------

    def append_inlet(
        self,
        *,
        deployment: str,
        preset: str,
        conversation_id: str | None,
        message_id: str | None,
        task_kind: TaskKind,
        principal: str | None,
        model: str | None,
        provider: str | None,
        compaction_enabled: bool,
        payload: str,
        openwebui_version: str | None = None,
        timestamp: str | None = None,
        task_metadata_raw: str | None = None,
        proof_mode: bool = False,
    ) -> dict[str, Any]:
        """Append the immutable inlet record and return it.

        ``record_id`` is ``<artifact_id>:<seq>`` — artifact identity plus
        append sequence. The artifact id is a uuid4 minted when the file is
        created, NOT derived from the deployment or preset: a class-derived
        id collides across every artifact of that class, and the "occurs in
        exactly one place" reading of the code is green against exactly that
        bug.

        Two compaction fields, because they are two different facts and one
        name for both was a lie:

        * ``compaction_enabled`` — the preset's CONFIGURATION. Known from
          settings, true on every turn of an enabled preset including the
          early ones that were too short to compact.
        * ``compaction_observed`` — whether THIS payload actually carries a
          compaction summary. Derived from the payload itself, so it is
          knowable at this record's own write time, which is the invariant
          the two-record split exists to hold.

        The whole read-then-append is under an exclusive lock; see
        :meth:`_exclusive`.
        """
        if not payload:
            raise AuditWriteError(
                "audit-payload-empty",
                "refusing to append an inlet record with an empty payload",
            )
        with self._exclusive():
            artifact_id, seq = self._scan()
            if artifact_id is None:
                artifact_id = uuid.uuid4().hex
            record_id = f"{artifact_id}:{seq:012d}"
            record = {
                "schema": AUDIT_SCHEMA_NAME,
                "schema_version": self.schema_version,
                "record_kind": RecordKind.INLET.value,
                "artifact_id": artifact_id,
                "record_seq": seq,
                "record_id": record_id,
                "source_revision": SOURCE_REVISION,
                "deployment": deployment,
                "preset": preset,
                "openwebui_version": openwebui_version,
                "conversation_id": conversation_id,
                "message_id": message_id,
                "task_kind": task_kind.value,
                # The raw signal the kind was derived FROM, so a reader can
                # tell "Open WebUI sent no task key" apart from "Open WebUI
                # declared a user turn" without re-deriving the mapping.
                "task_metadata_raw": task_metadata_raw,
                "principal": principal,
                "model": model,
                "provider": provider,
                "timestamp": timestamp or _utc_now(),
                "compaction_enabled": bool(compaction_enabled),
                "compaction_observed": COMPACTION_SUMMARY_MARKER in payload,
                "proof_mode": bool(proof_mode),
                "payload_canonicalization": PAYLOAD_CANONICALIZATION,
                PAYLOAD_FIELD: payload,
                PAYLOAD_DIGEST_FIELD: payload_sha256(payload),
            }
            self._append(record)
        return record

    def append_completion(
        self,
        *,
        record_id: str,
        observation: GenerationObservation,
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        """Append the turn completion record, joined by ``record_id``.

        The observation is the generation the turn's MCP envelopes actually
        carried — the same quantity the outlet stamp renders, derived once
        by :func:`observe_generation`. Its ``agreement`` field keeps three
        outcomes distinct that a single nullable id would collapse: a turn
        that read nothing, a turn that read and got no generation, and a
        turn whose reads disagreed.

        No ``trust_level``: nothing produces one per call.
        """
        with self._exclusive():
            return self._append_completion(record_id, observation, timestamp)

    def _append_completion(
        self,
        record_id: str,
        observation: GenerationObservation,
        timestamp: str | None,
    ) -> dict[str, Any]:
        record = {
            "schema": AUDIT_SCHEMA_NAME,
            "schema_version": self.schema_version,
            "record_kind": RecordKind.COMPLETION.value,
            "record_id": record_id,
            "graph_read": observation.graph_read,
            "generation_agreement": observation.agreement.value,
            "observed_generation_id": observation.generation_id,
            "observed_generation_ids": list(observation.generation_ids),
            "timestamp": timestamp or _utc_now(),
        }
        self._append(record)
        return record


class GenerationAgreement(str, Enum):
    """How the turn's envelopes agreed about the generation.

    Four states, none of which compares equal to another. ``NO_GRAPH_READ``
    is not the same fact as ``MISSING``: the first says the answer needed no
    stamp, the second says it needed one and did not get it.
    """

    NO_GRAPH_READ = "no_graph_read"
    SINGLE = "single"
    MISSING = "missing"
    CONFLICTING = "conflicting"


@dataclass(frozen=True)
class GenerationObservation:
    """What a turn's MCP envelopes said about the pinned generation."""

    agreement: GenerationAgreement
    generation_ids: tuple[str, ...] = ()

    @property
    def graph_read(self) -> bool:
        return self.agreement is not GenerationAgreement.NO_GRAPH_READ

    @property
    def generation_id(self) -> str | None:
        """The single observed generation, or None.

        None for every state except ``SINGLE`` — including CONFLICTING,
        where picking one of several would be a synthesized stamp.
        """
        if self.agreement is GenerationAgreement.SINGLE:
            return self.generation_ids[0]
        return None


def observe_generation(
    envelopes: Sequence[Mapping[str, Any]] | None,
) -> GenerationObservation:
    """Read the pinned generation out of a turn's MCP envelopes.

    ``pinned_generation_id`` is the canonical envelope field
    (``okg.substrate.mcp.envelope``); ``generation_id`` is the compat name
    still emitted alongside it, and the precedence here is the same one
    ``mcp/server.py`` uses.

    This function is PURE and takes only the current turn's envelopes. That
    is the mechanism, not an implementation detail: with no state to carry,
    reusing a previous turn's stamp is not something the caller can do by
    forgetting to clear a cache.
    """
    if not envelopes:
        return GenerationObservation(GenerationAgreement.NO_GRAPH_READ)
    seen: list[str] = []
    for envelope in envelopes:
        if not isinstance(envelope, Mapping):
            continue
        for field in ("pinned_generation_id", "generation_id"):
            value = envelope.get(field)
            if isinstance(value, str) and value.strip():
                found = value.strip()
                if found not in seen:
                    seen.append(found)
                break
    if not seen:
        return GenerationObservation(GenerationAgreement.MISSING)
    if len(seen) == 1:
        return GenerationObservation(GenerationAgreement.SINGLE, tuple(seen))
    return GenerationObservation(GenerationAgreement.CONFLICTING, tuple(seen))
