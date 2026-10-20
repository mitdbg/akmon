"""The observability verdict artifact (PACT chat-context-audit-filter).

The question this artifact answers
----------------------------------
"For THIS deployment, THIS preset, and THIS Open WebUI version, has anyone
actually measured that the filter inlet observes the bytes the model
receives?" Two children read the answer: ``chat-sync-projection`` decides
whether a preset may run with compaction on, and the filter itself refuses a
turn that would otherwise answer with an audit it cannot vouch for.

Absence IS the encoding of unverified
-------------------------------------
There is no ``unverified`` value to write. A key with no record is
unverified, which makes every verdict invalidated by construction when the
Open WebUI version changes: a new version is a new key, a new key has no
record, and compaction stays off until someone re-proves it. No scheduled
invalidation, nothing to forget to run, and no way to reuse an old version's
record by mistake.

An unrecognized ``schema_version`` reads as unverified too — never as
verified, and never as a best-effort parse.

Two verdicts, one key
---------------------
The proof is two-armed, so the vocabulary is two-valued:
``verified_compaction_on`` and ``verified_compaction_off``. A key may carry
both (both arms proved), one, or neither. That distinction is the whole
point: "proven safe with compaction off" and "never measured" are different
states, and a single boolean would erase the difference.

Writers are NAMED
-----------------
:func:`write_verdict` is called by the operator-run live proof
(``tests/live/chat/test_post_compaction_audit_equality.py``) and by nothing
else. The runtime filter imports :func:`is_verified` only. A runtime
component cannot know whether the operator-run proof happened, so a runtime
component must not be able to say that it did.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from okg.substrate.chat.audit_record import (
    resolve_chat_state_dir,
    schema_major,
)

__all__ = [
    "VERDICT_SCHEMA_NAME",
    "VERDICT_SCHEMA_VERSION",
    "Configuration",
    "Verdict",
    "VerdictKey",
    "VerdictRecord",
    "is_verified",
    "read_verdicts",
    "resolve_verdict_path",
    "verdict_for_configuration",
    "write_verdict",
]


VERDICT_SCHEMA_NAME = "okg.chat.observability_verdict"

#: MAJOR.MINOR; only the major participates in compatibility comparison,
#: the same rule the audit-record artifact states.
VERDICT_SCHEMA_VERSION = "1.0"


class Configuration(str, Enum):
    """The two configurations a preset can run in."""

    COMPACTION_ON = "compaction_on"
    COMPACTION_OFF = "compaction_off"


class Verdict(str, Enum):
    """The value vocabulary. There is deliberately no ``unverified``."""

    VERIFIED_COMPACTION_ON = "verified_compaction_on"
    VERIFIED_COMPACTION_OFF = "verified_compaction_off"


_VERDICT_BY_CONFIGURATION: dict[Configuration, Verdict] = {
    Configuration.COMPACTION_ON: Verdict.VERIFIED_COMPACTION_ON,
    Configuration.COMPACTION_OFF: Verdict.VERIFIED_COMPACTION_OFF,
}


def verdict_for_configuration(configuration: Configuration) -> Verdict:
    """The one verdict value that verifies a configuration."""
    return _VERDICT_BY_CONFIGURATION[configuration]


@dataclass(frozen=True)
class VerdictKey:
    """EXACTLY (deployment, preset, openwebui_version). Nothing else.

    The Open WebUI version is part of the key, not an attribute of the
    record, because that is what makes a version change produce a key with
    no record instead of a record someone has to notice is stale.
    """

    deployment: str
    preset: str
    openwebui_version: str

    def as_dict(self) -> dict[str, str]:
        return {
            "deployment": self.deployment,
            "preset": self.preset,
            "openwebui_version": self.openwebui_version,
        }

    def matches(self, record: Mapping[str, Any]) -> bool:
        return (
            record.get("deployment") == self.deployment
            and record.get("preset") == self.preset
            and record.get("openwebui_version") == self.openwebui_version
        )


@dataclass(frozen=True)
class VerdictRecord:
    """One proved (key, configuration) pair."""

    key: VerdictKey
    verdict: Verdict
    measured_at: str
    schema_version: str = VERDICT_SCHEMA_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": VERDICT_SCHEMA_NAME,
            "schema_version": self.schema_version,
            **self.key.as_dict(),
            "verdict": self.verdict.value,
            "measured_at": self.measured_at,
        }


def resolve_verdict_path(
    deployment: str,
    *,
    state_home: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """``<chat state dir>/observability/verdicts.jsonl``.

    The chat state dir resolution rule is the audit artifact's
    (:func:`okg.substrate.chat.audit_record.resolve_chat_state_dir`) — one
    rule, two artifacts, so a consumer that can find one can find the other.
    Operator state on the host filesystem in every branch; never a
    deployment graph database.
    """
    return (
        resolve_chat_state_dir(deployment, state_home=state_home, environ=environ)
        / "observability"
        / "verdicts.jsonl"
    )


def read_verdicts(
    path: str | os.PathLike[str],
    *,
    schema_version: str = VERDICT_SCHEMA_VERSION,
) -> tuple[VerdictRecord, ...]:
    """Every RECOGNIZED verdict record in the artifact.

    Records whose ``schema`` or major version is not recognized, and records
    whose verdict value is outside the vocabulary, are DROPPED — which makes
    them read as unverified, the fail-closed direction. A missing file
    yields an empty tuple for the same reason. Nothing here raises: the
    consumers of this artifact must be able to ask "is this verified" and
    get False, not an exception that a caller might catch into a default.
    """
    wanted = schema_major(schema_version)
    records: list[VerdictRecord] = []
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except (FileNotFoundError, NotADirectoryError, IsADirectoryError, OSError):
        return ()

    for line in lines:
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(raw, Mapping):
            continue
        if raw.get("schema") != VERDICT_SCHEMA_NAME:
            continue
        found = raw.get("schema_version")
        if not isinstance(found, str) or schema_major(found) != wanted:
            continue
        try:
            verdict = Verdict(raw.get("verdict"))
        except ValueError:
            continue
        deployment = raw.get("deployment")
        preset = raw.get("preset")
        version = raw.get("openwebui_version")
        if not all(isinstance(v, str) and v for v in (deployment, preset, version)):
            continue
        measured_at = raw.get("measured_at")
        records.append(
            VerdictRecord(
                key=VerdictKey(str(deployment), str(preset), str(version)),
                verdict=verdict,
                measured_at=measured_at if isinstance(measured_at, str) else "",
                schema_version=found,
            )
        )
    return tuple(records)


def is_verified(
    path: str | os.PathLike[str],
    key: VerdictKey,
    configuration: Configuration,
    *,
    schema_version: str = VERDICT_SCHEMA_VERSION,
) -> bool:
    """Has this exact key been proved for this configuration?

    False for: no artifact, no record for the key, a record for the key that
    proves only the OTHER configuration, and a record whose schema version
    this reader does not recognize. All four are unverified; only a
    recognized record carrying the matching verdict value is True.
    """
    wanted = verdict_for_configuration(configuration)
    for record in read_verdicts(path, schema_version=schema_version):
        if record.verdict is wanted and key.matches(record.key.as_dict()):
            return True
    return False


def write_verdict(
    path: str | os.PathLike[str],
    key: VerdictKey,
    configuration: Configuration,
    *,
    measured_at: str | None = None,
    schema_version: str = VERDICT_SCHEMA_VERSION,
) -> VerdictRecord:
    """Record that a configuration was PROVED for a key.

    THE ONLY POSITIVE WRITER, and it belongs to the operator-run live proof.
    The runtime filter must never reach this function: it cannot know
    whether the proof ran, so it must not be able to assert that it did.
    """
    record = VerdictRecord(
        key=key,
        verdict=verdict_for_configuration(configuration),
        measured_at=measured_at
        or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        schema_version=schema_version,
    )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record.as_dict(), sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return record
