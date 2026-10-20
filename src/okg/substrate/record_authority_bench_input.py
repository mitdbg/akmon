"""Strict, privately captured complete record snapshots for isolated benchmarks.

Only captured bundle bytes, record structure and internal commitments are
verified here. Producer/source provenance remains an external attestation.
JSONL decoding holds one whole record, so memory scales with the largest
record (and manifest), not a fixed authority-store chunk or the record count.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from okg.substrate.ingest.record_authority import (
    IDENTITY_FIELD_MAX_BYTES,
    AuthorityError,
    StreamingRecord,
    canonical,
    legacy_record_set_digest,
)
from okg.substrate.library.sources.base import RecordAuthorityOperation, RecordAuthorityStream

SNAPSHOT_SCHEMA = "okg.record_authority.normalized_snapshot.v1"
_MANIFEST_KEYS = frozenset((
    "schema", "projection", "complete", "records_file", "records_bytes",
    "records_sha256", "record_count", "component_count", "logical_sha256",
    "source", "adapter_source", "source_revision", "fixed_run_id",
    "source_snapshot", "counts", "provenance", "full_input_nul_sanitization_completed",
))
_COUNT_KEYS = frozenset((
    "input_rows", "emitted_inserts", "equal_duplicates", "final_unique",
    "rejected", "held", "unexpected_operations", "authority_normalization_changes",
))
_NODE_KEYS = frozenset(("node_id", "subtype", "source_record_id", "source_revision"))
_EDGE_KEYS = frozenset((
    "src", "dst", "edge_type", "attrs", "provenance", "confidence",
    "source_record_id", "source_revision",
))


def _refuse(message: str):
    raise AuthorityError("record snapshot: " + message)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _refuse("duplicate JSON object key")
        result[key] = value
    return result


def _float(text):
    value = float(text)
    if not math.isfinite(value):
        _refuse("nonfinite JSON number")
    return value


def _unicode(value):
    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
    elif isinstance(value, dict):
        for key, item in value.items():
            _unicode(key)
            _unicode(item)
    elif isinstance(value, list):
        for item in value:
            _unicode(item)


def _json(raw: bytes):
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                           parse_float=_float,
                           parse_constant=lambda _: _refuse("nonfinite JSON number"))
        _unicode(value)
        return value
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise AuthorityError("record snapshot: invalid strict JSON") from exc


def _keys(value, keys, label):
    if not isinstance(value, dict) or set(value) != keys:
        _refuse("invalid " + label + " fields")


def _text(value, label):
    if not isinstance(value, str) or not value or "\x00" in value:
        _refuse("invalid " + label)


def _count(value, label):
    if type(value) is not int or value < 0:
        _refuse("invalid " + label + " count")


def _sha(value, label):
    if (not isinstance(value, str) or len(value) != 64
            or any(c not in "0123456789abcdef" for c in value)):
        _refuse("invalid " + label + " SHA256")


def _manifest(raw: bytes):
    value = _json(raw)
    _keys(value, _MANIFEST_KEYS, "manifest")
    if value["schema"] != SNAPSHOT_SCHEMA or value["complete"] is not True:
        _refuse("requires a complete supported snapshot")
    if type(value["full_input_nul_sanitization_completed"]) is not bool:
        _refuse("invalid sanitization attestation")
    for name in ("projection", "source", "adapter_source", "fixed_run_id", "records_file"):
        _text(value[name], name)
    name = value["records_file"]
    if Path(name).name != name or name in (".", "..") or "\\" in name:
        _refuse("records_file must be one filename")
    for name in ("records_bytes", "record_count", "component_count"):
        _count(value[name], name)
    for name in ("records_sha256", "logical_sha256"):
        _sha(value[name], name)
    for name in ("source_revision", "source_snapshot", "provenance"):
        if not isinstance(value[name], dict):
            _refuse("invalid " + name + " mapping")
    counts = value["counts"]
    _keys(counts, _COUNT_KEYS, "counts")
    for name, count in counts.items():
        _count(count, name)
    if (counts["final_unique"] != value["record_count"]
            or counts["emitted_inserts"] != counts["final_unique"] + counts["equal_duplicates"]
            or any(counts[name] for name in ("rejected", "held", "unexpected_operations"))):
        _refuse("inconsistent complete-export counts")
    return value


def _component(value, *, edge: bool):
    if edge:
        _keys(value, _EDGE_KEYS, "edge")
        for name in ("src", "dst", "edge_type"):
            _text(value[name], name)
        if not isinstance(value["provenance"], str):
            _refuse("invalid edge provenance")
        if type(value["confidence"]) not in (int, float):
            _refuse("invalid edge confidence")
        mapping_names = ("attrs", "source_record_id", "source_revision")
        identity_fields = ("src", "dst", "edge_type")
        identity = canonical([value["src"], value["edge_type"], value["dst"]])
    else:
        if not isinstance(value, dict):
            _refuse("invalid node")
        keys = _NODE_KEYS | ({"ownership_mode"} if "ownership_mode" in value else set())
        _keys(value, keys, "node")
        for name in ("node_id", "subtype"):
            _text(value[name], name)
        if value.get("ownership_mode", "authoritative") not in ("authoritative", "substitute"):
            _refuse("invalid node ownership")
        mapping_names = ("source_record_id", "source_revision")
        identity_fields = ("node_id",) + (("ownership_mode",) if "ownership_mode" in value else ())
        identity = value["node_id"]
    for name in mapping_names:
        if not isinstance(value[name], dict):
            _refuse("invalid " + name + " mapping")
    if len(identity.encode("utf-8")) > IDENTITY_FIELD_MAX_BYTES:
        _refuse("identity exceeds existing byte limit")
    for name in identity_fields:
        if len(canonical(value[name]).encode("utf-8")) > IDENTITY_FIELD_MAX_BYTES:
            _refuse("canonical identity field exceeds existing byte limit")


def decode_record(raw: bytes) -> StreamingRecord:
    value = _json(raw)
    _keys(value, {"record_id", "primary", "descendants", "edges"}, "record")
    key = value["record_id"]
    _text(key, "record_id")
    if len(key.encode("utf-8")) > 2048:
        _refuse("record_id exceeds existing byte limit")
    if value["primary"] is not None:
        _component(value["primary"], edge=False)
    for name in ("descendants", "edges"):
        if not isinstance(value[name], list):
            _refuse("invalid " + name + " list")
        for component in value[name]:
            _component(component, edge=name == "edges")
    return StreamingRecord(**value)


def _components(record):
    return int(record.primary is not None) + len(record.descendants) + len(record.edges)


@dataclass(frozen=True)
class SnapshotSelection:
    records: int
    components: int
    logical_sha256: str
    policy: str
    digest_seconds: float


class PreparedSnapshot:
    """Context-owned frozen file; serialized passes only, no escaped iterator use.

    Privileged mutation of private internals is outside this API's contract.
    The original paths are never reopened after capture.
    """

    def __init__(self, handle, manifest, manifest_bytes, manifest_sha256):
        self._handle = handle
        self._manifest = manifest
        self._active = True
        self._reading = False
        self._selection = None
        self.manifest_bytes = manifest_bytes
        self.manifest_sha256 = manifest_sha256
        self.max_record_bytes = 0
        self.capture_seconds = 0.0
        self.validation_seconds = 0.0

    @property
    def manifest(self):
        self._check()
        return copy.deepcopy(self._manifest)

    def _check(self):
        if not self._active or self._handle.closed:
            _refuse("prepared input is closed")

    def _records(self, count: int | None = None) -> Iterator[StreamingRecord]:
        self._check()
        if self._reading:
            _refuse("overlapping input passes")
        self._reading = True
        try:
            self._handle.seek(0)
            previous = None
            index = 0
            while count is None or index < count:
                self._check()
                raw = self._handle.readline()
                if not raw:
                    break
                record = decode_record(raw)
                key = record.record_id.encode("utf-8")
                if previous is not None and key <= previous:
                    _refuse("record keys must be strictly increasing")
                previous = key
                self.max_record_bytes = max(self.max_record_bytes, len(raw))
                self._check()
                yield record
                self._check()
                index += 1
        finally:
            self._reading = False

    def select(self, sample_records: int | None = None) -> SnapshotSelection:
        self._check()
        if sample_records is not None:
            _count(sample_records, "sample_records")
            if sample_records > self._manifest["record_count"]:
                _refuse("sample exceeds complete snapshot count")
        count = self._manifest["record_count"] if sample_records is None else sample_records
        components = 0
        started = time.perf_counter()
        def selected():
            nonlocal components
            for record in self._records(count):
                components += _components(record)
                yield record
        digest = legacy_record_set_digest(selected())
        self._selection = SnapshotSelection(
            count, components, digest,
            "full" if sample_records is None else "canonical-prefix",
            time.perf_counter() - started)
        return self._selection

    def stream(self, selection: SnapshotSelection) -> RecordAuthorityStream:
        self._check()
        if selection is not self._selection:
            _refuse("selection does not belong to this prepared input")
        # The selection digest/counts are verified by stage/seal too. No object
        # is advertised as a reusable authority or externally trusted token.
        def operations():
            for ordinal, record in enumerate(self._records(selection.records)):
                yield RecordAuthorityOperation(ordinal=ordinal, record_id=record.record_id,
                                               primary=record.primary,
                                               descendants=record.descendants, edges=record.edges)
        return RecordAuthorityStream(operations=operations(),
                                     logical_sha256=selection.logical_sha256,
                                     record_count=selection.records,
                                     operation_count=selection.records,
                                     component_count=selection.components)


@contextmanager
def prepare_snapshot(manifest_path: str | Path):
    """Capture and authenticate the entire input before yielding anything ready."""
    started = time.perf_counter()
    path = Path(manifest_path)
    manifest_raw = path.read_bytes()
    manifest = _manifest(manifest_raw)
    with tempfile.TemporaryFile(mode="w+b", prefix="okg-authority-snapshot-") as handle:
        prepared = PreparedSnapshot(handle, manifest, len(manifest_raw),
                                    hashlib.sha256(manifest_raw).hexdigest())
        try:
            hasher = hashlib.sha256()
            size = 0
            with (path.parent / manifest["records_file"]).open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    hasher.update(chunk)
                    size += len(chunk)
                    handle.write(chunk)
            handle.flush()
            if size != manifest["records_bytes"] or hasher.hexdigest() != manifest["records_sha256"]:
                _refuse("captured byte count or SHA256 mismatch")
            prepared.capture_seconds = time.perf_counter() - started
            started = time.perf_counter()
            records = components = 0
            def validated():
                nonlocal records, components
                for record in prepared._records():
                    records += 1
                    components += _components(record)
                    yield record
            logical = legacy_record_set_digest(validated())
            if (records != manifest["record_count"] or components != manifest["component_count"]
                    or logical != manifest["logical_sha256"]):
                _refuse("complete logical digest or count mismatch")
            prepared.validation_seconds = time.perf_counter() - started
            yield prepared
        finally:
            prepared._active = False
