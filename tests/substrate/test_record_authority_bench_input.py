"""Complete input validation and capture lifetime for the real benchmark."""
from __future__ import annotations

import hashlib
import json
import tracemalloc
from dataclasses import replace
from pathlib import Path

import pytest

from okg.substrate.ingest.record_authority import AuthorityError, StreamingRecord, legacy_record_set_digest
from okg.substrate.library.sources.base import EdgeKey, RecordNode
from okg.substrate.record_authority_bench import _parse_args, build_stream, BenchShape, run_real_bench
from okg.substrate.record_authority_bench_input import SNAPSHOT_SCHEMA, decode_record, prepare_snapshot


def record(key="a", *, rich=False):
    primary = RecordNode("node:" + key, "dataset", {"id": key}, {"n_records": 200000, "ratio": 1.25}).to_cursor()
    return {"record_id": key, "primary": primary,
            "descendants": [RecordNode("child:" + key, "dataset", {}, {}, "substitute").to_cursor()] if rich else [],
            "edges": [EdgeKey("node:" + key, "child:" + key, "contains", {"unicode": "é雪"}).to_cursor()] if rich else []}


def write_snapshot(directory: Path, records, **overrides):
    directory.mkdir(parents=True, exist_ok=True)
    raw = b"".join((json.dumps(r, ensure_ascii=True) + "\n").encode() for r in records)
    manifest = {
        "schema": SNAPSHOT_SCHEMA, "projection": "complete_records.v1", "complete": True,
        "records_file": "records.jsonl", "records_bytes": len(raw),
        "records_sha256": hashlib.sha256(raw).hexdigest(), "record_count": len(records),
        "component_count": sum(int(r["primary"] is not None) + len(r["descendants"]) + len(r["edges"]) for r in records),
        "logical_sha256": legacy_record_set_digest(StreamingRecord(**r) for r in records),
        "source": "test.source", "adapter_source": "test_source", "fixed_run_id": "fixed",
        "source_revision": {"n_records": 200000, "complete_hash": "keep-me"},
        "source_snapshot": {"records_sha256": "a" * 64},
        "provenance": {"external_attestation": "not core authenticated"},
        "full_input_nul_sanitization_completed": True,
        "counts": {"input_rows": len(records), "emitted_inserts": len(records),
                   "equal_duplicates": 0, "final_unique": len(records), "rejected": 0,
                   "held": 0, "unexpected_operations": 0, "authority_normalization_changes": 0},
    }
    manifest.update(overrides)
    (directory / "records.jsonl").write_bytes(raw)
    path = directory / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def replace_records(path, raw):
    manifest = json.loads(path.read_text())
    manifest.update(records_bytes=len(raw), records_sha256=hashlib.sha256(raw).hexdigest())
    (path.parent / "records.jsonl").write_bytes(raw)
    path.write_text(json.dumps(manifest))


def test_full_sample_metadata_and_empty(tmp_path):
    records = [record("a", rich=True), record("é雪")]
    path = write_snapshot(tmp_path, records)
    with prepare_snapshot(path) as prepared:
        selected = prepared.select(1)
        assert selected.records == 1 and selected.components == 3
        assert selected.logical_sha256 == legacy_record_set_digest([StreamingRecord(**records[0])])
        operation = list(prepared.stream(selected).operations)[0]
        assert operation.primary == records[0]["primary"]
        assert "ownership_mode" not in operation.primary
        assert operation.descendants[0]["ownership_mode"] == "substitute"
        assert operation.edges[0] == records[0]["edges"][0]
        assert prepared.manifest["source_revision"]["n_records"] == 200000
        modified = prepared.manifest
        modified["source_revision"].clear()
        assert prepared.manifest["source_revision"]["n_records"] == 200000
        assert prepared.select().logical_sha256 == json.loads(path.read_text())["logical_sha256"]
        assert prepared.max_record_bytes > 0
        with pytest.raises(AuthorityError, match="sample"):
            prepared.select(3)
    empty = write_snapshot(tmp_path / "empty", [])
    with prepare_snapshot(empty) as prepared:
        selected = prepared.select()
        assert selected.records == selected.components == 0
        assert selected.policy == "full"
        assert selected.logical_sha256 == legacy_record_set_digest([])
        assert list(prepared.stream(selected).operations) == []
        assert prepared.max_record_bytes == 0


@pytest.mark.parametrize("sample", [True, -1, 1.5, "1"])
def test_invalid_sample(tmp_path, sample):
    with prepare_snapshot(write_snapshot(tmp_path, [record()])) as prepared:
        with pytest.raises(AuthorityError):
            prepared.select(sample)


@pytest.mark.parametrize("changes", [
    {"schema": "wrong"}, {"complete": False}, {"complete": 1},
    {"records_bytes": True}, {"record_count": True}, {"component_count": -1},
    {"records_sha256": "A" * 64}, {"records_sha256": "0" * 64},
    {"logical_sha256": "0" * 64}, {"records_bytes": 1}, {"component_count": 50},
    {"source_revision": []}, {"provenance": None}, {"records_file": "../records.jsonl"},
    {"records_file": "/tmp/records.jsonl"}, {"unknown": 1},
])
def test_manifest_refusals(tmp_path, changes):
    path = write_snapshot(tmp_path, [record()], **changes)
    with pytest.raises(AuthorityError), prepare_snapshot(path):
        pytest.fail("invalid manifest prepared")


@pytest.mark.parametrize("field,value", [("final_unique", True), ("held", 1),
                                          ("rejected", 1), ("unexpected_operations", 1),
                                          ("equal_duplicates", 1), ("final_unique", 2)])
def test_manifest_count_relations(tmp_path, field, value):
    path = write_snapshot(tmp_path, [record()])
    m = json.loads(path.read_text())
    m["counts"][field] = value
    path.write_text(json.dumps(m))
    with pytest.raises(AuthorityError), prepare_snapshot(path):
        pytest.fail("invalid counts prepared")


@pytest.mark.parametrize("raw", [
    b'{"record_id":"a","record_id":"b","primary":null,"descendants":[],"edges":[]}',
    b'{"record_id":"\\ud800","primary":null,"descendants":[],"edges":[]}',
    b'{"record_id":"\xff","primary":null,"descendants":[],"edges":[]}',
    b'\n', b'[]', b'{}',
])
def test_strict_json_refuses(raw):
    with pytest.raises(AuthorityError):
        decode_record(raw)


@pytest.mark.parametrize("path,value", [
    (("primary", "node_id"), ""), (("primary", "node_id"), "nul\x00"),
    (("primary", "subtype"), 1), (("primary", "source_record_id"), None),
    (("primary", "source_revision"), []), (("primary", "ownership_mode"), "other"),
    (("primary", "unexpected"), 0), (("edges", 0, "confidence"), True),
    (("edges", 0, "attrs"), []), (("edges", 0, "src"), 12),
    (("edges", 0, "provenance"), None), (("descendants",), {}),
    (("record_id",), "x" * 2049), (("primary", "node_id"), "x" * 4097),
    (("primary", "node_id"), "雪" * 683),
    (("edges", 0, "src"), "x" * 4090),
])
def test_exact_component_types_and_existing_limits(path, value):
    r = record(rich=True)
    parent = r
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    with pytest.raises(AuthorityError):
        decode_record(json.dumps(r).encode())


@pytest.mark.parametrize("component,field", [("primary", "source_revision"),
                                           ("primary", "source_record_id"),
                                           ("edges", "attrs"), ("edges", "confidence")])
def test_missing_fields_never_default(component, field):
    r = record(rich=True)
    del (r[component][0] if component == "edges" else r[component])[field]
    with pytest.raises(AuthorityError):
        decode_record(json.dumps(r).encode())


@pytest.mark.parametrize("late", [record("a"), record("0"), {"bad": "record"}])
def test_invalid_after_prefix_refuses_before_connection(tmp_path, monkeypatch, late):
    path = write_snapshot(tmp_path, [record("a"), record("b")])
    replace_records(path, (json.dumps(record("a")) + "\n" + json.dumps(late) + "\n").encode())
    monkeypatch.setattr("okg.substrate.record_authority_bench.connect",
                        lambda *a, **k: pytest.fail("database opened before complete validation"))
    with pytest.raises(AuthorityError):
        run_real_bench("unused", manifest_path=str(path), sample_records=1)


def test_full_logical_mismatch_refuses_before_connection(tmp_path, monkeypatch):
    path = write_snapshot(tmp_path, [record()], logical_sha256="0" * 64)
    monkeypatch.setattr("okg.substrate.record_authority_bench.connect",
                        lambda *a, **k: pytest.fail("database opened for wrong logical digest"))
    with pytest.raises(AuthorityError, match="logical"):
        run_real_bench("unused", manifest_path=str(path), sample_records=0)


def test_capture_survives_original_replacement_and_refuses_escape(tmp_path):
    records = [record("a"), record("b")]
    path = write_snapshot(tmp_path, records)
    with prepare_snapshot(path) as prepared:
        path.write_text("not JSON")
        (tmp_path / "records.jsonl").unlink()
        (tmp_path / "records.jsonl").write_text("replacement")
        selected = prepared.select()
        stream = prepared.stream(selected)
        escaped = iter(stream.operations)
        assert next(escaped).record_id == "a"
        assert selected.logical_sha256 == legacy_record_set_digest(StreamingRecord(**r) for r in records)
    with pytest.raises(AuthorityError, match="closed"):
        next(escaped)
    with pytest.raises(AuthorityError, match="closed"):
        prepared.select()
    assert prepared._handle.closed


def test_cleanup_on_invalid_capture_and_consumer_failure(tmp_path, monkeypatch):
    from okg.substrate.record_authority_bench_input import tempfile as snapshot_tempfile
    original = snapshot_tempfile.TemporaryFile
    handles = []
    def tracked(*a, **kw):
        handle = original(*a, **kw)
        handles.append(handle)
        return handle
    monkeypatch.setattr(snapshot_tempfile, "TemporaryFile", tracked)
    path = write_snapshot(tmp_path, [record()], logical_sha256="0" * 64)
    with pytest.raises(AuthorityError), prepare_snapshot(path):
        pytest.fail("bad digest")
    assert handles[-1].closed
    path = write_snapshot(tmp_path, [record()])
    caught = False
    try:
        with prepare_snapshot(path):
            raise RuntimeError("consumer")
    except RuntimeError as exc:
        caught = True
        assert str(exc) == "consumer"
    assert caught
    assert handles[-1].closed


def test_selection_binding_and_overlapping_passes_refuse(tmp_path):
    with prepare_snapshot(write_snapshot(tmp_path, [record("a"), record("b")])) as prepared:
        selected = prepared.select()
        with pytest.raises(AuthorityError, match="selection"):
            prepared.stream(replace(selected, records=1))
        iterator = iter(prepared.stream(selected).operations)
        next(iterator)
        with pytest.raises(AuthorityError, match="overlapping"):
            prepared.select()
        iterator.close()
        assert prepared.select().records == 2


def test_decoding_memory_does_not_collect_source(tmp_path):
    # Construct the corpus before tracing. A whole-source list of decoded
    # 16 KiB metadata records would exceed 16 MiB; only one record is required.
    rows = []
    for i in range(1200):
        r = record(f"{i:06}")
        r["primary"]["source_revision"]["body"] = "z" * 16384
        rows.append(r)
    path = write_snapshot(tmp_path, rows)
    tracemalloc.start()
    try:
        with prepare_snapshot(path) as prepared:
            selected = prepared.select()
            assert sum(1 for _ in prepared.stream(selected).operations) == 1200
            _, peak = tracemalloc.get_traced_memory()
            assert prepared.max_record_bytes > 16384
            assert peak < 8 * 1024 * 1024
    finally:
        tracemalloc.stop()


def test_synthetic_defaults_unchanged_and_real_options_explicit():
    args = _parse_args(["--dsn", "unused"])
    assert args.records == 20000 and args.seed == 1664
    assert args.input_manifest is None and args.sample_records is None
    assert build_stream(3, BenchShape(descendants=0, edges=0, body_bytes=0), 7).record_count == 3
    with pytest.raises(SystemExit):
        _parse_args(["--dsn", "unused", "--sample-records", "1"])
    with pytest.raises(SystemExit):
        _parse_args(["--dsn", "unused", "--input-manifest", "manifest", "--records", "1"])


@pytest.mark.parametrize("option", ["--rec", "--see", "--desc", "--ed", "--body-b"])
def test_real_input_refuses_abbreviated_synthetic_options(option):
    with pytest.raises(SystemExit):
        _parse_args(["--dsn", "unused", "--input-manifest", "manifest", option, "1"])
    assert _parse_args(["--dsn", "unused", "--rec", "1"]).records == 1


@pytest.mark.parametrize("provenance", ["", "metadata\x00kept"])
def test_edge_provenance_metadata_is_preserved(tmp_path, provenance):
    r = record(rich=True)
    r["edges"][0]["provenance"] = provenance
    with prepare_snapshot(write_snapshot(tmp_path, [r])) as prepared:
        selected = prepared.select()
        operation = next(iter(prepared.stream(selected).operations))
        assert operation.edges[0]["provenance"] == provenance
        assert selected.logical_sha256 == legacy_record_set_digest([StreamingRecord(**r)])


@pytest.mark.parametrize("number", [b"NaN", b"Infinity", b"-Infinity", b"1e999"])
def test_nonfinite_numbers_in_permitted_metadata_refuse(number):
    r = record()
    r["primary"]["source_revision"]["value"] = "NUMBER_TOKEN"
    raw = json.dumps(r).encode().replace(b'"NUMBER_TOKEN"', number)
    with pytest.raises(AuthorityError):
        decode_record(raw)


def test_duplicate_keys_in_permitted_metadata_refuse():
    r = record()
    r["primary"]["source_revision"] = {"value": 1}
    raw = json.dumps(r).encode().replace(b'"value": 1', b'"value": 1, "value": 2')
    with pytest.raises(AuthorityError):
        decode_record(raw)
