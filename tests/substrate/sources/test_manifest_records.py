"""Generic source-manifest reader (PACT no-code-source-manifest, task read).

The reader is driven through its `SourceAdapter.run` contract, the same
call the ingest runner makes, with the cursor each run returns handed to the
next. The runner computes retractions from the declared record set, so the
deletion rules are checked on that record set and on `completed_scope`.
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import EdgeFact, NodeFact, RecordSet
from okg.substrate.library.sources.manifest_records import ManifestRecordSource


MANIFEST = """
manifest: okg.source/v1
read:
  files: data/*.json
  records: issues
types:
  tracker_ticket:
    key: "{{key}}"
    title: fields.summary
    text: fields.description
    fields:
      status: fields.status
      votes: {{ from: fields.votes, type: integer }}
    links:
      assigned_to: {{ to: tracker_person, key: "{{fields.assignee.email}}" }}
      blocks: {{ to: tracker_ticket, each: fields.blocks, key: "{{.}}" }}
  tracker_person:
    from: fields.assignee
    key: "{{email}}"
    title: name
    fields:
      age: {{ from: age, type: integer }}
deletion: {deletion}
"""


def _ticket(key: str, *, summary: str = "", email: str = "ann@example.org", name: str = "Ann",
            blocks: tuple[str, ...] = (), votes: Any = 1, description: str = "") -> dict:
    return {"key": key, "fields": {
        "summary": summary or f"Ticket {key}", "status": "open", "votes": votes,
        "description": description or f"Body of {key} about turbidity",
        "assignee": {"email": email, "name": name}, "blocks": list(blocks),
    }}


class _Source:
    """One source over a tmp folder; each `run` carries the cursor forward."""

    def __init__(self, tmp_path: Path, *, deletion: str = "missing_from_full_read") -> None:
        self.root = tmp_path
        (tmp_path / "data").mkdir(exist_ok=True)
        self.manifest = tmp_path / "tickets.source.yaml"
        self.write_manifest(MANIFEST.format(deletion=deletion))
        self.cursor: dict | None = None

    def write_manifest(self, text: str) -> None:
        manifest = textwrap.dedent(text).replace("data/*", str(self.root / "data") + "/*")
        self.manifest.write_text(manifest)

    def write(self, name: str, tickets: list[dict]) -> None:
        (self.root / "data" / name).write_text(json.dumps({"issues": tickets}))

    def run(self, **kwargs):
        adapter = ManifestRecordSource(str(self.manifest), "tickets")
        result = adapter.run("run", mode="scope_complete", cursor=self.cursor, **kwargs)
        if result.next_cursor is not None:
            self.cursor = result.next_cursor
        self.metrics = adapter.last_run_metrics
        return result


def _nodes(run) -> dict[str, NodeFact]:
    return {f.node_id: f for f in run.facts if isinstance(f, NodeFact)}


def _edges(run) -> set[tuple[str, str, str]]:
    return {(f.src, f.edge_type, f.dst) for f in run.facts if isinstance(f, EdgeFact)}


def _record_edges(record_set: RecordSet) -> set[tuple[str, str, str]]:
    return {
        (e.src, e.edge_type, e.dst)
        for emission in record_set.records.values() for e in emission.edges
    }


def test_json_records_nested_people_links_and_dropped_links(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [
        _ticket("T-1", blocks=("T-2", "T-404")),
        _ticket("T-2", name="Ann B."),
        _ticket("T-3", email="bo@example.org", name="Bo"),
    ])
    run = source.run()
    assert run.completed_scope and run.run_mode == "scope_complete"
    assert set(run.record_set.records) == {
        "tracker_ticket:T-1", "tracker_ticket:T-2", "tracker_ticket:T-3",
        "tracker_person:ann@example.org", "tracker_person:bo@example.org",
    }
    nodes = _nodes(run)
    # One person record, shared by two parents; the later parent's value wins.
    ann = nodes["tickets:tracker_person:ann@example.org"]
    assert ann.attrs == {"title": "Ann B."}
    assert ann.source_record_id == {"type": "tracker_person", "key": "ann@example.org"}
    assert nodes["tickets:tracker_ticket:T-1"].attrs["votes"] == 1
    edges = _edges(run)
    assert ("tickets:tracker_ticket:T-1", "blocks", "tickets:tracker_ticket:T-2") in edges
    assert ("tickets:tracker_ticket:T-2", "assigned_to", "tickets:tracker_person:ann@example.org") in edges
    # The link to a record that is not in the read is not written.
    assert not any(dst.endswith("T-404") for _, _, dst in edges)
    assert source.metrics["links.dropped"] == 1
    assert source.metrics["nested.conflicts"] == 1
    assert "links.dropped=1" in run.health.reason
    # Parents carry only their edges; the person record owns its own node.
    ticket = run.record_set.records["tracker_ticket:T-2"]
    assert ticket.descendants == []
    assert {e.dst for e in ticket.edges} == {"tickets:tracker_person:ann@example.org"}


def test_jsonl_and_csv_inputs(tmp_path: Path) -> None:
    (tmp_path / "rows.jsonl").write_text(
        '{"id": "1", "name": "Alpha", "n": "4"}\n\n{"id": "2", "name": "Beta", "n": 5}\n'
    )
    (tmp_path / "rows.csv").write_text("id,name,n,flag\n3,Gamma,6,TRUE\n4,Delta,,false\n")
    for fmt, glob, expected in (
        ("jsonl", "rows.jsonl", {"item:1": 4, "item:2": 5}),
        ("csv", "rows.csv", {"item:3": 6, "item:4": None}),
    ):
        manifest = tmp_path / f"{fmt}.yaml"
        manifest.write_text(textwrap.dedent(f"""
            manifest: okg.source/v1
            read: {{files: {glob}}}
            types:
              item:
                key: "{{id}}"
                title: name
                fields:
                  n: {{from: n, type: integer}}
                  flag: {{from: flag, type: boolean}}
        """))
        run = ManifestRecordSource(str(manifest), "rows").run("r", mode="scope_complete")
        nodes = _nodes(run)
        assert set(run.record_set.records) == set(expected)
        for key, n in expected.items():
            attrs = nodes[f"rows:{key}"].attrs
            assert attrs.get("n") == n
        if fmt == "csv":
            assert nodes["rows:item:3"].attrs["flag"] is True
            assert nodes["rows:item:4"].attrs["flag"] is False


def test_long_text_becomes_linked_passages(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    long_text = " ".join(f"word{i}" for i in range(900))  # > 2,000 characters
    source.write("a.json", [_ticket("T-1", description=long_text), _ticket("T-2")])
    run = source.run()
    nodes = _nodes(run)
    passages = [n for n in nodes.values() if n.subtype == "tracker_ticket_passage"]
    assert len(passages) >= 2
    assert "".join(p.attrs["text"] for p in sorted(passages, key=lambda p: p.attrs["ordinal"]))[:1500] \
        == long_text[:1500]
    assert "body" not in nodes["tickets:tracker_ticket:T-1"].attrs
    assert nodes["tickets:tracker_ticket:T-2"].attrs["body"].startswith("Body of T-2")
    emission = run.record_set.records["tracker_ticket:T-1"]
    assert {d.node_id for d in emission.descendants} == {p.node_id for p in passages}
    assert {
        ("tickets:tracker_ticket:T-1", "contains", p.node_id) for p in passages
    } <= _edges(run)


def test_unchanged_records_write_nothing_and_an_edit_writes_only_that_record(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1"), _ticket("T-2")])
    first = source.run()
    assert len(_nodes(first)) == 3
    again = source.run()
    assert list(again.facts) == []
    assert set(again.record_set.records) == set(first.record_set.records)
    source.write("a.json", [_ticket("T-1", summary="Renamed"), _ticket("T-2")])
    edited = source.run()
    assert set(_nodes(edited)) == {"tickets:tracker_ticket:T-1"}
    assert _nodes(edited)["tickets:tracker_ticket:T-1"].attrs["title"] == "Renamed"


def test_a_manifest_edit_updates_every_affected_record(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1"), _ticket("T-2")])
    source.run()
    source.write_manifest(MANIFEST.format(deletion="missing_from_full_read").replace(
        "      status: fields.status\n", "      state: fields.status\n",
    ))
    run = source.run()
    nodes = _nodes(run)
    assert {"tickets:tracker_ticket:T-1", "tickets:tracker_ticket:T-2"} <= set(nodes)
    assert nodes["tickets:tracker_ticket:T-1"].attrs["state"] == "open"
    assert "status" not in nodes["tickets:tracker_ticket:T-1"].attrs


def test_change_probe_sees_file_and_manifest_changes(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1")])
    adapter = ManifestRecordSource(str(source.manifest), "tickets")
    token = adapter.change_probe.build_token()
    from okg.substrate.library.sources.change_probe import CURSOR_TOKEN_KEY

    cursor = {CURSOR_TOKEN_KEY: token}
    assert adapter.change_probe.probe(cursor=cursor, sync_scope=None).changed is False
    source.write("b.json", [_ticket("T-9")])
    assert adapter.change_probe.probe(cursor=cursor, sync_scope=None).changed is True
    (tmp_path / "data" / "b.json").unlink()
    assert adapter.change_probe.probe(cursor=cursor, sync_scope=None).changed is False
    source.write_manifest(MANIFEST.format(deletion="never"))
    assert adapter.change_probe.probe(cursor=cursor, sync_scope=None).changed is True


def test_missing_from_full_read_removes_absent_records_after_a_complete_read(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1", blocks=("T-2",)), _ticket("T-2")])
    source.run()
    source.write("a.json", [_ticket("T-1", blocks=("T-2",))])
    run = source.run()
    assert run.completed_scope
    assert "tracker_ticket:T-2" not in run.record_set.records
    # T-1's link to the removed record is gone from its footprint too.
    assert _record_edges(run.record_set) == {
        ("tickets:tracker_ticket:T-1", "assigned_to", "tickets:tracker_person:ann@example.org"),
    }


def test_a_read_that_rejects_an_earlier_record_applies_the_rest_and_removes_nothing(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1"), _ticket("T-2"), _ticket("T-3")])
    source.run()
    # T-1 edited, T-2 now invalid, T-3 left the export.
    source.write("a.json", [_ticket("T-1", summary="Edited"), _ticket("T-2", votes="many")])
    run = source.run()
    assert run.completed_scope and run.run_mode == "scope_complete"
    assert set(_nodes(run)) == {"tickets:tracker_ticket:T-1"}  # the valid edit applies
    assert {"tracker_ticket:T-2", "tracker_ticket:T-3"} <= set(run.record_set.records)  # nothing removed
    assert run.health.status == "ok"
    assert "ALL deletion is withheld" in run.health.reason and "tracker_ticket:T-2" in run.health.reason
    assert run.applied_token_authority == "exact"


def test_an_unreadable_file_withholds_deletion(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1")])
    source.write("c.json", [_ticket("T-3")])
    source.run()
    (tmp_path / "data" / "c.json").write_text("{not json")
    source.write("a.json", [_ticket("T-1", summary="Edited")])
    run = source.run()
    assert run.completed_scope
    assert "tracker_ticket:T-3" in run.record_set.records
    assert "tickets:tracker_ticket:T-1" in _nodes(run)
    assert "unreadable file" in run.health.reason


def test_a_new_invalid_record_is_rejected_and_the_read_stays_complete(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1"), _ticket("T-2", votes="many")])
    run = source.run()
    assert run.completed_scope
    assert set(r for r in run.record_set.records if r.startswith("tracker_ticket")) == {"tracker_ticket:T-1"}
    assert source.metrics["records.rejected"] == 1


def test_never_keeps_every_record_and_drops_stale_links(tmp_path: Path) -> None:
    source = _Source(tmp_path, deletion="never")
    source.write("a.json", [_ticket("T-1", blocks=("T-2",)), _ticket("T-2")])
    source.run()
    source.write("a.json", [_ticket("T-1")])  # T-2 gone, T-1 no longer blocks it
    run = source.run()
    assert run.completed_scope
    assert "tracker_ticket:T-2" in run.record_set.records
    assert ("tickets:tracker_ticket:T-1", "blocks", "tickets:tracker_ticket:T-2") not in _record_edges(
        run.record_set,
    )
    # T-2 comes back unchanged: nothing is written for it.
    source.write("a.json", [_ticket("T-1"), _ticket("T-2")])
    back = source.run()
    assert "tickets:tracker_ticket:T-2" not in _nodes(back)


def test_never_survives_a_replay_that_resets_the_cursor(tmp_path: Path) -> None:
    source = _Source(tmp_path, deletion="never")
    source.write("a.json", [_ticket("T-1"), _ticket("T-2")])
    first = source.run()
    source.write("a.json", [_ticket("T-1")])
    adapter = ManifestRecordSource(str(source.manifest), "tickets")
    adapter.prepare_authoritative_replay(record_set=first.record_set)
    run = adapter.run("replay", mode="scope_complete", cursor=None, authoritative_replay=True)
    assert "tracker_ticket:T-2" in run.record_set.records


def test_duplicate_keys_in_one_file_stop_the_run(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1"), _ticket("T-2")])
    source.run()
    before = dict(source.cursor)
    source.write("a.json", [_ticket("T-1"), _ticket("T-1", summary="again")])
    with pytest.raises(SubstrateError) as caught:
        source.run()
    assert caught.value.data["reason_code"] == "duplicate_key_in_file"
    assert source.cursor == before


def test_the_same_key_in_two_files_keeps_the_later_file(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1", summary="Old copy")])
    source.write("b.json", [_ticket("T-1", summary="New copy")])
    run = source.run()
    assert run.completed_scope
    assert _nodes(run)["tickets:tracker_ticket:T-1"].attrs["title"] == "New copy"


def test_a_glob_that_matches_nothing_fails_instead_of_reading_empty(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    with pytest.raises(SubstrateError) as caught:
        source.run()
    assert caught.value.data["code"] == "glob_no_match"


# ---- review fixes (69ef1b46d9) ------------------------------------------------


def _with_assignee(ticket: dict, **assignee) -> dict:
    ticket["fields"]["assignee"] = {"email": "ann@example.org", "name": "Ann", **assignee}
    return ticket


def test_a_shared_record_with_one_valid_copy_does_not_block_later_reads(tmp_path: Path) -> None:
    """B1: a bad nested copy under one parent must not count as losing the
    record another parent supplies validly; the source must keep updating."""
    source = _Source(tmp_path)
    source.write("a.json", [
        _with_assignee(_ticket("T-1"), age=3), _with_assignee(_ticket("T-2"), age="x"),
    ])
    first = source.run()
    assert first.completed_scope
    assert "tracker_person:ann@example.org" in first.record_set.records
    source.write("a.json", [
        _with_assignee(_ticket("T-1", summary="Edited"), age=3), _with_assignee(_ticket("T-2"), age="x"),
    ])
    second = source.run()
    assert second.completed_scope, second.health.reason
    assert _nodes(second)["tickets:tracker_ticket:T-1"].attrs["title"] == "Edited"


def test_a_few_keyless_records_are_skipped_and_deletion_goes_ahead(tmp_path: Path) -> None:
    """keyless A: a record with no key value is skipped and counted. It
    cannot be told from a deleted record, so an earlier record that lost its
    key is removed, like one that left the export; other deletions apply."""
    source = _Source(tmp_path)
    tickets = [_ticket(f"T-{n}") for n in range(1, 12)]
    source.write("a.json", tickets)
    source.run()
    keyless = _ticket("T-2")
    keyless["key"] = ""
    # T-2 lost its key; T-11 left the export. 1 of 11 read has no key.
    source.write("a.json", [tickets[0], keyless, *tickets[2:10], _ticket("T-12")])
    run = source.run()
    assert run.completed_scope
    kept = set(r for r in run.record_set.records if r.startswith("tracker_ticket"))
    assert "tracker_ticket:T-2" not in kept
    assert "tracker_ticket:T-11" not in kept
    assert "tracker_ticket:T-12" in kept
    assert source.metrics["records.skipped_keyless"] == 1
    assert source.metrics["records.rejected"] == 0
    assert "no usable key" not in run.health.reason


def test_a_nested_item_with_no_key_is_skipped_without_withholding_deletion(tmp_path: Path) -> None:
    """keyless A for nested items (an OpenAlex authorship with no author id)."""
    source = _Source(tmp_path)
    tickets = [_ticket(f"T-{n}", email=f"p{n}@example.org") for n in range(1, 12)]
    source.write("a.json", tickets)
    source.run()
    tickets[0]["fields"]["assignee"] = {"name": "Nobody"}  # no email: no key
    source.write("a.json", tickets[:10])  # T-11 left the export
    run = source.run()
    assert run.completed_scope
    assert "tracker_ticket:T-11" not in run.record_set.records
    assert "tracker_ticket:T-1" in run.record_set.records
    # The person who lost the key is gone like a deleted one.
    assert "tracker_person:p1@example.org" not in run.record_set.records
    assert "tracker_person:p2@example.org" in run.record_set.records
    assert source.metrics["records.skipped_keyless"] == 1


def test_too_many_keyless_nested_items_refuse_the_read(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    tickets = [_ticket(f"T-{n}", email=f"p{n}@example.org") for n in range(1, 6)]
    source.write("a.json", tickets)
    source.run()
    for ticket in tickets[:2]:
        ticket["fields"]["assignee"] = {"name": "Nobody"}
    source.write("a.json", tickets)
    with pytest.raises(SubstrateError) as caught:
        source.run()
    assert caught.value.data["keyless"] == {"tracker_person": {"keyless": 2, "read": 5}}


def test_a_keyless_record_still_yields_its_nested_items(tmp_path: Path) -> None:
    """Review M3: a person named only by a ticket with no key is still read
    (and counted), not silently dropped with its parent."""
    source = _Source(tmp_path)
    tickets = [_ticket(f"T-{n}", email=f"p{n}@example.org") for n in range(1, 12)]
    tickets[0]["key"] = ""
    source.write("a.json", tickets)
    run = source.run()
    assert run.completed_scope
    assert "tracker_ticket:T-1" not in run.record_set.records
    assert "tracker_person:p1@example.org" in run.record_set.records
    assert source.metrics["records.skipped_keyless"] == 1


def test_keyless_share_does_not_refuse_under_never(tmp_path: Path) -> None:
    """Review m2: nothing is removed under `deletion: never`, so skipping is enough."""
    source = _Source(tmp_path, deletion="never")
    source.write("a.json", [_ticket("T-1"), _ticket("T-2")])
    source.run()
    keyless = _ticket("T-9")
    keyless["key"] = ""
    source.write("a.json", [_ticket("T-1"), keyless])
    run = source.run()
    assert run.completed_scope
    assert "tracker_ticket:T-2" in run.record_set.records
    assert source.metrics["records.skipped_keyless"] == 1


def test_too_many_keyless_records_refuse_the_read(tmp_path: Path) -> None:
    """keyless A: more than 10% of one type's records with no key value
    means the key path is probably wrong; nothing is applied or removed."""
    source = _Source(tmp_path)
    source.write("a.json", [_ticket(f"T-{n}") for n in range(1, 6)])
    source.run()
    keyless = [_ticket(f"T-{n}") for n in range(1, 6)]
    for record in keyless[:2]:
        record["key"] = ""
    source.write("a.json", keyless)
    with pytest.raises(SubstrateError) as caught:
        source.run()
    assert caught.value.data["reason_code"] == "keyless_share_exceeded"
    assert caught.value.data["keyless"] == {"tracker_ticket": {"keyless": 2, "read": 5}}
    assert "nothing was applied or removed" in str(caught.value)


def test_a_key_with_a_line_break_is_refused(tmp_path: Path) -> None:
    """N7."""
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1"), _ticket("T-2\nX")])
    run = source.run()
    assert set(r for r in run.record_set.records if r.startswith("tracker_ticket")) == {"tracker_ticket:T-1"}
    assert source.metrics["records.rejected"] == 1


def test_yaml_inputs_read_like_json(tmp_path: Path) -> None:
    """#3417 phase 1: YAML data files, with `records:` and unquoted dates."""
    (tmp_path / "rows.yaml").write_text(textwrap.dedent("""
        meta: {exported: 2026-10-01}
        items:
          - {id: "1", name: Alpha, n: "4", seen: 2026-09-30}
          - id: "2"
            name: Beta
            n: 5
            seen: 2026-09-30T12:00:00Z
    """))
    (tmp_path / "one.yml").write_text("id: '3'\nname: Gamma\nn: 6\n")
    for glob, records, expected in (
        ("rows.yaml", "items", {"item:1": 4, "item:2": 5}),
        ("one.yml", None, {"item:3": 6}),
    ):
        manifest = tmp_path / f"{glob}.source.yaml"
        records_line = f"records: {records}, " if records else ""
        manifest.write_text(textwrap.dedent(f"""
            manifest: okg.source/v1
            read: {{files: {glob}, {records_line}}}
            types:
              item:
                key: "{{id}}"
                title: name
                fields:
                  n: {{from: n, type: integer}}
                  seen: seen
        """))
        run = ManifestRecordSource(str(manifest), "rows").run("r", mode="scope_complete")
        nodes = _nodes(run)
        assert set(run.record_set.records) == set(expected)
        for key, n in expected.items():
            assert nodes[f"rows:{key}"].attrs.get("n") == n
        if glob == "rows.yaml":
            # A YAML date reads as the same text JSON would carry.
            assert nodes["rows:item:1"].attrs["seen"] == "2026-09-30"
            assert nodes["rows:item:2"].attrs["seen"].startswith("2026-09-30T12:00:00")


def test_a_yaml_file_that_does_not_parse_is_unreadable(tmp_path: Path) -> None:
    (tmp_path / "bad.yaml").write_text("items: [\n")
    manifest = tmp_path / "m.source.yaml"
    manifest.write_text('manifest: okg.source/v1\nread: {files: bad.yaml, records: items}\n'
                        'types:\n  item: {key: "{id}"}\n')
    run = ManifestRecordSource(str(manifest), "rows").run("r", mode="scope_complete")
    assert "is not YAML" in run.health.reason


@pytest.mark.parametrize(("body", "reason"), [
    ("items: &a [*a]\n", "uses a YAML alias"),
    ("base: &b {id: '1'}\nitems: [*b, *b]\n", "uses a YAML alias"),
    ("items:\n  - {id: '1', 1: a, '1': b}\n", "both read as '1'"),
    ("items:\n  - {id: '1', 1: a, true: b}\n", "read as the same key"),
    ("items: " + "[" * 5000 + "]" * 5000 + "\n", "nests too deeply"),
])
def test_a_yaml_alias_or_a_key_clash_makes_the_file_unreadable(
    tmp_path: Path, body: str, reason: str,
) -> None:
    """A self-referring alias would recurse without end, repeated aliases
    expand without limit, and two keys that read as the same text would
    silently drop one value."""
    (tmp_path / "bad.yaml").write_text(body)
    manifest = tmp_path / "m.source.yaml"
    manifest.write_text('manifest: okg.source/v1\nread: {files: bad.yaml, records: items}\n'
                        'types:\n  item: {key: "{id}"}\n')
    run = ManifestRecordSource(str(manifest), "rows").run("r", mode="scope_complete")
    assert run.record_set.records == {}
    assert "unreadable file" in run.health.reason and reason in run.health.reason


def test_a_manifest_beside_its_yaml_data_is_not_read_as_data(tmp_path: Path) -> None:
    """`okg source draft a.yaml b.yaml > tickets.yaml` drafts the glob `*.yaml`,
    which matches the saved manifest too."""
    (tmp_path / "a.yaml").write_text("items: [{id: '1'}]\n")
    (tmp_path / "b.yaml").write_text("items: [{id: '2'}]\n")
    manifest = tmp_path / "tickets.yaml"
    manifest.write_text(f'manifest: okg.source/v1\nread: {{files: "{tmp_path}/*.yaml", records: items}}\n'
                        'types:\n  item: {key: "{id}"}\n')
    run = ManifestRecordSource(str(manifest), "rows").run("r", mode="scope_complete")
    assert set(run.record_set.records) == {"item:1", "item:2"}
    assert run.health.status == "ok" and "files.matched=2," in run.health.reason


def test_a_csv_file_with_a_repeated_column_is_not_read(tmp_path: Path) -> None:
    """N7: a repeated header would keep only one of the two values."""
    (tmp_path / "rows.csv").write_text("id,name,name\n1,Alpha,Beta\n")
    manifest = tmp_path / "m.yaml"
    manifest.write_text('manifest: okg.source/v1\nread: {files: rows.csv}\ntypes:\n  item: {key: "{id}"}\n')
    run = ManifestRecordSource(str(manifest), "rows").run("r", mode="scope_complete")
    assert run.record_set.records == {}
    assert "unreadable file" in run.health.reason and "repeats the CSV column" in run.health.reason


def test_blank_csv_column_names_are_not_a_repeat(tmp_path: Path) -> None:
    """Trailing empty spreadsheet columns have blank names."""
    (tmp_path / "rows.csv").write_text("id,name,,\n1,Alpha,,\n")
    manifest = tmp_path / "m.yaml"
    manifest.write_text('manifest: okg.source/v1\nread: {files: rows.csv}\ntypes:\n  item: {key: "{id}", title: name}\n')
    run = ManifestRecordSource(str(manifest), "rows").run("r", mode="scope_complete")
    assert set(run.record_set.records) == {"item:1"}
    assert _nodes(run)["rows:item:1"].attrs == {"title": "Alpha"}


def test_a_complete_read_returns_the_probe_token_of_what_it_parsed(tmp_path: Path) -> None:
    """S1: the exact applied token lets an unchanged `okg ingest` skip the read."""
    from okg.substrate.library.sources.change_probe import tokens_equal

    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1")])
    adapter = ManifestRecordSource(str(source.manifest), "tickets")
    assert adapter.probe_short_circuit_safe is True
    run = adapter.run("r", mode="scope_complete")
    assert run.applied_token_scope == "global" and run.applied_token_authority == "exact"
    assert tokens_equal(run.applied_raw_token, adapter.change_probe.build_token())
    source.write("b.json", [_ticket("T-9")])  # a new matching file
    assert not tokens_equal(run.applied_raw_token, adapter.change_probe.build_token())
    (tmp_path / "data" / "b.json").unlink()
    (tmp_path / "data" / "a.json").write_text(json.dumps({"issues": [_ticket("T-1", summary="x")]}))
    assert not tokens_equal(run.applied_raw_token, adapter.change_probe.build_token())


def test_a_read_with_bad_rows_still_returns_the_exact_token(tmp_path: Path) -> None:
    """The runner manages this reader's change identity: every read it
    applies must return the exact token, or the runner refuses the run."""
    from okg.substrate.library.sources.change_probe import tokens_equal

    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1")])
    (tmp_path / "data" / "b.json").write_text("{not json")
    adapter = ManifestRecordSource(str(source.manifest), "tickets")
    run = adapter.run("r", mode="scope_complete")
    assert run.applied_token_scope == "global" and run.applied_token_authority == "exact"
    assert tokens_equal(run.applied_raw_token, adapter.change_probe.build_token())


def test_links_to_withheld_records_are_kept(tmp_path: Path) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1", blocks=("T-2",)), _ticket("T-2")])
    source.run()
    source.write("a.json", [_ticket("T-1", summary="Edited", blocks=("T-2",)), _ticket("T-2", votes="many")])
    run = source.run()
    assert ("tickets:tracker_ticket:T-1", "blocks", "tickets:tracker_ticket:T-2") in _record_edges(run.record_set)
    assert source.metrics["links.dropped"] == 0


def test_bad_rows_are_warnings_under_never(tmp_path: Path) -> None:
    source = _Source(tmp_path, deletion="never")
    source.write("a.json", [_ticket("T-1"), _ticket("T-2")])
    source.run()
    source.write("a.json", [_ticket("T-1"), _ticket("T-3", votes="many")])
    run = source.run()
    assert run.completed_scope and run.health.status == "ok"
    assert "deletion: never" in run.health.reason
    assert "tracker_ticket:T-2" in run.record_set.records


def test_a_read_that_must_keep_records_it_no_longer_has_is_refused(tmp_path: Path) -> None:
    """The earlier record set is gone (an old cursor), so deletion cannot be
    withheld: the run is refused with its real cause, not applied."""
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1"), _ticket("T-2")])
    source.run()
    source.cursor = {k: v for k, v in source.cursor.items() if k != "emissions"}
    source.write("a.json", [_ticket("T-1"), _ticket("T-2", votes="many")])
    with pytest.raises(SubstrateError) as caught:
        source.run()
    assert caught.value.data["reason_code"] == "prior_record_set_unavailable"
    assert "tracker_ticket:T-2" in str(caught.value)



def test_a_home_relative_manifest_path_is_expanded(tmp_path: Path, monkeypatch) -> None:
    source = _Source(tmp_path)
    source.write("a.json", [_ticket("T-1")])
    monkeypatch.setenv("HOME", str(tmp_path))
    adapter = ManifestRecordSource("~/tickets.source.yaml", "tickets")
    run = adapter.run("r", mode="scope_complete")
    assert "tracker_ticket:T-1" in run.record_set.records
