"""task.row.durable-evidence witnesses: the scored layer survives the worktree.

The failure being fixed: every recorded run artifact this benchmark produced
lived under ``.scratch/``, which is gitignored and worktree-local, and two
worktree recycles destroyed all of it. The numbers survived in commit messages
and PACT evidence summaries; the ability to re-derive them did not.

The load-bearing test here is
:func:`test_the_committed_record_rescores_with_no_database_and_no_model` — it
reads the record COMMITTED IN THIS REPOSITORY, in a plain non-postgres run, and
rebuilds the scorecard from it. That is the benchmark's whole offer ("re-score
it yourself") executed against the durable artifact rather than asserted about
it.
"""

from __future__ import annotations

import gzip
import io
import json
import tarfile
from pathlib import Path

import pytest

from okg.bench.synth.durable import (
    ANALYSIS_RENDER_SUFFIX,
    ANALYSIS_SUFFIX,
    BUNDLE_SUFFIX,
    BUNDLED_FILENAMES,
    DEFAULT_RECORDS_ROOT,
    EXCLUDED_PREFIXES,
    INDEX_FILENAME,
    REQUIRED_ANALYSIS_LABELS,
    REQUIRED_FILENAMES,
    SIDECAR_FILENAMES,
    DurableRecordError,
    analysis_record_id,
    analysis_records_by_label,
    assert_analysis_recorded,
    content_digest_of,
    missing_analysis_labels,
    read_analysis,
    store_analysis_artifact,
    prune_index,
    read_index,
    record_id_for,
    record_id_of_stored,
    records_root_from_env,
    rescore_record,
    resolve_bundle,
    scored_members,
    sidecar_name,
    stored_members,
    store_scored_artifacts,
    unpack_record,
)
from okg.bench.synth.runner import (
    ENGINE_READS_FILENAME,
    FUNNEL_ROWS_FILENAME,
    MANIFEST_FILENAME,
    RUN_BINDING_FILENAME,
    SCORECARD_FILENAME,
    report_from_artifacts,
)


def _committed_records() -> list[str]:
    """The SCORED records — the ones a re-score can be run against.

    Analysis records (rows, spreads, the paired effect) are derived artifacts,
    not runs: they carry no manifest and no engine reads, so they are excluded
    here and witnessed by their own tests below.
    """
    return [
        row["record_id"]
        for row in read_index()["records"]
        if row.get("kind") != "analysis"
    ]


def test_at_least_one_record_is_committed() -> None:
    """Guards the parametrized acceptances above from passing vacuously.

    ``pytest.mark.parametrize`` over an empty list collects ZERO tests and
    reports green, so an empty records directory would make every re-score
    witness below disappear silently — the same shape of failure as an evidence
    item pointing at nothing.
    """
    assert _committed_records(), (
        f"no records under {DEFAULT_RECORDS_ROOT}; regenerate with "
        "scripts/regen_synth_scored_records.py"
    )


# =========================================================================== #
# THE ACCEPTANCE: a stored record re-scores, with no database and no model     #
# =========================================================================== #
@pytest.mark.parametrize("record_id", _committed_records())
def test_the_committed_record_rescores_with_no_database_and_no_model(
    record_id: str,
) -> None:
    """Re-derive the numbers from the committed artifact alone.

    No postgres mark, no DSN, no API key: if this passes, a reader in any
    checkout can check the benchmark's numbers, which is precisely what was
    lost when the artifacts lived in ``.scratch/``.
    """
    core = rescore_record(record_id)

    # Non-vacuous: the re-score must reproduce the record's OWN recorded
    # scorecard, not merely return some dict. A rescore that silently drifted
    # from the run it claims to reproduce is the failure this guards.
    row = next(r for r in read_index()["records"] if r["record_id"] == record_id)
    head = row["headline"]
    assert core["config_digest"] == head["config_digest"]
    assert core["manifest_digest"] == head["manifest_digest"]
    assert core["funnel"]["detection_recall"] == head["detection_recall"]
    assert core["funnel"]["repair_rate"] == head["repair_rate"]
    assert core["funnel"]["states"] == head["funnel_states"]

    # And the denominators are real, so the pass cannot be vacuous.
    assert core["funnel"]["detection_recall"]["end_to_end"]["denominator"] > 0


@pytest.mark.parametrize("record_id", _committed_records())
def test_the_committed_record_renders_an_operator_report(record_id: str) -> None:
    from okg.bench.synth.durable import open_record

    with open_record(record_id) as run_dir:
        text = report_from_artifacts(run_dir)
        # The generation pins ride the plain-text sidecar, so the reconstructed
        # directory is the whole run directory a reader would have had.
        pins = json.loads((run_dir / RUN_BINDING_FILENAME).read_text())
    assert "injection survival" in text
    assert "detection recall" in text
    assert pins["clean_generation_id"].startswith("okg:")
    assert pins["corrupted_generation_id"].startswith("okg:")


def test_the_records_directory_is_not_gitignored() -> None:
    """The whole point is that this survives a worktree recycle.

    ``.scratch/`` is in ``.gitignore``; if the records root ever landed under an
    ignored path the mechanism would fail in exactly the silent way it was built
    to prevent.
    """
    root = DEFAULT_RECORDS_ROOT
    assert root.is_dir(), f"{root} does not exist"
    parts = set(root.resolve().parts)
    assert ".scratch" not in parts
    ignore = (Path(__file__).resolve().parents[3] / ".gitignore").read_text()
    ignored = {
        line.strip().strip("/")
        for line in ignore.splitlines()
        if line.strip() and not line.startswith("#")
    }
    assert not ({"bench", "bench/synth", "bench/synth/records"} & ignored)


# =========================================================================== #
# The scope discipline: small scored layer in, large corpora out              #
# =========================================================================== #
@pytest.mark.parametrize("record_id", _committed_records())
def test_no_corpus_or_verbatim_response_rode_along(record_id: str) -> None:
    """The excluded set is checked, not merely documented.

    Rendered corpora, PDFs, verbatim model responses and full claim snapshots
    are large and are not what makes a claim checkable — re-score never reads
    them. A future edit that quietly widened the stored set would show up here.
    """
    bundle = resolve_bundle(record_id)
    with tarfile.open(bundle, "r:gz") as tar:
        names = tar.getnames()
    assert names, "empty bundle"
    for name in names:
        assert name in BUNDLED_FILENAMES, name
        for prefix in EXCLUDED_PREFIXES:
            assert not name.startswith(prefix), name


@pytest.mark.parametrize("record_id", _committed_records())
def test_the_bundle_stays_small(record_id: str) -> None:
    """Repository weight is a real constraint here (see the .gitignore preamble:
    two oversized blobs had to be cut out of history). A record that grew past
    this ceiling is a design change, not a rounding error, and should fail
    rather than accumulate quietly."""
    bundle = resolve_bundle(record_id)
    assert bundle.stat().st_size < 256 * 1024, (
        f"{bundle.name} is {bundle.stat().st_size:,}B; the scored layer was "
        "measured at ~26 KiB and is not supposed to carry corpora"
    )


# =========================================================================== #
# Fail-closed: a bundle that resolves but cannot answer is the original bug    #
# =========================================================================== #
def _fake_run_dir(tmp_path: Path) -> Path:
    """Borrow the committed record's real artifacts as a run directory."""
    from okg.bench.synth.durable import open_record

    with open_record(_committed_records()[0]) as src:
        dest = tmp_path / "run"
        dest.mkdir()
        for item in Path(src).iterdir():
            (dest / item.name).write_bytes(item.read_bytes())
    return dest


def test_storing_a_run_missing_the_manifest_is_refused(tmp_path: Path) -> None:
    run_dir = _fake_run_dir(tmp_path)
    (run_dir / MANIFEST_FILENAME).unlink()
    with pytest.raises(DurableRecordError, match=MANIFEST_FILENAME):
        store_scored_artifacts(run_dir, records_root=tmp_path / "records")


def test_storing_a_run_missing_the_engine_reads_is_refused(tmp_path: Path) -> None:
    """Storing manifest DIGESTS alone would not be enough, and this is why:
    ``rescore_from_artifacts`` reads the manifest and the engine reads and
    nothing else, so a record without them resolves as a path and still fails
    to answer — the exact failure mode being fixed."""
    run_dir = _fake_run_dir(tmp_path)
    (run_dir / ENGINE_READS_FILENAME).unlink()
    with pytest.raises(DurableRecordError, match=ENGINE_READS_FILENAME):
        store_scored_artifacts(run_dir, records_root=tmp_path / "records")


def test_a_bundle_naming_a_traversal_path_is_refused(tmp_path: Path) -> None:
    """A bundle is data read off disk, so it does not get to name its own
    extraction paths."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        info = tarfile.TarInfo("../escaped.json")
        payload = b"{}"
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    bundle = tmp_path / f"evil{BUNDLE_SUFFIX}"
    bundle.write_bytes(gzip.compress(buf.getvalue(), 9, mtime=0))
    with pytest.raises(DurableRecordError, match="not a known scored artifact"):
        unpack_record(bundle, tmp_path / "out")
    assert not (tmp_path / "escaped.json").exists()


# =========================================================================== #
# Round trip + the byte-stability the weight argument rests on                #
# =========================================================================== #
def test_store_then_unpack_round_trips_every_scored_file(tmp_path: Path) -> None:
    run_dir = _fake_run_dir(tmp_path)
    (run_dir / FUNNEL_ROWS_FILENAME).write_text(
        '[{"corruption_id":"proof-row","state":"repaired"}]\n',
        encoding="utf-8",
    )
    root = tmp_path / "records"
    bundle = store_scored_artifacts(run_dir, records_root=root)
    back = unpack_record(bundle, tmp_path / "back")
    for name in (
        *REQUIRED_FILENAMES,
        SCORECARD_FILENAME,
        FUNNEL_ROWS_FILENAME,
        *SIDECAR_FILENAMES,
    ):
        if (run_dir / name).is_file():
            assert (back / name).read_bytes() == (run_dir / name).read_bytes(), name


def test_restoring_an_unchanged_run_rewrites_an_identical_bundle(
    tmp_path: Path,
) -> None:
    """The weight argument in the module docstring, asserted.

    Regenerating after a worktree recycle must add nothing to the object store
    when nothing changed. Bundle mtimes, owners and the gzip header are all
    pinned to make this hold; if any of that regressed, every regeneration would
    silently cost a fresh 26 KiB blob forever.
    """
    run_dir = _fake_run_dir(tmp_path)
    root = tmp_path / "records"
    first = store_scored_artifacts(run_dir, records_root=root).read_bytes()
    second = store_scored_artifacts(run_dir, records_root=root).read_bytes()
    assert first == second


def test_the_volatile_binding_rides_outside_the_archive(tmp_path: Path) -> None:
    """Measured, not assumed: across two real runs of one configuration only
    ``run-binding.json`` differed. It is kept beside the bundle so the bundle
    itself stays byte-identical — and stays plain text so the generation pins
    are greppable without unpacking."""
    run_dir = _fake_run_dir(tmp_path)
    root = tmp_path / "records"
    bundle = store_scored_artifacts(run_dir, records_root=root)
    rid = bundle.name[: -len(BUNDLE_SUFFIX)]
    with tarfile.open(bundle, "r:gz") as tar:
        assert RUN_BINDING_FILENAME not in tar.getnames()
    beside = root / sidecar_name(rid, RUN_BINDING_FILENAME)
    assert beside.is_file()
    assert json.loads(beside.read_text())["clean_generation_id"].startswith("okg:")


# =========================================================================== #
# Recording is opt-in, so ordinary test runs never write into the repository   #
# =========================================================================== #
def test_recording_is_off_unless_asked() -> None:
    assert records_root_from_env({}) is None
    assert records_root_from_env({"OKG_SYNTH_RECORD": ""}) is None
    assert records_root_from_env({"OKG_SYNTH_RECORD": "0"}) is None
    assert records_root_from_env({"OKG_SYNTH_RECORD": "1"}) == DEFAULT_RECORDS_ROOT
    assert records_root_from_env({"OKG_SYNTH_RECORD": "/tmp/x"}) == Path("/tmp/x")


def test_an_unknown_record_names_what_is_known() -> None:
    with pytest.raises(DurableRecordError, match="known records"):
        resolve_bundle("no-such-record")


# =========================================================================== #
# req.row.record-identity: a repeat must not destroy the run it repeats       #
# =========================================================================== #
def _second_draw(run_dir: Path, dest: Path, *, vary_scored: bool = False) -> Path:
    """A SECOND DRAW of the configuration in ``run_dir``.

    Identical configuration, identical manifest — that is what a repeat IS, and
    what made the old identity scheme destroy one. Only the run's own outcome
    differs: fresh generation ids in the binding always, and when
    ``vary_scored`` is set, a decode that scored differently too.
    """
    dest.mkdir(parents=True, exist_ok=True)
    for item in run_dir.iterdir():
        (dest / item.name).write_bytes(item.read_bytes())
    binding = json.loads((dest / RUN_BINDING_FILENAME).read_text())
    for key in ("clean_generation_id", "corrupted_generation_id"):
        binding[key] = binding[key].replace("gen:2026", "gen:2027")
    (dest / RUN_BINDING_FILENAME).write_text(json.dumps(binding, sort_keys=True))
    if vary_scored:
        reads = json.loads((dest / ENGINE_READS_FILENAME).read_text())
        reads["_second_draw_marker"] = "a decode that came back different"
        (dest / ENGINE_READS_FILENAME).write_text(json.dumps(reads, sort_keys=True))
    return dest


@pytest.mark.parametrize("vary_scored", [False, True], ids=["binding", "scored"])
def test_a_second_draw_of_one_configuration_is_a_second_record(
    tmp_path: Path, vary_scored: bool
) -> None:
    """THE ACCEPTANCE. Measuring decode noise means keeping N draws of one
    configuration; the store must hold them without the caller naming them.

    Both parametrizations are real repeats: the ``binding`` case is the hardest,
    where even the archived bytes agree and only the fresh generation pins say
    two runs happened.
    """
    first_dir = _fake_run_dir(tmp_path)
    second_dir = _second_draw(
        first_dir, tmp_path / "draw2", vary_scored=vary_scored
    )
    root = tmp_path / "records"

    first = store_scored_artifacts(first_dir, records_root=root)
    first_bytes = first.read_bytes()
    second = store_scored_artifacts(second_dir, records_root=root)

    # A SECOND record: not an overwrite, and no error the caller had to work
    # around by inventing a name.
    assert second != first
    assert first.is_file() and second.is_file()
    assert first.read_bytes() == first_bytes, "the earlier draw was rewritten"
    assert len(read_index(root)["records"]) == 2
    assert sorted(p.name for p in root.glob(f"*{BUNDLE_SUFFIX}")) == sorted(
        [first.name, second.name]
    )

    # And both draws are independently re-scorable, so keeping N of them is
    # worth something.
    for bundle in (first, second):
        back = unpack_record(bundle, tmp_path / f"back-{bundle.name}")
        assert (back / MANIFEST_FILENAME).is_file()


def test_the_retired_identity_scheme_would_have_collided_on_that_repeat(
    tmp_path: Path,
) -> None:
    """The guard above is not vacuous: this is the bug, reconstructed.

    ``(preset, config_digest, manifest_digest)`` is EQUAL across two draws of one
    configuration — that is precisely why the second silently replaced the first.
    Content-addressing the scored bytes is what separates them.
    """
    first_dir = _fake_run_dir(tmp_path)
    second_dir = _second_draw(first_dir, tmp_path / "draw2", vary_scored=True)

    def retired_id(run_dir: Path) -> str:
        manifest = json.loads((run_dir / MANIFEST_FILENAME).read_text())
        return f"{manifest['config_digest'][:12]}-{manifest['manifest_digest'][:12]}"

    assert retired_id(first_dir) == retired_id(second_dir), (
        "the fixture is not a real repeat; the collision cannot be reconstructed"
    )
    assert record_id_for(first_dir) != record_id_for(second_dir)


def test_storing_over_a_taken_id_with_different_content_is_refused(
    tmp_path: Path,
) -> None:
    """A collision that would overwrite is REFUSED, loudly — silence is not an
    option.

    Under the content-addressed id this is unreachable without a SHA-256
    collision, so the guard is exercised the only way it can be reached: an
    explicit ``record_id`` that is already taken, which is exactly the hand-naming
    workaround the old scheme forced on callers.
    """
    first_dir = _fake_run_dir(tmp_path)
    second_dir = _second_draw(first_dir, tmp_path / "draw2", vary_scored=True)
    root = tmp_path / "records"

    bundle = store_scored_artifacts(first_dir, records_root=root, record_id="draw")
    before = bundle.read_bytes()
    with pytest.raises(DurableRecordError, match="would DESTROY"):
        store_scored_artifacts(second_dir, records_root=root, record_id="draw")
    assert bundle.read_bytes() == before, "refusal still let the write through"


def test_restoring_the_same_run_under_its_own_id_is_not_a_collision(
    tmp_path: Path,
) -> None:
    """The refusal must not fire on the ordinary case it would otherwise break:
    re-storing an unchanged run, which is how a worktree recycle is recovered."""
    run_dir = _fake_run_dir(tmp_path)
    root = tmp_path / "records"
    first = store_scored_artifacts(run_dir, records_root=root)
    second = store_scored_artifacts(run_dir, records_root=root)
    assert first == second
    assert len(read_index(root)["records"]) == 1


def test_removing_a_bundle_leaves_no_index_row(tmp_path: Path) -> None:
    """THE ACCEPTANCE. An index row citing a bundle that is gone is the
    "evidence item points at nothing" failure this change exists to end,
    reintroduced one level down."""
    run_dir = _fake_run_dir(tmp_path)
    second_dir = _second_draw(run_dir, tmp_path / "draw2", vary_scored=True)
    root = tmp_path / "records"
    doomed = store_scored_artifacts(run_dir, records_root=root)
    kept = store_scored_artifacts(second_dir, records_root=root)
    doomed_id = doomed.name[: -len(BUNDLE_SUFFIX)]
    kept_id = kept.name[: -len(BUNDLE_SUFFIX)]
    assert {r["record_id"] for r in read_index(root)["records"]} == {
        doomed_id,
        kept_id,
    }

    doomed.unlink()

    # No reader can be handed the orphan, even before anything is pruned.
    assert [r["record_id"] for r in read_index(root)["records"]] == [kept_id]
    # And the persisted file is reconciled, so the disagreement does not sit in
    # the tree either.
    assert prune_index(root) == [doomed_id]
    raw = json.loads((root / INDEX_FILENAME).read_text())
    assert [r["record_id"] for r in raw["records"]] == [kept_id]
    assert prune_index(root) == [], "pruning is not idempotent"


def test_the_next_store_also_prunes_an_orphan(tmp_path: Path) -> None:
    """Pruning does not depend on someone remembering to call it."""
    run_dir = _fake_run_dir(tmp_path)
    second_dir = _second_draw(run_dir, tmp_path / "draw2", vary_scored=True)
    root = tmp_path / "records"
    doomed = store_scored_artifacts(run_dir, records_root=root)
    doomed.unlink()
    store_scored_artifacts(second_dir, records_root=root)
    raw = json.loads((root / INDEX_FILENAME).read_text())
    assert doomed.name[: -len(BUNDLE_SUFFIX)] not in {
        r["record_id"] for r in raw["records"]
    }


# =========================================================================== #
# The committed records address their own content (the migration, asserted)   #
# =========================================================================== #
@pytest.mark.parametrize("record_id", _committed_records())
def test_the_committed_record_addresses_its_own_content(record_id: str) -> None:
    """Each committed record is filed under the id its own scored bytes produce.

    This is what makes the store checkable rather than merely populated: a
    record whose name and content had drifted apart — including one still
    carrying a hand-invented ``-r2`` suffix from the overwrite workaround —
    fails here.
    """
    assert record_id_of_stored(record_id) == record_id


def test_the_committed_repeats_are_kept_as_distinct_records() -> None:
    """The three draws that motivated this requirement are all still here.

    They share a configuration AND a byte-identical manifest, which is what the
    retired scheme collapsed. If a future edit reintroduced that collapse, this
    count drops.
    """
    by_config: dict[str, list[str]] = {}
    for row in read_index()["records"]:
        if row.get("kind") == "analysis":
            continue
        by_config.setdefault(row["headline"]["config_digest"], []).append(
            row["record_id"]
        )
    repeats = {k: v for k, v in by_config.items() if len(v) > 1}
    assert repeats, "no repeated configuration is recorded; the fix is untested here"
    for config_digest, ids in repeats.items():
        assert len(set(ids)) == len(ids), config_digest
        manifests = {stored_members(rid)[MANIFEST_FILENAME] for rid in ids}
        assert len(manifests) == 1, (
            f"{config_digest} draws differ in their manifests; they are not the "
            "within-seed repeat this requirement is about"
        )


def test_a_stored_record_digests_the_same_as_the_run_it_came_from(
    tmp_path: Path,
) -> None:
    """Store-time and read-back-time digests must agree, or a stored record
    could not be checked against the identity it is filed under."""
    run_dir = _fake_run_dir(tmp_path)
    root = tmp_path / "records"
    bundle = store_scored_artifacts(run_dir, records_root=root)
    rid = bundle.name[: -len(BUNDLE_SUFFIX)]
    assert content_digest_of(stored_members(rid, root)) == content_digest_of(
        scored_members(run_dir)
    )
    row = next(r for r in read_index(root)["records"] if r["record_id"] == rid)
    assert row["content_digest"] == content_digest_of(scored_members(run_dir))


# =========================================================================== #
# ANALYSIS RECORDS — the DERIVED artifacts a paper quotes                      #
#                                                                             #
# The rows and the spreads were the gap this task closed: the controlled-      #
# fixture row and the priced row landed only in pytest's tmp_path, and the     #
# cross-seed spread and the paired effect were never written to disk at all.   #
# Every one of them is a number the paper quotes, so every one of them was a   #
# citation a reader could not follow.                                          #
# =========================================================================== #
def _fake_row(marker: str = "a") -> dict:
    """A minimal derived artifact: enough shape to be stored and read back."""
    return {
        "schema": "okg.synth.controlled-fixture-row.v1",
        "row": "Controlled fixture",
        "config_digest": "d" * 64,
        "detection": {"exact": {"recall": marker}},
    }


def test_a_derived_artifact_stores_and_reads_back(tmp_path: Path) -> None:
    root = tmp_path / "records"
    row = _fake_row()
    path = store_analysis_artifact(
        row, label="controlled-fixture-row", rendered="a table\n", records_root=root
    )
    assert path.name.endswith(ANALYSIS_SUFFIX)
    assert read_analysis(path.name[: -len(ANALYSIS_SUFFIX)], root) == row
    # The rendering is committed beside it, so a reader greps a cell without
    # opening the JSON.
    rid = path.name[: -len(ANALYSIS_SUFFIX)]
    assert (root / f"{rid}{ANALYSIS_RENDER_SUFFIX}").read_text() == "a table\n"
    # It is indexed as its own kind, under its label.
    row_entry = next(r for r in read_index(root)["records"] if r["record_id"] == rid)
    assert row_entry["kind"] == "analysis"
    assert row_entry["label"] == "controlled-fixture-row"


def test_two_draws_of_a_derived_artifact_are_both_kept(tmp_path: Path) -> None:
    """Re-running a row produces a second record, never an overwrite.

    Same reasoning as the scored records: a row recomputed on a fresh publish
    differs (fresh generation pins), and it is a second draw, not a correction.
    """
    root = tmp_path / "records"
    first = store_analysis_artifact(
        _fake_row("a"), label="controlled-fixture-row", records_root=root
    )
    second = store_analysis_artifact(
        _fake_row("b"), label="controlled-fixture-row", records_root=root
    )
    assert first != second
    assert first.is_file() and second.is_file()
    assert len(analysis_records_by_label(root)["controlled-fixture-row"]) == 2


def test_restoring_an_identical_derived_artifact_rewrites_one_record(
    tmp_path: Path,
) -> None:
    root = tmp_path / "records"
    a = store_analysis_artifact(
        _fake_row(), label="controlled-fixture-row", rendered="t\n", records_root=root
    )
    b = store_analysis_artifact(
        _fake_row(), label="controlled-fixture-row", rendered="t\n", records_root=root
    )
    assert a == b
    assert len(analysis_records_by_label(root)["controlled-fixture-row"]) == 1


def test_a_store_that_would_replace_a_derived_artifact_is_refused(
    tmp_path: Path,
) -> None:
    """The scored records' rule, applied to the derived layer: an id names its
    content, so a write that would change it is destroying evidence."""
    root = tmp_path / "records"
    store_analysis_artifact(
        _fake_row("a"), label="row", records_root=root, record_id="fixed"
    )
    with pytest.raises(DurableRecordError, match="already holds different content"):
        store_analysis_artifact(
            _fake_row("b"), label="row", records_root=root, record_id="fixed"
        )


def test_a_rendering_change_alone_is_a_different_record(tmp_path: Path) -> None:
    """The rendered table is evidence too — it is what a reader greps — so two
    records whose JSON agrees and whose text does not are two records."""
    root = tmp_path / "records"
    a = store_analysis_artifact(
        _fake_row(), label="row", rendered="one\n", records_root=root
    )
    b = store_analysis_artifact(
        _fake_row(), label="row", rendered="two\n", records_root=root
    )
    assert a != b


def test_an_unlabelled_derived_artifact_is_refused(tmp_path: Path) -> None:
    with pytest.raises(DurableRecordError, match="must say what it is"):
        store_analysis_artifact(_fake_row(), label="", records_root=tmp_path)


def test_an_empty_derived_artifact_is_refused(tmp_path: Path) -> None:
    with pytest.raises(DurableRecordError, match="nothing to store"):
        store_analysis_artifact({}, label="row", records_root=tmp_path)


def test_removing_a_derived_artifact_leaves_no_orphan_index_row(
    tmp_path: Path,
) -> None:
    """The index and the artifacts on disk cannot disagree — the same guarantee
    the scored bundles have, which the analysis rows must not weaken."""
    root = tmp_path / "records"
    doomed = store_analysis_artifact(
        _fake_row("a"), label="row", records_root=root
    )
    store_analysis_artifact(_fake_row("b"), label="row", records_root=root)
    rid = doomed.name[: -len(ANALYSIS_SUFFIX)]
    doomed.unlink()
    assert rid not in {r["record_id"] for r in read_index(root)["records"]}
    assert prune_index(root) == [rid]


def test_storing_a_scored_record_does_not_prune_the_analysis_rows(
    tmp_path: Path,
) -> None:
    """The index is reconciled against BOTH kinds.

    Reconciling against the scored bundles alone would have deleted every
    analysis row on the next scored store — an index that drops live evidence,
    which is the same defect as one that keeps dead rows.
    """
    root = tmp_path / "records"
    analysis = store_analysis_artifact(
        _fake_row(), label="row", records_root=root
    )
    store_scored_artifacts(_fake_run_dir(tmp_path), records_root=root)
    rid = analysis.name[: -len(ANALYSIS_SUFFIX)]
    assert rid in {r["record_id"] for r in read_index(root)["records"]}


# ---- THE FAIL-CLOSED CHECK: a row that is not stored is a REFUSAL ---------- #
def test_a_declared_derived_artifact_with_no_record_is_refused(
    tmp_path: Path,
) -> None:
    """The non-vacuity witness for the whole task.

    A row computed inside a test and left in that test's temporary directory is
    indistinguishable, from outside, from a row that was never computed — and
    both read to a reader as a citation into nothing. This is the check that
    makes the difference visible, shown FIRING on an empty store.
    """
    root = tmp_path / "records"
    root.mkdir()
    assert missing_analysis_labels(root) == list(REQUIRED_ANALYSIS_LABELS)
    with pytest.raises(DurableRecordError) as excinfo:
        assert_analysis_recorded(root)
    message = str(excinfo.value)
    assert "have NO durable record" in message
    for label in REQUIRED_ANALYSIS_LABELS:
        assert label in message


def test_every_declared_derived_artifact_is_recorded_in_this_repository() -> None:
    """The acceptance, against the COMMITTED store rather than a fixture.

    If a future run computes a row and forgets to record it, or a record is
    removed, this fails and names the label. It is the counterpart of
    ``test_at_least_one_record_is_committed``: that one stops the scored
    witnesses from vanishing, this one stops the derived ones from being absent.
    """
    assert_analysis_recorded()
    by_label = analysis_records_by_label()
    for label in REQUIRED_ANALYSIS_LABELS:
        assert by_label[label], label


@pytest.mark.parametrize("label", list(REQUIRED_ANALYSIS_LABELS))
def test_the_committed_derived_artifact_reads_back(label: str) -> None:
    """Every committed derived artifact opens, and its id still addresses its
    content — so the citation resolves AND resolves to what it claims."""
    recorded = analysis_records_by_label().get(label) or []
    assert recorded, f"no record labelled {label!r} (see assert_analysis_recorded)"
    for record_id in recorded:
        artifact = read_analysis(record_id)
        assert artifact, record_id
        rendered_path = (
            DEFAULT_RECORDS_ROOT / f"{record_id}{ANALYSIS_RENDER_SUFFIX}"
        )
        rendered = (
            rendered_path.read_text(encoding="utf-8")
            if rendered_path.is_file()
            else None
        )
        assert analysis_record_id(label, artifact, rendered) == record_id, (
            f"{record_id} no longer addresses its own content"
        )


def test_the_committed_rendering_delta_is_not_quoted_as_a_point_figure() -> None:
    """The statistics acceptance, read off the stored artifact.

    The rendering delta was quoted as -3.19 points from ONE draw per arm. The
    stored summary must carry the paired design, the measured decode noise it is
    read against, and a verdict for EVERY choice of the repeated draw — and it
    must not claim separation it does not have.
    """
    ids = analysis_records_by_label()["rendering-delta-summary"]
    assert ids, "no rendering-delta summary is recorded"
    for record_id in ids:
        summary = read_analysis(record_id)
        effects = summary["paired_effects"]
        assert len(effects) >= 3, (
            f"{record_id} reports {len(effects)} paired effect(s); the repeated "
            "draw's choice must not decide the verdict unexamined"
        )
        for entry in effects:
            assert entry["n_pairs"] >= 3, entry
            assert entry["t_statistic"] is not None, entry
            assert entry["stdev_points"] > 0.0, entry
        noise = summary["within_seed_decode_noise"]
        assert noise["n"] >= 3 and noise["range_points"] > 0.0, noise
        # The verdict is a verdict, not an assumption: whichever way it went, the
        # summary states it for every choice and the two flags agree with the
        # per-effect verdicts.
        verdicts = {entry["separated_at_95"] for entry in effects}
        assert summary["separated_under_every_choice"] == (verdicts == {True})
        assert summary["separated_under_no_choice"] == (verdicts == {False})
        assert summary["cost_usd_by_arm"], summary
