"""Durable storage for the SCORED layer of a synth benchmark run.

Why this module exists
----------------------
The benchmark's whole offer to a reader is "every number came from a published
run you can re-score from recorded artifacts, with no model call and no live
database" — :func:`okg.bench.synth.runner.rescore_from_artifacts`,
:func:`~okg.bench.synth.runner.report_from_artifacts` and
:func:`okg.bench.synth.spread.aggregate_draws` exist to make that true. Every
run artifact this program produced was written under ``.scratch/``, which is
gitignored and worktree-local; two worktree recycles destroyed all of it. The
numbers survived in commit messages and PACT evidence summaries, so the findings
stand as claims — but the offer to re-derive them had become false.

What is stored, and what is not
-------------------------------
Only the SCORED, auditable layer: the sealed manifest, the pinned engine-side
reads, the scorecard, the run binding, and the small per-run reports. That is
exactly what a reader needs to check a number.

The rendered corpora, the verbatim model responses and the full-size
extracted-claim snapshots are NOT stored. They are large, and they are not what
makes a claim checkable — re-score never reads them.

Two of the stored files are load-bearing rather than decorative:
:data:`REQUIRED_FILENAMES` names them. ``rescore_from_artifacts`` reads the
manifest and the engine reads and nothing else, so a bundle missing either one
cannot be re-scored. Storing manifest *digests* alone would NOT be enough — the
digest identifies the manifest, it does not reconstruct it.

The mechanism, and its measured cost
------------------------------------
Measured on a real deterministic smoke run, not guessed. The scored layer is
282,403 bytes of sorted-key JSON, dominated by the manifest (169,445 B) and the
engine reads (100,100 B); the rendered corpora that are excluded come to another
37,392 B. Committing 276 KiB verbatim per run is not acceptable weight here —
the ``.gitignore`` preamble records two oversized blobs that had to be cut out
of history with git-filter-repo. Sorted-key JSON compresses about 10.6x, so the
layer is stored as ONE gzipped tar: **26,312 bytes** for the smoke preset.

The record id is content-addressed on ``(preset, config_digest,
content_digest)``, where the content digest covers the SCORED BYTES THEMSELVES —
see :func:`content_digest_of` and :func:`record_id_for`. Re-storing an unchanged
run therefore rewrites the SAME bundle rather than accruing a new file, while a
genuine second DRAW of the same configuration lands on a DIFFERENT id and is
kept alongside the first.

That last property is the whole reason the scheme is not
``(preset, config_digest, manifest_digest)``, which is what it used to be.
Measuring decode noise means running one configuration N times and keeping all N
draws; those draws share a byte-identical manifest, so all three components
matched and each draw SILENTLY OVERWROTE the one before it. Repeats are a normal
operation for this benchmark, not an edge case, so identity has to admit them
without the caller inventing names by hand.

Two real runs of one configuration were compared member by member: the manifest,
the engine reads, the scorecard and the sensitivity sweep came back
byte-identical, and only ``run-binding.json`` (681 B of fresh generation ids and
per-stage latencies) differed. Those volatile bytes are therefore kept OUTSIDE
the archive (see :data:`SIDECAR_FILENAMES`), which makes the bundle
byte-identical across regenerations. Regenerating after a worktree recycle then
costs nothing at all in the object store, instead of a new 26 KiB blob every
time. The content digest still reads the binding, because two draws whose
archives happen to agree are still two draws, and the fresh generation pins are
what say so.

Identity is the content of the evidence, so an id cannot address two different
records: a store that would change bytes already under an id is REFUSED rather
than performed (see :func:`store_scored_artifacts`). And the index is derived
from the bundles on disk rather than accumulated beside them, so a removed
bundle cannot leave a row pointing at nothing (see :func:`read_index`).

The second record kind: the artifacts a paper quotes
----------------------------------------------------
A scored record makes a RUN checkable. What a paper actually quotes cells out of
is DERIVED from one or more runs: the controlled-fixture row, the priced row, the
within-seed and cross-seed spreads, the paired-arm effect. Not one of those was
stored. The two rows landed in pytest's ``tmp_path`` and the spreads and the
effect were never written to disk at all — so the newest and most quotable
numbers in the program were exactly the ones a fresh checkout could not read.

They are stored as ANALYSIS records (:func:`store_analysis_artifact`): plain
sorted-key JSON with a rendered companion beside it, content-addressed on
``(label, content_digest)``, indexed with the scored bundles and covered by
``verify``. Plain rather than gzipped, and that is measured rather than assumed —
the derived layer is 205,677 bytes in the working tree, 38,792 gzipped, and
46,180 as git objects, because git deflates every blob anyway. Compressing by
hand would save about 7 KiB of repository weight and cost the citable layer its
legibility to ``grep``.

:data:`REQUIRED_ANALYSIS_LABELS` DECLARES which derived artifacts a published
claim rests on, and :func:`assert_analysis_recorded` refuses a store that is
missing one. That is the part that makes the mechanism hold: from outside, a row
computed and left in a temporary directory is indistinguishable from a row that
was never computed, and both read to a reader as a citation into nothing.

The trade-off accepted: a gzipped bundle is opaque to ``git diff`` and to
``grep``. It is mitigated twice — by the plain-text sidecar, which keeps the
generation pins readable in the tree, and by :data:`INDEX_FILENAME`, a sorted
JSON index committed alongside the bundles carrying each record's identity, its
pins and its headline numbers. A reader finds and cites a number without
unpacking anything, and unpacks only to re-derive it.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import tarfile
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from okg.bench.synth.controlled_fixture import CONTROLLED_FIXTURE_FILENAME
from okg.bench.synth.runner import (
    CORRECTIONS_FILENAME,
    ENGINE_READS_FILENAME,
    EXTRACTION_COST_FILENAME,
    EXTRACTION_FAILURES_FILENAME,
    FUNNEL_ROWS_FILENAME,
    MANIFEST_FILENAME,
    RUN_BINDING_FILENAME,
    SCORECARD_FILENAME,
    SENSITIVITY_FILENAME,
    report_from_artifacts,
    rescore_from_artifacts,
)
from okg.bench.synth.spread import SPREAD_FILENAME

RECORD_SCHEMA = "okg.synth.durable-record.v1"
INDEX_SCHEMA = "okg.synth.durable-index.v1"
INDEX_FILENAME = "index.json"
BUNDLE_SUFFIX = ".scored.tar.gz"

#: The second record kind: a DERIVED artifact, computed over one or more runs
#: rather than produced by one. The controlled-fixture row, the priced row, the
#: within-seed and cross-seed spreads and the paired-arm effect are all of this
#: kind — they are the artifacts a paper actually quotes, and until now not one
#: of them was stored where a fresh checkout could read it. The rows landed in
#: pytest's ``tmp_path``; the spreads and the effect were never written at all
#: outside the process that computed them.
ANALYSIS_SCHEMA = "okg.synth.durable-analysis.v1"
ANALYSIS_SUFFIX = ".analysis.json"
ANALYSIS_RENDER_SUFFIX = ".analysis.txt"

#: Without BOTH of these, ``rescore_from_artifacts`` cannot run. A bundle that
#: is missing one is refused at store time rather than discovered dead later —
#: the failure this module exists to prevent is precisely an artifact that looks
#: present and turns out not to answer.
REQUIRED_FILENAMES: tuple[str, ...] = (MANIFEST_FILENAME, ENGINE_READS_FILENAME)

#: Stored when the run produced them. Each is small and each backs a claim
#: somebody quotes: the scorecard is the byte-identity target for a re-score,
#: and the extraction cost is the dollar figure the cost cells report.
OPTIONAL_FILENAMES: tuple[str, ...] = (
    SCORECARD_FILENAME,
    FUNNEL_ROWS_FILENAME,
    SENSITIVITY_FILENAME,
    CORRECTIONS_FILENAME,
    EXTRACTION_COST_FILENAME,
    EXTRACTION_FAILURES_FILENAME,
    CONTROLLED_FIXTURE_FILENAME,
    SPREAD_FILENAME,
    "synth-capstone.json",
)

#: Kept OUTSIDE the gzip, as plain committed JSON beside it.
#:
#: Measured, not assumed: across two real smoke runs of one configuration, the
#: manifest, the engine reads, the scorecard and the sensitivity sweep are
#: byte-identical, and ``run-binding.json`` alone differs — it records the fresh
#: generation ids and the per-stage latency ledger. Folding those 681 volatile
#: bytes into the archive would have made the whole 26 KiB bundle a new git blob
#: on every regeneration, so regenerating after each worktree recycle would
#: quietly cost repository weight forever. Split out, the bundle is
#: byte-identical across regenerations and git records nothing, while the pins
#: stay greppable in the tree without unpacking anything.
SIDECAR_FILENAMES: tuple[str, ...] = (RUN_BINDING_FILENAME,)

#: Everything that goes into the gzip.
BUNDLED_FILENAMES: tuple[str, ...] = REQUIRED_FILENAMES + OPTIONAL_FILENAMES

#: Everything a stored record can carry, bundled or beside it.
SCORED_FILENAMES: tuple[str, ...] = BUNDLED_FILENAMES + SIDECAR_FILENAMES

#: Deliberately excluded, named so the exclusion is a decision and not an
#: oversight. Prefix match: the corpora directories, the rendered PDFs, the
#: verbatim model responses and the full claim snapshots.
EXCLUDED_PREFIXES: tuple[str, ...] = (
    "corpus_clean/",
    "corpus_corrupted/",
    "pdf/",
    "llm-responses-",
    "extracted-claims-",
)

#: Where records live when no root is given: ``bench/synth/records`` in the
#: repository, next to the rest of the benchmark material and inside git.
_REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_RECORDS_ROOT = _REPO_ROOT / "bench" / "synth" / "records"

#: Set to ``1`` to store into :data:`DEFAULT_RECORDS_ROOT`, or to a path to
#: store elsewhere. Unset means store nothing, so ordinary test runs never
#: write into the repository.
RECORD_ENV = "OKG_SYNTH_RECORD"


class DurableRecordError(RuntimeError):
    """A record could not be stored or read back."""


def records_root_from_env(env: Mapping[str, str] | None = None) -> Path | None:
    """Resolve :data:`RECORD_ENV` to a records root, or ``None`` if unset.

    ``1`` / ``true`` / ``yes`` select the in-repo default; anything else is
    taken as an explicit path, so an operator can record to a retained
    directory outside any worktree without touching the repository.
    """
    raw = (env if env is not None else os.environ).get(RECORD_ENV, "").strip()
    if not raw:
        return None
    if raw.lower() in {"1", "true", "yes", "on"}:
        return DEFAULT_RECORDS_ROOT
    if raw.lower() in {"0", "false", "no", "off"}:
        return None
    return Path(raw).expanduser()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _slug(text: str) -> str:
    keep = [c if (c.isalnum() or c in "-_") else "-" for c in text]
    return "".join(keep).strip("-").lower() or "unknown"


#: How many hex characters of each digest ride in a record id. Long enough that
#: an accidental collision is not a practical concern, short enough that the id
#: stays a filename a person can read out loud. A collision would in any case be
#: refused rather than silently performed — see :func:`store_scored_artifacts`.
_ID_DIGEST_CHARS = 12


def content_digest_of(members: Mapping[str, bytes]) -> str:
    """Digest the scored bytes of a record, whatever they came from.

    Takes ``{filename: bytes}`` rather than a directory so that the SAME digest
    is computable from a run directory (at store time) and from a stored bundle
    plus its sidecar (at migration or verification time). If those two ever
    disagreed, a stored record could not be checked against its own identity.

    The length of each member is folded in ahead of its bytes, so no two
    different member sets can concatenate to the same stream.
    """
    digest = hashlib.sha256()
    digest.update(RECORD_SCHEMA.encode("utf-8"))
    for name in sorted(members):
        payload = members[name]
        digest.update(f"\n{name}:{len(payload)}\n".encode("utf-8"))
        digest.update(payload)
    return digest.hexdigest()


def scored_members(run_dir: str | Path) -> dict[str, bytes]:
    """Every scored file a run directory actually carries, bundled or beside it."""
    run_dir = Path(run_dir)
    return {
        name: (run_dir / name).read_bytes()
        for name in SCORED_FILENAMES
        if (run_dir / name).is_file()
    }


def record_id_for(run_dir: Path) -> str:
    """Content-address the record on ``(preset, config_digest, content_digest)``.

    The third component is a digest of the SCORED BYTES — the manifest, the
    engine reads, the scorecard, the sensitivity sweep and the run binding — and
    not of the manifest alone. Both halves of the requirement follow from that
    one choice:

    * Re-storing an unchanged run recomputes the same digest, lands on the same
      bundle and rewrites it byte-for-byte, so regenerating after a worktree
      recycle stays free in repository weight.
    * A second DRAW of one configuration differs in its scored bytes — at
      minimum in the fresh generation ids the run binding records, and in
      practice in the engine reads and the scorecard too — so it lands on a
      DIFFERENT id and is kept ALONGSIDE the first rather than replacing it.
      Repeats need no hand-invented ``-r2`` name.

    Addressing on ``manifest_digest`` is what made a repeat destructive: N draws
    of one configuration share a byte-identical manifest, so every draw
    overwrote its predecessor without a word.
    """
    run_dir = Path(run_dir)
    manifest = _read_json(run_dir / MANIFEST_FILENAME)
    config = manifest.get("config") or {}
    preset = _slug(str(config.get("name") or config.get("preset") or "synth"))
    config_digest = str(manifest.get("config_digest") or "")
    manifest_digest = str(manifest.get("manifest_digest") or "")
    if not config_digest or not manifest_digest:
        raise DurableRecordError(
            f"{run_dir / MANIFEST_FILENAME} carries no config_digest/manifest_digest; "
            "the record cannot be content-addressed"
        )
    content = content_digest_of(scored_members(run_dir))
    return f"{preset}-{config_digest[:_ID_DIGEST_CHARS]}-{content[:_ID_DIGEST_CHARS]}"


def _headline(run_dir: Path) -> dict[str, Any]:
    """The few numbers a reader most often wants without unpacking anything.

    Read from the run's OWN scorecard and binding when present. Absent blocks
    are omitted, never defaulted — a missing number must read as missing, not
    as a zero somebody could quote.
    """
    out: dict[str, Any] = {}
    scorecard_path = run_dir / SCORECARD_FILENAME
    if scorecard_path.exists():
        core = _read_json(scorecard_path)
        for key in ("config_digest", "manifest_digest", "qualified"):
            if key in core:
                out[key] = core[key]
        funnel = core.get("funnel") or {}
        recall = funnel.get("detection_recall") or {}
        if recall:
            out["detection_recall"] = recall
        repair = funnel.get("repair_rate") or {}
        if repair:
            out["repair_rate"] = repair
        if "states" in funnel:
            out["funnel_states"] = funnel["states"]
    binding_path = run_dir / RUN_BINDING_FILENAME
    if binding_path.exists():
        binding = _read_json(binding_path)
        for key in (
            "deployment",
            "extractor",
            "clean_generation_id",
            "corrupted_generation_id",
            "surviving_conflict_realized",
            "extraction_cost",
        ):
            if key in binding:
                out[key] = binding[key]
    return out


def store_scored_artifacts(
    run_dir: str | Path,
    *,
    records_root: str | Path | None = None,
    record_id: str | None = None,
    note: str | None = None,
) -> Path:
    """Bundle a completed run's scored layer and write it into the records root.

    Returns the bundle path. Refuses a run missing either
    :data:`REQUIRED_FILENAMES` member, because such a bundle would resolve as a
    path and still fail to re-score — the exact failure mode this module exists
    to prevent.

    Storing a second DRAW of an identical configuration is an ordinary call: it
    lands on its own id and leaves the first draw where it was. Storing an
    UNCHANGED run is equally ordinary and rewrites its own bundle byte for byte.
    The one thing that cannot happen is a store that replaces evidence — an id
    already holding different bytes is refused loudly, not overwritten.
    """
    run_dir = Path(run_dir)
    for name in REQUIRED_FILENAMES:
        if not (run_dir / name).is_file():
            raise DurableRecordError(
                f"{run_dir} has no {name}; a bundle without it cannot be re-scored"
            )
    root = Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
    root.mkdir(parents=True, exist_ok=True)
    members = scored_members(run_dir)
    content = content_digest_of(members)
    rid = record_id or record_id_for(run_dir)
    bundle = root / f"{rid}{BUNDLE_SUFFIX}"

    # A record id names its content. If something is already stored under this
    # id, it must BE this content; anything else means the write would destroy a
    # record, which is exactly the failure this module exists to end. Under the
    # content-addressed id this is unreachable short of a SHA-256 collision --
    # it is reachable, and therefore worth refusing, when a caller passes an
    # explicit ``record_id`` that is already taken.
    if bundle.is_file():
        stored = content_digest_of(stored_members(rid, root))
        if stored != content:
            raise DurableRecordError(
                f"record {rid!r} already holds different scored content "
                f"(stored {stored[:_ID_DIGEST_CHARS]}, incoming "
                f"{content[:_ID_DIGEST_CHARS]}); storing would DESTROY the "
                "existing draw. Omit record_id and let the content address it, "
                "so both draws are kept."
            )

    present = [n for n in BUNDLED_FILENAMES if (run_dir / n).is_file()]
    buf = io.BytesIO()
    # Every timestamp and owner field is zeroed and members are added in a fixed
    # order, and the gzip header's own mtime is pinned too, so re-storing an
    # unchanged run produces a BYTE-IDENTICAL bundle and git records no change.
    # Without that, regenerating after each worktree recycle would grow the
    # repository by a fresh blob every time even when nothing had changed.
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name in present:
            info = tar.gettarinfo(str(run_dir / name), arcname=name)
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            with (run_dir / name).open("rb") as handle:
                tar.addfile(info, handle)
    payload = gzip.compress(buf.getvalue(), 9, mtime=0)
    bundle.write_bytes(payload)

    sidecars: list[str] = []
    for name in SIDECAR_FILENAMES:
        source = run_dir / name
        if source.is_file():
            target = root / sidecar_name(rid, name)
            target.write_bytes(source.read_bytes())
            sidecars.append(target.name)

    _update_index(
        root,
        {
            "schema": RECORD_SCHEMA,
            "record_id": rid,
            "bundle": bundle.name,
            "bundle_bytes": len(payload),
            "content_digest": content,
            "files": sorted(present),
            "sidecars": sorted(sidecars),
            "note": note or "",
            "headline": _headline(run_dir),
        },
    )
    return bundle


def sidecar_name(record_id: str, filename: str) -> str:
    """Name of a record's plain-text companion file, e.g.
    ``smoke-abc-def.run-binding.json``."""
    return f"{record_id}.{filename}"


def _record_id_of_bundle(bundle: Path) -> str:
    """The record id a bundle path carries, so its sidecars can be found."""
    if bundle.name.endswith(BUNDLE_SUFFIX):
        return bundle.name[: -len(BUNDLE_SUFFIX)]
    return bundle.stem


def stored_members(
    record_id: str, records_root: str | Path | None = None
) -> dict[str, bytes]:
    """Read a stored record's scored bytes back, without unpacking to disk.

    The counterpart of :func:`scored_members`: feeding either into
    :func:`content_digest_of` yields the same digest, so a stored record can be
    checked against the identity it claims.
    """
    bundle = resolve_bundle(record_id, records_root)
    rid = _record_id_of_bundle(bundle)
    out: dict[str, bytes] = {}
    with tarfile.open(bundle, mode="r:gz") as tar:
        for member in _safe_members(tar):
            extracted = tar.extractfile(member)
            if extracted is None:  # pragma: no cover - guarded by _safe_members
                raise DurableRecordError(f"bundle member {member.name!r} is empty")
            out[member.name] = extracted.read()
    for name in SIDECAR_FILENAMES:
        beside = bundle.parent / sidecar_name(rid, name)
        if beside.is_file():
            out[name] = beside.read_bytes()
    return out


def _live_record_ids(root: Path) -> set[str]:
    """The SCORED record ids that actually have a bundle on disk, right now."""
    if not root.is_dir():
        return set()
    return {
        path.name[: -len(BUNDLE_SUFFIX)] for path in root.glob(f"*{BUNDLE_SUFFIX}")
    }


def _live_analysis_ids(root: Path) -> set[str]:
    """The ANALYSIS record ids that actually have an artifact on disk, right now."""
    if not root.is_dir():
        return set()
    return {
        path.name[: -len(ANALYSIS_SUFFIX)] for path in root.glob(f"*{ANALYSIS_SUFFIX}")
    }


def _all_live_ids(root: Path) -> set[str]:
    """Every id of either kind that is backed by a file on disk.

    Index pruning reconciles against THIS, not against the scored bundles alone:
    reconciling against one kind would have silently deleted every row of the
    other on the next store — an index that drops live evidence is the same
    failure as an index that keeps dead rows, pointed the other way.
    """
    return _live_record_ids(root) | _live_analysis_ids(root)


def _write_index(root: Path, records: Mapping[str, Any]) -> Path:
    path = root / INDEX_FILENAME
    path.write_text(
        json.dumps(
            {
                "schema": INDEX_SCHEMA,
                "records": [records[k] for k in sorted(records)],
            },
            sort_keys=True,
            indent=1,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _indexed_rows(root: Path) -> dict[str, Any]:
    """Rows as the index file literally holds them — orphans included."""
    path = root / INDEX_FILENAME
    if not path.exists():
        return {}
    return {row["record_id"]: row for row in _read_json(path).get("records", [])}


def _update_index(root: Path, entry: Mapping[str, Any]) -> Path:
    """Rewrite the plain-text index, sorted by record id, PRUNED to disk.

    The index is the answer to "a gzip bundle is not greppable": identity,
    generation pins and headline numbers stay readable in the tree, and a
    reader unpacks only to re-derive.

    Pruning is not housekeeping. An index row whose bundle is gone is an
    evidence item citing a path that does not resolve — the failure this whole
    change exists to end — so rows are rebuilt against the bundles actually on
    disk rather than accumulated beside them.
    """
    records = _indexed_rows(root)
    records[entry["record_id"]] = dict(entry)
    live = _all_live_ids(root)
    return _write_index(root, {k: v for k, v in records.items() if k in live})


def prune_index(records_root: str | Path | None = None) -> list[str]:
    """Drop index rows whose bundle is gone, and return the ids dropped.

    Call it after removing a bundle by hand. Nothing else has to be called:
    :func:`read_index` never reports an orphan either, so a reader cannot be
    handed a row pointing at nothing even between a removal and a prune.
    """
    root = Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
    records = _indexed_rows(root)
    live = _all_live_ids(root)
    orphans = sorted(k for k in records if k not in live)
    if orphans:
        _write_index(root, {k: v for k, v in records.items() if k in live})
    return orphans


def read_index(records_root: str | Path | None = None) -> dict[str, Any]:
    """The index, reconciled against the bundles on disk.

    Filtered in memory rather than rewritten, so merely READING the records
    never writes into the repository — recording stays opt-in. The persisted
    file is pruned by :func:`prune_index` and by any store.
    """
    root = Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
    if not (root / INDEX_FILENAME).exists():
        return {"schema": INDEX_SCHEMA, "records": []}
    records = _indexed_rows(root)
    live = _all_live_ids(root)
    return {
        "schema": INDEX_SCHEMA,
        "records": [records[k] for k in sorted(records) if k in live],
    }


# --------------------------------------------------------------------------- #
# ANALYSIS RECORDS — the derived artifacts a paper quotes                       #
# --------------------------------------------------------------------------- #
#: Every derived artifact a published claim rests on, by label.
#:
#: This is a DECLARATION, not a description. :func:`missing_analysis_labels`
#: measures the store against it and ``python -m okg.bench.synth.durable verify``
#: fails when a label has no record, so a row or a spread that is computed and
#: then dropped on the floor is caught by the store rather than discovered by a
#: reader following a citation into nothing. Adding a claim to the paper means
#: adding its label here.
REQUIRED_ANALYSIS_LABELS: tuple[str, ...] = (
    "controlled-fixture-row",
    "priced-history-row",
    "prose-vs-structured-paired-effect",
    "prose-cross-seed-spread",
    "structured-cross-seed-spread",
    "prose-decode-noise-spread",
    "rendering-delta-summary",
)

#: Keys that, when a derived artifact carries one, identify WHICH configuration
#: it was computed over. Recorded in the index row so a reader can tie an
#: analysis record to the scored records it came from without opening it. Absent
#: keys are omitted, never defaulted.
_ANALYSIS_FINGERPRINT_KEYS: tuple[str, ...] = (
    "config_digest",
    "seed_invariant_digest",
    "metric",
    "n",
    "n_pairs",
    "seed",
    "seeds",
    "separated_at_95",
)


def analysis_digest_of(artifact_bytes: bytes, rendered: str | None) -> str:
    """Digest an analysis record's content — the JSON bytes and its rendering.

    The rendering is folded in because it is committed beside the JSON and is
    what a reader greps; two records whose JSON agreed but whose text did not
    would be two different pieces of evidence under one id.
    """
    digest = hashlib.sha256()
    digest.update(ANALYSIS_SCHEMA.encode("utf-8"))
    digest.update(f"\njson:{len(artifact_bytes)}\n".encode("utf-8"))
    digest.update(artifact_bytes)
    payload = (rendered or "").encode("utf-8")
    digest.update(f"\nrendered:{len(payload)}\n".encode("utf-8"))
    digest.update(payload)
    return digest.hexdigest()


def _analysis_bytes(artifact: Mapping[str, Any]) -> bytes:
    """Sorted-key, indented JSON — the same shape every other artifact is written in."""
    return (json.dumps(artifact, sort_keys=True, indent=1) + "\n").encode("utf-8")


def analysis_record_id(
    label: str, artifact: Mapping[str, Any], rendered: str | None = None
) -> str:
    """Content-address a derived artifact on ``(label, content_digest)``.

    Two components rather than three: unlike a run, a derived artifact has no
    single config digest to key on — a paired effect spans two configurations by
    construction — so the label carries the semantics and the content digest
    carries identity. A recomputation that produces the same bytes rewrites the
    same record; a genuinely different computation under the same label lands
    beside it. Nothing is ever replaced.
    """
    slug = _slug(label)
    if not label or slug == "unknown":
        raise DurableRecordError(
            f"analysis label {label!r} is not usable as a record name; a derived "
            "artifact must say what it is"
        )
    content = analysis_digest_of(_analysis_bytes(artifact), rendered)
    return f"{slug}-{content[:_ID_DIGEST_CHARS]}"


def store_analysis_artifact(
    artifact: Mapping[str, Any],
    *,
    label: str,
    rendered: str | None = None,
    records_root: str | Path | None = None,
    record_id: str | None = None,
    note: str | None = None,
) -> Path:
    """Store a derived artifact (a row, a spread, a paired effect) durably.

    Returns the path to the JSON. Writes plain sorted-key JSON plus, when a
    rendering is given, the rendered table beside it — deliberately NOT gzipped
    like the scored bundles. That choice is measured rather than assumed: the
    scored layer compresses 276 KiB to 26 KiB and is only ever read by a
    re-score, whereas a derived artifact is the thing a reader cites a cell out
    of, and the whole point of storing it is that the citation resolves to
    something legible. The measured cost is recorded in the records README.

    Refuses to write over an id holding different bytes, on the same grounds as
    :func:`store_scored_artifacts`: an id names its content, so a store that
    would change it is destroying evidence.
    """
    if not isinstance(artifact, Mapping) or not artifact:
        raise DurableRecordError(
            f"analysis artifact for {label!r} is empty; there is nothing to store"
        )
    root = Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
    root.mkdir(parents=True, exist_ok=True)
    payload = _analysis_bytes(artifact)
    content = analysis_digest_of(payload, rendered)
    rid = record_id or analysis_record_id(label, artifact, rendered)
    target = root / f"{rid}{ANALYSIS_SUFFIX}"
    render_path = root / f"{rid}{ANALYSIS_RENDER_SUFFIX}"

    if target.is_file():
        stored = analysis_digest_of(
            target.read_bytes(),
            render_path.read_text(encoding="utf-8") if render_path.is_file() else None,
        )
        if stored != content:
            raise DurableRecordError(
                f"analysis record {rid!r} already holds different content "
                f"(stored {stored[:_ID_DIGEST_CHARS]}, incoming "
                f"{content[:_ID_DIGEST_CHARS]}); storing would DESTROY the "
                "existing artifact. Omit record_id and let the content address "
                "it, so both are kept."
            )

    target.write_bytes(payload)
    if rendered is not None:
        render_path.write_text(rendered, encoding="utf-8")

    headline = {
        key: artifact[key] for key in _ANALYSIS_FINGERPRINT_KEYS if key in artifact
    }
    if "schema" in artifact:
        headline["artifact_schema"] = artifact["schema"]
    _update_index(
        root,
        {
            "schema": ANALYSIS_SCHEMA,
            "kind": "analysis",
            "label": _slug(label),
            "record_id": rid,
            "bundle": target.name,
            "bundle_bytes": len(payload),
            "content_digest": content,
            "files": sorted(
                [target.name] + ([render_path.name] if rendered is not None else [])
            ),
            "sidecars": [],
            "note": note or "",
            "headline": headline,
        },
    )
    return target


def record_analysis_if_enabled(
    artifact: Mapping[str, Any],
    *,
    label: str,
    rendered: str | None = None,
    note: str | None = None,
    env: Mapping[str, str] | None = None,
) -> Path | None:
    """Store a derived artifact when :data:`RECORD_ENV` names a records root.

    Returns the stored path, or ``None`` when recording is off. The one hook a
    row-producing test needs: a row computed in a test's temporary directory is
    indistinguishable, from outside, from a row that was never computed, and
    :func:`assert_analysis_recorded` is what turns that into a failure.
    """
    root = records_root_from_env(env)
    if root is None:
        return None
    return store_analysis_artifact(
        artifact, label=label, rendered=rendered, records_root=root, note=note
    )


def read_analysis(
    record_id: str, records_root: str | Path | None = None
) -> dict[str, Any]:
    """Read a stored derived artifact back, by id or by path."""
    candidate = Path(record_id)
    if candidate.is_file():
        return _read_json(candidate)
    root = Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
    for name in (record_id, f"{record_id}{ANALYSIS_SUFFIX}"):
        path = root / name
        if path.is_file():
            return _read_json(path)
    raise DurableRecordError(
        f"no analysis record {record_id!r} under {root}; known analysis records: "
        f"{sorted(_live_analysis_ids(root))}"
    )


def analysis_records_by_label(
    records_root: str | Path | None = None,
) -> dict[str, list[str]]:
    """``{label: [record_id, ...]}`` over the analysis records actually on disk.

    Read from the INDEX rows rather than from the filenames, and reconciled
    against disk, so a label is reported present only when both the row and the
    artifact are there.
    """
    root = Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
    live = _live_analysis_ids(root)
    out: dict[str, list[str]] = {}
    for row in _indexed_rows(root).values():
        rid = row.get("record_id")
        label = row.get("label")
        if not label or rid not in live:
            continue
        out.setdefault(str(label), []).append(str(rid))
    return {label: sorted(ids) for label, ids in sorted(out.items())}


def missing_analysis_labels(records_root: str | Path | None = None) -> list[str]:
    """Declared labels (:data:`REQUIRED_ANALYSIS_LABELS`) with no stored record."""
    present = analysis_records_by_label(records_root)
    return [label for label in REQUIRED_ANALYSIS_LABELS if not present.get(label)]


def assert_analysis_recorded(records_root: str | Path | None = None) -> None:
    """Refuse a store that is missing a declared derived artifact.

    Fail-closed on purpose. A row computed inside a test and left in the test's
    temporary directory looks, from the outside, exactly like a row that was
    never computed — and both read to a reader as a citation into nothing. This
    turns "somebody forgot to wire it" from an invisible state into a failure
    that names the label.
    """
    missing = missing_analysis_labels(records_root)
    if missing:
        root = (
            Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
        )
        raise DurableRecordError(
            f"{len(missing)} declared analysis artifact(s) have NO durable record "
            f"under {root}: {missing}. A claim resting on one of these cites "
            "nothing a reader can open — store it with store_analysis_artifact "
            "rather than leaving it in a temporary directory."
        )


def record_id_of_stored(
    record_id: str, records_root: str | Path | None = None
) -> str:
    """The id a STORED record would get under the current identity scheme.

    Equal to :func:`record_id_for` on the run directory the record came from,
    because both digest the same scored bytes. When it differs from the id the
    record is filed under, the record predates the scheme and
    :func:`migrate_records_root` renames it.
    """
    members = stored_members(record_id, records_root)
    manifest = json.loads(members[MANIFEST_FILENAME].decode("utf-8"))
    config = manifest.get("config") or {}
    preset = _slug(str(config.get("name") or config.get("preset") or "synth"))
    config_digest = str(manifest.get("config_digest") or "")
    if not config_digest:
        raise DurableRecordError(
            f"record {record_id!r} carries no config_digest; it cannot be "
            "content-addressed"
        )
    content = content_digest_of(members)
    return f"{preset}-{config_digest[:_ID_DIGEST_CHARS]}-{content[:_ID_DIGEST_CHARS]}"


def migrate_records_root(
    records_root: str | Path | None = None, *, dry_run: bool = False
) -> dict[str, str]:
    """Rename records filed under the old identity scheme, and return the map.

    The records that motivated this change were stored before it, and two of
    them carry hand-invented ``-r2`` / ``-r3`` suffixes — the workaround for the
    overwrite this scheme removes. They are paid runs, so migration RENAMES and
    never rewrites: the bundle bytes and the sidecar bytes are moved untouched,
    and only the filename and the index row change. Callers verify that at the
    byte level; the digest in the new id is computed from those same bytes, so a
    rename that altered content could not produce a matching id.

    Returns ``{old_id: new_id}`` for the records that moved.
    """
    root = Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
    rows = _indexed_rows(root)
    moved: dict[str, str] = {}
    for old_id in sorted(_live_record_ids(root)):
        members = stored_members(old_id, root)
        new_id = record_id_of_stored(old_id, root)
        row = dict(rows.get(old_id) or {})
        row["record_id"] = new_id
        row["content_digest"] = content_digest_of(members)
        if new_id != old_id:
            target = root / f"{new_id}{BUNDLE_SUFFIX}"
            if target.exists():
                raise DurableRecordError(
                    f"cannot migrate {old_id!r} to {new_id!r}: that record "
                    "already exists and holds its own bytes"
                )
            moved[old_id] = new_id
            if not dry_run:
                (root / f"{old_id}{BUNDLE_SUFFIX}").rename(target)
                for name in SIDECAR_FILENAMES:
                    beside = root / sidecar_name(old_id, name)
                    if beside.is_file():
                        beside.rename(root / sidecar_name(new_id, name))
            row["bundle"] = target.name
            row["sidecars"] = sorted(
                sidecar_name(new_id, name)
                for name in SIDECAR_FILENAMES
                if name in members
            )
        if not dry_run:
            rows.pop(old_id, None)
            rows[new_id] = row
    if not dry_run:
        # Reconciled against BOTH kinds: filtering to the scored bundles alone
        # would have deleted every analysis row on the next migration.
        live = _all_live_ids(root)
        _write_index(root, {k: v for k, v in rows.items() if k in live})
    return moved


def resolve_bundle(
    record_id: str, records_root: str | Path | None = None
) -> Path:
    """Accept a record id, a bundle filename, or a path, and return the path."""
    candidate = Path(record_id)
    if candidate.is_file():
        return candidate
    root = Path(records_root) if records_root is not None else DEFAULT_RECORDS_ROOT
    for name in (record_id, f"{record_id}{BUNDLE_SUFFIX}"):
        path = root / name
        if path.is_file():
            return path
    raise DurableRecordError(
        f"no record {record_id!r} under {root}; "
        f"known records: {[r['record_id'] for r in read_index(root)['records']]}"
    )


def _safe_members(tar: tarfile.TarFile) -> Iterator[tarfile.TarInfo]:
    """Yield only flat, regular members drawn from the known scored set.

    A bundle is data read from disk, so it is not trusted to name its own
    extraction paths: anything with a directory component, a link, or a name
    outside :data:`SCORED_FILENAMES` is refused rather than written.
    """
    for member in tar.getmembers():
        if not member.isfile():
            raise DurableRecordError(f"bundle member {member.name!r} is not a file")
        if member.name not in BUNDLED_FILENAMES:
            raise DurableRecordError(
                f"bundle member {member.name!r} is not a known scored artifact"
            )
        yield member


def unpack_record(bundle: str | Path, dest: str | Path) -> Path:
    """Reassemble a record into ``dest`` and return it, ready for re-score.

    Unpacks the archive AND copies back any plain-text sidecar sitting beside
    it, so the reconstructed directory looks exactly like the run directory the
    scored layer came from and every reader takes the same path.
    """
    bundle = Path(bundle)
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(bundle, mode="r:gz") as tar:
        for member in _safe_members(tar):
            extracted = tar.extractfile(member)
            if extracted is None:  # pragma: no cover - guarded by _safe_members
                raise DurableRecordError(f"bundle member {member.name!r} is empty")
            (dest / member.name).write_bytes(extracted.read())
    rid = _record_id_of_bundle(bundle)
    for name in SIDECAR_FILENAMES:
        beside = bundle.parent / sidecar_name(rid, name)
        if beside.is_file():
            (dest / name).write_bytes(beside.read_bytes())
    for name in REQUIRED_FILENAMES:
        if not (dest / name).is_file():
            raise DurableRecordError(
                f"{bundle} carries no {name}; it cannot be re-scored"
            )
    return dest


@contextmanager
def open_record(
    record_id: str, records_root: str | Path | None = None
) -> Iterator[Path]:
    """Yield a directory holding a record's scored artifacts.

    The directory is temporary and is what ``rescore_from_artifacts`` and
    ``report_from_artifacts`` take, so a reader in ANY checkout re-derives the
    numbers without a model call and without a live database.
    """
    bundle = resolve_bundle(record_id, records_root)
    with tempfile.TemporaryDirectory(prefix="synth-record-") as tmp:
        yield unpack_record(bundle, Path(tmp))


def rescore_record(
    record_id: str,
    records_root: str | Path | None = None,
    *,
    error_cost_scale: float = 1.0,
) -> dict[str, Any]:
    """Rebuild the full scorecard from a stored record alone."""
    with open_record(record_id, records_root) as run_dir:
        return rescore_from_artifacts(run_dir, error_cost_scale=error_cost_scale)


def report_record(
    record_id: str, records_root: str | Path | None = None
) -> str:
    """Render the operator-facing survival / FP / funnel report from a record."""
    with open_record(record_id, records_root) as run_dir:
        return report_from_artifacts(run_dir)


def _main(argv: list[str]) -> int:
    """``python -m okg.bench.synth.durable [list|report|rescore] [record]``.

    The read-back witness: run it in a different checkout against a record this
    worktree wrote, and the numbers come back.
    """
    import argparse

    parser = argparse.ArgumentParser(prog="okg.bench.synth.durable")
    parser.add_argument(
        "action",
        choices=("list", "report", "rescore", "verify", "migrate"),
        nargs="?",
        default="list",
    )
    parser.add_argument("record", nargs="?")
    parser.add_argument("--records-root", default=None)
    args = parser.parse_args(argv)

    root = Path(args.records_root) if args.records_root else DEFAULT_RECORDS_ROOT
    if args.action == "list":
        index = read_index(root)
        if not index["records"]:
            print(f"no records under {root}")
            return 1
        print(f"{len(index['records'])} record(s) under {root}")
        for row in index["records"]:
            head = row.get("headline", {})
            if row.get("kind") == "analysis":
                print(
                    f"  {row['record_id']}  {row['bundle_bytes']:,}B  "
                    f"ANALYSIS label={row.get('label', '?')}  "
                    f"schema={head.get('artifact_schema', '?')}"
                )
                continue
            print(
                f"  {row['record_id']}  {row['bundle_bytes']:,}B  "
                f"extractor={head.get('extractor', '?')}  "
                f"qualified={head.get('qualified', '?')}  "
                f"files={len(row.get('files', []))}"
            )
        return 0
    if args.action == "verify":
        # Every record must still address its own content, no index row may point
        # at a file that is gone, and every DECLARED derived artifact must have a
        # record — a row or a spread that was computed and dropped is the failure
        # this store exists to end, and it is invisible unless something asks.
        orphans = [
            row["record_id"]
            for row in _indexed_rows(root).values()
            if row["record_id"] not in _all_live_ids(root)
        ]
        mismatched = {
            rid: record_id_of_stored(rid, root)
            for rid in sorted(_live_record_ids(root))
            if record_id_of_stored(rid, root) != rid
        }
        by_label = analysis_records_by_label(root)
        for label, ids in by_label.items():
            for rid in ids:
                artifact = read_analysis(rid, root)
                render_path = root / f"{rid}{ANALYSIS_RENDER_SUFFIX}"
                rendered = (
                    render_path.read_text(encoding="utf-8")
                    if render_path.is_file()
                    else None
                )
                want = analysis_record_id(label, artifact, rendered)
                if want != rid:
                    mismatched[rid] = want
        missing = missing_analysis_labels(root)
        for rid in sorted(orphans):
            print(f"ORPHAN index row with no artifact: {rid}")
        for rid, want in sorted(mismatched.items()):
            print(f"STALE id {rid} -> should be {want} (run `migrate`)")
        for label in missing:
            print(f"MISSING declared analysis artifact: no record labelled {label!r}")
        if orphans or mismatched or missing:
            return 1
        print(
            f"{len(_live_record_ids(root))} scored + {len(_live_analysis_ids(root))} "
            f"analysis record(s) under {root}: ids match content, "
            f"all {len(REQUIRED_ANALYSIS_LABELS)} declared analysis labels present"
        )
        return 0
    if args.action == "migrate":
        moved = migrate_records_root(root)
        if not moved:
            print(f"nothing to migrate under {root}")
            return 0
        for old, new in sorted(moved.items()):
            print(f"{old} -> {new}")
        return 0
    if not args.record:
        parser.error(f"{args.action} needs a record id")
    if args.action == "report":
        print(report_record(args.record, root), end="")
    else:
        print(json.dumps(rescore_record(args.record, root), sort_keys=True, indent=1))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    import sys

    raise SystemExit(_main(sys.argv[1:]))
