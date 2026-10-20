# Synth benchmark scored records

Each `*.scored.tar.gz` here is the **scored layer** of one real benchmark run:
the sealed manifest, the pinned engine-side reads, the scorecard, and the
cost-sensitivity sweep. That is everything needed to re-derive the run's
numbers — with no model call and no live database.

The benchmark's claim is that every number came from a published run you can
re-score yourself. These files are what makes that offer true. They used to live
under `.scratch/`, which is gitignored and worktree-local; two worktree recycles
destroyed every artifact this program had produced, and several PACT evidence
items were left citing paths that no longer resolved.

## Check a number without unpacking anything

`index.json` carries, per record, the config and manifest digests, the clean and
corrupted generation ids, and the headline detection/repair figures. Each
record's `*.run-binding.json` sits beside its bundle as plain text and holds the
generation pins and the per-stage latency ledger.

## Re-derive a number

```sh
uv run python -m okg.bench.synth.durable list
uv run python -m okg.bench.synth.durable report  <record-id>   # survival + FP + funnel
uv run python -m okg.bench.synth.durable rescore <record-id>   # the full scorecard
```

`report` and `rescore` unpack to a temporary directory and call
`report_from_artifacts` / `rescore_from_artifacts`. Neither touches a database
or a model.

## Regenerate the deterministic records

The deterministic lane costs nothing to re-run, so a worktree recycle should be
a re-run rather than a loss:

```sh
psql "$OKG_ADMIN_DSN" \
  -c "DROP DATABASE IF EXISTS okg_test_synthdur WITH (FORCE)" \
  -c "CREATE DATABASE okg_test_synthdur"

uv run python scripts/regen_synth_scored_records.py \
  --dsn postgres://postgres:okg@127.0.0.1:5433/okg_test_synthdur
```

Records are content-addressed on `(preset, config_digest, content_digest)` —
where the content digest covers the scored bytes themselves — and the volatile
run binding is kept outside the archive, so re-running an unchanged
configuration rewrites a **byte-identical** bundle and costs no repository
weight.

## Repeats of one configuration

Measuring decode noise means running one configuration several times and keeping
every draw. Those draws share a byte-identical manifest, so an id built from the
manifest digest collapsed them and each draw silently replaced the one before
it. Because the id is built from the scored bytes instead, a repeat lands on its
own id and is kept alongside the earlier draw — no hand-invented `-r2` name, and
no call that has to be worked around:

```sh
uv run python -m okg.bench.synth.durable verify   # ids still match content, no orphan rows
uv run python -m okg.bench.synth.durable migrate  # rename records filed under the retired scheme
```

Nothing can quietly replace a record: an id already holding different bytes is
refused loudly, and `index.json` is reconciled against the bundles on disk, so a
removed bundle never leaves a row pointing at nothing.

The language-model arms are not regenerated here. They cost real money and
provider nondeterminism means a re-run would not reproduce a recorded draw
anyway; evidence resting on them is annotated rather than restored.

## Recording a new run

Recording is opt-in, so ordinary test runs never write into the repository:

```sh
OKG_SYNTH_RECORD=1        # store into this directory
OKG_SYNTH_RECORD=/some/path   # store somewhere else entirely
```

## The derived layer: rows, spreads and the paired effect

`*.analysis.json` files are the artifacts a paper actually quotes cells out of —
the controlled-fixture row, the priced row, the within-seed and cross-seed
spreads, and the paired-arm effect. Each carries a rendered `*.analysis.txt`
beside it, so a reader cites a number without opening any JSON.

They are stored as **plain** sorted-key JSON rather than gzipped like the scored
bundles, and that choice is measured rather than assumed (see Size below). They
are content-addressed on `(label, content_digest)`: a recomputation that produces
the same bytes rewrites its own record, and a genuinely different computation
under one label lands beside the first. Nothing is replaced.

`durable.REQUIRED_ANALYSIS_LABELS` **declares** which derived artifacts a
published claim rests on, and `verify` fails when one has no record:

```sh
uv run python -m okg.bench.synth.durable verify
# MISSING declared analysis artifact: no record labelled 'priced-history-row'
```

That is the fix for how these were lost. The two rows used to land only in
pytest's `tmp_path`, and the spreads and the effect were never written to disk at
all — from outside, a row computed and dropped looks exactly like a row that was
never computed, and both read to a reader as a citation into nothing.

Rebuild the sampling artifacts from the scored records — no model call, no
database, no credentials:

```sh
uv run python scripts/derive_synth_statistics.py --dry-run   # print
uv run python scripts/derive_synth_statistics.py             # store
```

`controlled-fixture/controlled-fixture-row.json` is a **frozen** pre-mechanism
copy of one row. It is the artifact the `req.row.precision` evidence item cites,
so it is left exactly as that run wrote it; new rows go to content-addressed
records instead of replacing it.

## Size

Measured on a real deterministic smoke run: the scored layer is 282,403 bytes of
sorted-key JSON (manifest 169,445 + engine reads 100,100 + scorecard 8,006 +
sensitivity 4,171 + binding 681), which compresses to a **26,007-byte** bundle
plus the 681-byte plain binding. The rendered corpora (37,392 bytes here, far
larger on the prose presets), the verbatim model responses and the full
extracted-claim snapshots are deliberately **not** stored — re-score never reads
them, so they are not what makes a claim checkable.

The whole directory, measured with 9 scored and 11 derived records present:

| layer | files | bytes |
| --- | --- | --- |
| scored bundles | 9 | 974,295 |
| scored run bindings (plain) | 9 | 103,230 |
| derived artifacts (plain JSON) | 11 | 168,086 |
| derived renderings (plain text) | 11 | 37,591 |
| frozen pre-mechanism row | 2 | 65,212 |
| `index.json` | 1 | 27,554 |
| **total** | **43** | **1,375,968** (1,344 KiB) |

Why the derived layer is not gzipped, on the numbers: it is 205,677 bytes in the
working tree and would be 38,792 bytes gzipped — an 81% saving that looks
decisive and is not, because git already stores every blob deflated. The 22
derived files weigh **46,180 bytes** as git objects, 22.5% of their plain size,
so compressing them by hand would save about 7 KiB of repository weight and cost
the citable layer its legibility to `git diff` and `grep`. The scored bundles
keep their gzip because they are 276 KiB each and are only ever read by a
re-score.

See `src/okg/bench/synth/durable.py` for the mechanism and the trade-off.
