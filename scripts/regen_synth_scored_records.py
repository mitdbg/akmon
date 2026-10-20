#!/usr/bin/env python3
"""Regenerate the DETERMINISTIC synth benchmark records, so a worktree recycle
is a re-run rather than a loss.

Every recorded artifact this benchmark produced used to live under ``.scratch/``,
which is gitignored and worktree-local; two recycles destroyed all of it and
left PACT evidence items citing paths that no longer resolved. This script is
the other half of the fix: the deterministic lane costs nothing to re-run — no
model call, no credentials, no spend — so the records it backs are always one
command away from existing again.

    # one-time, disposable database
    psql "$OKG_ADMIN_DSN" \
      -c "DROP DATABASE IF EXISTS okg_test_synthdur WITH (FORCE)" \
      -c "CREATE DATABASE okg_test_synthdur"

    # regenerate + store into bench/synth/records
    uv run python scripts/regen_synth_scored_records.py \
      --dsn postgres://postgres:okg@127.0.0.1:5433/okg_test_synthdur

Records are content-addressed on (preset, config_digest, content_digest), where
the content digest covers the scored bytes, so re-running this adds no
repository weight: an unchanged configuration rewrites its own bundle
byte-for-byte. A genuine second DRAW of one configuration differs in those bytes
and is kept as its own record rather than replacing the first.

What this does NOT regenerate: the language-model arms (the narrative and
structured rendering runs, the variance draws and the spread over them). Those
cost real money and provider nondeterminism means a re-run would not reproduce
the recorded draw anyway. They are the sibling statistics task's problem; the
evidence items resting on them are annotated rather than restored.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

DEPLOYMENT_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"


def _install_composed_catalog(dsn: str, workdir: Path) -> None:
    """Apply the composed extraction+consistency catalog the smoke run needs.

    Mirrors the fixture the e2e witness uses; kept here so the script stands on
    its own rather than importing out of the test tree.
    """
    import psycopg
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "scratch",
                "schema_dir": "schemas",
                "modules": ["extraction", "consistency"],
            }
        ),
        encoding="utf-8",
    )
    (workdir / "schemas").mkdir(exist_ok=True)
    composed = compose_catalog(workdir)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default="postgres://postgres:okg@127.0.0.1:5433/okg_test_synthdur",
        help="a DISPOSABLE postgres database; the run publishes into it",
    )
    parser.add_argument(
        "--preset",
        default="smoke",
        help="deterministic preset to run (default: smoke)",
    )
    parser.add_argument(
        "--records-root",
        default=None,
        help="where bundles land (default: bench/synth/records)",
    )
    parser.add_argument(
        "--keep-run-dir",
        default=None,
        help="retain the full run directory here (corpora included) for debugging",
    )
    args = parser.parse_args(argv)

    from okg.bench.synth.config import load_preset
    from okg.bench.synth.durable import (
        DEFAULT_RECORDS_ROOT,
        report_record,
        store_scored_artifacts,
    )
    from okg.bench.synth.runner import run_synth_benchmark

    config = load_preset(args.preset)
    # The runner routes on this exact value (``extractor == "llm"`` builds an
    # LlmLane); refusing it here is what keeps a stray --preset from spending
    # money on the operator's behalf.
    if config.realism.extractor == "llm":
        parser.error(
            f"preset {args.preset!r} declares extractor 'llm'; this script is "
            "the DETERMINISTIC lane only and will not spend money on your behalf"
        )

    records_root = (
        Path(args.records_root) if args.records_root else DEFAULT_RECORDS_ROOT
    )
    tmp = Path(tempfile.mkdtemp(prefix="synth-regen-"))
    try:
        _install_composed_catalog(args.dsn, tmp / "catalog")
        run_dir = tmp / "run"
        print(f"[regen] running preset {args.preset!r} against {args.dsn}")
        run_synth_benchmark(
            dsn=args.dsn,
            deployment_dir=DEPLOYMENT_DIR,
            out_dir=run_dir,
            preset=args.preset,
            config=config,
        )
        bundle = store_scored_artifacts(
            run_dir,
            records_root=records_root,
            note=f"deterministic {args.preset} lane, regenerated by "
            f"scripts/regen_synth_scored_records.py",
        )
        print(f"[regen] stored {bundle} ({bundle.stat().st_size:,} bytes)")
        print()
        print(report_record(bundle.name, records_root), end="")
        if args.keep_run_dir:
            keep = Path(args.keep_run_dir)
            if keep.exists():
                shutil.rmtree(keep)
            shutil.copytree(run_dir, keep)
            print(f"\n[regen] full run directory kept at {keep}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
