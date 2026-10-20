from __future__ import annotations

from pathlib import Path

from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import load_manifest, save_manifest


def test_split_files_are_deferred_with_equivalence_rule(tmp_path: Path) -> None:
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo"),
        prose={
            "non_goals": "Do not implement split-file materialization in V1.",
        },
    )
    save_manifest(tmp_path, manifest)

    loaded = load_manifest(tmp_path, "demo")

    assert loaded.change.id == "demo"
    assert not (tmp_path / "pact" / "changes" / "demo" / "requirements.yaml").exists()
