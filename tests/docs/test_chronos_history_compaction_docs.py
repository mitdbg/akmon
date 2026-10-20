from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_history_compaction_runbook_pins_safe_operator_contract() -> None:
    text = (ROOT / "docs/runbooks/troubleshooting/storage.md").read_text()

    assert "Dry-run creates an immutable plan" in text
    assert "--confirm <plan-id> --apply" in text
    assert "generation_compacted" in text
    assert "--offline-reclaim --workers-stopped" in text
    assert "live `VACUUM FULL` is forbidden" in text
    assert "pg_total_relation_size" in text
    assert "there is no universal safe interval" in text
