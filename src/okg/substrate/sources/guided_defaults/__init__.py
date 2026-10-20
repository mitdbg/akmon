"""Packaged defaults for `okg deployment add-source` (issue #1802)."""
from __future__ import annotations

from pathlib import Path

DEFAULTS_DIR = Path(__file__).resolve().parent
EXTRACTORS_YAML = DEFAULTS_DIR / "extractors.yaml"
