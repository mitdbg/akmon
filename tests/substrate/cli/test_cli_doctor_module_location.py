"""Regression test: ``_cmd_doctor`` and every ``_doctor_*`` helper is
defined in ``cli/doctor.py`` and absent from ``cli/main.py``.

Pinned by ``extract-cli-doctor``. Catches a future regression that
re-inlines a doctor handler back into the main dispatch module.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_DOCTOR = REPO_ROOT / "src/okg/substrate/cli/doctor.py"

EXPECTED_HANDLERS = (
    "_cmd_doctor",
    "_doctor_database",
    "_doctor_extraction_wrap",
    "_doctor_extraction",
    "_doctor_extractors",
    "_doctor_sources",
    # `_doctor_integrity` was retired by `chronos cutover (step 5 finale)`
    # (commit f8e061ea4): the legacy okg.integrity audit read the now-empty
    # legacy tables and was vacuous. The TransformConsistencyGate(chronos)
    # is the Phase B integrity gate, and `--check integrity-coverage`
    # reports coverage by integrity kind. So `_doctor_integrity` no longer
    # lives in cli/doctor.py — only `_doctor_integrity_coverage` does.
    "_doctor_integrity_coverage",
    "_doctor_lens_promotions",
    "_doctor_admission",
    "_doctor_formal_assurance",
    "_doctor_branches",
    "_doctor_edge_fact_strandings",
    "_doctor_acceptance_criteria",
    "_doctor_source_auth",
    "_doctor_external_live_authority",
    "_doctor_nomos",
    "_doctor_profile_diagnostics",
    "_build_doctor_registry",
)

DEF_PATTERN = re.compile(
    r"^def (_cmd_doctor|_doctor_[\w]+|_build_doctor_registry)\(",
    re.MULTILINE,
)


def test_doctor_handlers_live_in_doctor_module() -> None:
    text = CLI_DOCTOR.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), (
        f"cli/doctor.py is missing handlers expected to live there: "
        f"{sorted(missing)}"
    )


def test_doctor_handlers_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined doctor handlers: {sorted(defined)} "
        f"— they belong in cli/doctor.py per extract-cli-doctor"
    )


@pytest.mark.parametrize("handler", ["_cmd_doctor"])
def test_main_imports_cmd_doctor(handler: str) -> None:
    text = CLI_MAIN.read_text()
    assert handler in text, (
        f"cli/main.py does not reference {handler!r}; the dispatch "
        f"table will fail to resolve the doctor subcommand"
    )
