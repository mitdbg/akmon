"""The shared-module eviction guard in `tests/conftest.py` must not go vacuous.

The guard exists because evicting an already-imported module from
`sys.modules` leaves the process holding two copies of it, which silently
breaks `except`/`isinstance` against its classes for the rest of the session.
A guard that has quietly stopped checking looks exactly like a clean run, so
these tests re-prove it by running pytest in a subprocess against a throwaway
test that evicts a guarded module.
"""
from __future__ import annotations

import pytest

pytest_plugins = ["pytester"]

_GUARD_MESSAGE = "replaced in sys.modules"


def _write_conftest(pytester: pytest.Pytester) -> None:
    """Load the real repo-wide guard, nothing else from `tests/conftest.py`."""
    import tests.conftest as repo_conftest

    pytester.makeconftest(
        "import sys\n"
        f"sys.path.insert(0, {str(repo_conftest.__file__)!r}.rsplit('/', 2)[0])\n"
        "from tests.conftest import (  # noqa: F401\n"
        "    pytest_runtest_setup,\n"
        "    pytest_runtest_teardown,\n"
        ")\n"
    )


@pytest.mark.parametrize(
    ("module", "importer"),
    [
        # The module that caused the original incident.
        ("json.decoder", "import json.decoder"),
        # Imported lazily, so an eager configure-time snapshot would have left
        # it unguarded. Proves the lazy snapshot actually covers it.
        ("yaml", "import yaml"),
    ],
)
def test_guard_fails_the_test_that_evicts_a_guarded_module(
    pytester: pytest.Pytester, module: str, importer: str,
) -> None:
    _write_conftest(pytester)
    pytester.makepyfile(
        test_evictor=(
            "import importlib\n"
            "import sys\n"
            f"{importer}\n"
            "\n"
            "def test_evicts():\n"
            f"    sys.modules.pop({module!r}, None)\n"
            f"    importlib.import_module({module!r})\n"
        ),
    )
    result = pytester.runpytest_subprocess("-p", "no:randomly")
    result.assert_outcomes(passed=1, errors=1)
    result.stdout.fnmatch_lines([f"*test_evicts*{_GUARD_MESSAGE}*"])


def test_guard_restores_the_original_so_later_tests_are_unaffected(
    pytester: pytest.Pytester,
) -> None:
    """Only the evictor fails, and the alias skew it caused is repaired.

    Restoring `sys.modules` alone is not enough -- the parent package's
    attribute must be put back too, or `json.decoder.JSONDecodeError` still
    resolves to the orphan copy.
    """
    _write_conftest(pytester)
    pytester.makepyfile(
        test_a_evictor=(
            "import importlib\n"
            "import sys\n"
            "import json.decoder\n"
            "\n"
            "def test_evicts():\n"
            "    sys.modules.pop('json.decoder', None)\n"
            "    importlib.import_module('json.decoder')\n"
        ),
        test_b_victim=(
            "import json\n"
            "import json.decoder\n"
            "import sys\n"
            "\n"
            "def test_alias_skew_is_repaired():\n"
            "    assert json.decoder is sys.modules['json.decoder']\n"
            "    assert (\n"
            "        json.JSONDecodeError\n"
            "        is sys.modules['json.decoder'].JSONDecodeError\n"
            "    )\n"
            "\n"
            "def test_malformed_json_is_still_caught():\n"
            "    try:\n"
            "        json.loads('{not: json}')\n"
            "    except json.JSONDecodeError:\n"
            "        return\n"
            "    raise AssertionError('no error raised')\n"
        ),
    )
    result = pytester.runpytest_subprocess("-p", "no:randomly")
    # The evictor errors at its own teardown; both victims pass.
    result.assert_outcomes(passed=3, errors=1)
