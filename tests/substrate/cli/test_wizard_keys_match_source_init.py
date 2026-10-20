"""Pin the wizard / discoverer / source-class param-key
agreement invariant.

Per `tighten-setup-feedback-loop`: three places have to agree
on every source's parameter names — the source class's
`__init__`, the source-default YAML's `params:` block, and the
academic-researcher `WIZARDS` catalog (plus the optional discoverer in
`library/sources/discovery/registry.py`). Pre-fix, four of
these drifted (`paper_pdf_inbox`, `calendar_ics`,
`group_webpage_scrape`, `lab_webpage_talks`); each disagreement
silently broke `okg add <short>` for its source.

This test fails loudly when any wizard returns a dict key that
isn't a parameter of the source class's `__init__`, and when
any discoverer in the registry emits a candidate whose
`params` includes a key the source class doesn't accept.
"""
from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from unittest import mock

import yaml

from okg.substrate.cli.profile_cli_loader import get_profile_cli
from okg.substrate.library.sources.discovery.env import DiscoveryEnv
from okg.substrate.library.sources.discovery.registry import (
    DISCOVERERS,
)

_pi_cli_pkg = get_profile_cli("academic-researcher")
wiz = _pi_cli_pkg.wizards

SOURCE_DEFAULTS_DIR = Path(
    "src/okg/substrate/library/profiles/academic-researcher/source-defaults",
)

# URL wizards call urlopen() to verify reachability — mocking
# urllib bypasses the network.
URL_WIZARD_SHORT_NAMES: frozenset[str] = frozenset(
    # `canvas` added by add-academic-corpora-sources: _wizard_canvas
    # verifies the Canvas base URL, so it needs the same urlopen mock.
    {"cal-ics", "group-page", "lab-talks", "canvas"},
)


def _load_source_class(source_id: str):
    """Import the source class declared in
    `source-defaults/<source_id>.yaml`."""
    yaml_path = SOURCE_DEFAULTS_DIR / f"{source_id}.yaml"
    assert yaml_path.exists(), (
        f"missing source-default YAML at {yaml_path}"
    )
    doc = yaml.safe_load(yaml_path.read_text())
    assert isinstance(doc, dict), (
        f"source-default YAML {yaml_path} not a mapping"
    )
    inner = doc.get(source_id)
    assert isinstance(inner, dict), (
        f"no `{source_id}:` block in {yaml_path}"
    )
    module_str = inner.get("module")
    class_str = inner.get("class")
    assert module_str and class_str, (
        f"missing module/class in {yaml_path}"
    )
    mod = importlib.import_module(module_str)
    cls = getattr(mod, class_str)
    return cls, inner


def _init_param_names(cls) -> set[str]:
    sig = inspect.signature(cls.__init__)
    return {
        name for name in sig.parameters if name != "self"
    }


def _call_wizard(short: str, fn) -> set[str]:
    """Drive the wizard with mocked input + (for URL wizards)
    mocked urlopen. Returns the dict keys the wizard emits."""
    inputs = iter(["/tmp"] * 100)

    def _fake_input(_prompt):
        return next(inputs)

    if short in URL_WIZARD_SHORT_NAMES:
        # Feed a real-shape URL + stub urlopen so the wizard's
        # reachability probe returns True without making a
        # network request.
        url_inputs = iter(["https://example.com/feed"] * 20)

        def _fake_url_input(_prompt):
            return next(url_inputs)

        # Patch on the loaded wizards module directly — the
        # profile dir's hyphen means a string-path mock.patch
        # can't resolve it.
        with mock.patch.object(
            wiz, "_verify_url_reachable", return_value=True,
        ), mock.patch("builtins.input", _fake_url_input):
            result = fn("test-deployment")
    else:
        with mock.patch("builtins.input", _fake_input):
            result = fn("test-deployment")

    assert isinstance(result, dict), (
        f"wizard `{short}` returned {type(result)}, want dict"
    )
    return set(result.keys())


def test_every_wizard_emits_keys_the_source_class_accepts() -> None:
    """The agreement invariant: for every entry in
    `WIZARDS`, the wizard's returned dict keys SHALL all be
    parameters of the source class's `__init__`.

    Pre-fix this was violated by:
      - `_wizard_papers` (`paper_pdf_inbox_dir` vs `inbox_path`)
      - `_wizard_cal_ics` (`ics_url` vs `calendar_ics_url`)
      - `_wizard_group_page` (`group_webpage_url` vs
        `group_webpage`)
      - `_wizard_lab_talks` (`lab_talks_url` vs `talks_url`)
    """
    failures: list[str] = []
    for short, (source_id, _kind, fn) in sorted(
        wiz.WIZARDS.items(),
    ):
        cls, _yaml_block = _load_source_class(source_id)
        params = _init_param_names(cls)

        wizard_keys = _call_wizard(short, fn)

        bad = wizard_keys - params
        if bad:
            failures.append(
                f"wizard `{short}` (source_id={source_id}) "
                f"emits keys {sorted(bad)} not in "
                f"{cls.__name__}.__init__ params "
                f"({sorted(params)})"
            )

    assert not failures, (
        "wizard / source-class param-key drift:\n  "
        + "\n  ".join(failures)
    )


def test_every_discoverer_emits_keys_the_source_class_accepts() -> None:
    """The same invariant for `DISCOVERERS`: any candidate's
    `params` dict SHALL only contain keys the source class
    accepts. Pre-fix `_discover_paper_pdf_inbox` violated this
    by emitting `paper_pdf_inbox_dir`."""
    failures: list[str] = []
    env = DiscoveryEnv.from_machine()
    for short, (source_id, _kind, _fn) in sorted(
        wiz.WIZARDS.items(),
    ):
        disc = DISCOVERERS.get(source_id)
        if disc is None:
            continue
        cls, _yaml_block = _load_source_class(source_id)
        params = _init_param_names(cls)

        try:
            cands = disc(env)
        except Exception as e:  # noqa: BLE001
            # If discovery itself raises, that's a separate
            # bug — record it but don't blame the agreement
            # invariant.
            failures.append(
                f"discoverer for `{source_id}` raised: {e!r}"
            )
            continue

        for cand in cands:
            bad = set(cand.params.keys()) - params
            if bad:
                failures.append(
                    f"discoverer `{source_id}` (short={short}) "
                    f"emits candidate keys {sorted(bad)} not "
                    f"in {cls.__name__}.__init__ params "
                    f"({sorted(params)})"
                )

    assert not failures, (
        "discoverer / source-class param-key drift:\n  "
        + "\n  ".join(failures)
    )


def test_source_default_yaml_params_match_source_class_init() -> None:
    """Source-default YAMLs' `params:` blocks are the third
    seat at the table. Every key in `params:` SHALL be a
    parameter of the source class's `__init__`."""
    failures: list[str] = []
    for short, (source_id, _kind, _fn) in sorted(
        wiz.WIZARDS.items(),
    ):
        cls, yaml_block = _load_source_class(source_id)
        params = _init_param_names(cls)
        yaml_params = yaml_block.get("params") or {}
        bad = set(yaml_params.keys()) - params
        if bad:
            failures.append(
                f"source-default `{source_id}.yaml` declares "
                f"params {sorted(bad)} not in "
                f"{cls.__name__}.__init__ params "
                f"({sorted(params)})"
            )

    assert not failures, (
        "source-default YAML / source-class param-key drift:\n  "
        + "\n  ".join(failures)
    )
