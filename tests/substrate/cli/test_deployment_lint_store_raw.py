"""Lint names what ``store_raw: false`` needs and does (issue #2504).

The fixture is a copy of the in-repo beaver-okg deployment, whose two
sources take their source policy from
``nomos.source_policy_defaults.by_source_class.reference_catalog``.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from okg.substrate.deployment_lint import lint_deployment

_BEAVER = Path(__file__).resolve().parents[3] / "deployments" / "beaver-okg"
_VALID_KEY = "k" * 32
_SOURCES = ["beaver-domain", "beaver-schema"]
_DSN_ENV = {"BEAVER_OKG_DSN": "postgres://u:p@localhost:5432/okg_beaver"}


def _beaver(tmp_path: Path, *, store_raw: bool, pii_classes: str = "") -> Path:
    deployment_dir = tmp_path / "beaver-okg"
    shutil.copytree(_BEAVER, deployment_dir)
    manifest = deployment_dir / "deployment.yaml"
    text = manifest.read_text(encoding="utf-8")
    old = "      reference_catalog:\n        store_raw: true\n"
    assert text.count(old) == 1
    new = (
        "      reference_catalog:\n"
        f"        store_raw: {'true' if store_raw else 'false'}\n"
        + pii_classes
    )
    manifest.write_text(text.replace(old, new), encoding="utf-8")
    return deployment_dir


def _lint(deployment_dir: Path, **env: str) -> dict:
    return lint_deployment(deployment_dir, env={**_DSN_ENV, **env})


def _by_code(report: dict, code: str) -> list[dict]:
    return [f for f in report["findings"] if f["code"] == code]


def test_store_raw_false_without_key_names_the_key_and_sources(tmp_path: Path):
    report = _lint(_beaver(tmp_path, store_raw=False))

    (finding,) = _by_code(report, "deployment.nomos.pseudonym_key_missing")
    assert finding["severity"] == "warning"
    assert "OKG_PSEUDONYM_HMAC_KEY" in finding["message"]
    assert finding["observed"] == _SOURCES
    assert report["ok"] is True


def test_short_key_is_reported_without_printing_it(tmp_path: Path):
    short = "tooshort"
    report = _lint(
        _beaver(tmp_path, store_raw=False), OKG_PSEUDONYM_HMAC_KEY=short,
    )

    (finding,) = _by_code(report, "deployment.nomos.pseudonym_key_missing")
    assert "too short" in finding["details"]["error"]
    assert short not in repr(report)


def test_valid_key_clears_the_key_finding_but_not_the_search_warning(
    tmp_path: Path,
):
    report = _lint(
        _beaver(tmp_path, store_raw=False), OKG_PSEUDONYM_HMAC_KEY=_VALID_KEY,
    )

    assert not _by_code(report, "deployment.nomos.pseudonym_key_missing")
    masked = _by_code(report, "deployment.nomos.store_raw_masks_all")
    assert sorted(f["location"]["pointer"] for f in masked) == [
        f"/sources/{name}" for name in _SOURCES
    ]
    assert all(f["severity"] == "warning" for f in masked)
    assert all("search" in f["message"] for f in masked)
    assert report["ok"] is True


def test_store_raw_true_has_neither_finding(tmp_path: Path):
    report = _lint(_beaver(tmp_path, store_raw=True))

    assert not _by_code(report, "deployment.nomos.pseudonym_key_missing")
    assert not _by_code(report, "deployment.nomos.store_raw_masks_all")


@pytest.mark.parametrize(
    ("pii_class", "masks_all"),
    [
        ("direct_identifier", True),
        ("sensitive_attribute", True),
        ("non_pii", False),
        ("free_text", False),
    ],
)
def test_masks_all_depends_on_a_searchable_class(
    tmp_path: Path, pii_class: str, masks_all: bool,
):
    pii_classes = (
        "        pii_classes:\n"
        f"          text: {pii_class}\n"
    )
    report = _lint(
        _beaver(tmp_path, store_raw=False, pii_classes=pii_classes),
        OKG_PSEUDONYM_HMAC_KEY=_VALID_KEY,
    )

    masked = _by_code(report, "deployment.nomos.store_raw_masks_all")
    assert bool(masked) is masks_all
