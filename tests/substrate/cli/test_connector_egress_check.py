"""Doctor egress-coverage check (centralize-egress-url-validation).

A module that makes an outbound request must reference the central egress
validator. The check denies a synthetic unguarded sink and allows a guarded one,
and passes over every registered outbound-helper file (regression guard).
"""
from __future__ import annotations

from okg.substrate.cli.egress_check import (
    EGRESS_COVERAGE_EXCEPTIONS,
    evaluate_egress_coverage,
    file_has_unguarded_sink,
    scan_egress_coverage,
)

_UNGUARDED = '''
import requests
def deliver(url):
    return requests.post(url, data=b"x", timeout=5)
'''

_GUARDED = '''
import requests
from okg.substrate.library.sources._url_safety import assert_public_http_url
def deliver(url):
    assert_public_http_url(url)
    return requests.post(url, data=b"x", timeout=5, allow_redirects=False)
'''

# The dangerous middle ground the coarse scanner used to MISS: a first-hop
# assert_public_http_url followed by a RAW urlopen, which follows the 3xx
# redirect past the guard (the lab_webpage_talks bug). Must be flagged.
_FIRST_HOP_ONLY = '''
import urllib.request
from okg.substrate.library.sources._url_safety import assert_public_http_url
def fetch(url):
    assert_public_http_url(url)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.read()
'''

_NO_SINK = '''
def add(a, b):
    return a + b
'''


def test_unguarded_sink_is_flagged() -> None:
    assert file_has_unguarded_sink(_UNGUARDED) is True


def test_guarded_sink_passes() -> None:
    assert file_has_unguarded_sink(_GUARDED) is False


def test_first_hop_only_urlopen_is_flagged() -> None:
    # A validator marker present is NOT enough: a raw urlopen still follows the
    # redirect, so the scanner must flag it (closes the gap that hid the
    # lab_webpage_talks redirect-SSRF behind its assert_public_http_url).
    assert file_has_unguarded_sink(_FIRST_HOP_ONLY) is True


def test_module_without_a_sink_is_not_flagged() -> None:
    assert file_has_unguarded_sink(_NO_SINK) is False


def test_evaluate_denies_unguarded_and_allows_guarded(tmp_path) -> None:
    bad = tmp_path / "bad_sink.py"
    bad.write_text(_UNGUARDED)
    good = tmp_path / "good_sink.py"
    good.write_text(_GUARDED)

    result = evaluate_egress_coverage([bad, good], repo_root=tmp_path)
    assert result["ok"] is False
    assert str(bad) in result["violations"]
    assert str(good) not in result["violations"]


def test_tree_scan_finds_no_unguarded_sink() -> None:
    """Whole-tree scan: NO substrate file makes a RAW outbound request without
    the egress validator (or a documented exception). This catches a new raw
    sink that a hand-maintained file list would miss. (Files routed through
    open_validated_url carry no raw sink marker and are inherently safe.)"""
    result = scan_egress_coverage()
    assert result["ok"] is True, f"unguarded outbound sinks: {result['violations']}"
    # The scan actually inspected raw-sink-bearing files (sanity: not a no-op).
    assert len(result["checked"]) >= len(EGRESS_COVERAGE_EXCEPTIONS)


def test_evaluate_no_args_delegates_to_tree_scan() -> None:
    # Back-compat entry point now scans the tree (was a hardcoded file list).
    assert evaluate_egress_coverage()["ok"] is True


def test_every_egress_exception_has_a_written_reason() -> None:
    # A silent exception is a hole; each must justify why its sink is not an
    # SSRF target (false positive / intentional loopback).
    assert EGRESS_COVERAGE_EXCEPTIONS, "expected documented exceptions to exist"
    for path, reason in EGRESS_COVERAGE_EXCEPTIONS.items():
        assert reason and reason.strip(), f"exception {path} lacks a reason"


def test_scan_would_flag_a_synthetic_unguarded_sink(tmp_path) -> None:
    # Prove the scanner actually flags an unguarded sink (not just that the tree
    # happens to be clean): a synthetic substrate file with a raw sink violates.
    sub = tmp_path / "src" / "okg" / "substrate" / "library"
    sub.mkdir(parents=True)
    (sub / "rogue_sink.py").write_text(_UNGUARDED)
    (sub / "ok_sink.py").write_text(_GUARDED)
    result = scan_egress_coverage(repo_root=tmp_path)
    assert result["ok"] is False
    assert "src/okg/substrate/library/rogue_sink.py" in result["violations"]
    assert "src/okg/substrate/library/ok_sink.py" not in result["violations"]
