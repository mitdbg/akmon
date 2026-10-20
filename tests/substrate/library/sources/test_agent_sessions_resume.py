"""Resume for Claude transcripts with lines past the prefix-check window.

`append_state_for_path` resumes only when the file grew AND the last consumed
line re-hashes to the persisted value. That second check is what stands
between a REWRITTEN transcript and stale facts persisting in the graph, so
these tests pin both directions: a long line must not defeat the check, and a
modified prefix must still defeat it.

Scale, measured against the resume gate itself over the real corpus: 13 of
2496 Claude transcripts (20.6 MiB, 0.7% of bytes) fail to resume today, and
none do after the fix. Do NOT quote "351 transcripts / 79% of bytes" -- that
counts files containing such a line ANYWHERE, which is exposure over time,
not the per-cycle cost, because the check only inspects the last consumed
line. An earlier draft of this work made that mistake.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path

from okg.substrate.library.sources import _agent_sessions_extract as ex

WINDOW = 65536


def _state(*, consumed: int, line_hash: str, path: Path, line_count: int = 1):
    """A scan state as a prior run would have persisted it.

    Built reflectively so this does not break every time an unrelated counter
    is added to SessionScanState -- only the four fields the resume check
    reads are set deliberately.
    """
    kwargs: dict = {}
    for f in dataclasses.fields(ex.SessionScanState):
        if f.default is not dataclasses.MISSING:
            continue
        if f.default_factory is not dataclasses.MISSING:  # type: ignore[misc]
            continue
        annotation = str(f.type)
        kwargs[f.name] = (
            0 if "int" in annotation
            else False if "bool" in annotation
            else {} if "dict" in annotation
            else ""
        )
    kwargs.update(
        bytes=consumed,
        mtime_ns=path.stat().st_mtime_ns,
        line_count=line_count,
        last_line_hash=line_hash,
        append_safe=True,
    )
    return ex.SessionScanState(**kwargs)


def _write_two_lines(path: Path, first_payload_len: int) -> tuple[int, str]:
    """Write a transcript; return (offset after line 1, sha256 of line 1)."""
    first = json.dumps({"type": "user", "text": "x" * first_payload_len})
    second = json.dumps({"type": "assistant", "text": "reply"})
    path.write_bytes((first + "\n" + second + "\n").encode("utf-8"))
    return len(first) + 1, hashlib.sha256(first.encode("utf-8")).hexdigest()


def test_line_longer_than_the_window_still_resumes(tmp_path: Path) -> None:
    """The defect: a long last line hashed a suffix and never matched.

    Asserted through append_state_for_path, not the helper alone -- the helper
    returning True is only interesting if it actually produces a resume.
    """
    path = tmp_path / "session.jsonl"
    consumed, line_hash = _write_two_lines(path, 200_000)
    assert consumed > WINDOW, "fixture line did not exceed the window"

    state = _state(consumed=consumed, line_hash=line_hash, path=path)
    assert ex.prior_last_line_hash_matches(path, state) is True
    resumed = ex.append_state_for_path(
        state, path=path, size=path.stat().st_size
    )
    assert resumed is not None, "refused to resume -> full re-parse from zero"
    assert resumed.bytes == consumed


def test_short_line_still_resumes(tmp_path: Path) -> None:
    """The case that always worked, kept as the control."""
    path = tmp_path / "session.jsonl"
    consumed, line_hash = _write_two_lines(path, 1_024)
    assert consumed < WINDOW

    state = _state(consumed=consumed, line_hash=line_hash, path=path)
    assert ex.prior_last_line_hash_matches(path, state) is True
    assert ex.append_state_for_path(
        state, path=path, size=path.stat().st_size
    ) is not None


def _assert_rewrite_refused(tmp_path: Path, payload_len: int) -> None:
    path = tmp_path / f"rewritten-{payload_len}.jsonl"
    consumed, line_hash = _write_two_lines(path, payload_len)
    state = _state(consumed=consumed, line_hash=line_hash, path=path)
    assert ex.prior_last_line_hash_matches(path, state) is True

    # Same length, different content: the offset still looks plausible.
    raw = bytearray(path.read_bytes())
    raw[consumed - 20:consumed - 10] = b"Z" * 10
    path.write_bytes(bytes(raw))

    assert ex.prior_last_line_hash_matches(path, state) is False, (
        f"rewrite at payload_len={payload_len} was accepted"
    )
    assert ex.append_state_for_path(
        state, path=path, size=path.stat().st_size
    ) is None


def test_rewritten_short_prefix_is_still_refused(tmp_path: Path) -> None:
    """Rewrite detection at a length the OLD code also handled.

    Split from the long-line case deliberately: this one must keep passing
    even with the original single-window logic restored, which is what makes
    it evidence that the port did not weaken rewrite detection. (The long-line
    case cannot make that claim -- under the old code its precondition is
    unreachable, so its failure there says nothing about rewrites.)
    """
    _assert_rewrite_refused(tmp_path, 1_024)


def test_rewritten_long_prefix_is_still_refused(tmp_path: Path) -> None:
    """Rewrite detection on the newly-reachable path.

    The new code is length-dependent, so a "fix" that merely widened the
    window could pass the long-line resume test while silently accepting a
    rewrite. This is the case that catches that.
    """
    _assert_rewrite_refused(tmp_path, 200_000)


def test_rewrite_before_the_last_line_is_NOT_detected(tmp_path: Path) -> None:
    """Pins a real limit rather than leaving it implied.

    The check has only ever hashed the LAST consumed line, so an in-place edit
    earlier in the prefix is invisible. Before this change that gap was
    unreachable for transcripts whose last line exceeded the window -- they were
    refused for the window bug, and so were protected by accident. Removing the
    window bug removes that accident.

    Not fixed here: verifying the whole consumed prefix would mean re-hashing
    it every run, which defeats incremental resume. The same append-only
    assumption is already the documented soundness boundary of
    AppendOffsetProbe, which cannot see a same-size edit between its head and
    tail windows either.

    This test exists so the limit fails loudly if someone later assumes the
    check protects the whole prefix. If a future change DOES verify more, this
    test should be inverted deliberately, not deleted quietly.
    """
    path = tmp_path / "earlier-rewrite.jsonl"
    first = json.dumps({"type": "user", "text": "APPROVE-" + "." * 100})
    second = json.dumps({"type": "assistant", "text": "z" * 200_000})
    path.write_bytes((first + "\n" + second + "\n").encode("utf-8"))
    consumed = len(first) + 1 + len(second) + 1
    last_hash = hashlib.sha256(second.encode("utf-8")).hexdigest()

    state = _state(consumed=consumed, line_hash=last_hash, path=path, line_count=2)
    assert ex.prior_last_line_hash_matches(path, state) is True

    # Same-length edit to the FIRST line; last line untouched; file grows.
    raw = bytearray(path.read_bytes())
    idx = raw.find(b"APPROVE-")
    assert idx >= 0
    raw[idx:idx + 8] = b"REVOKED-"
    appended = json.dumps({"type": "user", "text": "next"}) + "\n"
    path.write_bytes(bytes(raw) + appended.encode("utf-8"))

    # Documented behavior: resume proceeds, so the edited bytes are never
    # re-read. Asserted, so the exposure is visible in the test suite.
    assert ex.prior_last_line_hash_matches(path, state) is True
    assert ex.append_state_for_path(
        state, path=path, size=path.stat().st_size
    ) is not None


def test_truncated_transcript_is_refused(tmp_path: Path) -> None:
    """A file that shrank below the consumed offset cannot be resumed."""
    path = tmp_path / "session.jsonl"
    consumed, line_hash = _write_two_lines(path, 200_000)
    state = _state(consumed=consumed, line_hash=line_hash, path=path)

    path.write_bytes(path.read_bytes()[: consumed // 2])
    assert ex.append_state_for_path(
        state, path=path, size=path.stat().st_size
    ) is None


def test_hashing_a_multi_megabyte_line_stays_bounded(tmp_path: Path) -> None:
    """Streamed, not held whole.

    The longest line in the real corpus is 5.2 MB; reading it into one bytes
    object to hash it would make peak memory track the largest line in the
    transcript rather than a fixed window.
    """
    import tracemalloc

    path = tmp_path / "huge-line.jsonl"
    consumed, line_hash = _write_two_lines(path, 8_000_000)
    state = _state(consumed=consumed, line_hash=line_hash, path=path)

    tracemalloc.start()
    try:
        assert ex.prior_last_line_hash_matches(path, state) is True
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    # Generous headroom over the 64 KiB working set, but far under the 8 MB
    # line -- so holding the line whole fails loudly.
    assert peak < 2_000_000, f"peak {peak} bytes suggests the line was buffered"


def test_first_line_of_file_resumes(tmp_path: Path) -> None:
    """Boundary: the consumed prefix is exactly one line, so line_start is 0.

    The backward scan finds no newline before it and must fall through to
    offset zero rather than reporting a mismatch.
    """
    path = tmp_path / "single.jsonl"
    only = json.dumps({"type": "user", "text": "y" * 150_000})
    path.write_bytes((only + "\n").encode("utf-8"))
    consumed = len(only) + 1
    line_hash = hashlib.sha256(only.encode("utf-8")).hexdigest()

    state = _state(consumed=consumed, line_hash=line_hash, path=path)
    assert ex.prior_last_line_hash_matches(path, state) is True

    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"type": "assistant", "text": "next"}) + "\n")
    assert ex.append_state_for_path(
        state, path=path, size=path.stat().st_size
    ) is not None
