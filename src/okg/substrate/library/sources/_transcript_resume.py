"""Shared append-resume prefix check for JSONL transcript sources.

Both session adapters — `codex_sessions` and the Claude Code
`agent_sessions` extractor — ingest append-only JSONL transcripts and
want to resume a later run at the byte offset the previous run stopped
at, rather than re-parsing the file from zero. Resuming is only sound if
the bytes already consumed are still an exact prefix of the file: a
rotated, truncated, or rewritten transcript must fall back to a full
re-parse or the graph inherits facts from a file that no longer exists.

The check is: hash the last line the previous run consumed, and compare
it to the hash the cursor recorded. This module is that check, once.

It used to exist twice, once per adapter, differing only in the cursor
dataclass it accepted and in leading-underscore naming. That cost the
same fix twice: the 64 KiB-window bug below was found and fixed in
codex_sessions (PR #872), then found again and ported to agent_sessions
months later (PACT `agent-sessions-resume-long-line`) — by which point
the copies had drifted apart in more than naming. Anything that reads a
cursor's `bytes`, `last_line_hash`, and `append_safe` should call these
functions instead of growing a third copy.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Protocol, TypeVar

# Backward scan and streaming read granularity. Bounds peak memory: no
# single read holds more than this, regardless of how long a transcript
# line is.
_WINDOW = 65536


class ResumeCursor(Protocol):
    """The only three fields the prefix check reads.

    Structural, so each adapter keeps its own full cursor dataclass
    (`_TranscriptParseState`, `SessionScanState`) and passes it directly.

    Declared read-only (`@property`, not bare annotations) because both
    concrete cursors are frozen dataclasses: a frozen attribute does not
    satisfy a mutable protocol member, so bare annotations here would
    make every real call site a type error.
    """

    @property
    def bytes(self) -> int: ...

    @property
    def last_line_hash(self) -> str: ...

    @property
    def append_safe(self) -> bool: ...


CursorT = TypeVar("CursorT", bound=ResumeCursor)


def append_resume_state(
    state: CursorT | None,
    *,
    path: Path,
    size: int,
) -> CursorT | None:
    """Return `state` if resuming from it is sound, else None.

    None means "re-parse the whole file". The four rejections, in order:
    no cursor or the previous run stopped mid-line (`append_safe` false),
    the file did not grow, no recorded line hash to verify against, and
    the recorded hash does not match what the file holds now.
    """
    if state is None or not state.append_safe:
        return None
    if size <= state.bytes:
        return None
    if not state.last_line_hash:
        return None
    if not prior_last_line_hash_matches(path, state):
        return None
    return state


def prior_last_line_hash_matches(path: Path, state: ResumeCursor) -> bool:
    """Does the file still end the previously-consumed region with the
    same line the cursor recorded?"""
    if state.bytes <= 0:
        return False
    try:
        with path.open("rb") as fh:
            # Find where the last consumed line BEGINS, walking backwards in
            # bounded steps. The version this replaced read one 64 KiB window
            # and treated whatever followed the last newline in it as the line
            # -- so a line longer than the window hashed a mere suffix, never
            # matched, and resume was refused. (The expansion loop it had only
            # ran when the window held nothing but newlines, so it never fired
            # for a long line.)
            #
            # Both transcript formats hit this routinely, because both embed
            # rendered tool output inline and a single line carrying it runs to
            # megabytes: 351 of 2496 Claude transcripts on this host contain
            # one. The consequences differed, which is why it was found twice.
            # For codex_sessions it was fatal -- a per-run ingest budget stopped
            # the re-parse at the same offset every time, so the file never
            # advanced (PR #872). For agent_sessions it was merely wasteful:
            # 13 transcripts (20.6 MiB) failing to resume at any given moment,
            # and none after the fix (PACT agent-sessions-resume-long-line).
            end = state.bytes
            # Strip the trailing newline(s) the writer added, reading in blocks
            # rather than a seek+read per byte: a long run of blank lines at the
            # cursor made the per-byte version ~500x slower (1.23s for a 1M-byte
            # run, against 2.4ms here).
            while end > 0:
                block_start = max(0, end - _WINDOW)
                fh.seek(block_start)
                block = fh.read(end - block_start)
                if not block:
                    return False
                stripped = block.rstrip(b"\n")
                end = block_start + len(stripped)
                if stripped:
                    break
            line_start = 0
            probe_end = end
            while probe_end > 0:
                probe_start = max(0, probe_end - _WINDOW)
                fh.seek(probe_start)
                chunk = fh.read(probe_end - probe_start)
                if not chunk:
                    return False
                found = chunk.rfind(b"\n")
                if found >= 0:
                    line_start = probe_start + found + 1
                    break
                probe_end = probe_start
            # Hash the line by streaming it, so a multi-megabyte line costs
            # bounded memory rather than being held whole.
            digest = hashlib.sha256()
            fh.seek(line_start)
            remaining = end - line_start
            while remaining > 0:
                chunk = fh.read(min(_WINDOW, remaining))
                if not chunk:
                    return False
                digest.update(chunk)
                remaining -= len(chunk)
            return digest.hexdigest() == state.last_line_hash
    except OSError:
        return False


def path_ends_with_newline(path: Path, size: int) -> bool:
    """Does byte `size` land on a line boundary?

    The answer becomes the cursor's `append_safe`, and resuming from an
    offset that splits a JSON object would corrupt the parse, so a False
    here forces the next run to re-parse from zero.

    `size` is whichever offset the CALLER intends to record as consumed,
    and the two callers deliberately pass different things:

    - `codex_sessions._cursor_entry` passes `consumed` -- the offset its
      parse loop actually stopped at, which it holds to a line boundary
      by refusing to consume a trailing line with no newline. Passing
      `st_size` there instead made a live-growing transcript re-parse
      from zero on every run, which is the bug its own docstring records
      as fixed; do not "simplify" it back.
    - `_agent_sessions_emit` passes `cur_size` (end of file), so an
      actively-appended transcript there does re-parse whenever the
      writer is caught mid-line.
    """
    if size <= 0:
        return False
    try:
        with path.open("rb") as fh:
            fh.seek(size - 1)
            return fh.read(1) == b"\n"
    except OSError:
        return False


def line_hash(line: bytes) -> str:
    """Hash one newline-stripped transcript line."""
    return hashlib.sha256(line).hexdigest()
