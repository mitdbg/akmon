"""The append-resume prefix check must exist exactly once.

`codex_sessions` and `_agent_sessions_extract` used to carry byte-for-byte
copies of this logic. The copies drifted: the 64 KiB-window bug was fixed in
codex_sessions (PR #872) and left standing in the agent_sessions copy, because
nothing made the divergence visible. These tests make it visible — if either
adapter grows its own body again, the identity assertions fail loudly instead
of the two implementations quietly disagreeing.
"""

from __future__ import annotations

import hashlib

import pytest

from okg.substrate.library.sources import _agent_sessions_extract, codex_sessions
from okg.substrate.library.sources import _transcript_resume


# Every (shared function, codex name, agent_sessions name) triple. The
# adapters bind these as aliases, so identity — not merely equal behavior —
# is the assertion.
SHARED_BINDINGS = [
    (
        "append_resume_state",
        _transcript_resume.append_resume_state,
        codex_sessions._append_delta_state,
        _agent_sessions_extract.append_state_for_path,
    ),
    (
        "prior_last_line_hash_matches",
        _transcript_resume.prior_last_line_hash_matches,
        codex_sessions._prior_last_line_hash_matches,
        _agent_sessions_extract.prior_last_line_hash_matches,
    ),
    (
        "path_ends_with_newline",
        _transcript_resume.path_ends_with_newline,
        codex_sessions._path_ends_with_newline,
        _agent_sessions_extract.path_ends_with_newline,
    ),
    (
        "line_hash",
        _transcript_resume.line_hash,
        codex_sessions._line_hash,
        _agent_sessions_extract._line_hash,
    ),
]


@pytest.mark.parametrize(
    "name,shared,codex_fn,agent_fn",
    SHARED_BINDINGS,
    ids=[b[0] for b in SHARED_BINDINGS],
)
def test_both_adapters_bind_the_shared_implementation(
    name, shared, codex_fn, agent_fn
):
    assert codex_fn is shared, (
        f"codex_sessions no longer uses the shared {name}; the resume logic has "
        "diverged again"
    )
    assert agent_fn is shared, (
        f"_agent_sessions_extract no longer uses the shared {name}; the resume "
        "logic has diverged again"
    )


def test_shared_module_is_the_only_definition():
    """Neither adapter may define its own body for these functions.

    An alias's `__module__` is the module that *defined* it. If an adapter
    re-grows a local `def`, this catches it even if the local copy happens to
    agree with the shared one today.
    """
    for name, shared, codex_fn, agent_fn in SHARED_BINDINGS:
        assert shared.__module__ == _transcript_resume.__name__
        assert codex_fn.__module__ == _transcript_resume.__name__, name
        assert agent_fn.__module__ == _transcript_resume.__name__, name


class _Cursor:
    """Minimal stand-in for either adapter's cursor dataclass.

    The shared check reads exactly three fields; this asserts that contract
    by supplying nothing else.
    """

    def __init__(self, *, size: int, last_line_hash: str, append_safe: bool = True):
        self.bytes = size
        self.last_line_hash = last_line_hash
        self.append_safe = append_safe


def _write(tmp_path, payload: bytes):
    path = tmp_path / "transcript.jsonl"
    path.write_bytes(payload)
    return path


def test_matching_prefix_resumes(tmp_path):
    consumed = b'{"a":1}\n{"b":2}\n'
    path = _write(tmp_path, consumed + b'{"c":3}\n')
    state = _Cursor(
        size=len(consumed),
        last_line_hash=hashlib.sha256(b'{"b":2}').hexdigest(),
    )
    assert _transcript_resume.append_resume_state(
        state, path=path, size=path.stat().st_size
    ) is state


def test_rewritten_prefix_forces_full_reparse(tmp_path):
    consumed = b'{"a":1}\n{"REWRITTEN":2}\n'
    path = _write(tmp_path, consumed + b'{"c":3}\n')
    state = _Cursor(
        size=len(consumed),
        last_line_hash=hashlib.sha256(b'{"b":2}').hexdigest(),
    )
    assert (
        _transcript_resume.append_resume_state(
            state, path=path, size=path.stat().st_size
        )
        is None
    )


def test_line_longer_than_the_window_still_matches(tmp_path):
    """The regression the duplication hid.

    A last consumed line past 64 KiB must be hashed whole. The old
    single-window check hashed only its trailing 64 KiB, so resume never
    matched for exactly the transcripts big enough to need it.
    """
    long_line = b'{"payload":"' + b"x" * 200_000 + b'"}'
    consumed = b'{"a":1}\n' + long_line + b"\n"
    path = _write(tmp_path, consumed + b'{"c":3}\n')
    state = _Cursor(
        size=len(consumed),
        last_line_hash=hashlib.sha256(long_line).hexdigest(),
    )
    assert len(long_line) > 65536
    assert _transcript_resume.append_resume_state(
        state, path=path, size=path.stat().st_size
    ) is state


def test_partial_last_line_blocks_resume(tmp_path):
    """`append_safe` is False when the writer was mid-line."""
    path = _write(tmp_path, b'{"a":1}\n{"b":2')
    assert _transcript_resume.path_ends_with_newline(path, path.stat().st_size) is False
    state = _Cursor(
        size=8,
        last_line_hash=hashlib.sha256(b'{"a":1}').hexdigest(),
        append_safe=False,
    )
    assert (
        _transcript_resume.append_resume_state(
            state, path=path, size=path.stat().st_size
        )
        is None
    )


def test_no_growth_means_nothing_to_resume(tmp_path):
    consumed = b'{"a":1}\n'
    path = _write(tmp_path, consumed)
    state = _Cursor(
        size=len(consumed),
        last_line_hash=hashlib.sha256(b'{"a":1}').hexdigest(),
    )
    assert (
        _transcript_resume.append_resume_state(
            state, path=path, size=path.stat().st_size
        )
        is None
    )
