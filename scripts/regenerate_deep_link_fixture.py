#!/usr/bin/env python
"""Regenerate the deep-link parity fixture.

    uv run python scripts/regenerate_deep_link_fixture.py

Writes ``tests/substrate/chat/fixtures/deep_link_urls.json`` from the current
``okg.substrate.chat.deep_links`` emitter. The console-side vitest
(``deepLinks.test.tsx``) asserts against that file, so regenerating it changes
what the console is held to — review the diff.

Deliberately NOT done by the test suite. A test that writes its own fixture
can never fail on staleness, and it dirties a read-only checkout on a plain
`pytest` run.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from tests.substrate.chat.test_deep_links import (  # noqa: E402
    FIXTURE_PATH,
    render_fixture_text,
)


def main() -> int:
    text = render_fixture_text()
    previous = FIXTURE_PATH.read_text() if FIXTURE_PATH.exists() else None

    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE_PATH.write_text(text)

    if previous == text:
        print(f"unchanged: {FIXTURE_PATH.relative_to(REPO_ROOT)}")
    else:
        print(f"rewrote:   {FIXTURE_PATH.relative_to(REPO_ROOT)}")
        print("review the diff — the console vitest asserts against this file")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
