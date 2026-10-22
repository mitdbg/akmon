"""Keep the operator-run live proofs out of the CI lanes.

These tests need something CI does not have: a Docker daemon, a pinned
image, a container brought up by hand. Collected in CI they would be noise;
SKIPPED in CI they would be noise that looks like coverage. So they are not
collected at all unless the operator asks for them.

Each subtree asks for its own flag, because the proofs need different things
and one flag would opt a reader into all of them:

* ``chat/`` needs ``OKG_CHAT_LIVE_PROOF=1`` -- a pinned Open WebUI image and
  two scratch Postgres DSNs.
* ``opensearch/`` needs ``OKG_OPENSEARCH_LIVE=1`` -- the reference OpenSearch
  container (``deployments/opensearch-reference/up.sh``) and a scratch
  Postgres DSN in ``OKG_TEST_DSN``.
* ``postgres_launch/`` needs ``OKG_PG_LAUNCH_LIVE=1`` -- a container engine
  and the ``ops/pg`` image; it starts the rendered okg-workspace Postgres on an
  empty data directory.
* ``mcp_deployment/`` needs ``OKG_MCP_DEPLOYMENT_LIVE=1`` -- a running
  deployment's streamable-HTTP MCP endpoint, its bearer token, two principals
  and an expectations file (#3413; inputs in ``mcp_deployment/harness.py``).

NOT COLLECTED is not the same as SKIPPED, and the difference is the whole
point. A skipped test reports as part of a green run; an uncollected one is
absent, and the guard test asserts that absence in both directions. Once a
flag IS set, its proofs FAIL rather than skip when the thing they need is
missing -- being asked to run and being unable to is a real failure.

Their result travels forward as an evidence receipt on the change that owns
them, not as a green CI check: a proof that ran once, on a pinned version,
keyed so that a version change invalidates it.

``pytest_ignore_collect``, NOT ``collect_ignore_glob``. The glob form only
covers files in the conftest's OWN directory, and these tests live one level
down -- so the first version of this guard collected all six anyway. It read
as protection and was not. The hook is consulted for every descendant path,
and ``test_live_conftest_guard.py`` asserts the guard actually holds rather
than trusting that it does.
"""

from __future__ import annotations

import os
from pathlib import Path

#: Subtree name -> the environment flag that opts into it. A subtree with no
#: entry here is never collected: an unlisted directory is an oversight, and
#: defaulting it to "collect" would put exactly the kind of test this file
#: exists to withhold back into every ordinary run.
SUBTREE_FLAGS = {
    "chat": "OKG_CHAT_LIVE_PROOF",
    "opensearch": "OKG_OPENSEARCH_LIVE",
    "postgres_launch": "OKG_PG_LAUNCH_LIVE",
    "mcp_deployment": "OKG_MCP_DEPLOYMENT_LIVE",
}

#: Kept for the chat proofs' own imports, which predate the per-subtree map.
ENV_FLAG = SUBTREE_FLAGS["chat"]

HERE = Path(__file__).parent


def _requested(flag: str) -> bool:
    return os.environ.get(flag) == "1"


def live_proofs_requested() -> bool:
    """True when ANY live subtree has been opted into.

    Used for the directory itself, which belongs to no single subtree.
    """
    return any(_requested(flag) for flag in SUBTREE_FLAGS.values())


def subtree_requested(relative: Path) -> bool:
    """Whether the subtree owning ``relative`` has been opted into."""
    if not relative.parts:
        return live_proofs_requested()
    flag = SUBTREE_FLAGS.get(relative.parts[0])
    if flag is None:
        # An unlisted subtree. Withheld rather than collected -- see the note
        # on SUBTREE_FLAGS.
        return False
    return _requested(flag)


def pytest_ignore_collect(collection_path: Path, config: object) -> bool | None:
    """Ignore everything under ``tests/live`` that was not asked for."""
    if collection_path == HERE:
        return None if live_proofs_requested() else True
    try:
        relative = collection_path.relative_to(HERE)
    except ValueError:
        return None
    return None if subtree_requested(relative) else True
