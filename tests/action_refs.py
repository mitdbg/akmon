"""Assert WHICH GitHub action a workflow step runs, never WHICH RELEASE of it.

A CI-shaped test that spells out `actions/upload-artifact@v4` is a self-DoS.
The weekly `github-actions` Dependabot group -- one `patterns: ["*"]` group over
`/`, targeting dev (see `.github/dependabot.yml` and
`tests/ci/test_dependabot_config.py`) -- rewrites every marketplace ref in this
repository, so a pinned major turns routine dependency maintenance into a red
test. When that test also runs in workflow-lint, the root of the needs graph,
the bump reds every downstream lane and the aggregate `test-results-gate`.

It has already happened: #630 bumped checkout v6 -> v7 and download-artifact
v4 -> v8 and left `tests/ci/test_docs_site_publish.py` asserting the old majors.
It went unnoticed only because that file ran in no CI step at all (#861).

Version-agnostic is not ref-agnostic. A branch ref (`@main`) would let the
action's code change under a job that holds deploy credentials, with no commit
in this repository, so the ref must still have the shape of a release: a tag
like `v4` or `v4.1.7`, or a full commit SHA.

Be precise about what that buys. Only the 40-character SHA form is genuinely
immutable. A major tag such as `v4` is a moving alias that its publisher
re-points on every release -- that is exactly why Dependabot bumps only on
major. So this check excludes BRANCH refs and enforces the release-tag shape;
it does not, on its own, freeze the action's code. Pinning by SHA is what does
that, and this helper accepts a SHA wherever a workflow chooses to use one.
"""

from __future__ import annotations

import re


# A release tag (`v4`, `v4.1.7`) or a full 40-character commit SHA. Anything
# else -- `@main`, `@master`, another branch, an abbreviated SHA -- is a
# mutable ref that this repository must not run an action from.
RELEASE_REF = re.compile(r"v\d+(?:\.\d+)*|[0-9a-f]{40}")


def action_identity(uses: str) -> str:
    """Return the `owner/repo` a `uses:` value names, after checking its ref.

    Compare the result against the exact owner/repo you intend to run: a
    look-alike such as `actions-upload/upload-artifact` is a different
    publisher and must not satisfy the assertion.
    """
    name, separator, ref = uses.partition("@")
    assert name, f"{uses!r} must name an action"
    assert separator and ref, f"{uses!r} must pin a ref"
    assert RELEASE_REF.fullmatch(ref), (
        f"{uses!r} must pin a release tag or commit SHA, not the mutable ref {ref!r}"
    )
    return name
