"""Generic substrate tests must not depend on CMS deployment fixtures."""
from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SUBSTRATE_TEST_ROOT = REPO_ROOT / "tests" / "substrate"

FORBIDDEN_SNIPPETS = (
    'find_deployment_dir("cms"',
    "deployments/cms",
    "cms_sources",
    "CERN_JIRA_TOKEN",
    "JIRA_CERN_TOKEN",
    "CERN_CMSWEB",
    "CMSWEB_COOKIE_FILE",
    "CMS_HTTP_GROUP_DOCS_COOKIE_FILE",
)


def test_substrate_tests_do_not_import_cms_fixtures() -> None:
    leaks: list[tuple[Path, int, str]] = []
    self_path = Path(__file__).resolve()

    for path in sorted(SUBSTRATE_TEST_ROOT.rglob("*")):
        if path == self_path or path.suffix not in {".py", ".md", ".yaml"}:
            continue
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            for snippet in FORBIDDEN_SNIPPETS:
                if snippet in line:
                    leaks.append((path.relative_to(REPO_ROOT), lineno, snippet))

    if leaks:
        lines = [
            f"  {path}:{lineno} contains {snippet!r}"
            for path, lineno, snippet in leaks
        ]
        raise AssertionError(
            "CMS deployment fixtures belong under tests/cms, not "
            "tests/substrate:\n" + "\n".join(lines)
        )
