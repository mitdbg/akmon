#!/usr/bin/env python3
"""Guard active docs against retired `okg` command and flag forms.

The audit found docs citing CLI verbs/flags that no longer exist (or never
did). A full argparse validator would false-positive on positional argument
values and on illustrative deployment-plan verbs, so this check is a precise
denylist of the exact retired forms we removed — it catches regressions
without guessing. Scope: the root README plus docs/**/*.md and the YAML
contracts.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
HISTORY_PREFIXES: tuple[str, ...] = ()

# (regex, human explanation of the correct form)
RETIRED_FORMS: list[tuple[str, str]] = [
    (r"--format\s+json", "use `--json` (there is no --format flag)"),
    (r"generation\s+list\s+--tail", "use `okg generation list --limit N`"),
    (r"metrics\s+--source-sync", "use `okg sync health` (source-sync metrics moved)"),
    (r"catalog\s+load\s+--dry-run", "a bare `okg catalog load` is the dry-run"),
    (r"catalog\s+load\s+--plan-out", "no plan-out flow; edit yaml then `--apply`"),
    (r"catalog\s+load\s+--reconcile", "no reconcile flow; edit yaml then `--apply`"),
    (r"catalog\s+reconcile", "there is no `okg catalog reconcile` verb"),
    (r"okg\s+add\s+gmail", "`okg add` does not expose gmail; see connector setup"),
    (r"consistency\s+rerun-post-publish",
     "use `okg consistency-rerun` (the nested alias was retired by "
     "consistency-single-engine)"),
]


def _active_doc_paths() -> list[Path]:
    paths = [ROOT / "README.md"]
    paths += sorted(DOCS.rglob("*.md"))
    paths += [DOCS / "reference" / "error_codes.yaml", DOCS / "reference" / "alerts.yaml"]
    out = []
    for p in paths:
        rel = p.relative_to(ROOT).as_posix()
        if rel.startswith(HISTORY_PREFIXES):
            continue
        if p.exists():
            out.append(p)
    return out


def run_checks() -> list[str]:
    errors: list[str] = []
    compiled = [(re.compile(rx), why) for rx, why in RETIRED_FORMS]
    for path in _active_doc_paths():
        rel = path.relative_to(ROOT).as_posix()
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for rx, why in compiled:
                if rx.search(line):
                    errors.append(f"retired command form in {rel}:{i}: {why}")
    return errors


def main() -> int:
    errors = run_checks()
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1
    print("doc commands: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
