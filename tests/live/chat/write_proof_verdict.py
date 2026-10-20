"""Record the result of the live proof — THE named positive writer.

``req.chat-context-audit-filter.observability-verdict`` names exactly one
writer of a positive verdict: the live equality-proof task, for the
(deployment, preset, openwebui_version) it actually proved. This is that
writer. The runtime filter reads the artifact and can never reach this file.

It refuses to write unless the arm's assertions pass in this same
invocation. A verdict written from a run whose equality never held would be
worse than no verdict: it is the permission slip that lets a preset run with
compaction on and an audit that proves nothing.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from okg.substrate.chat.observability_verdict import (  # noqa: E402
    Configuration,
    VerdictKey,
    read_verdicts,
    resolve_verdict_path,
    write_verdict,
)

ARM_TESTS = {
    "on": "test_audit_payload_equals_recorded_request_body",
    "off": "test_audit_payload_equals_request_body_compaction_disabled",
}
ARM_CONFIGURATIONS = {
    "on": Configuration.COMPACTION_ON,
    "off": Configuration.COMPACTION_OFF,
}


def _proof_passes(arm: str, on_root: Path, off_root: Path) -> bool:
    """Re-run the arm's assertion; the verdict is downstream of a green run."""
    test_file = Path(__file__).parent / "test_post_compaction_audit_equality.py"
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            f"{test_file}::{ARM_TESTS[arm]}",
            "-q",
        ],
        cwd=REPO_ROOT,
        env={
            **dict(__import__("os").environ),
            "OKG_CHAT_LIVE_PROOF": "1",
            "OKG_CHAT_LIVE_ON": str(on_root),
            "OKG_CHAT_LIVE_OFF": str(off_root),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    print(result.stdout[-4000:])
    print(result.stderr[-2000:], file=sys.stderr)
    return result.returncode == 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--on", type=Path, required=True, help="ON arm artifacts")
    parser.add_argument("--off", type=Path, required=True, help="OFF arm artifacts")
    parser.add_argument(
        "--state-home",
        type=Path,
        default=None,
        help="Operator-state root; defaults to the XDG rule.",
    )
    args = parser.parse_args()

    on_run = json.loads((args.on / "run.json").read_text("utf-8"))
    off_run = json.loads((args.off / "run.json").read_text("utf-8"))
    for name, run in (("on", on_run), ("off", off_run)):
        if run["openwebui_version"] != on_run["openwebui_version"]:
            print(
                f"the {name} arm ran at a different Open WebUI version; the "
                "verdict is keyed by version and cannot span two",
                file=sys.stderr,
            )
            return 2

    key = VerdictKey(
        deployment=on_run["deployment"],
        preset=on_run["preset"],
        openwebui_version=on_run["openwebui_version"],
    )
    path = resolve_verdict_path(key.deployment, state_home=args.state_home)

    written = []
    for arm in ("on", "off"):
        if not _proof_passes(arm, args.on, args.off):
            print(f"the {arm} arm did not pass; no verdict written for it")
            continue
        record = write_verdict(path, key, ARM_CONFIGURATIONS[arm])
        written.append(record.verdict.value)

    print(json.dumps({"verdict_path": str(path), "written": written}, indent=2))
    print(
        json.dumps(
            [record.as_dict() for record in read_verdicts(path)],
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if len(written) == 2 else 1


if __name__ == "__main__":
    raise SystemExit(main())
