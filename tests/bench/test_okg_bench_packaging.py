"""Packaged-artifact witnesses (PACT ship-okg-bench, task.ship.package).

Verification is against the BUILT wheel, not the editable checkout: build
the wheel, install it into a clean venv WITHOUT the [bench] extra, and
prove (a) `import okg.bench` / `import okg.bench.injection` succeed bare,
(b) an extra-needing surface (okg.bench.plot - matplotlib is the
genuinely extra-gated dep; jinja2 rides in via litellm) raises the named
okg[bench] install hint, (c) the agent-memory task specs ship as
package data. Marked slow: builds a wheel and resolves core deps.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=600, **kwargs)


@pytest.mark.slow
def test_wheel_ships_okg_bench(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    build = _run(
        ["uv", "build", "--wheel", "--out-dir", str(dist)], cwd=_REPO_ROOT
    )
    assert build.returncode == 0, build.stderr[-2000:]
    wheels = list(dist.glob("okg-*.whl"))
    assert len(wheels) == 1, wheels

    venv_dir = tmp_path / "venv"
    assert _run(["uv", "venv", str(venv_dir)]).returncode == 0
    python = venv_dir / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    install = _run(
        ["uv", "pip", "install", "--python", str(python), str(wheels[0])]
    )
    assert install.returncode == 0, install.stderr[-2000:]

    bare = _run([
        str(python), "-c",
        "import okg.bench, okg.bench.injection; print('bare-import-ok')",
    ])
    assert bare.returncode == 0, bare.stderr[-2000:]
    assert "bare-import-ok" in bare.stdout

    # jinja2 arrives transitively via core litellm, so the orchestrator
    # imports bare; matplotlib is the genuinely extra-gated dependency.
    hinted = _run([str(python), "-c", "import okg.bench.plot"])
    assert hinted.returncode != 0
    assert "okg[bench]" in hinted.stderr, hinted.stderr[-2000:]

    package_probe = "".join(
        [
            "from okg.bench.agent_memory import binder; ",
            "from okg.bench import per_source; from pathlib import Path; ",
            "am = Path(binder.__file__).parent; ",
            "assert (am / 'tasks.yaml').is_file(); ",
            "assert (am / 'profiles' / 'okg-ci.yaml').is_file(), 'profiles missing'; ",
            "ps = Path(per_source.__file__).parent; ",
            "assert list((ps / 'templates').glob('*.j2')), 'j2 templates missing'; ",
            "print('package-data-ok')",
        ]
    )
    data = _run([str(python), "-c", package_probe])
    assert data.returncode == 0, data.stderr[-2000:]
    assert "package-data-ok" in data.stdout
