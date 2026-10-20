from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from okg.viz.context import VizContext


def test_explicit_generation_context_does_not_repin_on_refresh() -> None:
    session = SimpleNamespace(generation_id="checkpoint:G0")
    ctx = VizContext(
        dsn="postgresql://unused",
        deployment_name="demo",
        manifest={},
        config={},
        pool=None,  # type: ignore[arg-type]
        session=session,  # type: ignore[arg-type]
        deployment_dir=Path("deployments/demo"),
        generation_refresh_enabled=False,
    )

    assert ctx.refresh_to_latest() == "checkpoint:G0"
    assert ctx.session is session
