"""Dev-loop reset for the DBOS runtime.

Per `sdk-design.md` §2.6 the SDK ships an ``okg runtime reset
--deployment X --confirm`` command and a pytest fixture for clean
DBOS state between test runs. The reset uses the documented
``DBOS.destroy()`` → ``DBOS.reset_system_database()`` cycle rather
than rolling our own truncates.

Refused outside of development environments per Decision and the
spec: a manifest field ``runtime.environment: production`` (or
similar non-``development`` value) blocks the destructive operation.
The absence of an explicit ``environment`` field is treated as
development (so the smoke test on okg-workspace dev works). The CLI
reset handler resolves ``environment`` from ``manifest.runtime.
environment`` (#682), so a production manifest fails closed here.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .config import RuntimeConfig
from .engine import build_dbos_config, release_connection_demand


__all__ = [
    "DevResetRefused",
    "DevResetResult",
    "reset_dbos",
]


log = logging.getLogger(__name__)


class DevResetRefused(RuntimeError):
    """Raised when reset is requested without ``--confirm`` or against
    a non-development deployment."""


@dataclass(frozen=True)
class DevResetResult:
    deployment: str
    cleared: bool
    schema_name: str


def reset_dbos(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
    confirm: bool,
    environment: str = "development",
) -> DevResetResult:
    """Reset the deployment's DBOS state.

    Sequence:
      1. Refuse without ``confirm=True``.
      2. Refuse if ``environment`` is not ``"development"``.
      3. ``DBOS.destroy()`` (defensive).
      4. Construct DBOS singleton against ``dsn``.
      5. ``DBOS.reset_system_database()``.
      6. ``DBOS.destroy()``.

    Returns :class:`DevResetResult` recording the outcome. Idempotent
    on a clean DBOS schema (no rows to clear).
    """
    if not confirm:
        raise DevResetRefused(
            "okg runtime reset requires --confirm; this command "
            "destroys all DBOS workflow state for the deployment."
        )
    if environment != "development":
        raise DevResetRefused(
            f"okg runtime reset is refused for environment="
            f"{environment!r}; only development is allowed. Set "
            "the manifest's runtime.environment field if you have "
            "a non-development deployment."
        )

    from dbos import DBOS

    log.info(
        "reset_dbos: deployment=%s schema=%s environment=%s",
        deployment, runtime.dbos_schema, environment,
    )

    # Step 3: defensive destroy.
    from okg.substrate.runtime.engine import defensive_destroy
    defensive_destroy()

    # Steps 4–6: construct, reset, destroy.
    try:
        cfg = build_dbos_config(
            deployment=deployment, dsn=dsn, runtime=runtime,
        )
        DBOS(config=cfg)
        DBOS.reset_system_database()
    finally:
        try:
            DBOS.destroy()
        finally:
            release_connection_demand(deployment)

    log.info(
        "reset_dbos: deployment=%s complete", deployment,
    )
    return DevResetResult(
        deployment=deployment,
        cleared=True,
        schema_name=runtime.dbos_schema,
    )
