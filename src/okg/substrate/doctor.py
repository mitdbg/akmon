"""Doctor-check registration and deployment loading.

The substrate owns generic checks. Deployments own deployment-specific
policy checks and expose them through ``deployment.yaml``:

    doctor_checks:
      - name: coding-context
        handler: doctor_checks:doctor_okg_workspace

The deployment directory is injected into ``sys.path`` before import, so
handlers can live next to the deployment manifest.
"""
from __future__ import annotations

import argparse
import importlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError


class DoctorCheckLoadError(SubstrateError):
    code = "doctor_check_load_error"


class DoctorEmitter(Protocol):
    def __call__(self, payload: dict[str, Any], *, as_json: bool) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class DoctorContext:
    """Substrate services passed to deployment-owned doctor checks."""

    resolve_dsn: Callable[[argparse.Namespace], str]
    emit_ok: DoctorEmitter


class DoctorHandler(Protocol):
    def __call__(
        self,
        args: argparse.Namespace,
        *,
        context: DoctorContext,
        as_json: bool,
    ) -> int:
        raise NotImplementedError


@dataclass(frozen=True)
class DoctorCheck:
    name: str
    handler: DoctorHandler
    help: str = ""


class DoctorCheckRegistry:
    """Name-to-callable registry for ``okg doctor --check``."""

    def __init__(self) -> None:
        self._checks: dict[str, DoctorCheck] = {}

    def register(
        self,
        name: str,
        handler: DoctorHandler,
        *,
        help: str = "",
    ) -> None:
        if not name:
            raise ValueError("doctor check name must be non-empty")
        self._checks[name] = DoctorCheck(name=name, handler=handler, help=help)

    def register_check(self, check: DoctorCheck) -> None:
        self.register(check.name, check.handler, help=check.help)

    def extend(self, checks: Iterable[DoctorCheck]) -> None:
        for check in checks:
            self.register_check(check)

    def names(self) -> list[str]:
        return sorted(self._checks)

    def get(self, name: str) -> DoctorCheck | None:
        return self._checks.get(name)

    def dispatch(
        self,
        name: str,
        args: argparse.Namespace,
        *,
        context: DoctorContext,
        as_json: bool,
    ) -> int:
        check = self.get(name)
        if check is None:
            raise KeyError(name)
        return check.handler(args, context=context, as_json=as_json)


def load_deployment_doctor_checks(
    deployment: str | None,
) -> list[DoctorCheck]:
    """Load ``deployment.yaml``'s optional ``doctor_checks`` block."""
    if not deployment:
        return []

    from okg.substrate.deployments import (
        find_deployment_dir,
        inject_deployment_path,
    )

    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return []
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return []

    with manifest_path.open() as f:
        manifest = fastyaml.safe_load(f) or {}
    specs = manifest.get("doctor_checks") or []
    if not specs:
        return []
    if not isinstance(specs, list):
        raise DoctorCheckLoadError(
            f"{manifest_path}: 'doctor_checks' must be a list",
            data={"path": str(manifest_path)},
        )

    inject_deployment_path(deployment_dir)
    checks: list[DoctorCheck] = []
    for spec in specs:
        if not isinstance(spec, dict):
            raise DoctorCheckLoadError(
                f"{manifest_path}: doctor check entry must be a mapping",
                data={"path": str(manifest_path), "entry": spec},
            )
        name = spec.get("name")
        handler_path = spec.get("handler")
        if not name or not isinstance(name, str):
            raise DoctorCheckLoadError(
                f"{manifest_path}: doctor check entry needs a string name",
                data={"path": str(manifest_path), "entry": spec},
            )
        if not handler_path or not isinstance(handler_path, str):
            raise DoctorCheckLoadError(
                f"{manifest_path}: doctor check {name!r} needs handler",
                data={"path": str(manifest_path), "entry": spec},
            )
        handler = _resolve_handler(handler_path, deployment_dir)
        checks.append(
            DoctorCheck(
                name=name,
                handler=handler,
                help=str(spec.get("help") or ""),
            )
        )
    return checks


def _resolve_handler(handler_path: str, deployment_dir: Path) -> DoctorHandler:
    if ":" in handler_path:
        module_path, attr_name = handler_path.split(":", 1)
    else:
        try:
            module_path, attr_name = handler_path.rsplit(".", 1)
        except ValueError as exc:
            raise DoctorCheckLoadError(
                "doctor check handler must use 'module:function' or "
                "'module.function' form",
                data={"handler": handler_path},
            ) from exc

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise DoctorCheckLoadError(
            f"could not import doctor check module {module_path!r}",
            data={
                "handler": handler_path,
                "deployment_dir": str(deployment_dir),
                "import_error": str(exc),
            },
            fix_hint=(
                "place the module next to deployment.yaml or use a fully "
                "qualified import path"
            ),
        ) from exc

    try:
        handler = getattr(module, attr_name)
    except AttributeError as exc:
        raise DoctorCheckLoadError(
            f"doctor check module {module_path!r} has no {attr_name!r}",
            data={"handler": handler_path},
        ) from exc

    if hasattr(handler, "run"):
        handler = handler.run
    if not callable(handler):
        raise DoctorCheckLoadError(
            f"doctor check handler {handler_path!r} is not callable",
            data={"handler": handler_path},
        )
    return handler
