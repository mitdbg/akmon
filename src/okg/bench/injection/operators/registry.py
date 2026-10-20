"""YAML ``module`` + ``class`` + ``params`` operator loader.

Mirrors the substrate's registry idiom (extraction registry / source
loader): importlib resolution, constructor params from config, and
refuse-to-boot on any failure — an unknown module, class, or param is a
loud :class:`InjectionError`, never a silent stand-in.

Config shape::

    operators:
      - module: okg.bench.injection.operators.library
        class: NumericValueOperator
        params: {predicates: [memory]}
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, Mapping

import yaml

from okg.bench.injection.operator_base import MutationOperator
from okg.bench.injection.types import InjectionError


def _instantiate(entry: Mapping[str, Any], index: int) -> MutationOperator:
    module_name = entry.get("module")
    class_name = entry.get("class")
    params = entry.get("params", {})
    if not isinstance(module_name, str) or not module_name:
        raise InjectionError(f"operators[{index}]: missing or non-string 'module'")
    if not isinstance(class_name, str) or not class_name:
        raise InjectionError(f"operators[{index}]: missing or non-string 'class'")
    if not isinstance(params, Mapping):
        raise InjectionError(f"operators[{index}]: 'params' must be a mapping")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise InjectionError(f"operators[{index}]: cannot import {module_name!r}: {exc}") from exc
    try:
        operator_type = getattr(module, class_name)
    except AttributeError as exc:
        raise InjectionError(
            f"operators[{index}]: module {module_name!r} has no class {class_name!r}"
        ) from exc
    try:
        operator = operator_type(**{str(key): value for key, value in params.items()})
    except TypeError as exc:
        raise InjectionError(
            f"operators[{index}]: {class_name}(**params) failed: {exc}"
        ) from exc
    for attribute in ("operator_id", "version"):
        value = getattr(operator, attribute, "")
        if not isinstance(value, str) or not value:
            raise InjectionError(
                f"operators[{index}]: {class_name} has no non-empty {attribute!r}"
            )
    for method in ("sites", "plan"):
        if not callable(getattr(operator, method, None)):
            raise InjectionError(f"operators[{index}]: {class_name} lacks callable {method!r}")
    return operator


def load_operators(config: Mapping[str, Any]) -> tuple[MutationOperator, ...]:
    """Instantiate every operator declared in a registry mapping."""
    entries = config.get("operators")
    if not isinstance(entries, list) or not entries:
        raise InjectionError("operator registry must declare a non-empty 'operators' list")
    return tuple(_instantiate(entry, index) for index, entry in enumerate(entries))


def load_operators_yaml(path: Path | str) -> tuple[MutationOperator, ...]:
    """Load an operator registry YAML file."""
    registry_path = Path(path)
    if not registry_path.is_file():
        raise InjectionError(f"operator registry {registry_path} does not exist")
    loaded = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, Mapping):
        raise InjectionError(f"operator registry {registry_path} is not a mapping")
    return load_operators(loaded)
