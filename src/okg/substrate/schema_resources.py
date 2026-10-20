"""Locate authored-file schemas in a source tree or an installed package."""

from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path


# Authored file name -> the schema `okg schema show <file>` prints for it.
# Bootstrap's validation hint reads the same map, so the command it suggests
# always exists.
AUTHORED_FILE_SCHEMAS: dict[str, str] = {
    "deployment.yaml": "deployment.schema.json",
    "bootstrap_plan.yaml": "bootstrap_plan.schema.json",
    "source_registry.yaml": "source_registry.schema.json",
    "linker_config.yaml": "linker_config.schema.json",
    "aliases.json": "aliases.schema.json",
    "invariants.yaml": "invariants.schema.json",
    "classes.yaml": "classes.schema.json",
    "narrowings.yaml": "narrowings.schema.json",
    "plan.yaml": "plan.schema.json",
    # A source manifest is named by its author (`tickets.yaml`); this is the
    # name `okg schema show` takes for its schema.
    "source_manifest.yaml": "source_manifest.schema.json",
}


def authored_file_for_schema(schema_name: str) -> str:
    """The file name `okg schema show` takes to print ``schema_name``."""

    for authored, schema in AUTHORED_FILE_SCHEMAS.items():
        if schema == schema_name:
            return authored
    raise KeyError(f"no authored file prints schema {schema_name!r}")


def schemas_dir() -> Traversable:
    """Return the schema authority belonging to this imported OKG.

    A source checkout uses its canonical schemas/ directory. A wheel uses
    okg/_schemas, populated at build time from that same directory. Returning
    a Traversable also supports zip resources without temporary extraction.
    The operator's working directory never participates in this choice.
    """
    module = Path(__file__).resolve()
    root = module.parents[3]
    if (
        module == root / "src" / "okg" / "substrate" / "schema_resources.py"
        and (root / "pyproject.toml").is_file()
    ):
        directory = root / "schemas"
    else:
        directory = files("okg").joinpath("_schemas")
    if not directory.is_dir():
        raise FileNotFoundError(
            f"OKG authored-file schema resources missing: {directory}"
        )
    return directory
