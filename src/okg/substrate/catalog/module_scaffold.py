"""Scaffolder for `okg new-module`.

`new_module` produces the minimal-but-valid module skeleton: a
`module.yaml` (name + 0.1.0 version + empty `provides`), a
`schema.yaml` carrying the LinkML preamble + an empty `classes:`
mapping, and a `tests/` directory with a smoke test that calls
the manifest loader.

The source-skeleton scaffolder behind `okg new-source` was removed with
issue #2479: the skeleton it wrote was not a working reader. Authors start
from `examples/json-record-connector` instead.

The composition resolver in `composition.py` accepts an empty
`classes:` map (verified: `_extract_module_subtypes` returns an
empty set when no archetype-annotated classes exist). So we keep
the validator strict and ship a truly empty `schema.yaml`.
"""
from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from okg.substrate.errors import SubstrateError


__all__ = [
    "ScaffoldedModule",
    "scaffold_module",
]


@dataclass(frozen=True)
class ScaffoldedModule:
    name: str
    module_dir: Path
    files_written: list[Path] = field(default_factory=list)


def scaffold_module(
    name: str,
    *,
    module_dir: Path,
    package_path: str,
    force: bool = False,
) -> ScaffoldedModule:
    """Create `module_dir` and the minimal scaffold for a new
    ontology module named `name`.

    `package_path` is the importable package path for the module
    (e.g. `okg.substrate.library.ontologies.wet_lab` for in-tree
    or `<deployment>.modules.<name>` for deployment-private).
    Used in the scaffolded test so `pytest <module_dir>/tests`
    works without import-path gymnastics.

    Refuses to overwrite an existing `module_dir` unless `force`.
    """
    if module_dir.exists() and any(module_dir.iterdir()):
        if not force:
            raise SubstrateError(
                f"module directory {module_dir} already exists "
                f"(and is non-empty); new-module refuses to "
                f"overwrite without --force",
                data={"module_dir": str(module_dir), "name": name},
                fix_hint="pass --force to scaffold on top of the existing dir",
            )

    module_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    # module.yaml — required name + version + empty provides.
    manifest_text = yaml.safe_dump(
        {
            "name": name,
            "version": "0.1.0",
            "description": f"Scaffolded by `okg new-module {name}`",
            "provides": {
                "subtypes_file": "schema.yaml",
            },
        },
        sort_keys=False,
    )
    manifest_path = module_dir / "module.yaml"
    manifest_path.write_text(manifest_text, encoding="utf-8")
    written.append(manifest_path)

    # schema.yaml — minimal LinkML preamble + empty classes map.
    # The composition resolver accepts this (verified:
    # `_extract_module_subtypes` skips files with no
    # archetype-annotated classes); operators add real subtypes
    # as they design the module.
    schema_text = textwrap.dedent(
        f"""\
        id: https://palimpzest.mit.edu/okg/modules/{name}
        name: {name}
        title: "{name} (scaffolded)"
        description: |-
          Scaffolded by `okg new-module {name}`. Replace this
          description and populate `classes:` with your domain
          subtypes (each must carry an `annotations.archetype`
          tag to be recognized as a node subtype).

        prefixes:
          linkml: https://w3id.org/linkml/
          okg: https://palimpzest.mit.edu/okg/

        default_range: string

        imports:
          - linkml:types

        classes: {{}}
        """,
    )
    schema_path = module_dir / "schema.yaml"
    schema_path.write_text(schema_text, encoding="utf-8")
    written.append(schema_path)

    # __init__.py — empty package marker.
    init_path = module_dir / "__init__.py"
    init_path.write_text("", encoding="utf-8")
    written.append(init_path)

    # tests/ — package marker + smoke test asserting the manifest
    # loads cleanly via the public substrate API.
    tests_dir = module_dir / "tests"
    tests_dir.mkdir(exist_ok=True)
    tests_init = tests_dir / "__init__.py"
    tests_init.write_text("", encoding="utf-8")
    written.append(tests_init)

    test_schema_text = textwrap.dedent(
        f"""\
        \"\"\"Smoke test for the {name!r} module.

        Scaffolded by `okg new-module {name}`. Verifies the
        `module.yaml` manifest parses through the substrate's
        public loader (catches typos / missing required keys
        early in CI). Extend this with real assertions as the
        module grows.
        \"\"\"
        from pathlib import Path

        from okg.substrate.catalog.module_manifest import load_manifest


        def test_{name.replace('-', '_')}_module_manifest_loads() -> None:
            manifest_path = Path(__file__).resolve().parents[1] / \"module.yaml\"
            manifest = load_manifest(manifest_path)
            assert manifest.name == {name!r}
            assert manifest.version == \"0.1.0\"
        """,
    )
    test_path = tests_dir / "test_schema.py"
    test_path.write_text(test_schema_text, encoding="utf-8")
    written.append(test_path)

    # README.md — author-facing next-steps template.
    readme_text = textwrap.dedent(
        f"""\
        # `{name}` module

        Scaffolded by `okg new-module {name}`.

        ## Next steps

        1. **Declare subtypes in `schema.yaml`.** Each subtype is a
           LinkML class with `annotations.archetype: <name>` (one of
           `artifact`, `event`, `agent`, …). Without the annotation
           the composition resolver ignores the class.

        2. **Declare cross-link rules.** Add intra-module rules under
           `module.yaml#provides.intra_cross_link_rules`. Add
           cross-module composition under `module.yaml#extends.<target>.cross_link_rules`.

        3. **Author source readers.** Copy the working connector in
           `examples/json-record-connector/` (see its README), then add
           it with `okg deployment add-source <deployment> <name>
           --registration <dir>/connector.json`.

        4. **Validate.** Run `pytest <module_dir>/tests/` and (when
           a deployment loads this module) `okg doctor --deployment <dep>`.

        ## File layout

        ```
        {name}/
            module.yaml        # manifest (name, version, provides, extends)
            schema.yaml        # LinkML subtypes
            __init__.py        # package marker
            tests/             # smoke + integration tests
                test_schema.py
        ```
        """,
    )
    readme_path = module_dir / "README.md"
    readme_path.write_text(readme_text, encoding="utf-8")
    written.append(readme_path)

    return ScaffoldedModule(
        name=name,
        module_dir=module_dir,
        files_written=written,
    )
