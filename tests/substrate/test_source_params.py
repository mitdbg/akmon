"""One source-parameter check for lint, doctor and ingest (#2571 item 2).

Before this, a misspelled or missing key under a source's `params` passed
`okg deployment lint` and `okg doctor --check sources` and failed only when
the source ran (#2484 rows 1 and 2).
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from pathlib import Path

import pytest
import yaml

from okg.substrate.deployment_lint import (
    lint_deployment,
    source_activation_probe_findings,
)
from okg.substrate.ingest.source_params import (
    SOURCE_PARAM_MISSING,
    SOURCE_PARAM_RESERVED,
    SOURCE_PARAM_UNKNOWN,
    ConstructorParameters,
    check_source_params,
    static_constructor_parameters,
)


REPO = Path(__file__).resolve().parents[2]
DOC_CORPUS = "okg.substrate.library.sources.doc_corpus.DocCorpusSource"


class _Reader:
    profile = "discovery_crawl"

    def __init__(
        self, *, repo_root: str, exclude_globs: tuple = (),
        dsn: str | None = None, source_name: str,
    ) -> None:
        pass


class _KwargsReader:
    def __init__(self, *, repo_root: str, **kwargs) -> None:
        pass


def _problems(params, cls=_Reader):
    return check_source_params(params, ConstructorParameters.from_class(cls))


# ------------------------------------------------------------- the one check


def test_a_misspelled_key_names_the_source_key_hint_and_accepted_keys():
    (problem,) = _problems({"repo_root": "/r", "exclude_glob": ["x"]})
    assert problem.code == SOURCE_PARAM_UNKNOWN == "source_param_unconsumed"
    assert problem.key == "exclude_glob"
    assert problem.refused_by_legacy is True
    rendered = problem.render("repo_docs")
    assert rendered.startswith(
        "source_param_unconsumed: source 'repo_docs' at "
        "/sources/repo_docs/params/exclude_glob: params key 'exclude_glob' "
        "is not a parameter of "
    )
    assert "Did you mean 'exclude_globs'?" in rendered
    # Substrate-owned names are not offered to the author.
    assert rendered.endswith("Accepted keys: exclude_globs, repo_root.")


def test_a_missing_required_key_is_named():
    (problem,) = _problems({})
    assert problem.code == SOURCE_PARAM_MISSING
    assert problem.key == "repo_root"
    assert "params is missing 'repo_root'" in problem.message
    assert problem.message.endswith("Accepted keys: exclude_globs, repo_root.")


def test_required_substrate_owned_names_are_never_missing():
    # `source_name` is required but injected (or, for a protocol name, left
    # to the bind check): telling the author to add it would contradict the
    # reserved-key finding telling them to delete it.
    assert _problems({"repo_root": "/r"}) == ()


def test_a_reserved_key_is_reported_and_legacy_lets_it_through():
    (problem,) = _problems({"repo_root": "/r", "dsn": "postgresql://x/y"})
    assert problem.code == SOURCE_PARAM_RESERVED
    assert problem.refused_by_legacy is False


def test_kwargs_is_not_proof_but_legacy_absorbs_it():
    (problem,) = _problems({"repo_root": "/r", "extra": 1}, _KwargsReader)
    assert problem.code == SOURCE_PARAM_UNKNOWN
    assert problem.refused_by_legacy is False
    assert "**kwargs is not proof" in problem.message


def test_a_key_that_is_not_text_is_named():
    (problem,) = _problems({"repo_root": "/r", 1: "x"})
    assert problem.code == "source_param_key_invalid"
    assert problem.key == "1"
    assert problem.refused_by_legacy is True


def test_an_unread_constructor_reports_only_reserved_keys():
    problems = check_source_params({"typo": 1, "dsn": "x"}, None)
    assert [p.code for p in problems] == [SOURCE_PARAM_RESERVED]


# --------------------------------------------------------- static read


def _static(
    source: str, name: str = "R", *, bases_safe: bool = False,
) -> ConstructorParameters | None:
    return static_constructor_parameters(
        ast.parse(textwrap.dedent(source)), name, target=f"m.{name}",
        base_is_safe=(lambda base: True) if bases_safe else None,
    )


def _runtime(source: str, name: str = "R") -> ConstructorParameters:
    namespace: dict = {}
    # dont_inherit: this file's `from __future__ import annotations` would
    # turn the fixture's annotations into strings and hide KW_ONLY.
    code = compile(textwrap.dedent(source), "<fixture>", "exec", dont_inherit=True)
    exec(code, namespace)  # noqa: S102 - test fixture
    return ConstructorParameters.from_signature(
        inspect.signature(namespace[name]), target=f"m.{name}",
    )


AGREES = {
    "explicit_init": """
        class R:
            def __init__(self, a, b=1, *, c, d=None, **kw): ...
    """,
    "receiver_not_named_self": """
        class R:
            def __init__(this, *, root): ...
    """,
    "optional_positional_only": """
        class R:
            def __init__(self, a=1, /, b=2, *, c): ...
    """,
    "explicit_init_with_base": """
        class Base: ...
        class R(Base):
            def __init__(self, *, root): ...
    """,
    "plain_dataclass": """
        from dataclasses import dataclass, field, InitVar, KW_ONLY
        from typing import ClassVar
        @dataclass(frozen=True)
        class R:
            a: str
            b: int = 1
            c: list = field(default_factory=list)
            d: int = field(init=False, default=0)
            e: ClassVar[int] = 3
            f: InitVar[str] = ""
            _: KW_ONLY
            g: str = "x"
            h: str = field()
    """,
    "module_dataclass_decorator": """
        import dataclasses
        @dataclasses.dataclass
        class R:
            a: str
    """,
}


@pytest.mark.parametrize("case", sorted(AGREES))
def test_static_read_matches_the_runtime_signature(case):
    source = AGREES[case]
    static = _static(source, bases_safe=True)
    assert static is not None
    runtime = _runtime(source)
    assert static.named == runtime.named
    assert static.required == runtime.required
    assert static.accepts_var_keyword == runtime.accepts_var_keyword


UNKNOWN = {
    "inherited_constructor": """
        class Base:
            def __init__(self, *, root): ...
        class R(Base): ...
    """,
    "required_positional_only": """
        class R:
            def __init__(self, a, /, *, b): ...
    """,
    "defines_new": """
        class R:
            def __new__(cls, *, x): ...
            def __init__(self, *, y): ...
    """,
    "assigns_signature": """
        class R:
            __signature__ = None
            def __init__(self, *, y): ...
    """,
    "assigned_init": """
        def _init(self, *, y): ...
        class R:
            __init__ = _init
    """,
    "decorated_init": """
        import functools
        class R:
            @functools.wraps(print)
            def __init__(self, *, y): ...
    """,
    "overloaded_init": """
        from typing import overload
        class R:
            @overload
            def __init__(self, *, a: int): ...
            def __init__(self, **kw): ...
    """,
    "metaclass": """
        class M(type): ...
        class R(metaclass=M):
            def __init__(self, *, y): ...
    """,
    "rebound_after_definition": """
        class R:
            def __init__(self, *, y): ...
        R = print
    """,
    "defined_twice": """
        class R:
            def __init__(self, *, a): ...
        class R:
            def __init__(self, *, b): ...
    """,
    "conditional_definition": """
        try:
            from elsewhere import R
        except ImportError:
            class R:
                def __init__(self, *, y): ...
    """,
    "attrs_dataclass": """
        from attr import dataclass
        @dataclass
        class R:
            _private: str
    """,
    "dataclass_without_init": """
        from dataclasses import dataclass
        @dataclass(init=False)
        class R:
            a: str
    """,
    "dataclass_with_base": """
        from dataclasses import dataclass
        @dataclass
        class Base:
            a: str
        @dataclass
        class R(Base):
            b: str
    """,
    "dataclass_extra_decorator": """
        from dataclasses import dataclass
        def register(cls): return cls
        @register
        @dataclass
        class R:
            a: str
    """,
    "dataclass_conditional_field": """
        from dataclasses import dataclass
        import sys
        @dataclass
        class R:
            if sys.version_info > (3,):
                a: str
    """,
    "aliased_classvar": """
        from dataclasses import dataclass
        from typing import ClassVar as CV
        @dataclass
        class R:
            a: CV[int] = 1
    """,
    "decorated_class_with_init": """
        def register(cls): return cls
        @register
        class R:
            def __init__(self, *, y): ...
    """,
    "base_not_cleared": """
        class Base: ...
        class R(Base):
            def __init__(self, *, root): ...
    """,
    "dataclass_field_then_method": """
        from dataclasses import dataclass
        @dataclass
        class R:
            root: str
            def root(self): ...
    """,
    "dataclass_field_twice": """
        from dataclasses import dataclass
        @dataclass
        class R:
            root: str
            root = "x"
    """,
    "setattr_on_class": """
        class R:
            def __init__(self, *, y): ...
        def _init(self, **kw): ...
        setattr(R, "__init__", _init)
    """,
    "attribute_assignment_on_class": """
        class R:
            def __init__(self, *, y): ...
        R.__init__ = lambda self, **kw: None
    """,
    "rebound_by_match": """
        import sys
        class R:
            def __init__(self, *, y): ...
        match sys.argv:
            case [*R]:
                pass
    """,
    "rebound_by_walrus": """
        class R:
            def __init__(self, *, y): ...
        (R := print)
    """,
    "rebound_by_type_alias": """
        class R:
            def __init__(self, *, y): ...
        type R = int
    """,
    "shadowed_dataclass": """
        from dataclasses import dataclass
        def dataclass(cls): return cls
        @dataclass
        class R:
            root: str
    """,
    "deleted": """
        class R:
            def __init__(self, *, y): ...
        del R
    """,
}


@pytest.mark.parametrize("case", sorted(UNKNOWN))
def test_static_read_answers_unknown_when_it_could_be_wrong(case):
    assert _static(UNKNOWN[case]) is None


# ------------------------------------------------------------------ lint

_ENTRY = {
    "module": "okg.substrate.library.sources.doc_corpus",
    "class": "DocCorpusSource",
    "ownership_id": "t.repo_docs",
    "source_class": "discovery_crawl",
    "record_identity_kind": "scoped_locator",
    "record_identity_fields": ["repo", "path"],
    "source_revision_kind": "content_hash",
    "deletion_semantics": "missing_from_completed_scope",
    "publication_mode": "published_generation",
}
_GOOD_PARAMS = {"repo": "t", "repo_root": "/r", "extractors_dir": "/e"}


def _deployment(
    tmp_path: Path, sources: dict, *, strict: bool, name: str = "sp",
) -> Path:
    directory = tmp_path / name
    directory.mkdir()
    (directory / "deployment.yaml").write_text(
        f"deployment: {name}\nsource_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    document: dict = {"sources": sources}
    if strict:
        document["admission_contract"] = "strict_v1"
    (directory / "source_registry.yaml").write_text(
        yaml.safe_dump(document), encoding="utf-8",
    )
    return directory


def _param_findings(report: dict) -> list[dict]:
    return [
        finding for finding in report["findings"]
        if finding["code"] in {
            "deployment.source_registry.param_unknown",
            "deployment.source_registry.param_missing",
        }
    ]


@pytest.mark.parametrize("strict", [False, True])
def test_lint_blocks_the_2484_rows(tmp_path, strict):
    typo = {**_GOOD_PARAMS, "exclude_glob": ["x"]}
    missing = {k: v for k, v in _GOOD_PARAMS.items() if k != "repo_root"}
    directory = _deployment(tmp_path, {
        "typo": {**_ENTRY, "ownership_id": "t.typo", "params": typo},
        "missing": {**_ENTRY, "ownership_id": "t.missing", "params": missing},
        "good": {**_ENTRY, "ownership_id": "t.good", "params": _GOOD_PARAMS},
    }, strict=strict)

    report = lint_deployment(directory, live=False)

    assert report["ok"] is False
    by_source = {f["details"]["source"]: f for f in _param_findings(report)}
    assert set(by_source) == {"typo", "missing"}
    typo_finding = by_source["typo"]
    assert typo_finding["code"] == "deployment.source_registry.param_unknown"
    assert typo_finding["severity"] == "blocker"
    assert typo_finding["location"]["pointer"] == (
        "/sources/typo/params/exclude_glob"
    )
    assert typo_finding["message"].startswith(
        f"source 'typo': params key 'exclude_glob' is not a parameter of "
        f"{DOC_CORPUS}. Did you mean 'exclude_globs'? Accepted keys: "
    )
    missing_finding = by_source["missing"]
    assert missing_finding["code"] == "deployment.source_registry.param_missing"
    assert missing_finding["message"].startswith(
        f"source 'missing': params is missing 'repo_root', which "
        f"{DOC_CORPUS} requires."
    )


@pytest.mark.parametrize("strict", [False, True])
def test_lint_names_a_misspelled_entry_key(tmp_path, strict):
    """#2484 row 3: `publicaton_mode:` for `publication_mode:` was reported
    only as `profile_invalid ... publication_mode=None`; the typo appeared
    nowhere in lint's output, while ingest's loader names it."""
    from okg.substrate.sources.registry import load_source_registry

    entry = {**_ENTRY, "ownership_id": "t.typo", "params": _GOOD_PARAMS}
    entry["publicaton_mode"] = entry.pop("publication_mode")
    directory = _deployment(tmp_path, {
        "typo": entry,
        "good": {**_ENTRY, "ownership_id": "t.good", "params": _GOOD_PARAMS},
    }, strict=strict)

    report = lint_deployment(directory, live=False)

    assert report["ok"] is False
    unknown = [
        f for f in report["findings"]
        if f["code"] == "deployment.source_registry.entry_key_unknown"
    ]
    assert len(unknown) == 1, unknown
    (finding,) = unknown
    assert finding["severity"] == "blocker"
    assert finding["message"] == "source 'typo' has an unknown key 'publicaton_mode'"
    assert finding["location"]["pointer"] == "/sources/typo/publicaton_mode"
    assert finding["details"]["suggestion"] == "publication_mode"
    assert "rename 'publicaton_mode' to 'publication_mode'" in finding["next_action"]
    # Not a lint-only rule: the registry loader ingest uses refuses it too.
    with pytest.raises(ValueError, match="publicaton_mode"):
        load_source_registry(directory / "source_registry.yaml")


def test_lint_points_an_unknown_reader_setting_under_params(tmp_path):
    entry = {**_ENTRY, "ownership_id": "t.flat", "params": _GOOD_PARAMS,
             "zzzz_reader_setting": 1}
    directory = _deployment(tmp_path, {"flat": entry}, strict=True)
    (finding,) = [
        f for f in lint_deployment(directory, live=False)["findings"]
        if f["code"] == "deployment.source_registry.entry_key_unknown"
    ]
    assert finding["details"]["suggestion"] is None
    assert "params" in finding["next_action"]


@pytest.mark.parametrize("strict", [False, True])
def test_lint_treats_a_retired_schedule_block_as_the_loader_does(tmp_path, strict):
    """Legacy: accepted (the loader only warns). Strict: refused, and the
    advice is to delete it, never to move it under params."""
    entry = {**_ENTRY, "ownership_id": "t.sched", "params": _GOOD_PARAMS,
             "schedule": {"cron": "0 * * * *"}}
    directory = _deployment(tmp_path, {"sched": entry}, strict=strict)
    unknown = [
        f for f in lint_deployment(directory, live=False)["findings"]
        if f["code"] == "deployment.source_registry.entry_key_unknown"
    ]
    if not strict:
        assert unknown == []
        return
    (finding,) = unknown
    assert "delete the `schedule` block" in finding["next_action"]
    assert "sync.poll" in finding["next_action"]
    assert "params" not in finding["next_action"]


def test_lint_prints_the_message_ingest_raises(tmp_path):
    from okg.substrate.ingest.adapter_factory import source_adapter_init_params
    from okg.substrate.library.sources.doc_corpus import DocCorpusSource
    from okg.substrate.sources.registry import load_source_registry

    typo = {**_GOOD_PARAMS, "exclude_glob": ["x"]}
    directory = _deployment(
        tmp_path, {"typo": {**_ENTRY, "params": typo}}, strict=True,
    )
    (finding,) = _param_findings(lint_deployment(directory, live=False))
    entry = load_source_registry(directory / "source_registry.yaml")["typo"]
    with pytest.raises(ValueError) as refused:
        source_adapter_init_params(
            entry, dsn="postgresql://x/y", deployment="sp",
            adapter_class=DocCorpusSource,
        )
    lint_message = finding["message"].removeprefix("source 'typo': ")
    assert str(refused.value).endswith(lint_message)


def test_lint_reads_a_re_exported_reader(tmp_path):
    directory = _deployment(tmp_path, {}, strict=False)
    package = directory / "spsrc"
    package.mkdir()
    (package / "__init__.py").write_text(
        "from .impl import Reader\n", encoding="utf-8",
    )
    (package / "impl.py").write_text(textwrap.dedent("""\
        class Reader:
            profile = "discovery_crawl"
            change_probe_kind = "always_changed"
            def __init__(self, *, repo_root, dsn=None):
                self.repo_root = repo_root
    """), encoding="utf-8")
    registry = directory / "source_registry.yaml"
    registry.write_text(yaml.safe_dump({"sources": {"r": {
        **_ENTRY,
        "module": "spsrc",
        "class": "Reader",
        "params": {"repo_rot": "/r"},
    }}}), encoding="utf-8")

    codes = {
        (f["code"], f["details"]["param"])
        for f in _param_findings(lint_deployment(directory, live=False))
    }
    assert codes == {
        ("deployment.source_registry.param_unknown", "repo_rot"),
        ("deployment.source_registry.param_missing", "repo_root"),
    }


def test_lint_says_nothing_about_an_inherited_constructor(tmp_path):
    directory = _deployment(tmp_path, {}, strict=False)
    (directory / "spinherit.py").write_text(textwrap.dedent("""\
        class Base:
            def __init__(self, *, repo_root):
                self.repo_root = repo_root
        class Reader(Base):
            profile = "discovery_crawl"
    """), encoding="utf-8")
    (directory / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {"r": {
            **_ENTRY, "module": "spinherit", "class": "Reader",
            "params": {"anything": 1},
        }},
    }), encoding="utf-8")

    assert _param_findings(lint_deployment(directory, live=False)) == []



@pytest.mark.parametrize("strict", [False, True])
def test_a_key_kwargs_absorbs_warns_on_legacy_and_blocks_on_strict(
    tmp_path, strict,
):
    """okg-deployments intersystems-ops readers take `**params` and read
    `seed` from it: legacy runs them, strict refuses them."""
    directory = _deployment(tmp_path, {}, strict=strict)
    (directory / "spkwargs.py").write_text(textwrap.dedent("""\
        class Reader:
            profile = "discovery_crawl"
            change_probe_kind = "always_changed"
            def __init__(self, **params):
                self.params = params
    """), encoding="utf-8")
    document: dict = {"sources": {"r": {
        **_ENTRY, "module": "spkwargs", "class": "Reader",
        "params": {"seed": 7},
    }}}
    if strict:
        document["admission_contract"] = "strict_v1"
    (directory / "source_registry.yaml").write_text(
        yaml.safe_dump(document), encoding="utf-8",
    )

    (finding,) = _param_findings(lint_deployment(directory, live=False))
    assert finding["code"] == "deployment.source_registry.param_unknown"
    assert finding["severity"] == ("blocker" if strict else "warning")
    if not strict:
        assert "add-source" in finding["why"]



def test_lint_reports_missing_keys_when_params_is_absent(tmp_path):
    body = {k: v for k, v in _ENTRY.items()}
    directory = _deployment(tmp_path, {"bare": body}, strict=False)
    keys = {
        (f["code"], f["details"]["param"])
        for f in _param_findings(lint_deployment(directory, live=False))
    }
    assert keys == {
        ("deployment.source_registry.param_missing", "repo"),
        ("deployment.source_registry.param_missing", "repo_root"),
        ("deployment.source_registry.param_missing", "extractors_dir"),
    }


_BASE_CASES = {
    # (base module source, reader imports, expect a finding)
    "safe_local_base": ("", "class Base:\n    pass\n", True),
    "abc_base": ("", "from abc import ABC as Base\n", True),
    "imported_safe_base": (
        "class Base:\n    pass\n", "from spbase_{case} import Base\n", True,
    ),
    "metaclass_base": (
        "class Meta(type):\n    def __call__(cls, *a, **k): ...\n"
        "class Base(metaclass=Meta):\n    pass\n",
        "from spbase_{case} import Base\n",
        False,
    ),
    "signature_base": (
        "class Base:\n    __signature__ = None\n",
        "from spbase_{case} import Base\n",
        False,
    ),
    "init_subclass_base": (
        "class Base:\n    def __init_subclass__(cls, **kw): ...\n",
        "from spbase_{case} import Base\n",
        False,
    ),
    "unresolvable_base": ("", "from nowhere_at_all import Base\n", False),
    "base_mutated_later": (
        "import inspect\nclass Base:\n    pass\n"
        "setattr(Base, '__signature__', inspect.Signature())\n",
        "from spbase_{case} import Base\n",
        False,
    ),
}


@pytest.mark.parametrize("case", sorted(_BASE_CASES))
def test_lint_reads_an_explicit_init_only_under_cleared_bases(tmp_path, case):
    base_source, preamble, expect_finding = _BASE_CASES[case]
    directory = _deployment(tmp_path, {}, strict=False)
    if base_source:
        (directory / f"spbase_{case}.py").write_text(base_source, encoding="utf-8")
    (directory / f"spreader_{case}.py").write_text(
        preamble.format(case=case)
        + textwrap.dedent("""\
            class Reader(Base):
                profile = "discovery_crawl"
                change_probe_kind = "always_changed"
                def __init__(self, *, repo_root):
                    self.repo_root = repo_root
        """),
        encoding="utf-8",
    )
    (directory / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {"r": {
            **_ENTRY, "module": f"spreader_{case}", "class": "Reader",
            "params": {"repo_rot": "/r"},
        }},
    }), encoding="utf-8")

    findings = _param_findings(lint_deployment(directory, live=False))
    assert bool(findings) is expect_finding, findings


def test_activation_grades_a_legacy_kwargs_key_like_lint(tmp_path):
    directory = _deployment(tmp_path, {}, strict=False)
    (directory / "spactkw.py").write_text(textwrap.dedent("""\
        class Reader:
            profile = "discovery_crawl"
            change_probe_kind = "always_changed"
            def __init__(self, **params):
                self.params = params
    """), encoding="utf-8")
    body = {**_ENTRY, "module": "spactkw", "class": "Reader",
            "params": {"seed": 7}}
    (directory / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {"r": body}}), encoding="utf-8",
    )
    codes = {
        finding["code"] for finding in source_activation_probe_findings(
            "r", body, deployment_dir=directory,
            registry_path=directory / "source_registry.yaml",
        )
    }
    assert "deployment.source_registry.param_unknown" not in codes


def test_activation_refuses_a_misspelled_key(tmp_path):
    body = {**_ENTRY, "params": {**_GOOD_PARAMS, "exclude_glob": ["x"]}}
    directory = _deployment(tmp_path, {"typo": body}, strict=False)
    codes = {
        finding["code"] for finding in source_activation_probe_findings(
            "typo",
            body,
            deployment_dir=directory,
            registry_path=directory / "source_registry.yaml",
        )
    }
    assert "deployment.source_registry.param_unknown" in codes


# ------------------------------------------------ tracked registries


def _tracked_registries() -> list[Path]:
    import subprocess

    listed = subprocess.run(
        ["git", "ls-files", "*source_registry.yaml"],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    return [REPO / path for path in listed]


def test_no_tracked_registry_has_a_bad_param_key():
    """Every source registry in this repo passes the check (census on dev
    e61612ff2a found none; a new finding here is a real defect)."""
    from okg.substrate.deployment_lint import _check_source_strict_admission

    found = []
    for registry in _tracked_registries():
        document = yaml.safe_load(registry.read_text(encoding="utf-8")) or {}
        for name, body in (document.get("sources") or {}).items():
            if not isinstance(body, dict) or not body.get("module"):
                continue
            findings: list = []
            _check_source_strict_admission(
                name,
                body,
                source_class=None,
                module_name=str(body["module"]),
                class_name=str(body.get("class") or "") or None,
                deployment_dir=registry.parent,
                registry_path=registry,
                pointer=f"/sources/{name}",
                findings=findings,
            )
            found += [
                (str(registry.relative_to(REPO)), name, f.code)
                for f in findings
                if f.code.endswith(("param_unknown", "param_missing"))
            ]
    assert found == []


# ---------------------------------------------------- one code path


@pytest.mark.parametrize("path", [
    "src/okg/substrate/ingest/adapter_factory.py",
    "src/okg/substrate/deployment_lint.py",
    "src/okg/substrate/cli/doctor.py",
])
def test_each_caller_uses_the_one_check(path):
    text = (REPO / path).read_text(encoding="utf-8")
    assert "check_source_params(" in text
    # No caller keeps its own comparison of params with the constructor.
    assert "set(params) - named_parameters" not in text
    assert "if str(key) in RESERVED_ADAPTER_PARAM_NAMES" not in text
