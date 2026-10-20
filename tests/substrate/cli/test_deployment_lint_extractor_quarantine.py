"""Local extractor quarantine contracts are inspected without importing code."""
import socket

import psycopg
import pytest

import okg.substrate.deployment_lint as lint

BASE = "okg.substrate.library.enrichers.extraction.base"
CLAIM = "okg.substrate.library.enrichers.extraction.claim_extractor"
MISSING = "deployment.enrichers.input_quarantine_limit_missing"
UNKNOWN = "deployment.enrichers.extractor_ancestry_unknown"


@pytest.fixture(autouse=True)
def deny_external_io(monkeypatch):
    attempts = []

    def deny(*args, **kwargs):
        attempts.append("forbidden I/O")
        raise AssertionError("static lint attempted external I/O")

    monkeypatch.setattr(socket, "getaddrinfo", deny)
    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket.socket, "connect_ex", deny)
    monkeypatch.setattr(psycopg, "connect", deny)
    monkeypatch.setattr(psycopg.Connection, "connect", deny)
    yield
    assert attempts == []


def diagnose(tmp_path, files, *, class_path="local.Extractor", config=None):
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    findings = []
    lint._check_enrichers(
        {"enrichers": [{"class": class_path, "config": config or {"llm_model": "fixture"}}]},
        deployment_dir=tmp_path, manifest_path=tmp_path / "deployment.yaml",
        findings=findings,
    )
    return [finding.code for finding in findings]


@pytest.mark.parametrize("source", [
    f"from {BASE} import LLMExtractor\nclass Extractor(LLMExtractor): pass\n",
    f"from {CLAIM} import ClaimExtractor\nclass Extractor(ClaimExtractor): pass\n",
    f"from {BASE} import LLMExtractor as B\nclass Extractor(B): pass\n",
    f"import {BASE} as b\nclass Extractor(b.LLMExtractor): pass\n",
    f"import {BASE}\nclass Extractor({BASE}.LLMExtractor): pass\n",
    f"from {BASE.rsplit('.', 1)[0]} import base\nclass Extractor(base.LLMExtractor): pass\n",
    f"from {BASE} import LLMExtractor\nclass Parent(LLMExtractor): pass\nclass Extractor(Parent): pass\n",
    f"from {BASE} import LLMExtractor\nclass Other: pass\nclass Extractor(Other, LLMExtractor): pass\n",
    f"from {BASE} import LLMExtractor\nclass Extractor(unknown(), LLMExtractor): pass\n",
    f"from {BASE} import LLMExtractor\nclass Extractor(LLMExtractor):\n def __init__(self): super().__init__()\n",
])
def test_recognizes_static_ancestry(tmp_path, source):
    assert diagnose(tmp_path, {"local.py": source}) == [MISSING]


@pytest.mark.parametrize("model_key", ["llm_model", "llm_canary"])
@pytest.mark.parametrize("limit", [None, False, True, 0, -1, "2", 2])
def test_explicit_positive_integer_contract(tmp_path, model_key, limit):
    config = {model_key: "fixture"}
    if limit is not None:
        config["input_quarantine_max_attempts"] = limit
    codes = diagnose(tmp_path, {"local.py": f"from {BASE} import LLMExtractor\nclass Extractor(LLMExtractor): pass\n"}, config=config)
    assert codes == ([] if limit == 2 and type(limit) is int else [MISSING])


def test_relative_import_and_package_reexport(tmp_path):
    files = {
        "local/__init__.py": "from .child import Extractor\n",
        "local/child.py": "from .parent import Parent as P\nclass Extractor(P): pass\n",
        "local/parent.py": f"from {BASE} import LLMExtractor\nclass Parent(LLMExtractor): pass\n",
    }
    assert diagnose(tmp_path, files) == [MISSING]


@pytest.mark.parametrize("source", [
    "class Extractor: pass\n",
    "class Extractor(object): pass\n",
    "class Parent: pass\nclass Extractor(Parent): pass\n",
])
def test_known_nonextractor_unchanged(tmp_path, source):
    assert diagnose(tmp_path, {"local.py": source}) == []


def test_nonmodel_unchanged(tmp_path):
    assert diagnose(tmp_path, {"local.py": "class Extractor(dynamic()): pass\n"}, config={"llm_model": None}) == []


@pytest.mark.parametrize("source", [
    "class Extractor(dynamic()): pass\n",
    "Extractor = factory()\n",
    f"from {BASE} import LLMExtractor as B\nB = Other\nclass Extractor(B): pass\n",
    f"from {BASE} import LLMExtractor as B\nfrom elsewhere import Other as B\nclass Extractor(B): pass\n",
    f"if condition:\n from {BASE} import LLMExtractor as B\nclass Extractor(B): pass\n",
    "class Extractor: pass\nclass Extractor: pass\n",
    "class Extractor: pass\nif condition:\n class Extractor: pass\n",
    f"from {BASE} import *\nclass Extractor(LLMExtractor): pass\n",
    "@decorate\nclass Extractor: pass\n",
    "class Extractor(metaclass=Meta): pass\n",
    "class A(B): pass\nclass B(A): pass\nclass Extractor(A): pass\n",
    "class object: pass\nobject = factory()\nclass Extractor(object): pass\n",
])
def test_ambiguous_ancestry_is_not_false_safe(tmp_path, source):
    assert diagnose(tmp_path, {"local.py": source}) == [UNKNOWN]


def test_reexport_cycle_terminates(tmp_path):
    assert diagnose(tmp_path, {"local.py": "from other import Extractor\n", "other.py": "from local import Extractor\n"}) == [UNKNOWN]


def test_depth_limit_terminates(tmp_path):
    source = "class C0: pass\n" + "".join(f"class C{i}(C{i-1}): pass\n" for i in range(1, 40)) + "class Extractor(C39): pass\n"
    assert diagnose(tmp_path, {"local.py": source}) == [UNKNOWN]


def test_hostile_module_and_package_are_not_executed(tmp_path):
    marker = tmp_path / "executed"
    hostile = f"from pathlib import Path\nPath({str(marker)!r}).write_text('executed')\nimport socket\nsocket.create_connection(('invalid.example', 1))\nraise RuntimeError('imported')\n"
    files = {"local/__init__.py": hostile + "from .child import Extractor\n", "local/child.py": hostile + f"from {BASE} import LLMExtractor\nclass Extractor(LLMExtractor): pass\n"}
    assert diagnose(tmp_path, files) == [MISSING]
    assert not marker.exists()


def test_canonical_name_shadow_is_not_trusted(tmp_path):
    files = {"okg/substrate/library/enrichers/extraction/base.py": "class LLMExtractor: pass\n", "local.py": f"from {BASE} import LLMExtractor\nclass Extractor(LLMExtractor): pass\n"}
    assert diagnose(tmp_path, files) == [UNKNOWN]


@pytest.mark.parametrize("class_path,files,expected", [
    ("absent.Extractor", {}, "deployment.enrichers.module_not_found"),
    ("local.Extractor", {"local.py": "class Other: pass\n"}, "deployment.enrichers.class_not_found"),
    ("local.Extractor", {"local.py": "invalid python!\n"}, "deployment.enrichers.module_source_unreadable"),
])
def test_existing_resolution_error_not_duplicated(tmp_path, class_path, files, expected):
    assert diagnose(tmp_path, files, class_path=class_path) == [expected]


def test_canonical_base_itself(tmp_path):
    assert diagnose(tmp_path, {}, class_path=BASE + ".LLMExtractor") == [MISSING]


@pytest.mark.parametrize("mutation", [
    "globals()['Extractor'] = LLMExtractor",
    "globals().update(Extractor=LLMExtractor)",
    "namespace = globals(); namespace['Extractor'] = LLMExtractor",
    "locals()['Extractor'] = LLMExtractor",
    "exec('Extractor = LLMExtractor')",
    "evaluate = eval; evaluate('globals().update(Extractor=LLMExtractor)')",
    "import sys; setattr(sys.modules[__name__], 'Extractor', LLMExtractor)",
    "import sys; sys.modules[__name__].__dict__.update(Extractor=LLMExtractor)",
    "import sys; vars(sys.modules[__name__])['Extractor'] = LLMExtractor",
    "class Mutator:\n global Extractor\n Extractor = LLMExtractor",
    "class Mutator:\n globals()['Extractor'] = LLMExtractor",
])
def test_namespace_rebinding_is_unknown(tmp_path, mutation):
    source = f"from {BASE} import LLMExtractor\nclass Extractor: pass\n{mutation}\n"
    assert diagnose(tmp_path, {"local.py": source}) == [UNKNOWN]


def test_diamond_ancestry_has_bounded_work(tmp_path, monkeypatch):
    calls = []
    original = lint._module_source_path

    def counted(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(lint, "_module_source_path", counted)
    source = "class A0: pass\nclass B0: pass\n"
    for i in range(1, 20):
        source += f"class A{i}(A{i-1}, B{i-1}): pass\nclass B{i}(A{i-1}, B{i-1}): pass\n"
    source += "class Extractor(A19, B19): pass\n"
    assert diagnose(tmp_path, {"local.py": source}) == []
    assert len(calls) < 100


@pytest.mark.parametrize("invocation", [
    "mutate()",
    "alias = mutate; alias()",
    "def trigger(value=mutate()): pass",
    "@mutate()\ndef trigger(): pass",
    "class Trigger:\n mutate()",
])
def test_local_namespace_mutator_is_not_false_safe(tmp_path, invocation):
    source = f"from {BASE} import LLMExtractor\nclass Extractor: pass\ndef mutate():\n global Extractor\n Extractor = LLMExtractor\n{invocation}\n"
    assert diagnose(tmp_path, {"local.py": source}) == [UNKNOWN]


def test_unused_ordinary_function_does_not_taint_ancestry(tmp_path):
    source = "def unused():\n raise RuntimeError('not executed')\nclass Extractor: pass\n"
    assert diagnose(tmp_path, {"local.py": source}) == []


@pytest.mark.parametrize("mutation", [
    "from builtins import globals as namespace; namespace()['Extractor'] = LLMExtractor",
    "from builtins import exec as execute; execute('Extractor = LLMExtractor')",
    "import builtins as b; b.globals()['Extractor'] = LLMExtractor",
    "from sys import modules as loaded; loaded[__name__].Extractor = LLMExtractor",
    "def mutate():\n Extractor.__bases__ = (LLMExtractor,)\nmutate()",
])
def test_namespace_import_aliases_and_base_mutation_are_unknown(tmp_path, mutation):
    source = f"from {BASE} import LLMExtractor\nclass Extractor: pass\n{mutation}\n"
    assert diagnose(tmp_path, {"local.py": source}) == [UNKNOWN]


@pytest.mark.parametrize("definition", [
    "def helper(arg=(Extractor := LLMExtractor)): pass",
    "class Mutator((Extractor := LLMExtractor)): pass",
    "@(Extractor := LLMExtractor)\ndef helper(): pass",
])
def test_definition_expression_rebinding_is_unknown(tmp_path, definition):
    source = f"from {BASE} import LLMExtractor\nclass Extractor: pass\n{definition}\n"
    assert diagnose(tmp_path, {"local.py": source}) == [UNKNOWN]


def test_match_mapping_binding_is_unknown(tmp_path):
    source = "class Extractor: pass\nmatch {}:\n case {**Extractor}: pass\n"
    assert diagnose(tmp_path, {"local.py": source}) == [UNKNOWN]
