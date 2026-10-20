"""TreeSitter TypeScript fixture."""
from __future__ import annotations

import textwrap

import pytest

pytest.importorskip("tree_sitter_language_pack")

from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterSymbolExtractor,
)


_TS = textwrap.dedent('''
    import { Logger } from './logger';

    interface Greetable {
      hello(): void;
    }

    class Service implements Greetable {
      constructor(private logger: Logger) {}
      hello(): void {
        this.logger.info("hi");
      }
    }

    function build(name: string): Service {
      return new Service(new Logger());
    }
''').strip() + "\n"


def test_typescript_extracts_class_and_method() -> None:
    rep = TreeSitterSymbolExtractor(
        language="typescript",
    ).extract(_TS, path="x.ts")
    qnames = {s.qualified_name for s in rep.symbols}
    assert "Service" in qnames
    assert "Service.hello" in qnames
    assert "build" in qnames


def test_typescript_extracts_imports() -> None:
    rep = TreeSitterSymbolExtractor(
        language="typescript",
    ).extract(_TS, path="x.ts")
    assert len(rep.imports) >= 1
