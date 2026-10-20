"""TreeSitter JavaScript fixture."""
from __future__ import annotations

import textwrap

import pytest

pytest.importorskip("tree_sitter_language_pack")

from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterSymbolExtractor,
)


_JS = textwrap.dedent('''
    import { foo } from 'bar';
    const baz = require('baz');

    function alpha(x) {
      beta();
      return x + 1;
    }

    class Greeter {
      constructor(name) {
        this.name = name;
      }
      hello() {
        console.log(`Hi ${this.name}`);
      }
    }

    const arrow = (x) => x * 2;
''').strip() + "\n"


def test_javascript_extracts_function_class_methods() -> None:
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(_JS, path="x.js")
    qnames = {s.qualified_name for s in rep.symbols}
    # alpha (function), Greeter (class), Greeter.constructor,
    # Greeter.hello (methods), arrow (function).
    assert "alpha" in qnames
    assert "Greeter" in qnames
    assert "Greeter.constructor" in qnames
    assert "Greeter.hello" in qnames
    assert "arrow" in qnames
    # Method kinds.
    by_qname = {s.qualified_name: s for s in rep.symbols}
    assert by_qname["Greeter.constructor"].kind == "method"
    assert by_qname["Greeter.hello"].kind == "method"
    assert by_qname["alpha"].kind == "function"
    assert by_qname["Greeter"].kind == "class"


def test_javascript_does_not_duplicate_function_for_nested_arrow_call() -> None:
    source = textwrap.dedent('''
        function initInspector() {
          function renderAttrs(node) {
            const attrs = node.attrs || {};
            const entries = Object.entries(attrs).slice(0, 40);
            if (!entries.length) return `<div>none</div>`;
            return entries
              .map(([k, v]) => {
                const vStr = typeof v === "object" ? JSON.stringify(v) : String(v);
                return `<div>${k}</div><div>${vStr}</div>`;
              })
              .join("");
          }
          return renderAttrs;
        }
    ''').strip() + "\n"
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(source, path="inspector.js")
    qnames = [s.qualified_name for s in rep.symbols]

    assert qnames.count("initInspector.renderAttrs") == 1


def test_javascript_extracts_imports() -> None:
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(_JS, path="x.js")
    # ES `import` + CommonJS `require()` both captured.
    assert len(rep.imports) >= 2
    named = {
        (imp.module, imp.name, imp.alias, imp.kind)
        for imp in rep.imports
    }
    assert ("bar", "foo", "", "from_import") in named
    assert ("baz", "", "", "import") in named


def test_javascript_extracts_calls() -> None:
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(_JS, path="x.js")
    callee_names = [r.callee_name for r in rep.references]
    # `beta()` inside alpha; `console.log` inside Greeter.hello.
    assert any("beta" in c for c in callee_names)


def test_javascript_normalizes_optional_chain_callees() -> None:
    source = textwrap.dedent('''
        function render(state) {
          state.ops?.setVisibleOps?.(["startup"]);
        }
    ''').strip() + "\n"
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(source, path="view.js")

    ref = next(
        ref for ref in rep.references
        if ref.callee_name == "state.ops.setVisibleOps"
    )

    assert ref.resolution_kind == "within_file_optional_chain"
    assert "?." not in ref.callee_name


def test_javascript_does_not_treat_optional_template_data_as_blank_callee() -> None:
    source = textwrap.dedent('''
        function render(gen) {
          return [
            card("Generation", `${gen.published?.generation_id ?? ""}`),
          ].join("");
        }
    ''').strip() + "\n"
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(source, path="system.js")

    assert all(ref.callee_name for ref in rep.references)
    assert any(ref.callee_name == "join" for ref in rep.references)


def test_javascript_extracts_callback_identifier_references() -> None:
    source = textwrap.dedent('''
        function isSemanticNode(node) { return !!node; }
        function entrySort(a, b) { return 0; }
        function closeReader() {}
        function renderBreadcrumb(nodes, onClose) {}
        function init(nodes, button) {
          nodes.filter(isSemanticNode).sort(entrySort);
          button.addEventListener("click", closeReader);
          renderBreadcrumb(nodes, closeReader);
        }
    ''').strip() + "\n"
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(source, path="callbacks.js")
    callbacks = {
        ref.callee_name
        for ref in rep.references
        if ref.resolution_kind == "callback_argument"
    }

    assert {"isSemanticNode", "entrySort", "closeReader"} <= callbacks


def test_javascript_extracts_public_export_evidence() -> None:
    source = textwrap.dedent('''
        import fs, { readFile as read, writeFile } from "node:fs";
        function pinNodeAt() {}
        function unpinNode() {}
        export { pinNodeAt as pin, unpinNode } from "./layout.js";
        export function publicFn() {}
    ''').strip() + "\n"
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(source, path="public.js")

    imports = {
        (imp.module, imp.name, imp.alias, imp.kind)
        for imp in rep.imports
    }
    exports = {
        ref.callee_name
        for ref in rep.references
        if ref.resolution_kind == "public_export"
    }

    assert ("node:fs", "default", "fs", "import") in imports
    assert ("node:fs", "readFile", "read", "from_import") in imports
    assert ("node:fs", "writeFile", "", "from_import") in imports
    assert {"pinNodeAt", "unpinNode", "publicFn"} <= exports


def test_javascript_extracts_returned_object_surface_references() -> None:
    source = textwrap.dedent('''
        function makeController() {
          function currentLayoutIntent() {}
          function pinNodeAt() {}
          function unpinNode() {}
          return {
            resolveIntent: currentLayoutIntent,
            pinNodeAt,
            unpinNode,
          };
        }
    ''').strip() + "\n"
    rep = TreeSitterSymbolExtractor(
        language="javascript",
    ).extract(source, path="controller.js")
    returned = {
        ref.callee_name
        for ref in rep.references
        if ref.resolution_kind == "returned_object"
    }

    assert {"currentLayoutIntent", "pinNodeAt", "unpinNode"} <= returned
