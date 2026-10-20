; python.scm — tree-sitter captures for Python.
; Used for AST-parity tests; the canonical Python backend
; is `python_ast.PythonAstSymbolExtractor` (stdlib).

(function_definition
  name: (identifier) @name.function) @definition.function

(class_definition
  name: (identifier) @name.class) @definition.class

(import_statement) @import
(import_from_statement) @import

(call) @call
