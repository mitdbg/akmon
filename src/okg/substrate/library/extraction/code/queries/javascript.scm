; javascript.scm — tree-sitter captures for JavaScript.

; Named function declarations.
(function_declaration
  name: (identifier) @name.function) @definition.function

; Function expressions assigned to a const/let/var with an
; identifier-on-the-left land as the assignment's name.
(variable_declarator
  name: (identifier) @name.function
  value: [(function_expression) (arrow_function)]) @definition.function

; Async variants.
(generator_function_declaration
  name: (identifier) @name.function) @definition.function

; Classes + their methods.
(class_declaration
  name: (identifier) @name.class) @definition.class

(method_definition
  name: (property_identifier) @name.method) @definition.method

; Imports — ES module + CommonJS.
(import_statement) @import
(call_expression
  function: (identifier) @_require
  (#eq? @_require "require")) @import

; Calls.
(call_expression) @call
