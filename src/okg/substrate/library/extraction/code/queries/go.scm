; go.scm — tree-sitter captures for Go.

; Top-level functions.
(function_declaration
  name: (identifier) @name.function) @definition.function

; Methods (have a receiver).
(method_declaration
  name: (field_identifier) @name.method) @definition.method

; Type definitions — interfaces / structs treated as
; "class"-shaped for ontology consistency.
(type_declaration
  (type_spec
    name: (type_identifier) @name.class)) @definition.class

; Imports — single + grouped.
(import_declaration) @import

; Calls.
(call_expression) @call
