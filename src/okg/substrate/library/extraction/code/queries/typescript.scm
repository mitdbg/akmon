; typescript.scm — tree-sitter captures for TypeScript.
; Mirrors the JavaScript shape; TypeScript-specific
; type-only declarations are intentionally NOT captured
; here (they belong on a future @type axis).

(function_declaration
  name: (identifier) @name.function) @definition.function

(variable_declarator
  name: (identifier) @name.function
  value: [(function_expression) (arrow_function)]) @definition.function

(generator_function_declaration
  name: (identifier) @name.function) @definition.function

(class_declaration
  name: (type_identifier) @name.class) @definition.class

(method_definition
  name: (property_identifier) @name.method) @definition.method

(import_statement) @import

(call_expression) @call
