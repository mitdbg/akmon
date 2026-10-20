; c.scm — tree-sitter captures for C (conservative).
;
; Captures functions, struct declarations, includes, and call
; expressions. Type aliases, macros, and preprocessor-heavy
; semantics are intentionally left to richer analyzer backends.

(function_definition
  declarator: (function_declarator
    declarator: (identifier) @name.function)) @definition.function

(struct_specifier
  name: (type_identifier) @name.class) @definition.class

(preproc_include) @import

(call_expression) @call
