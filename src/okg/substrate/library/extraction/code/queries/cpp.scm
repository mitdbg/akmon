; cpp.scm — tree-sitter captures for C++ (conservative).
;
; OUT OF SCOPE for this query file (per
; `add-treesitter-code-extractor` proposal): templates,
; preprocessor directives beyond #include, multiple
; inheritance, namespace-aware qname construction. The
; extractor captures the syntactic skeleton only; LSP is
; the answer for C++ semantic resolution.

; Free functions.
(function_definition
  declarator: (function_declarator
    declarator: (identifier) @name.function)) @definition.function

; Member functions defined inline in a class body — the
; declarator's identifier is a `field_identifier`.
(function_definition
  declarator: (function_declarator
    declarator: (field_identifier) @name.method)) @definition.method

; Out-of-class member function definitions:
;   void Foo::bar() { ... }
(function_definition
  declarator: (function_declarator
    declarator: (qualified_identifier
      name: (identifier) @name.method))) @definition.method

; Class / struct declarations.
(class_specifier
  name: (type_identifier) @name.class) @definition.class

(struct_specifier
  name: (type_identifier) @name.class) @definition.class

; Imports — preprocessor #include only (other directives
; stay out of scope).
(preproc_include) @import

; Function calls.
(call_expression) @call
