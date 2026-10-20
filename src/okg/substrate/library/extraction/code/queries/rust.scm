; rust.scm — tree-sitter captures for Rust.

; Functions.
(function_item
  name: (identifier) @name.function) @definition.function

; Structs / enums / traits — class-shaped for ontology.
(struct_item
  name: (type_identifier) @name.class) @definition.class

(enum_item
  name: (type_identifier) @name.class) @definition.class

(trait_item
  name: (type_identifier) @name.class) @definition.class

; impl blocks: every fn inside is a method.
(impl_item
  body: (declaration_list
          (function_item
            name: (identifier) @name.method) @definition.method))

; Imports: `use` statements.
(use_declaration) @import

; Calls — both function calls and macro invocations.
(call_expression) @call
(macro_invocation) @call
