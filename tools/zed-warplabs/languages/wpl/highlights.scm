; WPL Syntax Highlighting Queries for Zed
; In Zed the LAST matching pattern wins.

; ── Plain identifiers (lowest priority — must be FIRST) ──
(identifier) @variable

; ── Keywords ──
[
  "package"
  "rule"
  "plg_pipe"
] @keyword

; ── Group meta keywords ──
[
  "alt"
  "opt"
  "some_of"
  "seq"
  "not"
] @keyword.modifier

; ── Annotation keywords ──
[
  "tag"
  "copy_raw"
] @attribute

; ── Operators / punctuation ──
"*" @operator
"|" @operator
"@" @punctuation.special
":" @punctuation.delimiter

; ── Brackets ──
[ "(" ")" "{" "}" "[" "]" ] @punctuation.bracket
[ "<" ">" ] @punctuation.bracket

; ── Delimiters ──
[ "," ] @punctuation.delimiter

; ── String literals ──
(quoted_string) @string
(raw_string) @string

; ── Number literals ──
(number) @number

; ── Escape characters in separators ──
(escape_char) @string.escape

; ── Scope format <...> ──
(scope_format) @string.special

; ── Quote format " ──
(quote_format) @string.special

; ── Pattern separator {...} ──
(pattern_sep) @string.special

; ── Annotation start #[ ──
(annotation_start) @attribute

; ── Package name ──
(package_decl name: (path_name) @type.definition)

; ── Rule name ──
(rule_decl name: (path_name) @function.definition)

; ── Type names ──
; Generic type names
(type_name (identifier) @type)

; Namespace types (e.g. http/request, time/clf)
(ns_type
  namespace: (identifier) @type
  "/" @punctuation.delimiter
  name: (identifier) @type)

; Array type
(array_type
  "array" @type.builtin
  element: (identifier) @type)

; ── Builtin type names (LAST = wins over generic @type) ──
(type_name (identifier) @type.builtin (#eq? @type.builtin "auto"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "bool"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "chars"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "symbol"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "peek_symbol"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "digit"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "float"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "_"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "sn"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "time"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "time_iso"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "time_3339"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "time_2822"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "time_timestamp"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "ip"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "ip_net"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "domain"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "email"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "port"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "hex"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "base64"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "kv"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "kvarr"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "json"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "exact_json"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "url"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "proto_text"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "obj"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "id_card"))
(type_name (identifier) @type.builtin (#eq? @type.builtin "mobile_phone"))

; ── Variable binding (:name) ──
(field binding: (var_name) @variable)
(subfield binding: (var_name) @variable)

; ── Subfield @ref ──
(subfield ref: (ref_path) @variable.special)

; ── Preprocessor ──
(preproc_path
  ns: (identifier) @function.builtin
  name: (identifier) @function.builtin)

; ── Function calls in pipes ──
(fun_call function: (identifier) @function.call)

; ── Built-in function calls (LAST = wins) ──
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "take"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "last"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "has"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "f_has"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "f_chars_has"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "f_chars_not_has"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "f_chars_in"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "f_digit_has"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "f_digit_in"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "f_ip_in"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "chars_has"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "chars_not_has"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "chars_in"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "starts_with"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "regex_match"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "digit_has"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "digit_in"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "digit_range"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "ip_in"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "json_unescape"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "base64_decode"))
(fun_call function: (identifier) @function.builtin (#eq? @function.builtin "chars_replace"))
; not() wrapper function
(fun_call function: "not" @function.builtin)

; ── Annotation tag key ──
(tag_kv key: (identifier) @property)

; ── plg_pipe key ──
(plg_pipe_block key: (key) @string.special)

; ── Repeat prefix number ──
(repeat_prefix (number) @number)
