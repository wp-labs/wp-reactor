; WS (Window Schema) Syntax Highlighting Queries for Zed
; Uses fine-grained capture names for richer theme integration.

; ── Plain identifiers (lowest priority — must be FIRST so later patterns override) ──
(identifier) @variable

; ── Definition keyword ──
"window" @keyword.function

; ── Attribute keywords ──
[
  "stream"
  "time"
  "over"
  "fields"
] @keyword

; ── Array type keyword ──
"array" @keyword

; ── Built-in type names ──
(base_type) @type.builtin

; ── Window name (definition site) ──
(window_declaration name: (identifier) @type.definition)

; ── Field names in fields block ──
(field_name (identifier) @property)
(field_name (dotted_identifier) @property)
(field_name (quoted_identifier) @string.special)

; ── Time field reference ──
(time_attribute (identifier) @property)

; ── Array type slash ──
(array_type "/" @punctuation.delimiter)

; ── Brackets ──
[ "{" "}" "[" "]" ] @punctuation.bracket

; ── Delimiters ──
[ "," ":" "=" ] @punctuation.delimiter

; ── Dot in dotted identifiers ──
(dotted_identifier "." @punctuation.delimiter)

; ── String literals ──
(string) @string

; ── Duration literals ──
(duration) @number

; ── Zero literal in over = 0 ──
(over_attribute "0" @number)

; ── Comments ──
(comment) @comment

; ── Quoted identifiers ──
(quoted_identifier) @string.special
