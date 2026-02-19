; OML Syntax Highlighting Queries for Zed
; In Zed the LAST matching pattern wins.

; ── Plain identifiers (lowest priority — must be FIRST so later patterns override) ──
(identifier) @variable

; ── Header keywords ──
[
  "name"
  "rule"
] @keyword

; ── Read/Take keywords ──
[
  "read"
  "take"
] @function.builtin

; ── Structural keywords ──
[
  "pipe"
  "fmt"
  "object"
  "collect"
  "match"
] @keyword

; ── SQL keywords ──
[
  "select"
  "from"
  "where"
  "and"
  "or"
  "not"
] @keyword

; ── Condition keywords ──
"in" @keyword.operator

; ── Data types ──
(data_type) @type.builtin

; ── Privacy types ──
(privacy_type) @type.builtin

; ── Built-in function calls (Now::*) ──
[
  "Now::time"
  "Now::date"
  "Now::hour"
] @function.builtin

; ── Pipe functions with arguments ──
[
  "nth"
  "get"
  "base64_decode"
  "path"
  "url"
  "Time::to_ts_zone"
  "starts_with"
  "map_to"
] @function.builtin

; ── Pipe functions without arguments ──
[
  "base64_encode"
  "html_escape"
  "html_unescape"
  "str_escape"
  "str_unescape"
  "json_escape"
  "json_unescape"
  "Time::to_ts"
  "Time::to_ts_ms"
  "Time::to_ts_us"
  "to_json"
  "to_str"
  "skip_empty"
  "ip4_to_int"
  "extract_main_word"
  "extract_subject_object"
] @function.builtin

; ── Arrow operator ──
"=>" @keyword.operator

; ── Pipe operator ──
"|" @operator

; ── Not operator ──
"!" @operator

; ── SQL comparison operators ──
(sql_op) @operator

; ── Separator line ──
(separator) @punctuation.special

; ── @ref variable references ──
(at_ref "@" @punctuation.special)
(at_ref (identifier) @variable.special)

; ── Underscore wildcard ──
"_" @variable.builtin

; ── Boolean literals ──
(boolean) @constant.builtin

; ── Brackets ──
[ "(" ")" "{" "}" "[" "]" ] @punctuation.bracket

; ── Delimiters ──
[ "," ";" ":" "=" ] @punctuation.delimiter

; ── String literals ──
(string) @string

; ── Number literals ──
(number) @number

; ── IP literals ──
(ip_literal) @number

; ── Comments ──
(comment) @comment

; ── JSON paths ──
(json_path) @string.special

; ── Paths ──
(path) @string.special

; ── Wild keys (target names with *) ──
(wild_key) @property

; ── Header name value ──
(name_field name: (identifier) @type.definition)
(name_field name: (path) @type.definition)

; ── Rule field paths ──
(rule_field (path) @string.special)
(rule_field (identifier) @string.special)

; ── Target names (assignment LHS) ──
(target_name (identifier) @property)

; ── Privacy item field names ──
(privacy_item name: (identifier) @property)

; ── Object map target names ──
(map_targets (identifier) @property)

; ── SQL function calls ──
(sql_fun_call (identifier) @function.call)

; ── SQL column names ──
(sql_columns (identifier) @property)

; ── SQL table name ──
(sql_expr "from" (identifier) @type)

; ── Argument keywords ──
[
  "option"
  "keys"
] @keyword

; ── Get argument keyword ──
(get_arg "get" @keyword)

; ── Pipe function argument keywords ──
(pipe_fun "name" @string.special)
(pipe_fun "path" @string.special)
(pipe_fun "domain" @string.special)
(pipe_fun "host" @string.special)
(pipe_fun "uri" @string.special)
(pipe_fun "params" @string.special)
(pipe_fun "ms" @string.special)
(pipe_fun "us" @string.special)
(pipe_fun "ss" @string.special)
(pipe_fun "s" @string.special)
