; WFL Syntax Highlighting Queries for Zed
; Fine-grained captures; in Zed the LAST matching pattern wins.

; ── Plain identifiers (lowest priority — must be FIRST so later patterns override) ──
(identifier) @variable

; ── Import keyword ──
"use" @keyword.import

; ── Definition keywords ──
[
  "rule"
  "contract"
] @keyword

; ── Control flow ──
[
  "if"
  "then"
  "else"
] @keyword.control

; ── Pipeline / structural keywords ──
[
  "meta"
  "events"
  "match"
  "join"
  "yield"
  "entity"
  "conv"
  "derive"
  "score"
  "on"
  "event"
  "close"
] @keyword

; ── Contract test keywords ──
[
  "given"
  "expect"
  "options"
  "for"
  "row"
  "tick"
  "hits"
  "hit"
] @keyword

; ── Keyword operators ──
[
  "in"
  "not"
] @keyword.operator

; ── Join modes ──
[
  "snapshot"
  "asof"
] @keyword.modifier

; ── Window spec keywords ──
[
  "tumble"
  "session"
] @keyword.modifier

; ── Boolean literals ──
(boolean) @constant.builtin

; ── Comparison operators ──
(comparison_operator) @operator

; ── Arithmetic / logical operators ──
[
  "+"
  "-"
  "*"
  "/"
  "%"
  "&&"
  "||"
] @operator

; ── Pipe operators ──
"|" @operator
"|>" @keyword.operator
"->" @keyword.operator

; ── Score weight @ sign ──
(score_item "@" @punctuation.special)

; ── Brackets ──
[ "(" ")" "{" "}" "[" "]" ] @punctuation.bracket
[ "<" ">" ] @punctuation.bracket

; ── Delimiters ──
[ "," ";" ":" ] @punctuation.delimiter

; ── Dot accessor ──
"." @punctuation.delimiter

; ── Comments ──
(comment) @comment

; ── String literals ──
(string) @string

; ── Number literals ──
(number) @number

; ── Duration literals ──
(duration) @number

; ── Variables ($VAR, ${VAR:default}) ──
(variable) @variable.special

; ── Derive references (@name) ──
(derive_reference) @variable.special

; ── close_reason ──
(close_reason_ref) @variable.builtin

; ── Rule definition name ──
(rule_declaration name: (identifier) @function.definition)

; ── Contract name + target rule ──
(contract_block name: (identifier) @function.definition)
(contract_block rule: (identifier) @function)

; ── Event alias and window type ──
(event_declaration
  alias: (identifier) @variable
  window: (identifier) @type)

; ── Join window type ──
(join_clause window: (identifier) @type)

; ── Yield target type ──
(yield_clause target: (identifier) @type)

; ── Entity type ──
(entity_clause type: (identifier) @type)
(entity_clause type: (string) @type)

; ── Transform keywords (distinct in pipe chain) ──
(transform) @function.builtin

; ── Measure keywords (count/sum/avg/min/max in pipe chain) ──
(measure) @function.builtin

; ── Generic function calls (must come BEFORE builtins so builtins override) ──
(function_call
  function: (identifier) @function.call)

; ── Method-style calls (e.g. window.has, bad_domains.has) ──
(function_call
  object: (identifier) @type
  method: (identifier) @function.method)

; ── Built-in function calls (LAST = wins over generic @function.call) ──
; L1 aggregation
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "count"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "sum"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "avg"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "min"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "max"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "distinct"))
; L1 formatting
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "fmt"))
; L2 baseline / scoring
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "baseline"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "has"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "hit"))
; L2 string functions
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "contains"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "regex_match"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "len"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "lower"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "upper"))
; L2 time functions
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "time_diff"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "time_bucket"))
; L3 collection functions
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "collect_set"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "collect_list"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "first"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "last"))
; L3 statistics
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "stddev"))
(function_call function: (identifier) @function.builtin (#eq? @function.builtin "percentile"))

; ── Method-style built-in calls (has, etc.) ──
(function_call
  object: (identifier) @type
  method: (identifier) @function.builtin
  (#eq? @function.builtin "has"))

; ── Field references: alias.field ──
(field_reference
  object: (identifier) @variable
  field: (identifier) @property)

; ── Named arguments in yield ──
(named_argument name: (identifier) @property)

; ── Meta entry keys ──
(meta_entry key: (identifier) @property)

; ── Score item name ──
(score_item name: (identifier) @property)

; ── Score item weight ──
(score_item weight: (number) @number)

; ── Derive item name ──
(derive_item name: (identifier) @property)

; ── Conv operations ──
[
  "sort"
  "top"
  "dedup"
  "where"
] @function.builtin

; ── Option keys & values ──
(option_entry key: (identifier) @property)
(option_entry value: (identifier) @constant)

; ── Field assignment in given ──
(field_assignment field: (identifier) @property)
(field_assignment field: (string) @property)

; ── Expect hit assertions ──
(hit_assertion "score" @property)
(hit_assertion "close_reason" @property)
(hit_assertion "entity_type" @property)
(hit_assertion "entity_id" @property)
(hit_assertion "field" @function.builtin)

; ── Match params: key fields ──
(match_params (field_reference
  object: (identifier) @variable
  field: (identifier) @property))

; ── Source expression in step branches ──
(source_expression
  source: (identifier) @variable)
(source_expression
  source: (identifier) @variable
  field: (identifier) @property)

; ── Aggregate pipe source ──
(aggregate_pipe_expression
  source: (identifier) @variable)
(aggregate_pipe_expression
  source: (identifier) @variable
  field: (identifier) @property)
