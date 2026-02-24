
```toml
[alert]
sinks = ["file://alerts/wf-alerts.json"]
```

sinks 要引入 connectors 的体系。

wfs_parser 要补充错误信息，


 - Medium: limits 目前只实质执行了 max_cardinality，max_state / max_emit_rate 仅解析未执行（crates/wf-core/src/rule/match_engine/
    mod.rs:49, crates/wf-core/src/rule/match_engine/mod.rs:160）。
  - Medium: 规则层面对 limits 缺失不报错（直接跳过），与 v2.1“必填治理字段”不一致（crates/wf-lang/src/checker/rules/limits.rs:10）。
  - Medium: window.has() 的编译期语义检查不完整：未校验 qualifier window 是否存在、字段是否存在/类型匹配，导致错误规则可通过 lint，运
    行时才静默返回 false（crates/wf-lang/src/checker/types/check_expr.rs:151, crates/wf-lang/src/checker/types/check_funcs.rs:59）。
  - Medium: match<a.sip:...> 这类“限定 key”在多源异名字段场景下未被 checker 拦截；运行时提 key 时又会丢弃 alias 只按字段名取值，容易
    导致另一源事件被静默跳过（crates/wf-lang/src/checker/rules/keys.rs:33, crates/wf-core/src/rule/match_engine/key.rs:77, crates/wf-
    core/src/rule/match_engine/key.rs:84）。
