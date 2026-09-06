//! 内置函数分派（eval/funcs.rs 拆分）：`eval_func_call` 经 `eval_handler` 名称表分派到
//! 各族的 `pub(super)` handler——`funcs_str`（字符串/mv）、`funcs_num`（数值）、
//! `funcs_misc`（fmt/concat/join/空值合并）、`funcs_time`（哈希/时间/网络/聚合）。

use super::super::types::{EngineHashMap, FieldSource, RollingStats, Value, WindowLookup};
use wf_lang::ast::Expr;

use super::{funcs_misc::*, funcs_num::*, funcs_str::*, funcs_time::*};

pub(super) fn eval_func_call(
    name: &str,
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let handler = eval_handler(name)?;
    handler(args, event, windows, baselines)
}
type FuncHandler = fn(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value>;
fn eval_handler(name: &str) -> Option<FuncHandler> {
    use std::collections::HashMap;
    use std::sync::OnceLock;
    static HANDLERS: OnceLock<HashMap<&'static str, FuncHandler>> = OnceLock::new();
    let map = HANDLERS.get_or_init(|| {
        let mut map: HashMap<&'static str, FuncHandler> = HashMap::new();
        map.insert("count_char", eval_func_count_char);
        map.insert("contains", eval_func_contains);
        map.insert("startswith", eval_func_startswith);
        map.insert("endswith", eval_func_endswith);
        map.insert("substr", eval_func_substr);
        map.insert("replace", eval_func_replace);
        map.insert("trim", eval_func_trim);
        map.insert("lower", eval_func_lower);
        map.insert("upper", eval_func_upper);
        map.insert("len", eval_func_len);
        map.insert("mvcount", eval_func_mvcount);
        map.insert("mvjoin", eval_func_mvjoin);
        map.insert("mvindex", eval_func_mvindex);
        map.insert("mvappend", eval_func_mvappend);
        map.insert("split", eval_func_split);
        map.insert("mvdedup", eval_func_mvdedup);
        map.insert("abs", eval_func_abs);
        map.insert("round", eval_func_round);
        map.insert("ceil", eval_func_ceil);
        map.insert("floor", eval_func_floor);
        map.insert("sqrt", eval_func_sqrt);
        map.insert("pow", eval_func_pow);
        map.insert("log", eval_func_log);
        map.insert("exp", eval_func_exp);
        map.insert("clamp", eval_func_clamp);
        map.insert("sign", eval_func_sign);
        map.insert("trunc", eval_func_trunc);
        map.insert("is_finite", eval_func_is_finite);
        map.insert("ltrim", eval_func_ltrim);
        map.insert("rtrim", eval_func_rtrim);
        map.insert("fmt", eval_func_fmt);
        map.insert("concat", eval_func_concat);
        map.insert("join", eval_func_join);
        map.insert("join_by", eval_func_join_by);
        map.insert("indexof", eval_func_indexof);
        map.insert("replace_plain", eval_func_replace_plain);
        map.insert("startswith_any", eval_func_startswith_any);
        map.insert("endswith_any", eval_func_endswith_any);
        map.insert("coalesce", eval_func_coalesce);
        map.insert("merge", eval_func_merge);
        map.insert("isnull", eval_func_isnull);
        map.insert("isnotnull", eval_func_isnotnull);
        map.insert("is_blank", eval_func_is_blank);
        map.insert("null_if_blank", eval_func_null_if_blank);
        map.insert("default_if_blank", eval_func_default_if_blank);
        map.insert("md5", eval_func_md5);
        map.insert("sha1", eval_func_sha1);
        map.insert("sha1_n", eval_func_sha1_n);
        map.insert("sha256", eval_func_sha256);
        map.insert("hex", eval_func_hex);
        map.insert("stable_id", eval_func_stable_id);
        map.insert("mvsort", eval_func_mvsort);
        map.insert("mvreverse", eval_func_mvreverse);
        map.insert("now", eval_func_now);
        map.insert("now_ms", eval_func_now);
        map.insert("now_s", eval_func_now_s);
        map.insert("now_us", eval_func_now_us);
        map.insert("now_ns", eval_func_now_ns);
        map.insert("strftime", eval_func_strftime);
        map.insert("strptime", eval_func_strptime);
        map.insert("regex_match", eval_func_regex_match);
        map.insert("cidr_match", eval_func_cidr_match);
        map.insert("time_diff", eval_func_time_diff);
        map.insert("time_bucket", eval_func_time_bucket);
        map.insert("bucket_end", eval_func_bucket_end);
        map.insert("collect_set", eval_func_collect_set);
        map.insert("collect_list", eval_func_collect_set);
        map.insert("first", eval_func_collect_set);
        map.insert("last", eval_func_collect_set);
        map.insert("stddev", eval_func_stddev);
        map.insert("percentile", eval_func_stddev);
        map.insert("external", eval_func_external);
        map
    });
    map.get(name).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_table_covers_families_and_rejects_unknown() {
        // 分派表覆盖四个族 + 回退校验（funcs.rs 拆分后表仍完整）
        assert!(eval_handler("substr").is_some(), "str 族");
        assert!(eval_handler("mvindex").is_some(), "mv 族");
        assert!(eval_handler("round").is_some(), "num 族");
        assert!(eval_handler("merge").is_some(), "misc 族");
        assert!(eval_handler("stable_id").is_some(), "hash 族");
        assert!(eval_handler("strftime").is_some(), "time 族");
        assert!(eval_handler("external").is_some(), "外部分派");
        assert!(eval_handler("no_such_func").is_none(), "未知名拒绝");
    }
}
