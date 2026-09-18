//! eval 模块的 crate 级测试面补充（只改测试代码）。
//!
//! 目标：
//! - `match_engine/eval/funcs.rs`（纯 eval 路径）：`count_char` 与各类参数个数/类型错误分支；
//! - `match_engine/eval/cmp.rs`：数值/字符串/布尔比较边界、`try_eval_expr_to_f64` 的
//!   Neg / 算术 BinOp / 除零 / 非算术操作分支（经 `CepStateMachine` 阈值求值）；
//! - `executor/eval`（L3 路径）：经 `RuleExecutor::execute_match` 驱动 score 钳位与错误、
//!   entity_id 回退、yield 表达式中的系统变量 / L3 聚合 / 时间函数路由。
use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, Measure, ObjectItem};
use wf_lang::plan::{AggPlan, BranchPlan};

use crate::cep::{CepStateMachine, EngineHashMap, Event, StepResult, Value, eval_expr};

use super::helpers::{event, num, simple_key, simple_plan, step, str_val};

// ===========================================================================
// 纯路径辅助（与 tests/l2/expr.rs 一致）
// ===========================================================================

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

fn n(v: f64) -> Expr {
    Expr::Number(v)
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: name.to_string(),
        args,
    }
}

fn eval_ok(expr: &Expr) -> Value {
    let e = event(vec![]);
    eval_expr(expr, &e).unwrap_or_else(|| panic!("expected Some for {expr:?}"))
}

// ===========================================================================
// funcs.rs — count_char（纯 eval 路径，l2/expr.rs 未覆盖）
// ===========================================================================

#[test]
fn count_char_counts_occurrences() {
    assert_eq!(
        eval_ok(&call(
            "count_char",
            vec![str_lit("hello world"), str_lit("l")]
        )),
        num(3.0)
    );
    assert_eq!(
        eval_ok(&call("count_char", vec![str_lit("abc"), str_lit("z")])),
        num(0.0)
    );
    // 空 needle → 0（不 panic）
    assert_eq!(
        eval_ok(&call("count_char", vec![str_lit("abc"), str_lit("")])),
        num(0.0)
    );
    // 多字节按 char 计数
    assert_eq!(
        eval_ok(&call(
            "count_char",
            vec![str_lit("你好你好"), str_lit("你")]
        )),
        num(2.0)
    );
}

#[test]
fn count_char_error_branches() {
    let e = event(vec![("n", num(7.0))]);
    // 参数个数错误
    assert_eq!(eval_expr(&call("count_char", vec![str_lit("a")]), &e), None);
    assert_eq!(
        eval_expr(
            &call("count_char", vec![str_lit("a"), str_lit("b"), str_lit("c")]),
            &e
        ),
        None
    );
    // 非字符串参数
    assert_eq!(
        eval_expr(&call("count_char", vec![field("n"), str_lit("a")]), &e),
        None
    );
    assert_eq!(
        eval_expr(&call("count_char", vec![str_lit("a"), field("n")]), &e),
        None
    );
}

fn str_lit(s: &str) -> Expr {
    Expr::StringLit(s.to_string())
}

// ===========================================================================
// 第 2 步（精确整数 `Value::Int`）：数值域内置函数必须接受 `Int` 输入
// ===========================================================================

/// 构造侧接入 `Value::Int` 后，所有**消费数值**的内置函数必须能接受 `Int`：
/// 数值函数按 `i as f64` 归一（返回值仍 `Value::Float`，导出层整值归一为 Digit），
/// 而**整数索引/长度**走精确 `i64`（不经 f64）。
#[test]
fn numeric_builtins_accept_int_inputs() {
    let mut fields = EngineHashMap::default();
    fields.insert("n".into(), Value::Int(7));
    fields.insert("neg".into(), Value::Int(-3));
    fields.insert("s".into(), Value::Str("abcdef".into()));
    fields.insert("start".into(), Value::Int(3));
    fields.insert("idx".into(), Value::Int(2));
    fields.insert(
        "arr".into(),
        Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]),
    );
    let e = Event { fields };
    let ev = |expr: &Expr| eval_expr(expr, &e);

    // 一元数值函数：`Int` → f64 域计算
    assert_eq!(ev(&call("abs", vec![field("neg")])), Some(num(3.0)));
    assert_eq!(ev(&call("floor", vec![field("n")])), Some(num(7.0)));
    assert_eq!(ev(&call("ceil", vec![field("n")])), Some(num(7.0)));
    assert_eq!(ev(&call("round", vec![field("n")])), Some(num(7.0)));
    assert_eq!(ev(&call("trunc", vec![field("n")])), Some(num(7.0)));
    assert_eq!(ev(&call("sign", vec![field("neg")])), Some(num(-1.0)));
    assert_eq!(
        ev(&call("is_finite", vec![field("n")])),
        Some(Value::Bool(true))
    );
    // 二元数值函数：`Int` 与 `Float` 混合
    assert_eq!(ev(&call("pow", vec![field("n"), n(2.0)])), Some(num(49.0)));
    assert_eq!(
        ev(&call("clamp", vec![field("n"), n(0.0), n(5.0)])),
        Some(num(5.0))
    );

    // 整数索引/长度：走精确整数（`Int` 不经 f64 截断）
    assert_eq!(
        ev(&call(
            "substr",
            vec![field("s"), field("start"), field("idx")]
        )),
        Some(Value::Str("cd".into()))
    );
    assert_eq!(
        ev(&call("mvindex", vec![field("arr"), field("idx")])),
        Some(Value::Str("c".into()))
    );
}

// ===========================================================================
// funcs.rs — 纯 eval 路径的常规错误分支（参数个数 / 类型错误 → None）
// ===========================================================================

#[test]
fn plain_path_error_branches_return_none() {
    let mut fields = EngineHashMap::default();
    fields.insert("s".into(), Value::Str("abc".into()));
    fields.insert("n".into(), Value::Float(5.0));
    fields.insert(
        "arr".into(),
        Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())]),
    );
    let e = Event { fields };

    let cases: Vec<Expr> = vec![
        // 字符串类：参数个数错误
        call("contains", vec![field("s")]),
        call("startswith", vec![field("s")]),
        call("endswith", vec![field("s")]),
        call("substr", vec![field("s")]),
        call("replace", vec![field("s"), str_lit("a")]),
        call("trim", vec![]),
        call("lower", vec![]),
        call("upper", vec![]),
        call("len", vec![]),
        call("ltrim", vec![]),
        call("rtrim", vec![]),
        call("indexof", vec![field("s")]),
        call("replace_plain", vec![field("s"), str_lit("a")]),
        call("startswith_any", vec![field("s")]),
        call("endswith_any", vec![field("s")]),
        // 字符串类：类型错误
        call("contains", vec![field("n"), str_lit("a")]),
        call("startswith", vec![field("s"), field("n")]),
        call("len", vec![field("n")]),
        call("trim", vec![field("n")]),
        call("indexof", vec![field("n"), str_lit("a")]),
        // 数组类：参数个数错误
        call("mvcount", vec![]),
        call("mvjoin", vec![field("arr")]),
        call("mvindex", vec![field("arr")]),
        call("mvappend", vec![]),
        call("split", vec![str_lit("a")]),
        call("mvdedup", vec![]),
        call("mvsort", vec![]),
        call("mvreverse", vec![]),
        // 数组类：类型错误
        call("mvcount", vec![field("s")]),
        call("mvjoin", vec![field("s"), str_lit("|")]),
        call("mvjoin", vec![field("arr"), field("n")]),
        call("mvindex", vec![field("n"), n(0.0)]),
        call("mvdedup", vec![field("n")]),
        call("split", vec![field("n"), str_lit(",")]),
        call("split", vec![str_lit("a"), field("n")]),
        call("mvsort", vec![field("n")]),
        call("mvreverse", vec![field("n")]),
        // 数值类：参数个数错误 / 非法输入
        call("abs", vec![]),
        call("round", vec![n(1.0), n(1.0), n(1.0)]),
        call("ceil", vec![]),
        call("floor", vec![]),
        call("sqrt", vec![n(-1.0)]),
        call("sqrt", vec![str_lit("x")]),
        call("pow", vec![n(2.0)]),
        call("pow", vec![n(0.0), Expr::Neg(Box::new(n(1.0)))]),
        call("log", vec![n(0.0)]),
        call("log", vec![n(-1.0)]),
        call("log", vec![n(100.0), n(1.0)]),
        call("log", vec![n(100.0), n(0.0)]),
        call("exp", vec![]),
        call("clamp", vec![n(1.0), n(2.0)]),
        call("clamp", vec![n(50.0), n(10.0), n(5.0)]),
        call("clamp", vec![str_lit("x"), n(1.0), n(2.0)]),
        call("sign", vec![]),
        call("sign", vec![n(f64::NAN)]),
        call("trunc", vec![]),
        call("is_finite", vec![]),
        call("is_finite", vec![str_lit("x")]),
        // 合并/格式化类
        call("fmt", vec![str_lit("{}"), str_lit("a"), str_lit("b")]),
        call("fmt", vec![]),
        call("fmt", vec![n(1.0)]),
        call("concat", vec![]),
        call("join", vec![]),
        call("join_by", vec![str_lit("|")]),
        call("join_by", vec![n(1.0), str_lit("a")]),
        call("coalesce", vec![]),
        call("isnull", vec![]),
        call("isnotnull", vec![]),
        call("is_blank", vec![field("n")]),
        call("null_if_blank", vec![field("n")]),
        call("default_if_blank", vec![field("s")]),
        call("default_if_blank", vec![field("n"), str_lit("fb")]),
        // 哈希类
        call("md5", vec![]),
        call("md5", vec![field("n")]),
        call("sha1", vec![]),
        call("sha256", vec![field("n")]),
        call("hex", vec![field("n")]),
        call("sha1_n", vec![str_lit("x"), n(0.0)]),
        call("sha1_n", vec![str_lit("x"), n(41.0)]),
        call("sha1_n", vec![str_lit("x"), n(2.5)]),
        call("stable_id", vec![str_lit("p")]),
        call("stable_id", vec![str_lit("p"), field("arr")]),
        // 正则/时间类
        call("regex_match", vec![str_lit("abc"), str_lit("(")]),
        call("regex_match", vec![str_lit("abc")]),
        call("time_diff", vec![n(1.0)]),
        call("time_bucket", vec![n(1.0)]),
        call("time_bucket", vec![n(1.0), n(0.0)]),
        call("bucket_end", vec![n(1.0), n(-5.0)]),
        call("strptime", vec![str_lit("junk"), str_lit("%Y-%m-%d")]),
        call("strptime", vec![str_lit("x")]),
        // 未知函数
        call("no_such_builtin", vec![n(1.0)]),
    ];
    for expr in cases {
        assert_eq!(eval_expr(&expr, &e), None, "expected None for {expr:?}");
    }
}

// ===========================================================================
// cmp.rs — 比较边界（纯 eval 路径 BinOp）
// ===========================================================================

fn binop(op: BinOp, l: Expr, r: Expr) -> Expr {
    Expr::BinOp {
        op,
        left: Box::new(l),
        right: Box::new(r),
    }
}

#[test]
fn cmp_number_boundaries() {
    let e = event(vec![]);
    // epsilon 相等：0.1 + 0.2 == 0.3
    assert_eq!(
        eval_expr(
            &binop(BinOp::Eq, binop(BinOp::Add, n(0.1), n(0.2)), n(0.3)),
            &e
        ),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(
            &binop(BinOp::Ne, binop(BinOp::Add, n(0.1), n(0.2)), n(0.3)),
            &e
        ),
        Some(Value::Bool(false))
    );
    // 明显不等
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, n(1.0), n(2.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(1.0), n(2.0)), &e),
        Some(Value::Bool(true))
    );
    // 边界比较
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(1.0), n(2.0)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(2.0), n(2.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Gt, n(2.0), n(1.0)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Gt, n(1.0), n(1.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Le, n(2.0), n(2.0)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ge, n(2.0), n(2.0)), &e),
        Some(Value::Bool(true))
    );
    // 负值
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(-3.0), n(-2.0)), &e),
        Some(Value::Bool(true))
    );
    // NaN：`Eq` 为 false（不声称相等），`Ne` 取其逻辑补 → true；顺序比较仍全部 false。
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, n(f64::NAN), n(f64::NAN)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(f64::NAN), n(1.0)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(f64::NAN), n(f64::NAN)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(f64::NAN), n(1.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Gt, n(f64::NAN), n(1.0)), &e),
        Some(Value::Bool(false))
    );
    // inf：IEEE 精确相等（`inf == inf` 为 true——旧式 `(a-b).abs() < EPSILON` 因
    // `inf - inf = NaN` 返回 false）
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, n(f64::INFINITY), n(f64::INFINITY)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(f64::INFINITY), n(f64::INFINITY)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(
            &binop(BinOp::Ne, n(f64::INFINITY), n(f64::NEG_INFINITY)),
            &e
        ),
        Some(Value::Bool(true))
    );
}

/// 结构化值**递归结构相等**（回归 2026-09-18）。
///
/// 回归前 `values_equal` / `compare_values` 对 `Array`/`Object` 落进 `_ => false`
/// → **内容完全相同的两个结构化值也判不等**：`==` 恒 false、`!=` 恒 true、`in` 恒
/// 不命中、去重不合并。检查器 `compatible` 允许 `Object == Object` / `Array == Array`，
/// 所以这是一条可达且静默的语义错误。
#[test]
fn cmp_structured_values_use_recursive_equality() {
    let e = event(vec![]);
    let obj = |pairs: &[(&str, f64)]| {
        Expr::Object(
            pairs
                .iter()
                .map(|(k, v)| ObjectItem {
                    targets: vec![(*k).to_string()],
                    type_hint: None,
                    value: n(*v),
                })
                .collect(),
        )
    };
    let arr = |vals: Vec<Expr>| Expr::Array(vals);

    // 内容相同、键序不同 → 相等（对象按集合比较，与插入序无关）
    let a = obj(&[("ip", 1.0), ("port", 2.0)]);
    let b = obj(&[("port", 2.0), ("ip", 1.0)]);
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, a.clone(), b.clone()), &e),
        Some(Value::Bool(true)),
        "同内容对象必须相等（列式/行式同口径）"
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, a.clone(), b.clone()), &e),
        Some(Value::Bool(false))
    );

    // 叶子值不同 / 键数不同 → 不等，`Ne` 为逻辑补
    let c = obj(&[("ip", 1.0), ("port", 3.0)]);
    let d = obj(&[("ip", 1.0)]);
    for other in [c, d] {
        assert_eq!(
            eval_expr(&binop(BinOp::Eq, a.clone(), other.clone()), &e),
            Some(Value::Bool(false))
        );
        assert_eq!(
            eval_expr(&binop(BinOp::Ne, a.clone(), other), &e),
            Some(Value::Bool(true))
        );
    }

    // 数组：逐元素递归（含嵌套），长度不同 → 不等
    let a1 = arr(vec![n(1.0), arr(vec![n(2.0), n(3.0)])]);
    let a2 = arr(vec![n(1.0), arr(vec![n(2.0), n(3.0)])]);
    let a3 = arr(vec![n(1.0), arr(vec![n(2.0), n(4.0)])]);
    let a4 = arr(vec![n(1.0)]);
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, a1.clone(), a2), &e),
        Some(Value::Bool(true))
    );
    for other in [a3, a4] {
        assert_eq!(
            eval_expr(&binop(BinOp::Eq, a1.clone(), other), &e),
            Some(Value::Bool(false))
        );
    }

    // 结构化但不同构（Object vs Array）→ 不等；`Ne` 取补
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, a.clone(), a1.clone()), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, a.clone(), a1.clone()), &e),
        Some(Value::Bool(true))
    );

    // 标量 vs 结构化：不可比较 → `Eq` false / `Ne` true（不声称相等）
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, n(1.0), a.clone()), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(1.0), a), &e),
        Some(Value::Bool(true))
    );

    // 顺序比较在语言层被检查器拒绝（T8 要求数值）→ 直接构造 AST 时为 false
    assert_eq!(
        eval_expr(
            &binop(BinOp::Lt, obj(&[("k", 1.0)]), obj(&[("k", 2.0)])),
            &e
        ),
        Some(Value::Bool(false))
    );
}

/// 同义写法必须同结果：`x != y` ≡ `!(x == y)`。
///
/// 回归：此前 `Ne` 用 `(a - b).abs() >= EPSILON`，对 NaN 与 `inf == inf` 会和 `Eq`
/// **同时为假** → `x != y` 得 false 而 `!(x == y)` 得 true（`eval_not` 取反），
/// 同一命题两种答案；`not in (...)` 也走 `values_equal` 取反，同属这一族。
#[test]
fn cmp_ne_is_logical_complement_of_eq() {
    let e = event(vec![]);
    let cases = [
        (1.0, 1.0),
        (1.0, 2.0),
        (0.1 + 0.2, 0.3),
        (f64::NAN, f64::NAN),
        (f64::NAN, 1.0),
        (f64::INFINITY, f64::INFINITY),
        (f64::INFINITY, f64::NEG_INFINITY),
        (-0.0, 0.0),
    ];
    for (lhs, rhs) in cases {
        let eq = binop(BinOp::Eq, n(lhs), n(rhs));
        let ne = binop(BinOp::Ne, n(lhs), n(rhs));
        let not_eq = Expr::Not(Box::new(eq.clone()));
        let eq_val = eval_expr(&eq, &e);
        let is_eq = matches!(eq_val, Some(Value::Bool(true)));
        assert_eq!(
            eval_expr(&ne, &e),
            eval_expr(&not_eq, &e),
            "`x != y` 必须等于 `!(x == y)`（lhs={lhs} rhs={rhs}）"
        );
        assert_eq!(
            eval_expr(&ne, &e),
            Some(Value::Bool(!is_eq)),
            "`Ne` 必须是 `Eq` 的逻辑补（lhs={lhs} rhs={rhs}）"
        );
    }
}

#[test]
fn cmp_string_and_bool_boundaries() {
    let e = event(vec![]);
    // 字符串字典序
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, str_lit("a"), str_lit("a")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, str_lit("a"), str_lit("b")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, str_lit("a"), str_lit("b")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Gt, str_lit("ab"), str_lit("a")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Le, str_lit("a"), str_lit("a")), &e),
        Some(Value::Bool(true))
    );
    // 布尔：仅 Eq/Ne
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, Expr::Bool(true), Expr::Bool(true)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, Expr::Bool(true), Expr::Bool(false)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, Expr::Bool(false), Expr::Bool(true)), &e),
        Some(Value::Bool(false))
    );
    // 类型不匹配（不可比较）：不声称相等 → `Eq` false，`Ne` 取其补 → true，
    // 与列式路径同口径（结构化值同理，见 `compare_values` 的 `_` 分支）。
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, n(1.0), str_lit("1")), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(1.0), str_lit("1")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(1.0), str_lit("a")), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ge, Expr::Bool(true), n(1.0)), &e),
        Some(Value::Bool(false))
    );
}

#[test]
fn cmp_arithmetic_overflow_and_div_zero() {
    let e = event(vec![]);
    // 除零 / 模零 → None
    assert_eq!(eval_expr(&binop(BinOp::Div, n(1.0), n(0.0)), &e), None);
    assert_eq!(eval_expr(&binop(BinOp::Mod, n(1.0), n(0.0)), &e), None);
    // 非数值操作数 → None
    assert_eq!(
        eval_expr(&binop(BinOp::Add, n(1.0), str_lit("x")), &e),
        None
    );
    // 减法/乘法/模
    assert_eq!(
        eval_expr(&binop(BinOp::Sub, n(5.0), n(2.0)), &e),
        Some(Value::Float(3.0))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Mul, n(5.0), n(2.0)), &e),
        Some(Value::Float(10.0))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Mod, n(5.0), n(2.0)), &e),
        Some(Value::Float(1.0))
    );
    // 大数值溢出为 inf 仍返回（f64 语义）
    assert!(matches!(
        eval_expr(&binop(BinOp::Mul, n(1e300), n(1e300)), &e),
        Some(Value::Float(v)) if v.is_infinite()
    ));
}

// ===========================================================================
// cmp.rs — 阈值表达式静态求值（try_eval_expr_to_f64）经 CepStateMachine 覆盖
// ===========================================================================

fn threshold_plan(threshold: Expr) -> wf_lang::plan::MatchPlan {
    simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "fail".to_string(),
            field: None,
            guard: None,
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold,
            },
        }])],
    )
}

#[test]
fn threshold_constant_arithmetic_expressions() {
    // count >= -(2) → 第一次事件即匹配
    let mut sm = CepStateMachine::new(
        "t_neg".to_string(),
        threshold_plan(Expr::Neg(Box::new(n(2.0)))),
        None,
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // count >= (5 - 2) = 3
    let mut sm = CepStateMachine::new(
        "t_sub".to_string(),
        threshold_plan(binop(BinOp::Sub, n(5.0), n(2.0))),
        None,
    );
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // count >= (2 * 2) = 4
    let mut sm = CepStateMachine::new(
        "t_mul".to_string(),
        threshold_plan(binop(BinOp::Mul, n(2.0), n(2.0))),
        None,
    );
    for _ in 0..3 {
        assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    }
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // count >= (10 / 2) = 5
    let mut sm = CepStateMachine::new(
        "t_div".to_string(),
        threshold_plan(binop(BinOp::Div, n(10.0), n(2.0))),
        None,
    );
    for _ in 0..4 {
        assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    }
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // count >= (7 % 3) = 1 → 第一次即匹配
    let mut sm = CepStateMachine::new(
        "t_mod".to_string(),
        threshold_plan(binop(BinOp::Mod, n(7.0), n(3.0))),
        None,
    );
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));
}

#[test]
fn threshold_unresolvable_expressions_never_match() {
    // 除零 / 模零 → try_eval_expr_to_f64 None → count 永不满足
    for (name, threshold) in [
        ("t_div0", binop(BinOp::Div, n(1.0), n(0.0))),
        ("t_mod0", binop(BinOp::Mod, n(1.0), n(0.0))),
        // 非算术 BinOp（比较）→ None
        ("t_cmpop", binop(BinOp::Eq, n(1.0), n(1.0))),
        // 逻辑 BinOp → None
        (
            "t_logic",
            Expr::BinOp {
                op: BinOp::And,
                left: Box::new(Expr::Bool(true)),
                right: Box::new(Expr::Bool(true)),
            },
        ),
    ] {
        let mut sm = CepStateMachine::new(name.to_string(), threshold_plan(threshold), None);
        let e = event(vec![("sip", str_val("10.0.0.1"))]);
        for _ in 0..5 {
            assert_eq!(
                sm.advance("fail", &e),
                StepResult::Accumulate,
                "{name} must never match"
            );
        }
    }
}

// ===========================================================================
// executor/eval L3 路径 — 经 RuleExecutor::execute_match 驱动
// ===========================================================================
