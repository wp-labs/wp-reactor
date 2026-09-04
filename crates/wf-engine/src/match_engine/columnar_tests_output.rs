//! `columnar_tests.rs` 拆出的兄弟子模块（2026-09-04）：**output / 列式 cell**
//! 对拍——输出函数（fmt/strftime/count_char、split+mvindex 融合、concat）与
//! InList/IfThenElse 节点的列式 `eval_vec` 与解释路径逐行一致（含 yield 语义
//! None→空串包装、结构化参数编译期拦截→行式回退）。共享 harness 与 use 绑定
//! 在父模块，此处经 `use super::*` 复用。

use super::*;

/// 列式输出 cell（fmt/strftime/count_char）与解释路径逐行对拍，含 yield
/// 语义的 None→空串包装（`eval_yield_expr_with_meta` 对缺字段/null 参数
/// 替换空串）。
fn assert_output_equiv(expr: &Expr, batch: &RecordBatch) {
    let events = batch_to_events(batch);
    let view = ColumnarBatch::from_all_fields(batch);
    let plan = compile_guard(expr, &view).expect("输出函数应可编译");
    let cvec = plan.eval_vec(&view, view.num_rows());
    for (row, event) in events.iter().enumerate() {
        let columnar = match cvec.scalar_at(row) {
            Some(s) => cscalar_to_value(&s),
            None => Value::Str(SmolStr::default()),
        };
        let interpreted = eval_expr(expr, event).unwrap_or_else(|| Value::Str(SmolStr::default()));
        assert_eq!(columnar, interpreted, "row {row}: expr={expr:?}");
    }
}

#[test]
fn output_funcs_match_interpreted_cells() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("action", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
        Field::new("ts", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("fail_login"),
                None,
                Some("success"),
                Some("aa"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(3), Some(7), None, Some(2)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_001),
                None,
                Some(1_700_000_000_000_000_002),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));

    // fmt：字面量模板 + 字段参数（null action/count 行 → 空串）。
    let fmt = call(
        "fmt",
        vec![Expr::StringLit("a={}|n={}".into()), f("action"), f("count")],
    );
    assert!(wf_lang::columnar::columnar_output_expr(&fmt));
    assert_output_equiv(&fmt, &batch);
    // fmt：纯字面量参数。
    assert_output_equiv(
        &call(
            "fmt",
            vec![Expr::StringLit("x={}".into()), Expr::Number(42.0)],
        ),
        &batch,
    );

    // strftime：默认格式 + 自定义格式 + 常量 ts。
    assert_output_equiv(&call("strftime", vec![f("ts")]), &batch);
    assert_output_equiv(
        &call(
            "strftime",
            vec![f("ts"), Expr::StringLit("%Y-%m-%d".into())],
        ),
        &batch,
    );
    assert_output_equiv(
        &call("strftime", vec![Expr::Number(1_700_000_000_000_000_000.0)]),
        &batch,
    );

    // count_char：字面量 / 字段 needle；null 参数（action null 行）→ 空串。
    assert_output_equiv(
        &call("count_char", vec![f("action"), Expr::StringLit("a".into())]),
        &batch,
    );
    assert_output_equiv(
        &call("count_char", vec![f("action"), Expr::StringLit("l".into())]),
        &batch,
    );
    // 空 needle → 0。
    assert_output_equiv(
        &call(
            "count_char",
            vec![f("action"), Expr::StringLit(String::new())],
        ),
        &batch,
    );
}

#[test]
fn output_funcs_split_mvindex_concat_match_interpreted() {
    // 层 2（2026-08-25，q22 形态）：`mvindex(split(field, sep), idx)` 融合
    // 节点（SplitIndex）与 `concat` 必须与解释路径逐行对拍——含 null 行 /
    // 越界 / 空 sep（按字符切分）/ 负数索引（从尾数）。
    let schema = Arc::new(Schema::new(vec![Field::new("url", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm?query=1"),
            None,           // null 行
            Some("short"),  // 段数不足 → mvindex 越界 → null
            Some("a/b//d"), // 空段
        ])) as ArrayRef],
    )
    .unwrap();
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
    let split = |text: Expr, sep: &str| call("split", vec![text, Expr::StringLit(sep.into())]);
    let mvindex = |list: Expr, idx: f64| call("mvindex", vec![list, Expr::Number(idx)]);

    // mvindex(split(url, "/"), 3)——融合节点（正索引）。
    let idx3 = mvindex(split(f("url"), "/"), 3.0);
    assert!(wf_lang::columnar::columnar_output_expr(&idx3));
    assert_value_equiv(&idx3, &batch);
    // 负数索引（从尾数）。
    assert_value_equiv(&mvindex(split(f("url"), "/"), -1.0), &batch);
    // 空 sep → 按字符切分。
    assert_value_equiv(&mvindex(split(f("url"), ""), 4.0), &batch);

    // concat：字段 + 字面量；q22 detail 形态（3 段 mvindex 拼接）。
    let concat_suffix = call("concat", vec![f("url"), Expr::StringLit("-suffix".into())]);
    assert!(wf_lang::columnar::columnar_output_expr(&concat_suffix));
    assert_output_equiv(&concat_suffix, &batch);
    let q22_detail = call(
        "concat",
        vec![
            mvindex(split(f("url"), "/"), 3.0),
            Expr::StringLit("/".into()),
            mvindex(split(f("url"), "/"), 4.0),
            Expr::StringLit("/".into()),
            mvindex(split(f("url"), "/"), 5.0),
        ],
    );
    assert!(wf_lang::columnar::columnar_output_expr(&q22_detail));
    assert_output_equiv(&q22_detail, &batch);
}

/// Exact per-cell parity (incl. null-ness and value type): columnar
/// `eval_vec` vs interpreted `eval_expr` — the strictest lock for the
/// InList / IfThenElse output nodes.
fn assert_value_equiv(expr: &Expr, batch: &RecordBatch) {
    let events = batch_to_events(batch);
    let view = ColumnarBatch::from_all_fields(batch);
    let plan = compile_guard(expr, &view).expect("应可编译");
    let cvec = plan.eval_vec(&view, view.num_rows());
    for (row, event) in events.iter().enumerate() {
        let columnar = cvec.scalar_at(row).map(|s| cscalar_to_value(&s));
        let interpreted = eval_expr(expr, event);
        assert_eq!(columnar, interpreted, "row {row}: expr={expr:?}");
    }
}

/// InList：`values_equal` 成员语义（数字 epsilon 等值 / Str / Bool）、negated
/// 翻转、null 目标传播 None——与解释器逐行一致。
#[test]
fn inlist_matches_interpreted_cells() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("count", DataType::Int64, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                Some(3),
                Some(7),
                Some(8),
                None,
                Some(3),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                // 01:00 / 02:30 / 00:15 / null / 13:00 UTC（%H 小时）。
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000 + 90 * 3_600_000_000_000),
                Some(1_700_000_000_000_000_000 - 45 * 3_600_000_000_000),
                None,
                Some(1_700_000_000_000_000_000 + 13 * 3_600_000_000_000),
            ])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(false),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
    let in_list = |expr: Expr, list: Vec<Expr>, negated: bool| Expr::InList {
        expr: Box::new(expr),
        list,
        negated,
    };

    // 数字成员（Int64 列 vs 数字字面量列表）。
    let nums = in_list(f("count"), vec![num(3.0), num(7.0)], false);
    assert_value_equiv(&nums, &batch);
    // negated 翻转（None 目标行仍然 None，不因否定变 true——解释器同）。
    assert_value_equiv(&in_list(f("count"), vec![num(3.0)], true), &batch);
    // Bool 成员。
    assert_value_equiv(&in_list(f("flag"), vec![Expr::Bool(true)], false), &batch);
    // Q14 形态：strftime(ts, "%H") in ("00","01","02")。
    let hour = Expr::FuncCall {
        qualifier: None,
        name: "strftime".into(),
        args: vec![f("ts"), Expr::StringLit("%H".into())],
    };
    assert!(wf_lang::columnar::columnar_output_expr(&hour));
    assert_value_equiv(
        &in_list(
            hour,
            vec![
                Expr::StringLit("00".into()),
                Expr::StringLit("01".into()),
                Expr::StringLit("02".into()),
            ],
            false,
        ),
        &batch,
    );
}

/// IfThenElse：Bool cond 三值选值；非 Bool / null cond → None（解释器同）。
#[test]
fn ifthenelse_matches_interpreted_cells() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("count", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(3), Some(7), Some(8), None])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
    let ite = |cond: Expr, then_e: Expr, else_e: Expr| Expr::IfThenElse {
        cond: Box::new(cond),
        then_expr: Box::new(then_e),
        else_expr: Box::new(else_e),
    };

    // 比较条件（列式 Bool）→ 三值选值；分支类型切换（字符串 vs 数字）。
    let by_flag = ite(
        f("flag"),
        Expr::StringLit("yes".into()),
        Expr::StringLit("no".into()),
    );
    assert_value_equiv(&by_flag, &batch);
    // 分支类型不同：数字 vs 字符串（列式异构 Scalar 列）。
    let mixed = ite(f("flag"), num(1.0), Expr::StringLit("no".into()));
    assert_value_equiv(&mixed, &batch);
    // 非 Bool cond（数字列）→ 全 None。
    let non_bool = ite(f("count"), num(1.0), num(2.0));
    assert_value_equiv(&non_bool, &batch);
    // InList cond 组合（`count in (3,7)` 做条件）。
    let in_cond = Expr::InList {
        expr: Box::new(f("count")),
        list: vec![num(3.0), num(7.0)],
        negated: false,
    };
    assert_value_equiv(
        &ite(
            in_cond,
            Expr::StringLit("hit".into()),
            Expr::StringLit("miss".into()),
        ),
        &batch,
    );
}

/// Q14 全形态 value 对拍：`fmt("{} c={}", if strftime(ts,"%H") in (...)
/// then "nightTime" else "dayTime", count_char(extra,"c"))`。
#[test]
fn q14_fmt_shape_matches_interpreted_cells() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, true),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000 + 90 * 3_600_000_000_000),
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("abc c cc"),
                Some("no-c"),
                None,
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let is_night = Expr::InList {
        expr: Box::new(call(
            "strftime",
            vec![f("ts"), Expr::StringLit("%H".into())],
        )),
        list: vec![
            Expr::StringLit("00".into()),
            Expr::StringLit("01".into()),
            Expr::StringLit("02".into()),
        ],
        negated: false,
    };
    let detail = call(
        "fmt",
        vec![
            Expr::StringLit("{} c={}".into()),
            Expr::IfThenElse {
                cond: Box::new(is_night),
                then_expr: Box::new(Expr::StringLit("nightTime".into())),
                else_expr: Box::new(Expr::StringLit("dayTime".into())),
            },
            call("count_char", vec![f("extra"), Expr::StringLit("c".into())]),
        ],
    );
    assert!(wf_lang::columnar::columnar_output_expr(&detail));
    assert_value_equiv(&detail, &batch);
}

/// 真实 `q14.wfl` 形状：**嵌套 3 档 CASE**（nightTime/dayTime/otherTime，
/// 10/9 项 InList）——else 分支里再嵌 IfThenElse。列式 gate/编译/求值必须
/// 与解释器逐行一致（三档都覆盖 + null ts）。
#[test]
fn q14_real_three_way_case_matches_interpreted() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, true),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                // 22 时 → nightTime；10 时 → dayTime；07 时 → otherTime；null。
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_000_000_000 - 12 * 3_600_000_000_000),
                Some(1_700_000_000_000_000_000 - 15 * 3_600_000_000_000),
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("abc c cc"),
                Some("no-c"),
                Some("zz"),
                None,
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };
    let in_hours = |hours: &[&str]| Expr::InList {
        expr: Box::new(call(
            "strftime",
            vec![f("ts"), Expr::StringLit("%H".into())],
        )),
        list: hours.iter().map(|h| Expr::StringLit((*h).into())).collect(),
        negated: false,
    };
    let bid_time_type = Expr::IfThenElse {
        cond: Box::new(in_hours(&[
            "00", "01", "02", "03", "04", "05", "06", "20", "21", "22", "23",
        ])),
        then_expr: Box::new(Expr::StringLit("nightTime".into())),
        else_expr: Box::new(Expr::IfThenElse {
            cond: Box::new(in_hours(&[
                "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
            ])),
            then_expr: Box::new(Expr::StringLit("dayTime".into())),
            else_expr: Box::new(Expr::StringLit("otherTime".into())),
        }),
    };
    let detail = call(
        "fmt",
        vec![
            Expr::StringLit("{} c={}".into()),
            bid_time_type,
            call("count_char", vec![f("extra"), Expr::StringLit("c".into())]),
        ],
    );
    assert!(
        wf_lang::columnar::columnar_output_expr(&detail),
        "真实 q14 嵌套 3 档 CASE 必须可列式"
    );
    assert_value_equiv(&detail, &batch);
    // 语义抽查：三档分型 + count_char。
    let events = batch_to_events(&batch);
    assert_eq!(
        eval_expr(&detail, &events[0]).unwrap(),
        Value::Str("nightTime c=4".into())
    );
    assert_eq!(
        eval_expr(&detail, &events[1]).unwrap(),
        Value::Str("dayTime c=1".into())
    );
    assert_eq!(
        eval_expr(&detail, &events[2]).unwrap(),
        Value::Str("otherTime c=0".into())
    );
    assert_eq!(eval_expr(&detail, &events[3]), None);
}

#[test]
fn fmt_structured_arg_falls_back_to_row() {
    use crate::match_engine::WFL_FIELD_TYPE_OBJECT;

    // OBJECT 元数据的 Utf8 列：解释路径解析成 Value::Object 渲染
    // `[object]`，列式读原始 JSON 文本——字节不同，必须行式回退。
    let schema = Arc::new(Schema::new(vec![
        Field::new("ext", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )])),
        Field::new("id", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some(r#"{"k":1}"#)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
        ],
    )
    .unwrap();
    let fmt = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("x={}".into()),
            Expr::Field(FieldRef::Simple("ext".into())),
        ],
    };
    // 形状 gate 放行（flat 字段参数），但编译必须失败 → 行式回退。
    assert!(wf_lang::columnar::columnar_output_expr(&fmt));
    let view = ColumnarBatch::from_all_fields(&batch);
    assert!(
        compile_guard(&fmt, &view).is_none(),
        "fmt 结构化参数必须编译失败（行式回退）"
    );
    // 行式渲染：Value::Object → value_to_string → "[object]"。
    let events = batch_to_events(&batch);
    assert_eq!(
        eval_expr(&fmt, &events[0]).unwrap_or_else(|| Value::Str(SmolStr::default())),
        Value::Str("x=[object]".into()),
        "解释路径渲染 [object]"
    );
}

/// 结构化字段藏在 IfThenElse 分支 / InList 目标里：gate 放行（flat FieldRef
/// 不含元数据），但编译期 `arg_reads_structured` **递归**拦截 → 行式回退。
/// 否则列式读 OBJECT 列原始 JSON 文本，fmt 渲染原始 JSON / count_char 对
/// JSON 计数——与解释器 `[object]`/None 字节分叉。
#[test]
fn structured_nested_in_branch_compiles_fail() {
    use crate::match_engine::WFL_FIELD_TYPE_OBJECT;

    let schema = Arc::new(Schema::new(vec![
        Field::new("ext", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )])),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some(r#"{"k":1}"#), None])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as ArrayRef,
        ],
    )
    .unwrap();
    let view = ColumnarBatch::from_all_fields(&batch);
    let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
    let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args,
    };

    // fmt("{} {}", if flag then ext else "x", "y")——结构化藏在 then 分支。
    let fmt_branch = call(
        "fmt",
        vec![
            Expr::StringLit("{} {}".into()),
            Expr::IfThenElse {
                cond: Box::new(f("flag")),
                then_expr: Box::new(f("ext")),
                else_expr: Box::new(Expr::StringLit("x".into())),
            },
            Expr::StringLit("y".into()),
        ],
    );
    // gate 放行（分支是 flat FieldRef）……
    assert!(wf_lang::columnar::columnar_output_expr(&fmt_branch));
    // ……但编译必须失败（递归 arg_reads_structured 拦截）。
    assert!(
        compile_guard(&fmt_branch, &view).is_none(),
        "fmt 分支里的结构化字段必须编译失败"
    );

    // count_char(ext, "c")——结构化直接作 text 参数。
    let cc = call("count_char", vec![f("ext"), Expr::StringLit("c".into())]);
    assert!(wf_lang::columnar::columnar_output_expr(&cc));
    assert!(
        compile_guard(&cc, &view).is_none(),
        "count_char 结构化 text 参数必须编译失败"
    );

    // count_char("abc", ext)——结构化作 needle 参数（首字符计数分叉）。
    let cc2 = call("count_char", vec![Expr::StringLit("abc".into()), f("ext")]);
    assert!(wf_lang::columnar::columnar_output_expr(&cc2));
    assert!(
        compile_guard(&cc2, &view).is_none(),
        "count_char 结构化 needle 参数必须编译失败"
    );

    // InList 目标为结构化列，藏在 fmt 的 IfThenElse cond 里（极端形态）：
    // gate 放行（InList 列表字面量 + ext flat），但递归拦截必须使其编译失败。
    let fmt_inlist_cond = call(
        "fmt",
        vec![
            Expr::StringLit("{} {}".into()),
            Expr::IfThenElse {
                cond: Box::new(Expr::InList {
                    expr: Box::new(f("ext")),
                    list: vec![Expr::StringLit("{\"k\":1}".into())],
                    negated: false,
                }),
                then_expr: Box::new(Expr::StringLit("a".into())),
                else_expr: Box::new(Expr::StringLit("b".into())),
            },
            Expr::StringLit("y".into()),
        ],
    );
    assert!(wf_lang::columnar::columnar_output_expr(&fmt_inlist_cond));
    assert!(
        compile_guard(&fmt_inlist_cond, &view).is_none(),
        "fmt 内 InList 目标结构化必须编译失败"
    );
    // 裸 IfThenElse（非输出函数参数）作顶层 yield 从不走列式（executor 只对
    // 输出函数编译 general 槽位）——此处仅确认它不 panic 且不误报结构化。
    let bare_ite = Expr::IfThenElse {
        cond: Box::new(f("flag")),
        then_expr: Box::new(Expr::StringLit("a".into())),
        else_expr: Box::new(Expr::StringLit("b".into())),
    };
    assert!(wf_lang::columnar::columnar_output_expr(&bare_ite));
    assert!(compile_guard(&bare_ite, &view).is_some());

    // 行式基准：true 分支渲染 [object]；count_char 对 Object → None。
    let events = batch_to_events(&batch);
    assert_eq!(
        eval_expr(&fmt_branch, &events[0]).unwrap_or_else(|| Value::Str(SmolStr::default())),
        Value::Str("[object] y".into()),
        "解释路径：true 分支渲染 [object]"
    );
    assert_eq!(
        eval_expr(&cc, &events[0]).unwrap_or_else(|| Value::Str(SmolStr::default())),
        Value::Str(SmolStr::default()),
        "解释路径：count_char(Object) → None → 空串"
    );
}
