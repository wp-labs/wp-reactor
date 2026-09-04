//! `columnar_tests.rs` 拆出的兄弟子模块（2026-09-04）：**guard 语义**对拍——列式
//! 布尔 mask 与解释路径 `eval_expr_ext` 逐行一致（算术/比较/逻辑/not、native int
//! 2^53 边界、null/缺字段、epsilon、结构化 list-index（JSON array / native
//! list）、cidr/regex/str 搜索守卫、`mask_to_indices`+`materialize_rows` 收口）。
//! 共享 harness（`field`/`num`/`bin`/`make_batch`/`assert_equiv`/`interpreted_bool`）
//! 与 use 绑定在父模块，此处经 `use super::*` 复用。

use super::*;

#[test]
fn q2_guard_matches_interpreted() {
    let auction: Vec<Option<i64>> = (0..1000).map(Some).collect();
    let batch = make_batch(
        auction,
        vec![Some(7.0); 1000],
        vec![Some("mobile"); 1000],
        vec![Some(true); 1000],
    );
    let expr = bin(
        BinOp::Eq,
        bin(BinOp::Mod, field("auction"), num(123.0)),
        num(0.0),
    );
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);
}

#[test]
fn comparison_arithmetic_and_logic_match_interpreted() {
    let auction: Vec<Option<i64>> = vec![
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(-1),
        None,
        Some(1_000_000),
    ];
    let price: Vec<Option<f64>> = vec![
        Some(0.0),
        Some(1.5),
        Some(2.0),
        Some(-3.25),
        Some(7.0),
        None,
        Some(1e300),
    ];
    let channel: Vec<Option<&str>> = vec![
        Some("a"),
        Some("b"),
        Some("a"),
        None,
        Some(""),
        Some("z"),
        Some("a"),
    ];
    let flag: Vec<Option<bool>> = vec![
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        Some(true),
        None,
    ];
    let batch = make_batch(auction, price, channel, flag);

    let exprs = vec![
        bin(BinOp::Gt, field("auction"), num(1.0)),
        bin(BinOp::Eq, field("auction"), field("auction")),
        bin(BinOp::Ne, field("auction"), num(0.0)),
        bin(BinOp::Le, field("price"), num(2.0)),
        bin(BinOp::Ge, field("price"), num(-3.25)),
        bin(BinOp::Eq, field("channel"), Expr::StringLit("a".into())),
        bin(BinOp::Lt, field("channel"), Expr::StringLit("b".into())),
        bin(BinOp::Eq, field("flag"), Expr::Bool(true)),
        bin(BinOp::Add, field("auction"), num(2.0)),
        bin(BinOp::Sub, field("auction"), num(1.0)),
        bin(BinOp::Mul, field("auction"), num(3.0)),
        bin(BinOp::Div, field("auction"), num(2.0)),
        bin(BinOp::Mod, field("auction"), num(3.0)),
        Expr::Neg(Box::new(field("auction"))),
        bin(
            BinOp::And,
            bin(BinOp::Gt, field("auction"), num(0.0)),
            bin(BinOp::Lt, field("auction"), num(3.0)),
        ),
        bin(
            BinOp::Or,
            bin(BinOp::Eq, field("channel"), Expr::StringLit("a".into())),
            bin(BinOp::Eq, field("flag"), Expr::Bool(false)),
        ),
        bin(
            BinOp::And,
            field("flag"),
            bin(BinOp::Gt, field("auction"), num(0.0)),
        ),
        // 逻辑否定：not 比较 / not flag / 双重 not（列式 == 解释器）。
        Expr::Not(Box::new(bin(BinOp::Eq, field("auction"), num(1.0)))),
        Expr::Not(Box::new(field("flag"))),
        Expr::Not(Box::new(Expr::Not(Box::new(bin(
            BinOp::Eq,
            field("auction"),
            num(1.0),
        ))))),
    ];

    for expr in exprs {
        assert!(
            wf_lang::columnar::expr_is_columnar(&expr),
            "expr should be columnar: {expr:?}"
        );
        assert_equiv(&expr, &batch);
    }
}

/// 列式 `not (auction == 1)` vs `auction != 1`：语义等价、路径几乎相同
/// （not_vec 只对 bool 列逐格取反），吞吐应同量级，且 mask 逐位一致。
/// 保护 `not` 的列式实现不被退化成每行 fallback 或额外全列扫描。
#[test]
fn not_columnar_throughput_parity() {
    use std::time::Instant;

    let rows = 1_000usize;
    let auction: Vec<Option<i64>> = (0..rows).map(|i| Some((i % 50) as i64)).collect();
    let n = auction.len();
    let batch = make_batch(
        auction,
        vec![Some(0.0); n],
        vec![Some("a"); n],
        vec![Some(true); n],
    );
    let view = ColumnarBatch::from_all_fields(&batch);

    let not_expr = Expr::Not(Box::new(bin(BinOp::Eq, field("auction"), num(1.0))));
    let ne_expr = bin(BinOp::Ne, field("auction"), num(1.0));

    let rounds = 200usize;
    let start_not = Instant::now();
    let mut mask_not = BooleanArray::from(vec![false; rows]);
    for _ in 0..rounds {
        mask_not = eval_guard_columnar(&not_expr, &view);
    }
    let not_el = start_not.elapsed();

    let start_ne = Instant::now();
    let mut mask_ne = BooleanArray::from(vec![false; rows]);
    for _ in 0..rounds {
        mask_ne = eval_guard_columnar(&ne_expr, &view);
    }
    let ne_el = start_ne.elapsed();

    assert_eq!(mask_not.len(), rows);
    for r in 0..rows {
        assert_eq!(
            mask_not.value(r),
            mask_ne.value(r),
            "row {r}: not(...) 与 != 的列式结果必须一致"
        );
    }
    let ratio = not_el.as_secs_f64() / ne_el.as_secs_f64();
    eprintln!(
        "  columnar not={:?} ne={:?} ratio={:.2}x",
        not_el, ne_el, ratio
    );
    assert!(
        ratio < 2.5,
        "columnar `not` 相对 `!=` 开销过高：{:.2}x (not {:?} vs != {:?})",
        ratio,
        not_el,
        ne_el
    );
}

#[test]
fn native_int_matches_interpreted_below_2_53() {
    const TWO_POW_53: i64 = 9_007_199_254_740_992;
    let auction: Vec<Option<i64>> = vec![
        Some(0),
        Some(1),
        Some(-1),
        Some(TWO_POW_53 - 2),
        Some(TWO_POW_53 - 1),
        None,
    ];
    let n = auction.len();
    let batch = make_batch(
        auction,
        vec![Some(0.0); n],
        vec![Some("x"); n],
        vec![Some(true); n],
    );
    let exprs = vec![
        bin(
            BinOp::Eq,
            bin(BinOp::Mod, field("auction"), num(123.0)),
            num(0.0),
        ),
        bin(BinOp::Gt, field("auction"), num(0.0)),
        bin(BinOp::Le, field("auction"), field("auction")),
        bin(BinOp::Ne, field("auction"), num(1.0)),
    ];
    for expr in exprs {
        assert_equiv(&expr, &batch);
    }
}

#[test]
fn native_int_comparison_diverges_above_2_53() {
    const TWO_POW_53: i64 = 9_007_199_254_740_992;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![TWO_POW_53])) as ArrayRef,
            Arc::new(Int64Array::from(vec![TWO_POW_53 + 1])) as ArrayRef,
        ],
    )
    .unwrap();

    // a == b with a=2^53, b=2^53+1. Native i64 sees them distinct; the
    // interpreted f64 path rounds b down to 2^53 and reports equal.
    let expr = bin(BinOp::Eq, field("a"), field("b"));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    let view = ColumnarBatch::from_all_fields(&batch);
    let mask = eval_guard_columnar(&expr, &view);
    assert!(
        !mask.value(0),
        "native i64 should distinguish 2^53 and 2^53+1"
    );

    let events = batch_to_events(&batch);
    let interpreted = interpreted_bool(&expr, &events[0]);
    assert!(interpreted, "interpreted f64 rounds 2^53+1 to 2^53");
    assert_ne!(mask.value(0), interpreted);
}

#[test]
fn missing_field_is_null_and_not_matched() {
    let batch = make_batch(
        vec![Some(1), Some(2)],
        vec![Some(1.0), Some(2.0)],
        vec![Some("x"), Some("y")],
        vec![Some(true), Some(false)],
    );
    // `missing` is absent from the schema → columnar null, interpreted None.
    let expr = bin(BinOp::Gt, field("missing"), num(0.0));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);
}

#[test]
fn epsilon_equality_matches_interpreted_on_floats() {
    // 0.1 + 0.2 == 0.3 is true under epsilon equality; both tracks must agree.
    let batch = make_batch(
        vec![Some(1)],
        vec![Some(0.1 + 0.2)],
        vec![Some("x")],
        vec![Some(true)],
    );
    let expr = bin(BinOp::Eq, field("price"), num(0.3));
    assert_equiv(&expr, &batch);
}

#[test]
fn non_boolean_top_level_is_not_matched() {
    let batch = make_batch(
        vec![Some(5)],
        vec![Some(1.0)],
        vec![Some("x")],
        vec![Some(true)],
    );
    // Numeric expression at guard top level → interpreted `None` → false.
    let expr = bin(BinOp::Add, field("auction"), num(1.0));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);
}

/// A `tags`-style column: `Utf8` cells holding JSON arrays, marked with the
/// structured-array metadata the receiver attaches to `array/...` fields
/// (`wf.wfl.field_type = "array"`), plus an `auction` column for
/// composition tests.
fn json_array_batch(tags: Vec<Option<&str>>, auction: Vec<Option<i64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tags", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
            (
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_ARRAY.to_string(),
            ),
        ])),
        Field::new("auction", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(tags)) as ArrayRef,
            Arc::new(Int64Array::from(auction)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// `c.tags[i]` — the list-index path under test.
fn tags_index(index: usize) -> Expr {
    Expr::Field(FieldRef::Path {
        alias: "c".to_string(),
        segments: vec![
            PathSegment::Field("tags".to_string()),
            PathSegment::Index(index),
        ],
    })
}

#[test]
fn list_index_json_array_matches_interpreted() {
    let batch = json_array_batch(
        vec![
            Some(r#"["prod","edge","dmz"]"#),
            Some(r#"["edge"]"#),
            Some(r#"["prod"]"#),
            Some(r#"[]"#),
            None,
        ],
        vec![Some(1); 5],
    );
    let exprs = vec![
        bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into())),
        bin(BinOp::Eq, tags_index(1), Expr::StringLit("edge".into())),
        bin(BinOp::Eq, tags_index(2), Expr::StringLit("dmz".into())),
        // Out of range / null cell → null → not matched.
        bin(BinOp::Eq, tags_index(3), Expr::StringLit("x".into())),
        bin(BinOp::Ne, tags_index(0), Expr::StringLit("edge".into())),
        bin(BinOp::Gt, tags_index(0), Expr::StringLit("a".into())),
    ];
    for expr in exprs {
        assert!(
            wf_lang::columnar::expr_is_columnar(&expr),
            "expr should be columnar: {expr:?}"
        );
        assert_equiv(&expr, &batch);
    }
}

#[test]
fn list_index_json_array_null_elements_are_dropped() {
    let batch = json_array_batch(
        vec![
            Some(r#"["a", null, "b"]"#),
            Some(r#"[null, null, "c"]"#),
            Some(r#"[1, null, "x"]"#),
        ],
        vec![Some(1); 3],
    );
    // json_to_value drops null elements: [a, null, b] → [a, b], so [1] is "b".
    let expr = bin(BinOp::Eq, tags_index(1), Expr::StringLit("b".into()));
    assert_equiv(&expr, &batch);
    // [null, null, "c"] → ["c"], so [2] is out of range → null.
    let expr = bin(BinOp::Eq, tags_index(2), Expr::StringLit("c".into()));
    assert_equiv(&expr, &batch);
    // [1, null, "x"] → [1, "x"]; index 1 is the string "x".
    let expr = bin(BinOp::Eq, tags_index(1), Expr::StringLit("x".into()));
    assert_equiv(&expr, &batch);
    // And the numeric element before the null: index 0 == 1.
    let expr = bin(BinOp::Eq, tags_index(0), num(1.0));
    assert_equiv(&expr, &batch);
}

#[test]
fn list_index_json_numeric_and_bool_elements() {
    let batch = json_array_batch(
        vec![
            Some(r#"[5, 6.5]"#),
            Some(r#"[true, false]"#),
            Some(r#"[1e2]"#),
        ],
        vec![Some(1); 3],
    );
    // Number elements compare as f64 (interpreted `Value::Number`).
    let expr = bin(BinOp::Eq, tags_index(0), num(5.0));
    assert_equiv(&expr, &batch);
    let expr = bin(BinOp::Gt, tags_index(0), num(4.0));
    assert_equiv(&expr, &batch);
    // 1e2 → 100.
    let expr = bin(BinOp::Eq, tags_index(0), num(100.0));
    assert_equiv(&expr, &batch);
    // Bool elements compare as bools; a number never equals a bool.
    let expr = bin(BinOp::Eq, tags_index(0), Expr::Bool(true));
    assert_equiv(&expr, &batch);
    let expr = bin(BinOp::Eq, tags_index(1), Expr::Bool(false));
    assert_equiv(&expr, &batch);
}

#[test]
fn list_index_structured_elements_compare_false_not_null() {
    let batch = json_array_batch(
        vec![
            Some(r#"[{"k":1}, "prod"]"#),
            Some(r#"[[1,2]]"#),
            Some(r#"["prod"]"#),
        ],
        vec![Some(1); 3],
    );
    // Object / array elements are a definite false on compare, never null.
    let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into()));
    assert_equiv(&expr, &batch);
    // Out-of-range index reads a null slot (the close-step permissive
    // distinction) — lock it directly on the mask.
    let out_of_range = bin(BinOp::Eq, tags_index(2), Expr::StringLit("prod".into()));
    let view = ColumnarBatch::from_all_fields(&batch);
    let mask = eval_guard_columnar(&expr, &view);
    let mask_oob = eval_guard_columnar(&out_of_range, &view);
    assert!(
        !mask.value(0) && !mask.is_null(0),
        "object element → false, not null"
    );
    assert!(
        !mask.value(1) && !mask.is_null(1),
        "array element → false, not null"
    );
    assert!(mask.value(2), "string element compares equal");
    for row in 0..3 {
        assert!(
            mask_oob.is_null(row),
            "out-of-range reads null (permissive)"
        );
    }
}

/// A single-column batch whose `tags` column is a native Arrow list shape.
fn native_list_batch(col: ArrayRef, list_dt: DataType) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("tags", list_dt, true)]));
    RecordBatch::try_new(schema, vec![col]).unwrap()
}

#[test]
fn list_index_native_list_columns_match_interpreted() {
    // List<Utf8>: rows ["prod","edge"] / [null] / [] / ["dmz"].
    let values = StringArray::from(vec![Some("prod"), Some("edge"), None, Some("dmz")]);
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0i32, 2, 3, 3, 4].into()),
        Arc::new(values) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = native_list_batch(
        Arc::new(list) as ArrayRef,
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
    );
    // [null] drops the null element → empty → index 0 out of range → null.
    let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into()));
    assert_equiv(&expr, &batch);
    let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("dmz".into()));
    assert_equiv(&expr, &batch);

    // LargeList<Int64>: rows [1, 2] / [3, 4, 5].
    let large = LargeListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0i64, 2, 5].into()),
        Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = native_list_batch(
        Arc::new(large) as ArrayRef,
        DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
    );
    let expr = bin(BinOp::Eq, tags_index(1), num(2.0));
    assert_equiv(&expr, &batch);
    let expr = bin(BinOp::Eq, tags_index(2), num(5.0));
    assert_equiv(&expr, &batch);

    // FixedSizeList<Utf8> size 2: rows ["a","b"] / ["c", null] → ["c"].
    let values = StringArray::from(vec![Some("a"), Some("b"), Some("c"), None]);
    let fixed = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        2,
        Arc::new(values) as ArrayRef,
        None,
    );
    let batch = native_list_batch(
        Arc::new(fixed) as ArrayRef,
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8, true)), 2),
    );
    let expr = bin(BinOp::Eq, tags_index(1), Expr::StringLit("b".into()));
    assert_equiv(&expr, &batch);
    // [c, null] → [c]: index 1 out of range → null; index 0 == "c".
    let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("c".into()));
    assert_equiv(&expr, &batch);
    let expr = bin(BinOp::Eq, tags_index(1), Expr::StringLit("c".into()));
    assert_equiv(&expr, &batch);
}

#[test]
fn list_index_non_array_root_degrades_to_null() {
    // A plain Utf8 column named `tags` (no array metadata) whose cells are
    // JSON-array text: the interpreted walk hits `[0]` on a Str root → null.
    let schema = Arc::new(Schema::new(vec![Field::new("tags", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![Some(r#"["prod"]"#)])) as ArrayRef],
    )
    .unwrap();
    let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into()));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    let events = batch_to_events(&batch);
    let view = ColumnarBatch::from_all_fields(&batch);
    let mask = eval_guard_columnar(&expr, &view);
    assert!(
        !mask.value(0) && mask.is_null(0),
        "non-array root reads null"
    );
    assert_eq!(mask.value(0), interpreted_bool(&expr, &events[0]));

    // An Int64 column named `tags`: index on a Number root → null too.
    let schema = Arc::new(Schema::new(vec![Field::new("tags", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
    )
    .unwrap();
    let events = batch_to_events(&batch);
    let view = ColumnarBatch::from_all_fields(&batch);
    let mask = eval_guard_columnar(&expr, &view);
    assert!(!mask.value(0) && mask.is_null(0));
    assert_eq!(mask.value(0), interpreted_bool(&expr, &events[0]));
}

#[test]
fn bare_array_field_is_structured() {
    // `c.tags` (Qualified, no index) reads the whole array as a structured
    // value: never equal to a scalar, `!=` always true, present-but-null
    // distinguished (a present array is a definite false, not null).
    let batch = json_array_batch(vec![Some(r#"["prod","edge"]"#), None], vec![Some(1); 2]);
    let tags_field = || Expr::Field(FieldRef::Qualified("c".into(), "tags".into()));
    let expr = bin(BinOp::Eq, tags_field(), Expr::StringLit("prod".into()));
    assert_equiv(&expr, &batch);
    let expr = bin(BinOp::Ne, tags_field(), Expr::StringLit("prod".into()));
    assert_equiv(&expr, &batch);
    let view = ColumnarBatch::from_all_fields(&batch);
    let mask = eval_guard_columnar(
        &bin(BinOp::Eq, tags_field(), Expr::StringLit("x".into())),
        &view,
    );
    assert!(
        !mask.is_null(0),
        "present array is a definite false, not null"
    );
    assert!(mask.is_null(1), "null cell reads null");
}

#[test]
fn list_index_bool_logic_and_negation() {
    // tags is a JSON-array column; flag is a flat Bool column. Rows cover
    // bool elements, null-dropped arrays, and number elements — the
    // three-valued `&&` and unary negation over heterogeneous cells.
    let schema = Arc::new(Schema::new(vec![
        Field::new("tags", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
            (
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_ARRAY.to_string(),
            ),
        ])),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some(r#"[true]"#),
                Some(r#"[false]"#),
                Some(r#"[null]"#),
                Some(r#"[3.5]"#),
                Some(r#"[5]"#),
            ])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true); 5])) as ArrayRef,
        ],
    )
    .unwrap();
    // Bool elements flow through the three-valued `&&` (bool_at over a
    // heterogeneous cell); non-bool elements read null, exactly like
    // `Value::Bool` vs `Value::Number` in the interpreted evaluator.
    let expr = bin(BinOp::And, tags_index(0), field("flag"));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);
    // Unary negation widens Int/Float cells to -n and nulls everything else.
    let expr = bin(BinOp::Eq, Expr::Neg(Box::new(tags_index(0))), num(-5.0));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);
}

#[test]
fn bare_native_list_field_is_structured() {
    // `c.tags` (no index) over native List / LargeList / FixedSizeList
    // columns: a non-null structured value per row — never equal to a
    // scalar, `!=` always true.
    let tags_field = || Expr::Field(FieldRef::Qualified("c".into(), "tags".into()));
    let eq = bin(BinOp::Eq, tags_field(), Expr::StringLit("a".into()));
    let ne = bin(BinOp::Ne, tags_field(), Expr::StringLit("a".into()));

    let values = StringArray::from(vec![Some("a"), None]);
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0i32, 1, 2].into()),
        Arc::new(values) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = native_list_batch(
        Arc::new(list) as ArrayRef,
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
    );
    assert_equiv(&eq, &batch);
    assert_equiv(&ne, &batch);

    let large = LargeListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0i64, 1, 2].into()),
        Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = native_list_batch(
        Arc::new(large) as ArrayRef,
        DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
    );
    assert_equiv(&eq, &batch);
    assert_equiv(&ne, &batch);

    let fixed = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        1,
        Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
        None,
    );
    let batch = native_list_batch(
        Arc::new(fixed) as ArrayRef,
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8, true)), 1),
    );
    assert_equiv(&eq, &batch);
    assert_equiv(&ne, &batch);
}

#[test]
fn list_index_native_list_child_types() {
    // List<Timestamp(Ns)>: timestamp children read as native i64 (the same
    // documented precision as `TimestampNs` columns).
    let ts_values = TimestampNanosecondArray::from(vec![Some(1_700_000_000_000_000i64), Some(2)]);
    let ts_list = ListArray::try_new(
        Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )),
        OffsetBuffer::new(vec![0i32, 2].into()),
        Arc::new(ts_values) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = native_list_batch(
        Arc::new(ts_list) as ArrayRef,
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ))),
    );
    let expr = bin(BinOp::Eq, tags_index(0), num(1_700_000_000_000_000.0));
    assert_equiv(&expr, &batch);
    let expr = bin(BinOp::Eq, tags_index(1), num(2.0));
    assert_equiv(&expr, &batch);

    // List<Binary>: an unsupported child type is dropped before indexing
    // (like `extract_value` → None), so index 0 reads null.
    let bin_list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Binary, true)),
        OffsetBuffer::new(vec![0i32, 1].into()),
        Arc::new(BinaryArray::from(vec![Some(&b"x"[..])])) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = native_list_batch(
        Arc::new(bin_list) as ArrayRef,
        DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
    );
    let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("x".into()));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);
}

#[test]
fn list_index_composes_with_flat_guards() {
    let batch = json_array_batch(
        vec![Some(r#"["prod"]"#), Some(r#"["edge"]"#), None],
        vec![Some(1); 3],
    );
    // tags[0] == "prod" && auction > 0 — the qradar g_tag_prod guard shape.
    let expr = bin(
        BinOp::And,
        bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into())),
        bin(BinOp::Gt, field("auction"), num(0.0)),
    );
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);
}

#[test]
fn mask_to_indices_and_materialize_rows_match_batch_to_events() {
    let auction: Vec<Option<i64>> = vec![Some(0), Some(1), Some(2), Some(3), Some(4)];
    let batch = make_batch(
        auction,
        vec![Some(1.0); 5],
        vec![Some("x"); 5],
        vec![Some(true); 5],
    );
    // auction % 2 == 0 → hits rows 0, 2, 4.
    let expr = bin(
        BinOp::Eq,
        bin(BinOp::Mod, field("auction"), num(2.0)),
        num(0.0),
    );
    let view = ColumnarBatch::from_all_fields(&batch);
    let mask = eval_guard_columnar(&expr, &view);
    let indices = mask_to_indices(&mask);
    assert_eq!(indices, vec![0, 2, 4]);

    let hits = materialize_rows(&batch, &indices);
    let all = batch_to_events(&batch);
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0], all[0]);
    assert_eq!(hits[1], all[2]);
    assert_eq!(hits[2], all[4]);
}

/// 单一权威清单同步：wf-lang 的 `ColumnarFunc` 分类与 wf-engine 的
/// `StrFuncOp` 语义映射必须一致——`StrSearch` 分类 ↔ `StrFuncOp::from_name`
/// 一一对应，防止未来加函数时两处清单 drift。
#[test]
fn strfunc_op_stays_in_sync_with_columnar_func() {
    use wf_lang::columnar::{ColumnarFunc, columnar_func};

    // StrSearch 分类下的每个名字必须有 op；其他分类无 op。
    for name in ["contains", "startswith", "endswith"] {
        assert_eq!(columnar_func(name), Some(ColumnarFunc::StrSearch), "{name}");
        assert!(
            StrFuncOp::from_name(name).is_some(),
            "{name} 应有 StrFuncOp"
        );
    }
    for name in ["cidr_match", "regex_match"] {
        assert!(columnar_func(name).is_some(), "{name}");
        assert!(
            StrFuncOp::from_name(name).is_none(),
            "{name} 不应有 StrFuncOp"
        );
    }
    // 非列式函数两边都不认。
    for name in ["lower", "concat", "startswith_any", "bogus"] {
        assert!(columnar_func(name).is_none(), "{name}");
        assert!(StrFuncOp::from_name(name).is_none(), "{name}");
    }
}

/// `sip` Utf8 column + `count` Int64 column — the cidr_match guard shape.
fn ip_batch(sip: Vec<Option<&str>>, count: Vec<Option<i64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(sip)) as ArrayRef,
            Arc::new(Int64Array::from(count)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn cidr_call(ip: Expr, net: &str) -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: "cidr_match".into(),
        args: vec![ip, Expr::StringLit(net.into())],
    }
}

#[test]
fn cidr_match_matches_interpreted_and_composes() {
    let batch = ip_batch(
        vec![
            Some("10.1.2.3"),   // 10/8 命中
            Some("172.31.0.1"), // 不命中
            Some("fe80::1"),    // v6 与 v4 网段版本不一致
            Some("8.8.8.8"),    // 不命中
            None,               // null
            Some("11.0.0.1"),   // 不命中
        ],
        vec![Some(1), Some(5), Some(2), Some(0), Some(9), Some(7)],
    );
    let expr = cidr_call(field("sip"), "10.0.0.0/8");
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);

    // 组合：cidr_match && count > 1 — 整体列式且逐位一致。
    let combo = bin(BinOp::And, expr, bin(BinOp::Gt, field("count"), num(1.0)));
    assert!(wf_lang::columnar::expr_is_columnar(&combo));
    assert_equiv(&combo, &batch);

    // v6 网段。
    let v6 = cidr_call(field("sip"), "fe80::/10");
    assert!(wf_lang::columnar::expr_is_columnar(&v6));
    assert_equiv(&v6, &batch);

    // 字面量 IP 首参 → 非列式（回落解释器）。
    let lit_ip = cidr_call(Expr::StringLit("10.0.0.1".into()), "10.0.0.0/8");
    assert!(!wf_lang::columnar::expr_is_columnar(&lit_ip));
}

#[test]
fn regex_match_matches_interpreted_and_composes() {
    let batch = ip_batch(
        vec![
            Some("failed_login"), // 命中 fail.*
            Some("success"),      // 不命中
            Some("fail fast"),    // 命中
            Some("login"),        // 不命中
            None,                 // null
            Some("FAILED"),       // 大小写敏感 → 不命中
        ],
        vec![Some(1), Some(5), Some(2), Some(0), Some(9), Some(7)],
    );
    let rm = |arg1: Expr| Expr::FuncCall {
        qualifier: None,
        name: "regex_match".into(),
        args: vec![field("sip"), arg1],
    };
    let expr = rm(Expr::StringLit("fail.*".into()));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);

    // 组合：regex_match && count > 1 — 整体列式且逐位一致。
    let combo = bin(BinOp::And, expr, bin(BinOp::Gt, field("count"), num(1.0)));
    assert!(wf_lang::columnar::expr_is_columnar(&combo));
    assert_equiv(&combo, &batch);

    // 非字面量 pattern → 非列式（回落解释器）。
    let dyn_pat = rm(field("pat"));
    assert!(!wf_lang::columnar::expr_is_columnar(&dyn_pat));
}

/// `action` + `pattern` 双 Utf8 列 + `count` Int64 列 —— contains / startswith
/// / endswith 的两种 needle 形态（字面量 / 字段）都覆盖。
fn str_batch(
    action: Vec<Option<&str>>,
    pattern: Vec<Option<&str>>,
    count: Vec<Option<i64>>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("action", DataType::Utf8, true),
        Field::new("pattern", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(action)) as ArrayRef,
            Arc::new(StringArray::from(pattern)) as ArrayRef,
            Arc::new(Int64Array::from(count)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn str_func_call(name: &str, hay: Expr, needle: Expr) -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args: vec![hay, needle],
    }
}

#[test]
fn str_search_matches_interpreted_literal_and_field_needle() {
    let batch = str_batch(
        vec![
            Some("failed_login"), // 含 "fail"、以 "fail" 开头、以 "login" 结尾
            Some("login_fail"),   // 含 "fail"、不以 "fail" 开头、以 "fail" 结尾
            Some("success"),      // 都不命中
            None,                 // null
            Some("FAILED"),       // 大小写敏感 → 不命中
        ],
        vec![
            Some("fail"),
            Some("login"),
            Some("fail"),
            Some("fail"),
            None,
        ],
        vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
    );
    // 字面量 needle。
    for (name, expected) in [
        ("contains", vec![true, true, false, false, false]),
        ("startswith", vec![true, false, false, false, false]),
        ("endswith", vec![false, true, false, false, false]),
    ] {
        let expr = str_func_call(name, field("action"), Expr::StringLit("fail".into()));
        assert!(
            wf_lang::columnar::expr_is_columnar(&expr),
            "{name} 字面量形态应列式"
        );
        let mask = {
            let view = ColumnarBatch::from_all_fields(&batch);
            eval_guard_columnar(&expr, &view)
        };
        for (row, want) in expected.iter().enumerate() {
            assert_eq!(mask.value(row), *want, "{name} row {row}");
        }
        assert_equiv(&expr, &batch);
    }

    // 字段 needle（pattern 列）：null pattern 行 → null → false。
    let expr = str_func_call("contains", field("action"), field("pattern"));
    assert!(wf_lang::columnar::expr_is_columnar(&expr));
    assert_equiv(&expr, &batch);
    let sw = str_func_call("startswith", field("action"), field("pattern"));
    assert!(wf_lang::columnar::expr_is_columnar(&sw));
    assert_equiv(&sw, &batch);
    let ew = str_func_call("endswith", field("action"), field("pattern"));
    assert!(wf_lang::columnar::expr_is_columnar(&ew));
    assert_equiv(&ew, &batch);

    // 组合：contains(..., "fail") && count > 1 → 整体列式且逐位一致。
    let combo = bin(
        BinOp::And,
        str_func_call("contains", field("action"), Expr::StringLit("fail".into())),
        bin(BinOp::Gt, field("count"), num(1.0)),
    );
    assert!(wf_lang::columnar::expr_is_columnar(&combo));
    assert_equiv(&combo, &batch);

    // 空 needle 语义与解释一致（starts_with("") == true）。
    let empty = str_func_call("contains", field("action"), Expr::StringLit(String::new()));
    assert!(wf_lang::columnar::expr_is_columnar(&empty));
    assert_equiv(&empty, &batch);
}
