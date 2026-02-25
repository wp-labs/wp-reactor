use super::*;

// ---------------------------------------------------------------------------
// Helper: build a CloseOutput for conv testing
// ---------------------------------------------------------------------------

fn make_close_output(
    scope_key: Vec<Value>,
    event_step_data: Vec<StepData>,
    close_step_data: Vec<StepData>,
) -> CloseOutput {
    CloseOutput {
        rule_name: "test".to_string(),
        scope_key,
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        event_step_data,
        close_step_data,
        watermark_nanos: 0,
        last_event_nanos: 0,
    }
}

fn labeled_step(label: &str, value: f64) -> StepData {
    StepData {
        satisfied_branch_index: 0,
        label: Some(label.to_string()),
        measure_value: value,
    }
}

fn make_conv_plan(chains: Vec<Vec<ConvOpPlan>>) -> ConvPlan {
    ConvPlan {
        chains: chains
            .into_iter()
            .map(|ops| ConvChainPlan { ops })
            .collect(),
    }
}

// ===========================================================================
// Sort descending
// ===========================================================================

#[test]
fn conv_sort_descending() {
    let outputs = vec![
        make_close_output(
            vec![Value::Str("10.0.0.1".into())],
            vec![labeled_step("count", 3.0)],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("10.0.0.2".into())],
            vec![labeled_step("count", 10.0)],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("10.0.0.3".into())],
            vec![labeled_step("count", 7.0)],
            vec![],
        ),
    ];

    let plan = make_conv_plan(vec![vec![ConvOpPlan::Sort(vec![SortKeyPlan {
        expr: Expr::Field(FieldRef::Simple("count".into())),
        descending: true,
    }])]]);

    let keys = vec![FieldRef::Simple("sip".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, outputs);

    assert_eq!(result.len(), 3);
    // Highest first (10.0, 7.0, 3.0)
    assert_eq!(result[0].event_step_data[0].measure_value, 10.0);
    assert_eq!(result[1].event_step_data[0].measure_value, 7.0);
    assert_eq!(result[2].event_step_data[0].measure_value, 3.0);
}

// ===========================================================================
// Sort ascending
// ===========================================================================

#[test]
fn conv_sort_ascending() {
    let outputs = vec![
        make_close_output(
            vec![Value::Str("c".into())],
            vec![labeled_step("score", 9.0)],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("a".into())],
            vec![labeled_step("score", 1.0)],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("b".into())],
            vec![labeled_step("score", 5.0)],
            vec![],
        ),
    ];

    let plan = make_conv_plan(vec![vec![ConvOpPlan::Sort(vec![SortKeyPlan {
        expr: Expr::Field(FieldRef::Simple("score".into())),
        descending: false,
    }])]]);

    let keys = vec![FieldRef::Simple("sip".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, outputs);

    assert_eq!(result[0].event_step_data[0].measure_value, 1.0);
    assert_eq!(result[1].event_step_data[0].measure_value, 5.0);
    assert_eq!(result[2].event_step_data[0].measure_value, 9.0);
}

// ===========================================================================
// Top N
// ===========================================================================

#[test]
fn conv_top_n() {
    let outputs: Vec<CloseOutput> = (0..5)
        .map(|i| {
            make_close_output(
                vec![Value::Str(format!("ip{}", i))],
                vec![labeled_step("count", i as f64)],
                vec![],
            )
        })
        .collect();

    let plan = make_conv_plan(vec![vec![ConvOpPlan::Top(3)]]);
    let keys = vec![FieldRef::Simple("sip".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, outputs);

    assert_eq!(result.len(), 3);
}

// ===========================================================================
// Dedup
// ===========================================================================

#[test]
fn conv_dedup() {
    let outputs = vec![
        make_close_output(
            vec![Value::Str("10.0.0.1".into())],
            vec![labeled_step("count", 5.0)],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("10.0.0.1".into())],
            vec![labeled_step("count", 3.0)],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("10.0.0.2".into())],
            vec![labeled_step("count", 7.0)],
            vec![],
        ),
    ];

    let plan = make_conv_plan(vec![vec![ConvOpPlan::Dedup(Expr::Field(
        FieldRef::Simple("sip".into()),
    ))]]);

    let keys = vec![FieldRef::Simple("sip".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, outputs);

    // First entry per unique sip → two entries: 10.0.0.1 (first) and 10.0.0.2
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].scope_key[0], Value::Str("10.0.0.1".into()));
    assert_eq!(result[1].scope_key[0], Value::Str("10.0.0.2".into()));
    // The first 10.0.0.1 entry (count=5.0) is kept, second (count=3.0) is removed
    assert_eq!(result[0].event_step_data[0].measure_value, 5.0);
}

// ===========================================================================
// Where filter
// ===========================================================================

#[test]
fn conv_where_filter() {
    let outputs = vec![
        make_close_output(
            vec![Value::Str("10.0.0.1".into())],
            vec![labeled_step("count", 3.0)],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("10.0.0.2".into())],
            vec![labeled_step("count", 10.0)],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("10.0.0.3".into())],
            vec![labeled_step("count", 1.0)],
            vec![],
        ),
    ];

    // where(count > 5)
    let plan = make_conv_plan(vec![vec![ConvOpPlan::Where(Expr::BinOp {
        op: BinOp::Gt,
        left: Box::new(Expr::Field(FieldRef::Simple("count".into()))),
        right: Box::new(Expr::Number(5.0)),
    })]]);

    let keys = vec![FieldRef::Simple("sip".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, outputs);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].scope_key[0], Value::Str("10.0.0.2".into()));
}

// ===========================================================================
// Pipeline: sort + top
// ===========================================================================

#[test]
fn conv_chain_pipeline_sort_top() {
    let outputs: Vec<CloseOutput> = (0..10)
        .map(|i| {
            make_close_output(
                vec![Value::Str(format!("ip{}", i))],
                vec![labeled_step("score", i as f64)],
                vec![],
            )
        })
        .collect();

    let plan = make_conv_plan(vec![vec![
        ConvOpPlan::Sort(vec![SortKeyPlan {
            expr: Expr::Field(FieldRef::Simple("score".into())),
            descending: true,
        }]),
        ConvOpPlan::Top(3),
    ]]);

    let keys = vec![FieldRef::Simple("sip".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, outputs);

    // Top 3 by score descending: 9.0, 8.0, 7.0
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].event_step_data[0].measure_value, 9.0);
    assert_eq!(result[1].event_step_data[0].measure_value, 8.0);
    assert_eq!(result[2].event_step_data[0].measure_value, 7.0);
}

// ===========================================================================
// Multiple chains: applied sequentially
// ===========================================================================

#[test]
fn conv_multiple_chains_sequential() {
    let outputs: Vec<CloseOutput> = (0..5)
        .map(|i| {
            make_close_output(
                vec![Value::Str(format!("ip{}", i))],
                vec![labeled_step("score", i as f64)],
                vec![],
            )
        })
        .collect();

    // Chain 1: sort by score descending → [4, 3, 2, 1, 0]
    // Chain 2: top(2) → [4, 3]
    let plan = make_conv_plan(vec![
        vec![ConvOpPlan::Sort(vec![SortKeyPlan {
            expr: Expr::Field(FieldRef::Simple("score".into())),
            descending: true,
        }])],
        vec![ConvOpPlan::Top(2)],
    ]);

    let keys = vec![FieldRef::Simple("sip".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, outputs);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].event_step_data[0].measure_value, 4.0);
    assert_eq!(result[1].event_step_data[0].measure_value, 3.0);
}

// ===========================================================================
// Integration: scan_expired_at_with_conv
// ===========================================================================

#[test]
fn scan_expired_at_with_conv_applies_transformations() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan(
        vec![simple_key("sip")],
        dur,
        vec![step(vec![branch_with_label("fail", "count", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r_conv".to_string(), plan, None);

    // Feed events for 3 different IPs in same bucket [0, 10s)
    for (i, ip) in ["10.0.0.1", "10.0.0.2", "10.0.0.3"].iter().enumerate() {
        let e = event(vec![("sip", str_val(ip))]);
        // Feed enough events to satisfy count >= 1
        sm.advance_at("fail", &e, (i as i64 + 1) * 1_000_000_000);
    }

    // Create conv plan: sort by count descending, top 2
    let conv_plan = ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![
                ConvOpPlan::Sort(vec![SortKeyPlan {
                    expr: Expr::Field(FieldRef::Simple("count".into())),
                    descending: true,
                }]),
                ConvOpPlan::Top(2),
            ],
        }],
    };

    // Expire all at t=10s
    let results = sm.scan_expired_at_with_conv(10_000_000_000, Some(&conv_plan));

    // All 3 instances expire, conv truncates to 2
    assert_eq!(results.len(), 2);
}

// ===========================================================================
// scan_expired_at_with_conv with no conv plan → no transformation
// ===========================================================================

#[test]
fn scan_expired_at_with_conv_none_passthrough() {
    let dur = Duration::from_secs(10);
    let plan = fixed_plan(
        vec![simple_key("sip")],
        dur,
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r_conv".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("fail", &e, 1_000_000_000);

    let results = sm.scan_expired_at_with_conv(10_000_000_000, None);
    assert_eq!(results.len(), 1);
}

// ===========================================================================
// Empty outputs → conv is a no-op
// ===========================================================================

#[test]
fn conv_empty_outputs_noop() {
    let plan = make_conv_plan(vec![vec![ConvOpPlan::Top(5)]]);
    let keys = vec![FieldRef::Simple("sip".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, vec![]);
    assert!(result.is_empty());
}

// ===========================================================================
// Sort by scope key field
// ===========================================================================

#[test]
fn conv_sort_by_scope_key() {
    let outputs = vec![
        make_close_output(
            vec![Value::Str("charlie".into())],
            vec![],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("alpha".into())],
            vec![],
            vec![],
        ),
        make_close_output(
            vec![Value::Str("bravo".into())],
            vec![],
            vec![],
        ),
    ];

    let plan = make_conv_plan(vec![vec![ConvOpPlan::Sort(vec![SortKeyPlan {
        expr: Expr::Field(FieldRef::Simple("name".into())),
        descending: false,
    }])]]);

    let keys = vec![FieldRef::Simple("name".into())];
    let result = crate::rule::match_engine::apply_conv(&plan, &keys, outputs);

    assert_eq!(result[0].scope_key[0], Value::Str("alpha".into()));
    assert_eq!(result[1].scope_key[0], Value::Str("bravo".into()));
    assert_eq!(result[2].scope_key[0], Value::Str("charlie".into()));
}
