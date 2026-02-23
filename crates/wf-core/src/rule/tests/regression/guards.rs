use super::*;

// ---------------------------------------------------------------------------
// Guards: InList / FuncCall (22–25)
// ---------------------------------------------------------------------------

#[test]
fn guard_in_list() {
    // Guard `action in ("login", "logout")` should pass; "upload" should fail.
    let guard = Expr::InList {
        expr: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
        list: vec![
            Expr::StringLit("login".to_string()),
            Expr::StringLit("logout".to_string()),
        ],
        negated: false,
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "auth".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(2.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule22".to_string(), plan, None);

    // "upload" not in list → skipped
    let upload = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("upload")),
    ]);
    assert_eq!(sm.advance("auth", &upload), StepResult::Accumulate);
    assert_eq!(sm.advance("auth", &upload), StepResult::Accumulate);

    // "login" in list → counted
    let login = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("login")),
    ]);
    assert_eq!(sm.advance("auth", &login), StepResult::Accumulate);
    assert!(matches!(sm.advance("auth", &login), StepResult::Matched(_)));
}

#[test]
fn guard_not_in_list() {
    // Guard `action not in ("success")` — should count everything except "success".
    let guard = Expr::InList {
        expr: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
        list: vec![Expr::StringLit("success".to_string())],
        negated: true,
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "auth".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(1.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule23".to_string(), plan, None);

    // "success" → filtered out by NOT IN
    let ok = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("success")),
    ]);
    assert_eq!(sm.advance("auth", &ok), StepResult::Accumulate);

    // "failed" → passes NOT IN → matched
    let fail = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("failed")),
    ]);
    assert!(matches!(sm.advance("auth", &fail), StepResult::Matched(_)));
}

#[test]
fn guard_func_call_contains() {
    // Guard: `contains(cmd, "powershell")`
    let guard = Expr::FuncCall {
        qualifier: None,
        name: "contains".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("cmd".to_string())),
            Expr::StringLit("powershell".to_string()),
        ],
    };

    let plan = simple_plan(
        vec![simple_key("host")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "proc".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(1.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule24".to_string(), plan, None);

    // cmd without "powershell" → skipped
    let notepad = event(vec![
        ("host", str_val("srv1")),
        ("cmd", str_val("notepad.exe")),
    ]);
    assert_eq!(sm.advance("proc", &notepad), StepResult::Accumulate);

    // cmd with "powershell" → matched
    let ps = event(vec![
        ("host", str_val("srv1")),
        ("cmd", str_val("powershell -enc abc")),
    ]);
    assert!(matches!(sm.advance("proc", &ps), StepResult::Matched(_)));
}

#[test]
fn guard_func_lower_in_list() {
    // Guard: `lower(proto) in ("tcp", "udp")` — tests nested FuncCall + InList
    let guard = Expr::InList {
        expr: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "lower".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("proto".to_string()))],
        }),
        list: vec![
            Expr::StringLit("tcp".to_string()),
            Expr::StringLit("udp".to_string()),
        ],
        negated: false,
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "conn".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(1.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule25".to_string(), plan, None);

    // "ICMP" → lower → "icmp" → not in list → skipped
    let icmp = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("proto", str_val("ICMP")),
    ]);
    assert_eq!(sm.advance("conn", &icmp), StepResult::Accumulate);

    // "TCP" → lower → "tcp" → in list → matched
    let tcp = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("proto", str_val("TCP")),
    ]);
    assert!(matches!(sm.advance("conn", &tcp), StepResult::Matched(_)));
}
