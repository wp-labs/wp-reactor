use std::fmt;

use crate::ast::{BinOp, CmpOp, Expr, FieldRef, FieldSelector, Measure, Transform};
use crate::plan::{
    AggPlan, BindPlan, BranchPlan, JoinPlan, LimitsPlan, MatchPlan, RulePlan, StepPlan, WindowSpec,
    YieldPlan,
};
use crate::schema::WindowSchema;

/// Human-readable explanation of a compiled rule.
#[derive(Debug)]
pub struct RuleExplanation {
    pub name: String,
    pub bindings: Vec<BindingExpl>,
    pub match_expl: MatchExpl,
    pub score: String,
    pub joins: Vec<String>,
    pub entity_type: String,
    pub entity_id: String,
    pub yield_target: String,
    pub yield_fields: Vec<(String, String)>,
    pub limits: Option<String>,
    pub lineage: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct BindingExpl {
    pub alias: String,
    pub window: String,
    pub filter: Option<String>,
}

#[derive(Debug)]
pub struct MatchExpl {
    pub keys: String,
    pub window_spec: String,
    pub event_steps: Vec<String>,
    pub close_steps: Vec<String>,
}

/// Build explanations for a set of compiled rules.
pub fn explain_rules(plans: &[RulePlan], schemas: &[WindowSchema]) -> Vec<RuleExplanation> {
    plans.iter().map(|p| explain_rule(p, schemas)).collect()
}

fn explain_rule(plan: &RulePlan, schemas: &[WindowSchema]) -> RuleExplanation {
    let bindings = explain_binds(&plan.binds);
    let match_expl = explain_match(&plan.match_plan);
    let score = format_expr(&plan.score_plan.expr);
    let joins = explain_joins(&plan.joins);
    let entity_type = plan.entity_plan.entity_type.clone();
    let entity_id = format_expr(&plan.entity_plan.entity_id_expr);
    let yield_target = plan.yield_plan.target.clone();
    let yield_fields = explain_yield(&plan.yield_plan);
    let limits = plan.limits_plan.as_ref().map(explain_limits);
    let lineage = compute_lineage(&plan.binds, &plan.yield_plan, schemas);

    RuleExplanation {
        name: plan.name.clone(),
        bindings,
        match_expl,
        score,
        joins,
        entity_type,
        entity_id,
        yield_target,
        yield_fields,
        limits,
        lineage,
    }
}

// ---------------------------------------------------------------------------
// Bindings
// ---------------------------------------------------------------------------

fn explain_binds(binds: &[BindPlan]) -> Vec<BindingExpl> {
    binds
        .iter()
        .map(|b| BindingExpl {
            alias: b.alias.clone(),
            window: b.window.clone(),
            filter: b.filter.as_ref().map(format_expr),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Match
// ---------------------------------------------------------------------------

fn explain_match(mp: &MatchPlan) -> MatchExpl {
    let keys = if mp.keys.is_empty() {
        "(none)".to_string()
    } else {
        mp.keys
            .iter()
            .map(format_field_ref)
            .collect::<Vec<_>>()
            .join(", ")
    };

    let window_spec = match &mp.window_spec {
        WindowSpec::Sliding(d) => format!("sliding {}", format_duration(d)),
    };

    let event_steps = mp.event_steps.iter().map(format_step).collect();
    let close_steps = mp.close_steps.iter().map(format_step).collect();

    MatchExpl {
        keys,
        window_spec,
        event_steps,
        close_steps,
    }
}

fn format_step(step: &StepPlan) -> String {
    step.branches
        .iter()
        .map(format_branch)
        .collect::<Vec<_>>()
        .join(" || ")
}

fn format_branch(branch: &BranchPlan) -> String {
    let mut parts = Vec::new();

    if let Some(ref label) = branch.label {
        parts.push(format!("{}:", label));
    }

    let mut source = branch.source.clone();
    if let Some(ref field) = branch.field {
        source.push_str(&format_field_selector(field));
    }
    parts.push(source);

    if let Some(ref guard) = branch.guard {
        parts.push(format!("&& {}", format_expr(guard)));
    }

    parts.push(format!("|{}", format_agg(&branch.agg)));

    parts.join(" ")
}

fn format_agg(agg: &AggPlan) -> String {
    let mut chain = String::new();
    for t in &agg.transforms {
        chain.push_str(&format!(" {} |", format_transform(t)));
    }
    chain.push_str(&format!(
        " {} {} {}",
        format_measure(agg.measure),
        format_cmp(agg.cmp),
        format_expr(&agg.threshold)
    ));
    chain
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

fn explain_joins(joins: &[JoinPlan]) -> Vec<String> {
    joins
        .iter()
        .map(|j| {
            let mode = match &j.mode {
                crate::ast::JoinMode::Snapshot => "snapshot".to_string(),
                crate::ast::JoinMode::Asof { within: None } => "asof".to_string(),
                crate::ast::JoinMode::Asof { within: Some(d) } => {
                    format!("asof within {}", format_duration(d))
                }
            };
            let conds: Vec<String> = j
                .conds
                .iter()
                .map(|c| {
                    format!(
                        "{} == {}",
                        format_field_ref(&c.left),
                        format_field_ref(&c.right)
                    )
                })
                .collect();
            format!("join {} {} on {}", j.right_window, mode, conds.join(" && "))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

fn explain_limits(lp: &LimitsPlan) -> String {
    let mut parts = Vec::new();
    if let Some(max_state) = lp.max_state_bytes {
        parts.push(format!("max_state={}B", max_state));
    }
    if let Some(max_card) = lp.max_cardinality {
        parts.push(format!("max_cardinality={}", max_card));
    }
    if let Some(ref rate) = lp.max_emit_rate {
        parts.push(format!(
            "max_emit_rate={}/{}",
            rate.count,
            format_duration(&rate.per)
        ));
    }
    parts.push(format!("on_exceed={:?}", lp.on_exceed));
    parts.join(", ")
}

// ---------------------------------------------------------------------------
// Yield + lineage
// ---------------------------------------------------------------------------

fn explain_yield(yp: &YieldPlan) -> Vec<(String, String)> {
    yp.fields
        .iter()
        .map(|f| (f.name.clone(), format_expr(&f.value)))
        .collect()
}

fn compute_lineage(
    binds: &[BindPlan],
    yield_plan: &YieldPlan,
    _schemas: &[WindowSchema],
) -> Vec<(String, String)> {
    yield_plan
        .fields
        .iter()
        .map(|f| {
            let origin = trace_field_origin(&f.value, binds);
            (f.name.clone(), origin)
        })
        .collect()
}

fn trace_field_origin(expr: &Expr, binds: &[BindPlan]) -> String {
    match expr {
        Expr::Field(FieldRef::Qualified(alias, field)) => {
            let window = binds
                .iter()
                .find(|b| b.alias == *alias)
                .map(|b| b.window.as_str())
                .unwrap_or("?");
            format!("{}.{} (via {})", window, field, alias)
        }
        Expr::Field(FieldRef::Simple(name)) => {
            if let Some(bind) = binds.iter().find(|b| b.alias == *name) {
                format!("set-level ref to {}", bind.window)
            } else {
                format!("field `{}`", name)
            }
        }
        Expr::FuncCall { name, args, .. } => {
            let arg_str = args.iter().map(format_expr).collect::<Vec<_>>().join(", ");
            let inner = args.first().map(|a| trace_field_origin(a, binds));
            match inner {
                Some(origin) => format!("{}({}) over {}", name, arg_str, origin),
                None => format!("{}()", name),
            }
        }
        _ => format_expr(expr),
    }
}

// ---------------------------------------------------------------------------
// Expression formatting
// ---------------------------------------------------------------------------

pub fn format_expr(expr: &Expr) -> String {
    match expr {
        Expr::Number(n) => {
            if n.fract() == 0.0 {
                format!("{:.1}", n)
            } else {
                format!("{}", n)
            }
        }
        Expr::StringLit(s) => format!("\"{}\"", s),
        Expr::Bool(b) => format!("{}", b),
        Expr::Field(fref) => format_field_ref(fref),
        Expr::BinOp { op, left, right } => {
            format!(
                "{} {} {}",
                format_expr(left),
                format_binop(*op),
                format_expr(right)
            )
        }
        Expr::Neg(inner) => format!("-{}", format_expr(inner)),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => {
            let args_str = args.iter().map(format_expr).collect::<Vec<_>>().join(", ");
            match qualifier {
                Some(q) => format!("{}.{}({})", q, name, args_str),
                None => format!("{}({})", name, args_str),
            }
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let items = list.iter().map(format_expr).collect::<Vec<_>>().join(", ");
            let kw = if *negated { "not in" } else { "in" };
            format!("{} {} ({})", format_expr(inner), kw, items)
        }
    }
}

pub fn format_field_ref(fref: &FieldRef) -> String {
    match fref {
        FieldRef::Simple(name) => name.clone(),
        FieldRef::Qualified(alias, field) => format!("{}.{}", alias, field),
        FieldRef::Bracketed(alias, key) => format!("{}[\"{}\"]", alias, key),
    }
}

fn format_field_selector(fs: &FieldSelector) -> String {
    match fs {
        FieldSelector::Dot(name) => format!(".{}", name),
        FieldSelector::Bracket(name) => format!("[\"{}\"]", name),
    }
}

fn format_binop(op: BinOp) -> &'static str {
    match op {
        BinOp::And => "&&",
        BinOp::Or => "||",
        BinOp::Eq => "==",
        BinOp::Ne => "!=",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::Le => "<=",
        BinOp::Ge => ">=",
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
    }
}

pub fn format_cmp(cmp: CmpOp) -> &'static str {
    match cmp {
        CmpOp::Eq => "==",
        CmpOp::Ne => "!=",
        CmpOp::Lt => "<",
        CmpOp::Gt => ">",
        CmpOp::Le => "<=",
        CmpOp::Ge => ">=",
    }
}

pub fn format_measure(m: Measure) -> &'static str {
    match m {
        Measure::Count => "count",
        Measure::Sum => "sum",
        Measure::Avg => "avg",
        Measure::Min => "min",
        Measure::Max => "max",
    }
}

fn format_transform(t: &Transform) -> &'static str {
    match t {
        Transform::Distinct => "distinct",
    }
}

fn format_duration(d: &std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs == 0 {
        return "0s".to_string();
    }
    if secs.is_multiple_of(86400) {
        format!("{}d", secs / 86400)
    } else if secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{}s", secs)
    }
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

impl fmt::Display for RuleExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Rule: {}", self.name)?;

        // Bindings
        writeln!(f, "  Bindings:")?;
        for b in &self.bindings {
            match &b.filter {
                Some(filter) => {
                    writeln!(f, "    {} -> {}  [filter: {}]", b.alias, b.window, filter)?
                }
                None => writeln!(f, "    {} -> {}", b.alias, b.window)?,
            }
        }

        // Match
        writeln!(
            f,
            "  Match <{}> {}:",
            self.match_expl.keys, self.match_expl.window_spec
        )?;
        if !self.match_expl.event_steps.is_empty() {
            writeln!(f, "    on event:")?;
            for (i, step) in self.match_expl.event_steps.iter().enumerate() {
                writeln!(f, "      step {}: {}", i + 1, step)?;
            }
        }
        if !self.match_expl.close_steps.is_empty() {
            writeln!(f, "    on close:")?;
            for (i, step) in self.match_expl.close_steps.iter().enumerate() {
                writeln!(f, "      step {}: {}", i + 1, step)?;
            }
        }

        // Score
        writeln!(f, "  Score: {}", self.score)?;

        // Joins
        if !self.joins.is_empty() {
            writeln!(f, "  Joins:")?;
            for j in &self.joins {
                writeln!(f, "    {}", j)?;
            }
        }

        // Entity
        writeln!(f, "  Entity: {} = {}", self.entity_type, self.entity_id)?;

        // Yield
        writeln!(f, "  Yield -> {}:", self.yield_target)?;
        for (name, value) in &self.yield_fields {
            writeln!(
                f,
                "    {:width$} = {}",
                name,
                value,
                width = max_field_width(&self.yield_fields)
            )?;
        }

        // Lineage
        if !self.lineage.is_empty() {
            writeln!(f, "  Field Lineage:")?;
            for (name, origin) in &self.lineage {
                writeln!(
                    f,
                    "    {:width$} <- {}",
                    name,
                    origin,
                    width = max_field_width(&self.lineage)
                )?;
            }
        }

        // Limits
        if let Some(ref limits) = self.limits {
            writeln!(f, "  Limits: {}", limits)?;
        }

        Ok(())
    }
}

fn max_field_width(fields: &[(String, String)]) -> usize {
    fields.iter().map(|(n, _)| n.len()).max().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::compile_wfl;
    use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
    use crate::wfl_parser::parse_wfl;

    use super::*;

    fn bt(b: BaseType) -> FieldType {
        FieldType::Base(b)
    }

    fn auth_events_window() -> WindowSchema {
        WindowSchema {
            name: "auth_events".to_string(),
            streams: vec!["auth_stream".to_string()],
            time_field: Some("event_time".to_string()),
            over: Duration::from_secs(3600),
            fields: vec![
                FieldDef {
                    name: "sip".to_string(),
                    field_type: bt(BaseType::Ip),
                },
                FieldDef {
                    name: "action".to_string(),
                    field_type: bt(BaseType::Chars),
                },
                FieldDef {
                    name: "event_time".to_string(),
                    field_type: bt(BaseType::Time),
                },
            ],
        }
    }

    fn security_alerts_window() -> WindowSchema {
        WindowSchema {
            name: "security_alerts".to_string(),
            streams: vec![],
            time_field: None,
            over: Duration::from_secs(3600),
            fields: vec![
                FieldDef {
                    name: "sip".to_string(),
                    field_type: bt(BaseType::Ip),
                },
                FieldDef {
                    name: "fail_count".to_string(),
                    field_type: bt(BaseType::Digit),
                },
                FieldDef {
                    name: "message".to_string(),
                    field_type: bt(BaseType::Chars),
                },
            ],
        }
    }

    #[test]
    fn explain_brute_force_rule() {
        let input = r#"
rule brute_force_then_scan {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
        on close {
            fail | count >= 1;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} brute force detected", fail.sip)
    )
}
"#;
        let schemas = &[auth_events_window(), security_alerts_window()];
        let file = parse_wfl(input).unwrap();
        let plans = compile_wfl(&file, schemas).unwrap();
        let explanations = explain_rules(&plans, schemas);

        assert_eq!(explanations.len(), 1);
        let expl = &explanations[0];
        assert_eq!(expl.name, "brute_force_then_scan");
        assert_eq!(expl.bindings.len(), 1);
        assert_eq!(expl.bindings[0].alias, "fail");
        assert_eq!(expl.bindings[0].window, "auth_events");
        assert!(expl.bindings[0].filter.is_some());

        assert_eq!(expl.match_expl.event_steps.len(), 1);
        assert_eq!(expl.match_expl.close_steps.len(), 1);
        assert_eq!(expl.score, "70.0");
        assert_eq!(expl.entity_type, "ip");
        assert_eq!(expl.entity_id, "fail.sip");
        assert_eq!(expl.yield_target, "security_alerts");
        assert_eq!(expl.yield_fields.len(), 3);

        // Verify Display output
        let output = format!("{}", expl);
        assert!(output.contains("Rule: brute_force_then_scan"));
        assert!(output.contains("fail -> auth_events"));
        assert!(output.contains("action == \"failed\""));
        assert!(output.contains("Score: 70.0"));
        assert!(output.contains("Entity: ip = fail.sip"));
        assert!(output.contains("sip"));
        assert!(output.contains("Field Lineage:"));
    }

    #[test]
    fn format_expr_variants() {
        assert_eq!(format_expr(&Expr::Number(42.0)), "42.0");
        assert_eq!(format_expr(&Expr::Number(3.24)), "3.24");
        assert_eq!(format_expr(&Expr::StringLit("hello".into())), "\"hello\"");
        assert_eq!(format_expr(&Expr::Bool(true)), "true");
        assert_eq!(
            format_expr(&Expr::Field(FieldRef::Qualified("a".into(), "b".into()))),
            "a.b"
        );
        assert_eq!(
            format_expr(&Expr::FuncCall {
                qualifier: None,
                name: "count".into(),
                args: vec![Expr::Field(FieldRef::Simple("fail".into()))]
            }),
            "count(fail)"
        );
    }
}
