use crate::ast::{BinOp, CmpOp, Expr, FieldRef, FieldSelector, Measure, Transform};

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
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            format!(
                "if {} then {} else {}",
                format_expr(cond),
                format_expr(then_expr),
                format_expr(else_expr)
            )
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

pub(super) fn format_field_selector(fs: &FieldSelector) -> String {
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

pub(super) fn format_transform(t: &Transform) -> &'static str {
    match t {
        Transform::Distinct => "distinct",
    }
}

pub(super) fn format_duration(d: &std::time::Duration) -> String {
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
