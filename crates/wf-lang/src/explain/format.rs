use crate::ast::{
    BinOp, CmpOp, Expr, FieldRef, FieldSelector, Measure, PathSegment, SystemVar, Transform,
};
use crate::schema::{BaseType, FieldType};

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
        Expr::SystemVar(var) => format_system_var(*var).to_string(),
        Expr::WfuMeta(field) => format!("@{}", field.name()),
        Expr::Field(fref) => format_field_ref(fref),
        Expr::PresetParam(name) => format!("${name}"),
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
        Expr::Object(items) => {
            let items = items
                .iter()
                .map(|item| {
                    let targets = item.targets.join(", ");
                    let type_hint = item
                        .type_hint
                        .as_ref()
                        .map(|ty| format!(": {}", format_field_type(ty)))
                        .unwrap_or_default();
                    format!("{}{} = {}", targets, type_hint, format_expr(&item.value))
                })
                .collect::<Vec<_>>()
                .join("; ");
            format!("object {{ {} }}", items)
        }
        Expr::Array(items) => {
            let items = items.iter().map(format_expr).collect::<Vec<_>>().join(", ");
            format!("array [{}]", items)
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

fn format_system_var(var: SystemVar) -> &'static str {
    match var {
        SystemVar::Score => "@score",
        SystemVar::EventFirstTime => "@event_first_time",
        SystemVar::EventLastTime => "@event_last_time",
        SystemVar::EvidenceStartTime => "@evidence_start_time",
        SystemVar::EvidenceEndTime => "@evidence_end_time",
        SystemVar::WindowStartTime => "@window_start_time",
        SystemVar::WindowEndTime => "@window_end_time",
        SystemVar::EmitTime => "@emit_time",
    }
}

fn format_field_type(field_type: &FieldType) -> String {
    match field_type {
        FieldType::Base(base) => format_base_type(base).to_string(),
        FieldType::ArrayAny => "array".to_string(),
        FieldType::Array(base) => format!("array/{}", format_base_type(base)),
        FieldType::Object => "object".to_string(),
    }
}

fn format_base_type(base: &BaseType) -> &'static str {
    match base {
        BaseType::Chars => "chars",
        BaseType::Digit => "digit",
        BaseType::Float => "float",
        BaseType::Bool => "bool",
        BaseType::Time => "time",
        BaseType::Ip => "ip",
        BaseType::Hex => "hex",
    }
}

/// Render a nested path's segments as `member[0].member` — a `.member` step is
/// dot-joined, an `[index]` step is not, so there is no stray dot before a
/// bracket. Shared by explain, key-output naming, and the compiler.
pub(crate) fn format_path_segments(segments: &[PathSegment]) -> String {
    let mut s = String::new();
    for seg in segments {
        match seg {
            PathSegment::Field(name) => {
                if !s.is_empty() {
                    s.push('.');
                }
                s.push_str(name);
            }
            PathSegment::Index(idx) => s.push_str(&format!("[{idx}]")),
        }
    }
    s
}

pub fn format_field_ref(fref: &FieldRef) -> String {
    match fref {
        FieldRef::Simple(name) => name.clone(),
        FieldRef::Qualified(alias, field) => format!("{}.{}", alias, field),
        FieldRef::Bracketed(alias, key) => format!("{}[\"{}\"]", alias, key),
        FieldRef::Path { alias, segments } => {
            let segs = format_path_segments(segments);
            if segs.is_empty() {
                alias.clone()
            } else {
                format!("{}.{}", alias, segs)
            }
        }
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
    let millis = d.as_millis();
    let secs = d.as_secs();
    if millis == 0 {
        return "0s".to_string();
    }
    if d.subsec_nanos() != 0 {
        return format!("{millis}ms");
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
