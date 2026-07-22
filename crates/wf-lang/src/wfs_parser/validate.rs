use std::time::Duration;

use crate::schema::{BaseType, FieldDef, FieldType, StaticWindowSchema, WindowSchema};
use crate::{LangReason, LangResult};
use orion_error::conversion::ToStructError;

pub(super) fn validate_schemas(windows: &[WindowSchema]) -> LangResult<()> {
    // Check window name uniqueness
    let mut seen = std::collections::HashSet::new();
    for w in windows {
        if !seen.insert(&w.name) {
            return LangReason::Validation
                .to_err()
                .with_detail(format!("duplicate window name: '{}'", w.name))
                .err();
        }
    }

    for w in windows {
        if w.over > Duration::ZERO {
            // time attribute is required
            let Some(time_field) = w.time_field.as_ref() else {
                return LangReason::Validation
                    .to_err()
                    .with_detail(format!(
                        "window '{}': over > 0 requires a 'time' attribute",
                        w.name
                    ))
                    .err();
            };
            // Referenced field must exist
            let field = w.fields.iter().find(|f| f.name == *time_field);
            let Some(field) = field else {
                return LangReason::Validation
                    .to_err()
                    .with_detail(format!(
                        "window '{}': time field '{}' not found in fields",
                        w.name, time_field
                    ))
                    .err();
            };
            // Must be of type time
            if field.field_type != FieldType::Base(BaseType::Time) {
                return LangReason::Validation
                    .to_err()
                    .with_detail(format!(
                        "window '{}': time field '{}' must have type 'time', got {:?}",
                        w.name, time_field, field.field_type
                    ))
                    .err();
            }
        }
    }

    Ok(())
}

pub(super) fn validate_static_schemas(windows: &[StaticWindowSchema]) -> LangResult<()> {
    let mut seen = std::collections::HashSet::new();
    for w in windows {
        if !seen.insert(&w.name) {
            return LangReason::Validation
                .to_err()
                .with_detail(format!("duplicate window name: '{}'", w.name))
                .err();
        }
        validate_provider_fields(&w.name, &w.fields)?;
    }
    Ok(())
}

fn validate_provider_fields(window_name: &str, fields: &[FieldDef]) -> LangResult<()> {
    for field in fields {
        if is_structured_type(&field.field_type) {
            return LangReason::Validation
                .to_err()
                .with_detail(format!(
                    "window '{}': provider field '{}' uses structured type {:?}; provider object/array fields are not supported yet",
                    window_name, field.name, field.field_type
                ))
                .err();
        }
    }
    Ok(())
}

fn is_structured_type(field_type: &FieldType) -> bool {
    matches!(
        field_type,
        FieldType::ArrayAny | FieldType::Array(_) | FieldType::Object
    )
}
