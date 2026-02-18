use std::time::Duration;

use crate::schema::{BaseType, FieldType, WindowSchema};

pub(super) fn validate_schemas(windows: &[WindowSchema]) -> anyhow::Result<()> {
    // Check window name uniqueness
    let mut seen = std::collections::HashSet::new();
    for w in windows {
        if !seen.insert(&w.name) {
            anyhow::bail!("duplicate window name: '{}'", w.name);
        }
    }

    for w in windows {
        if w.over > Duration::ZERO {
            // time attribute is required
            let time_field = w.time_field.as_ref().ok_or_else(|| {
                anyhow::anyhow!("window '{}': over > 0 requires a 'time' attribute", w.name)
            })?;
            // Referenced field must exist
            let field = w
                .fields
                .iter()
                .find(|f| f.name == *time_field)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "window '{}': time field '{}' not found in fields",
                        w.name,
                        time_field
                    )
                })?;
            // Must be of type time
            if field.field_type != FieldType::Base(BaseType::Time) {
                anyhow::bail!(
                    "window '{}': time field '{}' must have type 'time', got {:?}",
                    w.name,
                    time_field,
                    field.field_type
                );
            }
        }
    }

    Ok(())
}
