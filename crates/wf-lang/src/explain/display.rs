use std::fmt;

use super::RuleExplanation;

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
