use std::collections::{BTreeMap, BTreeSet};

use crate::ast::WflFile;

use super::{CheckError, Severity};

/// Per-version entry: rule name + its yield field names.
type VersionEntry = Vec<(String, BTreeSet<String>)>;

/// T52: Check for field differences between adjacent yield versions
/// targeting the same output window.
///
/// Reports:
/// - **Warning** when a higher version *adds* new fields (consumers need to adapt)
/// - **Warning** when a higher version *removes* fields (consumers may depend on them)
///
/// Rules without an explicit `@vN` version are skipped.
pub fn check_yield_versions(file: &WflFile, errors: &mut Vec<CheckError>) {
    // Group rules by yield target, then by version.
    let mut by_target: BTreeMap<String, BTreeMap<u32, VersionEntry>> = BTreeMap::new();

    for rule in &file.rules {
        let yc = &rule.yield_clause;
        let Some(version) = yc.version else {
            continue; // skip rules without explicit version
        };
        let field_names: BTreeSet<String> = yc.args.iter().map(|a| a.name.clone()).collect();
        by_target
            .entry(yc.target.clone())
            .or_default()
            .entry(version)
            .or_default()
            .push((rule.name.clone(), field_names));
    }

    // Compare adjacent versions
    for (target, versions) in &by_target {
        let version_keys: Vec<u32> = versions.keys().copied().collect();
        for pair in version_keys.windows(2) {
            let (v_lo, v_hi) = (pair[0], pair[1]);
            let lo_entries = &versions[&v_lo];
            let hi_entries = &versions[&v_hi];

            // Use the union of fields from all rules at each version
            let lo_fields: BTreeSet<&str> = lo_entries
                .iter()
                .flat_map(|(_, fields)| fields.iter().map(|s| s.as_str()))
                .collect();
            let hi_fields: BTreeSet<&str> = hi_entries
                .iter()
                .flat_map(|(_, fields)| fields.iter().map(|s| s.as_str()))
                .collect();

            // Fields added in higher version
            for &field in hi_fields.difference(&lo_fields) {
                errors.push(CheckError {
                    severity: Severity::Warning,
                    rule: None,
                    test: None,
                    message: format!(
                        "yield target `{}`: field `{}` added in @v{} (not in @v{}); consumers may need to adapt",
                        target, field, v_hi, v_lo
                    ),
                });
            }

            // Fields removed in higher version
            for &field in lo_fields.difference(&hi_fields) {
                errors.push(CheckError {
                    severity: Severity::Warning,
                    rule: None,
                    test: None,
                    message: format!(
                        "yield target `{}`: field `{}` removed in @v{} (present in @v{}); consumers may depend on it",
                        target, field, v_hi, v_lo
                    ),
                });
            }
        }
    }
}
