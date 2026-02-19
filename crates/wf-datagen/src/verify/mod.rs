#[cfg(test)]
mod tests;

use std::collections::HashMap;

use crate::oracle::OracleAlert;

/// An actual alert to compare against oracle expectations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActualAlert {
    pub rule_name: String,
    pub score: f64,
    pub entity_type: String,
    pub entity_id: String,
    pub close_reason: Option<String>,
    pub fired_at: String,
}

/// Summary statistics of the verify comparison.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerifySummary {
    pub oracle_total: usize,
    pub actual_total: usize,
    pub matched: usize,
    pub missing: usize,
    pub unexpected: usize,
    pub field_mismatch: usize,
}

/// Detail record for a missing or unexpected alert.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AlertDetail {
    pub rule_name: String,
    pub entity_type: String,
    pub entity_id: String,
    pub score: f64,
    pub time: String,
}

/// Detail record for a score mismatch.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MismatchDetail {
    pub rule_name: String,
    pub entity_type: String,
    pub entity_id: String,
    pub expected_score: f64,
    pub actual_score: f64,
    pub expected_time: String,
    pub actual_time: String,
}

/// Full verification report.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerifyReport {
    pub status: String,
    pub summary: VerifySummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_details: Vec<AlertDetail>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unexpected_details: Vec<AlertDetail>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mismatch_details: Vec<MismatchDetail>,
}

impl VerifyReport {
    /// Render the report as a PR-friendly Markdown table.
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();
        md.push_str("## wf-datagen Verify Report\n\n");
        md.push_str(&format!(
            "**Status**: {}\n\n",
            self.status.to_uppercase()
        ));

        // Summary table
        md.push_str("### Summary\n\n");
        md.push_str("| Metric | Count |\n");
        md.push_str("|--------|-------|\n");
        md.push_str(&format!("| Oracle total | {} |\n", self.summary.oracle_total));
        md.push_str(&format!("| Actual total | {} |\n", self.summary.actual_total));
        md.push_str(&format!("| Matched | {} |\n", self.summary.matched));
        md.push_str(&format!("| Missing | {} |\n", self.summary.missing));
        md.push_str(&format!("| Unexpected | {} |\n", self.summary.unexpected));
        md.push_str(&format!(
            "| Field mismatch | {} |\n",
            self.summary.field_mismatch
        ));

        // Missing details
        if !self.missing_details.is_empty() {
            md.push_str(&format!(
                "\n### Missing ({})\n\n",
                self.missing_details.len()
            ));
            md.push_str("| Rule | Entity | Score | Time |\n");
            md.push_str("|------|--------|-------|------|\n");
            for d in &self.missing_details {
                md.push_str(&format!(
                    "| {} | {}:{} | {:.2} | {} |\n",
                    d.rule_name, d.entity_type, d.entity_id, d.score, d.time
                ));
            }
        }

        // Unexpected details
        if !self.unexpected_details.is_empty() {
            md.push_str(&format!(
                "\n### Unexpected ({})\n\n",
                self.unexpected_details.len()
            ));
            md.push_str("| Rule | Entity | Score | Time |\n");
            md.push_str("|------|--------|-------|------|\n");
            for d in &self.unexpected_details {
                md.push_str(&format!(
                    "| {} | {}:{} | {:.2} | {} |\n",
                    d.rule_name, d.entity_type, d.entity_id, d.score, d.time
                ));
            }
        }

        // Mismatch details
        if !self.mismatch_details.is_empty() {
            md.push_str(&format!(
                "\n### Field Mismatches ({})\n\n",
                self.mismatch_details.len()
            ));
            md.push_str("| Rule | Entity | Expected | Actual | Exp. Time | Act. Time |\n");
            md.push_str("|------|--------|----------|--------|-----------|----------|\n");
            for d in &self.mismatch_details {
                md.push_str(&format!(
                    "| {} | {}:{} | {:.2} | {:.2} | {} | {} |\n",
                    d.rule_name,
                    d.entity_type,
                    d.entity_id,
                    d.expected_score,
                    d.actual_score,
                    d.expected_time,
                    d.actual_time
                ));
            }
        }

        md
    }
}

/// Match key for grouping alerts.
type MatchKey = (String, String, String, Option<String>);

/// Compare actual alerts against oracle (expected) alerts.
///
/// Algorithm:
/// 1. Group both sides by match key `(rule_name, entity_type, entity_id, close_reason)`.
/// 2. Within each group, greedily pair by nearest time.
/// 3. Paired alerts with `|score_diff| > tolerance` count as field_mismatch.
/// 4. Unpaired expected → missing, unpaired actual → unexpected.
/// 5. Status = "pass" iff missing == 0 && unexpected == 0 && field_mismatch == 0.
pub fn verify(
    expected: &[OracleAlert],
    actual: &[ActualAlert],
    score_tolerance: f64,
) -> VerifyReport {
    let expected_groups = group_expected(expected);
    let actual_groups = group_actual(actual);

    let mut matched = 0usize;
    let mut missing = 0usize;
    let mut unexpected = 0usize;
    let mut field_mismatch = 0usize;

    let mut missing_details = Vec::new();
    let mut unexpected_details = Vec::new();
    let mut mismatch_details = Vec::new();

    // Collect all keys from both sides
    let mut all_keys: Vec<MatchKey> = Vec::new();
    for k in expected_groups.keys() {
        all_keys.push(k.clone());
    }
    for k in actual_groups.keys() {
        if !expected_groups.contains_key(k) {
            all_keys.push(k.clone());
        }
    }

    for key in &all_keys {
        let exp_list = expected_groups.get(key);
        let act_list = actual_groups.get(key);

        match (exp_list, act_list) {
            (Some(exp), Some(act)) => {
                let result = greedy_match(exp, act, score_tolerance);
                matched += result.matched;
                field_mismatch += result.mismatches.len();
                missing += result.missing_indices.len();
                unexpected += result.unexpected_indices.len();

                for &idx in &result.missing_indices {
                    missing_details.push(AlertDetail {
                        rule_name: exp[idx].rule_name.clone(),
                        entity_type: exp[idx].entity_type.clone(),
                        entity_id: exp[idx].entity_id.clone(),
                        score: exp[idx].score,
                        time: exp[idx].emit_time.clone(),
                    });
                }
                for &idx in &result.unexpected_indices {
                    unexpected_details.push(AlertDetail {
                        rule_name: act[idx].rule_name.clone(),
                        entity_type: act[idx].entity_type.clone(),
                        entity_id: act[idx].entity_id.clone(),
                        score: act[idx].score,
                        time: act[idx].fired_at.clone(),
                    });
                }
                for &(exp_idx, act_idx) in &result.mismatches {
                    mismatch_details.push(MismatchDetail {
                        rule_name: exp[exp_idx].rule_name.clone(),
                        entity_type: exp[exp_idx].entity_type.clone(),
                        entity_id: exp[exp_idx].entity_id.clone(),
                        expected_score: exp[exp_idx].score,
                        actual_score: act[act_idx].score,
                        expected_time: exp[exp_idx].emit_time.clone(),
                        actual_time: act[act_idx].fired_at.clone(),
                    });
                }
            }
            (Some(exp), None) => {
                missing += exp.len();
                for e in exp {
                    missing_details.push(AlertDetail {
                        rule_name: e.rule_name.clone(),
                        entity_type: e.entity_type.clone(),
                        entity_id: e.entity_id.clone(),
                        score: e.score,
                        time: e.emit_time.clone(),
                    });
                }
            }
            (None, Some(act)) => {
                unexpected += act.len();
                for a in act {
                    unexpected_details.push(AlertDetail {
                        rule_name: a.rule_name.clone(),
                        entity_type: a.entity_type.clone(),
                        entity_id: a.entity_id.clone(),
                        score: a.score,
                        time: a.fired_at.clone(),
                    });
                }
            }
            (None, None) => {}
        }
    }

    let status = if missing == 0 && unexpected == 0 && field_mismatch == 0 {
        "pass".to_string()
    } else {
        "fail".to_string()
    };

    VerifyReport {
        status,
        summary: VerifySummary {
            oracle_total: expected.len(),
            actual_total: actual.len(),
            matched,
            missing,
            unexpected,
            field_mismatch,
        },
        missing_details,
        unexpected_details,
        mismatch_details,
    }
}

// ---------------------------------------------------------------------------
// Grouping
// ---------------------------------------------------------------------------

fn match_key_expected(a: &OracleAlert) -> MatchKey {
    (
        a.rule_name.clone(),
        a.entity_type.clone(),
        a.entity_id.clone(),
        a.close_reason.clone(),
    )
}

fn match_key_actual(a: &ActualAlert) -> MatchKey {
    (
        a.rule_name.clone(),
        a.entity_type.clone(),
        a.entity_id.clone(),
        a.close_reason.clone(),
    )
}

fn group_expected(alerts: &[OracleAlert]) -> HashMap<MatchKey, Vec<&OracleAlert>> {
    let mut map: HashMap<MatchKey, Vec<&OracleAlert>> = HashMap::new();
    for a in alerts {
        map.entry(match_key_expected(a)).or_default().push(a);
    }
    map
}

fn group_actual(alerts: &[ActualAlert]) -> HashMap<MatchKey, Vec<&ActualAlert>> {
    let mut map: HashMap<MatchKey, Vec<&ActualAlert>> = HashMap::new();
    for a in alerts {
        map.entry(match_key_actual(a)).or_default().push(a);
    }
    map
}

// ---------------------------------------------------------------------------
// Greedy matching within a group
// ---------------------------------------------------------------------------

/// Result of greedy matching within a single key group.
struct MatchResult {
    matched: usize,
    /// (expected_idx, actual_idx) pairs with score mismatch.
    mismatches: Vec<(usize, usize)>,
    /// Indices into the expected slice that were not paired.
    missing_indices: Vec<usize>,
    /// Indices into the actual slice that were not paired.
    unexpected_indices: Vec<usize>,
}

/// Greedily pair expected and actual alerts within a group by nearest time.
fn greedy_match(
    expected: &[&OracleAlert],
    actual: &[&ActualAlert],
    score_tolerance: f64,
) -> MatchResult {
    let mut used_actual = vec![false; actual.len()];
    let mut matched = 0usize;
    let mut mismatches = Vec::new();
    let mut paired_expected = vec![false; expected.len()];

    // For each expected alert, find the nearest unused actual by time
    for (ei, exp) in expected.iter().enumerate() {
        let exp_time = parse_time_approx(&exp.emit_time);
        let mut best_idx: Option<usize> = None;
        let mut best_dist = f64::MAX;

        for (j, act) in actual.iter().enumerate() {
            if used_actual[j] {
                continue;
            }
            let act_time = parse_time_approx(&act.fired_at);
            let dist = (exp_time - act_time).abs();
            if dist < best_dist {
                best_dist = dist;
                best_idx = Some(j);
            }
        }

        if let Some(j) = best_idx {
            used_actual[j] = true;
            paired_expected[ei] = true;
            let score_diff = (exp.score - actual[j].score).abs();
            if score_diff <= score_tolerance {
                matched += 1;
            } else {
                mismatches.push((ei, j));
            }
        }
    }

    let missing_indices: Vec<usize> = paired_expected
        .iter()
        .enumerate()
        .filter(|(_, paired)| !**paired)
        .map(|(i, _)| i)
        .collect();

    let unexpected_indices: Vec<usize> = used_actual
        .iter()
        .enumerate()
        .filter(|(_, used)| !**used)
        .map(|(i, _)| i)
        .collect();

    MatchResult {
        matched,
        mismatches,
        missing_indices,
        unexpected_indices,
    }
}

/// Parse an ISO 8601 timestamp to seconds-since-epoch (approximate, for ordering).
fn parse_time_approx(s: &str) -> f64 {
    s.parse::<chrono::DateTime<chrono::Utc>>()
        .map(|dt| dt.timestamp() as f64 + dt.timestamp_subsec_millis() as f64 / 1000.0)
        .unwrap_or(0.0)
}
