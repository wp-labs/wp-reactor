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

/// Full verification report.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerifyReport {
    pub status: String,
    pub summary: VerifySummary,
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
                let (m, mm) = greedy_match(exp, act, score_tolerance);
                matched += m;
                field_mismatch += mm;
                missing += exp.len().saturating_sub(m + mm);
                unexpected += act.len().saturating_sub(m + mm);
            }
            (Some(exp), None) => {
                missing += exp.len();
            }
            (None, Some(act)) => {
                unexpected += act.len();
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

/// Returns `(matched_count, field_mismatch_count)`.
fn greedy_match(
    expected: &[&OracleAlert],
    actual: &[&ActualAlert],
    score_tolerance: f64,
) -> (usize, usize) {
    let mut used_actual = vec![false; actual.len()];
    let mut matched = 0usize;
    let mut field_mismatch = 0usize;

    // For each expected alert, find the nearest unused actual by time
    for exp in expected {
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
            let score_diff = (exp.score - actual[j].score).abs();
            if score_diff <= score_tolerance {
                matched += 1;
            } else {
                field_mismatch += 1;
            }
        }
        // If no actual found, it will be counted as missing by the caller
    }

    (matched, field_mismatch)
}

/// Parse an ISO 8601 timestamp to seconds-since-epoch (approximate, for ordering).
fn parse_time_approx(s: &str) -> f64 {
    s.parse::<chrono::DateTime<chrono::Utc>>()
        .map(|dt| dt.timestamp() as f64 + dt.timestamp_subsec_millis() as f64 / 1000.0)
        .unwrap_or(0.0)
}
