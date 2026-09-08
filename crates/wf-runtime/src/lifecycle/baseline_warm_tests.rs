//! warm_baseline_history（近端 B 启动装载）单测：正常装载 / 缺列报错 /
//! 空键行跳过 / BOM 容错。store 为 crate 全局共享，用例用独立实体键防并行污染。

use super::*;

fn case_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("wf_baseline_ut_{}_{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

fn write_csv(dir: &std::path::Path, content: &str) -> String {
    let rel = "history.csv";
    std::fs::write(dir.join(rel), content).expect("write csv");
    rel.to_string()
}

fn header() -> &'static str {
    "entity,metric,win_start,win_end,n,sum,sum_sq\n"
}

#[test]
fn warm_loads_rows_into_store() {
    let dir = case_dir("ok");
    let mut content = String::new();
    content.push_str(header());
    content.push_str("t_ok_a,qps,0,60000000000,2.0,20.0,210.0\n");
    content.push_str("t_ok_a,qps,60000000000,120000000000,3.0,30.0,300.0\n");
    content.push_str("t_ok_b,qps,0,60000000000,1.0,10.0,100.0\n");
    let rel = write_csv(&dir, &content);

    let r = warm_baseline_history(Some(&rel), 8, true, None, &dir);
    assert!(r.is_ok(), "warm 应成功: {r:?}");
    let st = wf_engine::baseline::store();
    assert_eq!(st.window_count("t_ok_a", "qps"), 2);
    assert_eq!(st.window_count("t_ok_b", "qps"), 1);
    // 数值落账：两窗均值同 10 → μ 与衰减模式无关（decay 语义由 wf-cep 单测覆盖）
    let (_, mu, _) = st.summary("t_ok_a", "qps").expect("summary");
    assert!((mu - 10.0).abs() < 1e-9, "μ 应为 10.0, 实际 {mu}");
}

#[test]
fn warm_missing_required_column_errors() {
    let dir = case_dir("missing");
    let content = "entity,metric,win_start,win_end,n,sum\nsvc_a,qps,0,1,1.0,2.0\n";
    let rel = write_csv(&dir, content);
    assert!(
        warm_baseline_history(Some(&rel), 8, true, None, &dir).is_err(),
        "缺 sum_sq 列应报错（防静默空历史）"
    );
}

#[test]
fn warm_skips_empty_key_rows_and_accepts_bom() {
    let dir = case_dir("empty_bom");
    let content = format!(
        "\u{feff}{header}t_emp_a,qps,0,60000000000,2.0,20.0,210.0\n,,1,2,1.0,1.0,1.0\n",
        header = header()
    );
    let rel = write_csv(&dir, &content);
    assert!(warm_baseline_history(Some(&rel), 8, true, None, &dir).is_ok());
    let st = wf_engine::baseline::store();
    assert_eq!(st.window_count("t_emp_a", "qps"), 1, "BOM 头仍应解析");
    assert_eq!(st.window_count("", ""), 0, "空键行应跳过");
}

#[test]
fn warm_none_path_is_noop() {
    assert!(warm_baseline_history(None, 8, true, None, Path::new(".")).is_ok());
}
