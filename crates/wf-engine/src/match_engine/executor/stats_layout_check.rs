/// 快速验证：q18 形状列式路径的 RowFields layout 是否紧凑（2026-08-26）。
#[test]
fn q18_columnar_layout_is_compact() {
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![
            last_measure("last_price", "price"),
            last_measure("last_channel", "channel"),
        ],
    );
    let mut exec = StatsExecutor::with_row_fields(plan, Some(full_bid_subset()));
    // 列式批（bid_events 形状：auction/price Int64 + channel Utf8）。
    let batch = arrow::record_batch::RecordBatch::try_new(
        std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("auction", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("price", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new(
                "channel",
                arrow::datatypes::DataType::Utf8,
                false,
            ),
        ])),
        vec![
            std::sync::Arc::new(arrow::array::Int64Array::from(vec![1, 1, 2])),
            std::sync::Arc::new(arrow::array::Int64Array::from(vec![100, 200, 300])),
            std::sync::Arc::new(arrow::array::StringArray::from(vec!["G", "G", "B"])),
        ],
    )
    .expect("batch");
    assert!(exec.process_batch(&batch), "列式前置应满足");
    let buckets = exec.close_window_by_bucket();
    assert_eq!(buckets.len(), 2, "2 个 auction 桶");
    // 行字段 layout：auction/price 数字槽 + channel 字符串槽。
    let layout = buckets[0].measures[0][0]
        .row_fields
        .as_ref()
        .expect("last 携带行字段")
        .layout();
    assert_eq!(layout.n_numeric, 2, "auction/price 数字槽");
    assert_eq!(layout.n_strings, 1, "channel 字符串槽");
}
