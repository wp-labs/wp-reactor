use super::*;

#[test]
fn sharded_and_full_agree_on_iteration() {
    // 同一逻辑行域：Sharded(绝对行号) 与 Full(n)（恒等）语义等价。
    let rows: [u32; 4] = [2, 5, 7, 9];
    let sharded = RowDomain::Sharded(&rows);
    let full = RowDomain::Full(10);

    // 只验证 Sharded 的 len 与其切片一致；Full 的 len 是独立输入。
    assert_eq!(sharded.len(), 4);
    assert_eq!(full.len(), 10);

    // Sharded: (i, rows[i])。
    for (i, &r) in rows.iter().enumerate() {
        assert_eq!(sharded.row_at(i), r as usize);
    }
    // Full: (i, i)。
    for i in 0..full.len() {
        assert_eq!(full.row_at(i), i);
    }
    // 与旧 `(0..n)` 恒等行域逐位一致。
    let legacy_full: Vec<usize> = (0..10).collect();
    assert_eq!(full.to_vec(), legacy_full);
}

#[test]
fn to_vec_matches_legacy_row_domain() {
    // to_vec 与旧 `Vec<usize>` 行域（分片转换 / 恒等）逐位一致。
    let rows: [u32; 3] = [0, 4, 8];
    let sharded = RowDomain::Sharded(&rows);
    let legacy: Vec<usize> = rows.iter().map(|&r| r as usize).collect();
    assert_eq!(sharded.to_vec(), legacy);

    let full = RowDomain::Full(5);
    assert_eq!(full.to_vec(), vec![0, 1, 2, 3, 4]);
}

#[test]
fn empty_domains_are_coherent() {
    // 空分片 / 空批：len 0，row_at 不可达（迭代 0 次）。
    let empty_slice: [u32; 0] = [];
    let sharded = RowDomain::Sharded(&empty_slice);
    assert_eq!(sharded.len(), 0);
    assert!(sharded.to_vec().is_empty());

    let full = RowDomain::Full(0);
    assert_eq!(full.len(), 0);
    assert!(full.to_vec().is_empty());
}
