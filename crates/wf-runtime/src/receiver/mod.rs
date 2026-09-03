pub mod arrow;
pub(crate) mod batch;
#[cfg(test)]
mod coverage_r4;
pub mod csv;
pub(crate) mod miss;
pub mod ndjson;
pub(crate) mod route;
pub(crate) mod schema;
#[cfg(test)]
mod tests;

pub(crate) use arrow::{replay_arrow_framed_file, replay_arrow_ipc_file};

pub(crate) use csv::replay_csv_file;
pub(crate) use miss::{WindowMiss, WindowMissReason, record_batch_window_miss, report_window_miss};
pub(crate) use ndjson::{normalize_stream_tag_field, replay_ndjson_file};
pub(crate) use route::{batch_machine_id, prepare_batch};
pub(crate) use schema::{maybe_resolve_stream_schema, resolve_stream_schema};

pub const DEFAULT_STREAM_TAG_FIELD: &str = "wp_oml_name";

#[derive(Clone, Copy, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Receiver")]
pub struct ReplayRoute<'a> {
    pub stream_name: &'a str,
    pub stream_tag_field: &'a str,
}
