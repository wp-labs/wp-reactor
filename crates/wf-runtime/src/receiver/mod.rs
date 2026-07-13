pub mod arrow;
pub(crate) mod batch;
pub mod csv;
pub mod ndjson;
pub(crate) mod route;
pub(crate) mod schema;
#[cfg(test)]
mod tests;

pub use arrow::{replay_arrow_framed_file, replay_arrow_ipc_file};

pub use csv::replay_csv_file;
pub use ndjson::{normalize_stream_tag_field, replay_ndjson_file};
pub(crate) use route::route_batch;
pub(crate) use schema::resolve_stream_schema;

pub const DEFAULT_STREAM_TAG_FIELD: &str = "wp_oml_name";

#[derive(Clone, Copy)]
pub struct ReplayRoute<'a> {
    pub stream_name: &'a str,
    pub stream_tag_field: &'a str,
}
