use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use toml::Value as TomlValue;

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigVars")]
pub enum SourceAtom {
    File(PathBuf),
    Explicit(String),
    Env(String),
    Default(String),
}

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigVars")]
pub struct TracedValue {
    pub value: String,
    pub sources: BTreeSet<SourceAtom>,
}

#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigVars")]
pub struct ExpandedToml {
    pub value: TomlValue,
    pub sources: HashMap<String, BTreeSet<SourceAtom>>,
}

impl TracedValue {
    pub(crate) fn new(value: String) -> Self {
        Self {
            value,
            sources: BTreeSet::new(),
        }
    }

    pub(crate) fn with_source(value: String, source: SourceAtom) -> Self {
        let mut traced = Self::new(value);
        traced.sources.insert(source);
        traced
    }

    pub fn rendered_sources(&self) -> String {
        render_source_label(&self.sources)
    }
}

impl ExpandedToml {
    pub fn source_for(&self, path: &str) -> Option<&BTreeSet<SourceAtom>> {
        self.sources.get(path)
    }

    pub fn rendered_source_for(&self, path: &str) -> Option<String> {
        self.source_for(path).map(render_source_label)
    }

    pub fn rendered_sources(&self) -> Vec<(String, String)> {
        let mut entries: Vec<(String, String)> = self
            .sources
            .iter()
            .map(|(path, sources)| (path.clone(), render_source_label(sources)))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }
}

pub fn render_source_label(sources: &BTreeSet<SourceAtom>) -> String {
    let mut iter = sources.iter();
    let Some(first) = iter.next() else {
        return "<unknown>".to_string();
    };
    if iter.next().is_none() {
        return render_single_source(first);
    }

    let tokens: Vec<String> = sources.iter().map(render_mixed_token).collect();
    format!("<mixed:{}>", tokens.join(","))
}

fn render_single_source(source: &SourceAtom) -> String {
    match source {
        SourceAtom::File(path) => path.display().to_string(),
        SourceAtom::Explicit(key) => format!("<cli:{key}>"),
        SourceAtom::Env(key) => format!("<env:{key}>"),
        SourceAtom::Default(key) => format!("<default:{key}>"),
    }
}

fn render_mixed_token(source: &SourceAtom) -> String {
    match source {
        SourceAtom::File(path) => format!("file:{}", path.display()),
        SourceAtom::Explicit(key) => format!("cli:{key}"),
        SourceAtom::Env(key) => format!("env:{key}"),
        SourceAtom::Default(key) => format!("default:{key}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_source_label_formats_single_and_mixed_sources() {
        let mut single = BTreeSet::new();
        single.insert(SourceAtom::Env("CASE_PATH".to_string()));
        assert_eq!(render_source_label(&single), "<env:CASE_PATH>");

        let mut mixed = BTreeSet::new();
        mixed.insert(SourceAtom::File(PathBuf::from("/tmp/conf.toml")));
        mixed.insert(SourceAtom::Explicit("CASE_PATH".to_string()));
        assert_eq!(
            render_source_label(&mixed),
            "<mixed:file:/tmp/conf.toml,cli:CASE_PATH>"
        );
    }
}
