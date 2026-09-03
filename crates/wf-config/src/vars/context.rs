use std::collections::HashMap;
#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigVars")]
pub struct ConfigVarContext {
    explicit_vars: HashMap<String, String>,
    env_vars: HashMap<String, String>,
}

impl Default for ConfigVarContext {
    fn default() -> Self {
        Self {
            explicit_vars: HashMap::new(),
            env_vars: std::env::vars().collect(),
        }
    }
}

impl ConfigVarContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_explicit_vars(explicit_vars: HashMap<String, String>) -> Self {
        Self {
            explicit_vars,
            ..Self::default()
        }
    }

    pub fn explicit_vars(&self) -> &HashMap<String, String> {
        &self.explicit_vars
    }

    pub fn env_var_value(&self, ident: &str) -> Option<String> {
        self.env_vars.get(ident).cloned()
    }

    pub fn resolve_external_var(&self, ident: &str) -> Option<String> {
        self.explicit_vars
            .get(ident)
            .cloned()
            .or_else(|| self.env_vars.get(ident).cloned())
    }

    pub fn materialize_vars(&self, file_vars: &HashMap<String, String>) -> HashMap<String, String> {
        let mut out = file_vars.clone();
        for (key, value) in &self.explicit_vars {
            out.insert(key.clone(), value.clone());
        }
        out
    }
}
