use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Admin API config (mirrors warp-parse admin_api pattern)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, PartialEq, Eq, Deserialize, Serialize, Clone, ::moju_derive::MoJu)]
#[serde(deny_unknown_fields)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct AdminApiTlsConf {
    #[serde(default, alias = "enable")]
    pub enabled: bool,
    #[serde(default)]
    pub cert_file: String,
    #[serde(default)]
    pub key_file: String,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize, Clone, ::moju_derive::MoJu)]
#[serde(deny_unknown_fields)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct AdminApiAuthConf {
    #[serde(default = "default_admin_api_auth_mode")]
    pub mode: String,
    #[serde(default = "default_admin_api_token_file")]
    pub token_file: String,
}

impl Default for AdminApiAuthConf {
    fn default() -> Self {
        Self {
            mode: default_admin_api_auth_mode(),
            token_file: default_admin_api_token_file(),
        }
    }
}

fn default_admin_api_auth_mode() -> String {
    "bearer_token".to_string()
}

fn default_admin_api_token_file() -> String {
    "${HOME}/.wfusion/admin_api.token".to_string()
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize, Clone, ::moju_derive::MoJu)]
#[serde(deny_unknown_fields)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct AdminApiConf {
    #[serde(default, alias = "enable")]
    pub enabled: bool,
    #[serde(default = "default_admin_api_bind")]
    pub bind: String,
    #[serde(default = "default_admin_api_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_admin_api_max_body_bytes")]
    pub max_body_bytes: usize,
    #[serde(default)]
    pub tls: AdminApiTlsConf,
    #[serde(default)]
    pub auth: AdminApiAuthConf,
}

impl Default for AdminApiConf {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_admin_api_bind(),
            request_timeout_ms: default_admin_api_request_timeout_ms(),
            max_body_bytes: default_admin_api_max_body_bytes(),
            tls: AdminApiTlsConf::default(),
            auth: AdminApiAuthConf::default(),
        }
    }
}

fn default_admin_api_bind() -> String {
    "127.0.0.1:19080".to_string()
}

fn default_admin_api_request_timeout_ms() -> u64 {
    15000
}

fn default_admin_api_max_body_bytes() -> usize {
    4096
}
