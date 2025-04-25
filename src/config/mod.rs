mod source;
mod transform;
mod sink;
mod execution;

pub use source::*;
pub use transform::*;
pub use sink::*;
pub use execution::*;

use std::fs;
use std::error::Error;
use serde::{Deserialize, Serialize};
use config::{Config, Value as ConfigValue};
use regex::Regex;
use crate::encrypt::aes_decrypt;

#[derive(Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub env: EnvConfig,
    pub sources: Vec<SourceOuter>,
    #[serde(default)]
    pub transforms: Vec<TransformOuter>,
    pub sinks: Vec<SinkOuter>,
    #[serde(default)]
    pub active_sinks: Vec<String>,
}

#[derive(Debug, Clone, Serialize,Deserialize)]
pub struct EnvConfig {
    pub application: ApplicationConfig,
    #[serde(default)]
    pub web: WebConfig,
}

#[derive(Debug, Clone, Serialize,Deserialize)]
pub struct WebConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_port")]
    pub port: u32,
    #[serde(default = "default_works")]
    pub works: u32,
}

impl Default for WebConfig {
    fn default() -> Self {
        WebConfig {
            enabled: false,
            port: default_port(),
            works: default_works(),
        }
    }
}

fn default_port() -> u32 {
    8000
}

fn default_works() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize,Deserialize)]
pub struct ApplicationConfig {
    #[serde(default = "default_application_name")]
    pub name: String,
    #[serde(default)]
    pub parallelism: u8,
}

fn default_application_name() -> String {
    "retl".to_string()
}

#[derive(Clone, Debug)]
pub struct WrapConfigValue(ConfigValue);

impl From<ConfigValue> for WrapConfigValue {
    fn from(value: ConfigValue) -> Self {
        Self(value)
    }
}

impl config::Source for WrapConfigValue {
    fn clone_into_box(&self) -> Box<dyn config::Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<config::Map<String, config::Value>, config::ConfigError> {
        self.0.clone().into_table()
    }
}

pub fn parse_config(config_path: &str) -> Result<AppConfig, Box<dyn Error>> {
    let content = fs::read_to_string(config_path).map_err(|e| format!("Failed to read config file: {}", e))?;
    let key = b"fd6b639dbcff0c2a";
    let iv = b"77b07a672d57d64c";
    let decrypted_content = decrypt_config_content(&content, key, iv).map_err(|e| format!("Failed to decrypt config: {}", e))?;
    let config = Config::builder().add_source(config::File::from_str(&decrypted_content, config::FileFormat::Yaml))
        .build()?.try_deserialize()?;
    Ok(config)
}

fn decrypt_config_content(content: &str, key: &[u8; 16], iv: &[u8; 16]) -> Result<String, String> {
    let re = Regex::new(r"enc@\(([^)]+)\)").map_err(|e| format!("Regex error: {}", e))?;
    let mut result = String::new();

    for line in content.lines() {
        let mut new_line = line.to_string();
        for cap in re.captures_iter(line) {
            let full_match = cap.get(0).unwrap().as_str();
            let ciphertext = cap.get(1).unwrap().as_str();
            let plaintext = aes_decrypt(ciphertext, key, iv)?;
            new_line = new_line.replace(full_match, &plaintext);
        }
        result.push_str(&new_line);
        result.push('\n');
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config()  {
        let config_path = "config/application.yaml";
        let config: AppConfig = parse_config(config_path).unwrap();
        println!("{:#?}", config);
        println!("{}", serde_yaml::to_string(&config).unwrap());
        println!("{}", serde_json::to_string_pretty(&config).unwrap());
    }

}