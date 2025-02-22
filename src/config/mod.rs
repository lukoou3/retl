mod source;
mod transform;
mod sink;

pub use source::*;
pub use transform::*;
pub use sink::*;

use std::error::Error;
use serde::{Deserialize, Serialize};
use config::{Config, Value as ConfigValue};

#[derive(Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub env: EnvConfig,
    pub sources: Vec<SourceOuter>,
    #[serde(default)]
    pub transforms: Vec<TransformOuter>,
    pub sinks: Vec<SinkOuter>,
}

#[derive(Debug, Serialize,Deserialize)]
pub struct EnvConfig {
    pub application: ApplicationConfig,
}

#[derive(Debug, Serialize,Deserialize)]
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
    let config = Config::builder().add_source(config::File::from(std::path::Path::new(config_path)))
        .build()?.try_deserialize()?;
    Ok(config)
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