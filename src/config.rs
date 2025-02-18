use std::error::Error;
use std::fs;
use serde::Deserialize;
use serde_yaml::Value;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub env: EnvConfig,
    pub sources: Vec<SourceConfig>,
    pub transforms: Vec<TransformConfig>,
    pub sinks: Vec<SinkConfig>,
}

#[derive(Debug, Deserialize)]
pub struct EnvConfig {
    pub application: ApplicationConfig,
}

#[derive(Debug, Deserialize)]
pub struct ApplicationConfig {
    pub name: Option<String>,
    pub parallelism: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    tpe: String,
    results: Vec<String>,
    schema: Value,
    options: Value,
}

#[derive(Debug, Deserialize)]
pub struct TransformConfig {
    #[serde(rename = "type")]
    tpe: String,
    dependencies: Vec<String>,
    results: Vec<String>,
    options: Value,
}

#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    #[serde(rename = "type")]
    tpe: String,
    dependencies: Vec<String>,
    options: Value,
}

pub fn parse_config(config_path: &str) -> Result<Config, Box<dyn Error>> {
    let config_str = fs::read_to_string(config_path)?;
    let config: Config = serde_yaml::from_str(config_str.as_str())?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config()  {
        let config_path = "config/application.yaml";
        let config: Config = parse_config(config_path).unwrap();
        println!("{:#?}", config);
    }

}