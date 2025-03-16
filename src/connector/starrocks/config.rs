use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use base64::Engine;
use base64::engine::general_purpose;
use isahc::config::RedirectPolicy;
use isahc::{HttpClient, ReadResponseExt, Request, RequestExt};
use isahc::prelude::Configurable;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::codecs::JsonSerializer;
use crate::Result;
use crate::config::{SinkConfig, SinkProvider, TaskContext};
use crate::connector::batch::{BatchConfig, BatchSettings};
use crate::connector::Sink;
use crate::connector::starrocks::StarRocksSink;
use crate::types::Schema;

#[derive(Clone, Copy, Debug, Default)]
pub struct StarRocksDefaultBatchSettings;

impl BatchSettings for StarRocksDefaultBatchSettings {
    const MAX_ROWS: usize = 10000;
    const MAX_BYTES: usize = 1024 * 1024 * 30;
    const INTERVAL_MS: u64 = 30000;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub host: String,
    pub username: String,
    pub password: String,
    pub database: String,
    pub table: String,
    #[serde(default)]
    pub compress: bool,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StarRocksSinkConfig {
    #[serde(flatten)]
    pub connection_config: ConnectionConfig,
    #[serde(flatten, default)]
    pub batch_config: BatchConfig<StarRocksDefaultBatchSettings>,
}

#[typetag::serde(name = "starrocks")]
impl SinkConfig for StarRocksSinkConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn SinkProvider>> {
        let mut config = self.clone();
        config.connection_config.supplement_properties()?;
        Ok(Box::new(StarRocksSinkProvider::new(schema, config)))
    }
}

#[derive(Debug, Clone)]
pub struct StarRocksSinkProvider {
    schema: Schema,
    sink_config: StarRocksSinkConfig
}

impl StarRocksSinkProvider {
    pub fn new(schema: Schema, sink_config: StarRocksSinkConfig) -> Self {
        Self {
            schema,
            sink_config
        }
    }
}

impl SinkProvider for StarRocksSinkProvider {
    fn create_sink(&self, task_context: TaskContext) -> Result<Box<dyn Sink>> {
        Ok(Box::new(StarRocksSink::new(
            task_context,
            self.sink_config.connection_config.clone(),
            self.sink_config.batch_config.clone(),
            JsonSerializer::new(self.schema.clone()),
        )))
    }
}

impl ConnectionConfig {
    pub fn build_urls(&self) -> Vec<String> {
        self.host
            .split(',')
            .map(|host| {
                format!(
                    "http://{}/api/{}/{}/_stream_load",
                    host.trim(),
                    self.database,
                    self.table
                )
            })
            .collect()
    }

    pub fn supplement_properties(&mut self) -> Result<()> {
        if !self.properties.contains_key("columns") {
            let columns = self.gene_stream_load_columns().map_err(|e| format!("{:?}", e))?;
            info!("{} geneStreamLoadColumns:{}", self.table, columns);
            self.properties.insert("columns".to_string(), columns);
        }
        if !self.properties.contains_key("timeout") {
            self.properties.insert("timeout".to_string(), "600".to_string());
        }
        if self.compress {
            self.properties.insert("compression".to_string(), "lz4_frame".to_string());
        } else if self.properties.contains_key("compression") {
            self.compress = true;
        }
        self.properties.insert("expect".to_string(), "100-continue".to_string());
        self.properties.insert("Authorization".to_string(), basic_auth_header(&self.username, &self.password));
        self.properties.insert("format".to_string(), "json".to_string());
        self.properties.insert("strip_outer_array".to_string(), "true".to_string());
        self.properties.insert("ignore_json_size".to_string(), "true".to_string());
        self.properties.insert("two_phase_commit".to_string(), "false".to_string());
        Ok(())
    }

    fn gene_stream_load_columns(&self) -> core::result::Result<String, Box<dyn Error>> {
        let sql = format!("DESC {}.{}", self.database, self.table);
        let hosts: Vec<&str> = self.host.split(',').map(|s| s.trim()).collect();

        let json_data = json!({"query": &sql});
        let body = json_data.to_string();

        for (i, host) in hosts.iter().enumerate() {
            let url = format!("http://{}/api/v1/catalogs/default_catalog/databases/{}/sql", host, self.database);
            let request = Request::post(url)
                .header("Authorization", basic_auth_header(&self.username, &self.password))
                .header("Content-Type", "application/json")
                .redirect_policy(RedirectPolicy::Limit(5))
                .timeout(Duration::from_secs(60))
                .body(body.as_str())?;

            let response_rst = request.send();
            if response_rst.is_err() {
                continue;
            }
            let mut response = response_rst.unwrap();

            if response.status().is_success() {
                let json: Value = serde_json::from_str(&response.text()?)?;
                let rows = json["data"].as_array().ok_or("Invalid response format")?;

                let mut cols = Vec::new();
                let mut col_mappings = Vec::new();

                for row in rows {
                    let name = row["Field"].as_str().ok_or("Missing 'Field' in response")?;
                    let type_str = row["Type"].as_str().ok_or("Missing 'Type' in response")?.to_lowercase();
                    let type_str = type_str.split('(').next().unwrap_or(&type_str); // 去除括号部分
                    let default_str = row["Default"].as_str().unwrap_or("");

                    if default_str.is_empty() {
                        cols.push(name.to_string());
                        continue;
                    }

                    if (type_str == "datetime" || type_str == "date") && default_str.to_lowercase() == "current_timestamp" {
                        continue;
                    }

                    cols.push(name.to_string());

                    match type_str {
                        "int" | "bigint" | "tinyint" | "smallint" | "largeint" => {
                            let default_value = default_str.parse::<i64>()?;
                            col_mappings.push(format!("{}=ifnull({},{})", name, name, default_value));
                        }
                        "float" | "double" | "decimal" => {
                            let default_value = default_str.parse::<f64>()?;
                            col_mappings.push(format!("{}=ifnull({},{})", name, name, default_value));
                        }
                        _ => {}
                    }
                }

                cols.extend(col_mappings);
                return Ok(cols.join(","));
            } else if i + 1 >= hosts.len().min(3) {
                return Err(format!("Failed to fetch columns from {}: {:?}", self.host, response).into());
            }
        }

        Err(format!("Failed to fetch columns from {}", self.host).into())
    }

/*    fn gene_stream_load_columns2(&self) -> core::result::Result<String, Box<dyn Error>> {
        let sql = format!("DESC {}.{}", self.database, self.table);
        let hosts: Vec<&str> = self.host.split(',').map(|s| s.trim()).collect();

        let client = reqwest::blocking::Client::builder().build()?;

        for (i, host) in hosts.iter().enumerate() {
            let url = format!("http://{}/api/v1/catalogs/default_catalog/databases/{}/sql", host, self.database);
            let mut data = HashMap::new();
            data.insert("query", &sql);

            let request = client
                .post(&url)
                .header("Authorization", basic_auth_header(&self.username, &self.password))
                .header("Content-Type", "application/json")
                .json(&data);
            println!("{:#?}", request);
            let response =  request.send()?;


            if response.status().is_success() {
                let json: Value = response.json()?;
                let rows = json["data"].as_array().ok_or("Invalid response format")?;

                let mut cols = Vec::new();
                let mut col_mappings = Vec::new();

                for row in rows {
                    let name = row["Field"].as_str().ok_or("Missing 'Field' in response")?;
                    let type_str = row["Type"].as_str().ok_or("Missing 'Type' in response")?.to_lowercase();
                    let type_str = type_str.split('(').next().unwrap_or(&type_str); // 去除括号部分
                    let default_str = row["Default"].as_str().unwrap_or("");

                    if default_str.is_empty() {
                        cols.push(name.to_string());
                        continue;
                    }

                    if (type_str == "datetime" || type_str == "date") && default_str.to_lowercase() == "current_timestamp" {
                        continue;
                    }

                    cols.push(name.to_string());

                    match type_str {
                        "int" | "bigint" | "tinyint" | "smallint" | "largeint" => {
                            let default_value = default_str.parse::<i64>()?;
                            col_mappings.push(format!("{}=ifnull({},{})", name, name, default_value));
                        }
                        "float" | "double" | "decimal" => {
                            let default_value = default_str.parse::<f64>()?;
                            col_mappings.push(format!("{}=ifnull({},{})", name, name, default_value));
                        }
                        _ => {}
                    }
                }

                cols.extend(col_mappings);
                return Ok(cols.join(","));
            } else if i + 1 >= hosts.len().min(3) {
                return Err(format!("Failed to fetch columns from {}: {:?}", self.host, response).into());
            }
        }

        Err(format!("Failed to fetch columns from {}", self.host).into())
    }*/
}

pub fn basic_auth_header(username: &str, password: &str) -> String {
    let to_encode = format!("{}:{}", username, password);
    let encoded = general_purpose::STANDARD.encode(&to_encode);
    format!("Basic {}", encoded)
}