use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::codecs::DeserializerConfig;
use crate::Result;
use crate::config::{SourceConfig, SourceProvider, TaskContext};
use crate::connector::inline::source::InlineSource;
use crate::connector::Source;
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InlineSourceConfig {
    data: String,
    #[serde(default)]
    data_type: DataType,
    #[serde(default = "default_rows_per_second")]
    rows_per_second: i32,
    #[serde(default = "default_number_of_rows")]
    number_of_rows: i64,
    #[serde(default = "default_millis_per_row")]
    millis_per_row: i64,
    decoding: Box<dyn DeserializerConfig>,
}

#[typetag::serde(name = "inline")]
impl SourceConfig for InlineSourceConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn SourceProvider>> {
        Ok(Box::new(InlineSourceProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct InlineSourceProvider {
    schema: Schema,
    source_config: InlineSourceConfig
}

impl InlineSourceProvider {
    pub fn new(schema: Schema, source_config: InlineSourceConfig) -> Self {
        Self { schema, source_config }
    }

    fn parse_data(&self) -> Result<Vec<Vec<u8>>> {
        let datas = match serde_json::from_str::<Value>(&self.source_config.data){
            Ok(Value::Array(array)) => {
                let mut datas = Vec::new();
                for value in array {
                    match value {
                        Value::String(s) => datas.push(self.parse_data_bytes(&s)?),
                        v=> datas.push(self.parse_data_bytes(&v.to_string())?),
                    }
                }
                datas
            },
            _ => {
                vec![self.parse_data_bytes(&self.source_config.data)?]
            },
        };
        Ok(datas)
    }

    fn parse_data_bytes(&self, data: &str) -> Result<Vec<u8>> {
        match self.source_config.data_type {
            DataType::String => Ok(data.as_bytes().to_vec()),
            DataType::Hex => hex::decode(data).map_err(|e| format!("Invalid hex string: {}", e)),
            DataType::Base64 => base64::engine::general_purpose::STANDARD.decode(data).map_err(|e| format!("Invalid base64 string: {}", e)),
        }
    }
}

impl SourceProvider for InlineSourceProvider {
    fn create_source(&self, task_context: TaskContext) -> Result<Box<dyn Source>> {
        let InlineSourceConfig{rows_per_second, number_of_rows, millis_per_row, ..} = &self.source_config;
        let datas = self.parse_data()?;
        let source = InlineSource::new(task_context, self.schema.clone(), self.source_config.decoding.build(self.schema.clone())?,
                                       datas, *rows_per_second, *number_of_rows, *millis_per_row);
        Ok(Box::new(source))
    }
}

#[derive(Clone, Debug, Copy, Deserialize, Serialize)]
pub enum DataType {
    #[serde(rename = "string")]
    String,
    #[serde(rename = "hex")]
    Hex,
    #[serde(rename = "base64")]
    Base64,
}

impl Default for DataType {
    fn default() -> Self {
        DataType::String
    }
}

fn default_rows_per_second() -> i32 {
    1
}

fn default_number_of_rows() -> i64 {
    i64::MAX
}

fn default_millis_per_row() -> i64 {
    0
}



