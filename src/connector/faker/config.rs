use serde::{Deserialize, Serialize};
use crate::Result;
use crate::config::{SourceConfig, SourceProvider, TaskContext};
use crate::connector::faker::{Faker, FakerSource};
use crate::connector::faker::parse::{FieldFakerConfig};
use crate::connector::Source;
use crate::types::Schema;

fn default_rows_per_second() -> i32 {
    1
}

fn default_number_of_rows() -> i64 {
    i64::MAX
}

fn default_millis_per_row() -> i64 {
    0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FakerSourceConfig {
    #[serde(default)]
    fields_desc_file: String,
    #[serde(default)]
    fields: Vec<FieldFakerConfig>,
    #[serde(default = "default_rows_per_second")]
    rows_per_second: i32,
    #[serde(default = "default_number_of_rows")]
    number_of_rows: i64,
    #[serde(default = "default_millis_per_row")]
    millis_per_row: i64,
}

#[typetag::serde(name = "faker")]
impl SourceConfig for FakerSourceConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn SourceProvider>> {
        Ok(Box::new(FakerSourceProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct FakerSourceProvider {
    schema: Schema,
    source_config: FakerSourceConfig
}

impl FakerSourceProvider {
    pub fn new(schema: Schema, source_config: FakerSourceConfig) -> Self {
        Self {
            schema,
            source_config
        }
    }
}

impl SourceProvider for FakerSourceProvider {
    fn create_source(&self, task_context: TaskContext) -> Result<Box<dyn Source>> {
        let FakerSourceConfig{fields_desc_file, fields, rows_per_second, number_of_rows, millis_per_row} = & self.source_config;
        let file_fields: Vec<FieldFakerConfig> = if !fields_desc_file.is_empty() {
            let text = std::fs::read(fields_desc_file).map_err(|e| format!("read file {} error {}", fields_desc_file, e))?;
            serde_json::from_slice(&text).map_err(|e| format!("parse json error {}", e))?
        } else {
            Vec::new()
        };
        let mut fields = if file_fields.is_empty() { fields } else { &file_fields };

        let mut fakers: Vec<(usize, Box<dyn Faker>)> = Vec::with_capacity(fields.len());
        for FieldFakerConfig{name, config} in fields {
            if let Some(i) = self.schema.field_index(name) {
                fakers.push((i, config.build(&self.schema, i)?))
            } else {
                let faker = config.build(&self.schema, 0)?;
                if faker.is_union_faker() {
                    fakers.push((0, faker))
                }
            }
        }
        Ok(Box::new(FakerSource::new(task_context,  self.schema.clone(), fakers, *rows_per_second, *number_of_rows, *millis_per_row)))
    }
}
