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
        let FakerSourceConfig{fields, rows_per_second, number_of_rows, millis_per_row} = & self.source_config;
        let mut fakers: Vec<(usize, Box<dyn Faker>)> = Vec::with_capacity(fields.len());
        for FieldFakerConfig{name, config} in fields {
            if let Some(i) = self.schema.field_index(name) {
                fakers.push((i, config.build()?))
            }
        }
        Ok(Box::new(FakerSource::new(task_context,  self.schema.clone(), fakers, *rows_per_second, *number_of_rows, *millis_per_row)))
    }
}
