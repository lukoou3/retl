use serde::{Deserialize, Serialize};
use crate::Result;
use crate::config::{SourceConfig, SourceProvider};
use crate::connector::faker::{Faker, FakerSource};
use crate::connector::faker::parse::{FieldFakerConfig};
use crate::connector::Source;
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FakerSourceConfig {
    fields: Vec<FieldFakerConfig>,
    #[serde(default)]
    rows_per_second: i32,
    #[serde(default)]
    number_of_rows: i64,
    #[serde(default)]
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
    fn create_source(&self) -> Result<Box<dyn Source>> {
        let FakerSourceConfig{fields, rows_per_second, number_of_rows, millis_per_row} = & self.source_config;
        let mut fakers: Vec<(usize, Box<dyn Faker>)> = Vec::with_capacity(fields.len());
        for FieldFakerConfig{name, config} in fields {
            if let Some(i) = self.schema.field_index(name) {
                fakers.push((i, config.build()?))
            }
        }
        Ok(Box::new(FakerSource::new(self.schema.clone(), fakers, *rows_per_second)))
    }
}
