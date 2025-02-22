use std::fmt::Debug;
use serde::{Deserialize, Serialize};
use typetag::serde;
use crate::Result;
use crate::data::Row;
use crate::codecs::{CsvSerializer, JsonSerializer};
use crate::types::Schema;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "codec", rename_all = "snake_case")]
pub enum SerializerConfig {
    Json,
    Csv,
}

impl SerializerConfig {
    pub fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>> {
        match self {
            SerializerConfig::Json =>
                Ok(Box::new(JsonSerializer::new(schema))),
            SerializerConfig::Csv =>
                Ok(Box::new(CsvSerializer::new(schema))),
        }

    }
}


pub trait Serializer: Debug {
    fn serialize(&mut self, row: &dyn Row) -> Result<&[u8]>;
}