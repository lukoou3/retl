use std::fmt::Debug;
use serde::{Deserialize, Serialize};
use crate::Result;
use crate::codecs::{CsvDeserializer, JsonDeserializer};
use crate::data::Row;
use crate::types::Schema;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "codec", rename_all = "snake_case")]
pub enum DeserializerConfig {
    Json,
    Csv,
}

impl DeserializerConfig {
    pub fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>> {
        match self {
            DeserializerConfig::Json =>
                Ok(Box::new(JsonDeserializer::new(schema))),
            DeserializerConfig::Csv =>
                Ok(Box::new(CsvDeserializer::new(schema))),
        }

    }
}

pub trait Deserializer: Debug {
    fn deserialize(&mut self, bytes: &[u8]) -> crate::Result<&dyn Row>;
}