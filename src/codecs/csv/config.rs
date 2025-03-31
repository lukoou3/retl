use serde::{Deserialize, Serialize};
use crate::Result;
use crate::codecs::{Deserializer, DeserializerConfig, Serializer, SerializerConfig};
use crate::codecs::csv::{CsvDeserializer, CsvSerializer};
use crate::types::Schema;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CsvSerializerConfig;

#[typetag::serde(name = "csv")]
impl SerializerConfig for CsvSerializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>> {
        Ok(Box::new(CsvSerializer::new(schema)))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CsvDeserializerConfig;

#[typetag::serde(name = "json")]
impl DeserializerConfig for CsvDeserializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>> {
        Ok(Box::new(CsvDeserializer::new(schema)))
    }

}