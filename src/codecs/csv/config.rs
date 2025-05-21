use serde::{Deserialize, Serialize};
use crate::Result;
use crate::codecs::{Deserializer, DeserializerConfig, Serializer, SerializerConfig};
use crate::codecs::csv::{CsvDeserializer, CsvSerializer};
use crate::types::Schema;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CsvSerializerConfig {
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub double_quote: Option<bool>,
    pub escape: Option<String>,
}

#[typetag::serde(name = "csv")]
impl SerializerConfig for CsvSerializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>> {
        Ok(Box::new(CsvSerializer::new(schema, self.clone())?))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CsvDeserializerConfig {
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub quoting: Option<bool>,
    pub double_quote: Option<bool>,
    pub escape: Option<String>,
}

#[typetag::serde(name = "csv")]
impl DeserializerConfig for CsvDeserializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>> {
        Ok(Box::new(CsvDeserializer::new(schema, self.clone())?))
    }

}