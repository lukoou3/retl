use serde::{Deserialize, Serialize};
use crate::Result;
use crate::codecs::{Deserializer, DeserializerConfig, Serializer, SerializerConfig};
use crate::codecs::raw::RawDeserializer;
use crate::codecs::raw::RawSerializer;
use crate::types::Schema;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawSerializerConfig;

#[typetag::serde(name = "raw")]
impl SerializerConfig for RawSerializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>> {
        Ok(Box::new(RawSerializer::new(schema)?))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawDeserializerConfig;

#[typetag::serde(name = "raw")]
impl DeserializerConfig for RawDeserializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>> {
        Ok(Box::new(RawDeserializer::new(schema)?))
    }
}
