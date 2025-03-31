use serde::{Deserialize, Serialize};
use crate::Result;
use crate::codecs::{Deserializer, DeserializerConfig, Serializer, SerializerConfig};
use crate::codecs::json::{JsonDeserializer, JsonSerializer};
use crate::data::Row;
use crate::types::Schema;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JsonSerializerConfig {
    #[serde(default)]
    pub pretty: bool,
}

#[typetag::serde(name = "json")]
impl SerializerConfig for JsonSerializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>> {
        if self.pretty {
            Ok(Box::new(JsonSerializer::new_with_pretty(schema)))
        } else {
            Ok(Box::new(JsonSerializer::new(schema)))
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JsonDeserializerConfig;

#[typetag::serde(name = "json")]
impl DeserializerConfig for JsonDeserializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>> {
        Ok(Box::new(JsonDeserializer::new(schema)))
    }

}


