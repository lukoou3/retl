use apache_avro::Schema as AvroSchema;
use serde::{Deserialize, Serialize};
use crate::Result;
use crate::codecs::{Deserializer, DeserializerConfig, Serializer, SerializerConfig};
use crate::types::Schema;
use crate::codecs::avro::decoding::AvroDeserializer;
use crate::codecs::avro::encoding::AvroSerializer;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvroSerializerConfig {
    pub schema_str: Option<String>,
    pub schema_file: Option<String>,
}

#[typetag::serde(name = "avro")]
impl SerializerConfig for AvroSerializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>> {
        let avro_schema = get_schema(&self.schema_str, &self.schema_file)?;
        Ok(Box::new(AvroSerializer::new(schema, avro_schema)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvroDeserializerConfig {
    pub schema_str: Option<String>,
    pub schema_file: Option<String>,
}

#[typetag::serde(name = "avro")]
impl DeserializerConfig for AvroDeserializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>> {
        let avro_schema = get_schema(&self.schema_str, &self.schema_file)?;
        Ok(Box::new(AvroDeserializer::new(schema, avro_schema)?))
    }
}

pub fn get_schema(schema_str: &Option<String>, schema_file: &Option<String>) -> Result<AvroSchema> {
    let str = match (schema_str, schema_file) {
        (Some(s), None) => s.clone(),
        (None, Some(f)) => std::fs::read_to_string(f).map_err(|e| format!("Failed to read schema file: {e}"))?,
        _ => return Err("Either schema_str or schema_file must be specified".to_string()),
    };
    AvroSchema::parse_str(&str).map_err(|e| format!("Failed to parse schema: {e}"))
}
