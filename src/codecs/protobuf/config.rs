use crate::Result;
use crate::codecs::protobuf::decoding::ProtobufDeserializer;
use crate::codecs::protobuf::encoding::ProtobufSerializer;
use crate::codecs::{Deserializer, DeserializerConfig, Serializer, SerializerConfig};
use crate::types::Schema;
use prost_reflect::{DescriptorPool, MessageDescriptor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtobufSerializerConfig {
    pub desc_file: String,
    pub message_name: String,
}

#[typetag::serde(name = "protobuf")]
impl SerializerConfig for ProtobufSerializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>> {
        let message_descriptor = get_message_descriptor(&self.desc_file, &self.message_name)?;
        Ok(Box::new(ProtobufSerializer::new(
            schema,
            message_descriptor,
        )?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtobufDeserializerConfig {
    pub desc_file: String,
    pub message_name: String,
}

#[typetag::serde(name = "protobuf")]
impl DeserializerConfig for ProtobufDeserializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>> {
        let message_descriptor = get_message_descriptor(&self.desc_file, &self.message_name)?;
        Ok(Box::new(ProtobufDeserializer::new(schema,message_descriptor,)?))
    }
}

pub fn get_message_descriptor(
    descriptor_set_path: &str,
    message_name: &str,
) -> Result<MessageDescriptor> {
    let b = std::fs::read(descriptor_set_path).map_err(|e| {
        format!("Failed to open protobuf desc file '{descriptor_set_path:?}': {e}",)
    })?;
    let pool = DescriptorPool::decode(b.as_slice()).map_err(|e| {
        format!("Failed to parse protobuf desc file '{descriptor_set_path:?}': {e}")
    })?;
    pool.get_message_by_name(message_name).ok_or_else(|| {
        format!("The message type '{message_name}' could not be found in '{descriptor_set_path:?}'")
    })
}
