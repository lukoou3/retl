use serde::{Serialize, Deserialize};
use serde::de::{self, Visitor};
use std::fmt;
use crate::Result;
use crate::codecs::{Deserializer, DeserializerConfig, Serializer, SerializerConfig};
use crate::codecs::msgpack::{MessagePackDeserializer, MessagePackSerializer};
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePackSerializerConfig {
    #[serde(default)]
    pub write_null: bool,
    #[serde(default)]
    pub timestamp_type: TimestampType,
}

#[typetag::serde(name = "msgpack")]
impl SerializerConfig for MessagePackSerializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>> {
        Ok(Box::new(MessagePackSerializer::new(schema, self.write_null, self.timestamp_type.clone())?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePackDeserializerConfig {
    #[serde(default)]
    pub timestamp_type: TimestampType,
}

#[typetag::serde(name = "msgpack")]
impl DeserializerConfig for MessagePackDeserializerConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>> {
        Ok(Box::new(MessagePackDeserializer::new(schema, self.timestamp_type.clone())?))
    }
}

#[derive(Debug, Clone)]
pub enum TimestampType {
    Seconds,
    Millis,
    Format(String),
}

impl Default for TimestampType {
    fn default() -> Self {
        TimestampType::Millis
    }
}

impl Serialize for TimestampType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            TimestampType::Seconds => serializer.serialize_str("seconds"),
            TimestampType::Millis => serializer.serialize_str("millis"),
            TimestampType::Format(format_str) => serializer.serialize_str(format_str),
        }
    }
}

impl<'de> Deserialize<'de> for TimestampType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TimestampTypeVisitor;

        impl<'de> Visitor<'de> for TimestampTypeVisitor {
            type Value = TimestampType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("`seconds`, `millis`, or a custom format string (e.g., `%Y-%m-%d`)")
            }

            fn visit_str<E>(self, value: &str) -> Result<TimestampType, E>
            where
                E: de::Error,
            {
                match value {
                    "seconds" => Ok(TimestampType::Seconds),
                    "millis" => Ok(TimestampType::Millis),
                    _ => Ok(TimestampType::Format(value.to_string())),
                }
            }
        }

        deserializer.deserialize_str(TimestampTypeVisitor)
    }
}