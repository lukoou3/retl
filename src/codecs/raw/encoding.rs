use crate::Result;
use crate::codecs::Serializer;
use crate::data::Row;
use crate::types::{DataType, Schema};

#[derive(Debug, Clone)]
pub struct RawSerializer {
    schema: Schema,
    is_str: bool,
}

impl RawSerializer {
    pub fn new(schema: Schema) -> Result<Self> {
        if schema.fields.len() != 1 || !matches!(schema.fields[0].data_type, DataType::Binary | DataType::String) {
            return Err("RawSerializer only support one binary/string field".into());
        }
        let is_str = schema.fields[0].data_type == DataType::String;
        Ok(Self { schema, is_str })
    }
}

impl Serializer for RawSerializer {
    fn serialize<'a>(&'a mut self, row: &'a dyn Row) -> Result<&'a [u8]> {
        if self.is_str {
            Ok(row.get_string_bytes(0))
        } else {
            Ok(row.get_binary_bytes(0))
        }
    }
}