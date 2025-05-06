use std::borrow::Cow;
use std::sync::Arc;
use crate::Result;
use crate::codecs::Deserializer;
use crate::data::{GenericRow, Row, Value};
use crate::types::{DataType, Schema};

#[derive(Debug, Clone)]
pub struct RawDeserializer {
    schema: Schema,
    is_str: bool,
    row: GenericRow,
}

impl RawDeserializer {
    pub fn new(schema: Schema) -> Result<Self> {
        if schema.fields.len() != 1 || !matches!(schema.fields[0].data_type, DataType::Binary | DataType::String) {
            return Err("RawDeserializer only support one binary/string field".into());
        }
        let is_str = schema.fields[0].data_type == DataType::String;
        let row = GenericRow::new_with_size(1);
        Ok(RawDeserializer { schema, is_str, row })
    }
}

impl Deserializer for RawDeserializer {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row> {
        if self.is_str {
            match String::from_utf8_lossy(bytes) {
                Cow::Borrowed(v) => self.row.update(0, Value::String(Arc::new(v.to_string()))),
                Cow::Owned(v) => self.row.update(0, Value::String(Arc::new(v))),
            }
        } else {
            self.row.update(0, Value::Binary(Arc::new(bytes.to_vec())));
        }
        Ok(&self.row)
    }
}
