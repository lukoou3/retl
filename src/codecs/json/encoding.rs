use std::io::Write;
use crate::Result;
use crate::codecs::{RowWriter, Serializer};
use crate::data::{Row, Value};
use crate::types::{DataType, Field, Schema};


#[derive(Debug, Clone)]
pub struct JsonSerializer {
    pub schema: Schema,
    pub bytes: Vec<u8>,
}

impl JsonSerializer {
    pub fn new(schema: Schema) -> Self {
        Self { schema, bytes: Vec::new() }
    }
}

impl Serializer for JsonSerializer {
    fn serialize(&mut self, row: &dyn Row) -> Result<&[u8]> {
        self.bytes.clear();
        match serde_json::to_writer(&mut self.bytes, &RowWriter::new(row, &self.schema.fields)) {
            Ok(_) => Ok(&self.bytes),
            Err(e) => Err(e.to_string())
        }
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::data::GenericRow;
    use crate::types::Fields;
    use super::*;

    #[test]
    fn test_json_serialization() {
        let fields = vec![
            Field::new("id", DataType::Long),
            Field::new("name", DataType::String),
            Field::new("age", DataType::Int),
            Field::new("score", DataType::Double),
            Field::new("struct", DataType::Struct(Fields(vec![
                Field::new("id", DataType::Long),
                Field::new("name", DataType::String),
            ]))),
            Field::new("array", DataType::Array(Box::new(DataType::Long))),
        ];
        let mut  serialization = JsonSerializer::new(Schema::new(fields));
        let mut row: Box<dyn Row> = Box::new(GenericRow::new(vec![
            Value::long(1),
            Value::string("莫南"),
            Value::int(18),
            Value::double(60.0),
            Value::Struct(Arc::new( GenericRow::new(vec![
                Value::long(2),
                Value::string("燕青丝"),
            ]))),
            Value::Array(Arc::new( vec![Value::long(1), Value::long(2), Value::long(3),] ))
        ]));
        let len = 100;
        for i in 0..len {
            row.update(0, Value::long(i));
            let s = serialization.serialize(row.as_ref()).unwrap();
            if len - i <= 5 {
                println!("{}", std::str::from_utf8(s).expect("Invalid UTF-8 data"));
                // println!("{}", String::from_utf8(bytes.clone()).unwrap());
            }
        }
    }
}
