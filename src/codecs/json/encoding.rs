use std::io::Write;
use crate::codecs::json::RowWriter;
use crate::Result;
use crate::codecs::Serializer;
use crate::data::{Row, Value};
use crate::types::{DataType, Field, Schema};


#[derive(Debug, Clone)]
pub struct JsonSerializer {
    pub schema: Schema,
    pub pretty: bool,
    pub write_null: bool,
    pub bytes: Vec<u8>,
}

impl JsonSerializer {
    pub fn new(schema: Schema) -> Self {
        Self { schema, pretty: false, write_null: false, bytes: Vec::new() }
    }

    pub fn new_with_pretty(schema: Schema) -> Self {
        Self { schema, pretty: true, write_null: false, bytes: Vec::new() }
    }
    
    pub fn write_null(mut self, write_null: bool) -> Self {
        self.write_null = write_null;
        self
    }
}

impl Serializer for JsonSerializer {
    fn serialize<'a>(&'a mut self, row: &'a dyn Row) -> Result<&'a [u8]> {
        self.bytes.clear();
        let rst = if self.pretty {
            serde_json::to_writer_pretty(&mut self.bytes, &RowWriter::new(row, &self.schema.fields, self.write_null))
        } else {
            serde_json::to_writer(&mut self.bytes, &RowWriter::new(row, &self.schema.fields, self.write_null))
        };
        match rst {
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
