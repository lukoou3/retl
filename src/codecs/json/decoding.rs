use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;
use std::vec;
use serde::de::MapAccess;
use serde::Deserializer as SerdeDeserializer;
use serde_json::Value as JsonValue;
use crate::Result;
use crate::data::{GenericRow, Row, Value};
use crate::codecs::Deserializer;
use crate::types::{DataType, Field, Fields, Schema};

#[derive(Debug, Clone)]
pub struct JsonDeserializer {
    pub row_visitor: RowVisitor,
}

impl JsonDeserializer {
    pub fn new(schema: Schema) -> Self {
        let row_visitor = RowVisitor::new(schema.fields);
        JsonDeserializer { row_visitor }
    }
}

impl Deserializer for JsonDeserializer {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row> {
        self.row_visitor.row.fill_null();
        let mut de = serde_json::Deserializer::from_slice(bytes);
        match de.deserialize_map(&mut self.row_visitor) {
            Ok(_) => Ok(& self.row_visitor.row),
            Err(e) => Err(e.to_string())
        }
    }
}

#[derive(Debug, Clone)]
pub struct RowVisitor {
    pub fields: Vec<Field>,
    pub field_types: HashMap<String, (usize, DataType)>,
    pub row: GenericRow,
}

impl RowVisitor {
    pub fn new(fields: Vec<Field>) -> RowVisitor {
        let mut field_types = HashMap::new();
        for (i, f) in fields.iter().enumerate() {
            field_types.insert(f.name.clone(), (i, f.data_type.clone()));
        }
        let mut row = GenericRow::new_with_size(field_types.len());
        RowVisitor{fields, field_types, row}
    }
}

impl<'de> serde::de::Visitor<'de> for &mut RowVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        todo!()
    }

    #[inline]
    fn visit_map<A>(self, mut visitor: A) -> core::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = visitor.next_key::<String>()? {
            if let Some((i, date_type)) = self.field_types.get(&key){
                let v:JsonValue = visitor.next_value()?;
                match json_value_to_value(v, date_type) {
                    Ok(value) => self.row.update(*i, value),
                    Err(e) => return Err(serde::de::Error::custom(e)),
                };
            } else {
                visitor.next_value::<JsonValue>()?;
            }
        }
        Ok(())
    }
}

fn json_value_to_value(value: JsonValue, data_type: &DataType) -> Result<Value> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(v) => match data_type {
            DataType::Boolean =>  Ok(Value::Boolean(v)),
            DataType::Int => if v { Ok(Value::Int(1)) } else { Ok(Value::Int(0)) },
            DataType::Long => if v { Ok(Value::Long(1)) } else { Ok(Value::Long(0)) },
            DataType::Float => if v { Ok(Value::Float(1f32)) } else { Ok(Value::Float(0f32)) },
            DataType::Double => if v { Ok(Value::Double(1f64)) } else { Ok(Value::Double(0f64)) },
            DataType::String => Ok(Value::String(Arc::new(v.to_string()))),
            _ => Err(format!("Cannot convert json bool to {}", data_type))
        }
        JsonValue::Number(v) => match data_type {
            DataType::Int => {
                if v.is_i64() {
                    Ok(Value::Int(v.as_i64().unwrap() as i32))
                } else if v.is_f64() {
                    Ok(Value::Int(v.as_f64().unwrap() as i32))
                } else {
                    Ok(Value::Null)
                }
            },
            DataType::Long => {
                if v.is_i64() {
                    Ok(Value::Long(v.as_i64().unwrap()))
                } else if v.is_f64() {
                    Ok(Value::Long(v.as_f64().unwrap() as i64))
                } else {
                    Ok(Value::Null)
                }
            },
            DataType::Float => {
                if v.is_i64() {
                    Ok(Value::Float(v.as_i64().unwrap()as f32 ))
                } else if v.is_f64() {
                    Ok(Value::Float(v.as_f64().unwrap() as f32))
                } else {
                    Ok(Value::Null)
                }
            },
            DataType::Double => {
                if v.is_i64() {
                    Ok(Value::Double(v.as_i64().unwrap() as f64 ))
                } else if v.is_f64() {
                    Ok(Value::Double(v.as_f64().unwrap()))
                } else {
                    Ok(Value::Null)
                }
            },
            DataType::Boolean => {
                if v.is_i64() {
                    Ok(Value::Boolean(v.as_i64().unwrap() != 0 ))
                } else if v.is_f64() {
                    Ok(Value::Boolean(!(v.as_f64().unwrap() == 0.0)))
                } else {
                    Ok(Value::Null)
                }
            },
            DataType::String => Ok(Value::String(Arc::new(v.to_string()))),
            _ => Err(format!("Cannot convert json number to {}", data_type)),
        }
        JsonValue::String(s) => match data_type {
            DataType::String => Ok(Value::String(Arc::new(s))),
            DataType::Int => match s.parse() {
                Ok(v) => Ok(Value::Int(v)),
                _ => Ok(Value::Null),
            },
            DataType::Long => match s.parse() {
                Ok(v) => Ok(Value::Long(v)),
                _ => Ok(Value::Null),
            },
            DataType::Float => match s.parse() {
                Ok(v) => Ok(Value::Float(v)),
                _ => Ok(Value::Null),
            },
            DataType::Double => match s.parse() {
                Ok(v) => Ok(Value::Double(v)),
                _ => Ok(Value::Null),
            },
            DataType::Boolean => match s.parse() {
                Ok(v) => Ok(Value::Boolean(v)),
                _ => Ok(Value::Null),
            },
            //DataType::Struct(_) => {}
            //DataType::Array(_) => {}
            _ => Err(format!("Cannot convert json number to {}", data_type)),
        },
        JsonValue::Array(values) => match data_type {
            DataType::Array(dt) => {
                let mut  array = Vec::with_capacity(values.len());
                for v in values {
                    array.push(json_value_to_value(v, &dt)?);
                }
                Ok(Value::Array(Arc::new(array)))
            },
            DataType::String => match serde_json::to_string(&values) {
                Ok(s) => Ok(Value::String(Arc::new(s))),
                Err(_) => Ok(Value::Null),
            },
            _ => Err(format!("Cannot convert json number to {}", data_type)),
        },
        JsonValue::Object(map) => match data_type {
            DataType::Struct(Fields(fields)) => {
                let field_types: HashMap<_, _> = fields.iter().enumerate().map(|(i, f)| (&f.name, (i, &f.data_type))).collect();
                let mut row = GenericRow::new_with_size(fields.len());
                for (name, value) in map.into_iter() {
                    if let Some((i, date_type)) = field_types.get(&name){
                        row.update(*i, json_value_to_value(value, *date_type)?);
                    }
                }
                Ok(Value::Struct(Arc::new(row)))
            },
            DataType::String => match serde_json::to_string(&map) {
                Ok(s) => Ok(Value::String(Arc::new(s))),
                Err(_) => Ok(Value::Null),
            },
            _ => Err(format!("Cannot convert json number to {}", data_type)),
        },
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_json_row_visitor() {
        let mut de = serde_json::Deserializer::from_str(r#"
        {
            "id": 1,
            "name": "John Doe",
            "age": 43,
            "struct":{"id":2,"name":"燕青丝"},
            "array":[1,2,3],
            "score":60,
            "age2": 43
        }"#);
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

        let mut row_visitor = RowVisitor::new(fields);
        match de.deserialize_map(&mut row_visitor) {
            Ok(_) => {
                println!("row: {:?}", row_visitor.row);
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    }

    #[test]
    fn test_deserializer() {
        let text = r#"
        {
            "id": 1,
            "name": "John Doe",
            "age": 43,
            "struct":{"id":2,"name":"燕青丝"},
            "array":[1,2,3],
            "score":60,
            "age2": 43
        }"#;
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
        let mut deserializer = JsonDeserializer::new(Schema::new(fields));
        match deserializer.deserialize(text.as_bytes()) {
            Ok(row) => {
                println!("row: {:?}", row);
                println!("row: {}", row);
            },
            Err(e) => println!("{}", e),
        };
    }
}
