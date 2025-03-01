use std::io::Write;
use serde::Serializer;
use crate::data::{Row, Value};
use crate::{date_utils, datetime_utils};
use crate::types::{DataType, Field};

macro_rules! tri {
    ($e:expr $(,)?) => {
        match $e {
            core::result::Result::Ok(val) => val,
            core::result::Result::Err(err) => return core::result::Result::Err(err.to_string()),
        }
    };
}

pub struct RowWriter<'a>{
    row: &'a dyn Row,
    fields: &'a Vec<Field>
}

impl RowWriter<'_>{
    pub fn new<'a>(row: &'a dyn Row, fields: &'a Vec<Field>) -> RowWriter<'a> {
        RowWriter{row, fields}
    }
}

impl serde::ser::Serialize for RowWriter<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        use serde::ser::SerializeMap;
        let row = self.row;
        let mut compound = serializer.serialize_map(None)?;
        for (i, field) in self.fields.iter().enumerate() {
            if row.is_null(i) {
                continue;
            }
            compound.serialize_key(& field.name)?;
            match & field.data_type {
                DataType::Int => compound.serialize_value(&row.get_int(i))?,
                DataType::Long => compound.serialize_value(&row.get_long(i))?,
                DataType::Float => compound.serialize_value(&row.get_float(i))?,
                DataType::Double => compound.serialize_value(&row.get_double(i))?,
                DataType::String => compound.serialize_value(row.get_string(i))?,
                DataType::Boolean => compound.serialize_value(&row.get_boolean(i))?,
                DataType::Date => {
                    let date = date_utils::num_days_to_date(row.get_int(i)).to_string();
                    compound.serialize_value(&date)?
                },
                DataType::Timestamp => {
                    let date = datetime_utils::from_timestamp_micros_utc(row.get_long(i)).format(datetime_utils::NORM_DATETIME_FMT).to_string();
                    compound.serialize_value(&date)?
                },
                DataType::Struct(fs) => compound.serialize_value(&RowWriter::new(row.get_struct(i).as_ref(), &fs.0))?,
                DataType::Array(dt) => {
                    compound.serialize_value(&ArrayWriter::new(row.get_array(i).as_ref(), dt.as_ref()))?;
                },
                _ => return Err(serde::ser::Error::custom(format!("does not support {} type", field.data_type))),
            }
        }

        compound.end()
    }
}

struct ArrayWriter<'a>{
    array: &'a Vec<Value>,
    data_type: &'a DataType
}

impl <'a> ArrayWriter<'a>{
    fn new(array: &'a Vec<Value>, data_type: &'a DataType) -> ArrayWriter<'a> {
        ArrayWriter{array, data_type}
    }
}

impl serde::ser::Serialize for ArrayWriter<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        use serde::ser::SerializeSeq;
        let array = self.array;
        let mut compound = serializer.serialize_seq(None)?;
        match self.data_type {
            DataType::Int => {
                for v in array {
                    compound.serialize_element(&v.get_int())?;
                }
            },
            DataType::Long => {
                for v in array {
                    compound.serialize_element(&v.get_long())?;
                }
            },
            DataType::Float => {
                for v in array {
                    compound.serialize_element(&v.get_float())?;
                }
            },
            DataType::Double => {
                for v in array {
                    compound.serialize_element(&v.get_double())?;
                }
            },
            DataType::String => {
                for v in array {
                    compound.serialize_element(v.get_string())?;
                }
            },
            DataType::Boolean => {
                for v in array {
                    compound.serialize_element(&v.get_boolean())?;
                }
            },
            DataType::Struct(fs) => {
                for v in array {
                    compound.serialize_element(&RowWriter::new(v.get_struct().as_ref(), &fs.0))?;
                }
            }
            DataType::Array(dt) => {
                for v in array {
                    compound.serialize_element(&ArrayWriter::new(v.get_array().as_ref(), dt.as_ref()))?;
                }
            },
            _ => return Err(serde::ser::Error::custom(format!("does not support {} type", self.data_type))),
        }

        compound.end()
    }
}

fn write_struct<T: Write>(serializer: &mut serde_json::Serializer<T>, row: &dyn Row, fields: &Vec<Field>) -> crate::Result<()> {
    use serde::ser::SerializeMap;
    if row.len() != fields.len() {
        return Err("field length mismatch".into());
    }

    let mut compound = tri!(serializer.serialize_map(None));
    for (i, field) in fields.iter().enumerate() {
        if row.is_null(i) {
            continue;
        }
        tri!(compound.serialize_key(& field.name));
        match & field.data_type {
            DataType::Int => tri!(compound.serialize_value(&row.get_int(i))),
            DataType::Long => tri!(compound.serialize_value(&row.get_long(i))),
            DataType::Float => tri!(compound.serialize_value(&row.get_float(i))),
            DataType::Double => tri!(compound.serialize_value(&row.get_double(i))),
            DataType::String => tri!(compound.serialize_value(row.get_string(i))),
            DataType::Boolean => tri!(compound.serialize_value(&row.get_boolean(i))),
            DataType::Struct(fs) => {
                tri!(compound.serialize_value(&RowWriter::new(row.get_struct(i).as_ref(), &fs.0)));
                //write_struct(serializer, row.get_struct(i).as_ref(), &fs.0)?;
            },
            DataType::Array(dt) => {
                tri!(compound.serialize_value(&ArrayWriter::new(row.get_array(i).as_ref(), dt.as_ref())));
                //write_array(serializer, row.get_array(i).as_ref(), dt)?;
            },
            _ => return Err(format!("does not support {} type", field.data_type)),
        }
    }

    compound.end().map_err(|e| e.to_string())
}

fn write_array<T: Write>(serializer: &mut serde_json::Serializer<T>, array: &Vec<Value>, data_type: &DataType) -> crate::Result<()> {
    use serde::ser::SerializeSeq;
    let mut compound = tri!(serializer.serialize_seq(None));

    match data_type {
        DataType::Int => {
            for v in array {
                tri!(compound.serialize_element(&v.get_int()));
            }
        },
        DataType::Long => {
            for v in array {
                tri!(compound.serialize_element(&v.get_long()));
            }
        },
        DataType::Float => {
            for v in array {
                tri!(compound.serialize_element(&v.get_float()));
            }
        },
        DataType::Double => {
            for v in array {
                tri!(compound.serialize_element(&v.get_double()));
            }
        },
        DataType::String => {
            for v in array {
                tri!(compound.serialize_element(v.get_string()));
            }
        },
        DataType::Boolean => {
            for v in array {
                tri!(compound.serialize_element(&v.get_boolean()));
            }
        },
        DataType::Struct(fs) => {
            for v in array {
                tri!(compound.serialize_element(&RowWriter::new(v.get_struct().as_ref(), &fs.0)));
            }
        }
        DataType::Array(dt) => {
            for v in array {
                tri!(compound.serialize_element(&ArrayWriter::new(v.get_array().as_ref(), dt.as_ref())));
            }
        },
        _ => return Err(format!("does not support {} type", data_type)),
    }

    compound.end().map_err(|e| e.to_string())
}

trait ValueWriter {
    fn write<T: Write>(&self, serializer: &mut serde_json::Serializer<T>, row: &dyn Row, i: usize) -> crate::Result<()>;
}

#[derive(Debug, Clone)]
struct IntWriter;

impl ValueWriter for IntWriter {
    fn write<T: Write>(&self, serializer: &mut serde_json::Serializer<T>, row: &dyn Row, i: usize) -> crate::Result<()> {
        serializer.serialize_i32(row.get_int(i)).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone)]
struct LongWriter;

impl ValueWriter for LongWriter {
    fn write<T: Write>(&self, serializer: &mut serde_json::Serializer<T>, row: &dyn Row, i: usize) -> crate::Result<()> {
        serializer.serialize_i64(row.get_long(i)).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::data::GenericRow;
    use crate::types::Fields;
    use super::*;

    #[test]
    fn test_write_struct_simple() {
        let fields = vec![
            Field::new("id", DataType::Long),
            Field::new("name", DataType::String),
            Field::new("age", DataType::Int),
            Field::new("score", DataType::Double),
        ];

        let mut row: Box<dyn Row> = Box::new(GenericRow::new(vec![
            Value::long(1),
            Value::string("莫南"),
            Value::int(18),
            Value::double(60.0),
        ]));
        let mut bytes = Vec::new();
        for i in 0..10 {
            let mut serializer = serde_json::Serializer::new(&mut bytes);
            row.update(0, Value::long(i));
            write_struct(&mut serializer, row.as_ref(), &fields).unwrap();
            println!("{}", std::str::from_utf8(&bytes).expect("Invalid UTF-8 data"));
            // println!("{}", String::from_utf8(bytes.clone()).unwrap());
            bytes.clear();
        }


    }

    #[test]
    fn test_write_struct_complex() {

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
        let mut bytes = Vec::new();
        let len = 100;
        for i in 0..len {
            let mut serializer = serde_json::Serializer::new(&mut bytes);
            row.update(0, Value::long(i));
            write_struct(&mut serializer, row.as_ref(), &fields).unwrap();
            if len - i <= 5 {
                println!("{}", std::str::from_utf8(&bytes).expect("Invalid UTF-8 data"));
                // println!("{}", String::from_utf8(bytes.clone()).unwrap());
            }
            bytes.clear();
        }

    }

}