use std::fmt::Debug;
use std::{io, result};
use itertools::Itertools;
use rmp::encode;
use rmp::encode::{RmpWrite, ValueWriteError};
use crate::{datetime_utils, Result};
use crate::codecs::msgpack::config::TimestampType;
use crate::codecs::Serializer;
use crate::data::{Row, Value};
use crate::types::{DataType, Field, Schema};

type EncodeResult = result::Result<(), ValueWriteError>;


#[derive(Debug)]
pub struct MessagePackSerializer {
    schema: Schema,
    buf: Vec<u8>,
    writers: StructWriter,
    write_null: bool,
    timestamp_type: TimestampType,
}

impl MessagePackSerializer {
    pub fn new(schema: Schema, write_null: bool, timestamp_type: TimestampType) -> Result<Self> {
        let mut buf = Vec::new();
        let fields = schema.fields.clone();
        Ok(MessagePackSerializer{
            schema,
            buf,
            writers: StructWriter::new(fields, write_null, timestamp_type.clone())?,
            write_null,
            timestamp_type,
        })
    }
}

impl Serializer for MessagePackSerializer {
    fn serialize<'a>(&'a mut self, row: &'a dyn Row) -> Result<&'a [u8]> {
        self.buf.clear();
        match self.writers.write_row(row, &mut self.buf) {
            Ok(_) => Ok(self.buf.as_slice()),
            Err(e) => Err(e.to_string())
        }
    }
}

fn create_writer(data_type: DataType, write_null: bool, timestamp_type: TimestampType) -> Result<Box<dyn ValueWriter>> {
    match data_type {
        DataType::Int => Ok(Box::new(IntWriter)),
        DataType::Long => Ok(Box::new(LongWriter)),
        DataType::Float => Ok(Box::new(FloatWriter)),
        DataType::Double => Ok(Box::new(DoubleWriter)),
        DataType::String => Ok(Box::new(StringWriter)),
        DataType::Boolean => Ok(Box::new(BooleanWriter)),
        DataType::Timestamp => Ok(Box::new(TimestampWriter{timestamp_type})),
        DataType::Struct(fields) => Ok(Box::new(StructWriter::new(fields.0, write_null, timestamp_type)?)),
        DataType::Array(array) => Ok(Box::new(ArrayWriter::new(*array, write_null, timestamp_type)?)),
        t => Err(format!("unsupported type: {:?}", t))
    }
}

trait ValueWriter: Debug {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult;
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MapMarker {
    FixMap,
    Map16,
    Map32,
}

#[derive(Debug)]
struct StructWriter {
    value_writers: Vec<(usize, String, Box<dyn ValueWriter>)>,
    write_null: bool,
    map_len: u32,
    map_marker: MapMarker,
}

impl StructWriter {
    fn new(fields: Vec<Field>, write_null: bool, timestamp_type: TimestampType) -> Result<Self> {
        let len = fields.len();
        let mut value_writers = Vec::with_capacity(fields.len());
        for (i, field) in fields.into_iter().enumerate() {
            let writer = create_writer(field.data_type, write_null, timestamp_type.clone())?;
            value_writers.push((i, field.name, writer));
        }
        let map_len = len as u32;
        let map_marker = if len < 16 {
            MapMarker::FixMap
        } else if len <= u16::MAX as usize {
            MapMarker::Map16
        } else {
            MapMarker::Map32
        };
        Ok(StructWriter { value_writers, write_null, map_len, map_marker})
    }

    fn write_row(&mut self, row: &dyn Row, buf: &mut Vec<u8>) -> EncodeResult {
        if self.write_null {
            encode::write_map_len(buf, self.map_len)?;
            for (i, name, writer) in &mut self.value_writers {
                let value = row.get(*i);
                match value {
                    Value::Null => {
                        encode::write_str(buf, name)?;
                        encode::write_nil(buf).map_err(ValueWriteError::InvalidMarkerWrite)?;
                    },
                    _ => {
                        encode::write_str(buf, name)?;
                        writer.write(value, buf)?;
                    },
                }
            }
        } else {
            // FixMap(u8), Map16, Map32
            let pos = buf.len();
            match self.map_marker {
                MapMarker::FixMap => {
                    buf.push(0x80 | (self.map_len as u8 & 0x0f));
                },
                MapMarker::Map16 => {
                    buf.push(0xde);
                    buf.extend_from_slice(&(self.map_len as u16).to_be_bytes()); // placeholder for size
                },
                MapMarker::Map32 => {
                    buf.push(0xdf);
                    buf.extend_from_slice(&(self.map_len as u32).to_be_bytes());
                },
            }
            let mut size = 0;
            for (i, name, writer) in &mut self.value_writers {
                let value = row.get(*i);
                match value {
                    Value::Null => (),
                    _ => {
                        encode::write_str(buf, name)?;
                        writer.write(value, buf)?;
                        size += 1;
                    },
                }
            }
            if size != self.map_len {
                let bytes = buf.as_mut_slice();
                match self.map_marker {
                    MapMarker::FixMap => {
                        bytes[pos] = 0x80 | (size as u8 & 0x0f);
                    },
                    MapMarker::Map16 => {
                        bytes[pos + 1.. pos + 3].copy_from_slice(&(size as u16).to_be_bytes());
                    },
                    MapMarker::Map32 => {
                        bytes[pos + 1.. pos + 5].copy_from_slice(&(size as u32).to_be_bytes());
                    },
                }
            }
        }
        Ok(())
    }
}

impl ValueWriter for StructWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        if let Value::Struct(v) = value {
            self.write_row(v.as_row(), buf)
        } else {
            Err(create_encode_error(format!("not struct type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct ArrayWriter {
    value_writer: Box<dyn ValueWriter>,
}

impl ArrayWriter {
    fn new(element_type: DataType, write_null: bool, timestamp_type: TimestampType) -> Result<Self> {
        let writer = create_writer(element_type, write_null, timestamp_type)?;
        Ok(ArrayWriter { value_writer: writer })
    }
}

impl ValueWriter for ArrayWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        if let Value::Array(vec) = value {
            encode::write_array_len(buf, vec.len() as u32)?;
            for v in vec.iter() {
                if v.is_null() {
                    encode::write_nil(buf).map_err(ValueWriteError::InvalidMarkerWrite)?;
                } else {
                    self.value_writer.write(v, buf)?;
                }
            }
            Ok(())
        } else {
            Err(create_encode_error(format!("not array type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct IntWriter;

impl ValueWriter for IntWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        match value {
            Value::Int(v) => {
                encode::write_sint(buf, *v as i64)?;
                Ok(())
            },
            _ => Err(create_encode_error(format!("not int type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct LongWriter;

impl ValueWriter for LongWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        match value {
            Value::Long(v) => {
                encode::write_sint(buf, *v)?;
                Ok(())
            },
            _ => Err(create_encode_error(format!("not long type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct FloatWriter;

impl ValueWriter for FloatWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        match value {
            Value::Float(v) => encode::write_f32(buf, *v),
            _ => Err(create_encode_error(format!("not float type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct DoubleWriter;

impl ValueWriter for DoubleWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        match value {
            Value::Double(v) => encode::write_f64(buf, *v),
            _ => Err(create_encode_error(format!("not double type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct StringWriter;

impl ValueWriter for StringWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        match value {
            Value::String(v) => encode::write_str(buf, v),
            _ => Err(create_encode_error(format!("not string type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct BooleanWriter;

impl ValueWriter for BooleanWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        match value {
            Value::Boolean(v) => encode::write_bool(buf, *v).map_err(ValueWriteError::InvalidMarkerWrite),
            _ => Err(create_encode_error(format!("not boolean type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct TimestampWriter {
    timestamp_type: TimestampType,
}

impl ValueWriter for TimestampWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        match value {
            Value::Long(v) => match &self.timestamp_type {
                TimestampType::Seconds => {
                    encode::write_sint(buf, *v / 1000_000)?;
                    Ok(())
                },
                TimestampType::Millis => {
                    encode::write_sint(buf, *v / 1000)?;
                    Ok(())
                },
                TimestampType::Format(format) => {
                    let tm = datetime_utils::from_timestamp_micros_utc(*v).format(format).to_string();
                    encode::write_str(buf, &tm)
                },
            },
            _ => Err(create_encode_error(format!("not timestamp type: {:?}", value)))
        }
    }
}

#[derive(Debug)]
struct BinaryWriter;

impl ValueWriter for BinaryWriter {
    fn write(&mut self, value: &Value, buf: &mut Vec<u8>) -> EncodeResult {
        match value {
            Value::Binary(v) => encode::write_bin(buf, v),
            _ => Err(create_encode_error(format!("not binary type: {:?}", value)))
        }
    }
}

fn create_encode_error(s: String) -> ValueWriteError {
    ValueWriteError::InvalidDataWrite(io::Error::new(io::ErrorKind::Other, s))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::codecs::Deserializer;
    use crate::codecs::msgpack::MessagePackDeserializer;
    use crate::data::GenericRow;
    use crate::types::Fields;
    use super::*;

    #[test]
    fn test_serialize() {
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
        let mut serialization = MessagePackSerializer::new(Schema::new(fields.clone()), false, TimestampType::Millis).unwrap();
        let mut deserializer = MessagePackDeserializer::new(Schema::new(fields), TimestampType::Millis).unwrap();
        let bytes = serialization.serialize(row.as_ref()).unwrap();
        println!("{}", row.as_ref());
        println!("{}", String::from_utf8_lossy(bytes));
        println!("{:?}", bytes);
        println!("{}", hex::encode(bytes));
        let rst = deserializer.deserialize(&bytes).unwrap();
        println!("{}", rst);

        let mut cursor = io::Cursor::new(bytes);
        let value = rmpv::decode::read_value(&mut cursor).unwrap();
        println!("{:?}", value);
        if let rmpv::Value::Map(map) = value {
            for (key, val) in map {
                match key {
                    rmpv::Value::String(s) => println!("Key: {}, Value: {:?}", s.to_string(), val),
                    _ => println!("Non-string key: {:?}", key),
                }
            }
        }

        row.set_null_at(2);
        let bytes = serialization.serialize(row.as_ref()).unwrap();
        println!("{}", row.as_ref());
        println!("{}", String::from_utf8_lossy(bytes));
        println!("{:?}", bytes);
        println!("{}", hex::encode(bytes));
        let rst = deserializer.deserialize(&bytes).unwrap();
        println!("{}", rst);

        let mut cursor = io::Cursor::new(bytes);
        let value = rmpv::decode::read_value(&mut cursor).unwrap();
        println!("{:?}", value);
        if let rmpv::Value::Map(map) = value {
            for (key, val) in map {
                match key {
                    rmpv::Value::String(s) => println!("Key: {}, Value: {:?}", s.to_string(), val),
                    _ => println!("Non-string key: {:?}", key),
                }
            }
        }

        row.set_null_at(4);
        let bytes = serialization.serialize(row.as_ref()).unwrap();
        println!("{}", row.as_ref());
        let rst = deserializer.deserialize(&bytes).unwrap();
        println!("{}", rst);
    }
}