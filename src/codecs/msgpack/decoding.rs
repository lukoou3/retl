use std::fmt::Debug;
use std::io::{Read,Cursor};
use std::{io, result};
use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;
use byteorder::ReadBytesExt;
use rmp::{decode, Marker};
use rmp::decode::{RmpRead, ValueReadError};
use crate::Result;
use crate::codecs::Deserializer;
use crate::codecs::msgpack::config::TimestampType;
use crate::data::{GenericRow, Row, Value};
use crate::types::{DataType, Field, Schema};

type DecodeResult = result::Result<Value, ValueReadError>;
// See https://github.com/3Hren/msgpack-rust/issues/151
const PREALLOC_MAX: usize = 64 * 1024; // 64 KiB

#[derive(Debug)]
pub struct MessagePackDeserializer {
    reader: StructReader,
    timestamp_type: TimestampType,
}

impl MessagePackDeserializer {
    pub fn new(schema: Schema, timestamp_type: TimestampType) -> Result<Self> {
        let reader = StructReader::new(schema.fields)?;
        Ok(Self{reader, timestamp_type})
    }
}

impl Deserializer for MessagePackDeserializer {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row> {
        let mut cursor = Cursor::new(bytes);
        match self.reader.read_row(&mut cursor) {
            Ok(v) => Ok(v),
            Err(e) => Err(format!("decode error: {:?}", e)),
        }
    }
}

fn create_reader(data_type: DataType) -> Result<Box<dyn ValueReader>> {
    match data_type {
        DataType::Int => Ok(Box::new(IntReader)),
        DataType::Long => Ok(Box::new(LongReader)),
        DataType::Float => Ok(Box::new(FloatReader)),
        DataType::Double => Ok(Box::new(DoubleReader)),
        DataType::String => Ok(Box::new(StringReader)),
        DataType::Boolean => Ok(Box::new(BooleanReader)),
        DataType::Struct(fields) => Ok(Box::new(StructReader::new(fields.0)?)),
        DataType::Array(element_type) => Ok(Box::new(ArrayReader::new(*element_type)?)),
        t => Err(format!("unsupported data type: {:?}", t))
    }
}

trait ValueReader: Debug {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult;
}

#[derive(Debug)]
struct StructReader {
    value_readers: HashMap<String, (usize, Box<dyn ValueReader>)>,
    row: GenericRow,
}

impl StructReader {
    fn new(fields: Vec<Field>) -> Result<Self> {
        let row = GenericRow::new_with_size(fields.len());
        let mut value_readers = HashMap::new();
        for (i, f) in fields.into_iter().enumerate() {
            let reader = create_reader(f.data_type)?;
            value_readers.insert(f.name, (i, reader));
        }
        Ok(Self{value_readers, row})
    }
    fn read_row(&mut self, rd: &mut Cursor<&[u8]>) -> result::Result<&GenericRow, ValueReadError> {
        let len = match decode::read_marker(rd)? {
            Marker::FixMap(len) => len as u32,
            Marker::Map16 => {
                let len = rd.read_data_u16()?;
                len as u32
            },
            Marker::Map32 => {
                let len = rd.read_data_u32()?;
                len
            },
            other_marker => return Err(create_decode_error(format!("marker can not convert to struct: {:?}", other_marker))),
        };
        self.row.fill_null();
        for _ in 0..len {
            let key = match decode::read_marker(rd)? {
                Marker::FixStr(len) => {
                    read_str_data(rd, len as usize)?
                },
                Marker::Str8 => {
                    let len = rd.read_data_u8()?;
                    read_str_data(rd, len as usize)?
                },
                Marker::Str16 => {
                    let len = rd.read_data_u16()?;
                    read_str_data(rd, len as usize)?
                },
                Marker::Str32 => {
                    let len = rd.read_data_u32()?;
                    read_str_data(rd, len as usize)?
                },
                other_marker => return Err(create_decode_error(format!("marker can not convert to filed key: {:?}", other_marker))),
            };
            let mark = (*rd.get_ref())[rd.position() as usize];
            // Null
            if mark == 0xc0 {
                rd.set_position(rd.position() + 1);
                continue;
            }
            if let Some((i, reader)) = self.value_readers.get_mut(&key) {
                let value = reader.read(rd)?;
                self.row.update(*i, value);
            } else {
                // skip unknown field
                skip_value(rd, 1)?;
            }
        }
        Ok(&self.row)
    }
}

impl ValueReader for StructReader {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult {
        let row = self.read_row(rd)?;
        Ok(Value::Struct(Arc::new(row.clone())))
    }
}

#[derive(Debug)]
struct ArrayReader {
    value_reader: Box<dyn ValueReader>,
}

impl ArrayReader {
    fn new(element_type: DataType) -> Result<Self> {
        let value_reader = create_reader(element_type)?;
        Ok(Self{value_reader})
    }
}

impl ValueReader for ArrayReader {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult {
        let len = match decode::read_marker(rd)? {
            Marker::FixArray(len) => len as usize,
            Marker::Array16 => {
                let len = rd.read_data_u16()?;
                len as usize
            },
            Marker::Array32 => {
                let len = rd.read_data_u32()?;
                len as usize
            },
            other_marker => return Err(create_decode_error(format!("marker can not convert to array: {:?}", other_marker))),
        };
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            let mark = (*rd.get_ref())[rd.position() as usize];
            // Null
            if mark == 0xc0 {
                rd.set_position(rd.position() + 1);
                values.push(Value::Null);
                continue;
            }
            let value = self.value_reader.read(rd)?;
            values.push(value);
        }
        Ok(Value::Array(Arc::new(values)))
    }
}

#[derive(Debug)]
struct IntReader;

impl ValueReader for IntReader {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult {
        let n = match decode::read_marker(rd)? {
            Marker::True => 1,
            Marker::False => 0,
            Marker::FixPos(val) => val as i32,
            Marker::FixNeg(val) => val as i32,
            Marker::U8 => rd.read_data_u8()? as i32,
            Marker::U16 => rd.read_data_u16()? as i32,
            Marker::U32 => rd.read_data_u32()? as i32,
            Marker::U64 => rd.read_data_u64()? as i32,
            Marker::I8 => rd.read_data_i8()? as i32,
            Marker::I16 => rd.read_data_i16()? as i32,
            Marker::I32 => rd.read_data_i32()? as i32,
            Marker::I64 => rd.read_data_i64()? as i32,
            Marker::F32 => rd.read_data_f32()? as i32,
            Marker::F64 => rd.read_data_f64()? as i32,
            Marker::FixStr(len) => {
                let s = read_str_data(rd, len as usize)?;
                str_to_int(s)?
            }
            Marker::Str8 => {
                let len = rd.read_data_u8()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_int(s)?
            }
            Marker::Str16 => {
                let len = rd.read_data_u16()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_int(s)?
            }
            Marker::Str32 => {
                let len = rd.read_data_u32()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_int(s)?
            }
            other_marker => return Err(create_decode_error(format!("marker can not convert to int: {:?}", other_marker))),
        };
        Ok(Value::Int(n))
    }
}

#[derive(Debug)]
struct LongReader;

impl ValueReader for LongReader {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult {
        let n = match decode::read_marker(rd)? {
            Marker::True => 1,
            Marker::False => 0,
            Marker::FixPos(val) => val as i64,
            Marker::FixNeg(val) => val as i64,
            Marker::U8 => rd.read_data_u8()? as i64,
            Marker::U16 => rd.read_data_u16()? as i64,
            Marker::U32 => rd.read_data_u32()? as i64,
            Marker::U64 => rd.read_data_u64()? as i64,
            Marker::I8 => rd.read_data_i8()? as i64,
            Marker::I16 => rd.read_data_i16()? as i64,
            Marker::I32 => rd.read_data_i32()? as i64,
            Marker::I64 => rd.read_data_i64()? as i64,
            Marker::F32 => rd.read_data_f32()? as i64,
            Marker::F64 => rd.read_data_f64()? as i64,
            Marker::FixStr(len) => {
                let s = read_str_data(rd, len as usize)?;
                str_to_long(s)?
            }
            Marker::Str8 => {
                let len = rd.read_data_u8()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_long(s)?
            }
            Marker::Str16 => {
                let len = rd.read_data_u16()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_long(s)?
            }
            Marker::Str32 => {
                let len = rd.read_data_u32()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_long(s)?
            }
            other_marker => return Err(create_decode_error(format!("marker can not convert to long: {:?}", other_marker))),
        };
        Ok(Value::Long(n))
    }
}

#[derive(Debug)]
struct FloatReader;

impl ValueReader for FloatReader {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult {
        let n = match decode::read_marker(rd)? {
            Marker::True => 1.0,
            Marker::False => 0.0,
            Marker::FixPos(val) => val as f32,
            Marker::FixNeg(val) => val as f32,
            Marker::U8 => rd.read_data_u8()? as f32,
            Marker::U16 => rd.read_data_u16()? as f32,
            Marker::U32 => rd.read_data_u32()? as f32,
            Marker::U64 => rd.read_data_u64()? as f32,
            Marker::I8 => rd.read_data_i8()? as f32,
            Marker::I16 => rd.read_data_i16()? as f32,
            Marker::I32 => rd.read_data_i32()? as f32,
            Marker::I64 => rd.read_data_i64()? as f32,
            Marker::F32 => rd.read_data_f32()? as f32,
            Marker::F64 => rd.read_data_f64()? as f32,
            Marker::FixStr(len) => {
                let s = read_str_data(rd, len as usize)?;
                str_to_float(s)?
            },
            Marker::Str8 => {
                let len = rd.read_data_u8()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_float(s)?
            },
            Marker::Str16 => {
                let len = rd.read_data_u16()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_float(s)?
            },
            Marker::Str32 => {
                let len = rd.read_data_u32()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_float(s)?
            },
            other_marker => return Err(create_decode_error(format!("marker can not convert to float: {:?}", other_marker))),
        };
        Ok(Value::Float(n))
    }
}

#[derive(Debug)]
struct DoubleReader;

impl ValueReader for DoubleReader {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult {
        let n = match decode::read_marker(rd)? {
            Marker::True => 1.0,
            Marker::False => 0.0,
            Marker::FixPos(val) => val as f64,
            Marker::FixNeg(val) => val as f64,
            Marker::U8 => rd.read_data_u8()? as f64,
            Marker::U16 => rd.read_data_u16()? as f64,
            Marker::U32 => rd.read_data_u32()? as f64,
            Marker::U64 => rd.read_data_u64()? as f64,
            Marker::I8 => rd.read_data_i8()? as f64,
            Marker::I16 => rd.read_data_i16()? as f64,
            Marker::I32 => rd.read_data_i32()? as f64,
            Marker::I64 => rd.read_data_i64()? as f64,
            Marker::F32 => rd.read_data_f32()? as f64,
            Marker::F64 => rd.read_data_f64()? as f64,
            Marker::FixStr(len) => {
                let s = read_str_data(rd, len as usize)?;
                str_to_double(s)?
            }
            Marker::Str8 => {
                let len = rd.read_data_u8()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_double(s)?
            }
            Marker::Str16 => {
                let len = rd.read_data_u16()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_double(s)?
            }
            Marker::Str32 => {
                let len = rd.read_data_u32()?;
                let s = read_str_data(rd, len as usize)?;
                str_to_double(s)?
            }
            other_marker => return Err(create_decode_error(format!("marker can not convert to double: {:?}", other_marker))),
        };
        Ok(Value::Double(n))
    }
}

#[derive(Debug)]
struct StringReader;

impl ValueReader for StringReader {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult {
        let str = match decode::read_marker(rd)? {
            Marker::FixStr(len) => {
                read_str_data(rd, len as usize)?
            },
            Marker::Str8 => {
                let len = rd.read_data_u8()?;
                read_str_data(rd, len as usize)?
            },
            Marker::Str16 => {
                let len = rd.read_data_u16()?;
                read_str_data(rd, len as usize)?
            },
            Marker::Str32 => {
                let len = rd.read_data_u32()?;
                read_str_data(rd, len as usize)?
            },
            Marker::True => "true".to_string(),
            Marker::False => "false".to_string(),
            Marker::FixPos(val) => val.to_string(),
            Marker::FixNeg(val) => val.to_string(),
            Marker::U8 => rd.read_data_u8()?.to_string(),
            Marker::U16 => rd.read_data_u16()?.to_string(),
            Marker::U32 => rd.read_data_u32()?.to_string(),
            Marker::U64 => rd.read_data_u64()?.to_string(),
            Marker::I8 => rd.read_data_i8()?.to_string(),
            Marker::I16 => rd.read_data_i16()?.to_string(),
            Marker::I32 => rd.read_data_i32()?.to_string(),
            Marker::I64 => rd.read_data_i64()?.to_string(),
            Marker::F32 => rd.read_data_f32()?.to_string(),
            Marker::F64 => rd.read_data_f64()?.to_string(),
            other_marker => return Err(create_decode_error(format!("marker can not convert to str: {:?}", other_marker))),
        };
        Ok(Value::String(Arc::new(str)))
    }
}

#[derive(Debug)]
struct BooleanReader;

impl ValueReader for BooleanReader {
    fn read(&mut self, rd: &mut Cursor<&[u8]>) -> DecodeResult {
        let b = match decode::read_marker(rd)? {
            Marker::True => true,
            Marker::False => false,
            other_marker => return Err(create_decode_error(format!("marker can not convert to bool: {:?}", other_marker))),
        };
        Ok(Value::Boolean(b))
    }
}

fn str_to_int(s: String) -> Result<i32, ValueReadError> {
    s.parse().map_err(|_| create_decode_error(format!("cant convert to int: {}", s)))
}

fn str_to_long(s: String) -> Result<i64, ValueReadError> {
    s.parse().map_err(|_| create_decode_error(format!("cant convert to long: {}", s)))
}

fn str_to_float(s: String) -> Result<f32, ValueReadError> {
    s.parse().map_err(|_| create_decode_error(format!("cant convert to float: {}", s)))
}

fn str_to_double(s: String) -> Result<f64, ValueReadError> {
    s.parse().map_err(|_| create_decode_error(format!("cant convert to double: {}", s)))
}

fn read_str_data(rd: &mut Cursor<&[u8]>, len: usize) -> Result<String, ValueReadError> {
    let mut buf = Vec::with_capacity(min(len, PREALLOC_MAX));
    let bytes_read = rd.take(len as u64).read_to_end(&mut buf).map_err(ValueReadError::InvalidDataRead)?;
    if bytes_read != len {
        return Err(ValueReadError::InvalidDataRead(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("Expected {len} bytes, read {bytes_read} bytes"),
        )));
    }
    match String::from_utf8(buf) {
        Ok(s) => Ok(s),
        Err(e) => Ok(String::from_utf8_lossy(e.as_bytes()).into_owned())
    }
}

fn read_bin_data(rd: &mut Cursor<&[u8]>, len: usize) -> Result<Vec<u8>, ValueReadError> {
    let mut buf = Vec::with_capacity(min(len, PREALLOC_MAX));
    let bytes_read = rd.take(len as u64).read_to_end(&mut buf).map_err(ValueReadError::InvalidDataRead)?;
    if bytes_read != len {
        return Err(ValueReadError::InvalidDataRead(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("Expected {len} bytes, read {bytes_read} bytes"),
        )));
    }

    Ok(buf)
}

fn skip_value(rd: &mut Cursor<&[u8]>, mut count: u32) -> Result<(), ValueReadError> {
    while count > 0 {
        let marker = decode::read_marker(rd)?;
        match marker {
            Marker::Null |
            Marker::True |
            Marker::False |
            Marker::FixPos(_) |
            Marker::FixNeg(_) => (),
            Marker::U8 | Marker::I8 => skip_bytes(rd, 1),
            Marker::U16 | Marker::I16 => skip_bytes(rd, 2),
            Marker::U32 | Marker::I32 | Marker::F32 => skip_bytes(rd, 4),
            Marker::U64 | Marker::I64 | Marker::F64 => skip_bytes(rd, 8),
            Marker::FixStr(len) => skip_bytes(rd, len as u64),
            Marker::Bin8 | Marker::Str8 => {
                let len = rd.read_data_u8()?;
                skip_bytes(rd, len as u64)
            },
            Marker::Bin16 | Marker::Str16 => {
                let len = rd.read_data_u16()?;
                skip_bytes(rd, len as u64)
            },
            Marker::Bin32 | Marker::Str32 => {
                let len = rd.read_data_u32()?;
                skip_bytes(rd, len as u64)
            },
            Marker::FixMap(len) => {
                count += (len as u32) * 2;
            },
            Marker::Map16 => {
                let len = rd.read_data_u16()?;
                count += (len as u32) * 2;
            },
            Marker::Map32 => {
                let len = rd.read_data_u32()?;
                count += (len as u32) * 2;
            },
            Marker::FixArray(len) => {
                count += len as u32;
            },
            Marker::Array16 => {
                let len = rd.read_data_u16()?;
                count += len as u32;
            },
            Marker::Array32 => {
                let len = rd.read_data_u32()?;
                count += len as u32;
            },
            other_marker => return Err(create_decode_error(format!("not support marker: {:?}", other_marker))),
        };
        count -= 1;
    }
    Ok(())
}

fn skip_bytes(rd: &mut Cursor<&[u8]>, num: u64) {
    let pos = rd.position();
    rd.set_position(pos + num);
}

fn create_decode_error(s: String) -> ValueReadError {
    ValueReadError::InvalidDataRead(io::Error::new(io::ErrorKind::Other, s))
}
