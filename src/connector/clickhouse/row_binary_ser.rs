use std::fmt::Debug;
use bytes::{BufMut, BytesMut};
use crate::connector::clickhouse::ClickHouseType;
use crate::Result;
use crate::data::Value;
use crate::types::{DataType, Field, Schema};

const MAX_UNIX_TIME_USE_U64: u64 = u32::MAX as u64;

struct RowBinarySerializer<B: BufMut> {
    buffer: B,
}

macro_rules! impl_num {
    ($ty:ty, $ser_method:ident, $writer_method:ident) => {
        #[inline]
        fn $ser_method(&mut self, v: $ty) {
            self.buffer.$writer_method(v);
        }
    };
}
impl<B: BufMut> RowBinarySerializer<B> {
    pub fn new(buffer: B) -> Self {
        Self { buffer }
    }

    impl_num!(i8, serialize_i8, put_i8);

    impl_num!(i16, serialize_i16, put_i16_le);

    impl_num!(i32, serialize_i32, put_i32_le);

    impl_num!(i64, serialize_i64, put_i64_le);

    impl_num!(i128, serialize_i128, put_i128_le);

    impl_num!(u8, serialize_u8, put_u8);

    impl_num!(u16, serialize_u16, put_u16_le);

    impl_num!(u32, serialize_u32, put_u32_le);

    impl_num!(u64, serialize_u64, put_u64_le);

    impl_num!(u128, serialize_u128, put_u128_le);

    impl_num!(f32, serialize_f32, put_f32_le);

    impl_num!(f64, serialize_f64, put_f64_le);

    #[inline]
    fn serialize_bool(&mut self, v: bool) {
        self.buffer.put_u8(v as _);
    }

    #[inline]
    fn serialize_str(&mut self, v: &str) {
        self.serialize_varint(v.len() as u64);
        self.buffer.put_slice(v.as_bytes());
    }

    #[inline]
    fn serialize_bytes(&mut self, v: &[u8])  {
        self.serialize_varint(v.len() as u64);
        self.buffer.put_slice(v);
    }

    #[inline]
    fn serialize_none(&mut self) {
        self.buffer.put_u8(1);
    }

    #[inline]
    fn serialize_some_header(&mut self) {
        self.buffer.put_u8(0);
    }

    #[inline]
    fn serialize_array_header(&mut self, len: u64) {
        self.serialize_varint(len);
    }

    #[inline]
    fn serialize_varint(&mut self, mut value: u64) {
        while {
            let mut byte = value as u8 & 0x7f;
            value >>= 7;

            if value != 0 {
                byte |= 0x80;
            }

            self.buffer.put_u8(byte);

            value != 0
        } {}
    }
}

pub trait ValueWriter {
    fn write(&mut self, v: &Value, ser: &mut RowBinarySerializer<BytesMut>) -> Result<()> ;
}

/*
Int2I8Writer, Int, serialize_i8, i8
pub struct Int2I8Writer;

impl ValueWriter for Int2I8Writer {
    fn write<B>(&mut self, v: &Value, ser: &mut RowBinarySerializer<B>) -> Result<()> where B: BufMut {
        match v {
            Value::Int(v) => Ok(ser.serialize_i8(*v as i8)),
            _ => Err(format!("invalid value for i8: {:?}", v)),
        }
    }
}
*/

macro_rules! impl_number_value_writer {
    ($struct_name:ident, $variant:ident, $method:ident, $type:ty) => {
        pub struct $struct_name;

        impl ValueWriter for $struct_name {
            fn write(&mut self, v: &Value, ser: &mut RowBinarySerializer<BytesMut>) -> Result<()> {
                match v {
                    Value::$variant(v) => Ok(ser.$method(*v as $type)),
                    _ => Err(format!("invalid value for {}: {:?}", stringify!($struct_name), v)),
                }
            }
        }
    };
}

impl_number_value_writer!(Int2I8Writer, Int, serialize_i8, i8);
impl_number_value_writer!(Long2I8Writer, Long, serialize_i8, i8);
impl_number_value_writer!(Float2I8Writer, Float, serialize_i8, i8);
impl_number_value_writer!(Double2I8Writer, Double, serialize_i8, i8);
impl_number_value_writer!(Int2I16Writer, Int, serialize_i16, i16);
impl_number_value_writer!(Long2I16Writer, Long, serialize_i16, i16);
impl_number_value_writer!(Float2I16Writer, Float, serialize_i16, i16);
impl_number_value_writer!(Double2I16Writer, Double, serialize_i16, i16);
impl_number_value_writer!(Int2I32Writer, Int, serialize_i32, i32);
impl_number_value_writer!(Long2I32Writer, Long, serialize_i32, i32);
impl_number_value_writer!(Float2I32Writer, Float, serialize_i32, i32);
impl_number_value_writer!(Double2I32Writer, Double, serialize_i32, i32);
impl_number_value_writer!(Int2I64Writer, Int, serialize_i64, i64);
impl_number_value_writer!(Long2I64Writer, Long, serialize_i64, i64);
impl_number_value_writer!(Float2I64Writer, Float, serialize_i64, i64);
impl_number_value_writer!(Double2I64Writer, Double, serialize_i64, i64);

impl_number_value_writer!(Int2U8Writer, Int, serialize_u8, u8);
impl_number_value_writer!(Long2U8Writer, Long, serialize_u8, u8);
impl_number_value_writer!(Float2U8Writer, Float, serialize_u8, u8);
impl_number_value_writer!(Double2U8Writer, Double, serialize_u8, u8);
impl_number_value_writer!(Int2U16Writer, Int, serialize_u16, u16);
impl_number_value_writer!(Long2U16Writer, Long, serialize_u16, u16);
impl_number_value_writer!(Float2U16Writer, Float, serialize_u16, u16);
impl_number_value_writer!(Double2U16Writer, Double, serialize_u16, u16);
impl_number_value_writer!(Int2U32Writer, Int, serialize_u32, u32);
impl_number_value_writer!(Long2U32Writer, Long, serialize_u32, u32);
impl_number_value_writer!(Float2U32Writer, Float, serialize_u32, u32);
impl_number_value_writer!(Double2U32Writer, Double, serialize_u32, u32);
impl_number_value_writer!(Int2U64Writer, Int, serialize_u64, u64);
impl_number_value_writer!(Long2U64Writer, Long, serialize_u64, u64);
impl_number_value_writer!(Float2U64Writer, Float, serialize_u64, u64);
impl_number_value_writer!(Double2U64Writer, Double, serialize_u64, u64);

pub struct Long2DateTimeWriter;

impl ValueWriter for Long2DateTimeWriter {
    fn write(&mut self, v: &Value, ser: &mut RowBinarySerializer<BytesMut>) -> Result<()> {
        match v {
            Value::Long(v) => {
                let value = if *v < 0 {0} else{ *v as u64};
                let unix_time = if value <= MAX_UNIX_TIME_USE_U64 {
                    value as u32
                } else{
                    (value / 1000) as u32
                };
                Ok(ser.serialize_u32(unix_time))
            },
            _ => Err(format!("invalid value for i8: {:?}", v)),
        }
    }
}

pub struct Timestamp2DateTimeWriter;

impl ValueWriter for Timestamp2DateTimeWriter {
    fn write(&mut self, v: &Value, ser: &mut RowBinarySerializer<BytesMut>) -> Result<()> {
        match v {
            Value::Long(v) => {
                let unix_time = (*v / 1_000_000) as u32;
                Ok(ser.serialize_u32(unix_time))
            },
            _ => Err(format!("invalid value for i8: {:?}", v)),
        }
    }
}

pub struct StringWriter;

impl ValueWriter for StringWriter {
    fn write(&mut self, v: &Value, ser: &mut RowBinarySerializer<BytesMut>) -> Result<()> {
        match v {
            Value::String(v) => {
                Ok(ser.serialize_str(v.as_str()))
            },
            _ => Err(format!("invalid value for i8: {:?}", v)),
        }
    }
}

pub struct NotNullValueWriter {
    value_writer: Box<dyn ValueWriter>,
    default_value: Value,
}

impl ValueWriter for NotNullValueWriter {
    fn write(&mut self, v: &Value, ser: &mut RowBinarySerializer<BytesMut>) -> Result<()>  {
        match v {
            Value::Null => self.value_writer.write(&self.default_value, ser),
            _ => self.value_writer.write(v, ser),
        }
    }
}

pub struct NullableValueWriter {
    value_writer: Box<dyn ValueWriter>,
}

impl ValueWriter for NullableValueWriter {
    fn write(&mut self, v: &Value, ser: &mut RowBinarySerializer<BytesMut>) -> Result<()> {
        match v {
            Value::Null => Ok(ser.serialize_none()),
            _ => {
                ser.serialize_some_header();
                self.value_writer.write(v, ser)
            },
        }
    }
}

pub struct ArrayValueWriter {
    value_writer: Box<dyn ValueWriter>,
}

impl ValueWriter for ArrayValueWriter {
    fn write(&mut self, v: &Value, ser: &mut RowBinarySerializer<BytesMut>) -> Result<()> {
        match v {
            Value::Array(values) => {
                ser.serialize_array_header(values.len() as u64);
                for v in values.iter() {
                    self.value_writer.write(v, ser)?;
                }
                Ok(())
            },
            _ => Err(format!("invalid value for ArrayValue: {:?}", v)),
        }
    }
}

pub struct RowWriter {
    fields: Vec<Field>,
    field_writers: Vec<Box<dyn ValueWriter>>,
}

impl RowWriter {
    pub fn new(schema: Schema, ck_types: Vec<ClickHouseType>) -> Result<RowWriter> {
        let fields = schema.fields;
        let mut field_writers = Vec::new();
        for (field, ck_type) in fields.iter().zip(ck_types.iter()) {
            let value_writer = create_value_writer(&field.data_type, ck_type)?;
            field_writers.push(value_writer);
        }
        Ok(RowWriter{ fields, field_writers })
    }
}

fn create_value_writer(data_type: &DataType, ck_type: &ClickHouseType) -> Result<Box<dyn ValueWriter>> {
    Err("".into())
}



#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};
    use super::*;

    #[test]
    fn test_number_value_writer() {
        let mut ser = RowBinarySerializer::new(BytesMut::new());
        let mut int2i8writer = Int2I8Writer;
        let values = vec![Value::Int(3), Value::Int(20), Value::Int(-5),];
        for value in &values {
            ser.buffer.clear();
            int2i8writer.write(value, &mut ser).unwrap();
            println!("{:?}", ser.buffer);
            println!("{:?},{:?}", value,  ser.buffer.get_i8());
        }

        let mut long2i8writer = Long2I8Writer;
        let values = vec![Value::Long(3), Value::Long(20), Value::Long(-5),];
        for value in &values {
            ser.buffer.clear();
            long2i8writer.write(value, &mut ser).unwrap();
            println!("{:?}", ser.buffer);
            println!("{:?},{:?}", value, ser.buffer.get_i8());
        }
    }
}