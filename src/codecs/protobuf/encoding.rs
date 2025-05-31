use std::fmt::{Debug, Formatter};
use bytes::Bytes;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value as PValue};
use prost_reflect::prost::Message;
use crate::Result;
use crate::codecs::Serializer;
use crate::data::{Row, Value};
use crate::types::{DataType, Field, Schema};

pub struct ProtobufSerializer {
    schema: Schema,
    message_descriptor: MessageDescriptor,
    converter: StructToMessageConverter,
    buf: Vec<u8>,
}

impl ProtobufSerializer {
    pub fn new(schema: Schema, message_descriptor: MessageDescriptor) -> Result<Self> {
        let converter = StructToMessageConverter::new(&schema.fields, &message_descriptor)?;
        let mut buf = Vec::new();
        Ok(Self { schema, message_descriptor, converter, buf})
    }
}

impl Debug for ProtobufSerializer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
         f.debug_struct("ProtobufSerializer")
            .field("schema", &self.schema)
            .field("message_descriptor", &self.message_descriptor)
            .finish()
    }
}

impl Serializer for ProtobufSerializer {
    fn serialize<'a>(&'a mut self, row: &'a dyn Row) -> Result<&'a [u8]> {
        let message = self.converter.convert_row(row)?;
        self.buf.clear();
        let len = message.encoded_len();
        self.buf.reserve(len);
        message.encode_raw(&mut self.buf);
        Ok(&self.buf[..len])
    }
}

fn create_converter(data_type: &DataType, fd: &FieldDescriptor)  -> Result<Box<dyn ValueConverter>> {
    if fd.is_list() {
        return match data_type {
            DataType::Array(ele_type) => Ok(Box::new(ArrayToListConverter::new(ele_type, fd)?)),
            _ => Err(format!("type: {:?} can not convert to list field: {:?}", data_type, fd)),
        }
    }
    create_ele_converter(data_type, fd)
}

fn create_ele_converter(data_type: &DataType, fd: &FieldDescriptor)  -> Result<Box<dyn ValueConverter>> {
    let kind = &fd.kind();
    if fd.is_map() {
        return Err(format!("not support map field: {:?}", fd));
    }
    let converter: Box<dyn ValueConverter> = match kind {
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => match data_type {
            DataType::Int => Box::new(IntToI32Converter),
            DataType::Long => Box::new(LongToI32Converter),
            DataType::Float => Box::new(FloatToI32Converter),
            DataType::Double => Box::new(DoubleToI32Converter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => match data_type {
            DataType::Int => Box::new(IntToI64Converter),
            DataType::Long => Box::new(LongToI64Converter),
            DataType::Float => Box::new(FloatToI64Converter),
            DataType::Double => Box::new(DoubleToI64Converter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Uint32 | Kind::Fixed32 => match data_type {
            DataType::Int => Box::new(IntToU32Converter),
            DataType::Long => Box::new(LongToU32Converter),
            DataType::Float => Box::new(FloatToU32Converter),
            DataType::Double => Box::new(DoubleToU32Converter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Uint64 | Kind::Fixed64 => match data_type {
            DataType::Int => Box::new(IntToU64Converter),
            DataType::Long => Box::new(LongToU64Converter),
            DataType::Float => Box::new(FloatToU64Converter),
            DataType::Double => Box::new(DoubleToU64Converter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Float => match data_type {
            DataType::Int => Box::new(IntToF32Converter),
            DataType::Long => Box::new(LongToF32Converter),
            DataType::Float => Box::new(FloatToF32Converter),
            DataType::Double => Box::new(DoubleToF32Converter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Double => match data_type {
            DataType::Int => Box::new(IntToF64Converter),
            DataType::Long => Box::new(LongToF64Converter),
            DataType::Float => Box::new(FloatToF64Converter),
            DataType::Double => Box::new(DoubleToF64Converter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Bool => match data_type {
            DataType::Boolean => Box::new(BooleanToBoolConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::String => match data_type {
            DataType::String => Box::new(StringToStringConverter),
            DataType::Int => Box::new(IntToStringConverter),
            DataType::Long => Box::new(LongToStringConverter),
            DataType::Float => Box::new(FloatToStringConverter),
            DataType::Double => Box::new(DoubleToStringConverter),
            DataType::Boolean => Box::new(BooleanToStringConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Bytes => match data_type {
            DataType::Binary => Box::new(BinaryToBytesConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Enum(_) => match data_type {
            DataType::Int => Box::new(IntToEnumConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Message(message_descriptor) => match data_type {
            DataType::Struct(fields) => Box::new(StructToMessageConverter::new(&fields.0, message_descriptor)?),
            _ => return Err(not_match_err(data_type, fd)),
        },
        //_ => return Err(format!("not support field: {:?}", fd)),
    };
    Ok(converter)
}

fn not_match_err(data_type: &DataType, fd: &FieldDescriptor) -> String {
    format!("not support type: {:?} for field: {:?}", data_type, fd)
}

enum ConverterResult {
    Null,
    Value(PValue),
    Err(String),
}

trait ValueConverter {
    fn convert(&self, value: &Value) -> ConverterResult;
}

/*
// IntToI32Converter, Int, I32, i32
struct IntToI32Converter;

impl ValueConverter for IntToI32Converter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Int(v) => ConverterResult::Value(PValue::I32(*v as i32)),
            _ => ConverterResult::Err(format!("invalid value for I32: {:?}", value)),
        }
    }
}*/

macro_rules! impl_number_value_converter {
    ($struct_name:ident, $value1:ident, $value2:ident, $type:ty) => {
        pub struct $struct_name;

        impl ValueConverter for $struct_name {
            fn convert(&self, value: &Value) -> ConverterResult {
                match value {
                    Value::Null => ConverterResult::Null,
                    Value::$value1(v) => ConverterResult::Value(PValue::$value2(*v as $type)),
                    _ => ConverterResult::Err(format!("invalid value for {}: {:?}", stringify!($struct_name), value)),
                }
            }
        }
    };
}

impl_number_value_converter!(IntToI32Converter, Int, I32, i32);
impl_number_value_converter!(LongToI32Converter, Long, I32, i32);
impl_number_value_converter!(FloatToI32Converter, Float, I32, i32);
impl_number_value_converter!(DoubleToI32Converter, Double, I32, i32);
impl_number_value_converter!(IntToI64Converter, Int, I64, i64);
impl_number_value_converter!(LongToI64Converter, Long, I64, i64);
impl_number_value_converter!(FloatToI64Converter, Float, I64, i64);
impl_number_value_converter!(DoubleToI64Converter, Double, I64, i64);

impl_number_value_converter!(IntToU32Converter, Int, U32, u32);
impl_number_value_converter!(LongToU32Converter, Long, U32, u32);
impl_number_value_converter!(FloatToU32Converter, Float, U32, u32);
impl_number_value_converter!(DoubleToU32Converter, Double, U32, u32);
impl_number_value_converter!(IntToU64Converter, Int, U64, u64);
impl_number_value_converter!(LongToU64Converter, Long, U64, u64);
impl_number_value_converter!(FloatToU64Converter, Float, U64, u64);
impl_number_value_converter!(DoubleToU64Converter, Double, U64, u64);

impl_number_value_converter!(IntToF32Converter, Int, F32, f32);
impl_number_value_converter!(LongToF32Converter, Long, F32, f32);
impl_number_value_converter!(FloatToF32Converter, Float, F32, f32);
impl_number_value_converter!(DoubleToF32Converter, Double, F32, f32);

impl_number_value_converter!(IntToF64Converter, Int, F64, f64);
impl_number_value_converter!(LongToF64Converter, Long, F64, f64);
impl_number_value_converter!(FloatToF64Converter, Float, F64, f64);
impl_number_value_converter!(DoubleToF64Converter, Double, F64, f64);

macro_rules! impl_number_to_string_converter {
    ($struct_name:ident, $value1:ident) => {
        pub struct $struct_name;

        impl ValueConverter for $struct_name {
            fn convert(&self, value: &Value) -> ConverterResult {
                match value {
                    Value::Null => ConverterResult::Null,
                    Value::$value1(v) => ConverterResult::Value(PValue::String(v.to_string())),
                    _ => ConverterResult::Err(format!("invalid value for {}: {:?}", stringify!($struct_name), value)),
                }
            }
        }
    };
}

impl_number_to_string_converter!(IntToStringConverter, Int);
impl_number_to_string_converter!(LongToStringConverter, Long);
impl_number_to_string_converter!(FloatToStringConverter, Float);
impl_number_to_string_converter!(DoubleToStringConverter, Double);
impl_number_to_string_converter!(BooleanToStringConverter, Boolean);

struct StringToStringConverter;

impl ValueConverter for StringToStringConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::String(v) => ConverterResult::Value(PValue::String(v.as_ref().clone())),
            _ => ConverterResult::Err(format!("invalid value for StringToStringConverter: {:?}", value)),
        }
    }
}

struct BooleanToBoolConverter;

impl ValueConverter for BooleanToBoolConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::Boolean(v) => ConverterResult::Value(PValue::Bool(*v)),
            _ => ConverterResult::Err(format!("invalid value for BooleanToBoolConverter: {:?}", value)),
        }
    }
}

struct BinaryToBytesConverter;

impl ValueConverter for BinaryToBytesConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::Binary(v) => ConverterResult::Value(PValue::Bytes(Bytes::copy_from_slice(v.as_slice()))),
            _ => ConverterResult::Err(format!("invalid value for BinaryToBytesConverter: {:?}", value)),
        }
    }
}

struct IntToEnumConverter;

impl ValueConverter for IntToEnumConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::Int(v) => ConverterResult::Value(PValue::EnumNumber(*v)),
            _ => ConverterResult::Err(format!("invalid value for IntToEnumConverter: {:?}", value)),
        }
    }
}

struct StructToMessageConverter {
    message_descriptor: MessageDescriptor,
    converters: Vec<(usize,  FieldDescriptor, Box<dyn ValueConverter>)>,
}

impl StructToMessageConverter {
    fn new(fields: &Vec<Field>, message_descriptor: &MessageDescriptor) -> Result<Self> {
        let mut converters = Vec::new();
        for (i, field) in fields.into_iter().enumerate() {
            match message_descriptor.get_field_by_name(&field.name) {
                Some(fd) => {
                    let converter = create_converter(&field.data_type, &fd)?;
                    converters.push((i, fd, converter));
                },
                None => return Err(format!("Field {} not found in message {}", field.name, message_descriptor.name())),
            }
        }
        Ok(Self{message_descriptor: message_descriptor.clone(), converters})
    }

    fn convert_row(&self, row: &dyn Row) -> Result<DynamicMessage> {
        let mut message = DynamicMessage::new(self.message_descriptor.clone());
        for (i, fd, converter) in &self.converters {
            let value = row.get(*i);
            match converter.convert(value) {
                ConverterResult::Value(v) => message.try_set_field(fd, v)
                    .map_err(|e| format!("Error setting {} field: {}", fd.name(), e))?,
                ConverterResult::Null => (),
                ConverterResult::Err(e) => return Err(e),
            }
        }
        Ok( message)
    }
}

impl ValueConverter for StructToMessageConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::Struct(v) => match self.convert_row(v.as_row()) {
                Ok(m) => ConverterResult::Value(PValue::Message(m)),
                Err(e) => ConverterResult::Err(e),
            },
            _ => ConverterResult::Err(format!("invalid value for StructToMessageConverter: {:?}", value)),
        }
    }
}

struct ArrayToListConverter {
    converter: Box<dyn ValueConverter>,
}

impl ArrayToListConverter {
    fn new(ele_type: &DataType, fd: &FieldDescriptor) -> Result<Self> {
        let converter = create_ele_converter(ele_type, fd)?;
        Ok(Self{converter})
    }
}

 impl ValueConverter for ArrayToListConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::Array(vec) => {
                let mut list = Vec::with_capacity(vec.len());
                for item in vec.iter() {
                    match self.converter.convert(item) {
                        ConverterResult::Value(v) => list.push(v),
                        ConverterResult::Null => return ConverterResult::Err("repeated item can not is null".into()),
                        ConverterResult::Err(e) => return ConverterResult::Err(e),
                    }
                }
                ConverterResult::Value(PValue::List(list))
            },
            _ => ConverterResult::Err(format!("invalid value for ArrayToListConverter: {:?}", value)),
        }
    }
 }

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::codecs::Deserializer;
    use crate::codecs::protobuf::config::get_message_descriptor;
    use crate::codecs::protobuf::decoding::ProtobufDeserializer;
    use crate::data::GenericRow;
    use super::*;

    #[test]
    fn test_serialize() {
        let schema = crate::parser::parse_schema(r#"
        `int64` BIGINT,
        `int32` INT,
        `text` STRING,
        `bytes` BINARY,
        `enum_val` INT,
        `message` STRUCT<`id`: BIGINT, `name`: STRING, `age`: INT, `score`: DOUBLE, `optional_id`: BIGINT, `optional_age`: INT>,
        `optional_int64` BIGINT,
        `optional_int32` INT,
        `optional_text` STRING,
        `optional_bytes` BINARY,
        `optional_enum_val` INT,
        `optional_message` STRUCT<`id`: BIGINT, `name`: STRING, `age`: INT, `score`: DOUBLE, `optional_id`: BIGINT, `optional_age`: INT>,
        `repeated_int64` ARRAY<BIGINT>,
        `repeated_int32` ARRAY<INT>,
        `repeated_message` ARRAY<STRUCT<`id`: BIGINT, `name`: STRING, `age`: INT, `score`: DOUBLE, `optional_id`: BIGINT, `optional_age`: INT>>
        "#).unwrap();
        let descriptor = get_message_descriptor("schema/proto3_types.desc", "Proto3Types").unwrap();
        let row: Box<dyn Row> = Box::new(GenericRow::new(vec![
            Value::long(2),
            Value::int(18),
            Value::string("莫南"),
            Value::Binary(Arc::new(vec![0x01, 0x02, 0x03, 0x04])),
            Value::int(1),
            Value::Struct(Arc::new(GenericRow::new(vec![
                Value::long(3), Value::string("燕青丝"), Value::int(18), Value::double(90.0),
                Value::long(5), Value::int(180),
            ]))),
            Value::long(20),
            Value::int(10),
            Value::string("ut8字符串"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Array(Arc::new( vec![Value::long(1), Value::long(2), Value::long(3),] )),
            Value::Array(Arc::new( vec![Value::int(10), Value::int(20), Value::int(30),] )),
             Value::Array(Arc::new( vec![
                Value::Struct(Arc::new(GenericRow::new(vec![
                    Value::long(1), Value::string("张三"), Value::int(18), Value::double(90.0),
                    Value::long(5), Value::int(180),
                ]))),
                Value::Struct(Arc::new(GenericRow::new(vec![
                    Value::long(2), Value::string("王五"), Value::int(18), Value::double(90.0),
                     Value::Null, Value::Null,
                ])))
             ]))
        ]));
        let mut serialization = ProtobufSerializer::new(schema.clone(), descriptor.clone()).unwrap();
        let mut deserializer = ProtobufDeserializer::new(schema.clone(), descriptor.clone()).unwrap();
        let bytes = serialization.serialize(row.as_ref()).unwrap();
        println!("{}", row.as_ref());
        let rst = deserializer.deserialize(&bytes).unwrap();
        println!("{}", rst);
    }

    #[test]
    fn test_default_serialize() {
        let schema = crate::parser::parse_schema(r#"
        `int64` BIGINT,
        `int32` INT,
        `text` STRING,
        `bytes` BINARY,
        `enum_val` INT,
        `message` STRUCT<`id`: BIGINT, `name`: STRING, `age`: INT, `score`: DOUBLE, `optional_id`: BIGINT, `optional_age`: INT>,
        `optional_int64` BIGINT,
        `optional_int32` INT,
        `optional_text` STRING,
        `optional_bytes` BINARY,
        `optional_enum_val` INT,
        `optional_message` STRUCT<`id`: BIGINT, `name`: STRING, `age`: INT, `score`: DOUBLE, `optional_id`: BIGINT, `optional_age`: INT>,
        `repeated_int64` ARRAY<BIGINT>,
        `repeated_int32` ARRAY<INT>,
        `repeated_message` ARRAY<STRUCT<`id`: BIGINT, `name`: STRING, `age`: INT, `score`: DOUBLE, `optional_id`: BIGINT, `optional_age`: INT>>
        "#).unwrap();
        let descriptor = get_message_descriptor("schema/proto3_types.desc", "Proto3Types").unwrap();
        let row: Box<dyn Row> = Box::new(GenericRow::new(vec![
            Value::long(2),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::int(1),
            Value::Struct(Arc::new(GenericRow::new(vec![
                Value::Null, Value::string("燕青丝"), Value::int(18), Value::Null,
                Value::Null, Value::int(180),
            ]))),
            Value::Null,
            Value::Null,
            Value::string("ut8字符串"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Array(Arc::new( vec![Value::long(1), Value::long(2), Value::long(3),] )),
            Value::Array(Arc::new( vec![Value::int(10), Value::int(20), Value::int(30),] )),
            Value::Array(Arc::new( vec![
                Value::Struct(Arc::new(GenericRow::new(vec![
                    Value::Null, Value::Null, Value::int(18), Value::double(90.0),
                    Value::Null, Value::Null,
                ]))),
                Value::Struct(Arc::new(GenericRow::new(vec![
                    Value::long(2), Value::string("王五"), Value::int(18), Value::double(90.0),
                    Value::Null, Value::Null,
                ])))
            ]))
        ]));
        let mut serialization = ProtobufSerializer::new(schema.clone(), descriptor.clone()).unwrap();
        let mut deserializer = ProtobufDeserializer::new(schema.clone(), descriptor.clone()).unwrap();
        let bytes = serialization.serialize(row.as_ref()).unwrap();
        println!("{}", row.as_ref());
        let rst = deserializer.deserialize(&bytes).unwrap();
        println!("{}", rst);
    }

}
