use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value as PValue};
use crate::Result;
use crate::codecs::Deserializer;
use crate::data::{GenericRow, Row, Value};
use crate::types::{DataType, Field, Schema};

pub struct ProtobufDeserializer {
    schema: Schema,
    message_descriptor: MessageDescriptor,
    converter: MessageToStructConverter,
    row: GenericRow,
}

impl ProtobufDeserializer {
    pub fn new(schema: Schema, message_descriptor: MessageDescriptor) -> Result<Self> {
        let converter = MessageToStructConverter::new(&schema.fields, &message_descriptor)?;
        let row = GenericRow::new_with_size(schema.fields.len());
        Ok(Self{schema, message_descriptor, converter, row,})
    }
}

impl Debug for ProtobufDeserializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufDeserializer")
            .field("schema", &self.schema)
            .field("message_descriptor", &self.message_descriptor)
            .finish()
    }
}

impl Deserializer for ProtobufDeserializer {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row> {
        let message = DynamicMessage::decode(self.message_descriptor.clone(), bytes)
            .map_err(|error| format!("Error parsing protobuf: {:?}", error))?;
        self.row.fill_null();
        self.converter.read_row(message, &mut self.row)?;
        Ok(&self.row)
    }
}

fn create_converter(data_type: &DataType, fd: &FieldDescriptor)  -> Result<Box<dyn ValueConverter>> {
    if fd.is_list() {
        return match data_type {
            DataType::Array(ele_type) => Ok(Box::new(ListToArrayConverter::new(ele_type, fd)?)),
            _ => Err(format!("list can not convert to type: {:?}", data_type)),
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
            DataType::Int => Box::new(I32ToIntConverter),
            DataType::Long => Box::new(I32ToLongConverter),
            DataType::Float => Box::new(I32ToFloatConverter),
            DataType::Double => Box::new(I32ToDoubleConverter),
            DataType::String => Box::new(I32ToStringConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => match data_type {
            DataType::Int => Box::new(I64ToIntConverter),
            DataType::Long => Box::new(I64ToLongConverter),
            DataType::Float => Box::new(I64ToFloatConverter),
            DataType::Double => Box::new(I64ToDoubleConverter),
            DataType::String => Box::new(I64ToStringConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Uint32 | Kind::Fixed32 => match data_type {
            DataType::Int => Box::new(U32ToIntConverter),
            DataType::Long => Box::new(U32ToLongConverter),
            DataType::Float => Box::new(U32ToFloatConverter),
            DataType::Double => Box::new(U32ToDoubleConverter),
            DataType::String => Box::new(U32ToStringConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Uint64 | Kind::Fixed64 => match data_type {
            DataType::Int => Box::new(U64ToIntConverter),
            DataType::Long => Box::new(U64ToLongConverter),
            DataType::Float => Box::new(U64ToFloatConverter),
            DataType::Double => Box::new(U64ToDoubleConverter),
            DataType::String => Box::new(U64ToStringConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Float => match data_type {
            DataType::Int => Box::new(F32ToIntConverter),
            DataType::Long => Box::new(F32ToLongConverter),
            DataType::Float => Box::new(F32ToFloatConverter),
            DataType::Double => Box::new(F32ToDoubleConverter),
            DataType::String => Box::new(F32ToStringConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Double => match data_type {
            DataType::Int => Box::new(F64ToIntConverter),
            DataType::Long => Box::new(F64ToLongConverter),
            DataType::Float => Box::new(F64ToFloatConverter),
            DataType::Double => Box::new(F64ToDoubleConverter),
            DataType::String => Box::new(F64ToStringConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Bool => match data_type {
            DataType::Boolean => Box::new(BoolToBooleanConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::String => match data_type {
            DataType::String => Box::new(StringToStringConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Bytes => match data_type {
            DataType::Binary => Box::new(BytesToBinaryConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Enum(_) => match data_type {
            DataType::Int => Box::new(EnumToIntConverter),
            _ => return Err(not_match_err(data_type, fd)),
        },
        Kind::Message(message_descriptor) => match data_type {
            DataType::Struct(fields) => Box::new(MessageToStructConverter::new(&fields.0, message_descriptor)?),
            _ => return Err(not_match_err(data_type, fd)),
        },
        //_ => return Err(format!("not support field: {:?}", fd)),
    };
    Ok(converter)
}

fn not_match_err(data_type: &DataType, fd: &FieldDescriptor) -> String {
    format!("not support type: {:?} for field: {:?}", data_type, fd)
}

trait ValueConverter {
    fn convert(&self, value: PValue) -> Result<Value>;
}

/*
// I32ToIntConverter, I32, Int, i32
struct I32ToIntConverter;

impl ValueConverter for I32ToIntConverter {
    fn convert(&self, value: PValue) -> Result<Value> {
        match value {
            PValue::I32(v) => Ok(Value::Int(v as i32)),
            v => Err(format!("invalid value for Int: {:?}", v)),
        }
    }
}*/

macro_rules! impl_number_value_converter {
    ($struct_name:ident, $value1:ident, $value2:ident, $type:ty) => {
        pub struct $struct_name;

        impl ValueConverter for $struct_name {
            fn convert(&self, value: PValue) -> Result<Value> {
                match value {
                    PValue::$value1(v) => Ok(Value::$value2(v as $type)),
                    v => Err(format!("invalid value for {}: {:?}", stringify!($struct_name), v)),
                }
            }
        }
    };
}

impl_number_value_converter!(I32ToIntConverter, I32, Int, i32);
impl_number_value_converter!(I64ToIntConverter, I64, Int, i32);
impl_number_value_converter!(U32ToIntConverter, U32, Int, i32);
impl_number_value_converter!(U64ToIntConverter, U64, Int, i32);
impl_number_value_converter!(F32ToIntConverter, F32, Int, i32);
impl_number_value_converter!(F64ToIntConverter, F64, Int, i32);

impl_number_value_converter!(I32ToLongConverter, I32, Long, i64);
impl_number_value_converter!(I64ToLongConverter, I64, Long, i64);
impl_number_value_converter!(U32ToLongConverter, U32, Long, i64);
impl_number_value_converter!(U64ToLongConverter, U64, Long, i64);
impl_number_value_converter!(F32ToLongConverter, F32, Long, i64);
impl_number_value_converter!(F64ToLongConverter, F64, Long, i64);

impl_number_value_converter!(I32ToFloatConverter, I32, Float, f32);
impl_number_value_converter!(I64ToFloatConverter, I64, Float, f32);
impl_number_value_converter!(U32ToFloatConverter, U32, Float, f32);
impl_number_value_converter!(U64ToFloatConverter, U64, Float, f32);
impl_number_value_converter!(F32ToFloatConverter, F32, Float, f32);
impl_number_value_converter!(F64ToFloatConverter, F64, Float, f32);

impl_number_value_converter!(I32ToDoubleConverter, I32, Double, f64);
impl_number_value_converter!(I64ToDoubleConverter, I64, Double, f64);
impl_number_value_converter!(U32ToDoubleConverter, U32, Double, f64);
impl_number_value_converter!(U64ToDoubleConverter, U64, Double, f64);
impl_number_value_converter!(F32ToDoubleConverter, F32, Double, f64);
impl_number_value_converter!(F64ToDoubleConverter, F64, Double, f64);

macro_rules! impl_number_to_string_converter {
    ($struct_name:ident, $value1:ident) => {
        pub struct $struct_name;

        impl ValueConverter for $struct_name {
            fn convert(&self, value: PValue) -> Result<Value> {
                match value {
                    PValue::$value1(v) => Ok(Value::String(Arc::new(v.to_string()))),
                    v => Err(format!("invalid value for {}: {:?}", stringify!($struct_name), v)),
                }
            }
        }
    };
}

impl_number_to_string_converter!(I32ToStringConverter, I32);
impl_number_to_string_converter!(I64ToStringConverter, I64);
impl_number_to_string_converter!(U32ToStringConverter, U32);
impl_number_to_string_converter!(U64ToStringConverter, U64);
impl_number_to_string_converter!(F32ToStringConverter, F32);
impl_number_to_string_converter!(F64ToStringConverter, F64);
impl_number_to_string_converter!(BoolToStringConverter, Bool);

struct StringToStringConverter;

impl ValueConverter for StringToStringConverter {
    fn convert(&self, value: PValue) -> Result<Value> {
        match value {
            PValue::String(v) => Ok(Value::String(Arc::new(v))),
            v => Err(format!("invalid value for StringToStringConverter: {:?}", v)),
        }
    }
}

struct BoolToBooleanConverter;

impl ValueConverter for BoolToBooleanConverter {
    fn convert(&self, value: PValue) -> Result<Value> {
        match value {
            PValue::Bool(v) => Ok(Value::Boolean(v)),
            v => Err(format!("invalid value for BoolToBooleanConverter: {:?}", v)),
        }
    }
}

struct BytesToBinaryConverter;

impl ValueConverter for BytesToBinaryConverter {
    fn convert(&self, value: PValue) -> Result<Value> {
        match value {
            PValue::Bytes(v) => Ok(Value::Binary(Arc::new(v.to_vec()))),
            v => Err(format!("invalid value for BytesToBinaryConverter: {:?}", v)),
        }
    }
}

struct EnumToIntConverter;

impl ValueConverter for EnumToIntConverter {
    fn convert(&self, value: PValue) -> Result<Value> {
        match value {
            PValue::EnumNumber(v) => Ok(Value::Int(v)),
            v => Err(format!("invalid value for EnumToIntConverter: {:?}", v)),
        }
    }
}

struct MessageToStructConverter {
    message_descriptor: MessageDescriptor,
    value_converters: HashMap<u32, (usize, Box<dyn ValueConverter>)>,
    size: usize,
}

impl MessageToStructConverter {
    fn new(fields: &Vec<Field>, message_descriptor: &MessageDescriptor) -> Result<Self> {
        let mut value_converters = HashMap::new();
        for (i, field) in fields.into_iter().enumerate() {
            match message_descriptor.get_field_by_name(&field.name) {
                Some(fd) => {
                    let converter = create_converter(&field.data_type, &fd)?;
                    value_converters.insert( fd.number(), (i, converter));
                },
                None => return Err(format!("Field {} not found in message {}", field.name, message_descriptor.name())),
            }
        }
         Ok(Self {
            message_descriptor: message_descriptor.clone(),
            value_converters,
            size: fields.len(),
        })
    }
    fn read_row(&self, mut message: DynamicMessage, row: &mut GenericRow) -> Result<()> {
        for (fd, value) in message.take_fields() {
            if let Some((i, converter)) = self.value_converters.get(&fd.number()) {
                let v = converter.convert(value)?;
                row.update(*i, v);
            }
        }
        Ok(())
    }
}

impl ValueConverter for MessageToStructConverter {
    fn convert(&self, value: PValue) -> Result<Value> {
         match value {
            PValue::Message(message) => {
                let mut row = GenericRow::new_with_size(self.size);
                self.read_row(message, &mut row)?;
                Ok(Value::Struct(Arc::new(row)))
            }
            v => Err(format!("invalid value for MessageToStructConverter: {:?}", v)),
        }
    }
}

struct ListToArrayConverter {
     value_converter: Box<dyn ValueConverter>,
}

impl ListToArrayConverter {
    fn new(ele_type: &DataType, fd: &FieldDescriptor) -> Result<Self> {
        let value_converter = create_ele_converter(ele_type, fd)?;
        Ok(Self{value_converter})
    }
}

impl ValueConverter for ListToArrayConverter {
    fn convert(&self, value: PValue) -> Result<Value> {
        match value {
            PValue::List(list) => {
                let mut array = Vec::with_capacity(list.len());
                for v in list {
                    array.push(self.value_converter.convert(v)?);
                }
                Ok(Value::Array(Arc::new(array)))
            }
            v => Err(format!("invalid value for ListToArrayConverter: {:?}", v)),
        }
    }
}
