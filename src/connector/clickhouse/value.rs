use std::sync::Arc;
use crate::connector::clickhouse::ClickHouseType;
use crate::Result;
use crate::data::Value;
use crate::types::DataType;

const MAX_UNIX_TIME_USE_U64: u64 = u32::MAX as u64;

#[derive(Clone, Debug)]
pub enum ClickHouseValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    String(Arc<String>),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Date(u16),
    DateTime(u32),
    DateTime64(i64, u32),
    Array(Vec<ClickHouseValue>),
}

impl ClickHouseValue {
    pub fn is_null(&self) -> bool {
        match self {
            ClickHouseValue::Null => true,
            _ => false,
        }
    }
}

pub fn make_value_converter(data_type: DataType, ck_type: ClickHouseType) -> Result<Box<dyn ToCkValueConverter>> {
    match ck_type {
        ClickHouseType::Int8 => match data_type {
            DataType::Int => Ok(Box::new(IntToInt8Converter)),
            DataType::Long => Ok(Box::new(LongToInt8Converter)),
            DataType::Float => Ok(Box::new(FloatToInt8Converter)),
            DataType::Double => Ok(Box::new(DoubleToInt8Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::Int16 => match data_type {
            DataType::Int => Ok(Box::new(IntToInt16Converter)),
            DataType::Long => Ok(Box::new(LongToInt16Converter)),
            DataType::Float => Ok(Box::new(FloatToInt16Converter)),
            DataType::Double => Ok(Box::new(DoubleToInt16Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::Int32 => match data_type {
            DataType::Int => Ok(Box::new(IntToInt32Converter)),
            DataType::Long => Ok(Box::new(LongToInt32Converter)),
            DataType::Float => Ok(Box::new(FloatToInt32Converter)),
            DataType::Double => Ok(Box::new(DoubleToInt32Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::Int64 => match data_type {
            DataType::Int => Ok(Box::new(IntToInt64Converter)),
            DataType::Long => Ok(Box::new(LongToInt64Converter)),
            DataType::Float => Ok(Box::new(FloatToInt64Converter)),
            DataType::Double => Ok(Box::new(DoubleToInt64Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::UInt8 => match data_type {
            DataType::Int => Ok(Box::new(IntToUInt8Converter)),
            DataType::Long => Ok(Box::new(LongToUInt8Converter)),
            DataType::Float => Ok(Box::new(FloatToUInt8Converter)),
            DataType::Double => Ok(Box::new(DoubleToUInt8Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::UInt16 => match data_type {
            DataType::Int => Ok(Box::new(IntToUInt16Converter)),
            DataType::Long => Ok(Box::new(LongToUInt16Converter)),
            DataType::Float => Ok(Box::new(FloatToUInt16Converter)),
            DataType::Double => Ok(Box::new(DoubleToUInt16Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::UInt32 => match data_type {
            DataType::Int => Ok(Box::new(IntToUInt32Converter)),
            DataType::Long => Ok(Box::new(LongToUInt32Converter)),
            DataType::Float => Ok(Box::new(FloatToUInt32Converter)),
            DataType::Double => Ok(Box::new(DoubleToUInt32Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::UInt64 => match data_type {
            DataType::Int => Ok(Box::new(IntToUInt64Converter)),
            DataType::Long => Ok(Box::new(LongToUInt64Converter)),
            DataType::Float => Ok(Box::new(FloatToUInt64Converter)),
            DataType::Double => Ok(Box::new(DoubleToUInt64Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::Float32 => match data_type {
            DataType::Int => Ok(Box::new(IntToFloat32Converter)),
            DataType::Long => Ok(Box::new(LongToFloat32Converter)),
            DataType::Float => Ok(Box::new(FloatToFloat32Converter)),
            DataType::Double => Ok(Box::new(DoubleToFloat32Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::Float64 => match data_type {
            DataType::Int => Ok(Box::new(IntToFloat64Converter)),
            DataType::Long => Ok(Box::new(LongToFloat64Converter)),
            DataType::Float => Ok(Box::new(FloatToFloat64Converter)),
            DataType::Double => Ok(Box::new(DoubleToFloat64Converter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::String => match data_type {
            DataType::Int => Ok(Box::new(IntToStringConverter)),
            DataType::Long => Ok(Box::new(LongToStringConverter)),
            DataType::Float => Ok(Box::new(FloatToStringConverter)),
            DataType::Double => Ok(Box::new(DoubleToStringConverter)),
            DataType::String => Ok(Box::new(StringToStringConverter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::DateTime => match data_type {
            DataType::Long => Ok(Box::new(LongToDateTimeConverter)),
            DataType::Timestamp => Ok(Box::new(TimestampToDateTimeConverter)),
            _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
        },
        ClickHouseType::Nullable(ck_tp) => {
            let value_converter = make_value_converter(data_type, *ck_tp)?;
            Ok(Box::new(NullableValueConverter { value_converter }))
        },
        _ => Err(format!("cant not converter {} to {}", data_type, ck_type)),
    }
}


pub trait ToCkValueConverter: Send + 'static {
    fn convert(&self, value: &Value) -> Result<ClickHouseValue>;
}

/*
// IntToInt8Converter, Int, Int8, i8
struct IntToInt8Converter;

impl ToCkValueConverter for IntToInt8Converter {
    fn convert(&self, value: &Value) -> Result<ClickHouseValue> {
        match value {
            Value::Int(v) => Ok(ClickHouseValue::Int8(*v as i8)),
            _ => Err(format!("invalid value for i8: {:?}", value)),
        }
    }
}*/

macro_rules! impl_number_value_converter {
    ($struct_name:ident, $value1:ident, $value2:ident, $type:ty) => {
        pub struct $struct_name;

        impl ToCkValueConverter for $struct_name {
            fn convert(&self, value: &Value) -> Result<ClickHouseValue> {
                match value {
                    Value::$value1(v) => Ok(ClickHouseValue::$value2(*v as $type)),
                    _ => Err(format!("invalid value for {}: {:?}", stringify!($struct_name), value)),
                }
            }
        }
    };
}

impl_number_value_converter!(IntToInt8Converter, Int, Int8, i8);
impl_number_value_converter!(LongToInt8Converter, Long, Int8, i8);
impl_number_value_converter!(FloatToInt8Converter, Float, Int8, i8);
impl_number_value_converter!(DoubleToInt8Converter, Double, Int8, i8);
impl_number_value_converter!(IntToInt16Converter, Int, Int16, i16);
impl_number_value_converter!(LongToInt16Converter, Long, Int16, i16);
impl_number_value_converter!(FloatToInt16Converter, Float, Int16, i16);
impl_number_value_converter!(DoubleToInt16Converter, Double, Int16, i16);
impl_number_value_converter!(IntToInt32Converter, Int, Int32, i32);
impl_number_value_converter!(LongToInt32Converter, Long, Int32, i32);
impl_number_value_converter!(FloatToInt32Converter, Float, Int32, i32);
impl_number_value_converter!(DoubleToInt32Converter, Double, Int32, i32);
impl_number_value_converter!(IntToInt64Converter, Int, Int64, i64);
impl_number_value_converter!(LongToInt64Converter, Long, Int64, i64);
impl_number_value_converter!(FloatToInt64Converter, Float, Int64, i64);
impl_number_value_converter!(DoubleToInt64Converter, Double, Int64, i64);

impl_number_value_converter!(IntToUInt8Converter, Int, UInt8, u8);
impl_number_value_converter!(LongToUInt8Converter, Long, UInt8, u8);
impl_number_value_converter!(FloatToUInt8Converter, Float, UInt8, u8);
impl_number_value_converter!(DoubleToUInt8Converter, Double, UInt8, u8);
impl_number_value_converter!(IntToUInt16Converter, Int, UInt16, u16);
impl_number_value_converter!(LongToUInt16Converter, Long, UInt16, u16);
impl_number_value_converter!(FloatToUInt16Converter, Float, UInt16, u16);
impl_number_value_converter!(DoubleToUInt16Converter, Double, UInt16, u16);
impl_number_value_converter!(IntToUInt32Converter, Int, UInt32, u32);
impl_number_value_converter!(LongToUInt32Converter, Long, UInt32, u32);
impl_number_value_converter!(FloatToUInt32Converter, Float, UInt32, u32);
impl_number_value_converter!(DoubleToUInt32Converter, Double, UInt32, u32);
impl_number_value_converter!(IntToUInt64Converter, Int, UInt64, u64);
impl_number_value_converter!(LongToUInt64Converter, Long, UInt64, u64);
impl_number_value_converter!(FloatToUInt64Converter, Float, UInt64, u64);
impl_number_value_converter!(DoubleToUInt64Converter, Double, UInt64, u64);

impl_number_value_converter!(IntToFloat32Converter, Int, Float32, f32);
impl_number_value_converter!(LongToFloat32Converter, Long, Float32, f32);
impl_number_value_converter!(FloatToFloat32Converter, Float, Float32, f32);
impl_number_value_converter!(DoubleToFloat32Converter, Double, Float32, f32);

impl_number_value_converter!(IntToFloat64Converter, Int, Float64, f64);
impl_number_value_converter!(LongToFloat64Converter, Long, Float64, f64);
impl_number_value_converter!(FloatToFloat64Converter, Float, Float64, f64);
impl_number_value_converter!(DoubleToFloat64Converter, Double, Float64, f64);

macro_rules! impl_number_to_string_converter {
    ($struct_name:ident, $value1:ident) => {
        pub struct $struct_name;

        impl ToCkValueConverter for $struct_name {
            fn convert(&self, value: &Value) -> Result<ClickHouseValue> {
                match value {
                    Value::$value1(v) => Ok(ClickHouseValue::String(Arc::new(v.to_string()))),
                    _ => Err(format!("invalid value for {}: {:?}", stringify!($struct_name), value)),
                }
            }
        }
    };
}

impl_number_to_string_converter!(IntToStringConverter, Int);
impl_number_to_string_converter!(LongToStringConverter, Long);
impl_number_to_string_converter!(FloatToStringConverter, Float);
impl_number_to_string_converter!(DoubleToStringConverter, Double);

struct StringToStringConverter;

impl ToCkValueConverter for StringToStringConverter {
    fn convert(&self, value: &Value) -> Result<ClickHouseValue> {
        match value {
            Value::String(v) => Ok(ClickHouseValue::String(Arc::clone(v))),
            _ => Err(format!("invalid value for StringToStringConverter: {:?}", value)),
        }
    }
}

struct TimestampToDateTimeConverter;

impl ToCkValueConverter for TimestampToDateTimeConverter {
    fn convert(&self, value: &Value) -> Result<ClickHouseValue> {
        match value {
            Value::Long(v) => {
                let unix_time = if *v >= 0 {
                    (*v / 1_000_000) as u32
                } else {
                    0u32
                };
                Ok(ClickHouseValue::DateTime(unix_time))
            },
            _ => Err(format!("invalid value for TimestampToDateTimeConverter: {:?}", value)),
        }
    }
}

struct LongToDateTimeConverter;

impl ToCkValueConverter for LongToDateTimeConverter {
    fn convert(&self, value: &Value) -> Result<ClickHouseValue> {
        match value {
            Value::Long(v) => {
                let value = if *v < 0 {0} else{ *v as u64};
                let unix_time = if value <= MAX_UNIX_TIME_USE_U64 {
                    value as u32
                } else{
                    (value / 1000) as u32
                };
                Ok(ClickHouseValue::DateTime(unix_time))
            },
            _ => Err(format!("invalid value for LongToDateTimeConverter: {:?}", value)),
        }
    }
}

struct NullableValueConverter {
    value_converter: Box<dyn ToCkValueConverter>,
}

impl ToCkValueConverter for NullableValueConverter {
    fn convert(&self, value: &Value) -> Result<ClickHouseValue> {
        match value {
            Value::Null => Ok(ClickHouseValue::Null),
            _ => self.value_converter.convert(value),
        }
    }
}

struct ArrayToArrayConverter {
    ele_converter: Box<dyn ToCkValueConverter>,
}

impl ToCkValueConverter for ArrayToArrayConverter {
    fn convert(&self, value: &Value) -> Result<ClickHouseValue> {
        match value {
            Value::Array(v) => {
                let mut values = Vec::with_capacity(v.len());
                for ele in v.as_ref() {
                    values.push(self.ele_converter.convert(ele)?);
                }
                Ok(ClickHouseValue::Array(values))
            },
            _ => Err(format!("invalid value for ArrayToArrayConverter: {:?}", value)),
        }
    }
}



