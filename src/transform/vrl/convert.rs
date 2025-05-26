use crate::Result;
use crate::data::Value;
use crate::transform::vrl::VrlValue;
use std::borrow::Cow;
use std::fmt::Debug;
use crate::datetime_utils::{from_timestamp_micros_utc, NORM_DATETIME_FMT};
use crate::types::DataType;

pub fn create_vrl_to_value(data_type: DataType) -> Result<Box<dyn VrlValueToValue>> {
    match data_type {
        DataType::Int => Ok(Box::new(VrlValueToInt)),
        DataType::Long => Ok(Box::new(VrlValueToLong)),
        DataType::Float => Ok(Box::new(VrlValueToFloat)),
        DataType::Double => Ok(Box::new(VrlValueToDouble)),
        DataType::String => Ok(Box::new(VrlValueToString)),
        DataType::Timestamp => Ok(Box::new(VrlValueToTimestamp)),
        _ => Err(format!("Unsupported data type: {:?}", data_type)),
    }
}

pub fn create_value_to_vrl(data_type: DataType) -> Result<Box<dyn ValueToVrlValue>> {
    match data_type {
        DataType::Int => Ok(Box::new(IntValueToVrlValue)),
        DataType::Long => Ok(Box::new(LongValueToVrlValue)),
        DataType::Float => Ok(Box::new(FloatValueToVrlValue)),
        DataType::Double => Ok(Box::new(DoubleValueToVrlValue)),
        DataType::String => Ok(Box::new(StringValueToVrlValue)),
        DataType::Timestamp => Ok(Box::new(TimestampValueToVrlValue)),
        _ => Err(format!("Unsupported data type: {:?}", data_type)),
    }
}

pub trait VrlValueToValue: Debug {
    fn to_value(&self, value: VrlValue) -> Value;
}

#[derive(Debug)]
struct VrlValueToInt;

impl VrlValueToValue for VrlValueToInt {
    fn to_value(&self, value: VrlValue) -> Value {
        match value {
            VrlValue::Integer(i) => Value::Int(i as i32),
            VrlValue::Float(f) => Value::Int(f.into_inner() as i32),
            VrlValue::Bytes(bytes) => match String::from_utf8_lossy(&bytes) {
                Cow::Borrowed(s) => s.parse().map(|i| Value::Int(i)).unwrap_or(Value::Null),
                Cow::Owned(s) => s.parse().map(|i| Value::Int(i)).unwrap_or(Value::Null),
            },
            _ => Value::Null,
        }
    }
}

#[derive(Debug)]
struct VrlValueToLong;

impl VrlValueToValue for VrlValueToLong {
    fn to_value(&self, value: VrlValue) -> Value {
        match value {
            VrlValue::Integer(i) => Value::Long(i),
            VrlValue::Float(f) => Value::Long(f.into_inner() as i64),
            VrlValue::Bytes(bytes) => match String::from_utf8_lossy(&bytes) {
                Cow::Borrowed(s) => s.parse().map(|i| Value::Long(i)).unwrap_or(Value::Null),
                Cow::Owned(s) => s.parse().map(|i| Value::Long(i)).unwrap_or(Value::Null),
            },
            _ => Value::Null,
        }
    }
}

#[derive(Debug)]
struct VrlValueToFloat;

impl VrlValueToValue for VrlValueToFloat {
    fn to_value(&self, value: VrlValue) -> Value {
        match value {
            VrlValue::Integer(i) => Value::Float(i as f32),
            VrlValue::Float(f) => Value::Float(f.into_inner() as f32),
            VrlValue::Bytes(bytes) => match String::from_utf8_lossy(&bytes) {
                Cow::Borrowed(s) => s.parse().map(|f| Value::Float(f)).unwrap_or(Value::Null),
                Cow::Owned(s) => s.parse().map(|f| Value::Float(f)).unwrap_or(Value::Null),
            },
            _ => Value::Null,
        }
    }
}

#[derive(Debug)]
struct VrlValueToDouble;

impl VrlValueToValue for VrlValueToDouble {
    fn to_value(&self, value: VrlValue) -> Value {
        match value {
            VrlValue::Integer(i) => Value::Double(i as f64),
            VrlValue::Float(f) => Value::Double(f.into_inner()),
            VrlValue::Bytes(bytes) => match String::from_utf8_lossy(&bytes) {
                Cow::Borrowed(s) => s.parse().map(|f| Value::Double(f)).unwrap_or(Value::Null),
                Cow::Owned(s) => s.parse().map(|f| Value::Double(f)).unwrap_or(Value::Null),
            },
            _ => Value::Null,
        }
    }
}

#[derive(Debug)]
struct VrlValueToString;

impl VrlValueToValue for VrlValueToString {
    fn to_value(&self, value: VrlValue) -> Value {
        match value {
            VrlValue::Bytes(bytes) => match String::from_utf8_lossy(&bytes) {
                Cow::Borrowed(s) => Value::string(s),
                Cow::Owned(s) => Value::string(s),
            },
            VrlValue::Integer(i) => Value::string(i.to_string()),
            VrlValue::Float(f) => Value::string(f.to_string()),
            VrlValue::Boolean(b) => Value::string(b.to_string()),
            VrlValue::Timestamp(t) => Value::string(t.format(NORM_DATETIME_FMT).to_string()),
            _ => Value::Null,
        }
    }
}

#[derive(Debug)]
struct VrlValueToTimestamp;

impl VrlValueToValue for VrlValueToTimestamp {
    fn to_value(&self, value: VrlValue) -> Value {
        match value {
            VrlValue::Timestamp(t) => Value::Long(t.timestamp_micros()),
            _ => Value::Null,
        }
    }
}

pub trait ValueToVrlValue: Debug {
    fn to_vrl(&self, value: &Value) -> VrlValue;
}

#[derive(Debug)]
struct IntValueToVrlValue;

impl ValueToVrlValue for IntValueToVrlValue {
    fn to_vrl(&self, value: &Value) -> VrlValue {
        match value {
            Value::Int(i) => VrlValue::Integer(*i as i64),
            _ => VrlValue::Null,
        }
    }
}

#[derive(Debug)]
struct LongValueToVrlValue;

impl ValueToVrlValue for LongValueToVrlValue {
    fn to_vrl(&self, value: &Value) -> VrlValue {
        match value {
            Value::Long(l) => VrlValue::Integer(*l),
            _ => VrlValue::Null,
        }
    }
}

#[derive(Debug)]
struct FloatValueToVrlValue;

impl ValueToVrlValue for FloatValueToVrlValue {
    fn to_vrl(&self, value: &Value) -> VrlValue {
        match value {
            Value::Float(f) => VrlValue::from_f64_or_zero(*f as f64),
            _ => VrlValue::Null,
        }
    }
}

#[derive(Debug)]
struct DoubleValueToVrlValue;

impl ValueToVrlValue for DoubleValueToVrlValue {
    fn to_vrl(&self, value: &Value) -> VrlValue {
        match value {
            Value::Double(f) => VrlValue::from_f64_or_zero(*f),
            _ => VrlValue::Null,
        }
    }
}

#[derive(Debug)]
struct StringValueToVrlValue;

impl ValueToVrlValue for StringValueToVrlValue {
    fn to_vrl(&self, value: &Value) -> VrlValue {
        match value {
            Value::String(s) => VrlValue::from(s.as_bytes()),
            _ => VrlValue::Null,
        }
    }
}

#[derive(Debug)]
struct TimestampValueToVrlValue;

impl ValueToVrlValue for TimestampValueToVrlValue {
    fn to_vrl(&self, value: &Value) -> VrlValue {
        match value {
            Value::Long(t) => VrlValue::Timestamp(from_timestamp_micros_utc(*t)),
            _ => VrlValue::Null,
        }
    }
}

