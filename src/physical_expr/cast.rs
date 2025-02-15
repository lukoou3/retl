use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::{PhysicalExpr};
use crate::types::DataType;

pub type CastFunc = dyn Fn(Value) -> Value + Send + Sync;

#[derive(Clone)]
pub struct Cast {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub cast: Arc<CastFunc>,
}

impl Cast {
    pub fn new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Self {
        let cast = get_cast_func(child.data_type(), data_type.clone());
        Cast { child, data_type,  cast}
    }
}

impl Debug for Cast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cast")
            .field("child", &self.child)
            .field("data_type", &self.data_type)
            .finish()
    }
}

impl PartialEq for Cast {
    fn eq(&self, other: &Cast) -> bool {
        self.child.eq(&other.child) && self.data_type == other.data_type
    }
}

impl Eq for Cast{}

impl Hash for Cast {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.data_type.hash(state);
    }
}

impl PhysicalExpr for Cast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        if value.is_null() {
            return Value::Null;
        }
        (self.cast)(value)
    }
}

fn identity(v: Value) -> Value {
    v
}

fn int_to_string(v: Value) -> Value {
    Value::String(Arc::new(v.get_int().to_string()))
}

fn long_to_string(v: Value) -> Value {
    Value::String(Arc::new(v.get_long().to_string()))
}

fn float_to_string(v: Value) -> Value {
    Value::String(Arc::new(v.get_float().to_string()))
}

fn double_to_string(v: Value) -> Value {
    Value::String(Arc::new(v.get_double().to_string()))
}

fn boolean_to_string(v: Value) -> Value {
    Value::String(Arc::new(v.get_boolean().to_string()))
}

fn value_to_string(v: Value) -> Value {
    Value::String(Arc::new(format!("{v}")))
}

fn long_to_int(v: Value) -> Value {
    Value::Int(v.get_long() as i32)
}

fn float_to_int(v: Value) -> Value {
    Value::Int(v.get_float() as i32)
}

fn double_to_int(v: Value) -> Value {
    Value::Int(v.get_double() as i32)
}

fn string_to_int(v: Value) -> Value {
    match v.get_string().parse() {
        Ok(v) => Value::Int(v),
        Err(_) => Value::Null,
    }
}

fn boolean_to_int(v: Value) -> Value {
    if v.get_boolean() {
        Value::Int(1)
    } else {
        Value::Int(0)
    }
}

fn int_to_long(v: Value) -> Value {
    Value::Long(v.get_int() as i64)
}

fn float_to_long(v: Value) -> Value {
    Value::Long(v.get_float() as i64)
}

fn double_to_long(v: Value) -> Value {
    Value::Long(v.get_double() as i64)
}

fn string_to_long(v: Value) -> Value {
    match v.get_string().parse() {
        Ok(v) => Value::Long(v),
        Err(_) => Value::Null,
    }
}

fn boolean_to_long(v: Value) -> Value {
    if v.get_boolean() {
        Value::Long(1)
    } else {
        Value::Long(0)
    }
}

fn int_to_float(v: Value) -> Value {
    Value::Float(v.get_int() as f32)
}

fn long_to_float(v: Value) -> Value {
    Value::Float(v.get_long() as f32)
}

fn double_to_float(v: Value) -> Value {
    Value::Float(v.get_double() as f32)
}

fn string_to_float(v: Value) -> Value {
    match v.get_string().parse() {
        Ok(v) => Value::Float(v),
        Err(_) => Value::Null,
    }
}

fn boolean_to_float(v: Value) -> Value {
    if v.get_boolean() {
        Value::Float(1f32)
    } else {
        Value::Float(0f32)
    }
}

fn int_to_double(v: Value) -> Value {
    Value::Double(v.get_int() as f64)
}

fn long_to_double(v: Value) -> Value {
    Value::Double(v.get_long() as f64)
}

fn float_to_double(v: Value) -> Value {
    Value::Double(v.get_float() as f64)
}

fn string_to_double(v: Value) -> Value {
    match v.get_string().parse() {
        Ok(v) => Value::Double(v),
        Err(_) => Value::Null,
    }
}

fn boolean_to_double(v: Value) -> Value {
    if v.get_boolean() {
        Value::Double(1f64)
    } else {
        Value::Double(0f64)
    }
}

fn get_cast_func(from: DataType, to: DataType) -> Arc<CastFunc> {
    match to {
        dt if dt == from => Arc::new(identity),
        DataType::String => match from {
            DataType::Int => Arc::new(int_to_string),
            DataType::Long => Arc::new(long_to_string),
            DataType::Float => Arc::new(float_to_string),
            DataType::Double => Arc::new(double_to_string),
            DataType::Boolean => Arc::new(boolean_to_string),
            _ =>  Arc::new(value_to_string),
        },
        DataType::Int => match from {
            DataType::Long => Arc::new(long_to_int),
            DataType::Float => Arc::new(float_to_int),
            DataType::Double => Arc::new(double_to_int),
            DataType::String => Arc::new(string_to_int),
            DataType::Boolean => Arc::new(boolean_to_int),
            _ =>  panic!("Cannot cast {from} to {to}.")
        },
        DataType::Long => match from {
            DataType::Int => Arc::new(int_to_long),
            DataType::Float => Arc::new(float_to_long),
            DataType::Double => Arc::new(double_to_long),
            DataType::String => Arc::new(string_to_long),
            DataType::Boolean => Arc::new(boolean_to_long),
            _ =>  panic!("Cannot cast {from} to {to}.")
        },
        DataType::Float => match from {
            DataType::Int => Arc::new(int_to_float),
            DataType::Long => Arc::new(long_to_float),
            DataType::Double => Arc::new(double_to_float),
            DataType::String => Arc::new(string_to_float),
            DataType::Boolean => Arc::new(boolean_to_float),
            _ =>  panic!("Cannot cast {from} to {to}.")
        },
        DataType::Double => match from {
            DataType::Int => Arc::new(int_to_double),
            DataType::Long => Arc::new(long_to_double),
            DataType::Float => Arc::new(float_to_double),
            DataType::String => Arc::new(string_to_double),
            DataType::Boolean => Arc::new(boolean_to_double),
            _ =>  panic!("Cannot cast {from} to {to}.")
        },
        _ =>  panic!("Cannot cast {from} to {to}.")
    }
}

pub fn can_cast(from: DataType, to: DataType) -> bool {
    match (from, to) {
        (from_type, to_type) if from_type == to_type => true,
        (_, DataType::String) => true,
        (DataType::String | DataType::Boolean, to_type) if to_type.is_numeric_type() => true,
        (from_type, to_type) if from_type.is_numeric_type() && to_type.is_numeric_type() => true,
        (_, _) => false
    }
}


