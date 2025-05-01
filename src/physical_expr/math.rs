use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::{BinaryExpr, PhysicalExpr};
use crate::types::DataType;

#[derive(Debug)]
pub struct Pow {
    left: Box<dyn PhysicalExpr>,
    right: Box<dyn PhysicalExpr>,
}

impl Pow {
    pub fn new(left: Box<dyn PhysicalExpr>, right: Box<dyn PhysicalExpr>) -> Self {
        Self {left, right}
    }
}

impl BinaryExpr for Pow {
    fn left(&self) -> &dyn PhysicalExpr {
        self.left.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.right.as_ref()
    }

    fn null_safe_eval(&self, left: Value, right: Value) -> Value {
        let left = left.get_double();
        let right = right.get_double();
        Value::Double(left.powf(right))
    }
}

impl PhysicalExpr for Pow {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::Double
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct Round {
    child: Box<dyn PhysicalExpr>,
    scale: Box<dyn PhysicalExpr>,
}

impl Round {
    pub fn new(child: Box<dyn PhysicalExpr>, scale: Box<dyn PhysicalExpr>) -> Self {
        Self {child, scale}
    }
}

impl BinaryExpr for Round {
    fn left(&self) -> &dyn PhysicalExpr {
        self.child.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.scale.as_ref()
    }

    fn null_safe_eval(&self, child: Value, scale: Value) -> Value {
        let number = child.get_double();
        let decimals = scale.get_int();
        let factor = 10.0_f64.powi(decimals);
        Value::Double((number * factor).round() / factor)
    }
}

impl PhysicalExpr for Round {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::Double
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct Floor {
    child: Box<dyn PhysicalExpr>,
}

impl Floor {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl PhysicalExpr for Floor {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let child = self.child.eval(input);
        match child {
            Value::Long(v) => Value::Long(v),
            Value::Double(v) => Value::Long(v as i64),
            _ => Value::Null
        }
    }
}

#[derive(Debug)]
pub struct Ceil {
    child: Box<dyn PhysicalExpr>,
}

impl Ceil {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl PhysicalExpr for Ceil {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let child = self.child.eval(input);
        match child {
            Value::Long(v) => Value::Long(v),
            Value::Double(v) => Value::Long(v.ceil() as i64),
            _ => Value::Null
        }
    }
}

#[derive(Debug)]
pub struct Bin {
    child: Box<dyn PhysicalExpr>,
    padding: bool,
}

impl Bin {
    pub fn new(child: Box<dyn PhysicalExpr>, padding: bool) -> Self {
        Self {child, padding}
    }
}

impl PhysicalExpr for Bin {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let child = self.child.eval(input);
        match child {
            Value::Int(v) => Value::String(Arc::new(i32_to_binary_string(v, self.padding))),
            Value::Long(v) => Value::String(Arc::new(i64_to_binary_string(v, self.padding))),
            _ => Value::Null
        }
    }
}

pub fn i32_to_binary_string(n: i32, pad_to_32: bool) -> String {
    if n == 0 {
        return if pad_to_32 {
            "0".repeat(32)
        } else {
            "0".to_string()
        };
    }
    let num = n as u32;
    let mut result = Vec::with_capacity(32);
    let mut started = pad_to_32;
    for i in (0..32).rev() {
        let bit = (num >> i) & 1;
        if pad_to_32 || bit == 1 || started {
            result.push(if bit == 1 { '1' } else { '0' });
            started |= bit == 1;
        }
    }
    if result.is_empty() {
        "0".to_string()
    } else {
        result.into_iter().collect()
    }
}

pub fn i64_to_binary_string(n: i64, pad_to_64: bool) -> String {
    if n == 0 {
        return if pad_to_64 {
            "0".repeat(64)
        } else {
            "0".to_string()
        };
    }
    let num = n as u64;
    let mut result = Vec::with_capacity(64);
    let mut started = pad_to_64;
    for i in (0..64).rev() {
        let bit = (num >> i) & 1;
        if pad_to_64 || bit == 1 || started {
            result.push(if bit == 1 { '1' } else { '0' });
            started |= bit == 1;
        }
    }
    if result.is_empty() {
        "0".to_string()
    } else {
        result.into_iter().collect()
    }
}