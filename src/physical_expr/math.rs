use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::{PhysicalExpr};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct Pow {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
}

impl Pow {
    pub fn new(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Self {
        Self {left, right}
    }
}

impl PartialEq for Pow {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left)
            && self.right.eq(&other.right)
    }
}

impl Eq for Pow{}

impl Hash for Pow{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.right.hash(state);
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
        let left = self.left.eval(input);
        if left.is_null() {
            return Value::Null;
        }
        let right = self.right.eval(input);
        if right.is_null() {
            return Value::Null;
        }
        let left = left.get_double();
        let right = right.get_double();
        Value::Double(left.powf(right))
    }
}

#[derive(Debug, Clone)]
pub struct Round {
    child: Arc<dyn PhysicalExpr>,
    scale: Arc<dyn PhysicalExpr>,
}

impl Round {
    pub fn new(child: Arc<dyn PhysicalExpr>, scale: Arc<dyn PhysicalExpr>) -> Self {
        Self {child, scale}
    }
}

impl PartialEq for Round {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.scale.eq(&other.scale)
    }
}

impl Eq for Round{}

impl Hash for Round{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.scale.hash(state);
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
        let child = self.child.eval(input);
        if child.is_null() {
            return Value::Null;
        }
        let scale = self.scale.eval(input);
        if scale.is_null() {
            return Value::Null;
        }
        let number = child.get_double();
        let decimals = scale.get_int();
        let factor = 10.0_f64.powi(decimals);
        Value::Double((number * factor).round() / factor)
    }
}

#[derive(Debug, Clone)]
pub struct Bin {
    child: Arc<dyn PhysicalExpr>,
    padding: bool,
}

impl Bin {
    pub fn new(child: Arc<dyn PhysicalExpr>, padding: bool) -> Self {
        Self {child, padding}
    }
}

impl PartialEq for Bin {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.padding.eq(&other.padding)
    }
}

impl Eq for Bin{}

impl Hash for Bin{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.padding.hash(state);
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