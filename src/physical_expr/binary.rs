use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::Operator;
use crate::physical_expr::{BoundReference, PhysicalExpr};
use crate::types::DataType;

pub type BinaryFunc = dyn Fn(Value, Value) -> Value + Send + Sync;

/// Binary expression
#[derive(Clone)]
pub struct BinaryArithmetic {
    pub left: Arc<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Arc<dyn PhysicalExpr>,
    pub f: Arc<BinaryFunc>
}
// f: impl Fn(Value, Value) -> Result<Value>
// f: impl Fn(Value, Value) -> Value
// f: Box<dyn Fn(Value, Value) -> Value>

impl BinaryArithmetic {
    pub fn new(left: Arc<dyn PhysicalExpr>, op: Operator, right: Arc<dyn PhysicalExpr>) -> Self {
        let data_type = left.data_type();
        let f = get_binary_arithmetic_func(op, data_type);
        Self {left, op, right, f}
    }
}

impl Debug for BinaryArithmetic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryArithmetic")
            .field("left",  &self.left)
            .field("op", &self.op)
            .field("right",  &self.right)
            .finish()
    }
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for BinaryArithmetic {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left)
            && self.op.eq(&other.op)
            && self.right.eq(&other.right)
    }
}

impl Hash for BinaryArithmetic {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.op.hash(state);
        self.right.hash(state);
    }
}

impl Eq for BinaryArithmetic {

}

impl PhysicalExpr for BinaryArithmetic {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.left.data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let left_value = self.left.eval(input);
        if left_value.is_null()  {
            return Value::Null;
        }
        let right_value = self.right.eval(input);
        if right_value.is_null() {
            return Value::Null;
        }
        (self.f)(left_value, right_value)
    }
}

fn get_binary_arithmetic_func(op: Operator, data_type: DataType) -> Arc<BinaryFunc> {
    match op {
        Operator::Plus => match data_type {
            DataType::Int => Arc::new(binary_int_add),
            DataType::Long => Arc::new(binary_long_add),
            DataType::Float => Arc::new(binary_float_add),
            DataType::Double => Arc::new(binary_double_add),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::Minus => match data_type {
            DataType::Int => Arc::new(binary_int_subtract),
            DataType::Long => Arc::new(binary_long_subtract),
            DataType::Float => Arc::new(binary_float_subtract),
            DataType::Double => Arc::new(binary_double_subtract),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::Multiply => match data_type {
            DataType::Int => Arc::new(binary_int_multiply),
            DataType::Long => Arc::new(binary_long_multiply),
            DataType::Float => Arc::new(binary_float_multiply),
            DataType::Double => Arc::new(binary_double_multiply),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::Divide => match data_type {
            DataType::Long => Arc::new(binary_long_divide),
            DataType::Double => Arc::new(binary_double_divide),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::Modulo => match data_type {
            DataType::Int => Arc::new(binary_int_multiply),
            DataType::Long => Arc::new(binary_long_multiply),
            DataType::Float => Arc::new(binary_float_multiply),
            DataType::Double => Arc::new(binary_double_multiply),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        _ => panic!("{:?} not support data type {:?}", op, data_type),
    }
}


fn binary_int_add(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x + y),
        _ => Value::Null,
    }
}

fn binary_long_add(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Long(x), Value::Long(y)) => Value::Long(x + y),
        _ => Value::Null,
    }
}

fn binary_float_add(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Float(x), Value::Float(y)) => Value::Float(x + y),
        _ => Value::Null,
    }
}

fn binary_double_add(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Double(x), Value::Double(y)) => Value::Double(x + y),
        _ => Value::Null,
    }
}

fn binary_int_subtract(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x - y),
        _ => Value::Null,
    }
}

fn binary_long_subtract(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Long(x), Value::Long(y)) => Value::Long(x - y),
        _ => Value::Null,
    }
}

fn binary_float_subtract(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Float(x), Value::Float(y)) => Value::Float(x - y),
        _ => Value::Null,
    }
}

fn binary_double_subtract(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Double(x), Value::Double(y)) => Value::Double(x - y),
        _ => Value::Null,
    }
}

fn binary_int_multiply(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x * y),
        _ => Value::Null,
    }
}

fn binary_long_multiply(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Long(x), Value::Long(y)) => Value::Long(x * y),
        _ => Value::Null,
    }
}

fn binary_float_multiply(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Float(x), Value::Float(y)) => Value::Float(x * y),
        _ => Value::Null,
    }
}

fn binary_double_multiply(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Double(x), Value::Double(y)) => Value::Double(x * y),
        _ => Value::Null,
    }
}

fn binary_long_divide(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Long(x), Value::Long(y)) => if y == 0 {Value::Null } else { Value::Long(x /  y) },
        _ => Value::Null,
    }
}

fn binary_double_divide(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Double(x), Value::Double(y)) => if y == 0.0 {Value::Null } else { Value::Double(x /  y) },
        _ => Value::Null,
    }
}

fn binary_int_modulo(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Int(x), Value::Int(y)) => if y == 0 {Value::Null } else { Value::Int(x % y) },
        _ => Value::Null,
    }
}

fn binary_long_modulo(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Long(x), Value::Long(y)) => if y == 0 {Value::Null } else { Value::Long(x % y) },
        _ => Value::Null,
    }
}

fn binary_float_modulo(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Float(x), Value::Float(y)) => if y == 0.0 {Value::Null } else { Value::Float(x % y) },
        _ => Value::Null,
    }
}

fn binary_double_modulo(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Double(x), Value::Double(y)) => if y == 0.0 {Value::Null } else { Value::Double(x % y) },
        _ => Value::Null,
    }
}

//Binary Comparison

fn get_binary_comparison_func(op: Operator) -> Arc<BinaryFunc> {
    match op {
        Operator::Eq => Arc::new(binary_eq),
        Operator::NotEq => Arc::new(binary_ne),
        Operator::Lt => Arc::new(binary_lt),
        Operator::LtEq => Arc::new(binary_lte),
        Operator::Gt => Arc::new(binary_gt),
        Operator::GtEq => Arc::new(binary_gte),
        _ => panic!("unsupported operator {:?}", op),
    }
}

fn binary_eq(left: Value, right: Value) -> Value {
    Value::Boolean(left == right)
}

fn binary_ne(left: Value, right: Value) -> Value {
    Value::Boolean(left != right)
}

fn binary_lt(left: Value, right: Value) -> Value {
    Value::Boolean(left < right)
}

fn binary_gt(left: Value, right: Value) -> Value {
    Value::Boolean(left > right)
}

fn binary_lte(left: Value, right: Value) -> Value {
    Value::Boolean(left <= right)
}

fn binary_gte(left: Value, right: Value) -> Value {
    Value::Boolean(left >= right)
}

#[derive(Clone)]
pub struct BinaryComparison {
    pub left: Arc<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Arc<dyn PhysicalExpr>,
    pub f: Arc<BinaryFunc>
}

impl BinaryComparison {
    pub fn new(left: Arc<dyn PhysicalExpr>, op: Operator, right: Arc<dyn PhysicalExpr>) -> Self {
        let f = get_binary_comparison_func(op);
        Self {left, op, right, f}
    }
}

impl Debug for BinaryComparison {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryComparison")
            .field("left",  &self.left)
            .field("op", &self.op)
            .field("right",  &self.right)
            .finish()
    }
}

impl PartialEq for BinaryComparison {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left)
            && self.op.eq(&other.op)
            && self.right.eq(&other.right)
    }
}

impl Hash for BinaryComparison {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.op.hash(state);
        self.right.hash(state);
    }
}

impl Eq for BinaryComparison {}

impl PhysicalExpr for BinaryComparison {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let left_value = self.left.eval(input);
        if left_value.is_null() {
            return Value::Null;
        }
        let right_value = self.right.eval(input);
        if right_value.is_null() {
            return Value::Null;
        }

        (self.f)(left_value, right_value)
    }
}

#[derive(Debug, Clone)]
pub struct And {
    pub left: Arc<dyn PhysicalExpr>,
    pub right: Arc<dyn PhysicalExpr>,
}

impl And {
    pub fn new(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Self {
        Self { left, right }
    }
}

impl PartialEq for And {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left) && self.right.eq(&other.right)
    }
}

impl Eq for And {}

impl Hash for And {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.right.hash(state);
    }
}

impl PhysicalExpr for And {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let left_value = self.left.eval(input);
        let left_is_null = left_value.is_null();
        if !left_is_null && !left_value.get_boolean() {
           Value::Boolean(false)
        } else {
            let right_value = self.right.eval(input);
            let right_is_null = right_value.is_null();
            if !right_is_null && !right_value.get_boolean() {
                Value::Boolean(false)
            } else {
                if !left_is_null && !right_is_null {
                    Value::Boolean(true)
                }else {
                    Value::Null
                }
            }
        }
    }

}

#[derive(Debug, Clone)]
pub struct Or {
    pub left: Arc<dyn PhysicalExpr>,
    pub right: Arc<dyn PhysicalExpr>,
}

impl Or {
    pub fn new(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Self {
        Self { left, right }
    }
}

impl PartialEq for Or {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left) && self.right.eq(&other.right)
    }
}

impl Eq for Or {}

impl Hash for Or {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.right.hash(state);
    }
}

impl PhysicalExpr for Or {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let left_value = self.left.eval(input);
        let left_is_null = left_value.is_null();
        if !left_is_null && left_value.get_boolean() {
            Value::Boolean(true)
        } else {
            let right_value = self.right.eval(input);
            let right_is_null = right_value.is_null();
            if !right_is_null && right_value.get_boolean() {
                Value::Boolean(true)
            } else {
                if !left_is_null && !right_is_null {
                    Value::Boolean(false)
                }else {
                    Value::Null
                }
            }
        }
    }

}
