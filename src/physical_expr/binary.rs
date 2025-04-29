use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use crate::data::{Row, Value};
use crate::Operator;
use crate::physical_expr::{BinaryExpr, PhysicalExpr};
use crate::types::DataType;

pub type BinaryFunc = dyn Fn(Value, Value) -> Value + Send + Sync;

/// Binary expression
pub struct BinaryArithmetic {
    pub left: Box<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Box<dyn PhysicalExpr>,
    pub f: Box<BinaryFunc>
}
// f: impl Fn(Value, Value) -> Result<Value>
// f: impl Fn(Value, Value) -> Value
// f: Box<dyn Fn(Value, Value) -> Value>

impl BinaryArithmetic {
    pub fn new(left: Box<dyn PhysicalExpr>, op: Operator, right: Box<dyn PhysicalExpr>) -> Self {
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

impl BinaryExpr for BinaryArithmetic {
    fn left(&self) -> &dyn PhysicalExpr {
        self.left.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.right.as_ref()
    }

    fn null_safe_eval(&self, left_value: Value, right_value: Value) -> Value {
        (self.f)(left_value, right_value)
    }
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
        BinaryExpr::eval(self, input)
    }
}

fn get_binary_arithmetic_func(op: Operator, data_type: DataType) -> Box<BinaryFunc> {
    match op {
        Operator::Plus => match data_type {
            DataType::Int => Box::new(binary_int_add),
            DataType::Long => Box::new(binary_long_add),
            DataType::Float => Box::new(binary_float_add),
            DataType::Double => Box::new(binary_double_add),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::Minus => match data_type {
            DataType::Int => Box::new(binary_int_subtract),
            DataType::Long => Box::new(binary_long_subtract),
            DataType::Float => Box::new(binary_float_subtract),
            DataType::Double => Box::new(binary_double_subtract),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::Multiply => match data_type {
            DataType::Int => Box::new(binary_int_multiply),
            DataType::Long => Box::new(binary_long_multiply),
            DataType::Float => Box::new(binary_float_multiply),
            DataType::Double => Box::new(binary_double_multiply),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::Divide => match data_type {
            DataType::Long => Box::new(binary_long_divide),
            DataType::Double => Box::new(binary_double_divide),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::Modulo => match data_type {
            DataType::Int => Box::new(binary_int_modulo),
            DataType::Long => Box::new(binary_long_modulo),
            DataType::Float => Box::new(binary_float_modulo),
            DataType::Double => Box::new(binary_double_modulo),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::BitAnd => match data_type {
            DataType::Int => Box::new(int_bitwise_and),
            DataType::Long => Box::new(long_bitwise_and),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::BitOr => match data_type {
            DataType::Int => Box::new(int_bitwise_or),
            DataType::Long => Box::new(long_bitwise_or),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        Operator::BitXor => match data_type {
            DataType::Int => Box::new(int_bitwise_x_or),
            DataType::Long => Box::new(long_bitwise_x_or),
            _ => panic!("{:?} not support data type {:?}", op, data_type),
        }
        _ => panic!("{:?} not support data type {:?}", op, data_type),
    }
}

fn int_bitwise_and(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x & y),
        _ => Value::Null,
    }
}

fn long_bitwise_and(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Long(x), Value::Long(y)) => Value::Long(x & y),
        _ => Value::Null,
    }
}

fn int_bitwise_or(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x | y),
        _ => Value::Null,
    }
}

fn long_bitwise_or(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Long(x), Value::Long(y)) => Value::Long(x | y),
        _ => Value::Null,
    }
}

fn int_bitwise_x_or(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x ^ y),
        _ => Value::Null,
    }
}

fn long_bitwise_x_or(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Long(x), Value::Long(y)) => Value::Long(x ^ y),
        _ => Value::Null,
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

fn get_binary_comparison_func(op: Operator) -> Box<BinaryFunc> {
    match op {
        Operator::Eq => Box::new(binary_eq),
        Operator::NotEq => Box::new(binary_ne),
        Operator::Lt => Box::new(binary_lt),
        Operator::LtEq => Box::new(binary_lte),
        Operator::Gt => Box::new(binary_gt),
        Operator::GtEq => Box::new(binary_gte),
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

pub struct BinaryComparison {
    pub left: Box<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Box<dyn PhysicalExpr>,
    pub f: Box<BinaryFunc>
}

impl BinaryComparison {
    pub fn new(left: Box<dyn PhysicalExpr>, op: Operator, right: Box<dyn PhysicalExpr>) -> Self {
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

impl BinaryExpr for BinaryComparison {
    fn left(&self) -> &dyn PhysicalExpr {
        self.left.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.right.as_ref()
    }

    fn null_safe_eval(&self, left_value: Value, right_value: Value) -> Value {
        (self.f)(left_value, right_value)
    }
}

impl PhysicalExpr for BinaryComparison {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

/// Binary Shift expression
pub struct BinaryShift {
    pub left: Box<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Box<dyn PhysicalExpr>,
    pub f: Box<BinaryFunc>
}


impl BinaryShift {
    pub fn new(left: Box<dyn PhysicalExpr>, op: Operator, right: Box<dyn PhysicalExpr>) -> Self {
        let f = get_binary_shift_func(op);
        Self {left, op, right, f}
    }
}

impl Debug for BinaryShift {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryShift")
            .field("left",  &self.left)
            .field("op", &self.op)
            .field("right",  &self.right)
            .finish()
    }
}

impl BinaryExpr for BinaryShift {
    fn left(&self) -> &dyn PhysicalExpr {
        self.left.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.right.as_ref()
    }

    fn null_safe_eval(&self, left_value: Value, right_value: Value) -> Value {
        (self.f)(left_value, right_value)
    }
}

impl PhysicalExpr for BinaryShift {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.left.data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

fn get_binary_shift_func(op: Operator) -> Box<BinaryFunc> {
    match op {
        Operator::BitShiftLeft => Box::new(binary_shift_left),
        Operator::BitShiftRight => Box::new(binary_shift_right),
        Operator::BitShiftRightUnsigned => Box::new(binary_shift_righ_unsigned),
        _ => panic!("unsupported operator {:?}", op),
    }
}

fn binary_shift_left(left: Value, right: Value) -> Value {
    let n = right.get_int();
    if n < 0 {
        return Value::Null;
    }
    match left {
        Value::Int(v) => Value::Int(v << n),
        Value::Long(v) => Value::Long(v << n),
        _ => Value::Null,
    }
}

fn binary_shift_right(left: Value, right: Value) -> Value {
    let n = right.get_int();
    if n < 0 {
        return Value::Null;
    }
    match left {
        Value::Int(v) => Value::Int(v >> n),
        Value::Long(v) => Value::Long(v >> n),
        _ => Value::Null,
    }
}

fn binary_shift_righ_unsigned(left: Value, right: Value) -> Value {
    let n = right.get_int();
    if n < 0 {
        return Value::Null;
    }
    match left {
        Value::Int(v) => Value::Int((v as u32 >> n) as i32),
        Value::Long(v) => Value::Long((v as u64 >> n) as i64),
        _ => Value::Null,
    }
}

#[derive(Debug)]
pub struct And {
    pub left: Box<dyn PhysicalExpr>,
    pub right: Box<dyn PhysicalExpr>,
}

impl And {
    pub fn new(left: Box<dyn PhysicalExpr>, right: Box<dyn PhysicalExpr>) -> Self {
        Self { left, right }
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

#[derive(Debug)]
pub struct Or {
    pub left: Box<dyn PhysicalExpr>,
    pub right: Box<dyn PhysicalExpr>,
}

impl Or {
    pub fn new(left: Box<dyn PhysicalExpr>, right: Box<dyn PhysicalExpr>) -> Self {
        Self { left, right }
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

