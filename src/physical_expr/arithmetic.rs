use std::any::Any;
use std::hash::Hash;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug)]
pub struct UnaryMinus {
    pub child: Box<dyn PhysicalExpr>,
}

impl UnaryMinus {
    pub fn new(child: Box<dyn PhysicalExpr>) -> UnaryMinus {
        UnaryMinus { child }
    }
}

impl PhysicalExpr for UnaryMinus {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.child.data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        match value {
            Value::Int(v) => Value::Int(-v),
            Value::Long(v) => Value::Long(-v),
            Value::Float(v) => Value::Float(-v),
            Value::Double(v) => Value::Double(-v),
            _ => Value::Null
        }
    }
}

#[derive(Debug)]
pub struct BitwiseNot {
    pub child: Box<dyn PhysicalExpr>,
}

impl BitwiseNot {
    pub fn new(child: Box<dyn PhysicalExpr>) -> BitwiseNot {
        BitwiseNot { child }
    }
}

impl PhysicalExpr for BitwiseNot {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.child.data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        match value {
            Value::Int(v) => Value::Int(!v),
            Value::Long(v) => Value::Long(!v),
            _ => Value::Null
        }
    }
}

#[derive(Debug)]
pub struct Least {
    children: Vec<Box<dyn PhysicalExpr>>
}

impl Least {
    pub fn new(children: Vec<Box<dyn PhysicalExpr>>) -> Least {
        Least { children }
    }
}

impl PhysicalExpr for Least {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.children[0].data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        self.children.iter().fold(Value::Null, |r, c| {
            let v = c.eval(input);
            if v.is_null() {
                r
            } else {
                if r.is_null() || v < r {
                    v
                } else {
                    r
                }
            }
        })
    }
}

#[derive(Debug)]
pub struct Greatest {
    children: Vec<Box<dyn PhysicalExpr>>
}

impl Greatest {
    pub fn new(children: Vec<Box<dyn PhysicalExpr>>) -> Greatest {
        Greatest { children }
    }
}

impl PhysicalExpr for Greatest {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.children[0].data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        self.children.iter().fold(Value::Null, |r, c| {
            let v = c.eval(input);
            if v.is_null() {
                r
            } else {
                if r.is_null() || v > r {
                    v
                } else {
                    r
                }
            }
        })
    }
}

