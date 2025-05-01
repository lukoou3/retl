use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug)]
pub struct Not {
    pub child: Box<dyn PhysicalExpr>,
}

impl Not {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Not { child }
    }
}

impl PhysicalExpr for Not {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        if value.is_null() {
            return Value::Null;
        }
        Value::Boolean(!value.get_boolean())
    }
}

#[derive(Debug)]
pub struct In {
    pub value: Box<dyn PhysicalExpr>,
    pub list: Vec<Box<dyn PhysicalExpr>>,
}

impl In {
    pub fn new(value: Box<dyn PhysicalExpr>, list: Vec<Box<dyn PhysicalExpr>>) -> Self {
        In { value, list }
    }
}

impl PhysicalExpr for In {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.value.eval(input);
        if value.is_null() {
            return Value::Null;
        }

        let mut  has_null = false;
        for e in &self.list {
            let v = e.eval(input);
            if v.is_null() {
                has_null = true;
            } else if v == value {
                return Value::Boolean(true);
            }
        }

        if has_null {
            Value::Null
        } else {
            Value::Boolean(false)
        }
    }
}

#[derive(Debug)]
pub struct InSet {
    pub value: Box<dyn PhysicalExpr>,
    pub hset: HashSet<Value>,
    pub has_null: bool,
}

impl InSet {
    pub fn new(value: Box<dyn PhysicalExpr>, hset: HashSet<Value>) -> Self {
        let has_null = hset.contains(&Value::Null);
        InSet { value, hset, has_null }
    }
}

impl PhysicalExpr for InSet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.value.eval(input);
        if value.is_null() {
            return Value::Null;
        }
        if self.hset.contains(&value) {
            Value::Boolean(true)
        } else if self.has_null {
            Value::Null
        } else {
            Value::Boolean(false)
        }
    }
}
