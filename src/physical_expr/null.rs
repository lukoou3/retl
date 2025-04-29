use std::any::Any;
use std::hash::Hash;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug)]
pub struct IsNull {
    pub child: Box<dyn PhysicalExpr>,
}

impl IsNull {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        IsNull { child }
    }
}

impl PhysicalExpr for IsNull {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        Value::Boolean(value.is_null())
    }
}

#[derive(Debug)]
pub struct IsNotNull {
    pub child: Box<dyn PhysicalExpr>,
}

impl IsNotNull {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        IsNotNull { child }
    }
}

impl PhysicalExpr for IsNotNull {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        Value::Boolean(value.is_null())
    }
}

#[derive(Debug)]
pub struct Coalesce {
    children: Vec<Box<dyn PhysicalExpr>>,
}

impl Coalesce {
    pub fn new(children: Vec<Box<dyn PhysicalExpr>>) -> Coalesce {
        Coalesce { children }
    }
}

impl PhysicalExpr for Coalesce {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.children[0].data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        for child in &self.children {
            let value = child.eval(input);
            if !value.is_null() {
                return value;
            }
        }
        Value::Null
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





