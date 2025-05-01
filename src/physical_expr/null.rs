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





