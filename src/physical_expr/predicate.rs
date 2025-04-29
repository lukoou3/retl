use std::any::Any;
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