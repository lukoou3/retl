use std::any::Any;
use std::hash::Hash;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug)]
pub struct If {
    predicate: Box<dyn PhysicalExpr>,
    true_value: Box<dyn PhysicalExpr>,
    false_value: Box<dyn PhysicalExpr>,
}

impl If {
    pub fn new( predicate: Box<dyn PhysicalExpr>, true_value: Box<dyn PhysicalExpr>, false_value: Box<dyn PhysicalExpr>, ) -> Self {
        Self { predicate, true_value, false_value, }
    }
}

impl PhysicalExpr for If {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.true_value.data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        match self.predicate.eval(input) {
            Value::Boolean(true) => self.true_value.eval(input),
            _ => self.false_value.eval(input),
        }
    }
}

#[derive(Debug)]
pub struct CaseWhen {
    branches: Vec<(Box<dyn PhysicalExpr>, Box<dyn PhysicalExpr>)>,
    else_value: Box<dyn PhysicalExpr>,
}

impl CaseWhen {
    pub fn new(branches: Vec<(Box<dyn PhysicalExpr>, Box<dyn PhysicalExpr>)>, else_value: Box<dyn PhysicalExpr>) -> Self {
        Self { branches, else_value, }
    }
}

impl PhysicalExpr for CaseWhen {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.else_value.data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        for (when, then) in &self.branches {
            if when.eval(input).is_true() {
                return then.eval(input);
            }
        }
        self.else_value.eval(input)
    }
}



