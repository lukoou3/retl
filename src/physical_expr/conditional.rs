use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct If {
    predicate: Arc<dyn PhysicalExpr>,
    true_value: Arc<dyn PhysicalExpr>,
    false_value: Arc<dyn PhysicalExpr>,
}

impl If {
    pub fn new( predicate: Arc<dyn PhysicalExpr>, true_value: Arc<dyn PhysicalExpr>, false_value: Arc<dyn PhysicalExpr>, ) -> Self {
        Self { predicate, true_value, false_value, }
    }
}

impl PartialEq for If{
    fn eq(&self, other: &If) -> bool {
        self.predicate.eq(&other.predicate)
            && self.true_value.eq(&other.true_value)
            && self.false_value.eq(&other.false_value)
    }
}

impl Eq for If{}

impl Hash for If{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.true_value.hash(state);
        self.false_value.hash(state);
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

#[derive(Debug, Clone)]
pub struct CaseWhen {
    branches: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    else_value: Arc<dyn PhysicalExpr>,
}

impl CaseWhen {
    pub fn new(branches: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>, else_value: Arc<dyn PhysicalExpr>) -> Self {
        Self { branches, else_value, }
    }
}

impl PartialEq for CaseWhen{
    fn eq(&self, other: &CaseWhen) -> bool {
        self.branches.eq(&other.branches)
            && self.else_value.eq(&other.else_value)
    }
}

impl Eq for CaseWhen{}

impl Hash for CaseWhen{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.branches.hash(state);
        self.else_value.hash(state);
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



