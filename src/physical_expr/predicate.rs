use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct Not {
    pub child: Arc<dyn PhysicalExpr>,
}

impl Not {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Not { child }
    }
}

impl PartialEq for Not {
    fn eq(&self, other: &Not) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for Not{}

impl Hash for Not {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
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