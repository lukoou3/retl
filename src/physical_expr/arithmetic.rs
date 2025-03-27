use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct UnaryMinus {
    pub child: Arc<dyn PhysicalExpr>,
}

impl UnaryMinus {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> UnaryMinus {
        UnaryMinus { child }
    }
}

impl PartialEq for UnaryMinus {
    fn eq(&self, other: &UnaryMinus) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for UnaryMinus {}

impl Hash for UnaryMinus {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
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
        if value.is_null() {
            return Value::Null;
        }
        match value {
            Value::Int(v) => Value::Int(-v),
            Value::Long(v) => Value::Long(-v),
            Value::Float(v) => Value::Float(-v),
            Value::Double(v) => Value::Double(-v),
            _ => Value::Null
        }
    }
}
