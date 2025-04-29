use std::any::Any;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Clone, Debug)]
pub struct Literal {
    pub value: Value,
    pub data_type: DataType,
}

impl Literal {
    pub fn new(value: Value, data_type: DataType) -> Self {
        Self { value, data_type }
    }
}

impl PhysicalExpr for Literal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn eval(&self, _input: &dyn Row) -> Value {
        self.value.clone()
    }
}


