use std::any::Any;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Clone, Debug)]
pub struct BoundReference {
    pub ordinal: usize,
    pub data_type: DataType,
}

impl BoundReference {
    pub fn new(ordinal: usize, data_type: DataType) -> Self {
        Self { ordinal, data_type }
    }
}

impl PhysicalExpr for BoundReference {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        input.get(self.ordinal).clone()
    }

}

