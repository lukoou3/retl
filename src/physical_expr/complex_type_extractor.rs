use std::any::Any;
use std::hash::Hash;
use crate::data::{Row, Value};
use crate::physical_expr::{BinaryExpr, PhysicalExpr};
use crate::types::DataType;

#[derive(Debug)]
pub struct GetArrayItem {
    child: Box<dyn PhysicalExpr>,
    ordinal: Box<dyn PhysicalExpr>,
    ele_type: DataType,
}

impl GetArrayItem {
    pub fn new(child: Box<dyn PhysicalExpr>, ordinal: Box<dyn PhysicalExpr>, ele_type: DataType) -> GetArrayItem {
        GetArrayItem { child, ordinal, ele_type }
    }
}

impl BinaryExpr for GetArrayItem {
    fn left(&self) -> &dyn PhysicalExpr {
        self.child.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.ordinal.as_ref()
    }

    fn null_safe_eval(&self, child: Value, ordinal: Value) -> Value {
        let array = child.get_array();
        let index = ordinal.get_int();
        if index >= array.len() as i32 || index < 0 {
            return Value::Null;
        }
        let value = array[index as usize].clone();
        value
    }
}

impl PhysicalExpr for GetArrayItem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.ele_type.clone()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}
