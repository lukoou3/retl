use std::any::Any;
use std::sync::Arc;
use std::hash::Hash;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct GetArrayItem {
    child: Arc<dyn PhysicalExpr>,
    ordinal: Arc<dyn PhysicalExpr>,
    ele_type: DataType,
}

impl GetArrayItem {
    pub fn new(child: Arc<dyn PhysicalExpr>, ordinal: Arc<dyn PhysicalExpr>, ele_type: DataType) -> GetArrayItem {
        GetArrayItem { child, ordinal, ele_type }
    }
}

impl PartialEq for GetArrayItem {
    fn eq(&self, other: &GetArrayItem) -> bool {
        self.child.eq(&other.child) && self.ordinal.eq(&other.ordinal)
    }
}

impl Eq for GetArrayItem {}

impl Hash for GetArrayItem {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
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
        let child = self.child.eval(input);
        if child.is_null() {
            return Value::Null;
        }
        let ordinal = self.ordinal.eval(input);
        if ordinal.is_null() {
            return Value::Null;
        }
        let array = child.get_array();
        let index = ordinal.get_int();
        if index >= array.len() as i32 || index < 0 {
            return Value::Null;
        }
        let value = array[index as usize].clone();
        value
    }
}
