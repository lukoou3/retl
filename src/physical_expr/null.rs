use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct IsNull {
    pub child: Arc<dyn PhysicalExpr>,
}

impl IsNull {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        IsNull { child }
    }
}

impl PartialEq for IsNull {
    fn eq(&self, other: &IsNull) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for IsNull{}

impl Hash for IsNull {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
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

#[derive(Debug, Clone)]
pub struct IsNotNull {
    pub child: Arc<dyn PhysicalExpr>,
}

impl IsNotNull {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        IsNotNull { child }
    }
}

impl PartialEq for IsNotNull {
    fn eq(&self, other: &IsNotNull) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for IsNotNull{}

impl Hash for IsNotNull {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
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

#[derive(Debug, Clone)]
pub struct In {
    pub value: Arc<dyn PhysicalExpr>,
    pub list: Vec<Arc<dyn PhysicalExpr>>,
}

impl In {
    pub fn new(value: Arc<dyn PhysicalExpr>, list: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        In { value, list }
    }
}

impl PartialEq for In {
    fn eq(&self, other: &In) -> bool {
        self.value.eq(&other.value) && self.list.eq(&other.list)
    }
}

impl Eq for In{}

impl Hash for In {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.list.hash(state);
    }
}

impl PhysicalExpr for In {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.value.eval(input);
        if value.is_null() {
            return Value::Null;
        }

        let mut  has_null = false;
        for e in &self.list {
            let v = e.eval(input);
            if v.is_null() {
                has_null = true;
            } else if v == value {
                return Value::Boolean(true);
            }
        }

        if has_null {
            Value::Null
        } else {
            Value::Boolean(false)
        }
    }
}





