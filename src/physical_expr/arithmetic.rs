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

#[derive(Debug, Clone)]
pub struct BitwiseNot {
    pub child: Arc<dyn PhysicalExpr>,
}

impl BitwiseNot {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> BitwiseNot {
        BitwiseNot { child }
    }
}

impl PartialEq for BitwiseNot {
    fn eq(&self, other: &BitwiseNot) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for BitwiseNot {}

impl Hash for BitwiseNot {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PhysicalExpr for BitwiseNot {
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
            Value::Int(v) => Value::Int(!v),
            Value::Long(v) => Value::Long(!v),
            _ => Value::Null
        }
    }
}

#[derive(Debug, Clone)]
pub struct Least {
    children: Vec<Arc<dyn PhysicalExpr>>
}

impl Least {
    pub fn new(children: Vec<Arc<dyn PhysicalExpr>>) -> Least {
        Least { children }
    }
}

impl PartialEq for Least{
    fn eq(&self, other: &Least) -> bool {
        self.children.eq(&other.children)
    }
}

impl Eq for Least{}

impl Hash for Least{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.children.hash(state);
    }
}

impl PhysicalExpr for Least {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.children[0].data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        self.children.iter().fold(Value::Null, |r, c| {
            let v = c.eval(input);
            if v.is_null() {
                r
            } else {
                if r.is_null() || v < r {
                    v
                } else {
                    r
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct Greatest {
    children: Vec<Arc<dyn PhysicalExpr>>
}

impl Greatest {
    pub fn new(children: Vec<Arc<dyn PhysicalExpr>>) -> Greatest {
        Greatest { children }
    }
}

impl PartialEq for Greatest{
    fn eq(&self, other: &Greatest) -> bool {
        self.children.eq(&other.children)
    }
}

impl Eq for Greatest{}

impl Hash for Greatest{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.children.hash(state);
    }
}

impl PhysicalExpr for Greatest {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.children[0].data_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        self.children.iter().fold(Value::Null, |r, c| {
            let v = c.eval(input);
            if v.is_null() {
                r
            } else {
                if r.is_null() || v > r {
                    v
                } else {
                    r
                }
            }
        })
    }
}

