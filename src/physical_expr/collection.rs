use std::any::Any;
use std::hash::Hash;
use crate::data::{Row, Value};
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug)]
pub struct Concat {
    children: Vec<Box<dyn PhysicalExpr>>,
}

impl Concat {
    pub fn new(children: Vec<Box<dyn PhysicalExpr>>) -> Concat {
        Concat { children }
    }
}

impl PhysicalExpr for Concat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let args = self
            .children
            .iter()
            .map(|child| child.eval(input))
            .collect::<Vec<_>>();
        if args.iter().any(|arg| arg.is_null()) {
            return Value::Null;
        }
        let string = args.iter()
            .map(|arg| arg.get_string())
            .collect::<Vec<_>>().concat();
        Value::string(string)
    }
}