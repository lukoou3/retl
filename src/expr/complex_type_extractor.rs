use std::any::Any;
use crate::expr::{Expr, ScalarFunction};
use crate::types::{AbstractDataType, DataType};

#[derive(Debug, Clone)]
pub struct GetArrayItem {
    pub child: Box<Expr>,
    pub ordinal: Box<Expr>,
}

impl GetArrayItem {
    pub fn new(child: Box<Expr>, ordinal: Box<Expr>) -> GetArrayItem {
        GetArrayItem { child, ordinal }
    }
}

impl ScalarFunction for GetArrayItem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "GetArrayItem"
    }

    fn data_type(&self) -> &DataType {
        if let DataType::Array( data_type) = self.child.data_type() {
            data_type.as_ref()
        } else {
            panic!("GetArrayItem child must be array")
        }
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child, &self.ordinal]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Any, AbstractDataType::Type(DataType::Int)])
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let (Some(first), Some(second)) = (iter.next(), iter.next()) {
            Box::new(GetArrayItem::new(Box::new(first), Box::new(second)))
        } else {
            panic!("args count not match")
        }
    }
}




