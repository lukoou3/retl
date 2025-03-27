use std::any::Any;
use crate::expr::{Expr, ScalarFunction};
use crate::types::{AbstractDataType, DataType};

#[derive(Debug, Clone)]
pub struct UnaryMinus {
    pub child: Box<Expr>,
}

impl UnaryMinus {
    pub fn new(child: Box<Expr>) -> UnaryMinus {
        UnaryMinus { child }
    }
}

impl ScalarFunction for UnaryMinus {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "UnaryMinus"
    }

    fn data_type(&self) -> &DataType {
        self.child.data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Numeric])
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        Box::new(UnaryMinus::new(Box::new(args[0].clone())))
    }
}
