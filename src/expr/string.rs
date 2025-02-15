use std::any::Any;
use crate::expr::{Expr, ScalarFunction};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct Length {
    pub child: Box<Expr>,
}

impl Length {
    pub fn new(child: Box<Expr>) -> Length {
        Length { child }
    }
}

impl ScalarFunction for Length {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Length"
    }

    fn data_type(&self) -> DataType {
        DataType::Int
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let Some(child) = iter.next() {
            Box::new(Length::new(Box::new(child)))
        } else {
            panic!("args count not match")
        }
    }
}

#[derive(Debug, Clone)]
pub struct Substring {
    pub str: Box<Expr>,
    pub pos: Box<Expr>,
    pub len: Box<Expr>,
}

impl Substring {
    pub fn new(str: Box<Expr>, pos: Box<Expr>, len: Box<Expr>) -> Substring {
        Substring{str, pos, len}
    }
}

impl ScalarFunction for Substring {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Substring"
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.str, &self.pos, &self.len]
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let (Some(first), Some(second), Some(third)) = (iter.next(), iter.next(), iter.next()) {
            Box::new(Substring::new(Box::new(first), Box::new(second), Box::new(third)))
        } else {
            panic!("args count not match")
        }
    }
}
