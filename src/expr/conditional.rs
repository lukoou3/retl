use std::any::Any;
use crate::Result;
use crate::expr::{Expr, ScalarFunction};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct If {
    pub predicate: Box<Expr>,
    pub true_value: Box<Expr>,
    pub false_value: Box<Expr>,
}

impl If {
    pub fn new(predicate: Box<Expr>, true_value: Box<Expr>, false_value: Box<Expr>) -> Self {
        Self { predicate, true_value, false_value, }
    }
}

impl ScalarFunction for If {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "If"
    }

    fn data_type(&self) -> &DataType {
        self.true_value.data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.predicate, &self.true_value, &self.false_value]
    }

    fn check_input_data_types(&self) -> Result<()> {
        if self.predicate.data_type() != DataType::boolean_type() {
            Err(format!("type of predicate expression in If should be boolean,, not {}", self.predicate.data_type()))
        } else if self.true_value.data_type() != self.false_value.data_type() {
            Err(format!("type of true_value and false_value expression in If should be same, not {} and {}", self.true_value.data_type(), self.false_value.data_type()))
        } else {
            Ok(())
        }
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let (Some(first), Some(second), Some(third)) = (iter.next(), iter.next(), iter.next()) {
            Box::new(Self::new(Box::new(first), Box::new(second), Box::new(third)))
        } else {
            panic!("args count not match")
        }
    }
}









