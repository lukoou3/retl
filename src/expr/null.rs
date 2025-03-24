use std::any::Any;
use crate::expr::{Expr, ScalarFunction};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct Coalesce {
    pub children: Vec<Expr>,
}

impl Coalesce {
    pub fn new(children: Vec<Expr>) -> Coalesce {
        Coalesce { children }
    }
}

impl ScalarFunction for Coalesce {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Coalesce"
    }

    fn data_type(&self) -> &DataType {
        self.children[0].data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        self.children.iter().collect()
    }

    fn check_input_data_types(&self) -> crate::Result<()> {
        if self.children.is_empty() {
            Err("Coalesce requires at least one argument".to_string())
        } else if self.children.iter().all(|child| child.data_type() == self.children[0].data_type()) {
            Ok(())
        } else {
            Err(format!("Coalesce requires all arguments to have the same type: {:?}", self.children))
        }
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        Box::new(Coalesce::new(args))
    }
}

