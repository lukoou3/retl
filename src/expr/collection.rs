use std::any::Any;
use crate::expr::{Expr, ScalarFunction};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct Concat {
    pub children: Vec<Expr>,
}

impl Concat {
    pub fn new(children: Vec<Expr>) -> Concat {
        Concat { children }
    }
}

impl ScalarFunction for Concat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Concat"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        self.children.iter().collect()
    }

    fn check_input_data_types(&self) -> crate::Result<()> {
        if !self.children.iter().all(|child| child.data_type() == DataType::string_type()) {
            Err("Concat requires string type".to_string())
        } else {
            Ok(())
        }
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        Box::new(Concat::new(args))
    }
}