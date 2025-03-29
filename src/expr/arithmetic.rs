use crate::Result;
use crate::expr::{CreateScalarFunction, Expr, ScalarFunction, create_physical_expr};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::{AbstractDataType, DataType};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct UnaryMinus {
    pub child: Box<Expr>,
}

impl UnaryMinus {
    pub fn new(child: Box<Expr>) -> UnaryMinus {
        UnaryMinus { child }
    }
}

impl CreateScalarFunction for UnaryMinus {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        Ok(Box::new(UnaryMinus::new(Box::new(args[0].clone()))))
    }
}

impl ScalarFunction for UnaryMinus {

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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(phy::UnaryMinus::new(create_physical_expr(&self.child)?)))
    }
}
