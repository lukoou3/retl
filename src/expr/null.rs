use std::sync::Arc;
use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::DataType;

pub struct Nvl;

impl CreateScalarFunction for Nvl {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }
        Ok(Box::new(Coalesce::new(args)))
    }
}

#[derive(Debug, Clone)]
pub struct Coalesce {
    pub children: Vec<Expr>,
}

impl Coalesce {
    pub fn new(children: Vec<Expr>) -> Coalesce {
        Coalesce { children }
    }
}

impl CreateScalarFunction for Coalesce {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.is_empty() {
            return Err(format!("requires at least 1 argument, found:{}", args.len()));
        }
        Ok(Box::new(Self::new(args)))
    }
}

impl ScalarFunction for Coalesce {

    fn name(&self) -> &str {
        "coalesce"
    }

    fn data_type(&self) -> &DataType {
        self.children[0].data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        self.children.iter().collect()
    }

    fn check_input_data_types(&self) -> Result<()> {
        if self.children.is_empty() {
            Err("Coalesce requires at least one argument".to_string())
        } else if self.children.iter().all(|child| child.data_type() == self.children[0].data_type()) {
            Ok(())
        } else {
            Err(format!("Coalesce requires all arguments to have the same type: {:?}", self.children))
        }
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{children} = self;
        let args = children.into_iter().map(|child| create_physical_expr(child)).collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(phy::Coalesce::new(args)))
    }
}

