use std::sync::Arc;
use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::types::DataType;
use crate::physical_expr::{self as phy, PhysicalExpr};

#[derive(Debug, Clone)]
pub struct Concat {
    pub children: Vec<Expr>,
}

impl Concat {
    pub fn new(children: Vec<Expr>) -> Concat {
        Concat { children }
    }
}

impl CreateScalarFunction for Concat {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        Ok(Box::new(Concat::new(args)))
    }
}

impl ScalarFunction for Concat {

    fn name(&self) -> &str {
        "Concat"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        self.children.iter().collect()
    }

    fn check_input_data_types(&self) -> Result<()> {
        if !self.children.iter().all(|child| child.data_type() == DataType::string_type()) {
            Err("Concat requires string type".to_string())
        } else {
            Ok(())
        }
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{children} = self;
        let args = children.into_iter().map(|child| create_physical_expr(child)).collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(phy::Concat::new(args)))
    }
}