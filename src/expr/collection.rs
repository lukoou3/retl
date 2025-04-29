use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::types::{AbstractDataType, DataType};
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
        "concat"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        self.children.iter().collect()
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        let mut types = Vec::with_capacity(self.children.len());
        types.resize(self.children.len(), AbstractDataType::string_type());
        Some(types)
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{children} = self;
        let args = children.into_iter().map(|child| create_physical_expr(child)).collect::<Result<Vec<_>>>()?;
        Ok(Box::new(phy::Concat::new(args)))
    }
}