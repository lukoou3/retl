use crate::Result;
use crate::expr::{CreateScalarFunction, Expr, ScalarFunction, create_physical_expr};
use crate::physical_expr::{self as phy, PhysicalExpr};
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

impl CreateScalarFunction for GetArrayItem {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }

        let child = args[0].clone();
        let ordinal = args[1].clone();

        Ok(Box::new(GetArrayItem::new(
            Box::new(child),
            Box::new(ordinal),
        )))
    }
}

impl ScalarFunction for GetArrayItem {
    fn name(&self) -> &str {
        "GetArrayItem"
    }

    fn data_type(&self) -> &DataType {
        if let DataType::Array(data_type) = self.child.data_type() {
            data_type.as_ref()
        } else {
            panic!("GetArrayItem child must be array")
        }
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child, &self.ordinal]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![
            AbstractDataType::Any,
            AbstractDataType::Type(DataType::Int),
        ])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        Ok(Box::new(phy::GetArrayItem::new(
            create_physical_expr(&self.child)?,
            create_physical_expr(&self.ordinal)?,
            self.data_type().clone(),
        )))
    }
}
