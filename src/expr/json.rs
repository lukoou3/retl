use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::{AbstractDataType, DataType};

#[derive(Debug, Clone)]
pub struct GetJsonObject {
    pub json: Box<Expr>,
    pub path: Box<Expr>,
}

impl GetJsonObject {
    pub fn new(json: Box<Expr>, path: Box<Expr>) -> Self {
        Self{json, path}
    }
}

impl CreateScalarFunction for GetJsonObject {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let json = iter.next().unwrap();
        let path = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(json), Box::new(path))))
    }
}

impl ScalarFunction for GetJsonObject {
    fn name(&self) -> &str {
        "get_json_object"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.json, &self.path]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::string_type(), AbstractDataType::string_type()])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{json, path} = self;
        Ok(Box::new(phy::GetJsonObject::new(create_physical_expr(json)?, create_physical_expr(path)?)))
    }
}

