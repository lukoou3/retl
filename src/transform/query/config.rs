use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::Result;
use crate::config::{TransformConfig, TransformProvider};
use crate::expr::Expr;
use crate::physical_expr::{create_physical_expr, PhysicalExpr};
use crate::transform::{QueryTransform, Transform};
use crate::types::{DataType, Schema};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTransformConfig {
    sql: String,
}

#[typetag::serde(name = "query")]
impl TransformConfig for QueryTransformConfig {
    fn build(&self, schema: Schema) -> crate::Result<Box<dyn TransformProvider>> {
        let exprs = vec![
            Expr::col(0, DataType::Int),
            Expr::col(1, DataType::String),
            Expr::col(2, DataType::String),
            Expr::col(3, DataType::Long),
            Expr::col(4, DataType::Long),
            Expr::col(3, DataType::Long) + Expr::col(4, DataType::Long),
        ];
        Ok(Box::new(PrintSinkProvider::new(exprs)))
    }

}

#[derive(Debug, Clone)]
pub struct PrintSinkProvider {
    exprs: Vec<Expr>,
}

impl PrintSinkProvider {
    pub fn new(exprs: Vec<Expr>) -> Self {
        Self {exprs}
    }
}

impl TransformProvider for PrintSinkProvider {
    fn create_transform(&self) -> Result<Box<dyn Transform>> {
        let exprs: Result<Vec<Arc<dyn PhysicalExpr>>, String> = self.exprs.iter()
            .map(|expr| create_physical_expr(expr)).collect();
        Ok(Box::new(QueryTransform::new(exprs?)))
    }
}