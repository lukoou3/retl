use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::{parser, Result};
use crate::analysis::Analyzer;
use crate::config::{TransformConfig, TransformProvider};
use crate::expr::{AttributeReference, BoundReference, Expr};
use crate::logical_plan::{LogicalPlan, Project, RelationPlaceholder};
use crate::physical_expr::{create_physical_expr, PhysicalExpr};
use crate::transform::{QueryTransform, Transform};
use crate::tree_node::{Transformed, TreeNode};
use crate::types::{DataType, Schema};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTransformConfig {
    sql: String,
}

#[typetag::serde(name = "query")]
impl TransformConfig for QueryTransformConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn TransformProvider>> {
        let mut temp_views = HashMap::new();
        temp_views.insert("tbl".to_string(), RelationPlaceholder::new("tbl".to_string(), schema.to_attributes()));
        let plan = parser::parse_query(&self.sql)?;
        let plan = Analyzer::new(temp_views).analyze(plan)?;
        println!("{:?}", plan);
        Ok(Box::new(PrintSinkProvider::new(plan)))
    }

}

#[derive(Debug, Clone)]
pub struct PrintSinkProvider {
    schema: Schema,
    plan: LogicalPlan,
}

impl PrintSinkProvider {
    pub fn new(plan: LogicalPlan) -> Self {
        let schema = Schema::from_attributes(plan.output());
        Self{schema, plan}
    }
}

impl TransformProvider for PrintSinkProvider {
    fn create_transform(&self) -> Result<Box<dyn Transform>> {
        let exprs = match self.plan.clone() {
            LogicalPlan::Project(Project{project_list, child}) => {
                let input = child.output();
                bind_references(project_list, input)
            },
            _ => return Err(format!("not support plan: {:?}", self.plan))
        }?;
        let exprs: Result<Vec<Arc<dyn PhysicalExpr>>, String> = exprs.iter()
            .map(|expr| create_physical_expr(expr)).collect();
        Ok(Box::new(QueryTransform::new(self.schema.clone(), exprs?)))
    }
}

fn bind_references(exprs: Vec<Expr>, input: Vec<AttributeReference>) -> Result<Vec<Expr>> {
    let expr_id_to_ordinal: HashMap<u32, usize> = input.iter().enumerate().map(|(i, x)| (x.expr_id, i)).collect();
    let mut  new_exprs = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let e = expr.transform_up(|expr| {
            if let Expr::AttributeReference(AttributeReference{data_type, expr_id, ..}) = &expr {
                if let Some(ordinal) = expr_id_to_ordinal.get(expr_id){
                    return Ok(Transformed::yes(Expr::BoundReference(BoundReference::new(*ordinal, data_type.clone()))));
                } else { return Err(format!("not found {:?} in {:?}", expr, input)) }
            } else {
                Ok(Transformed::no(expr))
            }
        })?.data;
        new_exprs.push(e);
    }
    Ok(new_exprs)
}