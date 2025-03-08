use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::analysis::Analyzer;
use crate::config::{TaskContext, TransformConfig, TransformProvider};
use crate::expr::BoundReference;
use crate::logical_plan::{Filter, LogicalPlan, RelationPlaceholder};
use crate::parser;
use crate::physical_expr::create_physical_expr;
use crate::transform::{FilterTransform, Transform};
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterTransformConfig {
    condition: String,
}

#[typetag::serde(name = "filter")]
impl TransformConfig for FilterTransformConfig {
    fn build(&self, schema: Schema) -> crate::Result<Box<dyn TransformProvider>> {
        let expr = parser::parse_expr(&self.condition)?;
        let plan = LogicalPlan::Filter(Filter::new(expr, Arc::new(LogicalPlan::RelationPlaceholder(RelationPlaceholder::new("tbl".to_string(), schema.to_attributes())))));
        let plan = Analyzer::new(HashMap::new()).analyze(plan)?;
        if let LogicalPlan::Filter(filter) = plan {
            Ok(Box::new(FilterTransformProvider{schema, filter}))
        } else {
            Err("Invalid filter plan".into())
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterTransformProvider {
    schema: Schema,
    filter: Filter,
}

impl TransformProvider for FilterTransformProvider {
    fn create_transform(&self, task_context: TaskContext) -> crate::Result<Box<dyn Transform>> {
        let filter = self.filter.clone();
        let condition = filter.condition;
        let child = filter.child;
        let predicate = BoundReference::bind_reference(condition.clone(), child.output())?;
        let predicate = create_physical_expr(&predicate)?;
        Ok(Box::new(FilterTransform::new(task_context, self.schema.clone(), predicate)))
    }
}