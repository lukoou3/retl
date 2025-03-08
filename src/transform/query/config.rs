use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::{parser, Result};
use crate::analysis::Analyzer;
use crate::config::{TaskContext, TransformConfig, TransformProvider};
use crate::logical_plan::{LogicalPlan, RelationPlaceholder};
use crate::transform::{get_process_operator_chain, QueryTransform, Transform};
use crate::tree_node::{TreeNode};
use crate::types::{Schema};

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
        // println!("{:?}", plan);
        Ok(Box::new(QueryTransformProvider::new(plan)))
    }

}

#[derive(Debug, Clone)]
pub struct QueryTransformProvider {
    schema: Schema,
    plan: LogicalPlan,
}

impl QueryTransformProvider {
    pub fn new(plan: LogicalPlan) -> Self {
        let schema = Schema::from_attributes(plan.output());
        Self{schema, plan}
    }
}

impl TransformProvider for QueryTransformProvider {
    fn create_transform(&self, task_context: TaskContext) -> Result<Box<dyn Transform>> {
        let process_operator = get_process_operator_chain(self.plan.clone())?;
        Ok(Box::new(QueryTransform::new(task_context, self.schema.clone(), process_operator)))
    }
}

