use serde::{Deserialize, Serialize};
use crate::Result;
use crate::config::{TaskContext, TransformConfig, TransformProvider};
use crate::logical_plan::LogicalPlan;
use crate::sql_utils;
use crate::transform::{get_process_operator_chain, Transform};
use crate::transform::query::QueryTransform;
use crate::tree_node::{TreeNode};
use crate::types::{Schema};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTransformConfig {
    sql: String,
}

#[typetag::serde(name = "query")]
impl TransformConfig for QueryTransformConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn TransformProvider>> {
        let optimized_plan = sql_utils::sql_plan(&self.sql, &schema)?;
        Ok(Box::new(QueryTransformProvider::new(optimized_plan)))
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

