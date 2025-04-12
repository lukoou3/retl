use serde::{Deserialize, Serialize};
use crate::{sql_utils, Result};
use crate::config::{TaskContext, TransformConfig, TransformProvider};
use crate::data::{Row};
use crate::expr::{AttributeReference, Expr};
use crate::logical_plan::LogicalPlan;
use crate::transform::{TaskAggregateTransform, Transform};
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAggregateTransformConfig {
    sql: String,
}

#[typetag::serde(name = "task_aggregate")]
impl TransformConfig for TaskAggregateTransformConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn TransformProvider>> {
        let plan = sql_utils::sql_plan(&self.sql, &schema)?;
        if let LogicalPlan::Aggregate(agg) = &plan {
            let schema = Schema::from_attributes(plan.output());
            let (group_exprs, agg_exprs, result_exprs, child) = agg.extract_exprs();
            let input_attrs = child.output();
            Ok(Box::new(TaskAggregateTransformProvider {
                schema,
                input_attrs,
                group_exprs,
                agg_exprs,
                result_exprs,
            }))
        } else {
            Err(format!("plan is not aggregate plan:{:?}", plan))
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskAggregateTransformProvider {
    schema: Schema,
    input_attrs: Vec<AttributeReference>,
    group_exprs: Vec<Expr>,
    agg_exprs: Vec<Expr>,
    result_exprs: Vec<Expr>,
}

impl TransformProvider for TaskAggregateTransformProvider {
    fn create_transform(&self, task_context: TaskContext) -> Result<Box<dyn Transform>> {
        let input_attrs = self.input_attrs.clone();
        let group_exprs = self.group_exprs.clone();
        let agg_exprs = self.agg_exprs.clone();
        let result_exprs = self.result_exprs.clone();
        let transform= TaskAggregateTransform::new(task_context, self.schema.clone(), agg_exprs, group_exprs, result_exprs, input_attrs)?;
        Ok(Box::new(transform))
    }
}

