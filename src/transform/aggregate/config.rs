use serde::{Deserialize, Serialize};
use crate::{sql_utils, Result};
use crate::config::{TaskContext, TransformConfig, TransformProvider};
use crate::data::{Row};
use crate::expr::{AttributeReference, Expr};
use crate::logical_plan::LogicalPlan;
use crate::transform::{Transform, OutOperator, ProcessOperator, get_process_operator_chain};
use crate::transform::aggregate::TaskAggregateTransform;
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAggregateTransformConfig {
    sql: String,
    #[serde(default = "default_max_rows")]
    max_rows: usize,
    #[serde(default = "default_interval_ms")]
    interval_ms: u64,
}

fn default_max_rows() -> usize {
    3000000
}

fn default_interval_ms() -> u64 {
    5000
}

#[typetag::serde(name = "task_aggregate")]
impl TransformConfig for TaskAggregateTransformConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn TransformProvider>> {
        let plan = sql_utils::sql_plan(&self.sql, &schema)?;
        if let LogicalPlan::Aggregate(agg) = &plan {
            let schema = Schema::from_attributes(plan.output());
            let (group_exprs, agg_exprs, result_exprs, child) = agg.extract_exprs();
            let child = child.as_ref().clone();
            let input_attrs = child.output();
            Ok(Box::new(TaskAggregateTransformProvider {
                schema,
                input_attrs,
                child,
                group_exprs,
                agg_exprs,
                result_exprs,
                max_rows: self.max_rows,
                interval_ms: self.interval_ms,
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
    child: LogicalPlan,
    group_exprs: Vec<Expr>,
    agg_exprs: Vec<Expr>,
    result_exprs: Vec<Expr>,
    max_rows: usize,
    interval_ms: u64,
}

impl TransformProvider for TaskAggregateTransformProvider {
    fn create_transform(&self, task_context: TaskContext) -> Result<Box<dyn Transform>> {
        let (no_pre, pre_process) = if let LogicalPlan::RelationPlaceholder(_) = &self.child {
            (true, Box::new(OutOperator) as Box<dyn ProcessOperator>)
        } else {
            let process_operator = get_process_operator_chain(self.child.clone())?;
            (false, process_operator)
        };
        let input_attrs = self.input_attrs.clone();
        let group_exprs = self.group_exprs.clone();
        let agg_exprs = self.agg_exprs.clone();
        let result_exprs = self.result_exprs.clone();
        let transform= TaskAggregateTransform::new(task_context, self.schema.clone(), no_pre, pre_process, agg_exprs, group_exprs, result_exprs, input_attrs, self.max_rows, self.interval_ms)?;
        Ok(Box::new(transform))
    }
}

