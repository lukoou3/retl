use std::sync::Arc;
use crate::config::TaskContext;
use crate::data::Row;
use crate::execution::{Collector, TimeService};
use crate::physical_expr::PhysicalExpr;
use crate::transform::Transform;
use crate::types::Schema;

#[derive(Debug)]
pub struct FilterTransform {
    task_context: TaskContext,
    schema: Schema,
    predicate: Box<dyn PhysicalExpr>,
}

impl FilterTransform {
    pub fn new(task_context: TaskContext, schema: Schema, predicate: Box<dyn PhysicalExpr>) -> Self {
        Self {task_context, schema, predicate}
    }
}

impl Transform for FilterTransform {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector, time_service: &mut TimeService) -> crate::Result<()> {
        let value = self.predicate.eval(row);
        if !value.is_null() && value.get_boolean() {
            out.collect(row)?;
        }
        Ok(())
    }
}
