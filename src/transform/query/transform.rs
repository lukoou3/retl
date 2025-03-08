use crate::config::TaskContext;
use crate::Result;
use crate::data::{Row};
use crate::execution::Collector;
use crate::transform::{ProcessOperator, Transform};
use crate::types::Schema;

#[derive(Debug)]
pub struct QueryTransform {
    task_context: TaskContext,
    schema: Schema,
    process_operator: Box<dyn ProcessOperator>,
}

impl QueryTransform {
    pub fn new(task_context: TaskContext, schema: Schema, process_operator: Box<dyn ProcessOperator>) -> Self {
        Self {task_context, schema, process_operator}
    }
}

impl Transform for QueryTransform {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector) -> Result<()> {
        self.task_context.base_iometrics.num_records_in_inc_by(1);
        let rows = self.process_operator.process(row, out)?;
        self.task_context.base_iometrics.num_records_out_inc_by(rows);
        Ok(())
    }
}