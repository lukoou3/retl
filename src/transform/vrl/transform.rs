use std::collections::BTreeMap;
use vrl::compiler::TimeZone;
use vrl::core::Value;
use vrl::prelude::KeyString;
use crate::Result;
use crate::config::TaskContext;
use crate::data::Row;
use crate::execution::{Collector, TimeService};
use crate::transform::Transform;
use crate::transform::vrl::convert::ValueToVrlValue;
use crate::transform::vrl::pipeline::Pipeline;
use crate::types::Schema;

#[derive(Debug)]
pub struct VrlTransform {
    task_context: TaskContext,
    schema: Schema,
    drop_on_error: bool,
    drop_on_abort: bool,
    pipeline: Box<dyn Pipeline>,
    converts: Vec<(usize, KeyString, Box<dyn ValueToVrlValue>)>,
}

impl VrlTransform {
    pub fn new(task_context: TaskContext, schema: Schema, drop_on_error: bool, drop_on_abort: bool,
               pipeline: Box<dyn Pipeline>, converts: Vec<(usize, KeyString, Box<dyn ValueToVrlValue>)>) -> Self {
        Self {task_context, schema, drop_on_error, drop_on_abort, pipeline, converts}
    }
}

impl Transform for VrlTransform {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector, _: &mut TimeService) -> Result<()> {
        let mut map = BTreeMap::new();
        for (i, key, convert) in &self.converts {
            map.insert(key.clone(), convert.to_vrl(row.get(*i)));
        }
        self.pipeline.process(Value::Object(map), out)
    }
}

