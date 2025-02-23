use std::sync::Arc;
use crate::Result;
use crate::data::{GenericRow, Row};
use crate::execution::Collector;
use crate::physical_expr::PhysicalExpr;
use crate::transform::Transform;

#[derive(Debug)]
pub struct QueryTransform {
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    row: GenericRow,
}

impl QueryTransform {
    pub fn new(exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        let row = GenericRow::new_with_size(exprs.len());
        Self { exprs, row}
    }
}

impl Transform for QueryTransform {
    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector) -> Result<()> {
        self.row.fill_null();
        for (i, expr) in self.exprs.iter().enumerate() {
            self.row.update(i, expr.eval(row));
        }
        out.collect(& self.row)
    }
}