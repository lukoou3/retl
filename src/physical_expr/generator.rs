use std::fmt::Debug;
use std::sync::Arc;
use crate::data::{GenericRow, Row};
use crate::physical_expr::PhysicalExpr;

pub trait PhysicalGenerator: Debug {
    fn generate(&mut self, input: &dyn Row) -> &[GenericRow];
}

#[derive(Debug, Clone)]
pub struct Explode {
    pub child: Arc<dyn PhysicalExpr>,
    pub rows: Vec<GenericRow>,
}

impl Explode {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        let rows = Vec::new();
        Explode { child, rows}
    }
}

impl PhysicalGenerator for Explode {
    fn generate(&mut self, input: &dyn Row) -> &[GenericRow]{
        let value = self.child.eval(input);
        if value.is_null() {
            return &self.rows[..0];
        }
        let array = value.get_array();
        if self.rows.len() >= array.len() {
            if self.rows.len() > 100 && array.len() <= 100 {
                self.rows.truncate(100);
            }
        } else {
            for _ in self.rows.len()..array.len() {
                self.rows.push(GenericRow::new_with_size(1));
            }
        }

        for (i, value) in array.iter().enumerate() {
            self.rows[i].update(0, value.clone());
        }

        &self.rows[ ..array.len()]
    }
}