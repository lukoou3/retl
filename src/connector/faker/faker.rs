use crate::data::{GenericRow, Row, Value};
use crate::types::DataType;
use crate::Result;
use std::fmt::Debug;

pub trait Faker: Debug {
    fn data_type(&self) -> DataType;
    fn init(&mut self) -> Result<()> {
        Ok(())
    }
    fn gene_value(&mut self) -> Value;
    fn destroy(&mut self) -> Result<()> {
        Ok(())
    }

    fn is_union_faker(&self) -> bool {
        false
    }
    fn gene_union_value(&mut self, row: &mut GenericRow) {}
    fn is_compute_faker(&self) -> bool {
        false
    }
    fn gene_compute_value(&mut self, row: & GenericRow) -> Value {
        Value::Null
    }
}
