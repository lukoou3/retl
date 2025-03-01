use crate::data::Value;
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
}
