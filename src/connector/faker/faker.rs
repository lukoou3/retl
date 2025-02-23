use crate::data::Value;
use crate::types::DataType;
use crate::Result;
use std::fmt::Debug;

pub trait Faker: Debug + Send + Sync  + CloneFaker {
    fn data_type(&self) -> DataType;
    fn init(&mut self) -> Result<()> {
        Ok(())
    }
    fn gene_value(&mut self) -> Value;
    fn destroy(&mut self) -> Result<()> {
        Ok(())
    }
}

pub trait CloneFaker {
    fn clone_box(&self) -> Box<dyn Faker>;
}

impl<T: Faker + Clone + 'static> CloneFaker for T {
    fn clone_box(&self) -> Box<dyn Faker> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Faker> {
    fn clone(&self) -> Box<dyn Faker> {
        self.clone_box()
    }
}
