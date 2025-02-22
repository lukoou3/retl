use std::fmt::Debug;
use crate::data::Row;
use crate::execution::Collector;
use crate::Result;
use crate::types::Schema;

pub trait Source: Debug + CloneSource {
    fn name(&self) -> &str;
    fn schema(&self) -> &Schema;
    fn open(&mut self) -> Result<()> {
        Ok(())
    }
    fn run(&mut self, out: &mut dyn Collector);

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

pub trait CloneSource {
    fn clone_box(&self) -> Box<dyn Source>;
}

impl<T: Source + Clone + 'static> CloneSource for T {
    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }
}


