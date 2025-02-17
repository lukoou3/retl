use std::fmt::Debug;
use crate::data::Row;
use crate::Result;

pub trait Sink: Debug + CloneSink {
    fn name(&self) -> &str;
    fn open(&mut self) -> Result<()> {
        Ok(())
    }
    fn invoke(&mut self, row: &dyn Row);

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

pub trait CloneSink {
    fn clone_box(&self) -> Box<dyn Sink>;
}

impl<T: Sink + Clone + 'static> CloneSink for T {
    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }
}