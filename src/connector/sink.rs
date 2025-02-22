use std::fmt::Debug;
use crate::data::Row;
use crate::Result;

pub trait Sink: Debug {
    fn name(&self) -> &str;
    fn open(&mut self) -> Result<()> {
        Ok(())
    }
    fn invoke(&mut self, row: &dyn Row);

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
