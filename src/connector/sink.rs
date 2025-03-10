use std::fmt::Debug;
use crate::data::Row;
use crate::Result;

pub trait Sink: Debug {
    fn open(&mut self) -> Result<()> {
        Ok(())
    }
    fn invoke(&mut self, row: &dyn Row) -> Result<()> ;

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
