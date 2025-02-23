use std::fmt::Debug;
use crate::Result;
use crate::data::Row;
use crate::execution::Collector;
use crate::types::Schema;

pub trait Transform: Debug {
    fn schema(&self) -> &Schema;

    fn open(&mut self) -> Result<()> {
        Ok(())
    }

    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector) -> Result<()> ;

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
