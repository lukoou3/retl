use std::fmt::Debug;
use crate::Result;
use crate::connector::Collector;
use crate::data::Row;

pub trait Transform: Debug {
    fn name(&self) -> &str;

    fn open(&mut self) -> Result<()> {
        Ok(())
    }

    fn process(&self, row: &dyn Row, out: &dyn Collector);

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
