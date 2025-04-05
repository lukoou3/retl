use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use crate::data::Row;
use crate::execution::{Collector, PollStatus};
use crate::Result;
use crate::types::Schema;



pub trait Source: Debug {
    fn schema(&self) -> &Schema;
    fn open(&mut self) -> Result<()> {
        Ok(())
    }
    //fn run(&mut self, out: &mut dyn Collector, terminated: Arc<AtomicBool>) -> Result<()>;

    fn poll_next(&mut self, out: &mut dyn Collector) -> Result<PollStatus>;

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}



