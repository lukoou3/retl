use std::fmt::Debug;
use crate::data::Row;
use crate::Result;

pub trait Source: Debug + CloneSource {
    fn name(&self) -> &str;
    fn open(&mut self) -> Result<()> {
        Ok(())
    }
    fn run(&mut self, out: &dyn Collector);

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

pub trait Collector{
    fn collect(&self, row: &dyn Row);
}

pub struct PrintCollector;

impl Collector for PrintCollector {
    fn collect(&self, row: &dyn Row) {
        println!("{}", row);
    }
}
