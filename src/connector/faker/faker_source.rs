use std::thread::sleep;
use std::time::Duration;
use crate::Result;
use crate::connector::faker::Faker;
use crate::connector::{Collector, Source};
use crate::data::{GenericRow, Row};

#[derive(Debug)]
pub struct FakerSource {
    fakers: Vec<Box<dyn Faker>>,
    rows_per_second: i32,
}

impl FakerSource {
    pub fn new(fakers: Vec<Box<dyn Faker>>, rows_per_second: i32) -> Self {
        Self{fakers, rows_per_second}
    }
}

impl Clone for FakerSource {
    fn clone(&self) -> Self {
        Self::new(self.fakers.iter().map(|x| x.clone_box()).collect(), self.rows_per_second)
    }
}

impl Source for FakerSource  {
    fn name(&self) -> &str {
        "FakerSource"
    }
    fn open(&mut self) -> Result<()> {
        for f in self.fakers.iter_mut() {
            f.init()?
        }
        Ok(())
    }

    fn run(&mut self, out: &dyn Collector) {
        let sec = Duration::from_secs(1);
        let mut row = GenericRow::new_with_size(self.fakers.len());
        let mut rows = 0;
        loop {
            for (i, faker) in self.fakers.iter_mut().enumerate() {
                row.update(i, faker.gene_value());
            }
            rows += 1;
            out.collect(&row);
            if rows >= self.rows_per_second {
                sleep(sec);
                rows = 0;
            }
        }
    }
}

