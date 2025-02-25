use std::cmp;
use std::fmt::Debug;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::Result;
use crate::connector::faker::Faker;
use crate::connector::Source;
use crate::data::{GenericRow, Row};
use crate::execution::Collector;
use crate::physical_expr::{get_cast_func, CastFunc};
use crate::types::Schema;

pub struct FakerSource {
    schema: Schema,
    fakers: Vec<(usize, Box<dyn Faker>)>,
    converters: Vec<Box<CastFunc>>,
    rows_per_second: i32,
    number_of_rows: i64,
    millis_per_row: i64,
}

impl FakerSource {
    pub fn new(schema: Schema, fakers: Vec<(usize, Box<dyn Faker>)>, rows_per_second: i32, number_of_rows: i64, millis_per_row: i64) -> Self {
        let fields = &schema.fields;
        let converters: Vec<Box<CastFunc>> = fakers.iter().map(|(i, x)| {
            let from = x.data_type();
            let to = fields[*i].data_type.clone();
            if from != to {
                println!("{}({}) cast from {} to {}", fields[*i].name, *i, from, to)
            }
            get_cast_func(from, to)
        }).collect();
        Self{ schema, converters, fakers, rows_per_second, number_of_rows, millis_per_row }
    }
}

impl Clone for FakerSource {
    fn clone(&self) -> Self {
        Self::new(self.schema.clone(), self.fakers.iter().map(|(i, x)| (*i, x.clone_box())).collect(),
                  self.rows_per_second, self.number_of_rows, self.millis_per_row)
    }
}

impl Debug for FakerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakerSource")
            .field("schema", &self.schema)
            .field("fakers", &self.fakers)
            .field("rows_per_second", &self.rows_per_second)
            .finish()
    }
}


impl Source for FakerSource  {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        for (_, f) in self.fakers.iter_mut() {
            f.init()?
        }
        Ok(())
    }

    fn run(&mut self, out: &mut dyn Collector) -> Result<()> {
        let rows_for_subtask = self.number_of_rows;
        let rows_per_second = self.rows_per_second;
        let mut row = GenericRow::new_with_size(self.schema.fields.len());
        let mut rows = 0;
        let mut batch_rows = 0;
        let mut next_read_ts = current_timestamp_millis();
        let mut current_ts = 0;
        let mut wait_ms = 0;

        while rows < rows_for_subtask {
            for (i, faker) in self.fakers.iter_mut() {
                row.update(*i, self.converters[*i](faker.gene_value()));
            }
            out.collect(&row)?;
            rows += 1;

            if self.millis_per_row > 0 {
                sleep(Duration::from_millis(self.millis_per_row as u64));
            } else {
                batch_rows += 1;
                if batch_rows >= rows_per_second {
                    batch_rows = 0;
                    next_read_ts += 1000;
                    current_ts = current_timestamp_millis();
                    if next_read_ts > current_ts {
                        wait_ms = next_read_ts - current_ts;
                        sleep(Duration::from_millis(wait_ms))
                    }
                }
            }
        }
        Ok(())
    }
}


#[inline]
fn current_timestamp_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system time before Unix epoch").as_millis() as u64
}
