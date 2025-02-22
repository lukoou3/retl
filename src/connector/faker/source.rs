use std::fmt::Debug;
use std::thread::sleep;
use std::time::Duration;
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
}

impl FakerSource {
    pub fn new(schema: Schema, fakers: Vec<(usize, Box<dyn Faker>)>, rows_per_second: i32) -> Self {
        let fields = &schema.fields;
        let converters: Vec<Box<CastFunc>> = fakers.iter().map(|(i, x)| {
            let from = x.data_type();
            let to = fields[*i].data_type.clone();
            get_cast_func(from, to)
        }).collect();
        Self{ schema, converters, fakers, rows_per_second }
    }
}

impl Clone for FakerSource {
    fn clone(&self) -> Self {
        Self::new(self.schema.clone(), self.fakers.iter().map(|(i, x)| (*i, x.clone_box())).collect(), self.rows_per_second)
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
    fn name(&self) -> &str {
        "FakerSource"
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        for (_, f) in self.fakers.iter_mut() {
            f.init()?
        }
        Ok(())
    }

    fn run(&mut self, out: &mut dyn Collector) {
        let sec = Duration::from_secs(1);
        let mut row = GenericRow::new_with_size(self.schema.fields.len());
        let mut rows = 0;
        loop {
            for (i, faker) in self.fakers.iter_mut() {
                row.update(*i, self.converters[*i](faker.gene_value()));
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

