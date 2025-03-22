use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::config::TaskContext;
use crate::Result;
use crate::connector::faker::{Faker, FieldFaker};
use crate::connector::Source;
use crate::data::{GenericRow, Row};
use crate::execution::Collector;
use crate::physical_expr::{get_cast_func, CastFunc};
use crate::types::{DataType, Schema};

pub struct FakerSource {
    task_context: TaskContext,
    schema: Schema,
    field_fakers: Vec<FieldFaker>,
    rows_per_second: i32,
    number_of_rows: i64,
    millis_per_row: i64,
}

impl FakerSource {
    pub fn new(task_context: TaskContext, schema: Schema, fakers: Vec<(usize, Box<dyn Faker>)>, rows_per_second: i32, number_of_rows: i64, millis_per_row: i64) -> Self {
        let fields = &schema.fields;
        let field_fakers = fakers.into_iter().map(|(i, faker)| {
            if faker.is_union_faker() {
                return FieldFaker::new(0, faker, get_cast_func(DataType::Null, DataType::Null));
            }
            let from = faker.data_type();
            let to = fields[i].data_type.clone();
            if from != to {
                println!("{}({}) cast from {} to {}", fields[i].name, i, from, to)
            }
            FieldFaker::new(i, faker, get_cast_func(from, to))
        }).collect();
        Self{ task_context, schema, field_fakers, rows_per_second, number_of_rows, millis_per_row }
    }

    fn get_rows_for_subtask(&self) -> i64 {
        if self.number_of_rows < 0 {
            i64::MAX
        } else {
            let num_subtasks = self.task_context.task_config.subtask_parallelism as i64;
            let index_of_this_subtask = self.task_context.task_config.subtask_index as i64;
            let base_rows_per_subtask = self.number_of_rows / num_subtasks;
            if self.number_of_rows % num_subtasks > index_of_this_subtask  {
                base_rows_per_subtask + 1
            } else {
                base_rows_per_subtask
            }
        }
    }

    fn get_rows_per_second_subtask(&self) -> i32 {
        if self.rows_per_second < 0 {
            1
        } else {
            let num_subtasks = self.task_context.task_config.subtask_parallelism as i32;
            let index_of_this_subtask = self.task_context.task_config.subtask_index as i32;
            let base_rows_per_second_per_subtask = self.rows_per_second / num_subtasks;
            if self.rows_per_second % num_subtasks > index_of_this_subtask {
                base_rows_per_second_per_subtask + 1
            } else {
                base_rows_per_second_per_subtask
            }
        }
    }
}

impl Debug for FakerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakerSource")
            .field("schema", &self.schema)
            .field("rows_per_second", &self.rows_per_second)
            .finish()
    }
}


impl Source for FakerSource  {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        for field_faker in self.field_fakers.iter_mut() {
            field_faker.faker.init()?
        }
        Ok(())
    }

    fn run(&mut self, out: &mut dyn Collector, terminated: Arc<AtomicBool>) -> Result<()> {
        let rows_for_subtask = self.get_rows_for_subtask();
        let rows_per_second = self.get_rows_per_second_subtask();
        let mut row = GenericRow::new_with_size(self.schema.fields.len());
        let mut rows = 0;
        let mut batch_rows = 0;
        let mut next_read_ts = current_timestamp_millis();
        let mut current_ts = 0;
        let mut wait_ms = 0;

        while !terminated.load(Ordering::Acquire) && rows < rows_for_subtask {
            self.task_context.base_iometrics.num_records_in_inc_by(1);
            row.fill_null();
            for field_faker in self.field_fakers.iter_mut() {
                if field_faker.faker.is_union_faker() {
                    field_faker.faker.gene_union_value(&mut row);
                    continue;
                }
                let value = if field_faker.faker.is_compute_faker(){
                    field_faker.faker.gene_compute_value(&row)
                } else {
                    field_faker.faker.gene_value()
                };
                if ! value.is_null() {
                    let value = (field_faker.converter)(value);
                    row.update(field_faker.index, value);
                }
            }
            out.collect(&row)?;
            rows += 1;
            self.task_context.base_iometrics.num_records_out_inc_by(1);

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
