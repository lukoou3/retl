use std::fmt::{Debug, Formatter};
use std::thread::sleep;
use std::time::Duration;
use crate::Result;
use crate::codecs::Deserializer;
use crate::config::TaskContext;
use crate::connector::Source;
use crate::datetime_utils::current_timestamp_millis;
use crate::execution::{Collector, PollStatus};
use crate::types::Schema;

pub struct InlineSource {
    task_context: TaskContext,
    schema: Schema,
    deserializer: Box<dyn Deserializer>,
    datas: Vec<Vec<u8>>,
    rows_per_second: i32,
    number_of_rows: i64,
    millis_per_row: i64,
    rows_for_subtask: i64,
    rows_per_second_subtask: i32,
    index: usize,
    rows: i64,
    batch_rows: i32,
    next_read_ts: u64,
}

impl InlineSource {
    pub fn new(task_context: TaskContext, schema: Schema, deserializer: Box<dyn Deserializer>, datas: Vec<Vec<u8>>, rows_per_second: i32, number_of_rows: i64, millis_per_row: i64) -> Self {
        let rows_for_subtask = Self::get_rows_for_subtask(number_of_rows, &task_context);
        let rows_per_second_subtask = Self::get_rows_per_second_subtask(rows_per_second, &task_context);
        let index = 0;
        let rows = 0;
        let batch_rows = 0;
        let next_read_ts = current_timestamp_millis() / 1000 * 1000;
        Self{ task_context, schema, deserializer, datas, rows_per_second, number_of_rows, millis_per_row, rows_for_subtask, rows_per_second_subtask, index, rows, batch_rows, next_read_ts }
    }

    fn get_rows_for_subtask(number_of_rows: i64, task_context: &TaskContext) -> i64 {
        if number_of_rows < 0 {
            i64::MAX
        } else {
            let num_subtasks = task_context.task_config.subtask_parallelism as i64;
            let index_of_this_subtask = task_context.task_config.subtask_index as i64;
            let base_rows_per_subtask = number_of_rows / num_subtasks;
            if number_of_rows % num_subtasks > index_of_this_subtask  {
                base_rows_per_subtask + 1
            } else {
                base_rows_per_subtask
            }
        }
    }

    fn get_rows_per_second_subtask(rows_per_second: i32, task_context: &TaskContext) -> i32 {
        if rows_per_second < 0 {
            1
        } else {
            let num_subtasks = task_context.task_config.subtask_parallelism as i32;
            let index_of_this_subtask = task_context.task_config.subtask_index as i32;
            let base_rows_per_second_per_subtask = rows_per_second / num_subtasks;
            if rows_per_second % num_subtasks > index_of_this_subtask {
                base_rows_per_second_per_subtask + 1
            } else {
                base_rows_per_second_per_subtask
            }
        }
    }

}

impl Debug for InlineSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InlineSource")
            .field("schema", &self.schema)
            .field("rows_per_second", &self.rows_per_second)
            .finish()
    }
}

impl Source for InlineSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        self.next_read_ts = current_timestamp_millis() / 1000 * 1000;
        Ok(())
    }

    fn poll_next(&mut self, out: &mut dyn Collector) -> Result<PollStatus> {
        if self.rows >= self.rows_for_subtask {
            return Ok(PollStatus::End);
        }

        self.task_context.base_iometrics.num_records_in_inc_by(1);
        let row = self.deserializer.deserialize(self.datas[self.index].as_slice())?;
        out.collect(row)?;
        self.rows += 1;
        self.index += 1;
        if self.index >= self.datas.len() {
            self.index = 0;
        }
        self.task_context.base_iometrics.num_records_out_inc_by(1);

        if self.millis_per_row > 0 {
            sleep(Duration::from_millis(self.millis_per_row as u64));
        } else {
            self.batch_rows += 1;
            if self.batch_rows >= self.rows_per_second_subtask {
                self.batch_rows = 0;
                self.next_read_ts += 1000;
                let current_ts = current_timestamp_millis();
                if self.next_read_ts > current_ts {
                    let wait_ms = self.next_read_ts - current_ts;
                    sleep(Duration::from_millis(wait_ms))
                }
            }
        }

        Ok(PollStatus::More)
    }
}