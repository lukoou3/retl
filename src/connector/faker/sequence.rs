use crate::connector::faker::Faker;
use crate::data::Value;
use crate::types::DataType;

#[derive(Debug)]
pub struct SequenceFaker {
    start: i64,
    step: i64,
    batch: u32,
    cnt: u32,
    value: i64,
}

impl SequenceFaker {
    pub fn new(start: i64, step: i64, batch: u32) -> Self {
        Self {start, step, batch, cnt: 0, value: start}
    }
}

impl Faker for SequenceFaker {
    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn gene_value(&mut self) -> Value {
        let rst = Value::Long(self.value);
        self.cnt += 1;
        if self.cnt == self.batch {
            self.cnt = 0;
            self.value += self.step;
        }
        rst
    }
}