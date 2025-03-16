use std::sync::Arc;
use rand::Rng;
use crate::connector::faker::Faker;
use crate::data::Value;
use crate::types::DataType;

#[derive(Debug)]
pub struct ArrayFaker {
    ele_faker: Box<dyn Faker>,
    array_len_min: usize,
    array_len_max: usize,
}

impl ArrayFaker {
    pub fn new(ele_faker: Box<dyn Faker>, array_len_min: usize, array_len_max: usize) -> Self {
        ArrayFaker {
            ele_faker,
            array_len_min,
            array_len_max,
        }
    }
}

impl Faker for ArrayFaker {

    fn data_type(&self) -> DataType {
        DataType::Array(Box::new(self.ele_faker.data_type()))
    }

    fn init(&mut self) -> crate::Result<()> {
        self.ele_faker.init()
    }

    fn gene_value(&mut self) -> Value {
        let len = rand::thread_rng().gen_range(self.array_len_min..=self.array_len_max);
        if len == 0 {
            return Value::Array(Arc::new(vec![]));
        }
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(self.ele_faker.gene_value());
        }
        Value::Array(Arc::new(values))
    }

    fn destroy(&mut self) -> crate::Result<()> {
        self.ele_faker.destroy()
    }
}

#[derive(Debug)]
pub struct NullAbleFaker {
    ele_faker: Box<dyn Faker>,
    null_rate: f32,
}

impl NullAbleFaker {
    pub fn new(ele_faker: Box<dyn Faker>, null_rate: f32) -> Self {
        NullAbleFaker {
            ele_faker,
            null_rate,
        }
    }
}

impl Faker for NullAbleFaker {
    fn data_type(&self) -> DataType {
        self.ele_faker.data_type()
    }

    fn init(&mut self) -> crate::Result<()> {
        self.ele_faker.init()
    }

    fn gene_value(&mut self) -> Value {
        if rand::thread_rng().gen_range(0.0f32..1.0f32) < self.null_rate {
            Value::Null
        } else {
            self.ele_faker.gene_value()
        }
    }

    fn destroy(&mut self) -> crate::Result<()> {
        self.ele_faker.destroy()
    }
}

