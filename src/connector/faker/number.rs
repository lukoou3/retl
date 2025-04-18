use crate::connector::faker::Faker;
use crate::data::Value;
use crate::types::DataType;
use rand::Rng;

#[derive(Debug)]
pub struct RangeIntFaker {
    start: i32,
    end: i32,
    random: bool,
    one_value: bool,
    value: i32,
}

impl RangeIntFaker {
    pub fn new(start: i32, end: i32, random: bool) -> Self {
        if start >= end {
            panic!("RangeIntFaker start must not be greater than end.");
        }
        Self {start, end, random, one_value: start + 1 == end, value: start }
    }
}

impl Faker for RangeIntFaker {
    fn data_type(&self) -> DataType {
        DataType::Int
    }

    fn gene_value(&mut self) -> Value {
        if self.one_value {
            return Value::Int(self.start);
        }
        if self.random {
            Value::Int(rand::thread_rng().gen_range(self.start..self.end))
        } else {
            if self.value == self.end {
                self.value = self.start;
            }
            let value = Value::Int(self.value);
            self.value += 1;
            value
        }
    }
}

#[derive(Debug)]
pub struct OptionIntFaker {
    options: Box<[Value]>,
    random: bool,
    index: usize,
}

impl OptionIntFaker {
    pub fn new(options: Vec<Value>, random: bool) -> Self {
        let options = options.into_boxed_slice();
        Self{options, random, index: 0}
    }
}

impl Faker for OptionIntFaker {
    fn data_type(&self) -> DataType {
        DataType::Int
    }
    fn gene_value(&mut self) -> Value {
        if self.options.len() == 0 {
            Value::Null
        } else if self.options.len() == 1 {
            self.options[0].clone()
        } else {
            if !self.random {
                if self.index == self.options.len() {
                    self.index = 0;
                }
                let value = self.options[self.index].clone();
                self.index += 1;
                value
            } else {
                self.options[rand::thread_rng().gen_range(0..self.options.len())].clone()
            }
        }
    }
}

#[derive(Debug)]
pub struct RangeLongFaker {
    start: i64,
    end: i64,
    random: bool,
    one_value: bool,
    value: i64,
}

impl RangeLongFaker {
    pub fn new(start: i64, end: i64, random: bool) -> Self {
        if start >= end {
            panic!("RangeLongFaker start must not be greater than end");
        }
        Self {start, end, random, one_value: start + 1 == end, value: start }
    }
}

impl Faker for RangeLongFaker {
    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn gene_value(&mut self) -> Value {
        if self.one_value {
            return Value::Long(self.start);
        }
        if self.random {
            Value::Long(rand::thread_rng().gen_range(self.start..self.end))
        } else {
            if self.value == self.end {
                self.value = self.start;
            }
            let value = Value::Long(self.value);
            self.value += 1;
            value
        }
    }
}

#[derive(Debug)]
pub struct OptionLongFaker {
    options: Box<[Value]>,
    random: bool,
    index: usize,
}

impl OptionLongFaker {
    pub fn new(options: Vec<Value>, random: bool) -> Self {
        let options = options.into_boxed_slice();
        Self{options, random, index: 0}
    }
}

impl Faker for OptionLongFaker {
    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn gene_value(&mut self) -> Value {
        if self.options.len() == 0 {
            Value::Null
        } else if self.options.len() == 1 {
            self.options[0].clone()
        } else {
            if !self.random {
                if self.index == self.options.len() {
                    self.index = 0;
                }
                let value = self.options[self.index].clone();
                self.index += 1;
                value
            } else {
                self.options[rand::thread_rng().gen_range(0..self.options.len())].clone()
            }
        }
    }
}

#[derive(Debug)]
pub struct RangeDoubleFaker {
    start: f64,
    end: f64,
}

impl RangeDoubleFaker {
    pub fn new(start: f64, end: f64) -> Self {
        if start >= end {
            panic!("RangeDoubleFaker start must not be greater than end");
        }
        Self { start, end }
    }
}

impl Faker for RangeDoubleFaker {
    fn data_type(&self) -> DataType {
        DataType::Double
    }

    fn gene_value(&mut self) -> Value {
        Value::Double(rand::thread_rng().gen_range(self.start..self.end))
    }
}

#[derive(Debug)]
pub struct OptionDoubleFaker {
    options: Box<[Value]>,
    random: bool,
    index: usize,
}

impl OptionDoubleFaker {
    pub fn new(options: Vec<Value>, random: bool) -> Self {
        let options = options.into_boxed_slice();
        Self{options, random, index: 0}
    }
}

impl Faker for OptionDoubleFaker {
    fn data_type(&self) -> DataType {
        DataType::Double
    }

    fn gene_value(&mut self) -> Value {
        if self.options.len() == 0 {
            Value::Null
        } else if self.options.len() == 1 {
            self.options[0].clone()
        } else {
            if !self.random {
                if self.index == self.options.len() {
                    self.index = 0;
                }
                let value = self.options[self.index].clone();
                self.index += 1;
                value
            } else {
                self.options[rand::thread_rng().gen_range(0..self.options.len())].clone()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use super::*;
    #[test]
    fn test_range_int() {
        let mut fakes:Vec<Box<dyn Faker>> = vec![
            Box::new(RangeIntFaker::new(0, 20, true)),
            Box::new(RangeIntFaker::new(0, 20, false)),
            Box::new(OptionIntFaker::new(vec![1, 3, 5, 7, 9].into_iter().map(|x| Value::Int(x)).collect(), true)),
            Box::new(OptionIntFaker::new(vec![1, 3, 5, 7, 9].into_iter().map(|x| Value::Int(x)).collect(), false)),
            Box::new(RangeLongFaker::new(0, 20, true)),
            Box::new(RangeLongFaker::new(0, 20, false)),
            Box::new(OptionLongFaker::new(vec![1, 3, 5, 7, 9].into_iter().map(|x| Value::Long(x)).collect(), true)),
            Box::new(OptionLongFaker::new(vec![1, 3, 5, 7, 9].into_iter().map(|x| Value::Long(x)).collect(), false)),
            Box::new(RangeDoubleFaker::new(0.0, 10000.0)),
            Box::new(OptionDoubleFaker::new(vec![1.0, 3.0, 5.0, 7.0, 9.0].into_iter().map(|x| Value::Double(x)).collect(), true)),
            Box::new(OptionDoubleFaker::new(vec![1.0, 3.0, 5.0, 7.0, 9.0].into_iter().map(|x| Value::Double(x)).collect(), false)),
        ];
        for _ in 0..30 {
            let values:Vec<_> = fakes.iter_mut().map(| f| f.gene_value()).collect();
            println!("{}", values.iter().map(|x| format!("{x:<20?}")).join(", "));
        }
    }
}