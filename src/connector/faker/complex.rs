use std::fmt::Debug;
use std::sync::Arc;
use rand::Rng;
use crate::connector::faker::Faker;
use crate::data::{GenericRow, Row, Value};
use crate::physical_expr::{CastFunc, PhysicalExpr};
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

#[derive(Debug)]
pub struct EvalFaker {
    expr: Arc<dyn PhysicalExpr>,
}

impl EvalFaker {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        EvalFaker { expr, }
    }
}

impl Faker for EvalFaker {
    fn data_type(&self) -> DataType {
        self.expr.data_type()
    }

    fn gene_value(&mut self) -> Value {
        panic!("EvalFaker::gene_value not implemented")
    }

    fn is_compute_faker(&self) -> bool {
        true
    }

    fn gene_compute_value(&mut self, row: & GenericRow) -> Value {
        self.expr.eval(row)
    }
}


pub struct FieldFaker {
    pub index: usize,
    pub faker: Box<dyn Faker>,
    pub converter: Box<CastFunc>,
}

impl FieldFaker {
    pub fn new(index: usize, faker: Box<dyn Faker>, converter: Box<CastFunc>) -> Self {
        FieldFaker {
            index,
            faker,
            converter,
        }
    }
}

impl Debug for FieldFaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldFaker")
            .field("index", &self.index)
            .field("faker", &self.faker)
            .finish()
    }
}

#[derive(Debug)]
pub struct FieldsFaker {
    field_fakers: Vec<FieldFaker>,
    weight: u32,
    weight_index: u32,
}

impl FieldsFaker {
    pub fn new(field_fakers: Vec<FieldFaker>, weight: u32) -> Self {
        FieldsFaker {
            field_fakers,
            weight,
            weight_index: 0,
        }
    }
}

#[derive(Debug)]
pub struct UnionFaker {
    fields_fakers: Vec<FieldsFaker>,
    random: bool,
    weights: Vec<u32>,
    weight_max: u32,
    index: usize,
}

impl UnionFaker {
    pub fn new(fields_fakers: Vec<FieldsFaker>, random: bool) -> Self {
        let mut weights = Vec::with_capacity(fields_fakers.len());
        for (i, faker) in fields_fakers.iter().enumerate() {
            if i == 0 {
                weights.push(faker.weight);
            } else {
                weights.push(faker.weight + weights[i - 1]);
            }
        }
        let weight_max = weights[weights.len() - 1];
        UnionFaker {
            fields_fakers,
            random,
            weights,
            weight_max,
            index: 0,
        }
    }
}

impl Faker for UnionFaker {
    fn data_type(&self) -> DataType {
        DataType::Null
    }

    fn init(&mut self) -> crate::Result<()> {
        for fields_faker in self.fields_fakers.iter_mut() {
            for field_faker in fields_faker.field_fakers.iter_mut() {
                field_faker.faker.init()?;
            }
        }
        Ok(())
    }

    fn gene_value(&mut self) -> Value {
        panic!("UnionFaker::gene_value not implemented")
    }

    fn destroy(&mut self) -> crate::Result<()> {
        let mut rst = Ok(());
        for fields_faker in self.fields_fakers.iter_mut() {
            for field_faker in fields_faker.field_fakers.iter_mut() {
                if let Err(e) = field_faker.faker.destroy() {
                    rst = Err(e);
                }
            }
        }
        rst
    }

    fn is_union_faker(&self) -> bool {
        true
    }

    fn gene_union_value(&mut self, row: &mut GenericRow) {
        let mut fields_faker;
        if self.random {
            let key = rand::thread_rng().gen_range(1..= self.weight_max) ;
            let index = self.weights.binary_search(&key).unwrap_or_else(|i| i);
            fields_faker = &mut self.fields_fakers[index];
        } else {
            fields_faker = &mut self.fields_fakers[self.index];
            if fields_faker.weight_index == fields_faker.weight {
                fields_faker.weight_index = 0;
                self.index += 1;
                if self.index == self.fields_fakers.len() {
                    self.index = 0;
                }
                fields_faker = &mut self.fields_fakers[self.index];
            }
            fields_faker.weight_index += 1;
        }

        for field_faker in fields_faker.field_fakers.iter_mut() {
            let value = if field_faker.faker.is_compute_faker(){
                field_faker.faker.gene_compute_value(row)
            } else {
                field_faker.faker.gene_value()
            };
            if ! value.is_null() {
                let value = (field_faker.converter)(value);
                row.update(field_faker.index, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use rand::Rng;

    #[test]
    fn test_weight_random() {
        let mut r = rand::thread_rng();
        let items = vec![3, 1, 2, 6];
        let mut weights = vec![items[0]];
        for i in 1..items.len() {
            weights.push(items[i] + weights[i - 1]);
        }
        let weight_max = weights[weights.len() - 1];
        println!("items: {:?}", items);
        println!("weights: {:?}", weights);
        println!("weight_max: {}", weight_max);

        let mut item_counts = HashMap::new();
        for i in 0..1000000 {
            let key = r.gen_range(1..= weight_max) ;
            let index = weights.binary_search(&key).unwrap_or_else(|i| i);
            //println!("key: {}, index: {}", key, index);
            if let Some(count) = item_counts.get_mut(&items[index]) {
                *count += 1;
            } else {
                item_counts.insert(items[index], 1);
            }
        }
        println!("{}", "".repeat(80));

        for (&item, &count) in item_counts.iter() {
            println!("item: {}, count: {}", item, count);
        }
    }
}
