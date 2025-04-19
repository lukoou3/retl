use std::fmt::Debug;
use std::sync::Arc;
use base64::Engine;
use rand::Rng;
use crate::connector::faker::Faker;
use crate::data::Value;
use crate::sketch::hll::hll::Hll;
use crate::sketch::tdigest::TDigest;
use crate::types::DataType;

pub struct HllFaker {
    item_count: u64,
    batch_count: u32,
    log2m: u32,
    regwidth: u32,
    hll: Hll,
    cache_batch_count: u32,
    max_cache_count: u32,
    cache_count: u32,
}

impl HllFaker {
    pub fn new(item_count: u64, batch_count: u32, log2m: u32, regwidth: u32) -> Self {
        let mut hll = Hll::new(log2m, regwidth);
        let cache_batch_count = batch_count * 8 / 10;
        let max_cache_count = 10000;
        let cache_count = 0;
        let mut rng = rand::rng();
        for _ in 0..cache_batch_count {
            hll.add(&rng.random_range(0..item_count));
        }
        Self {item_count, batch_count, log2m, regwidth, hll, cache_batch_count, max_cache_count, cache_count}
    }
}

impl Faker for HllFaker {
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn gene_value(&mut self) -> Value {
        let item_count = self.item_count;
        let mut rng = rand::rng();
        if self.cache_count >= self.max_cache_count {
            self.cache_count = 0;
            self.hll.clear();
            for _ in 0..self.cache_batch_count {
                self.hll.add(&rng.random_range(0..item_count));
            }
        }
        let mut hll = self.hll.clone();
        for _ in self.cache_batch_count..self.batch_count {
            hll.add(&rng.random_range(0..item_count));
        }
        self.cache_count += 1;
        //println!("{:?}", hll);
        Value::String(Arc::new(base64::engine::general_purpose::STANDARD.encode(&hll.to_bytes())))
    }
}

impl Debug for HllFaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HllFaker")
            .field("item_count", &self.item_count)
            .field("batch_count", &self.batch_count)
            .field("log2m", &self.log2m)
            .field("regwidth", &self.regwidth)
            .finish()
    }
}

#[derive(Debug)]
pub struct TDigestFaker {
    max: u32,
    batch_count: u32,
    compression: u32,
    tdigest: TDigest,
    cache_batch_count: u32,
    max_cache_count: u32,
    cache_count: u32,
}

impl TDigestFaker {
    pub fn new(max: u32, batch_count: u32, compression: u32) -> Self {
        let mut tdigest = TDigest::new(compression as usize);
        let cache_batch_count = batch_count * 9 / 10;
        let max_cache_count = 10000;
        let cache_count = 0;
        let mut rng = rand::rng();
        let values: Vec<f64> = (0..cache_batch_count).map(|_| rng.random_range(0..max) as f64).collect();
        tdigest = tdigest.merge_unsorted_f64(values);
        Self {max, batch_count, compression, tdigest, cache_batch_count, max_cache_count, cache_count}
    }
}

impl Faker for TDigestFaker {
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn gene_value(&mut self) -> Value {
        let max = self.max;
        let mut rng = rand::rng();
        if self.cache_count >= self.max_cache_count {
            self.cache_count = 0;
            self.tdigest = TDigest::new(self.compression as usize);
            let values: Vec<f64> = (0..self.cache_batch_count).map(|_| rng.random_range(0..max) as f64).collect();
            self.tdigest.merge_unsorted_f64(values);
        }
        let mut tdigest = self.tdigest.clone();
        let values: Vec<f64> = (self.cache_batch_count..self.batch_count).map(|_| rng.random_range(0..max) as f64).collect();
        tdigest = tdigest.merge_unsorted_f64(values);
        self.cache_count += 1;
        //println!("{:?}", tdigest);
        Value::String(Arc::new(base64::engine::general_purpose::STANDARD.encode(&tdigest.to_bytes())))
    }
}
