use std::net::Ipv6Addr;
use std::sync::Arc;
use rand::Rng;
use crate::connector::faker::Faker;
use crate::data::Value;
use crate::types::DataType;

#[derive(Debug)]
pub struct Ipv4Faker {
    start: u32,
    end: u32,
}

impl Ipv4Faker {
    pub fn new(start: u32, end: u32) -> Self {
        Self { start, end }
    }
}

impl Faker for Ipv4Faker {
    fn data_type(&self) -> DataType {
        DataType::String
    }
    fn gene_value(&mut self) -> Value {
        let mut buf = String::with_capacity(15);
        let ip = rand::rng().random_range(self.start..self.end);
        buf.clear();
        buf.push_str(&((ip >> 24) & 0xff).to_string());
        buf.push('.');
        buf.push_str(&((ip >> 16) & 0xff).to_string());
        buf.push('.');
        buf.push_str(&((ip >> 8) & 0xff).to_string());
        buf.push('.');
        buf.push_str(&(ip & 0xff).to_string());
        Value::String(Arc::new(buf))
    }

}

#[derive(Debug)]
pub struct Ipv6Faker {
    start: u128,
    end: u128,
}

impl Ipv6Faker {
    pub fn new(start: u128, end: u128) -> Self {
        Self { start, end }
    }
}

impl Faker for Ipv6Faker {
    fn data_type(&self) -> DataType {
        DataType::String
    }
    fn gene_value(&mut self) -> Value {
        let ip = rand::rng().random_range(self.start..self.end);
        let addr = Ipv6Addr::from(ip);
        Value::String(Arc::new(addr.to_string()))
    }
}
