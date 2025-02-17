use std::fmt::Debug;
use crate::Result;
use crate::data::Row;

pub trait Serialization: Debug {
    fn serialize(&mut self, row: &dyn Row) -> Result<&[u8]>;
}

pub trait Deserialization: Debug {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row>;
}