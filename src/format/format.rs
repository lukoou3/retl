use std::fmt::Debug;
use crate::Result;
use crate::data::Row;

pub trait Serialization: Debug + SerializationSink {
    fn serialize(&mut self, row: &dyn Row) -> Result<&[u8]>;
}

pub trait SerializationSink {
    fn clone_box(&self) -> Box<dyn Serialization>;
}

impl<T: Serialization + Clone + 'static> SerializationSink for T {
    fn clone_box(&self) -> Box<dyn Serialization> {
        Box::new(self.clone())
    }
}

pub trait Deserialization: Debug {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row>;
}

pub trait DeserializationSink {
    fn clone_box(&self) -> Box<dyn Deserialization>;
}

impl<T: Deserialization + Clone + 'static> DeserializationSink for T {
    fn clone_box(&self) -> Box<dyn Deserialization> {
        Box::new(self.clone())
    }
}
