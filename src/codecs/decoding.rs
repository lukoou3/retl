use std::fmt::Debug;
use dyn_clone::DynClone;
use typetag::serde;
use crate::Result;
use crate::data::Row;
use crate::types::Schema;

#[serde(tag = "codec")]
pub trait DeserializerConfig: DynClone + Debug + Send + Sync {
    fn build(&self, schema: Schema) -> Result<Box<dyn Deserializer>>;
}
dyn_clone::clone_trait_object!(DeserializerConfig);

pub trait Deserializer: Debug {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row>;
}