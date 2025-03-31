use std::fmt::Debug;
use dyn_clone::DynClone;
use typetag::serde;
use crate::Result;
use crate::data::Row;
use crate::types::Schema;

#[serde(tag = "codec")]
pub trait SerializerConfig: DynClone + Debug + Send + Sync {
    fn build(&self, schema: Schema) -> Result<Box<dyn Serializer>>;
}
dyn_clone::clone_trait_object!(SerializerConfig);


pub trait Serializer: Debug {
    fn serialize(&mut self, row: &dyn Row) -> Result<&[u8]>;
}