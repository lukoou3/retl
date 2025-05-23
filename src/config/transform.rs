use std::fmt::Debug;
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use crate::config::TaskContext;
use crate::Result;
use crate::transform::Transform;
use crate::types::Schema;

#[derive(Clone, Debug, Serialize,Deserialize)]
pub struct TransformOuter {
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    #[serde(flatten)]
    pub inner: BoxedTransformConfig,
}

pub type BoxedTransformConfig = Box<dyn TransformConfig>;

#[typetag::serde(tag = "type")]
pub trait TransformConfig: DynClone + Debug + Send + Sync {
    fn build(&self, schema: Schema) -> Result<Box<dyn TransformProvider>>;
}
dyn_clone::clone_trait_object!(TransformConfig);

pub trait TransformProvider: DynClone + Send + Sync {
    fn create_transform(&self, task_context: TaskContext) -> Result<Box<dyn Transform>>;
}