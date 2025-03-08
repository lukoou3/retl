use std::fmt::Debug;
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use crate::config::TaskContext;
use crate::Result;
use crate::connector::Source;
use crate::types::Schema;

#[derive(Clone, Debug, Serialize,Deserialize)]
pub struct SourceOuter {
    pub outputs: Vec<String>,
    pub schema: String,
    #[serde(flatten)]
    pub inner: BoxedSourceConfig,
}

pub type BoxedSourceConfig = Box<dyn SourceConfig>;

#[typetag::serde(tag = "type")]
pub trait SourceConfig: DynClone + Debug + Send + Sync {
    fn build(&self, schema: Schema) -> Result<Box<dyn SourceProvider>>;
}
dyn_clone::clone_trait_object!(SourceConfig);

pub trait SourceProvider: DynClone + Send + Sync {
    fn create_source(&self, task_context: TaskContext) -> Result<Box<dyn Source>>;
}
dyn_clone::clone_trait_object!(SourceProvider);
