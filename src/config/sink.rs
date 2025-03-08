use std::fmt::Debug;
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use crate::config::TaskContext;
use crate::Result;
use crate::connector::Sink;
use crate::types::Schema;

#[derive(Clone, Debug, Serialize,Deserialize)]
pub struct SinkOuter {
    pub inputs: Vec<String>,
    #[serde(flatten)]
    pub inner: BoxedSinkConfig,
}

pub type BoxedSinkConfig = Box<dyn SinkConfig>;

#[typetag::serde(tag = "type")]
pub trait SinkConfig: DynClone + Debug + Send + Sync {
    fn build(&self, schema: Schema) -> Result<Box<dyn SinkProvider>>;
}
dyn_clone::clone_trait_object!(SinkConfig);

pub trait SinkProvider: DynClone + Send + Sync {
    fn create_sink(&self, task_context: TaskContext) -> Result<Box<dyn Sink>>;
}

