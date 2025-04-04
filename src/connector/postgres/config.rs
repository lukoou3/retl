use serde::{Deserialize, Serialize};
use crate::config::{SinkConfig, SinkProvider, TaskContext};
use crate::connector::batch::{BatchConfig, BatchSettings};
use crate::connector::postgres::sink::PostgresSink;
use crate::connector::Sink;
use crate::types::Schema;

#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresDefaultBatchSettings;

impl BatchSettings for PostgresDefaultBatchSettings {
    const MAX_ROWS: usize = 10000;
    const MAX_BYTES: usize = 1024 * 1024 * 30;
    const INTERVAL_MS: u64 = 30000;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSinkConfig {
    pub connect_params: String,
    pub table: String,
    #[serde(flatten, default)]
    pub batch_config: BatchConfig<PostgresDefaultBatchSettings>,
}

#[typetag::serde(name = "postgres")]
impl SinkConfig for PostgresSinkConfig {
    fn build(&self, schema: Schema) -> crate::Result<Box<dyn SinkProvider>> {
        Ok(Box::new(PostgresSinkProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct PostgresSinkProvider {
    schema: Schema,
    sink_config: PostgresSinkConfig
}

impl PostgresSinkProvider {
    pub fn new(schema: Schema, sink_config: PostgresSinkConfig) -> Self {
        Self { schema, sink_config }
    }
}

impl SinkProvider for PostgresSinkProvider {
    fn create_sink(&self, task_context: TaskContext) -> crate::Result<Box<dyn Sink>> {
        Ok(Box::new(PostgresSink::new(task_context, self.schema.clone(), self.sink_config.clone())?))
    }
}

