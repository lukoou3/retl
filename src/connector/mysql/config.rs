use serde::{Deserialize, Serialize};
use crate::config::{SinkConfig, SinkProvider, TaskContext};
use crate::connector::batch::{BatchConfig, BatchSettings};
use crate::connector::mysql::sink::MysqlSink;
use crate::connector::Sink;
use crate::types::Schema;

#[derive(Clone, Copy, Debug, Default)]
pub struct MysqlDefaultBatchSettings;

impl BatchSettings for MysqlDefaultBatchSettings {
    const MAX_ROWS: usize = 10000;
    const MAX_BYTES: usize = 1024 * 1024 * 10;
    const INTERVAL_MS: u64 = 30000;
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlSinkConfig {
    pub url: String,
    pub table: String,
    #[serde(default)]
    pub upsert: bool,
    #[serde(flatten, default)]
    pub batch_config: BatchConfig<MysqlDefaultBatchSettings>,
}

#[typetag::serde(name = "mysql")]
impl SinkConfig for MysqlSinkConfig {
    fn build(&self, schema: Schema) -> crate::Result<Box<dyn SinkProvider>> {
        Ok(Box::new(MysqlSinkProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct MysqlSinkProvider {
    schema: Schema,
    sink_config: MysqlSinkConfig
}

impl MysqlSinkProvider {
    pub fn new(schema: Schema, sink_config: MysqlSinkConfig) -> Self {
        Self {
            schema,
            sink_config
        }
    }
}

impl SinkProvider for MysqlSinkProvider {
    fn create_sink(&self, task_context: TaskContext) -> crate::Result<Box<dyn Sink>> {
        Ok(Box::new(MysqlSink::new(task_context, self.schema.clone(), self.sink_config.clone()).map_err(|e| e.to_string())?))
    }
}

