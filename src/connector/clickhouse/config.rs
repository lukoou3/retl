use serde::{Deserialize, Serialize};
use crate::Result;
use crate::config::{SinkConfig, SinkProvider, TaskContext};
use crate::connector::batch::{BatchConfig, BatchSettings};
use crate::connector::clickhouse::ClickHouseSink;
use crate::connector::Sink;
use crate::types::Schema;

#[derive(Clone, Copy, Debug, Default)]
pub struct ClickHouseDefaultBatchSettings;

impl BatchSettings for ClickHouseDefaultBatchSettings {
    const MAX_ROWS: usize = 100000;
    const MAX_BYTES: usize = 1024 * 1024 * 60;
    const INTERVAL_MS: u64 = 30000;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub host: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
}

impl ConnectionConfig {
    pub fn build_urls(&self) -> Vec<String> {
        self.host.split(',')
            .map(|host| format!( "http://{}", host.trim()))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseSinkConfig {
    #[serde(flatten)]
    pub connection_config: ConnectionConfig,
    #[serde(flatten, default)]
    pub batch_config: BatchConfig<ClickHouseDefaultBatchSettings>,
}

#[typetag::serde(name = "clickhouse")]
impl SinkConfig for ClickHouseSinkConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn SinkProvider>> {
        let config = self.clone();
        Ok(Box::new(ClickHouseSinkProvider::new(schema, config)))
    }
}

#[derive(Debug, Clone)]
pub struct ClickHouseSinkProvider {
    schema: Schema,
    sink_config: ClickHouseSinkConfig
}

impl ClickHouseSinkProvider {
    pub fn new(schema: Schema, sink_config: ClickHouseSinkConfig) -> Self {
        Self {
            schema,
            sink_config
        }
    }
}

impl SinkProvider for ClickHouseSinkProvider {
    fn create_sink(&self, task_context: TaskContext) -> Result<Box<dyn Sink>> {
        ClickHouseSink::new(
            task_context,
            &self.schema,
            self.sink_config.connection_config.clone(),
            self.sink_config.batch_config.clone()
        ).map(|sink| Box::new(sink) as Box<dyn Sink>)
    }
}

