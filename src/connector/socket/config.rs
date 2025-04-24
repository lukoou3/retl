use serde::{Deserialize, Serialize};
use crate::codecs::DeserializerConfig;
use crate::Result;
use crate::config::{SourceConfig, SourceProvider, TaskContext};
use crate::connector::socket::source::SocketSource;
use crate::connector::Source;
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocketSourceConfig {
    hostname: String,
    port: u16,
    decoding: Box<dyn DeserializerConfig>,
}

#[typetag::serde(name = "socket")]
impl SourceConfig for SocketSourceConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn SourceProvider>> {
        Ok(Box::new(SocketSourceProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct SocketSourceProvider {
    schema: Schema,
    source_config: SocketSourceConfig
}

impl SocketSourceProvider {
    pub fn new(schema: Schema, source_config: SocketSourceConfig) -> Self {
        Self { schema, source_config }
    }
}

impl SourceProvider for SocketSourceProvider {
    fn create_source(&self, task_context: TaskContext) -> Result<Box<dyn Source>> {
        let deserializer = self.source_config.decoding.build(self.schema.clone())?;
        let source = SocketSource::new(task_context, self.schema.clone(), self.source_config.hostname.clone(), self.source_config.port, deserializer)?;
        Ok(Box::new(source))
    }
}
