use serde::{Deserialize, Serialize};
use crate::Result;
use crate::codecs::{DeserializerConfig, SerializerConfig};
use crate::config::{SinkConfig, SinkProvider, SourceConfig, SourceProvider, TaskContext};
use crate::connector::{Sink, Source};
use crate::connector::udp::sink::UdpSink;
use crate::connector::udp::source::UdpSource;
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpSourceConfig {
    #[serde(default = "default_source_hostname")]
    hostname: String,
    port: u16,
    #[serde(default = "default_buffer_size")]
    buffer_size: i32,
    decoding: Box<dyn DeserializerConfig>,
}

#[typetag::serde(name = "udp")]
impl SourceConfig for UdpSourceConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn SourceProvider>> {
        Ok(Box::new(UdpSourceProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct UdpSourceProvider {
    schema: Schema,
    source_config: UdpSourceConfig
}

impl UdpSourceProvider {
    pub fn new(schema: Schema, source_config: UdpSourceConfig) -> Self {
        Self { schema, source_config }
    }
}

impl SourceProvider for UdpSourceProvider {
    fn create_source(&self, task_context: TaskContext) -> Result<Box<dyn Source>> {
        let udp_source = UdpSource::new(
            task_context,
            self.schema.clone(),
            self.source_config.hostname.clone(),
            self.source_config.port,
            self.source_config.buffer_size as usize,
            self.source_config.decoding.build(self.schema.clone())?
        )?;
        Ok(Box::new(udp_source))
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpSinkConfig {
    hostname: String,
    port: u16,
    encoding: Box<dyn SerializerConfig>,
}

#[typetag::serde(name = "udp")]
impl SinkConfig for UdpSinkConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn SinkProvider>> {
        Ok(Box::new(UdpSinkProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct UdpSinkProvider {
    schema: Schema,
    sink_config: UdpSinkConfig
}

impl UdpSinkProvider {
    pub fn new(schema: Schema, sink_config: UdpSinkConfig) -> Self {
        Self { schema, sink_config }
    }
}

impl SinkProvider for UdpSinkProvider {
    fn create_sink(&self, task_context: TaskContext) -> Result<Box<dyn Sink>> {
        let serializer = self.sink_config.encoding.build(self.schema.clone())?;
        Ok(Box::new(UdpSink::new(
            task_context,
            self.sink_config.hostname.clone(),
            self.sink_config.port,
            serializer,
        )?))
    }
}

fn default_source_hostname() -> String {
    "0.0.0.0".to_string()
}

fn default_buffer_size() -> i32 {
    65536
}
