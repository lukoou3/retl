use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::Result;
use crate::codecs::{DeserializerConfig, SerializerConfig};
use crate::config::{SinkConfig, SinkProvider, SourceConfig, SourceProvider, TaskContext};
use crate::connector::kafka::{KafkaSink, KafkaSource};
use crate::connector::{Sink, Source};
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSourceConfig {
    topics: Vec<String>,
    properties: HashMap<String, String>,
    decoding: DeserializerConfig,
}

#[typetag::serde(name = "kafka")]
impl SourceConfig for KafkaSourceConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn SourceProvider>> {
        Ok(Box::new(KafkaSourceProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSourceProvider {
    schema: Schema,
    source_config: KafkaSourceConfig
}

impl KafkaSourceProvider {
    pub fn new(schema: Schema, source_config: KafkaSourceConfig) -> Self {
        Self {
            schema,
            source_config
        }
    }
}

impl SourceProvider for KafkaSourceProvider {
    fn create_source(&self, task_context: TaskContext) -> crate::Result<Box<dyn Source>> {
        let kafka_source = KafkaSource::new(
            task_context,
            self.schema.clone(),
            self.source_config.topics.clone(),
            self.source_config.properties.clone(),
            self.source_config.decoding.build(self.schema.clone())?
        )?;
        Ok(Box::new(kafka_source))
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    topic: String,
    properties: HashMap<String, String>,
    encoding: SerializerConfig,
}

#[typetag::serde(name = "kafka")]
impl SinkConfig for KafkaSinkConfig {
    fn build(&self, schema: Schema) -> crate::Result<Box<dyn SinkProvider>> {
        Ok(Box::new(KafkaSinkProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSinkProvider {
    schema: Schema,
    sink_config: KafkaSinkConfig
}

impl KafkaSinkProvider {
    pub fn new(schema: Schema, sink_config: KafkaSinkConfig) -> Self {
        Self {
            schema,
            sink_config
        }
    }
}

impl SinkProvider for KafkaSinkProvider {
    fn create_sink(&self, task_context: TaskContext) -> crate::Result<Box<dyn Sink>> {
        let serializer = self.sink_config.encoding.build(self.schema.clone())?;
        let kafka_sink = KafkaSink::new(task_context, self.sink_config.topic.clone(), self.sink_config.properties.clone(), serializer)?;
        Ok(Box::new(kafka_sink))
    }
}