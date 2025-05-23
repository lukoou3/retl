use serde::{Deserialize, Serialize};
use crate::config::{SinkConfig, SinkProvider, TaskContext};
use crate::connector::Sink;
use crate::connector::print::{PrintMode, PrintSink};
use crate::codecs::{SerializerConfig};
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrintSinkConfig {
    #[serde(default)]
    print_mode: PrintMode,
    encoding: Box<dyn SerializerConfig>,
}

#[typetag::serde(name = "print")]
impl SinkConfig for PrintSinkConfig {
    fn build(&self, schema: Schema) -> crate::Result<Box<dyn SinkProvider>> {
        Ok(Box::new(PrintSinkProvider::new(schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct PrintSinkProvider {
    schema: Schema,
    sink_config: PrintSinkConfig
}

impl PrintSinkProvider {
    pub fn new(schema: Schema, sink_config: PrintSinkConfig) -> Self {
        Self {
            schema,
            sink_config
        }
    }
}

impl SinkProvider for PrintSinkProvider {
    fn create_sink(&self, task_context: TaskContext) -> crate::Result<Box<dyn Sink>> {
        let serializer = self.sink_config.encoding.build(self.schema.clone())?;
        Ok(Box::new(PrintSink::new(serializer, self.sink_config.print_mode, task_context)))
    }
}