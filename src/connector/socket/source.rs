use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use crate::Result;
use crate::codecs::Deserializer;
use crate::config::TaskContext;
use crate::connector::Source;
use crate::execution::{Collector, PollStatus};
use crate::types::Schema;

#[derive(Debug)]
pub struct SocketSource {
    task_context: TaskContext,
    schema: Schema,
    reader: BufReader<TcpStream>,
    line: String,
    deserializer: Box<dyn Deserializer>,
}

impl SocketSource {
    pub fn new(task_context: TaskContext, schema: Schema, hostname: String, port: u16, deserializer: Box<dyn Deserializer>) -> Result<Self> {
        let stream = TcpStream::connect((hostname, port)).map_err(|e| e.to_string())?;
        let reader = BufReader::new(stream);
        let line = String::new();
        Ok(Self { task_context, schema, reader, line, deserializer })
    }
}

impl Source for SocketSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll_next(&mut self, out: &mut dyn Collector) -> Result<PollStatus> {
        self.line.clear();
        match self.reader.read_line(&mut self.line) {
            Ok(0) => Ok(PollStatus::End),
            Ok(_) => {
                let line = self.line.trim();
                if line.is_empty() {
                    return Ok(PollStatus::More);
                }
                let bytes = line.as_bytes();
                let row = self.deserializer.deserialize(bytes)?;
                out.collect(row)?;
                Ok(PollStatus::More)
            },
            Err(e) => Err(e.to_string())
        }
    }

    fn close(&mut self) -> Result<()> {
        let stream = self.reader.get_ref();
        stream.shutdown(std::net::Shutdown::Both).map_err(|e| e.to_string())
    }
}
