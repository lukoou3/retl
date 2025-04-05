use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use rdkafka::{ClientConfig, Message};
use rdkafka::consumer::{BaseConsumer, Consumer};
use crate::Result;
use crate::codecs::Deserializer;
use crate::config::TaskContext;
use crate::connector::Source;
use crate::execution::{Collector, PollStatus};
use crate::types::Schema;

static POLL_TIMEOUT: Duration = Duration::from_millis(200);

pub struct KafkaSource {
    task_context: TaskContext,
    schema: Schema,
    topics: Vec<String>,
    consumer: BaseConsumer,
    deserializer: Box<dyn Deserializer>,
}

impl KafkaSource {
    pub fn new(task_context: TaskContext, schema: Schema, topics: Vec<String>, properties: HashMap<String, String>, deserializer: Box<dyn Deserializer>) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (k, v) in properties.into_iter() {
            config.set(k, v);
        }
        let consumer = config.create().map_err(|e| e.to_string())?;
        Ok(Self { task_context, schema, topics, consumer, deserializer, })
    }

    fn run(&mut self, out: &mut dyn Collector, terminated: Arc<AtomicBool>) -> Result<()> {
        loop {
            if terminated.load(Ordering::Acquire) {
                return Ok(());
            }
            let message =  self.consumer.poll(std::time::Duration::from_secs(1));
            match message {
                Some(Ok(message)) => {
                    // 处理消息
                    if let Some(payload) = message.payload() {
                        self.task_context.base_iometrics.num_records_in_inc_by(1);
                        self.task_context.base_iometrics.num_bytes_in_inc_by(payload.len() as u64);
                        let row = self.deserializer.deserialize(payload)?;
                        self.task_context.base_iometrics.num_records_out_inc_by(1);
                        out.collect(row)?;

                    }
                }
                Some(Err(e)) => return Err(e.to_string()),
                None => continue,
            }
        }
    }

}

impl Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("topics", &self.topics)
            .field("consumer", &"<rdkafka::consumer::BaseConsumer>")
            .field("deserializer", &self.deserializer)
            .finish()
    }
}

impl Source for KafkaSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        self.consumer.subscribe(self.topics.iter().map(|t| t.as_str()).collect::<Vec<_>>().as_slice()).map_err(|e| e.to_string())
    }

    fn poll_next(&mut self, out: &mut dyn Collector) -> Result<PollStatus> {
        let message =  self.consumer.poll(POLL_TIMEOUT);
        match message {
            Some(Ok(message)) => {
                // 处理消息
                if let Some(payload) = message.payload() {
                    self.task_context.base_iometrics.num_records_in_inc_by(1);
                    self.task_context.base_iometrics.num_bytes_in_inc_by(payload.len() as u64);
                    let row = self.deserializer.deserialize(payload)?;
                    self.task_context.base_iometrics.num_records_out_inc_by(1);
                    out.collect(row)?;
                }
            }
            Some(Err(e)) => return Err(e.to_string()),
            None => (),
        }
        Ok(PollStatus::More)
    }

}

