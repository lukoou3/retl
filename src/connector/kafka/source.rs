use std::collections::HashMap;
use std::fmt::Debug;
use rdkafka::{ClientConfig, Message};
use rdkafka::consumer::{BaseConsumer, Consumer};
use crate::Result;
use crate::codecs::Deserializer;
use crate::connector::Source;
use crate::execution::Collector;
use crate::types::Schema;

pub struct KafkaSource {
    schema: Schema,
    topics: Vec<String>,
    consumer: BaseConsumer,
    deserializer: Box<dyn Deserializer>,
}

impl KafkaSource {
    pub fn new(schema: Schema, topics: Vec<String>, properties: HashMap<String, String>, deserializer: Box<dyn Deserializer>) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (k, v) in properties.into_iter() {
            config.set(k, v);
        }
        let consumer = config.create().map_err(|e| e.to_string())?;
        Ok(Self { schema, topics, consumer, deserializer, })
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

    fn run(&mut self, out: &mut dyn Collector) -> Result<()> {
        loop {
            let message =  self.consumer.poll(std::time::Duration::from_secs(1));
            match message {
                Some(Ok(message)) => {
                    // 处理消息
                    if let Some(payload) = message.payload() {
                        out.collect(self.deserializer.deserialize(payload)?)?;
                    }
                }
                Some(Err(e)) => return Err(e.to_string()),
                None => continue,
            }
        }

    }

}

