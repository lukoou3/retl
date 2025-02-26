use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::time::Duration;
use log::error;
use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;
use crate::Result;
use crate::codecs::Serializer;
use crate::connector::Sink;
use crate::data::Row;

pub struct KafkaSink {
    topic: String,
    producer: BaseProducer,
    serializer: Box<dyn Serializer>,
}

impl KafkaSink {
    pub fn new(topic: String, properties: HashMap<String, String>, serializer: Box<dyn Serializer>) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (k, v) in properties.into_iter() {
            config.set(k, v);
        }
        let producer = config.create().map_err(|e| e.to_string())?;
        Ok(Self { topic, producer, serializer, })
    }
}

impl Debug for KafkaSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("topic", &self.topic)
            .field("producer", &"<rdkafka::producer::BaseProducer>")
            .field("serializer", &self.serializer)
            .finish()
    }
}

impl Sink for KafkaSink {
    fn invoke(&mut self, row: &dyn Row) -> Result<()> {
        let bytes = self.serializer.serialize(row)?;
        let record: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(&self.topic).payload(bytes);
        match self.producer.send(record) {
            Ok(_) => {
                self.producer.poll(Duration::ZERO);
                Ok(())
            },
            Err((e, _)) => {
                if let KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) = e {
                    error!("Kafka queue full");
                    self.producer.poll(Duration::from_millis(100));
                    Ok(())
                } else {
                    error!("Kafka send error: {}", e);
                    Ok(())
                    //Err(e.to_string())
                }
            },
        }
    }

    fn close(&mut self) -> Result<()> {
        self.producer.flush(Duration::from_secs(5)).map_err(|e| e.to_string())
    }
}

