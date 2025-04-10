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
use crate::config::TaskContext;
use crate::connector::Sink;
use crate::data::Row;

pub struct KafkaSink {
    task_context: TaskContext,
    topic: String,
    producer: BaseProducer,
    serializer: Box<dyn Serializer>,
}

impl KafkaSink {
    pub fn new(task_context: TaskContext, topic: String, properties: HashMap<String, String>, serializer: Box<dyn Serializer>) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (k, v) in properties.into_iter() {
            config.set(k, v);
        }
        let producer = config.create().map_err(|e| e.to_string())?;
        Ok(Self { task_context, topic, producer, serializer, })
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
        self.task_context.base_iometrics.num_records_in_inc_by(1);
        let bytes = self.serializer.serialize(row)?;
        self.task_context.base_iometrics.num_records_out_inc_by(1);
        self.task_context.base_iometrics.num_bytes_out_inc_by(bytes.len() as u64);

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
        self.producer.flush(Duration::from_secs(30)).map_err(|e| e.to_string())
    }
}

