use std::time::Duration;
use chrono::{DateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;

fn consumer_message() {
    // 创建 Kafka 消费者
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", "my_group")
        .set("bootstrap.servers", "192.168.216.86:9092")
        .create()
        .expect("Consumer creation failed");

    // 订阅 Kafka 主题
    consumer.subscribe(&["logs"]).expect("Failed to subscribe to topic");

    // 消费消息
    loop {
        // 轮询消息
        let message = consumer.poll(std::time::Duration::from_secs(1));

        match message {
            Some(Ok(message)) => {
                // 处理消息
                if let Some(payload) = message.payload() {
                    let payload_str = String::from_utf8_lossy(payload);
                    println!("{} Received message: {}", chrono::Local::now(), payload_str);
                }
            }
            Some(Err(e)) => {
                eprintln!("Error while receiving message: {:?}", e);
            }
            None => {
                // 没有消息时继续轮询
                continue;
            }
        }
    }
}

fn consumer_offset() -> Result<(), Box<dyn std::error::Error>> {
    let topic = "logs";
    let consumer: BaseConsumer = ClientConfig::new()
        //.set("group.id", "my_group")
        .set("bootstrap.servers", "192.168.216.86:9092")
        .create()?;
    // 1. 获取 Topic 的 Offset（最早和最新）
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(5))?;
    if let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) {
        for partition in t.partitions() {
            let (low, high) = consumer
                .fetch_watermarks(topic, partition.id(), Duration::from_secs(5))?;
            println!(
                "Partition {}: Low offset: {}, High offset: {}",
                partition.id(),
                low,
                high
            );
        }
    }

    // 2. 查询特定时间点的 Offset
    let timestamp_str = "2025-05-01T08:35:00Z";
    let timestamp: DateTime<Utc> = timestamp_str.parse()?;
    let timestamp_ms = timestamp.timestamp_millis();

    let topic_metadata = metadata.topics().first().ok_or_else(|| {
        rdkafka::error::KafkaError::MetadataFetch(rdkafka::types::RDKafkaErrorCode::UnknownTopic)
    })?;

    let mut tpl = TopicPartitionList::new();
    // 为每个分区设置时间戳查询
    for partition in topic_metadata.partitions() {
        tpl.add_partition_offset(topic, partition.id(), rdkafka::Offset::Offset(timestamp_ms))?;
    }

    // 执行查询
    let offsets = consumer.offsets_for_times(tpl, Duration::from_secs(5)).unwrap();

    //let offsets = consumer.offsets_for_timestamp(timestamp_ms, Duration::from_secs(5)).unwrap();
    for elem in offsets.elements() {
        if let Some(offset) = elem.offset().to_raw() {
            println!(
                "Partition {} offset at timestamp {}: {}",
                elem.partition(),
                timestamp_str,
                offset
            );
        } else {
            println!(
                "No offset found for partition {} at timestamp {}",
                elem.partition(),
                timestamp_str
            );
        }
    }

    Ok(())
}

fn main() {
    //consumer_message();
    consumer_offset().unwrap();
}
