use std::collections::HashMap;
use std::time::Duration;
use anyhow::anyhow;
use rdkafka::{ClientConfig, TopicPartitionList};
use rdkafka::consumer::{BaseConsumer, Consumer};

pub fn show_topic(brokers: &str, topic: &str, props: &HashMap<String, String>) -> anyhow::Result<()> {
    let consumer = create_consumer(brokers, None, props)?;
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))?;
    if let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) {
        for partition in t.partitions() {
            let (low, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::from_secs(5))?;
            println!("Partition {}: Low offset: {}, High offset: {}", partition.id(), low, high );
        }
    } else {
        println!("not find topic:{}", topic)
    }

    Ok(())
}

pub fn reset_group_offset_latest(brokers: &str, topic: &str, group_id: &str, props: &HashMap<String, String>) -> anyhow::Result<()> {
    let consumer = create_consumer(brokers, Some(group_id), props)?;
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))?;
    if let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) {
        let mut tpl = TopicPartitionList::new();
        for partition in t.partitions() {
            let (_, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::from_secs(5))?;
            let offset = rdkafka::Offset::Offset(high);
            tpl.add_partition_offset(topic, partition.id(), offset)?;
        }
        consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;
    } else {
        println!("not find topic:{}", topic)
    }

    Ok(())
}

pub fn reset_group_offset_for_ts(ts: i64, brokers: &str, topic: &str, group_id: &str, props: &HashMap<String, String>) -> anyhow::Result<()> {
    let consumer = create_consumer(brokers, Some(group_id), props)?;
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))?;
    let topic_metadata = metadata.topics()
        .get(0).ok_or_else(|| anyhow!("No topic found"))?;
    let mut tpl = TopicPartitionList::new();
    // 为每个分区设置时间戳查询
    for partition in topic_metadata.partitions() {
        tpl.add_partition_offset(topic, partition.id(), rdkafka::Offset::Offset(ts))?;
    }
    // 执行查询
    let offsets = consumer.offsets_for_times(tpl, Duration::from_secs(10))?;
    let mut tpl = TopicPartitionList::new();
    for elem in offsets.elements() {
        if let Some(offset) = elem.offset().to_raw() {
            tpl.add_partition_offset(topic, elem.partition(), rdkafka::Offset::Offset(offset))?;
            println!("Partition {} offset at timestamp {}: {}", elem.partition(), ts, offset );
        } else {
            println!("No offset found for partition {} at timestamp {}", elem.partition(), ts);
        }
    }
    consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;
    Ok(())
}

fn create_consumer(brokers: &str, group_id: Option<&str>, props: &HashMap<String, String>) -> anyhow::Result<BaseConsumer> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers).set("enable.auto.commit", "false");
    for v in group_id {
        config.set("group.id", v);
    }
    for (k, v) in props {
        config.set(k.clone(), v.clone());
    }
    let consumer = config.create()?;
    Ok(consumer)
}