use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::Duration;

/*
zkServer.sh start
zkServer.sh status
bin/kafka-server-start.sh -daemon config/server.properties
kafka-topics.sh --bootstrap-server 192.168.216.86:9092 --list
bin/kafka-console-producer.sh  --bootstrap-server 192.168.216.86:9092 --topic logs
bin/kafka-console-consumer.sh  --bootstrap-server 192.168.216.86:9092 --topic logs
bin/kafka-consumer-groups.sh --bootstrap-server 192.168.216.86:9092  --describe --group test-group
*/
fn main() {
    // 创建 Kafka 生产者
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "192.168.216.86:9092")
        .set("batch.size", "16384") // 设置批量大小
        .set("linger.ms", "100")    // 设置等待时间
        .set("compression.type", "lz4") // lz4, snappy
        .create()
        .expect("Producer creation failed");

    // 发送多条消息
    for i in 0..100 {
        let value = format!("{} : {}", chrono::Local::now(), i);

        let record: BaseRecord<'_, String, String> = BaseRecord::to("logs")
            .payload(&value);

        if let Err(x) = producer.send(record) {
            println!("{} Producer send failed {:?}", chrono::Local::now(), x.1);
        }

        println!("{} Sent message in : {}",  chrono::Local::now(), value);
        std::thread::sleep(Duration::from_millis(10));
    }

    // 刷新生产者，确保所有消息都已发送
    producer.flush(Duration::from_secs(10)).unwrap();
}
