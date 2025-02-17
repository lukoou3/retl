use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;

fn main() {
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