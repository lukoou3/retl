use rmpv::{encode, decode, Value, Utf8String};

fn test_write_value() -> Result<(), Box<dyn std::error::Error>> {
    // 创建一个 MsgPack 数据结构：{"name": "Alice", "age": 25}
    let value = Value::Map(vec![
        (
            Value::String(Utf8String::from("name")),
            Value::String(Utf8String::from("Alice")),
        ),
        (
            Value::String(Utf8String::from("age")),
            Value::Integer(25.into()),
        ),
    ]);

    // 序列化为 MsgPack 字节
    let mut buf = Vec::new();
    encode::write_value(&mut buf, &value)?;

    // 打印生成的字节
    println!("Serialized: {:?}", buf);
    println!("Serialized: {}", hex::encode(buf.as_slice()));

    Ok(())
}

fn test_read_value() -> Result<(), Box<dyn std::error::Error>> {
    // 假设这是 MsgPack 格式的字节数据
    // 示例数据：{"name": "Alice", "age": 25}
    let msgpack_bytes = vec![
        0x82, // map with 2 entries
        0xa4, 0x6e, 0x61, 0x6d, 0x65, // "name"
        0xa5, 0x41, 0x6c, 0x69, 0x63, 0x65, // "Alice"
        0xa3, 0x61, 0x67, 0x65, // "age"
        0x19, // 25 (uint8)
    ];

    // 从字节流中读取 MsgPack 数据
    let mut cursor = std::io::Cursor::new(&msgpack_bytes);
    let value = decode::read_value(&mut cursor)?;

    // 打印解析结果
    println!("Parsed: {:?}", value);

    // 访问具体字段
    if let Value::Map(map) = value {
        for (key, val) in map {
            match key {
                Value::String(s) => println!("Key: {}, Value: {:?}", s.to_string(), val),
                _ => println!("Non-string key: {:?}", key),
            }
        }
    }

    Ok(())
}


fn main() {
    test_write_value().unwrap();
    test_read_value().unwrap();
}