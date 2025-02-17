use serde_json::{Serializer as JsonSerializer, Value};
use serde_json::Deserializer as JsonDeserializer;
use serde::ser::SerializeMap;
use std::io::{self, Write};
use serde::Serializer;

fn test_write_map() -> serde_json::Result<()> {
    let mut bytes = Vec::new();
    for _ in 0..10 {
        let mut serializer: serde_json::Serializer<&mut Vec<u8>> = JsonSerializer::new(&mut bytes);
        serializer.serialize_i32(1)?;
        let mut compound = serializer.serialize_map(None)?;
        for i in 0..6  {
            let k = format!("key_{}", i);
            let v = format!("value_{}", i);
            compound.serialize_key(&k)?;
            if i % 2 == 0 {
                compound.serialize_value(&v)?;
            } else {
                compound.serialize_value(&i)?;
            }
        }
        compound.end();
        println!("{}", String::from_utf8(bytes.clone()).unwrap());
        let stream = JsonDeserializer::from_slice(&bytes).into_iter::<Value>();
        // 遍历并处理每个键值对
        for item in stream {
            let item = item?;
            println!(" Value: {}", item);
        }
        bytes.clear();
    }
    Ok(())
}


// simd_json只用来高效解析json
fn test_parse_json_zero_copy() {
    /*use simd_json::{BorrowedValue, Error};
    fn parse_json_zero_copy(json_data: &mut [u8]) -> Result<(), Error> {


        // 将 JSON 数据解析为 BorrowedValue
        let value: BorrowedValue = simd_json::to_borrowed_value(json_data)?;

        // 遍历 JSON 对象的字段
        if let BorrowedValue::Object(map) = value {
            for (key, val) in map.iter() {
                println!("Key: {}, Value: {}", key, val);
            }
        }

        Ok(())
    }

    let mut json_data = br#"
        {
            "name": "Alice",
            "age": 30,
            "city": "New York"
        }
    "#.to_vec();

    if let Err(err) = parse_json_zero_copy(json_data.as_mut_slice()) {
        println!("Error during parsing: {}", err);
    }*/
}

fn main() {
    test_write_map().unwrap();
    let v: Value = serde_json::from_str("{}").unwrap();
    // test_parse_json_zero_copy();
}
