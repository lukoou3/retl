use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use flexi_logger::with_thread;
use retl::config::{SinkConfig, SourceConfig, TaskContext, TransformConfig};
use retl::connector::print::{PrintSinkConfig};
use retl::connector::faker::{FakerSourceConfig};
use retl::execution::{Collector, PollStatus, SinkCollector, TransformCollector};
use retl::transform::QueryTransformConfig;
use retl::types::{DataType, Field, Schema};

fn main() {
    println!("Size of VecDeque<i32>: {} bytes", size_of::<VecDeque<i32>>());

    flexi_logger::Logger::try_with_str("info")
        .unwrap()
        .format(with_thread)
        .start()
        .unwrap();

    let in_schema = Schema::new(vec![
        Field::new("id", DataType::Int),
        Field::new("cate", DataType::String),
        Field::new("text", DataType::String),
        Field::new("in_bytes", DataType::Long),
        Field::new("out_bytes", DataType::Long),
    ]);
    /*let out_schema = Schema::new(vec![
        Field::new("id", DataType::Int),
        Field::new("cate", DataType::String),
        Field::new("text", DataType::String),
        Field::new("in_bytes", DataType::Long),
        Field::new("out_bytes", DataType::Long),
        Field::new("bytes", DataType::Long),
    ]);*/
    let source_text = r#"
    {
        "fields": [
            {"name": "id", "type": "int", "min": 1, "max": 1000000, "random": false},
            {"name": "cate", "type": "string", "options": ["a", "b", null, "c", "d"] },
            {"name": "text", "type": "string", "regex": "12[a-z]{2}" },
            {"name": "in_bytes", "type": "int", "min": 100, "max": 10000},
            {"name": "out_bytes", "type": "int", "min": 100, "max": 10000}
        ],
        "number_of_rows": 10000,
        "millis_per_row": 200
    }
    "#;
    let transform_text = r#"
    {
        "sql": "select id, cate, text, in_bytes, out_bytes, (in_bytes + out_bytes) bytes, (10 + out_bytes) bytes2 from tbl"
    }
    "#;
    let sink_text = r#"
    {
        "print_mode": "log_warn",
        "encoding": {
            "codec": "json"
        }
    }
    "#;

    let sink_config: PrintSinkConfig = serde_json::from_str(sink_text).unwrap();
    let transform_config: QueryTransformConfig = serde_json::from_str(transform_text).unwrap();
    let source_config: FakerSourceConfig = serde_json::from_str(source_text).unwrap();

    let mut source = source_config.build(in_schema).unwrap().create_source(TaskContext::default()).unwrap();
    let transform = transform_config.build(source.schema().clone()).unwrap().create_transform(TaskContext::default()).unwrap();
    let sink = sink_config.build(transform.schema().clone()).unwrap().create_sink(TaskContext::default()).unwrap();
    let sink_collector = SinkCollector::new(sink);
    let mut transform_collector = TransformCollector::new(transform, Box::new(sink_collector));
    transform_collector.open().unwrap();
    source.open().unwrap();
    loop {
        match source.poll_next(&mut transform_collector).unwrap() {
            PollStatus::More => continue,
            PollStatus::End => break,
        }
    }
}