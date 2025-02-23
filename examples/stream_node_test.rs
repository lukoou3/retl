use flexi_logger::with_thread;
use retl::config::{SinkConfig, SourceConfig, TransformConfig};
use retl::connector::print::{PrintSinkConfig};
use retl::connector::faker::{FakerSourceConfig};
use retl::execution::{Collector, SinkCollector, TransformCollector};
use retl::transform::QueryTransformConfig;
use retl::types::{DataType, Field, Schema};

fn main() {
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
    let out_schema = Schema::new(vec![
        Field::new("id", DataType::Int),
        Field::new("cate", DataType::String),
        Field::new("text", DataType::String),
        Field::new("in_bytes", DataType::Long),
        Field::new("out_bytes", DataType::Long),
        Field::new("bytes", DataType::Long),
    ]);
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
        "sql": "log_warn"
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

    let mut source = source_config.build(in_schema.clone()).unwrap().create_source().unwrap();
    let transform = transform_config.build(in_schema.clone()).unwrap().create_transform().unwrap();
    let sink = sink_config.build(out_schema.clone()).unwrap().create_sink().unwrap();
    let sink_collector = SinkCollector::new(sink);
    let mut transform_collector = TransformCollector::new(transform, Box::new(sink_collector));
    transform_collector.open().unwrap();
    source.open().unwrap();
    source.run(&mut transform_collector);

}