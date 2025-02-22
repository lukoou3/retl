use flexi_logger::with_thread;
use retl::config::{SinkConfig, SourceConfig};
use retl::connector::print::{PrintSinkConfig};
use retl::connector::faker::{FakerSourceConfig};
use retl::execution::{Collector, SinkCollector};
use retl::types::{DataType, Field, Schema};

fn main() {
    flexi_logger::Logger::try_with_str("info")
        .unwrap()
        .format(with_thread)
        .start()
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Long),
        Field::new("cate", DataType::String),
        Field::new("text", DataType::String),
        Field::new("count", DataType::Int),
    ]);
    let source_text = r#"
    {
        "fields": [
            {"name": "id", "type": "long", "min": 1, "max": 100},
            {"name": "cate", "type": "string", "options": ["a", "b", null, "c", "d"] },
            {"name": "text", "type": "string", "regex": "12[a-z]{2}" },
            {"name": "count", "type": "int", "min": 1, "max": 100}
        ],
        "rows_per_second": 2
    }
    "#;
    let sink_text = r#"
    {
        "print_mode": "log_warn",
        "encoding": {
            "codec": "csv"
        }
    }
    "#;

    let sink_config:PrintSinkConfig = serde_json::from_str(sink_text).unwrap();
    let source_config:FakerSourceConfig = serde_json::from_str(source_text).unwrap();

    let mut source = source_config.build(schema.clone()).unwrap().create_source().unwrap();
    let sink = sink_config.build(schema.clone()).unwrap().create_sink().unwrap();
    let mut sink_collector = SinkCollector::new(sink);
    sink_collector.open().unwrap();
    source.open().unwrap();
    source.run(&mut sink_collector);

}