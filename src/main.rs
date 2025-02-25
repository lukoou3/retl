use log::{error, info};
use flexi_logger::with_thread;
use retl::config::{self, AppConfig};
use retl::execution::{self, NodeParser};

fn parse_and_execution_graph() -> retl::Result<()> {
    let config_path = "config/application.yaml";
    let config: AppConfig = config::parse_config(config_path).unwrap();
    let mut parser = NodeParser::new();
    let graph = parser.parse_node_graph(&config)?;
    execution::execution_graph(&graph)
}
fn main() {
    flexi_logger::Logger::try_with_str("info")
        .unwrap()
        .format(with_thread)
        .start()
        .unwrap();
    if let Err(e) = parse_and_execution_graph() {
        error!("execution error {}", e);
    } else {
        info!("execution success")
    }
}
