use log::{error, info};
use flexi_logger::with_thread;
use std::env;
use retl::config::{self, AppConfig};
use retl::execution::{self, NodeParser};

fn parse_and_execution_graph() -> retl::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let config_path = if args.len() > 0 {
        args[0].as_str()
    } else {
        "config/application.yaml"
    };
    println!("config path: {}", config_path);
    let config: AppConfig = config::parse_config(config_path).unwrap();
    let mut parser = NodeParser::new();
    let graph = parser.parse_node_graph(&config)?;
    graph.print_node_chains();
    execution::execution_graph(&graph)
    //Ok(())
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
