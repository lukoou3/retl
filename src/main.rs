use std::collections::HashMap;
use std::process::exit;
use clap::{Parser, Subcommand};
use log::{error, info};
use flexi_logger::with_thread;
use retl::execution::application;

#[derive(Parser)]
#[command(name = "retl", version, author, about, long_about = None)]
pub struct Cli {
    #[arg(short = 'l', default_value = "info",  global = true, value_parser = ["debug", "info", "warn", "error"])]
    log_level: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        config_file: String,
    },
    Sql {
        #[arg(short = 'e')]
        sql: Option<String>,
        #[arg(short = 'f')]
        filename: Option<String>,
    },
    Kafka {
        #[command(subcommand)]
        kafka_command: KafkaCommands
    },
    /// Vector Remap Language CLI
    Vrl(vrl::cli::Opts),
}

#[derive(Subcommand)]
enum KafkaCommands {
    ShowTopic {
        #[command(flatten)]
        common: KafkaCommonArgs,
        #[arg(long = "topic")]
        topic: String,
    },
    DescGroup {
        #[command(flatten)]
        common: KafkaCommonArgs,
        #[arg(long = "group")]
        group_id: String,
        #[arg(long = "topic")]
        topic: String,
    },
    ResetGroupOffsetLatest {
        #[command(flatten)]
        common: KafkaCommonArgs,
        #[arg(long = "group")]
        group_id: String,
        #[arg(long = "topic")]
        topic: String,
    },
    ResetGroupOffsetForTs {
        #[command(flatten)]
        common: KafkaCommonArgs,
        #[arg(long = "group")]
        group_id: String,
        #[arg(long = "topic")]
        topic: String,
        #[arg(long = "ts")]
        ts: i64,
    },
}

#[derive(Parser)]
pub struct KafkaCommonArgs {
    /// Kafka broker addresses (comma separated)
    #[arg(long = "brokers", default_value = "localhost:9092")]
    pub brokers: String,

    /// Kafka properties in key=value format
    #[arg(long = "prop", value_name = "KEY=VALUE", num_args = 1..)]
    pub props: Vec<String>,
}

impl KafkaCommonArgs {
    /// Convert props to HashMap
    pub fn props_map(&self) -> HashMap<String, String> {
        self.props.iter()
            .filter_map(|s| {
                let mut split = s.splitn(2, '=');
                match (split.next(), split.next()) {
                    (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                    _ => None,
                }
            })
            .collect()
    }
}

fn run_sql_command(sql: Option<String>, filename: Option<String>) {
    #[cfg(feature = "batch")]
    {
        match retl::batch::run_sql_command(sql, filename) {
            Ok(_) => (),
            Err(e) => error!("run sql error:{}", e),
        }
    }
    #[cfg(not(feature = "batch"))]
    {
        error!("batch feature not enabled");
    }
}

fn run_kafka_command(kafka_command: KafkaCommands) {
    #[cfg(feature = "kafka")]
    {
        match kafka_command {
            KafkaCommands::ShowTopic {  common, topic } => {
                let props = common.props_map();
                println!("show topic {}, brokers:{}, props:{:?}", topic, common.brokers, props);
                retl::connector::kafka::show_topic(&common.brokers, &topic, &props).unwrap();
            },
            KafkaCommands::DescGroup { common, group_id, topic } => {
                let props = common.props_map();
                println!("desc group, brokers:{}, props:{:?}, group:{}, topic:{}", common.brokers, props, group_id, topic);
                retl::connector::kafka::desc_group(&common.brokers, &topic, &group_id, &props).unwrap();
            },
            KafkaCommands::ResetGroupOffsetLatest { common, group_id, topic } => {
                let props = common.props_map();
                println!("reset group offset latest, brokers:{}, props:{:?}, group:{}, topic:{}", common.brokers, props, group_id, topic);
                retl::connector::kafka::reset_group_offset_latest(&common.brokers, &topic, &group_id, &props).unwrap();
                println!("reset group offset end");
                retl::connector::kafka::desc_group(&common.brokers, &topic, &group_id, &props).unwrap();
            },
            KafkaCommands::ResetGroupOffsetForTs { common, group_id, topic, ts } => {
                let props = common.props_map();
                println!("reset group offset for ts, brokers:{}, props:{:?}, group:{}, topic:{}, ts:{}", common.brokers, props, group_id, topic, ts);
                retl::connector::kafka::reset_group_offset_for_ts(ts, &common.brokers, &topic, &group_id, &props).unwrap();
                println!("reset group offset end");
                retl::connector::kafka::desc_group(&common.brokers, &topic, &group_id, &props).unwrap();
            },
        }
    }
    #[cfg(not(feature = "kafka"))]
    {
        error!("kafka feature not enabled");
    }
}

fn main() {
    let cli = Cli::parse();
    flexi_logger::Logger::try_with_str(cli.log_level)
        .unwrap()
        .format(with_thread)
        .start()
        .unwrap();
    match cli.command {
        Commands::Run { config_file } => {
            if let Err(e) = application::run_application(&config_file) {
                error!("execution error {}", e);
            } else {
                info!("execution success");
            }
        },
        Commands::Sql { sql, filename } => {
            run_sql_command(sql, filename);
        },
        Commands::Kafka { kafka_command} => {
            run_kafka_command(kafka_command);
        },
        Commands::Vrl(s) => {
            let functions = vrl::stdlib::all();
            let rst = vrl::cli::cmd::cmd(&s, functions);
            exit(rst);
        },
    }
}

