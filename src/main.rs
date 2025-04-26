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
        }
    }
}

