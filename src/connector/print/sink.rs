use crate::connector::Sink;
use crate::data::Row;
use crate::Result;
use crate::codecs::{JsonSerializer, Serializer};
use crate::types::Schema;
use log::{debug, info, warn};
use std::str::FromStr;
use config::{Config, Value};
use config::{Value as ConfigValue};
use serde::{Deserialize, Serialize};
use typetag::serde;
use crate::config::TaskContext;

#[derive(Debug)]
pub struct PrintSink {
    serializer: Box<dyn Serializer>,
    print_mode: PrintMode,
    task_context: TaskContext,
}

impl PrintSink {
    pub fn new(serializer: Box<dyn Serializer>, print_mode: PrintMode, task_context: TaskContext) -> Self {
        Self {serializer, print_mode, task_context }
    }
}

impl Sink for PrintSink {
    fn invoke(&mut self, row: &dyn Row) -> Result<()>  {
        self.task_context.base_iometrics.num_records_in_inc_by(1);
        match self.serializer.serialize(row) {
            Ok(bytes) => {
                self.task_context.base_iometrics.num_records_out_inc_by(1);
                self.task_context.base_iometrics.num_bytes_out_inc_by(bytes.len() as u64);
                match self.print_mode {
                    PrintMode::Stdout => {
                        println!("{}", String::from_utf8_lossy(bytes));
                    },
                    PrintMode::Debug => {
                        debug!("{}", String::from_utf8_lossy(bytes));
                    },
                    PrintMode::LogInfo => {
                        info!("{}", String::from_utf8_lossy(bytes));
                    },
                    PrintMode::LogWarn => {
                        warn!("{}", String::from_utf8_lossy(bytes));
                    },
                    PrintMode::Null => {}
                }
            },
            Err(err) => match self.print_mode {
                PrintMode::Stdout => {
                    println!("{}", err);
                },
                PrintMode::Debug => {
                    debug!("{}", err);
                },
                PrintMode::LogInfo => {
                    info!("{}", err);
                },
                PrintMode::LogWarn => {
                    warn!("{}", err);
                },
                PrintMode::Null => {}
            }
        }
        Ok(())
    }
}


#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrintMode {
    Stdout,
    Debug,
    LogInfo,
    LogWarn,
    Null
}

impl Default for PrintMode {
    fn default() -> Self {
        PrintMode::Stdout
    }
}

impl FromStr for PrintMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "stdout" => Ok(PrintMode::Stdout),
            "debug" => Ok(PrintMode::Debug),
            "log_info" => Ok(PrintMode::LogInfo),
            "log_warn" => Ok(PrintMode::LogWarn),
            "null" => Ok(PrintMode::Null),
            _ => Err("Invalid PrintMode".to_string()),
        }
    }
}
