use crate::connector::Sink;
use crate::data::Row;
use crate::Result;
use crate::format::Serialization;
use log::{debug, info, warn};
use std::str::FromStr;

#[derive(Debug)]
pub struct PrintSink {
    serialization: Box<dyn Serialization>,
    print_mode: PrintMode,
}

impl PrintSink {
    pub fn new(serialization: Box<dyn Serialization>, print_mode: PrintMode) -> Self {
        Self {serialization, print_mode }
    }
}

impl Clone for PrintSink {
    fn clone(&self) -> Self {
        Self::new(self.serialization.clone_box(), self.print_mode.clone())
    }
}

impl Sink for PrintSink {
    fn name(&self) -> &str {
        "PrintSink"
    }

    fn invoke(&mut self, row: &dyn Row) {
        match self.serialization.serialize(row) {
            Ok(bytes) => match self.print_mode {
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
    }
}


#[derive(Debug, Clone)]
pub enum PrintMode {
    Stdout,
    Debug,
    LogInfo,
    LogWarn,
    Null
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