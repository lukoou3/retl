use chrono::{Local, Utc};
use serde::{Deserialize, Serialize};
use crate::connector::faker::Faker;
use crate::data::Value;
use crate::types::DataType;

#[derive(Debug)]
pub struct TimestampFaker {
    pub unit: TimestampUnit,
    pub timestamp_type: TimestampType,
}

impl TimestampFaker {
    pub fn new(unit: TimestampUnit, timestamp_type: TimestampType) -> Self {
        TimestampFaker { unit, timestamp_type, }
    }
}

impl Faker for TimestampFaker {
    fn data_type(&self) -> DataType {
        match self.timestamp_type {
            TimestampType::Number => DataType::Long,
            TimestampType::Datetime => DataType::Timestamp,
        }
    }

    fn gene_value(&mut self) -> Value {
        match self.timestamp_type {
            TimestampType::Number => {
                match self.unit {
                    TimestampUnit::Seconds => Value::Long(Utc::now().timestamp()),
                    TimestampUnit::Millis => Value::Long(Utc::now().timestamp_millis()),
                    TimestampUnit::Micros => Value::Long(Utc::now().timestamp_micros()),
                }
            },
            TimestampType::Datetime => {
                match self.unit {
                    TimestampUnit::Seconds => {
                        Value::Long(Utc::now().timestamp() * 1000_000)
                    },
                    TimestampUnit::Millis => {
                        Value::Long(Utc::now().timestamp_millis() * 1000)
                    },
                    TimestampUnit::Micros => {
                        Value::Long(Utc::now().timestamp_micros())
                    },
                }
            },
        }
    }
}

#[derive(Clone, Debug, Copy, Deserialize, Serialize)]
pub enum TimestampUnit {
    #[serde(rename = "seconds")]
    Seconds,
    #[serde(rename = "millis")]
    Millis,
    #[serde(rename = "micros")]
    Micros,
}

impl Default for TimestampUnit {
    fn default() -> Self {
        TimestampUnit::Seconds
    }
}

#[derive(Clone, Debug, Copy, Deserialize, Serialize)]
pub enum TimestampType {
    #[serde(rename = "number")]
    Number,
    #[serde(rename = "datetime")]
    Datetime,
}

impl Default for TimestampType {
    fn default() -> Self {
        TimestampType::Number
    }
}

#[derive(Debug)]
pub struct FormatTimestampFaker {
    pub format: String,
    pub utc: bool,
}

impl Faker for FormatTimestampFaker {
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn gene_value(&mut self) -> Value {
        if self.utc {
            Value::string(Utc::now().format(&self.format).to_string())
        } else {
            Value::string(Local::now().format(&self.format).to_string())
        }
    }
}