use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use chrono::Utc;
use crate::data::{empty_row, Row, Value};
use crate::datetime_utils::from_timestamp_micros_utc;
use crate::physical_expr::{BinaryExpr, Literal, PhysicalExpr};
use crate::types::DataType;

#[derive(Debug)]
pub struct CurrentTimestamp;

impl PhysicalExpr for CurrentTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Timestamp
    }

    fn eval(&self, _: &dyn Row) -> Value {
        Value::Long(Utc::now().timestamp_micros())
    }
}

#[derive(Debug)]
pub struct FromUnixTime {
    sec: Box<dyn PhysicalExpr>,
    format: Box<dyn PhysicalExpr>,
}

impl FromUnixTime {
    pub fn new(sec: Box<dyn PhysicalExpr>, format: Box<dyn PhysicalExpr>) -> FromUnixTime {
        FromUnixTime { sec, format }
    }
}

impl BinaryExpr for FromUnixTime {
    fn left(&self) -> &dyn PhysicalExpr {
        self.sec.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.format.as_ref()
    }

    fn null_safe_eval(&self, sec: Value, format: Value) -> Value {
        Value::String(Arc::new(from_timestamp_micros_utc(sec.get_long() * 1000_000).format(format.get_string()).to_string()))
    }
}

impl PhysicalExpr for FromUnixTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct FromUnixTimeMillis {
    sec: Box<dyn PhysicalExpr>,
    format: Box<dyn PhysicalExpr>,
}

impl FromUnixTimeMillis {
    pub fn new(sec: Box<dyn PhysicalExpr>, format: Box<dyn PhysicalExpr>) -> FromUnixTimeMillis {
        FromUnixTimeMillis { sec, format }
    }
}

impl BinaryExpr for FromUnixTimeMillis {
    fn left(&self) -> &dyn PhysicalExpr {
        self.sec.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.format.as_ref()
    }

    fn null_safe_eval(&self, sec: Value, format: Value) -> Value {
        Value::String(Arc::new(from_timestamp_micros_utc(sec.get_long() * 1000).format(format.get_string()).to_string()))
    }
}

impl PhysicalExpr for FromUnixTimeMillis {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct ToUnixTimestamp {
    time_expr: Box<dyn PhysicalExpr>,
    format: Box<dyn PhysicalExpr>,
}

impl ToUnixTimestamp {
    pub fn new(time_expr: Box<dyn PhysicalExpr>, format: Box<dyn PhysicalExpr>) -> ToUnixTimestamp {
        ToUnixTimestamp { time_expr, format }
    }
}

impl BinaryExpr for ToUnixTimestamp {
    fn left(&self) -> &dyn PhysicalExpr {
        self.time_expr.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.format.as_ref()
    }

    fn null_safe_eval(&self, time_expr: Value, format: Value) -> Value {
        match self.time_expr.data_type() {
            DataType::Timestamp => {
                Value::Long(time_expr.get_long() / 1000_000)
            },
            DataType::String => {
                match chrono::NaiveDateTime::parse_from_str(time_expr.get_string(), format.get_string()) {
                    Ok(dt) => Value::Long(dt.and_utc().timestamp()),
                    Err(_) => Value::Null,
                }
            },
            _ => panic!("ToUnixTimestamp: time_expr must be Timestamp or String"),
        }
    }
}

impl PhysicalExpr for ToUnixTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct ToUnixTimestampMillis {
    time_expr: Box<dyn PhysicalExpr>,
    format: Box<dyn PhysicalExpr>,
}

impl ToUnixTimestampMillis {
    pub fn new(time_expr: Box<dyn PhysicalExpr>, format: Box<dyn PhysicalExpr>) -> ToUnixTimestampMillis {
        ToUnixTimestampMillis { time_expr, format }
    }
}

impl BinaryExpr for ToUnixTimestampMillis {
    fn left(&self) -> &dyn PhysicalExpr {
        self.time_expr.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.format.as_ref()
    }

    fn null_safe_eval(&self, time_expr: Value, format: Value) -> Value {
        match self.time_expr.data_type() {
            DataType::Timestamp => {
                Value::Long(time_expr.get_long() / 1000)
            },
            DataType::String => {
                match chrono::NaiveDateTime::parse_from_str(time_expr.get_string(), format.get_string()) {
                    Ok(dt) => Value::Long(dt.and_utc().timestamp_millis()),
                    Err(_) => Value::Null,
                }
            },
            _ => panic!("ToUnixTimestampMillis: time_expr must be Timestamp or String"),
        }
    }
}

impl PhysicalExpr for ToUnixTimestampMillis {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

const TRUNC_INVALID: i8 = -1;
const TRUNC_TO_MICROSECOND: i8 = 0;
const TRUNC_TO_MILLISECOND: i8 = 1;
const TRUNC_TO_SECOND: i8 = 2;
const TRUNC_TO_MINUTE: i8 = 3;
const TRUNC_TO_HOUR: i8 = 4;
const TRUNC_TO_DAY: i8 = 5;

#[derive(Debug)]
pub struct TruncTimestamp {
    format: Box<dyn PhysicalExpr>,
    timestamp: Box<dyn PhysicalExpr>,
    level_static: Option<i8>,
}

impl TruncTimestamp {
    pub fn new(format: Box<dyn PhysicalExpr>, timestamp: Box<dyn PhysicalExpr>) -> TruncTimestamp {
        let level_static = if let Some(literal) = format.as_any().downcast_ref::<Literal>() {
            let value = literal.eval(empty_row());
            if value.is_null() {
                Some(TRUNC_INVALID)
            } else {
                Some(TruncTimestamp::parse_trunc_level(value.get_string()))
            }
        } else {
            None
        };
        TruncTimestamp { format, timestamp, level_static}
    }
    
    fn parse_trunc_level(format: &str) -> i8 {
        match format.to_lowercase().as_str() {
            "microsecond" => TRUNC_TO_MICROSECOND,
            "millisecond" => TRUNC_TO_MILLISECOND,
            "second" => TRUNC_TO_SECOND,
            "minute" => TRUNC_TO_MINUTE,
            "hour" => TRUNC_TO_HOUR,
            "day" => TRUNC_TO_DAY,
            _ => TRUNC_INVALID,
        }
    }
}

impl PhysicalExpr for TruncTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Timestamp
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let level = if let Some(level) = &self.level_static {
            *level
        } else {
            let format = self.format.eval(input);
            if format.is_null() {
                return Value::Null;
            }
            TruncTimestamp::parse_trunc_level(format.get_string())
        };

        let timestamp = self.timestamp.eval(input);
        if timestamp.is_null() {
            return Value::Null;
        }
        let micros = timestamp.get_long();
        match level {
            TRUNC_TO_MICROSECOND => Value::Long(micros),
            TRUNC_TO_MILLISECOND => Value::Long(micros / 1_000 * 1_000),
            TRUNC_TO_SECOND => Value::Long(micros / 1_000_000 * 1_000_000),
            TRUNC_TO_MINUTE => Value::Long(micros / 60_000_000 * 60_000_000),
            TRUNC_TO_HOUR => Value::Long(micros / 3_600_000_000 * 3_600_000_000),
            TRUNC_TO_DAY => Value::Long(micros / 86_400_000_000 * 86_400_000_000),
            _ => Value::Long(micros),
        }
    }
}

#[derive(Debug)]
pub struct TimestampFloor {
    timestamp: Box<dyn PhysicalExpr>,
    interval: i64,
}

impl TimestampFloor {
    pub fn new(timestamp: Box<dyn PhysicalExpr>, interval: i64) -> TimestampFloor {
        TimestampFloor { timestamp, interval }
    }
}

impl PhysicalExpr for TimestampFloor {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Timestamp
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let timestamp = self.timestamp.eval(input);
        if timestamp.is_null() {
            return Value::Null;
        }
        let micros = timestamp.get_long();
        let interval = self.interval;
        Value::Long(micros / interval * interval)
    }
}
