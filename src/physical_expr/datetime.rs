use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use chrono::Utc;
use crate::data::{empty_row, Row, Value};
use crate::datetime_utils::from_timestamp_micros_utc;
use crate::physical_expr::{Literal, PhysicalExpr};
use crate::types::DataType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone)]
pub struct FromUnixTime {
    sec: Arc<dyn PhysicalExpr>,
    format: Arc<dyn PhysicalExpr>,
}

impl FromUnixTime {
    pub fn new(sec: Arc<dyn PhysicalExpr>, format: Arc<dyn PhysicalExpr>) -> FromUnixTime {
        FromUnixTime { sec, format }
    }
}

impl PartialEq for FromUnixTime {
    fn eq(&self, other: &FromUnixTime) -> bool {
        self.sec.eq(&other.sec) && self.format.eq(&other.format)
    }
}

impl Eq for FromUnixTime {}

impl Hash for FromUnixTime {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sec.hash(state);
        self.format.hash(state);
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
        let sec = self.sec.eval(input);
        if sec.is_null() {
            return Value::Null;
        }
        let format = self.format.eval(input);
        if format.is_null() {
            return Value::Null;
        }
        Value::String(Arc::new(from_timestamp_micros_utc(sec.get_long() * 1000_000).format(format.get_string()).to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct ToUnixTimestamp {
    time_expr: Arc<dyn PhysicalExpr>,
    format: Arc<dyn PhysicalExpr>,
}

impl ToUnixTimestamp {
    pub fn new(time_expr: Arc<dyn PhysicalExpr>, format: Arc<dyn PhysicalExpr>) -> ToUnixTimestamp {
        ToUnixTimestamp { time_expr, format }
    }
}

impl PartialEq for ToUnixTimestamp {
    fn eq(&self, other: &ToUnixTimestamp) -> bool {
        self.time_expr.eq(&other.time_expr) && self.format.eq(&other.format)
    }
}

impl Eq for ToUnixTimestamp {}

impl Hash for ToUnixTimestamp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.time_expr.hash(state);
        self.format.hash(state);
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
        let time_expr = self.time_expr.eval(input);
        if time_expr.is_null() {
            return Value::Null;
        }
        let format = self.format.eval(input);
        if format.is_null() {
            return Value::Null;
        }
        match self.time_expr.data_type() {
            DataType::Timestamp => {
                Value::Long(time_expr.get_long() / 1000_1000)
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

const TRUNC_INVALID: i8 = -1;
const TRUNC_TO_MICROSECOND: i8 = 0;
const TRUNC_TO_MILLISECOND: i8 = 1;
const TRUNC_TO_SECOND: i8 = 2;
const TRUNC_TO_MINUTE: i8 = 3;
const TRUNC_TO_HOUR: i8 = 4;
const TRUNC_TO_DAY: i8 = 5;

#[derive(Debug, Clone)]
pub struct TruncTimestamp {
    format: Arc<dyn PhysicalExpr>,
    timestamp: Arc<dyn PhysicalExpr>,
    level_static: Option<i8>,
}

impl TruncTimestamp {
    pub fn new(format: Arc<dyn PhysicalExpr>, timestamp: Arc<dyn PhysicalExpr>) -> TruncTimestamp {
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

impl PartialEq for TruncTimestamp {
    fn eq(&self, other: &TruncTimestamp) -> bool {
        self.format.eq(&other.format) && self.timestamp.eq(&other.timestamp)
    }
}

impl Eq for TruncTimestamp {}

impl Hash for TruncTimestamp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.format.hash(state);
        self.timestamp.hash(state);
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

#[derive(Debug, Clone)]
pub struct TimestampFloor {
    timestamp: Arc<dyn PhysicalExpr>,
    interval: i64,
}

impl TimestampFloor {
    pub fn new(timestamp: Arc<dyn PhysicalExpr>, interval: i64) -> TimestampFloor {
        TimestampFloor { timestamp, interval }
    }
}

impl PartialEq for TimestampFloor {
    fn eq(&self, other: &TimestampFloor) -> bool {
        self.timestamp.eq(&other.timestamp) && self.interval.eq(&other.interval)
    }
}

impl Eq for TimestampFloor {}

impl Hash for TimestampFloor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.interval.hash(state);
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
