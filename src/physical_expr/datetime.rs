use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use chrono::Utc;
use crate::data::{Row, Value};
use crate::datetime_utils::from_timestamp_micros_utc;
use crate::physical_expr::{PhysicalExpr};
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
