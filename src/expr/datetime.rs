use regex::Regex;
use crate::datetime_utils::NORM_DATETIME_FMT;
use crate::Result;
use crate::expr::{create_physical_expr, Literal, CreateScalarFunction, Expr, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::{AbstractDataType, DataType};

#[derive(Debug, Clone)]
pub struct CurrentTimestamp;

impl CreateScalarFunction for CurrentTimestamp {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if !args.is_empty() {
            return Err("requires no arguments".to_string());
        }
        Ok(Box::new(CurrentTimestamp))
    }
}

impl ScalarFunction for CurrentTimestamp {

    fn name(&self) -> &str {
        "current_timestamp"
    }

    fn foldable(&self) -> bool {
        false
    }

    fn data_type(&self) -> &DataType {
        DataType::timestamp_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![]
    }

    fn check_input_data_types(&self) -> Result<()> {
        Ok(())
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        Ok(Box::new(phy::CurrentTimestamp))
    }
}

#[derive(Debug, Clone)]
pub struct FromUnixTime {
    pub sec: Box<Expr>,
    pub format: Box<Expr>,
}

impl FromUnixTime {
    pub fn new(sec: Box<Expr>, format: Box<Expr>) -> FromUnixTime {
        FromUnixTime { sec, format }
    }
}

impl CreateScalarFunction for FromUnixTime {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() < 1 || args.len() > 2 {
            return Err(format!("requires 1 or 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let sec = iter.next().unwrap();
        let format = iter.next().unwrap_or(Expr::string_lit(NORM_DATETIME_FMT));
        Ok(Box::new(Self::new(Box::new(sec), Box::new(format))))
    }
}

impl ScalarFunction for FromUnixTime {
    fn name(&self) -> &str {
        "from_unixtime"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.sec, &self.format]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::long_type(), AbstractDataType::string_type()])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{sec, format} = self;
        Ok(Box::new(phy::FromUnixTime::new(create_physical_expr(sec)?, create_physical_expr(format)?)))
    }
}

#[derive(Debug, Clone)]
pub struct FromUnixTimeMillis {
    pub sec: Box<Expr>,
    pub format: Box<Expr>,
}

impl FromUnixTimeMillis {
    pub fn new(sec: Box<Expr>, format: Box<Expr>) -> FromUnixTimeMillis {
        FromUnixTimeMillis { sec, format }
    }
}

impl CreateScalarFunction for FromUnixTimeMillis {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() < 1 || args.len() > 2 {
            return Err(format!("requires 1 or 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let sec = iter.next().unwrap();
        let format = iter.next().unwrap_or(Expr::string_lit(NORM_DATETIME_FMT));
        Ok(Box::new(Self::new(Box::new(sec), Box::new(format))))
    }
}

impl ScalarFunction for FromUnixTimeMillis {
    fn name(&self) -> &str {
        "from_unixtime__millis"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.sec, &self.format]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::long_type(), AbstractDataType::string_type()])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{sec, format} = self;
        Ok(Box::new(phy::FromUnixTimeMillis::new(create_physical_expr(sec)?, create_physical_expr(format)?)))
    }
}

pub struct UnixTimestamp;

impl CreateScalarFunction for UnixTimestamp {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() > 2 {
            return Err(format!("requires 0-2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let time_expr = iter.next().unwrap_or(Expr::ScalarFunction(Box::new(CurrentTimestamp)));
        let format = iter.next().unwrap_or(Expr::string_lit(NORM_DATETIME_FMT));
        Ok(Box::new(ToUnixTimestamp::new(Box::new(time_expr), Box::new(format))))
    }
}

#[derive(Debug, Clone)]
pub struct ToUnixTimestamp {
    pub time_expr: Box<Expr>,
    pub format: Box<Expr>,
}

impl ToUnixTimestamp {
    pub fn new(time_expr: Box<Expr>, format: Box<Expr>) -> ToUnixTimestamp {
        ToUnixTimestamp { time_expr, format }
    }
}

impl CreateScalarFunction for ToUnixTimestamp {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() < 1 || args.len() > 2 {
            return Err(format!("requires 1 or 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let time_expr = iter.next().unwrap();
        let format = iter.next().unwrap_or(Expr::string_lit(NORM_DATETIME_FMT));
        Ok(Box::new(Self::new(Box::new(time_expr), Box::new(format))))
    }
}

impl ScalarFunction for ToUnixTimestamp {
    fn name(&self) -> &str {
        "to_unix_timestamp"
    }

    fn data_type(&self) -> &DataType {
        DataType::long_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.time_expr, &self.format]
    }

    fn check_input_data_types(&self) -> Result<()> {
        if !matches!(self.time_expr.data_type(), DataType::String | DataType::Timestamp) {
            Err(format!("{:?} requires string/timestamp type, not {}", self.time_expr, self.time_expr.data_type()))
        } else if self.format.data_type() != DataType::string_type() {
            Err(format!("{:?} requires string type, not {}", self.format, self.format.data_type()))
        } else {
            Ok(())
        }
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{time_expr, format} = self;
        Ok(Box::new(phy::ToUnixTimestamp::new(create_physical_expr(time_expr)?, create_physical_expr(format)?)))
    }
}

pub struct UnixTimestampMillis;

impl CreateScalarFunction for UnixTimestampMillis {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() > 2 {
            return Err(format!("requires 0-2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let time_expr = iter.next().unwrap_or(Expr::ScalarFunction(Box::new(CurrentTimestamp)));
        let format = iter.next().unwrap_or(Expr::string_lit(NORM_DATETIME_FMT));
        Ok(Box::new(ToUnixTimestampMillis::new(Box::new(time_expr), Box::new(format))))
    }
}

#[derive(Debug, Clone)]
pub struct ToUnixTimestampMillis {
    pub time_expr: Box<Expr>,
    pub format: Box<Expr>,
}

impl ToUnixTimestampMillis {
    pub fn new(time_expr: Box<Expr>, format: Box<Expr>) -> ToUnixTimestampMillis {
        ToUnixTimestampMillis { time_expr, format }
    }
}

impl CreateScalarFunction for ToUnixTimestampMillis {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() < 1 || args.len() > 2 {
            return Err(format!("requires 1 or 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let time_expr = iter.next().unwrap();
        let format = iter.next().unwrap_or(Expr::string_lit(NORM_DATETIME_FMT));
        Ok(Box::new(Self::new(Box::new(time_expr), Box::new(format))))
    }
}

impl ScalarFunction for ToUnixTimestampMillis {
    fn name(&self) -> &str {
        "to_unix_timestamp_millis"
    }

    fn data_type(&self) -> &DataType {
        DataType::long_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.time_expr, &self.format]
    }

    fn check_input_data_types(&self) -> Result<()> {
        if !matches!(self.time_expr.data_type(), DataType::String | DataType::Timestamp) {
            Err(format!("{:?} requires string/timestamp type, not {}", self.time_expr, self.time_expr.data_type()))
        } else if self.format.data_type() != DataType::string_type() {
            Err(format!("{:?} requires string type, not {}", self.format, self.format.data_type()))
        } else {
            Ok(())
        }
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{time_expr, format} = self;
        Ok(Box::new(phy::ToUnixTimestampMillis::new(create_physical_expr(time_expr)?, create_physical_expr(format)?)))
    }
}

#[derive(Debug, Clone)]
pub struct TruncTimestamp {
    pub format: Box<Expr>,
    pub timestamp: Box<Expr>,
}

impl TruncTimestamp {
    pub fn new(format: Box<Expr>, timestamp: Box<Expr>) -> Self {
        Self { format, timestamp }
    }
}

impl CreateScalarFunction for TruncTimestamp {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires  2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let format = iter.next().unwrap();
        let timestamp = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(format), Box::new(timestamp))))
    }
}

impl ScalarFunction for TruncTimestamp {
    fn name(&self) -> &str {
        "date_trunc"
    }

    fn data_type(&self) -> &DataType {
        DataType::timestamp_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.format, &self.timestamp]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::string_type(), AbstractDataType::timestamp_type()])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{format, timestamp} = self;
        Ok(Box::new(phy::TruncTimestamp::new(create_physical_expr(format)?, create_physical_expr(timestamp)?)))
    }
}

#[derive(Debug, Clone)]
pub struct TimestampFloor {
    pub timestamp: Box<Expr>,
    pub interval: Box<Expr>,
}

impl TimestampFloor {
    pub fn new(timestamp: Box<Expr>, interval: Box<Expr>) -> Self {
        Self { timestamp, interval }
    }
    
    fn parse_interval(interval: &str) -> Result<i64> {
        let re = Regex::new(r"^\s*(\d+)\s*(\w+)\s*$").map_err(|e| e.to_string())?;
        let caps = re.captures(interval).ok_or("Invalid interval format")?;
        
        let count: i64 = caps[1].parse().map_err(|e| format!("Invalid number: {}", e))?;
        let unit_str = caps[2].to_lowercase();

        let interval = match unit_str.as_str() {
            "second" | "seconds" => count * 1_000_000,
            "minute" | "minutes" => count * 60_000_000,
            "hour" | "hours" => count * 3_600_000_000,
            "day" | "days" => count * 86_400_000_000,
            _ => return Err(format!("Unsupported time unit: {}", unit_str)),
        };
        Ok(interval)
    }
}

impl CreateScalarFunction for TimestampFloor {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires  2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let timestamp = iter.next().unwrap();
        let interval = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(timestamp), Box::new(interval))))
    }
}

impl ScalarFunction for TimestampFloor {
    fn name(&self) -> &str {
        "time_floor"
    }

    fn data_type(&self) -> &DataType {
        DataType::timestamp_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.timestamp, &self.interval]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::timestamp_type(), AbstractDataType::string_type()])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{timestamp, interval} = self;
        match interval.as_ref() {
            Expr::Literal(Literal{value, data_type}) if data_type == DataType::string_type() => {
                let interval = TimestampFloor::parse_interval(value.get_string())?;
                Ok(Box::new(phy::TimestampFloor::new(create_physical_expr(timestamp)?, interval)))
            },
            _ => Err("interval argument should be a string literal.".to_string())
        }
        
    }
}
