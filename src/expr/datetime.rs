use std::sync::Arc;
use crate::datetime_utils::NORM_DATETIME_FMT;
use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
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
        "CurrentTimestamp"
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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(phy::CurrentTimestamp))
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
        "FromUnixTime"
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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{sec, format} = self;
        Ok(Arc::new(phy::FromUnixTime::new(create_physical_expr(sec)?, create_physical_expr(format)?)))
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
        "ToUnixTimestamp"
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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{time_expr, format} = self;
        Ok(Arc::new(phy::ToUnixTimestamp::new(create_physical_expr(time_expr)?, create_physical_expr(format)?)))
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
        "TruncTimestamp"
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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{format, timestamp} = self;
        Ok(Arc::new(phy::TruncTimestamp::new(create_physical_expr(format)?, create_physical_expr(timestamp)?)))
    }
}
