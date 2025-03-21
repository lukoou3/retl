use std::any::Any;
use crate::Result;
use crate::expr::{Expr, ScalarFunction};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct CurrentTimestamp;

impl ScalarFunction for CurrentTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn rewrite_args(&self, _: Vec<Expr>) -> Box<dyn ScalarFunction> {
        Box::new(CurrentTimestamp)
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

impl ScalarFunction for FromUnixTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "FromUnixTime"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.sec, &self.format]
    }

    fn check_input_data_types(&self) -> Result<()> {
        if self.sec.data_type() != DataType::long_type() {
            Err(format!("{:?} requires long type, not {}", self.sec, self.sec.data_type()))
        } else if self.format.data_type() != DataType::string_type() {
            Err(format!("{:?} requires string type, not {}", self.format, self.format.data_type()))
        } else {
            Ok(())
        }
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let (Some(first), Some(second)) = (iter.next(), iter.next()) {
            Box::new(FromUnixTime::new(Box::new(first), Box::new(second)))
        } else {
            panic!("args count not match")
        }
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

impl ScalarFunction for ToUnixTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let (Some(first), Some(second)) = (iter.next(), iter.next()) {
            Box::new(ToUnixTimestamp::new(Box::new(first), Box::new(second)))
        } else {
            panic!("args count not match")
        }
    }
}








