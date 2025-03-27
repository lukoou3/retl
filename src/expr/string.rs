use std::any::Any;
use crate::Result;
use crate::expr::{Expr, ScalarFunction};
use crate::types::{AbstractDataType, DataType};

#[derive(Debug, Clone)]
pub struct Length {
    pub child: Box<Expr>,
}

impl Length {
    pub fn new(child: Box<Expr>) -> Length {
        Length { child }
    }
}

impl ScalarFunction for Length {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Length"
    }

    fn data_type(&self) -> &DataType {
        DataType::int_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn check_input_data_types(&self) -> Result<()> {
        if self.child.data_type() != DataType::string_type() {
            Err(format!("{:?} requires string type, not {}", self.child, self.child.data_type()))
        } else {
            Ok(())
        }
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let Some(child) = iter.next() {
            Box::new(Length::new(Box::new(child)))
        } else {
            panic!("args count not match")
        }
    }
}

#[derive(Debug, Clone)]
pub struct Substring {
    pub str: Box<Expr>,
    pub pos: Box<Expr>,
    pub len: Box<Expr>,
}

impl Substring {
    pub fn new(str: Box<Expr>, pos: Box<Expr>, len: Box<Expr>) -> Substring {
        Substring{str, pos, len}
    }
}

impl ScalarFunction for Substring {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Substring"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.str, &self.pos, &self.len]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::String), AbstractDataType::Type(DataType::Int), AbstractDataType::Type(DataType::Int)])
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let (Some(first), Some(second), Some(third)) = (iter.next(), iter.next(), iter.next()) {
            Box::new(Substring::new(Box::new(first), Box::new(second), Box::new(third)))
        } else {
            panic!("args count not match")
        }
    }
}

#[derive(Debug, Clone)]
pub struct StringSplit {
    pub str: Box<Expr>,
    pub delimiter: Box<Expr>,
}

impl StringSplit {
    pub fn new(str: Box<Expr>, delimiter: Box<Expr>) -> StringSplit {
        StringSplit{str, delimiter}
    }
}

impl ScalarFunction for StringSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "StringSplit"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_array_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.str, &self.delimiter]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::String), AbstractDataType::Type(DataType::String)])
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let (Some(first), Some(second)) = (iter.next(), iter.next()) {
            Box::new(StringSplit::new(Box::new(first), Box::new(second)))
        } else {
            panic!("args count not match")
        }
    }
}

#[derive(Debug, Clone)]
pub struct SplitPart {
    pub str: Box<Expr>,
    pub delimiter: Box<Expr>,
    pub part: Box<Expr>,
}

impl SplitPart {
    pub fn new(str: Box<Expr>, delimiter: Box<Expr>, part: Box<Expr>) -> SplitPart {
        SplitPart{str, delimiter, part}
    }
}

impl ScalarFunction for SplitPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "SplitPart"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.str, &self.delimiter, &self.part]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::String), AbstractDataType::Type(DataType::String), AbstractDataType::Type(DataType::Int)])
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        let mut  iter = args.into_iter();
        if let (Some(first), Some(second), Some(third)) = (iter.next(), iter.next(), iter.next()) {
            Box::new(SplitPart::new(Box::new(first), Box::new(second), Box::new(third)))
        } else {
            panic!("args count not match")
        }
    }
}







