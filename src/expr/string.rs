use std::sync::Arc;
use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
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

impl CreateScalarFunction for Length {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl ScalarFunction for Length {

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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(phy::Length::new(create_physical_expr(&self.child)?)))
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

impl CreateScalarFunction for Substring {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() < 2 || args.len() > 3 {
            return Err(format!("requires 2 or 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let str = iter.next().unwrap();
        let pos = iter.next().unwrap();
        let len = iter.next().unwrap_or(Expr::int_lit(i32::MAX));
        Ok(Box::new(Self::new(Box::new(str), Box::new(pos), Box::new(len))))
    }
}

impl ScalarFunction for Substring {

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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{str, pos, len} = self;
        Ok(Arc::new(phy::Substring::new(create_physical_expr(str)?, create_physical_expr(pos)?, create_physical_expr(len)?)))
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

impl CreateScalarFunction for StringSplit {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let str = iter.next().unwrap();
        let delimiter = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(str), Box::new(delimiter))))
    }
}

impl ScalarFunction for StringSplit {

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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{str, delimiter} = self;
        Ok(Arc::new(phy::StringSplit::new(create_physical_expr(str)?, create_physical_expr(delimiter)?)))
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

impl CreateScalarFunction for SplitPart {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 3 {
            return Err(format!("requires 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let str = iter.next().unwrap();
        let delimiter = iter.next().unwrap();
        let part = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(str), Box::new(delimiter), Box::new(part))))
    }
}

impl ScalarFunction for SplitPart {

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

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{str, delimiter, part} = self;
        Ok(Arc::new(phy::SplitPart::new(create_physical_expr(str)?, create_physical_expr(delimiter)?, create_physical_expr(part)?)))
    }
}







