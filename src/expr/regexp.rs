use std::sync::Arc;
use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::{AbstractDataType, DataType};

#[derive(Debug, Clone)]
pub struct RegExpExtract {
    pub subject: Box<Expr>,
    pub regexp: Box<Expr>,
    pub idx: Box<Expr>,
}

impl RegExpExtract {
    pub fn new(subject: Box<Expr>, regexp: Box<Expr>, idx: Box<Expr>) -> Self {
        Self { subject, regexp, idx, }
    }
}

impl CreateScalarFunction for RegExpExtract {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() != 3 {
            return Err(format!("requires 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let subject = iter.next().unwrap();
        let regexp = iter.next().unwrap();
        let idx = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(subject), Box::new(regexp), Box::new(idx))))
    }
}

impl ScalarFunction for RegExpExtract {
    fn name(&self) -> &str {
        "RegExpExtract"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.subject, &self.regexp, &self.idx]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::string_type(), AbstractDataType::string_type(), AbstractDataType::int_type()])
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{subject, regexp, idx} = self;
        Ok(Arc::new(phy::RegExpExtract::new(create_physical_expr(subject)?, create_physical_expr(regexp)?, create_physical_expr(idx)?)))
    }
}


#[derive(Debug, Clone)]
pub struct RegExpReplace {
    pub subject: Box<Expr>,
    pub regexp: Box<Expr>,
    pub rep: Box<Expr>,
}

impl RegExpReplace {
    pub fn new(subject: Box<Expr>, regexp: Box<Expr>, rep: Box<Expr>) -> Self {
        Self { subject, regexp, rep, }
    }
}

impl CreateScalarFunction for RegExpReplace {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() != 3 {
            return Err(format!("requires 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let subject = iter.next().unwrap();
        let regexp = iter.next().unwrap();
        let rep = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(subject), Box::new(regexp), Box::new(rep))))
    }
}

impl ScalarFunction for RegExpReplace {
    fn name(&self) -> &str {
        "RegExpReplace"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.subject, &self.regexp, &self.rep]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::String), AbstractDataType::Type(DataType::String), AbstractDataType::Type(DataType::String)])
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{subject, regexp, rep} = self;
        Ok(Arc::new(phy::Substring::new(create_physical_expr(subject)?, create_physical_expr(regexp)?, create_physical_expr(rep)?)))
    }
}


