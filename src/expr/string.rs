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
        "length"
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

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        Ok(Box::new(phy::Length::new(create_physical_expr(&self.child)?)))
    }
}

#[derive(Debug, Clone)]
pub struct ConcatWs {
    pub sep: Box<Expr>,
    pub str_args: Vec<Expr>,
}

impl ConcatWs {
    pub fn new(sep: Box<Expr>, str_args: Vec<Expr>) -> ConcatWs {
        ConcatWs{sep, str_args}
    }
}

impl CreateScalarFunction for ConcatWs {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() < 2 {
            return Err(format!("requires at least 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let sep = iter.next().unwrap();
        let str_args = iter.collect();
        Ok(Box::new(Self::new(Box::new(sep), str_args)))
    }
}

impl ScalarFunction for ConcatWs {
    fn name(&self) -> &str {
        "concat_ws"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        let mut args = vec![self.sep.as_ref()];
        args.extend(self.str_args.iter());
        args
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        let mut types = Vec::with_capacity(self.str_args.len() + 1);
        types.resize(self.str_args.len() + 1, AbstractDataType::string_type());
        Some(types)
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{sep, str_args} = self;
        Ok(Box::new(phy::ConcatWs::new(create_physical_expr(sep)?, str_args.iter().map(|arg| create_physical_expr(arg)).collect::<Result<Vec<_>>>()?)))
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
        "substring"
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

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{str, pos, len} = self;
        Ok(Box::new(phy::Substring::new(create_physical_expr(str)?, create_physical_expr(pos)?, create_physical_expr(len)?)))
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
        "split"
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

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{str, delimiter} = self;
        Ok(Box::new(phy::StringSplit::new(create_physical_expr(str)?, create_physical_expr(delimiter)?)))
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
        "split_part"
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

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{str, delimiter, part} = self;
        Ok(Box::new(phy::SplitPart::new(create_physical_expr(str)?, create_physical_expr(delimiter)?, create_physical_expr(part)?)))
    }
}

#[derive(Debug, Clone)]
pub struct StringReplace {
    pub str: Box<Expr>,
    pub search: Box<Expr>,
    pub replace: Box<Expr>,
}

impl StringReplace {
    pub fn new(str: Box<Expr>, search: Box<Expr>, replace: Box<Expr>) -> StringReplace {
        StringReplace{str, search, replace}
    }
}

impl CreateScalarFunction for StringReplace {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 3 {
            return Err(format!("requires 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let str = iter.next().unwrap();
        let search = iter.next().unwrap();
        let replace = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(str), Box::new(search), Box::new(replace))))
    }
}

impl ScalarFunction for StringReplace {
    fn name(&self) -> &str {
        "replace"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.str, &self.search, &self.replace]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::String), AbstractDataType::Type(DataType::String), AbstractDataType::Type(DataType::String)])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{str, search, replace} = self;
        Ok(Box::new(phy::StringReplace::new(create_physical_expr(str)?, create_physical_expr(search)?, create_physical_expr(replace)?)))
    }
}

#[derive(Debug, Clone)]
pub struct StringTrim {
    pub src_str: Box<Expr>,
    pub trim_str: Box<Expr>,
}

impl StringTrim {
    pub fn new(src_str: Box<Expr>, trim_str: Box<Expr>) -> StringTrim {
        StringTrim{src_str, trim_str}
    }
}

impl CreateScalarFunction for StringTrim {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() < 1 || args.len() > 2 {
            return Err(format!("requires 1 or 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let src_str = iter.next().unwrap();
        let trim_str = iter.next().unwrap_or(Expr::string_lit(" "));
        Ok(Box::new(Self::new(Box::new(src_str), Box::new(trim_str))))
    }
}

impl ScalarFunction for StringTrim {
    fn name(&self) -> &str {
        "trim"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.src_str, &self.trim_str]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::String),AbstractDataType::Type(DataType::String)])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{src_str, trim_str} = self;
        Ok(Box::new(phy::StringTrim::new(create_physical_expr(src_str)?, create_physical_expr(trim_str)?)))
    }
}

#[derive(Debug, Clone)]
pub struct Lower {
    pub child: Box<Expr>,
}

impl Lower {
    pub fn new(child: Box<Expr>) -> Lower {
        Lower{child}
    }
}

impl CreateScalarFunction for Lower {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl ScalarFunction for Lower {
    fn name(&self) -> &str {
        "lower"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::String)])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{child} = self;
        Ok(Box::new(phy::Lower::new(create_physical_expr(child)?)))
    }
}

#[derive(Debug, Clone)]
pub struct Upper {
    pub child: Box<Expr>,
}

impl Upper {
    pub fn new(child: Box<Expr>) -> Upper {
        Upper{child}
    }
}

impl CreateScalarFunction for Upper {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl ScalarFunction for Upper {
    fn name(&self) -> &str {
        "upper"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::String)])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{child} = self;
        Ok(Box::new(phy::Upper::new(create_physical_expr(child)?)))
    }
}

#[derive(Debug, Clone)]
pub struct ToBase64 {
    pub child: Box<Expr>,
}

impl ToBase64 {
    pub fn new(child: Box<Expr>) -> ToBase64 {
        ToBase64{child}
    }
}

impl CreateScalarFunction for ToBase64 {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl ScalarFunction for ToBase64 {
    fn name(&self) -> &str {
        "to_base64"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Collection(vec![AbstractDataType::Type(DataType::Binary), AbstractDataType::Type(DataType::String)])])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{child} = self;
        Ok(Box::new(phy::ToBase64::new(create_physical_expr(child)?)))
    }
}

#[derive(Debug, Clone)]
pub struct FromBase64 {
    pub child: Box<Expr>,
}

impl FromBase64 {
    pub fn new(child: Box<Expr>) -> FromBase64 {
        FromBase64{child}
    }
}

impl CreateScalarFunction for FromBase64 {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl ScalarFunction for FromBase64 {
    fn name(&self) -> &str {
        "from_base64"
    }

    fn data_type(&self) -> &DataType {
        DataType::binary_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Collection(vec![AbstractDataType::Type(DataType::Binary), AbstractDataType::Type(DataType::String)])])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{child} = self;
        Ok(Box::new(phy::FromBase64::new(create_physical_expr(child)?)))
    }
}

#[derive(Debug, Clone)]
pub struct Hex {
    pub child: Box<Expr>,
}

impl Hex {
    pub fn new(child: Box<Expr>) -> Hex {
        Hex{child}
    }
}

impl CreateScalarFunction for Hex {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl ScalarFunction for Hex {
    fn name(&self) -> &str {
        "hex"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Collection(vec![AbstractDataType::Type(DataType::Binary), AbstractDataType::Type(DataType::String)])])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{child} = self;
        Ok(Box::new(phy::Hex::new(create_physical_expr(child)?)))
    }
}

#[derive(Debug, Clone)]
pub struct Unhex {
    pub child: Box<Expr>,
}

impl Unhex {
    pub fn new(child: Box<Expr>) -> Unhex {
        Unhex{child}
    }
}

impl CreateScalarFunction for Unhex {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl ScalarFunction for Unhex {
    fn name(&self) -> &str {
        "unhex"
    }

    fn data_type(&self) -> &DataType {
        DataType::binary_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Collection(vec![AbstractDataType::Type(DataType::Binary), AbstractDataType::Type(DataType::String)])])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{child} = self;
        Ok(Box::new(phy::Unhex::new(create_physical_expr(child)?)))
    }
}