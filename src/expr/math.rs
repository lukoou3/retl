use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, Literal, ScalarFunction, StringSplit};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::{AbstractDataType, DataType};

#[derive(Debug, Clone)]
pub struct Pow {
    pub left: Box<Expr>,
    pub right: Box<Expr>,
}

impl Pow {
    pub fn new(left: Box<Expr>, right: Box<Expr>) -> Pow {
        Pow{left, right}
    }
}

impl CreateScalarFunction for Pow {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let left = iter.next().unwrap();
        let right = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(left), Box::new(right))))
    }
}

impl ScalarFunction for Pow {

    fn name(&self) -> &str {
        "pow"
    }

    fn data_type(&self) -> &DataType {
        DataType::double_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.left, &self.right]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::double_type(), AbstractDataType::double_type()])
    }

    fn create_physical_expr(&self) -> crate::Result<Box<dyn PhysicalExpr>> {
        let Self{left, right} = self;
        Ok(Box::new(phy::Pow::new(create_physical_expr(left)?, create_physical_expr(right)?)))
    }
}

#[derive(Debug, Clone)]
pub struct Round {
    pub child: Box<Expr>,
    pub scale: Box<Expr>,
}

impl Round {
    pub fn new(child: Box<Expr>, scale: Box<Expr>) -> Round {
        Round{ child, scale }
    }
}

impl CreateScalarFunction for Round {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() < 1 || args.len() > 2 {
            return Err(format!("requires 1 or 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        let scale = iter.next().unwrap_or(Expr::int_lit(0));
        Ok(Box::new(Self::new(Box::new(child), Box::new(scale))))
    }
}

impl ScalarFunction for Round {

    fn name(&self) -> &str {
        "round"
    }

    fn data_type(&self) -> &DataType {
        DataType::double_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child, &self.scale]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::double_type(), AbstractDataType::int_type()])
    }

    fn create_physical_expr(&self) -> crate::Result<Box<dyn PhysicalExpr>> {
        let Self{ child, scale } = self;
        Ok(Box::new(phy::Round::new(create_physical_expr(child)?, create_physical_expr(scale)?)))
    }
}

#[derive(Debug, Clone)]
pub struct Bin {
    pub child: Box<Expr>,
    pub padding: Box<Expr>,
}

impl Bin {
    pub fn new(child: Box<Expr>, padding: Box<Expr>) -> Bin {
        Bin{ child, padding }
    }
}

impl CreateScalarFunction for Bin {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() < 1 || args.len() > 2 {
            return Err(format!("requires 1 or 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        let padding = iter.next().unwrap_or(Expr::boolean_lit(false));
        Ok(Box::new(Self::new(Box::new(child), Box::new(padding))))
    }
}

impl ScalarFunction for Bin {

    fn name(&self) -> &str {
        "bin"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child, &self.padding]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Collection(vec![AbstractDataType::int_type(), AbstractDataType::long_type()]), AbstractDataType::boolean_type()])
    }

    fn create_physical_expr(&self) -> crate::Result<Box<dyn PhysicalExpr>> {
        let Self{ child, padding } = self;
        match padding.as_ref() {
            Expr::Literal(Literal{value, data_type}) if data_type == DataType::boolean_type() => {
                Ok(Box::new(phy::Bin::new(create_physical_expr(child)?, value.get_boolean())))
            },
            _ =>  Err(format!("requires padding argument to be boolean literal, found:{:?}", padding.as_ref()))
        }
    }
}