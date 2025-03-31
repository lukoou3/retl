use crate::Result;
use crate::expr::{CreateScalarFunction, Expr, ScalarFunction, create_physical_expr};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::{AbstractDataType, DataType};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct UnaryMinus {
    pub child: Box<Expr>,
}

impl UnaryMinus {
    pub fn new(child: Box<Expr>) -> UnaryMinus {
        UnaryMinus { child }
    }
}

impl CreateScalarFunction for UnaryMinus {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        Ok(Box::new(UnaryMinus::new(Box::new(args[0].clone()))))
    }
}

impl ScalarFunction for UnaryMinus {

    fn name(&self) -> &str {
        "UnaryMinus"
    }

    fn data_type(&self) -> &DataType {
        self.child.data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Numeric])
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(phy::UnaryMinus::new(create_physical_expr(&self.child)?)))
    }
}

#[derive(Debug, Clone)]
pub struct Least {
    pub children: Vec<Expr>,
}

impl Least {
    pub fn new(children: Vec<Expr>) -> Least {
        Least { children }
    }
}

impl CreateScalarFunction for Least {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() < 2 {
            return Err(format!("requires at least 2 argument, found:{}", args.len()));
        }
        Ok(Box::new(Least::new(args)))
    }
}

impl ScalarFunction for Least {
    fn name(&self) -> &str {
        "Least"
    }

    fn data_type(&self) -> &DataType {
        self.children[0].data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        self.children.iter().collect()
    }

    fn check_input_data_types(&self) -> Result<()> {
        let tp = self.children[0].data_type();
        if self.children.iter().all(|child| child.data_type() == tp) {
            Ok(())
        } else if !tp.is_numeric_type() && tp != DataType::string_type() {
            Err(format!("Coalesce requires numeric/string type, not {}", tp))
        } else {
            Err(format!("Coalesce requires all arguments to have the same type: {:?}", self.children))
        }
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(phy::Least::new(
            self.children.iter().map(|child| create_physical_expr(child)).collect::<Result<Vec<_>>>()?
        )))
    }
}

#[derive(Debug, Clone)]
pub struct Greatest {
    pub children: Vec<Expr>,
}

impl Greatest {
    pub fn new(children: Vec<Expr>) -> Greatest {
        Greatest { children }
    }
}

impl CreateScalarFunction for Greatest {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() < 2 {
            return Err(format!("requires at least 2 argument, found:{}", args.len()));
        }
        Ok(Box::new(Greatest::new(args)))
    }
}

impl ScalarFunction for Greatest {
    fn name(&self) -> &str {
        "Greatest"
    }

    fn data_type(&self) -> &DataType {
        self.children[0].data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        self.children.iter().collect()
    }

    fn check_input_data_types(&self) -> Result<()> {
        let tp = self.children[0].data_type();
        if self.children.iter().all(|child| child.data_type() == tp) {
            Ok(())
        } else if !tp.is_numeric_type() && tp != DataType::string_type() {
            Err(format!("Coalesce requires numeric/string type, not {}", tp))
        } else {
            Err(format!("Coalesce requires all arguments to have the same type: {:?}", self.children))
        }
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(phy::Greatest::new(
            self.children.iter().map(|child| create_physical_expr(child)).collect::<Result<Vec<_>>>()?
        )))
    }
}









