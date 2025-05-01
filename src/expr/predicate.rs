use std::collections::HashSet;
use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct InSet {
    pub child: Box<Expr>,
    pub hset: Vec<Expr>,
}

impl InSet {
    pub fn new(child: Box<Expr>, hset: Vec<Expr>) -> Self {
        InSet { child, hset, }
    }
}

impl CreateScalarFunction for InSet {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>>{
        if args.len() < 2 {
            return Err(format!("requires at least 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        let hset = iter.collect();
        Ok(Box::new(Self::new(Box::new(child), hset)))
    }
}

impl ScalarFunction for InSet {
    fn name(&self) -> &str {
        "in_set"
    }

    fn data_type(&self) -> &DataType {
        DataType::boolean_type()
    }

    fn args(&self) -> Vec<&Expr> {
        let mut args = vec![self.child.as_ref()];
        args.extend(self.hset.iter());
        args
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{child, hset} = self;
        let child = create_physical_expr(child)?;
        let mut set = HashSet::new();
        for x in hset {
            match x {
                Expr::Literal(x) => {
                    set.insert(x.value.clone());
                },
                _ => {
                    return Err(format!("in_set function only support literal arguments"));
                }
            }
        }
        Ok(Box::new(phy::InSet::new(child, set)))
    }
}