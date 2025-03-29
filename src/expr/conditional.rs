use std::sync::Arc;
use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct If {
    pub predicate: Box<Expr>,
    pub true_value: Box<Expr>,
    pub false_value: Box<Expr>,
}

impl If {
    pub fn new(predicate: Box<Expr>, true_value: Box<Expr>, false_value: Box<Expr>) -> Self {
        Self { predicate, true_value, false_value, }
    }
}

impl CreateScalarFunction for If {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 3 {
            return Err(format!("requires 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let predicate = iter.next().unwrap();
        let true_value = iter.next().unwrap();
        let false_value = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(predicate), Box::new(true_value), Box::new(false_value))))
    }
}

impl ScalarFunction for If {

    fn name(&self) -> &str {
        "If"
    }

    fn data_type(&self) -> &DataType {
        self.true_value.data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.predicate, &self.true_value, &self.false_value]
    }

    fn check_input_data_types(&self) -> Result<()> {
        if self.predicate.data_type() != DataType::boolean_type() {
            Err(format!("type of predicate expression in If should be boolean,, not {}", self.predicate.data_type()))
        } else if self.true_value.data_type() != self.false_value.data_type() {
            Err(format!("type of true_value and false_value expression in If should be same, not {} and {}", self.true_value.data_type(), self.false_value.data_type()))
        } else {
            Ok(())
        }
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{predicate, true_value, false_value} = self;
        Ok(Arc::new(phy::If::new(create_physical_expr(predicate)?, create_physical_expr(true_value)?, create_physical_expr(false_value)?)))
    }
}

#[derive(Debug, Clone)]
pub struct CaseWhen {
    pub branches: Vec<(Expr, Expr)>,
    pub else_value: Box<Expr>,
}

impl CaseWhen {
    pub fn new(branches: Vec<(Expr, Expr)>, else_value: Box<Expr>) -> Self {
        Self { branches, else_value, }
    }
}

impl CreateScalarFunction for CaseWhen {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        let mut branches = Vec::new();
        for i in (0..args.len()).step_by(2) {
            if i + 1 >= args.len() {
                break
            }
            branches.push((args[i].clone(), args[i + 1].clone()));
        }
        let else_value= if args.len() % 2 == 1 {
            args[args.len() - 1].clone()
        } else {
            Expr::null_lit()
        };
        Ok(Box::new(Self::new(branches, Box::new(else_value))))
    }
}

impl ScalarFunction for CaseWhen {

    fn name(&self) -> &str {
        "CaseWhen"
    }

    fn data_type(&self) -> &DataType {
        self.else_value.data_type()
    }

    fn args(&self) -> Vec<&Expr> {
        let mut result = Vec::with_capacity(self.branches.len() * 2 + 1);
        for (condition, value) in &self.branches {
            result.push(condition);
            result.push(value);
        }
        result.push(&self.else_value);
        result
    }

    fn check_input_data_types(&self) -> Result<()> {
        for (condition, value) in &self.branches {
            if condition.data_type() != DataType::boolean_type() {
                return Err(format!("type of condition expression in CaseWhen should be boolean,, not {}", condition.data_type()));
            }
            if value.data_type() != self.else_value.data_type() {
                return Err(format!("type of value expression in CaseWhen should be same, not {} and {}", value.data_type(), self.else_value.data_type()));
            }
        }
        Ok(())
    }

    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let Self{branches, else_value} = self;
        let mut physical_branches = Vec::new();
        for (condition, value) in branches {
            physical_branches.push((create_physical_expr(condition)?, create_physical_expr(value)?));
        }
        Ok(Arc::new(phy::CaseWhen::new(physical_branches, create_physical_expr(else_value)?)))
    }
}





