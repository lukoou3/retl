use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::{Operator, Result};
use crate::data::Value;
use crate::expr::binary_expr;
use crate::tree_node::{Transformed, TreeNodeContainer, TreeNodeRecursion};
use crate::types::DataType;

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum Expr {
    UnresolvedAttribute(String),
    BoundReference(BoundReference),
    AttributeReference(AttributeReference),
    Alias(Alias),
    Literal(Literal),
    UnresolvedFunction(UnresolvedFunction),
    BinaryOperator(BinaryOperator),
    Like(Like),
    RLike(Like),
}

impl Expr {
    pub fn children(&self) -> Vec<&Expr> {
        match self {
            Expr::UnresolvedAttribute(_)
            | Expr::BoundReference(_)
            | Expr::AttributeReference(_)
            | Expr::Literal(_) => Vec::new(),
            Expr::Alias(Alias{ child, ..}) =>
                vec![child],
            Expr::BinaryOperator(BinaryOperator { left, right, .. }) =>
                vec![left, right],
            Expr::Like(Like{expr, pattern})
            | Expr::RLike(Like{expr, pattern}) =>
                vec![expr, pattern],
            Expr::UnresolvedFunction(UnresolvedFunction{name, arguments}) =>
                arguments.iter().map(|a| a).collect(),
        }
    }

    pub fn alias(self, name: impl Into<String>) -> Expr {
        Expr::Alias(Alias::new(self, name.into()))
    }

    pub fn col(ordinal: usize, data_type: DataType) -> Expr {
        Expr::BoundReference(BoundReference::new(ordinal, data_type))
    }

    pub fn lit(value: Value, data_type: DataType) -> Expr {
        Expr::Literal(Literal::new(value, data_type))
    }

    /// Return `self == other`
    pub fn eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Eq, other)
    }


    /// Return `self LIKE other`
    pub fn like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            Box::new(self),
            Box::new(other),
        ))
    }
}

impl<'a> TreeNodeContainer<'a, Self> for Expr {
    fn apply_elements<F: FnMut(&'a Self) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        f(self)
    }

    fn map_elements<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        f(self)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct BoundReference {
    pub ordinal: usize,
    pub data_type: DataType,
}

impl BoundReference {
    pub fn new(ordinal: usize, data_type: DataType) -> Self {
        Self { ordinal, data_type }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Alias {
    pub child: Box<Expr>,
    pub name: String,
}

impl Alias {
    pub fn new(
        expr: Expr,
        name: impl Into<String>,
    ) -> Self {
        Self {
            child: Box::new(expr),
            name: name.into(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct AttributeReference {
    pub name: String,
    pub data_type: DataType,
    pub expr_id: u32,
}

impl AttributeReference {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        let expr_id = ExprIdGenerator::get_next_expr_id();
        AttributeReference{name: name.into(), data_type, expr_id}
    }

    pub fn new_with_expr_id(name: impl Into<String>, data_type: DataType, expr_id: u32) -> Self {
        AttributeReference{name: name.into(), data_type, expr_id}
    }

    pub fn with_expr_id(&self, expr_id: u32) -> Self {
        AttributeReference{ name: self.name.clone(), data_type: self.data_type.clone(), expr_id: self.expr_id }
    }
}

struct ExprIdGenerator {
    counter: AtomicU32,
}

impl ExprIdGenerator {
    fn get_next_expr_id() -> u32 {
        static INSTANCE: ExprIdGenerator = ExprIdGenerator {
            counter: AtomicU32::new(0),
        };
        INSTANCE.counter.fetch_add(1, Ordering::SeqCst)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Literal {
    pub value: Value,
    pub data_type: DataType,
}

impl Literal {
    pub fn new(value: Value, data_type: DataType) -> Self {
        Self { value, data_type }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct UnresolvedFunction {
    pub name: String,
    pub arguments: Vec<Expr>,
}

/// Binary operator
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct BinaryOperator {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: Operator,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryOperator {
    /// Create a new binary expression
    pub fn new(left: Box<Expr>, op: Operator, right: Box<Expr>) -> Self {
        Self { left, op, right }
    }
}

/*#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryExpr {
    pub fn new(left: Box<Expr>, right: Box<Expr>) -> Self {
        Self { left, right }
    }
}*/

/// LIKE expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Like {
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
}

impl Like {
    pub fn new(expr: Box<Expr>, pattern: Box<Expr>) -> Self {
        Self { expr, pattern }
    }
}

