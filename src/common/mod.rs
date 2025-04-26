pub mod date_utils;
pub mod datetime_utils;
pub mod buffer_pool;
pub mod encrypt;
pub mod rate_stat;
pub mod buffer_block;

use std::cell::Cell;
use std::fmt::{Debug, Formatter};
use std::result;
use std::sync::{Arc, OnceLock};

pub type Result<T, E = String> = result::Result<T, E>;

#[derive(Clone)]
pub struct LazyValue<T>
where
    T: Clone + Send + Sync + 'static
{
    value: OnceLock<T>,
    init: Arc<dyn Fn() -> T + Send + Sync >,
}

impl<T> LazyValue<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new(init: Arc<dyn Fn() -> T + Send + Sync >) -> Self {
        LazyValue { value: OnceLock::new(), init, }
    }
    pub fn get(&self) -> &T {
        self.value.get_or_init(|| (self.init)())
    }
}

impl<T> Debug for  LazyValue<T>
where
    T: Clone + Send + Sync + 'static
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyValue").finish()
    }
}

/// Operators applied to expressions
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulo,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
}

impl Operator {
    pub fn sql_operator(&self) -> &'static str {
        match self {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Modulo => "%",
            Operator::And => "and",
            Operator::Or=> "or",
        }
    }
}