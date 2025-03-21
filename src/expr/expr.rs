use std::any::Any;
use std::cmp::{Ordering, PartialEq};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use crate::{Operator, Result};
use crate::data::Value;
use crate::expr::binary_expr;
use crate::physical_expr::{can_cast, PhysicalExpr};
use crate::tree_node::{Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion};
use crate::types::DataType;

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum Expr {
    UnresolvedAttribute(String),
    BoundReference(BoundReference),
    AttributeReference(AttributeReference),
    Alias(Alias),
    Cast(Cast),
    Literal(Literal),
    UnresolvedFunction(UnresolvedFunction),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    BinaryOperator(BinaryOperator),
    Like(Like),
    RLike(Like),
    In(In),
    ScalarFunction(Box<dyn ScalarFunction>),
}

impl Expr {
    pub fn foldable(&self) -> bool {
        match self {
            Expr::UnresolvedAttribute(_) | Expr::UnresolvedFunction(_) | Expr::BoundReference(_) => false,
            // We should never fold named expressions in order to not remove the alias.
            Expr::AttributeReference(_) | Expr::Alias(_)  => false,
            Expr::Literal(_)  => true,
            Expr::ScalarFunction(f) => f.foldable(),
            _ => self.children().iter().all(|c| c.foldable()),
        }
    }


    pub fn data_type(&self) -> &DataType {
        match self {
            Expr::UnresolvedAttribute(_) | Expr::UnresolvedFunction(_)  =>
                panic!("UnresolvedExpr:{:?}", self),
            Expr::BoundReference(b) => &b.data_type,
            Expr::AttributeReference(a) => &a.data_type,
            Expr::Alias(e) => e.child.data_type(),
            Expr::Literal(l) => &l.data_type,
            Expr::Cast(c) => &c.data_type,
            Expr::Not(_) | Expr::IsNull(_) | Expr::IsNotNull(_) => DataType::boolean_type(),
            Expr::BinaryOperator(BinaryOperator{left, op, right:_ }) =>  match op {
                Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo =>
                    left.data_type(),
                Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq =>
                    DataType::boolean_type(),
                Operator::And | Operator::Or =>
                    DataType::boolean_type(),
            },
            Expr::Like(_) => DataType::boolean_type(),
            Expr::RLike(_) => DataType::boolean_type(),
            Expr::In(_) => DataType::boolean_type(),
            Expr::ScalarFunction(f) => f.data_type(),
        }
    }

    pub fn resolved(&self) -> bool {
        match self {
            Expr::UnresolvedAttribute(_) | Expr::UnresolvedFunction(_) =>
                false,
            _ => self.children_resolved() && self.check_input_data_types().is_ok()
        }
    }

    pub fn children_resolved(&self) -> bool {
        self.children().iter().all(|c| c.resolved())
    }

    pub fn check_input_data_types(&self) -> Result<()> {
        match self {
            Expr::UnresolvedAttribute(_)
             | Expr::UnresolvedFunction(_)
             | Expr::BoundReference(_)
             | Expr::AttributeReference(_)
             | Expr::Literal(_)
             | Expr::Alias(_) =>
                Ok(()),
            Expr::Cast(Cast{child, data_type}) =>{
                let from = child.data_type();
                if can_cast(from, data_type) {
                    Ok(())
                } else {
                    Err(format!("cannot cast {} to {}", from, data_type))
                }
            },
            Expr::Not(child) => {
                if child.data_type() != DataType::boolean_type() {
                    Err(format!("{:?} requires boolean type, not {}", self, child.data_type()))
                } else {
                    Ok(())
                }
            },
            Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(()),
            Expr::BinaryOperator(BinaryOperator{left, op, right}) => {
                if left.data_type() != right.data_type() {
                    return Err(format!("differing types in {:?}", self));
                }
                match op {
                    Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo => {
                        if !left.data_type().is_numeric_type() {
                            Err(format!("{:?} requires numeric type, not {}", self, left.data_type()))
                        } else if *op == Operator::Divide && left.data_type() != DataType::long_type() && left.data_type() != DataType::double_type() {
                            Err(format!("{:?} requires long/double type, not {}", self, left.data_type()))
                        } else {
                            Ok(())
                        }
                    },
                    Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq =>
                        if !left.data_type().is_numeric_type() && left.data_type() != DataType::string_type()  {
                            Err(format!("{:?} requires numeric/string type, not {}", self, left.data_type()))
                        } else {
                            Ok(())
                        },
                    Operator::And | Operator::Or =>
                        if left.data_type() != DataType::boolean_type() {
                            Err(format!("{:?} requires boolean type, not {}", self, left.data_type()))
                        } else {
                            Ok(())
                        },
                }
            },
            Expr::Like(Like{expr, pattern})
             | Expr::RLike(Like{expr, pattern}) => {
                if expr.data_type() != DataType::string_type(){
                    Err(format!("{:?} requires string type, not {}", self, expr.data_type()))
                } else if pattern.data_type() != DataType::string_type() {
                    Err(format!("{:?} requires string type, not {}", self, pattern.data_type()))
                } else {
                    Ok(())
                }
            },
            Expr::In(In{value, list}) => {
                if list.iter().any(|e| value.data_type() != e.data_type()) {
                    Err(format!("{:?} requires same type", self))
                } else {
                    Ok(())
                }
            },
            Expr::ScalarFunction(f) => {
                f.check_input_data_types()
            },
        }
    }

    pub fn children(&self) -> Vec<&Expr> {
        match self {
            Expr::UnresolvedAttribute(_)
            | Expr::BoundReference(_)
            | Expr::AttributeReference(_)
            | Expr::Literal(_) => Vec::new(),
            Expr::Alias(Alias{ child, ..})
            | Expr::Cast(Cast{ child, ..})
            | Expr::Not(child)
            | Expr::IsNull(child) | Expr::IsNotNull(child) =>
                vec![child],
            Expr::BinaryOperator(BinaryOperator { left, right, .. }) =>
                vec![left, right],
            Expr::Like(Like{expr, pattern})
            | Expr::RLike(Like{expr, pattern}) =>
                vec![expr, pattern],
            Expr::In(In{value, list}) =>
                vec![value.as_ref()].into_iter().chain(list.iter()).collect(),
            Expr::ScalarFunction(f) => f.args(),
            Expr::UnresolvedFunction(UnresolvedFunction{name: _, arguments}) =>
                arguments.iter().map(|a| a).collect(),
        }
    }

    pub fn alias(self, name: impl Into<String>) -> Expr {
        Expr::Alias(Alias::new(self, name.into()))
    }

    pub fn cast(self, data_type: DataType) -> Expr {
        Expr::Cast(Cast::new(self, data_type))
    }

    pub fn not(self) -> Expr {
        Expr::Not(Box::new(self))
    }

    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(Box::new(self))
    }

    pub fn col(ordinal: usize, data_type: DataType) -> Expr {
        Expr::BoundReference(BoundReference::new(ordinal, data_type))
    }

    pub fn lit(value: Value, data_type: DataType) -> Expr {
        Expr::Literal(Literal::new(value, data_type))
    }

    pub fn int_lit(v: i32) -> Expr {
        Expr::Literal(Literal::new(Value::Int(v), DataType::Int))
    }

    pub fn long_lit(v: i64) -> Expr {
        Expr::Literal(Literal::new(Value::Long(v), DataType::Long))
    }

    pub fn string_lit(s:impl  Into<String>) -> Expr {
        Expr::Literal(Literal::new(Value::string(s), DataType::String))
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

    pub fn bind_reference(expr: Expr, input: Vec<AttributeReference>) -> Result<Expr> {
        let expr_id_to_ordinal: HashMap<u32, usize> = input.iter().enumerate().map(|(i, x)| (x.expr_id, i)).collect();
        let new_expr= expr.transform_up(|expr| {
            if let Expr::AttributeReference(AttributeReference{data_type, expr_id, ..}) = &expr {
                if let Some(ordinal) = expr_id_to_ordinal.get(expr_id){
                    return Ok(Transformed::yes(Expr::BoundReference(BoundReference::new(*ordinal, data_type.clone()))));
                } else { return Err(format!("not found {:?} in {:?}", expr, input)) }
            } else {
                Ok(Transformed::no(expr))
            }
        })?.data;
        Ok(new_expr)
    }

    pub fn bind_references(exprs: Vec<Expr>, input: Vec<AttributeReference>) -> Result<Vec<Expr>> {
        let expr_id_to_ordinal: HashMap<u32, usize> = input.iter().enumerate().map(|(i, x)| (x.expr_id, i)).collect();
        let mut new_exprs = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let e = expr.transform_up(|expr| {
                if let Expr::AttributeReference(AttributeReference{data_type, expr_id, ..}) = &expr {
                    if let Some(ordinal) = expr_id_to_ordinal.get(expr_id){
                        return Ok(Transformed::yes(Expr::BoundReference(BoundReference::new(*ordinal, data_type.clone()))));
                    } else { return Err(format!("not found {:?} in {:?}", expr, input)) }
                } else {
                    Ok(Transformed::no(expr))
                }
            })?.data;
            new_exprs.push(e);
        }
        Ok(new_exprs)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Alias {
    pub child: Box<Expr>,
    pub name: String,
    pub expr_id: u32,
}

impl Alias {
    pub fn new(
        expr: Expr,
        name: impl Into<String>,
    ) -> Self {
        Self {
            child: Box::new(expr),
            name: name.into(),
            expr_id: ExprIdGenerator::get_next_expr_id(),
        }
    }

    pub fn new_with_expr_id(expr: Expr, name: impl Into<String>, expr_id: u32, )-> Self{
        Self {
            child: Box::new(expr),
            name: name.into(),
            expr_id,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Cast {
    pub child: Box<Expr>,
    pub data_type: DataType,
}

impl Cast {
    pub fn new(expr: Expr, data_type: DataType) -> Self {
        Self{child: Box::new(expr), data_type}
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
    counter: std::sync::atomic::AtomicU32,
}

impl ExprIdGenerator {
    fn get_next_expr_id() -> u32 {
        static INSTANCE: ExprIdGenerator = ExprIdGenerator {
            counter: std::sync::atomic::AtomicU32::new(0),
        };
        INSTANCE.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct In {
    pub value: Box<Expr>,
    pub list: Vec<Expr>,
}

impl In {
    pub fn new(value: Box<Expr>, list: Vec<Expr>) -> Self {
        Self { value, list }
    }
}

pub trait ScalarFunction: Debug + Send + Sync + CloneScalarFunction {
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &str;
    fn foldable(&self) -> bool {
        self.args().iter().all(|arg| arg.foldable())
    }
    fn data_type(&self) -> &DataType;
    fn args(&self) -> Vec<&Expr>;
    fn check_input_data_types(&self) -> Result<()>;
    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction>;
}

pub trait CloneScalarFunction {
    fn clone_box(&self) -> Box<dyn ScalarFunction>;
}

impl<T: ScalarFunction + Clone + 'static> CloneScalarFunction for T {
    fn clone_box(&self) -> Box<dyn ScalarFunction> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ScalarFunction> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialEq for Box<dyn ScalarFunction> {
    fn eq(&self, other: &Self) -> bool {
        if self.as_any().type_id() != other.as_any().type_id() {
            return false;
        }
        let args1 = self.args();
        let args2 = other.args();
        if args1.len() != args2.len() {
            return false;
        };
        args1.iter().zip(args2.iter()).all(|(a, b)| a == b)
    }
}

impl Eq for Box<dyn ScalarFunction> {}

impl PartialOrd for Box<dyn ScalarFunction> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let args1 = self.args();
        let args2 = other.args();
        if args1.len() != args2.len() {
            return None;
        };
        for i in 0..args1.len() {
            match args1[i].partial_cmp(args2[i]) {
                None => return None, // 某个元素无法比较
                Some(Ordering::Equal) => continue, // 继续比较下一个元素
                Some(ord) => return Some(ord), // 返回当前元素的比较结果
            }
        }
        // 所有元素都相等
        Some(Ordering::Equal)
    }
}

impl Hash for Box<dyn ScalarFunction> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for x in self.args() {
            x.hash(state);
        }
    }
}