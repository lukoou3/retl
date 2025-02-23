use std::fmt::Debug;
use std::sync::Arc;
use pest::{
    iterators::{Pair},
    Parser,
};
use pest_derive::Parser;
use serde_json::Value as JValue;
use crate::{Operator, Result};
use crate::data::Value;
use crate::expr::{BinaryOperator, Expr, Literal, UnresolvedFunction};
use crate::logical_plan::{LogicalPlan, Project};
use crate::types::*;

#[derive(Parser)]
#[grammar = "parser/parser.pest"]
pub struct SqlParser;

#[derive(Debug, Clone)]
pub enum Ast {
    Expression(Expr),
    Plan(LogicalPlan),
    Projects(Vec<Expr>),
    DataType(DataType),
}

pub fn parse_query(sql: &str) -> Result<LogicalPlan> {
    let pair = SqlParser::parse(Rule::singleQuery, sql).map_err(|e| format!("{:?}", e))?.next().unwrap();
    match parse_ast(pair)? {
        Ast::Plan(plan) => Ok(plan),
        x => Err(format!("not a logical plan:{:?}", x)),
    }
}

pub fn parse_data_type(sql: &str) -> Result<DataType> {
    let pair = SqlParser::parse(Rule::singleDataType, sql).map_err(|e| format!("{:?}", e))?.next().unwrap();
    match parse_ast(pair)? {
        Ast::DataType(dt) => Ok(dt),
        x => Err(format!("not a data type:{:?}", x)),
    }
}

pub fn parse_schema(sql: &str) -> Result<Schema> {
    if let DataType::Struct(fields) =  parse_data_type(sql)? {
        Ok(Schema::new(fields.0))
    } else {
        Err(format!("not a struct type: {}", sql))
    }
}

pub fn parse_ast(pair: Pair<Rule>) -> Result<Ast> {
    match pair.as_rule() {
        Rule::queryPrimary => {
            let query = pair;
            let mut project_list: Vec<_> = Vec::new();
            let mut child: Option<LogicalPlan> = None;
            for pair in query.into_inner() {
                match pair.as_rule() {
                    Rule::selectClause => {
                        let ast = parse_ast(pair)?;
                        if let Ast::Projects(projects) = ast {
                            project_list = projects;
                        } else {
                            return Err(format!("Expected a projects but found {:?}", ast));
                        }
                    }
                    Rule::fromClause => {
                        child = Some(LogicalPlan::UnresolvedRelation(pair.into_inner().next().unwrap().as_str().to_string()));
                    }
                    _ => {}
                }
            }
            Ok(Ast::Plan(LogicalPlan::Project(Project{project_list, child: Arc::new(child.unwrap())})))
        }
        Rule::namedExpressionSeq =>  parse_named_expression_seq(pair).map(|x| Ast::Projects(x)),
        Rule::functionCall => parse_function_call(pair).map(|x| Ast::Expression(x)),
        Rule::constant => parse_constant(pair).map(|x| Ast::Expression(x)),
        Rule::columnReference => parse_column_reference(pair).map(|x| Ast::Expression(x)),
        Rule::arithmeticExpression => parse_arithmetic_expression(pair).map(|x| Ast::Expression(x)),
        Rule::comparisonExpression => parse_comparison_expression(pair).map(|x| Ast::Expression(x)),
        Rule::arrayDataType => parse_array_data_type(pair).map(|x| Ast::DataType(x)),
        Rule::structDataType => parse_struct_data_type(pair).map(|x| Ast::DataType(x)),
        Rule::primitiveDataType => parse_primitive_data_type(pair).map(|x| Ast::DataType(x)),
        _ => {
            let pairs:Vec<_> = pair.clone().into_inner().collect();
            if pairs.len() > 0 {
                parse_ast(pairs[0].clone())
            }else {
                Err(pair.as_str().to_string())
            }
        }
    }
}

fn parse_array_data_type(pair: Pair<Rule>) -> Result<DataType> {
    let ast = parse_ast(pair.into_inner().next().unwrap())?;
    if let Ast::DataType(datatype) = ast {
        Ok(DataType::Array(Box::new(datatype)))
    } else {
        Err(format!("Expected a data type but found {:?}", ast))
    }
}

fn parse_struct_data_type(pair: Pair<Rule>) -> Result<DataType> {
    let fields:Vec<_> = pair.into_inner().map(|complex| {
        let mut pairs = complex.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let tp = parse_ast(pairs.next().unwrap()).unwrap();
        if let Ast::DataType(data_type) = tp {
            Field { name, data_type }
        } else {
            panic!("Expected a data type but found {:?}", tp)
        }
    }).collect();
    Ok(DataType::Struct(Fields(fields)))
}

fn parse_primitive_data_type(pair: Pair<Rule>) -> Result<DataType> {
    match pair.as_str() {
        "boolean" => Ok(DataType::Boolean),
        "int" | "integer" => Ok(DataType::Int),
        "bigint" | "long" => Ok(DataType::Long),
        "float" | "real" => Ok(DataType::Float),
        "double" => Ok(DataType::Double),
        "string" => Ok(DataType::String),
        "binary" => Ok(DataType::Binary),
        _ => Err(format!("not supported data type: {:?}", pair))
    }
}

fn parse_named_expression_seq(pair: Pair<Rule>) -> Result<Vec<Expr>> {
    let mut named_expressions = Vec::new();
    for named_expr in pair.into_inner() {
        let pairs:Vec<_> = named_expr.into_inner().collect();
        if pairs.len() == 1 {
            let ast = parse_ast(pairs[0].clone())?;
            if let Ast::Expression(e) = ast {
                match e {
                    Expr::UnresolvedAttribute(_) | Expr::AttributeReference(_)  => named_expressions.push(e),
                    _ => return Err(format!("Expected a named expr but found {:?}", e)),
                }
            } else {
                return Err(format!("Expected a projects but found {:?}", ast));
            }
        } else {
            let ast = parse_ast(pairs[0].clone())?;
            let name = pairs.last().unwrap().as_str().to_string();
            if let Ast::Expression(child) = ast {
                named_expressions.push(child.alias(name));
            } else {
                return Err(format!("Expected a projects but found {:?}", ast));
            }
        }

    }
    Ok(named_expressions)
}

fn parse_expression(pair: Pair<Rule>) -> Result<Expr> {
    let ast = parse_ast(pair).unwrap();
    if let Ast::Expression(e) = ast {
        Ok(e)
    } else {
        Err(format!("expected a expression but found {:?}", ast))
    }
}

fn parse_function_call(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let args_pair = pairs.next().unwrap();
    let arguments:Vec<_> = args_pair.into_inner().map(|pair| {
        parse_expression(pair).unwrap()
    }).collect();
    Ok(Expr::UnresolvedFunction(UnresolvedFunction{name, arguments}))
}

fn parse_arithmetic_expression(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let left = parse_expression(pairs.next().unwrap())?;
    let arithmetic = pairs.next().unwrap().into_inner().next().unwrap();
    let right = parse_expression(pairs.next().unwrap())?;

    match arithmetic.as_rule() {
        Rule::PLUS => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Plus, Box::new(right)))),
        Rule::MINUS => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Minus, Box::new(right)))),
        Rule::ASTERISK => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Multiply, Box::new(right)))),
        Rule::SLASH => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Divide, Box::new(right)))),
        Rule::PERCENT => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Modulo, Box::new(right)))),
        _ => Err(format!("Unexpected arithmetic {:?}", arithmetic))
    }
}

fn parse_comparison_expression(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let left = parse_expression(pairs.next().unwrap())?;
    let comparison = pairs.next().unwrap().into_inner().next().unwrap();
    let right = parse_expression(pairs.next().unwrap())?;

    match comparison.as_rule() {
        Rule::EQ => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Eq, Box::new(right)))),
        Rule::NEQ | Rule::NEQJ | Rule::NSEQ => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::NotEq, Box::new(right)))),
        Rule::LT => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Lt, Box::new(right)))),
        Rule::LTE => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::LtEq, Box::new(right)))),
        Rule::GT => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Gt, Box::new(right)))),
        Rule::GTE => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::GtEq, Box::new(right)))),
        _ => Err(format!("Unexpected comparison {:?}", comparison))
    }
}

fn parse_constant(pair: Pair<Rule>) -> Result<Expr> {
    let p = pair.clone().into_inner().next().unwrap();
    match p.as_rule() {
        Rule::NULL => Ok(Expr::Literal(Literal::new(Value::Null, DataType::Null))),
        Rule::number => {
            let v:JValue = serde_json::from_str(pair.as_str()).unwrap();
            match v {
                JValue::Number(n) => {
                    if n.is_f64() {
                        Ok(Expr::Literal(Literal::new(Value::Double(n.as_f64().unwrap()), DataType::Double)))
                    } else if n.is_i64() {
                        let v = n.as_i64().unwrap();
                        if v <= i32::MAX as i64 {
                            Ok(Expr::Literal(Literal::new(Value::int(v as i32), DataType::Int)))
                        } else {
                            Ok(Expr::Literal(Literal::new(Value::Long(v), DataType::Long)))
                        }
                    } else {
                        Err(format!("Unexpected parse_constant {:?}", n))
                    }
                },
                _ => Err(format!("Unexpected parse_constant {:?}", v))
            }
        },
        Rule::STRING => Ok(Expr::Literal(Literal::new(Value::string(pair.as_str()), DataType::String))),
        _ => Err(format!("Unexpected parse_constant {:?}", p))
    }
}

fn parse_column_reference(pair: Pair<Rule>) -> Result<Expr> {
    Ok(Expr::UnresolvedAttribute(pair.as_str().to_string()))
}


pub fn parse_query2(sql: &str) -> Result<(), pest::error::Error<Rule>> {
    let query_ast = SqlParser::parse(Rule::singleQuery, sql)?.next().unwrap();

    println!("{:?}", query_ast.as_rule());
    for rule in query_ast.clone().into_inner() {
        for rule in rule.clone().into_inner() {
            println!("{:?}", rule);
            println!("{:?}", rule.as_rule());
            println!("{:?}", rule.as_str());
            println!("{}", "*".repeat(30));
        }

        println!("{:?}", rule);
        println!("{:?}", rule.as_rule());
        println!("{:?}", rule.as_str());
        println!("{}", "#".repeat(30));
    }

    Ok(())
}