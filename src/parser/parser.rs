use std::fmt::Debug;
use pest::{
    iterators::{Pair},
    Parser,
};
use pest_derive::Parser;
use crate::{Operator, Result};
use crate::data::Value;
use crate::expr::{BinaryOperator, Expr, Literal, UnresolvedFunction};
use crate::types::*;

#[derive(Parser)]
#[grammar = "parser/parser.pest"]
pub struct SqlParser;

#[derive(Debug, Clone)]
pub enum Ast {
    Expression(Expr),
    Project(Project),
    Projects(Vec<Expr>),
    DataType(DataType),
}

#[derive(Clone, Debug)]
pub struct Project{
    projects: Vec<Expr>,
    child: String
}

pub fn parse_query(sql: &str) -> Result<Ast> {
    let pair = SqlParser::parse(Rule::singleQuery, sql).map_err(|e| format!("{:?}", e))?.next().unwrap();
    parse_ast(pair)
}

pub fn parse_data_type(sql: &str) -> Result<Ast> {
    let pair = SqlParser::parse(Rule::singleDataType, sql).map_err(|e| format!("{:?}", e))?.next().unwrap();
    parse_ast(pair)
}

pub fn parse_ast(pair: Pair<Rule>) -> Result<Ast> {
    match pair.as_rule() {
        Rule::queryPrimary => {
            let query = pair;
            let mut project = Project { projects: Vec::new(), child: String::new(), };
            for pair in query.into_inner() {
                match pair.as_rule() {
                    Rule::selectClause => {
                        let ast = parse_ast(pair)?;
                        if let Ast::Projects(projects) = ast {
                            project.projects = projects;
                        } else {
                            return Err(format!("Expected a projects but found {:?}", ast));
                        }
                    }
                    Rule::fromClause => {
                        project.child = pair.into_inner().next().unwrap().as_str().to_string();
                    }
                    _ => {}
                }
            }
            Ok(Ast::Project(project))
        }
        Rule::namedExpressionSeq =>  parse_named_expression_seq(pair).map(|x| Ast::Projects(x)),
        Rule::functionCall => parse_function_call(pair).map(|x| Ast::Expression(x)),
        Rule::constant => parse_constant(pair).map(|x| Ast::Expression(x)),
        Rule::columnReference => parse_column_reference(pair).map(|x| Ast::Expression(x)),
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
                named_expressions.push(e);
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

fn parse_comparison_expression(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let left = parse_expression(pairs.next().unwrap())?;
    let comparison = pairs.next().unwrap().into_inner().next().unwrap();
    let right = parse_expression(pairs.next().unwrap())?;

    match comparison.as_rule() {
        Rule::EQ => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Eq, Box::new(right)))),
        Rule::LT => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Lt, Box::new(right)))),
        Rule::GT => Ok(Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Gt, Box::new(right)))),
        _ => Err(format!("Unexpected comparison {:?}", comparison))
    }
}

fn parse_constant(pair: Pair<Rule>) -> Result<Expr> {
    Ok(Expr::Literal(Literal::new(Value::string(pair.as_str()), DataType::String)))
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