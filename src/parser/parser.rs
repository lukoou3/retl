use std::fmt::Debug;
use std::sync::Arc;
use itertools::Itertools;
use pest::{
    iterators::{Pair},
    Parser,
};
use pest_derive::Parser;
use serde_json::Value as JValue;
use crate::{Operator, Result};
use crate::data::Value;
use crate::expr::{BinaryOperator, CaseWhen, Cast, Expr, In, Like, Literal, UnaryMinus, BitwiseNot,UnresolvedExtractValue, UnresolvedFunction, UnresolvedGenerator};
use crate::logical_plan::{Aggregate, Filter, Generate, LogicalPlan, Project, SubqueryAlias};
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

pub fn parse_expr(sql: &str) -> Result<Expr> {
    let pair = SqlParser::parse(Rule::singleExpression, sql).map_err(|e| format!("{:?}", e))?.next().unwrap();
    match parse_ast(pair)? {
        Ast::Expression(expr) => Ok(expr),
        x => Err(format!("not a expression:{:?}", x)),
    }
}

pub fn parse_data_type(sql: &str) -> Result<DataType> {
    let sql_trim = sql.trim();
    let pair = if sql_trim.starts_with("struct<") && sql_trim.ends_with(">") {
        SqlParser::parse(Rule::singleDataType, sql).map_err(|e| format!("{:?}", e))?.next().unwrap()
    } else {
        SqlParser::parse(Rule::singleTableSchema, sql).map_err(|e| format!("{:?}", e))?.next().unwrap()
    };
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

pub fn parse_ast(mut pair: Pair<Rule>) -> Result<Ast> {
    loop {
        match pair.as_rule() {
            Rule::singleQuery | Rule::singleExpression | Rule::singleDataType =>
                pair = pair.into_inner().next().unwrap(),
            Rule::singleTableSchema => return parse_single_table_schema(pair),
            Rule::queryPrimary => return parse_query_primary_ast(pair),
            Rule::tableNameRelation => return parse_table_name_relation_ast(pair),
            Rule::subqueryAliasRelation => return parse_subquery_alias_relation_ast(pair),
            Rule::namedExpressionSeq => return parse_named_expression_seq(pair).map(|x| Ast::Projects(x)),
            Rule::functionCall => return parse_function_call(pair).map(|x| Ast::Expression(x)),
            Rule::constant => return parse_constant(pair).map(|x| Ast::Expression(x)),
            Rule::star => return parse_star(pair).map(|x| Ast::Expression(x)),
            Rule::columnReference => return parse_column_reference(pair).map(|x| Ast::Expression(x)),
            Rule::cast => return parse_cast(pair).map(|x| Ast::Expression(x)),
            Rule::searchedCase => return parse_searched_case(pair).map(|x| Ast::Expression(x)),
            Rule::simpleCase => return parse_simple_case(pair).map(|x| Ast::Expression(x)),
            Rule::logicalNotExpression => return parse_logical_not_expression_ast(pair),
            Rule::predicateExpression => return parse_predicate_expression(pair).map(|x| Ast::Expression(x)),
            Rule::logicalAndExpression => return parse_logical_and_expression_ast(pair),
            Rule::logicalOrExpression => return parse_logical_or_expression_ast(pair),
            Rule::addSubExpression | Rule::mulDivExpression | Rule::bitShiftExpression | Rule::bitAndExpression | Rule::bitXorExpression | Rule::bitOrExpression => 
                return parse_arithmetic_expression(pair).map(|x| Ast::Expression(x)),
            Rule::comparisonExpression => return parse_comparison_expression(pair).map(|x| Ast::Expression(x)),
            Rule::unaryExpression => return parse_unary_expression_ast(pair),
            Rule::primaryExpression => return parse_primary_expression_ast(pair),
            Rule::arrayDataType => return parse_array_data_type(pair).map(|x| Ast::DataType(x)),
            Rule::structDataType => return parse_struct_data_type(pair).map(|x| Ast::DataType(x)),
            Rule::primitiveDataType => return parse_primitive_data_type(pair).map(|x| Ast::DataType(x)),
            _ => {
                let mut pairs = pair.into_inner();
                if pairs.len() == 1 {
                    pair = pairs.next().unwrap();
                    // return parse_ast(pairs.next().unwrap());
                } else {
                    return Err(format!("Expected a single child but found {}:{}", pairs.len(), pairs.into_iter().map(|pair| pair.as_str()).join(", ")))
                }
            }
        }
    }
}

fn parse_identifier(mut pair: Pair<Rule>) -> Result<&str> {
    loop {
        match pair.as_rule() {
            Rule::unquotedIdentifier => return Ok(pair.as_str()),
            Rule::quotedIdentifier => {
                let s = pair.as_str();
                assert!(s.starts_with('`') && s.ends_with('`'));
                return Ok( &s[1..s.len() - 1]);
            },
            _ => {
                let mut pairs = pair.into_inner();
                if pairs.len() == 1 {
                    pair = pairs.next().unwrap();
                } else {
                    return Err(format!("identifier expected a single child but found {}:{}", pairs.len(), pairs.into_iter().map(|pair| pair.as_str()).join(", ")))
                }
            }
        }
    }
}

fn parse_query_primary_ast(pair: Pair<Rule>) -> Result<Ast> {
    let query = pair;
    let mut project_list: Vec<_> = Vec::new();
    let mut from: Option<LogicalPlan> = Some(LogicalPlan::OneRowRelation);
    let mut filter: Option<Expr> = None;
    let mut lateral_view: Option<Generate> = None;
    let mut group_exprs: Option<Vec<Expr>> = None;
    for pair in query.into_inner() {
        match pair.as_rule() {
            Rule::selectClause => {
                let ast = parse_ast(pair)?;
                if let Ast::Projects(projects) = ast {
                    project_list = projects;
                } else {
                    return Err(format!("Expected a projects but found {:?}", ast));
                }
            },
            Rule::fromClause => {
                let ast = parse_ast(pair.into_inner().next().unwrap())?;
                if let Ast::Plan(plan) = ast {
                    from = Some(plan);
                } else {
                    return Err(format!("Expected a logical plan but found {:?}", ast));
                }
            },
            Rule::whereClause => {
                filter = Some(parse_expression(pair)?);
            },
            Rule::lateralView => {
                let mut pairs = pair.into_inner();
                let first = pairs.next().unwrap();
                let outer = first.as_rule() == Rule::OUTER;
                let name = if outer {
                    parse_identifier(pairs.next().unwrap())?.to_string()
                } else {
                    parse_identifier(first)?.to_string()
                };
                let args_pair = pairs.next().unwrap();
                let arguments:Vec<_> = args_pair.into_inner().map(parse_expression).try_collect()?;
                let table_name = parse_identifier(pairs.next().unwrap())?.to_string();
                let col_names: Vec<_> = pairs.map(|pair| parse_identifier(pair).map(|i| i.to_string())).try_collect()?;
                let generator = Expr::UnresolvedGenerator(UnresolvedGenerator{ name, arguments});
                let generator_output: Vec<_> = col_names.into_iter().map(|name| Expr::attr_quoted(name)).collect();
                lateral_view = Some(Generate::new(generator, vec![], outer, Some(table_name), generator_output, Arc::new(LogicalPlan::UnresolvedRelation("".to_string()))));
            },
            Rule::aggregationClause => {
                group_exprs = Some(pair.into_inner().map(parse_expression).try_collect()?);
            },
            _ => {}
        }
    }
    let mut child = Arc::new(from.unwrap());
    if let Some(mut generate) = lateral_view {
        generate.child = child;
        child = Arc::new(LogicalPlan::Generate(generate));
    }
    if let Some(filter) = filter {
        child = Arc::new(LogicalPlan::Filter(Filter::new(filter, child)));
    }
    if let Some(group_exprs) = group_exprs {
        Ok(Ast::Plan(LogicalPlan::Aggregate(Aggregate::new(group_exprs, project_list, child))))
    } else {
        Ok(Ast::Plan(LogicalPlan::Project(Project::new(project_list, child))))
    }
}

fn parse_table_name_relation_ast(pair: Pair<Rule>) -> Result<Ast> {
    let mut pairs = pair.into_inner();
    let name = parse_identifier(pairs.next().unwrap())?.to_string();
    let mut p = LogicalPlan::UnresolvedRelation(name);
    if let Some(pair) = pairs.next() {
        let alias = parse_identifier(pair)?.to_string();
        p = LogicalPlan::SubqueryAlias(SubqueryAlias::new(alias, Arc::new(p)));
    }
    Ok(Ast::Plan(p))
}

fn parse_subquery_alias_relation_ast(pair: Pair<Rule>) -> Result<Ast> {
    let mut pairs = pair.into_inner();
    let ast = parse_query_primary_ast(pairs.next().unwrap())?;
    if let Ast::Plan(plan) = ast {
        let pair_option = pairs.next();
        if let Some(pair) = pair_option {
            let name = parse_identifier(pair)?.to_string();
            Ok(Ast::Plan(LogicalPlan::SubqueryAlias(SubqueryAlias::new(name, Arc::new(plan)))))
        } else {
            Ok(Ast::Plan(plan))
        }
    } else {
        Err(format!("Expected a plan but found {:?}", ast))
    }
}

fn parse_single_table_schema(pair: Pair<Rule>) -> Result<Ast> {
    parse_col_type_list(pair.into_inner().next().unwrap()).map(|x| Ast::DataType(x))
}

fn parse_logical_not_expression_ast(pair: Pair<Rule>) -> Result<Ast> {
    let mut pairs:Vec<_> = pair.clone().into_inner().collect();
    let mut expr = parse_expression(pairs.last().unwrap().clone())?;
    if pairs.len() == 2 {
        expr = Expr::Not(Box::new(expr));
    }
    Ok(Ast::Expression(expr))
}

fn parse_logical_and_expression_ast(pair: Pair<Rule>) -> Result<Ast> {
    let mut pairs = pair.clone().into_inner();
    let mut expr = parse_expression(pairs.next().unwrap())?;
    for pair in pairs {
        expr = Expr::BinaryOperator(BinaryOperator::new(Box::new(expr), Operator::And, Box::new(parse_expression(pair)?)));
    }
    Ok(Ast::Expression(expr))
}

fn parse_logical_or_expression_ast(pair: Pair<Rule>) -> Result<Ast> {
    let mut pairs = pair.clone().into_inner();
    let mut expr = parse_expression(pairs.next().unwrap())?;
    for pair in pairs {
        expr = Expr::BinaryOperator(BinaryOperator::new(Box::new(expr), Operator::Or, Box::new(parse_expression(pair)?)));
    }
    Ok(Ast::Expression(expr))
}

fn parse_unary_expression_ast(pair: Pair<Rule>) -> Result<Ast> {
    let mut pairs = pair.into_inner();
    if pairs.len() == 1 {
        Ok(Ast::Expression(parse_expression(pairs.next().unwrap())?))
    } else {
        match pairs.next().unwrap().as_rule() {
            Rule::PLUS => {
                let expr = parse_expression(pairs.next().unwrap())?;
                Ok(Ast::Expression(expr))
            },
            Rule::MINUS => {
                let expr = parse_expression(pairs.next().unwrap())?;
                Ok(Ast::Expression(Expr::ScalarFunction(Box::new(UnaryMinus::new(Box::new(expr))))))
            },
            Rule::TILDE => {
                let expr = parse_expression(pairs.next().unwrap())?;
                Ok(Ast::Expression(Expr::ScalarFunction(Box::new(BitwiseNot::new(Box::new(expr))))))
            },
            r => Err(format!("Expected a unary expression but found {:?}", r))
        }
    }
}

fn parse_primary_expression_ast(pair: Pair<Rule>) -> Result<Ast> {
    let mut pairs = pair.into_inner();
    let mut expr = parse_expression(pairs.next().unwrap())?;
    for pair in pairs {
        match pair.as_rule() {
            Rule::subscriptOp => {
                let index = parse_expression(pair)?;
                expr = Expr::UnresolvedExtractValue(UnresolvedExtractValue::new(Box::new(expr), Box::new(index)));
            },
            Rule::dereferenceOp => {
                let attr = parse_identifier(pair.into_inner().next().unwrap())?.to_string();
                expr = match expr {
                    Expr::UnresolvedAttribute(mut name_parts) => {
                        name_parts.push(attr);
                        Expr::UnresolvedAttribute(name_parts)
                    },
                    e => Expr::UnresolvedExtractValue(UnresolvedExtractValue::new(Box::new(e), Box::new(Expr::string_lit(attr))))
                };
            },
            _ => return Err(format!("Expected a subscriptOp expression but found {:?}", pair))
        }
    }
    Ok(Ast::Expression(expr))
}

fn parse_col_type_list(pair: Pair<Rule>) -> Result<DataType> {
    let mut fields = Vec::new();
    for col_type in pair.into_inner() {
        let mut pairs = col_type.into_inner();
        let name = parse_identifier(pairs.next().unwrap())?.to_string();
        let tp = parse_ast(pairs.next().unwrap())?;
        if let Ast::DataType(data_type) = tp {
            fields.push(Field { name, data_type });
        } else {
            return Err(format!("Expected a data type but found {:?}", tp));
        }
    }
    Ok(DataType::Struct(Fields(fields)))
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
    let mut fields = Vec::new();
    for complex in pair.into_inner() {
        let mut pairs = complex.into_inner();
        let name = parse_identifier(pairs.next().unwrap())?.to_string();
        let tp = parse_ast(pairs.next().unwrap())?;
        if let Ast::DataType(data_type) = tp {
            fields.push(Field { name, data_type });
        } else {
            return Err(format!("Expected a data type but found {:?}", tp));
        }
    }
    Ok(DataType::Struct(Fields(fields)))
}

fn parse_primitive_data_type(pair: Pair<Rule>) -> Result<DataType> {
    let tp = parse_identifier(pair)?.to_string();
    let tp_lower = tp.to_lowercase();
    match tp_lower.as_str() {
        "boolean" => Ok(DataType::Boolean),
        "int" | "integer" => Ok(DataType::Int),
        "bigint" | "long" => Ok(DataType::Long),
        "float" | "real" => Ok(DataType::Float),
        "double" => Ok(DataType::Double),
        "string" => Ok(DataType::String),
        "date" => Ok(DataType::Date),
        "timestamp" => Ok(DataType::Timestamp),
        "binary" => Ok(DataType::Binary),
        _ => Err(format!("not supported data type: {}", tp))
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
                    e => named_expressions.push(Expr::UnresolvedAlias(Box::new(e))),
                }
            } else {
                return Err(format!("Expected a projects but found {:?}", ast));
            }
        } else {
            let ast = parse_ast(pairs[0].clone())?;
            let name = parse_identifier(pairs.last().unwrap().clone())?;
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
    let ast = parse_ast(pair)?;
    if let Ast::Expression(e) = ast {
        Ok(e)
    } else {
        Err(format!("expected a expression but found {:?}", ast))
    }
}

fn parse_datatype(pair: Pair<Rule>) -> Result<DataType> {
    let ast = parse_ast(pair)?;
    if let Ast::DataType(d) = ast {
        Ok(d)
    } else {
        Err(format!("expected a data type but found {:?}", ast))
    }
}

fn parse_function_call(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let name = parse_identifier(pairs.next().unwrap())?.to_string();
    let args_pair = pairs.next().unwrap();
    let mut arguments:Vec<_> = args_pair.into_inner().map(parse_expression).try_collect()?;
    // Transform count(*) into count(1).
    if arguments.len() == 1 && name.to_lowercase() == "count" {
        arguments = match & arguments[0]{
            Expr::UnresolvedStar(target) if target.is_empty() => vec![Expr::int_lit(1)],
            _ => arguments,
        };
    }
    Ok(Expr::UnresolvedFunction(UnresolvedFunction{name, arguments}))
}

fn parse_cast(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let expr = parse_expression(pairs.next().unwrap())?;
    let data_type = parse_datatype(pairs.next().unwrap())?;
    Ok(Expr::Cast(Cast::new(expr, data_type)))
}

fn parse_searched_case(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let mut branches = Vec::new();
    let mut else_value = Expr::null_lit();
    for when_else in pairs {
        match when_else.as_rule() {
            Rule::whenClause => {
                let mut when = when_else.into_inner();
                let condition = parse_expression(when.next().unwrap())?;
                let value = parse_expression(when.next().unwrap())?;
                branches.push((condition, value));
            },
            _ => {
                else_value = parse_expression(when_else)?;
            }
        }
    }
    Ok(Expr::ScalarFunction(Box::new(CaseWhen::new(branches, Box::new(else_value)))))
}

fn parse_simple_case(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let expr = parse_expression(pairs.next().unwrap())?;
    let mut branches = Vec::new();
    let mut else_value = Expr::null_lit();
    for when_else in pairs {
        match when_else.as_rule() {
            Rule::whenClause => {
                let mut when = when_else.into_inner();
                let condition = parse_expression(when.next().unwrap())?;
                let value = parse_expression(when.next().unwrap())?;
                branches.push((condition.eq(expr.clone()), value));
            },
            _ => {
                else_value = parse_expression(when_else)?;
            }
        }
    }
    Ok(Expr::ScalarFunction(Box::new(CaseWhen::new(branches, Box::new(else_value)))))
}

fn parse_predicate_expression(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let expr = parse_expression(pairs.next().unwrap())?;
    if let Some(p) = pairs.next() {
        let predicate = p.into_inner().next().unwrap();
        match predicate.as_rule() {
            Rule::predicateBetween => {
                let mut pairs = predicate.into_inner();
                let no = pairs.len() > 2;
                if no {
                    pairs.next();
                }
                let lower = parse_expression(pairs.next().unwrap())?;
                let upper = parse_expression(pairs.next().unwrap())?;

                Ok(expr.clone().ge(lower).and(expr.le(upper)))
            },
            Rule::predicateIn => {
                let pairs:Vec<_> = predicate.into_inner().collect();
                let no = pairs[0].as_rule() == Rule::NOT;
                let list:Vec<_> = if no {
                    pairs.into_iter().skip(1).map(|pair| {parse_expression(pair).unwrap()}).collect()
                } else {
                    pairs.into_iter().map(|pair| {parse_expression(pair).unwrap()}).collect()
                };
                let expr = Expr::In(In::new(Box::new(expr), list));
                if no {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            },
            Rule::predicateNull => {
                let pairs:Vec<_> = predicate.into_inner().collect();
                if pairs.len() == 0 {
                    Ok(Expr::IsNull(Box::new(expr)))
                } else {
                    Ok(Expr::IsNotNull(Box::new(expr)))
                }
            },
            Rule::predicateLike => {
                let mut pairs = predicate.into_inner();
                let no = pairs.len() > 1;
                if no {
                    pairs.next();
                }
                let regex = parse_expression(pairs.next().unwrap())?;
                Ok(Expr::Like(Like::new(Box::new(expr), Box::new(regex))))
            },
            Rule::predicateRlike => {
                let mut pairs = predicate.into_inner();
                let no = pairs.len() > 1;
                if no {
                    pairs.next();
                }
                let regex = parse_expression(pairs.next().unwrap())?;
                Ok(Expr::RLike(Like::new(Box::new(expr), Box::new(regex))))
            },
            _ => {
                Err(format!("Expected a predicate but found {:?}", predicate))
            }
        }
    } else {
        Ok(expr)
    }
}

fn parse_arithmetic_expression(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let mut left = parse_expression(pairs.next().unwrap())?;
    let mut arithmetic_option = pairs.next();
    while let Some(arithmetic) = arithmetic_option {
        let right = parse_expression(pairs.next().unwrap())?;
        match arithmetic.as_rule() {
            Rule::PLUS => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Plus, Box::new(right))),
            Rule::MINUS => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Minus, Box::new(right))),
            Rule::ASTERISK => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Multiply, Box::new(right))),
            Rule::SLASH => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Divide, Box::new(right))),
            Rule::PERCENT => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::Modulo, Box::new(right))),
            Rule::AMPERSAND => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::BitAnd, Box::new(right))),
            Rule::HAT => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::BitXor, Box::new(right))),
            Rule::PIPE => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::BitOr, Box::new(right))),
            Rule::SHIFT_LEFT => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::BitShiftLeft, Box::new(right))),
            Rule::SHIFT_RIGHT => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::BitShiftRight, Box::new(right))),
            Rule::SHIFT_RIGHT_UNSIGNED => left = Expr::BinaryOperator(BinaryOperator::new(Box::new(left), Operator::BitShiftRightUnsigned, Box::new(right))),
            _ => return Err(format!("Unexpected arithmetic {:?}", arithmetic))
        }
        arithmetic_option = pairs.next();
    }
    Ok(left)
}

fn parse_comparison_expression(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs = pair.into_inner();
    let left = parse_expression(pairs.next().unwrap())?;
    let comparison_option = pairs.next();
    if comparison_option.is_none() {
        return Ok(left);
    }
    let comparison = comparison_option.unwrap().into_inner().next().unwrap();
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
            let num = p.into_inner().next().unwrap();
            match num.as_rule() {
                Rule::integerLiteral => {
                    let v:JValue = serde_json::from_str(num.as_str()).unwrap();
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
                Rule::bigIntLiteral => {
                    let s = num.as_str();
                    let s = &s[0.. s.len() - 1];
                    Ok(Expr::Literal(Literal::new(Value::Long(s.parse::<i64>().map_err(|e| format!("parse bigint error:{}", e) ) ?), DataType::Long)))
                },
                Rule::decimalLiteral => {
                    let s = num.as_str();
                    Ok(Expr::Literal(Literal::new(Value::Double(s.parse::<f64>().map_err(|e| format!("parse double error:{}", e) ) ?), DataType::Double)))
                },
                Rule::floatLiteral => {
                    let s = num.as_str();
                    let s = &s[0.. s.len() - 1];
                    Ok(Expr::Literal(Literal::new(Value::Float(s.parse::<f32>().map_err(|e| format!("parse float error:{}", e) ) ?), DataType::Float)))
                },
                Rule::doubleLiteral => {
                    let s = num.as_str();
                    let s = &s[0.. s.len() - 1];
                    Ok(Expr::Literal(Literal::new(Value::Double(s.parse::<f64>().map_err(|e| format!("parse double error:{}", e) ) ?), DataType::Double)))
                },
                _ => Err(format!("Unexpected parse constant number {:?}", num))
            }
        },
        Rule::booleanValue => {
            let s = pair.as_str().trim().to_lowercase();
            match s.as_str() {
                "true" => Ok(Expr::Literal(Literal::new(Value::Boolean(true), DataType::Boolean))),
                "false" => Ok(Expr::Literal(Literal::new(Value::Boolean(false), DataType::Boolean))),
                _ => Err(format!("Unexpected parse_constant {:?}", p))
            }
        },
        Rule::STRING => parse_string_constant(pair),
        _ => Err(format!("Unexpected parse_constant {:?}", p))
    }
}

fn parse_star(pair: Pair<Rule>) -> Result<Expr> {
    let mut pairs:Vec<_> = pair.clone().into_inner().collect();
    if pairs.len() <= 1 {
        Ok(Expr::UnresolvedStar(Vec::new()))
    } else {
        let n = pairs.len() - 1;
        let target: Vec<_> = pairs.into_iter().take(n).map(|pair| parse_identifier(pair).map(|i| i.to_string())).try_collect()?;
        Ok(Expr::UnresolvedStar(target))
    }
}

fn parse_string_constant(pair: Pair<Rule>) -> Result<Expr> {
    let s = pair.as_str();
    let s = &s[1.. s.len() - 1];

    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(escaped) = chars.next() {
                match escaped {
                    '\\' => result.push('\\'),
                    '\'' => result.push('\''),
                    '"' => result.push('"'),
                    'n' => result.push('\n'),
                    't' => result.push('\t'),
                    'r' => result.push('\r'),
                    _ => result.push(escaped), // 其他转义字符原样保留
                }
            }
        } else {
            result.push(c);
        }
    }

    Ok(Expr::Literal(Literal::new(Value::string(result), DataType::String)))
}

fn parse_column_reference(pair: Pair<Rule>) -> Result<Expr> {
    Ok(Expr::attr_quoted(parse_identifier(pair)?))
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_number() -> Result<()>{
        let sql = r"
        select
            1 a,
            -1 b,
            1111111122222 c,
            1111111122255555552 d,
            1111111122225555555555556666652 e,
            1.2 f,
            1.3f g,
            1.3d h,
            -1.3 i
        from tab
        ";
        let result = parse_query(sql);
        println!("{:?}", result);
        println!("{:#?}", result);
        Ok(())
    }
    #[test]
    fn test_comment() -> Result<()>{
        let sql = r"
        select
            a + 1 a, -- comment1
            -- comment2
            func('1') b,
            /* comment3 */
            data + 10 data,
            /* /* nested */ */
            d,
            'text; -- not comment' e
        from tab
        ";
        let result = parse_query(sql);
        println!("{:?}", result);
        println!("{:#?}", result);
        Ok(())
    }
}