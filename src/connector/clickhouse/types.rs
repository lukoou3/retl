use std::borrow::Cow;
use std::fmt::Display;
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

#[derive(Clone, Debug)]
pub enum ClickHouseType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Date,
    DateTime,
    DateTime64(u32),
    Nullable(Box<ClickHouseType>),
    Array(Box<ClickHouseType>),
}

impl ClickHouseType {
    pub fn is_datetime(&self) -> bool {
        matches!(self, ClickHouseType::DateTime | ClickHouseType::DateTime64(_))
    }

    pub fn to_string(&self) -> Cow<'static, str> {
        match self {
            ClickHouseType::Bool => "Bool".into(),
            ClickHouseType::Int8 => "Int8".into(),
            ClickHouseType::Int16 => "Int16".into(),
            ClickHouseType::Int32 => "Int32".into(),
            ClickHouseType::Int64 => "Int64".into(),
            ClickHouseType::UInt8 => "UInt8".into(),
            ClickHouseType::UInt16 => "UInt16".into(),
            ClickHouseType::UInt32 => "UInt32".into(),
            ClickHouseType::UInt64 => "UInt64".into(),
            ClickHouseType::Float32 => "Float32".into(),
            ClickHouseType::Float64 => "Float64".into(),
            ClickHouseType::String => "String".into(),
            ClickHouseType::Date => "Date".into(),
            ClickHouseType::DateTime => "DateTime".into(),
            ClickHouseType::DateTime64(precision) => format!("DateTime64({precision})").into(),
            ClickHouseType::Nullable(inner) => format!("Nullable({})", inner).into(),
            ClickHouseType::Array(inner) => format!("Array({})", inner).into(),
        }
    }
}

impl Display for ClickHouseType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}


macro_rules! matches_any_ignore_case {
    ($var:expr, $($pattern:literal)|+) => {
        {
            let s = $var;
            false $(|| s.eq_ignore_ascii_case($pattern))+
        }
    };
}

#[derive(Parser)]
#[grammar = "connector/clickhouse/types.pest"]
pub struct TypeParser;

pub fn parse_date_type(input: &str) -> Option<ClickHouseType> {
    TypeParser::parse(Rule::singleDataType, input).ok()?.next()
        .and_then(|pair| parse_pair_type(pair.into_inner().next().unwrap()))
}

fn parse_pair_type(pair: Pair<Rule>) -> Option<ClickHouseType> {
    match pair.as_rule() {
        Rule::dataType => {
            parse_pair_type(pair.into_inner().next().unwrap())
        },
        Rule::arrayDataType => {
            parse_pair_type(pair.into_inner().next().unwrap())
                .map(|inner| ClickHouseType::Array(Box::new(inner)))
        },
        Rule::nullableDataType => {
            parse_pair_type(pair.into_inner().next().unwrap())
                .map(|inner| ClickHouseType::Nullable(Box::new(inner)))
        },
        Rule::primitiveDataType => {
            let pairs:Vec<_> = pair.into_inner().collect();
            match pairs[0].as_str() {
                s if matches_any_ignore_case!(s, "Bool") => Some(ClickHouseType::Bool),
                s if matches_any_ignore_case!(s, "UInt8") => Some(ClickHouseType::UInt8),
                s if matches_any_ignore_case!(s, "UInt16") => Some(ClickHouseType::UInt16),
                s if matches_any_ignore_case!(s, "UInt32") => Some(ClickHouseType::UInt32),
                s if matches_any_ignore_case!(s, "UInt64") => Some(ClickHouseType::UInt64),
                s if matches_any_ignore_case!(s, "Int8" | "TinyInt") => Some(ClickHouseType::Int8),
                s if matches_any_ignore_case!(s, "Int16" | "SmallInt") => Some(ClickHouseType::Int16),
                s if matches_any_ignore_case!(s, "Int32" | "Int" | "Integer" ) => Some(ClickHouseType::Int32),
                s if matches_any_ignore_case!(s, "Int64" | "BigInt") => Some(ClickHouseType::Int64),
                s if matches_any_ignore_case!(s, "Float32" | "Float") => Some(ClickHouseType::Float32),
                s if matches_any_ignore_case!(s, "Float64" | "Double") => Some(ClickHouseType::Float64),
                s if matches_any_ignore_case!(s, "String" | "Char" | "Varchar" | "Text" | "TinyText" | "MediumText" | "LongText" | "Blob" | "TinyBlob" | "MediumBlob" | "LongBlob") => Some(ClickHouseType::String),
                s if matches_any_ignore_case!(s, "Date") => Some(ClickHouseType::Date),
                s if matches_any_ignore_case!(s, "DateTime") => Some(ClickHouseType::DateTime),
                s if matches_any_ignore_case!(s, "DateTime64") => {
                    if pairs.len() > 1 {
                        pairs[1].as_str().parse::<u32>().map(|precision| ClickHouseType::DateTime64(precision)).ok()
                    }else {
                        None
                    }
                },
                s => None
            }
        },
        _ => None
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_parse_type() {
        let test_cases = vec![
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Float32",
            "Float64",
            "String",
            "Date",
            "DateTime",
            "DateTime64(3)",
            "Array(Int32)",
            "Nullable(String)",
            "Nullable(Int32)",
            "LowCardinality(String)",
        ];

        for test_case in test_cases {
            let result = parse_date_type(test_case);
            println!("{} => {:?}", test_case, result);
        }
    }
}