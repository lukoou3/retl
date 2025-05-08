use crate::{parser, Result};
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, Literal, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::{AbstractDataType, DataType, Schema};

#[derive(Debug, Clone)]
pub struct GetJsonObject {
    pub json: Box<Expr>,
    pub path: Box<Expr>,
}

impl GetJsonObject {
    pub fn new(json: Box<Expr>, path: Box<Expr>) -> Self {
        Self{json, path}
    }
}

impl CreateScalarFunction for GetJsonObject {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let json = iter.next().unwrap();
        let path = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(json), Box::new(path))))
    }
}

impl ScalarFunction for GetJsonObject {
    fn name(&self) -> &str {
        "get_json_object"
    }

    fn data_type(&self) -> &DataType {
        DataType::string_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.json, &self.path]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::string_type(), AbstractDataType::string_type()])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{json, path} = self;
        Ok(Box::new(phy::GetJsonObject::new(create_physical_expr(json)?, create_physical_expr(path)?)))
    }
}

#[derive(Debug, Clone)]
pub struct GetJsonInt {
    pub json: Box<Expr>,
    pub path: Box<Expr>,
}

impl GetJsonInt {
    pub fn new(json: Box<Expr>, path: Box<Expr>) -> Self {
        Self{json, path}
    }
}

impl CreateScalarFunction for GetJsonInt {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let json = iter.next().unwrap();
        let path = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(json), Box::new(path))))
    }
}

impl ScalarFunction for GetJsonInt {
    fn name(&self) -> &str {
        "get_json_int"
    }

    fn data_type(&self) -> &DataType {
        DataType::long_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.json, &self.path]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::string_type(), AbstractDataType::string_type()])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{json, path} = self;
        Ok(Box::new(phy::GetJsonInt::new(create_physical_expr(json)?, create_physical_expr(path)?)))
    }
}

#[derive(Debug, Clone)]
pub struct JsonToStructs {
    pub json: Box<Expr>,
    pub schema_expr: Box<Expr>,
    pub schema: Schema,
    pub data_type: DataType,
}

impl JsonToStructs {
    pub fn new(json: Box<Expr>, schema_expr: Box<Expr>) -> Result<Self> {
        match schema_expr.as_ref() {
            Expr::Literal(Literal{value, data_type}) if data_type == DataType::string_type() => {
                let schema_sql =value.get_string();
                let schema = parser::parse_schema(schema_sql)?;
                let data_type = schema.to_struct_type();
                Ok(Self{json, schema_expr, schema, data_type})
            },
            _ => Err("The second argument should be a string literal.".to_string()),
        }
    }
}

impl CreateScalarFunction for JsonToStructs {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let json = iter.next().unwrap();
        let schema_expr = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(json), Box::new(schema_expr))?))
    }
}

impl ScalarFunction for JsonToStructs {
    fn name(&self) -> &str {
        "from_json"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.json, &self.schema_expr]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Collection(vec![AbstractDataType::string_type(), AbstractDataType::binary_type()]), AbstractDataType::string_type()])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{json, schema, ..} = self;
        Ok(Box::new(phy::JsonToStructs::new(create_physical_expr(json)?, schema.clone())))
    }
}
