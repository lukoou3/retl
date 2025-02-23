use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use itertools::Itertools;
use crate::expr::AttributeReference;

static NULL_TYPE: DataType = DataType::Null;
static INT_TYPE: DataType = DataType::Int;
static LONG_TYPE: DataType = DataType::Long;
static FLOAT_TYPE: DataType = DataType::Float;
static DOUBLE_TYPE: DataType = DataType::Double;
static BOOLEAN_TYPE: DataType = DataType::Boolean;
static STRING_TYPE: DataType = DataType::String;
static BINARY_TYPE: DataType = DataType::Binary;


#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum DataType {
    Null,
    Int,
    Long,
    Float,
    Double,
    String,
    Boolean,
    Binary,
    Struct(Fields),
    Array(Box<DataType>),
}

impl DataType {
    pub fn is_numeric_type(&self) -> bool {
        match self {
            DataType::Int | DataType::Long | DataType::Float | DataType::Double => true,
            _ => false
        }
    }

    pub fn null_type() -> &'static DataType {
        &NULL_TYPE
    }

    pub fn int_type() -> &'static DataType {
        &INT_TYPE
    }

    pub fn long_type() -> &'static DataType {
        &LONG_TYPE
    }

    pub fn float_type() -> &'static DataType {
        &FLOAT_TYPE
    }

    pub fn double_type() -> &'static DataType {
        &DOUBLE_TYPE
    }

    pub fn string_type() -> &'static DataType {
        &STRING_TYPE
    }

    pub fn boolean_type() -> &'static DataType {
        &BOOLEAN_TYPE
    }

    pub fn binary_type() -> &'static DataType {
        &BINARY_TYPE
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Null => write!(f, "null"),
            DataType::Int => write!(f, "int"),
            DataType::Long => write!(f, "long"),
            DataType::Float => write!(f, "float"),
            DataType::Double => write!(f, "double"),
            DataType::String => write!(f, "string"),
            DataType::Boolean => write!(f, "boolean"),
            DataType::Binary => write!(f, "binary"),
            DataType::Struct(fields) => write!(f, "struct<{}>", fields.0.iter().map(|field| format!("{}: {}", field.name, field.data_type)).join(",")),
            DataType::Array(element_type) => write!(f, "array<{}>", element_type),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
}

impl Display for Field {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)
    }
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Field {
        Field { name: name.into(), data_type, }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Fields(pub Vec<Field>);



#[derive(Clone, Debug)]
pub struct Schema {
    pub fields: Vec<Field>,
    pub name_to_index: HashMap<String, usize>,
    pub name_to_field: HashMap<String, Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Schema {
        let name_to_field = fields.iter().map(|field| (field.name.clone(), field.clone())).collect();
        let name_to_index = fields.iter().enumerate().map(|(i, field)| (field.name.clone(), i)).collect();
        Schema {fields, name_to_index, name_to_field }
    }

    pub fn field_names(&self) -> Vec<String> {
        self.fields.iter().map(|field| field.name.clone()).collect()
    }

    pub fn get_filed_by_name(&self, name: &str) -> Option<&Field> {
        self.name_to_field.get(name)
    }

    pub fn field_types(&self) -> HashMap<String, DataType> {
        self.fields.iter().map(|field| (field.name.clone(), field.data_type.clone())).collect()
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).cloned()
    }

    pub fn field_type(&self, name: &str) -> Option<DataType> {
        self.name_to_field.get(name).map(|field| field.data_type.clone())
    }

    pub fn to_attributes(&self) -> Vec<AttributeReference> {
        self.fields.iter().map(|field| AttributeReference::new(field.name.clone(), field.data_type.clone())).collect()
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Schema[")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", field)?;
        }
        write!(f, "]")
    }
}


impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Eq for Schema {}

impl PartialOrd for Schema {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.fields.partial_cmp(&other.fields)
    }
}

impl Hash for Schema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_types() {
        let fields = vec![Field::new("id", DataType::Int), Field::new("name", DataType::String)];
        let schema = Schema::new(fields.clone());
        println!("{:?}", schema);
        println!("{}", schema);
        let struct_type = DataType::Struct(Fields(fields.clone()));
        println!("{:?}", struct_type);
        println!("{}", struct_type);
    }
}
