use std::fmt::{Display, Formatter};
use itertools::Itertools;

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum DataType {
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
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
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



#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Schema {
    pub fields: Vec<Field>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_types() {
        let fields = vec![Field::new("id", DataType::Int), Field::new("name", DataType::String)];
        let schema = Schema { fields:fields.clone() };
        println!("{:?}", schema);
        println!("{}", schema);
        let struct_type = DataType::Struct(Fields(fields.clone()));
        println!("{:?}", struct_type);
        println!("{}", struct_type);
    }
}
