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

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_types() {
        let fields = vec![Field::new("id", DataType::Int), Field::new("name", DataType::String)];
        let schema = Schema { fields:fields.clone() };
        println!("{:?}", schema);
        let struct_type = DataType::Struct(Fields(fields.clone()));
        println!("{:?}", struct_type);
    }
}
