use crate::Result;
use crate::codecs::Deserializer;
use crate::data::{GenericRow, Row, Value};
use crate::types::{DataType, Field, Schema};
use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{RecordField, UnionSchema};
use apache_avro::types::Value as AvroValue;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::sync::Arc;

pub struct AvroDeserializer {
    schema: Schema,
    avro_schema: AvroSchema,
    converter: RecordToStructConverter,
    row: GenericRow,
}

impl AvroDeserializer {
    pub fn new(schema: Schema, avro_schema: AvroSchema) -> Result<Self> {
        if let AvroSchema::Record(inner) = &avro_schema {
            let converter = RecordToStructConverter::new(&schema.fields, &inner.fields)?;
            let row = GenericRow::new_with_size(schema.fields.len());
            Ok(Self{schema,avro_schema,converter,row,})
        } else {
            Err(format!("not support schema: {:?}", avro_schema))
        }
    }
}

impl Debug for AvroDeserializer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroDeserializer")
            .field("schema", &self.schema)
            .field("avro_schema", &self.avro_schema)
            .finish()
    }
}

impl Deserializer for AvroDeserializer {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row> {
        let mut cursor = Cursor::new(bytes);
        let value = apache_avro::from_avro_datum(&self.avro_schema, &mut cursor, None).map_err(|e| e.to_string())?;
        self.row.fill_null();
        self.converter.read_row(value, &mut self.row)?;
        Ok(&self.row)
    }
}

fn create_converter(data_type: &DataType, schema: &AvroSchema)  -> Result<Box<dyn ValueConverter>> {
    let converter: Box<dyn ValueConverter> = match schema {
        AvroSchema::Int => match data_type {
             DataType::Int => Box::new(IntToIntConverter),
             DataType::Long => Box::new(IntToLongConverter),
             DataType::Float => Box::new(IntToFloatConverter),
             DataType::Double => Box::new(IntToDoubleConverter),
             DataType::String => Box::new(IntToStringConverter),
             _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Long => match data_type {
            DataType::Int => Box::new(LongToIntConverter),
            DataType::Long => Box::new(LongToLongConverter),
            DataType::Float => Box::new(LongToFloatConverter),
            DataType::Double => Box::new(LongToDoubleConverter),
            DataType::String => Box::new(LongToStringConverter),
            _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Float => match data_type {
            DataType::Int => Box::new(FloatToIntConverter),
            DataType::Long => Box::new(FloatToLongConverter),
            DataType::Float => Box::new(FloatToFloatConverter),
            DataType::Double => Box::new(FloatToDoubleConverter),
            DataType::String => Box::new(FloatToStringConverter),
            _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Double => match data_type {
            DataType::Int => Box::new(DoubleToIntConverter),
            DataType::Long => Box::new(DoubleToLongConverter),
            DataType::Float => Box::new(DoubleToFloatConverter),
            DataType::Double => Box::new(DoubleToDoubleConverter),
            DataType::String => Box::new(DoubleToStringConverter),
            _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Boolean => match data_type {
            DataType::Boolean => Box::new(BooleanToBooleanConverter),
            DataType::String => Box::new(BooleanToStringConverter),
            _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::String => match data_type {
            DataType::String => Box::new(StringToStringConverter),
            _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Bytes => match data_type {
            DataType::Binary => Box::new(BytesToBinaryConverter),
            _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Union(inner) => Box::new(UnionNullConverter::new(data_type, inner)?),
        AvroSchema::Record(inner) => match data_type {
            DataType::Struct(fields) => Box::new(RecordToStructConverter::new(&fields.0, &inner.fields)?),
            _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Array(inner) => match data_type {
            DataType::Array(ele_type) => Box::new(ArrayToArrayConverter::new(ele_type, &inner.items)?),
            _ => return Err(not_match_err(data_type, schema)),
        },
        _ => return Err(format!("not support schema: {:?}", schema)),
    };
    Ok(converter)
}

fn not_match_err(data_type: &DataType, schema: &AvroSchema) -> String {
    format!("not support type: {:?} for schema: {:?}", data_type, schema)
}

trait ValueConverter {
    fn convert(&self, value: AvroValue) -> Result<Value>;
}

macro_rules! impl_number_value_converter {
    ($struct_name:ident, $value1:ident, $value2:ident, $type:ty) => {
        pub struct $struct_name;

        impl ValueConverter for $struct_name {
            fn convert(&self, value: AvroValue) -> Result<Value> {
                match value {
                    AvroValue::$value1(v) => Ok(Value::$value2(v as $type)),
                    v => Err(format!("invalid value for {}: {:?}", stringify!($struct_name), v)),
                }
            }
        }
    };
}

impl_number_value_converter!(IntToIntConverter, Int, Int, i32);
impl_number_value_converter!(LongToIntConverter, Long, Int, i32);
impl_number_value_converter!(FloatToIntConverter, Float, Int, i32);
impl_number_value_converter!(DoubleToIntConverter, Double, Int, i32);

impl_number_value_converter!(IntToLongConverter, Int, Long, i64);
impl_number_value_converter!(LongToLongConverter, Long, Long, i64);
impl_number_value_converter!(FloatToLongConverter, Float, Long, i64);
impl_number_value_converter!(DoubleToLongConverter, Double, Long, i64);

impl_number_value_converter!(IntToFloatConverter, Int, Float, f32);
impl_number_value_converter!(LongToFloatConverter, Long, Float, f32);
impl_number_value_converter!(FloatToFloatConverter, Float, Float, f32);
impl_number_value_converter!(DoubleToFloatConverter, Double, Float, f32);

impl_number_value_converter!(IntToDoubleConverter, Int, Double, f64);
impl_number_value_converter!(LongToDoubleConverter, Long, Double, f64);
impl_number_value_converter!(FloatToDoubleConverter, Float, Double, f64);
impl_number_value_converter!(DoubleToDoubleConverter, Double, Double, f64);

macro_rules! impl_number_to_string_converter {
    ($struct_name:ident, $value1:ident) => {
        pub struct $struct_name;

        impl ValueConverter for $struct_name {
            fn convert(&self, value: AvroValue) -> Result<Value> {
                match value {
                    AvroValue::$value1(v) => Ok(Value::String(Arc::new(v.to_string()))),
                    v => Err(format!("invalid value for {}: {:?}", stringify!($struct_name), v)),
                }
            }
        }
    };
}

impl_number_to_string_converter!(IntToStringConverter, Int);
impl_number_to_string_converter!(LongToStringConverter, Long);
impl_number_to_string_converter!(FloatToStringConverter, Float);
impl_number_to_string_converter!(DoubleToStringConverter, Double);
impl_number_to_string_converter!(BooleanToStringConverter, Boolean);

struct StringToStringConverter;

impl ValueConverter for StringToStringConverter {
    fn convert(&self, value: AvroValue) -> Result<Value> {
        match value {
            AvroValue::String(v) => Ok(Value::String(Arc::new(v))),
            v => Err(format!("invalid value for StringToStringConverter: {:?}", v)),
        }
    }
}

struct BooleanToBooleanConverter;

impl ValueConverter for BooleanToBooleanConverter {
    fn convert(&self, value: AvroValue) -> Result<Value> {
        match value {
            AvroValue::Boolean(v) => Ok(Value::Boolean(v)),
            v => Err(format!("invalid value for BooleanToBooleanConverter: {:?}", v)),
        }
    }
}

struct BytesToBinaryConverter;

impl ValueConverter for BytesToBinaryConverter {
    fn convert(&self, value: AvroValue) -> Result<Value> {
         match value {
            AvroValue::Bytes(v) => Ok(Value::Binary(Arc::new(v))),
            v => Err(format!("invalid value for BytesToBinaryConverter: {:?}", v)),
        }
    }
}

struct UnionNullConverter {
    converter: Box<dyn ValueConverter>,
}

impl UnionNullConverter {
    fn new(data_type: &DataType, schema: &UnionSchema) -> Result<Self> {
        let schemas = schema.variants();
        if !schema.is_nullable() || schemas.len()  != 2 {
            return Err(format!("only supported union schema with null, but find:: {:?}", schema));
        }
        let converter = if schemas[0] == AvroSchema::Null {
            create_converter(data_type, &schemas[1])?
        } else {
            create_converter(data_type, &schemas[0])?
        };
        Ok(UnionNullConverter{converter})
    }
}

impl ValueConverter for UnionNullConverter {
    fn convert(&self, value: AvroValue) -> Result<Value> {
        match value {
            AvroValue::Union(_, val) => match *val {
                AvroValue::Null => Ok(Value::Null),
                v => self.converter.convert(v),
            },
            v => Err(format!("invalid value for UnionNullConverter: {:?}", v)),
        }
    }
}

struct RecordToStructConverter {
    value_converters: HashMap<String, (usize, Box<dyn ValueConverter>)>,
    size: usize,
}

impl RecordToStructConverter {
    fn new(fields: &Vec<Field>, record_fields: &Vec<RecordField>) -> Result<Self> {
        let size = fields.len();
        let name_fields: HashMap<_, _> = record_fields.iter().map(|f| (f.name.clone(), f)).collect();
        let mut value_converters = HashMap::new();
        for (i, f) in fields.iter().enumerate() {
            let name = f.name.clone();
            match name_fields.get(&name) {
                Some(rf) => {
                    let converter = create_converter(&f.data_type, &rf.schema)?;
                    value_converters.insert(name, (i, converter));
                },
                None => return Err(format!("record field not found: {}", name)),
            }
        }
        Ok(RecordToStructConverter{value_converters, size})
    }

    fn read_row(&self, value: AvroValue, row: &mut GenericRow) -> Result<()> {
        let AvroValue::Record(fields) = value else {
            return Err("Expected an avro Record".to_string());
        };
        for (k, v) in fields {
            if let Some((i, converter)) = self.value_converters.get(&k) {
                let value = converter.convert(v)?;
                row.update(*i, value);
            }
        }
        Ok(())
    }
}

impl ValueConverter for RecordToStructConverter {
    fn convert(&self, value: AvroValue) -> Result<Value> {
        let mut row = GenericRow::new_with_size(self.size);
        self.read_row(value, &mut row)?;
        Ok(Value::Struct(Arc::new(row)))
    }
}

struct ArrayToArrayConverter {
    converter: Box<dyn ValueConverter>,
}

impl ArrayToArrayConverter {
    fn new(ele_type: &DataType, ele_schema: &AvroSchema)  -> Result<Self> {
        let converter = create_converter(ele_type, ele_schema)?;
        Ok(Self{converter})
    }
}

impl ValueConverter for ArrayToArrayConverter {
    fn convert(&self, value: AvroValue) -> Result<Value> {
        match value {
            AvroValue::Array(values) => {
                let mut array = Vec::with_capacity(values.len());
                for v in values {
                    array.push(self.converter.convert(v)?);
                }
                Ok(Value::Array(Arc::new(array)))
            }
            v => Err(format!("invalid value for ArrayToArrayConverter: {:?}", v)),
        }
    }
}

