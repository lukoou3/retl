use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{RecordField, UnionSchema};
use apache_avro::types::Value as AvroValue;
use crate::codecs::Serializer;
use crate::Result;
use crate::data::{Row, Value};
use crate::types::{DataType, Field, Schema};

pub struct AvroSerializer {
    schema: Schema,
    avro_schema: AvroSchema,
    converter: StructToRecordConverter,
    buf: Vec<u8>,
}

impl AvroSerializer {
    pub fn new(schema: Schema, avro_schema: AvroSchema) -> Result<Self> {
        if let AvroSchema::Record(inner) = &avro_schema {
            let converter = StructToRecordConverter::new(&schema.fields, &inner.fields)?;
            Ok(Self{schema,avro_schema,converter,buf: Vec::new(),})
        } else {
            Err(format!("not support schema: {:?}", avro_schema))
        }
    }
}

impl Debug for AvroSerializer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
         f.debug_struct("AvroSerializer")
            .field("schema", &self.schema)
            .field("avro_schema", &self.avro_schema)
            .finish()
    }
}

impl Serializer for AvroSerializer {
    fn serialize<'a>(&'a mut self, row: &'a dyn Row) -> Result<&'a [u8]> {
        let value = self.converter.convert_row(row)?;
        let bytes = apache_avro::to_avro_datum(&self.avro_schema, value).map_err( |e| e.to_string())?;
        self.buf = bytes;
        Ok(&self.buf)
    }
}

fn create_converter(data_type: &DataType, schema: &AvroSchema)  -> Result<Box<dyn ValueConverter>> {
    let converter: Box<dyn ValueConverter> = match schema {
        AvroSchema::Int => match data_type {
             DataType::Int => Box::new(IntToIntConverter),
             DataType::Long => Box::new(LongToIntConverter),
             DataType::Float => Box::new(FloatToIntConverter),
             DataType::Double => Box::new(DoubleToIntConverter),
             _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Long => match data_type {
             DataType::Int => Box::new(IntToLongConverter),
             DataType::Long => Box::new(LongToLongConverter),
             DataType::Float => Box::new(FloatToLongConverter),
             DataType::Double => Box::new(DoubleToLongConverter),
             _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Float => match data_type {
             DataType::Int => Box::new(IntToFloatConverter),
             DataType::Long => Box::new(LongToFloatConverter),
             DataType::Float => Box::new(FloatToFloatConverter),
             DataType::Double => Box::new(DoubleToFloatConverter),
             _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Double => match data_type {
             DataType::Int => Box::new(IntToDoubleConverter),
             DataType::Long => Box::new(LongToDoubleConverter),
             DataType::Float => Box::new(FloatToDoubleConverter),
             DataType::Double => Box::new(DoubleToDoubleConverter),
             _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Boolean => match data_type {
            DataType::Boolean => Box::new(BooleanToBooleanConverter),
            _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::String => match data_type {
            DataType::Int => Box::new(IntToStringConverter),
            DataType::Long => Box::new(LongToStringConverter),
            DataType::Float => Box::new(FloatToStringConverter),
            DataType::Double => Box::new(DoubleToStringConverter),
            DataType::Boolean => Box::new(BooleanToStringConverter),
            DataType::String => Box::new(StringToStringConverter),
             _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Bytes => match data_type {
            DataType::Binary => Box::new(BinaryToBytesConverter),
             _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Union(inner) => Box::new(UnionNullConverter::new(data_type, inner)?),
        AvroSchema::Array(inner) => match data_type {
             DataType::Array(ele_type) => Box::new(ArrayToArrayConverter::new(ele_type, &inner.items)?),
             _ => return Err(not_match_err(data_type, schema)),
        },
        AvroSchema::Record(inner) => match data_type {
            DataType::Struct(fields) => Box::new(StructToRecordConverter::new(&fields.0, &inner.fields)?),
             _ => return Err(not_match_err(data_type, schema)),
        },
        _ => return Err(format!("not support schema: {:?}", schema)),
    };
    Ok(converter)
}

fn not_match_err(data_type: &DataType, schema: &AvroSchema) -> String {
    format!("not support type: {:?} for schema: {:?}", data_type, schema)
}

#[derive(Debug)]
enum ConverterResult {
    Null,
    Value(AvroValue),
    Err(String),
}

trait ValueConverter {
    fn convert(&self, value: &Value) -> ConverterResult;
}

macro_rules! impl_number_value_converter {
    ($struct_name:ident, $value1:ident, $value2:ident, $type:ty) => {
        pub struct $struct_name;

        impl ValueConverter for $struct_name {
            fn convert(&self, value: &Value) -> ConverterResult {
                match value {
                    Value::Null => ConverterResult::Null,
                    Value::$value1(v) => ConverterResult::Value(AvroValue::$value2(*v as $type)),
                    _ => ConverterResult::Err(format!("invalid value for {}: {:?}", stringify!($struct_name), value)),
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
            fn convert(&self, value: &Value) -> ConverterResult {
                match value {
                    Value::Null => ConverterResult::Null,
                    Value::$value1(v) => ConverterResult::Value(AvroValue::String(v.to_string())),
                    _ => ConverterResult::Err(format!("invalid value for {}: {:?}", stringify!($struct_name), value)),
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
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::String(v) => ConverterResult::Value(AvroValue::String(v.as_ref().clone())),
            _ => ConverterResult::Err(format!("invalid value for StringToStringConverter: {:?}", value)),
        }
    }
}

struct BooleanToBooleanConverter;

impl ValueConverter for BooleanToBooleanConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::Boolean(v) => ConverterResult::Value(AvroValue::Boolean(*v)),
            _ => ConverterResult::Err(format!("invalid value for BooleanToBooleanConverter: {:?}", value)),
        }
    }
}

struct BinaryToBytesConverter;

impl ValueConverter for BinaryToBytesConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::Binary(v) => ConverterResult::Value(AvroValue::Bytes(v.as_ref().clone())),
            _ => ConverterResult::Err(format!("invalid value for BinaryToBytesConverter: {:?}", value)),
        }
    }
}

struct UnionNullConverter {
    converter: Box<dyn ValueConverter>,
    null_index: u32,
    other_index: u32,
}

impl UnionNullConverter {
    fn new(data_type: &DataType, schema: &UnionSchema) -> Result<Self> {
        let schemas = schema.variants();
        if !schema.is_nullable() || schemas.len()  != 2 {
            return Err(format!("only supported union schema with null, but find:: {:?}", schema));
        }
        let (null_index, other_index, converter) = if schemas[0] == AvroSchema::Null {
            (0, 1, create_converter(data_type, &schemas[1])?)
        } else {
            (1, 0, create_converter(data_type, &schemas[0])?)
        };
        Ok(UnionNullConverter{converter,null_index,other_index,})
    }
}

impl ValueConverter for UnionNullConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
        match self.converter.convert(value) {
            ConverterResult::Null => ConverterResult::Value(AvroValue::Union(self.null_index, Box::new(AvroValue::Null))),
            ConverterResult::Value(v) => ConverterResult::Value(AvroValue::Union(self.other_index, Box::new(v))),
            e => e,
        }
    }
}

struct FieldInfo {
    name: String,
    index: usize,
    converter: Box<dyn ValueConverter>,
    nullable: bool,
    default_value: AvroValue,
}

struct StructToRecordConverter {
    field_infos: Vec<FieldInfo>,
}

impl StructToRecordConverter {
    fn new(fields: &Vec<Field>, record_fields: &Vec<RecordField>) -> Result<Self> {
        let len = fields.len();
        let types: HashMap<_, _> = fields.iter().enumerate().map(|(i,field)| (field.name.clone(), (i, &field.data_type))).collect();
        let mut field_infos = Vec::with_capacity(record_fields.len());
        for rf in record_fields {
            let name = rf.name.clone();
            let nullable = rf.is_nullable();
            let default_value = match &rf.default {
                Some(v) => AvroValue::from(v.clone()).resolve(&rf.schema).map_err( |e| format!("invalid default value for field {:?}: {}", rf, e))?,
                None => if nullable {
                     AvroValue::Null
                } else {
                    match rf.schema {
                        AvroSchema::Int => AvroValue::Int(0),
                        AvroSchema::Long => AvroValue::Long(0),
                        AvroSchema::Float => AvroValue::Float(0f32),
                        AvroSchema::Double => AvroValue::Double(0f64),
                        AvroSchema::Boolean => AvroValue::Boolean(false),
                        AvroSchema::Bytes => AvroValue::Bytes(Vec::new()),
                        AvroSchema::String => AvroValue::String("".to_string()),
                        AvroSchema::Array(_) => AvroValue::Array(Vec::new()),
                        _ => return Err(format!("not default value for field {:?}", rf)),
                    }
                },
            };
            match types.get(&name) {
                Some((i, tp)) => {
                    let index = *i;
                    let converter = create_converter(tp, &rf.schema)?;
                    field_infos.push(FieldInfo {name, index, converter, nullable, default_value});
                },
                None => {
                    let index = len;
                    let converter = Box::new(IntToIntConverter);
                    field_infos.push(FieldInfo {name, index, converter, nullable, default_value});
                },
            };
        }
        Ok(StructToRecordConverter {field_infos,})
    }
    fn convert_row(&self, row: &dyn Row) -> Result<AvroValue> {
        let len = row.len();
        let mut fields = Vec::with_capacity(self.field_infos.len());
        for f in &self.field_infos {
            if f.index >= len {
                fields.push((f.name.clone(), f.default_value.clone()));
                continue;
            }
            let result = f.converter.convert(row.get(f.index));
            let info = format!("{} -> {:?}", f.name, result);
            println!( "{}", info);
            match result {
                ConverterResult::Null => fields.push((f.name.clone(), f.default_value.clone())),
                ConverterResult::Value(v) => fields.push((f.name.clone(), v)),
                ConverterResult::Err(e) => return Err(e),
            }
        }
        Ok(AvroValue::Record(fields))
    }
}

impl ValueConverter for StructToRecordConverter {
    fn convert(&self, value: &Value) -> ConverterResult {
         match value {
            Value::Null => ConverterResult::Null,
            Value::Struct(r) => match self.convert_row(r.as_row()) {
                Ok(v) => ConverterResult::Value(v),
                Err(e) => ConverterResult::Err(e),
            },
            _ => ConverterResult::Err(format!("invalid value for StructToRecordConverter: {:?}", value)),
        }
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
    fn convert(&self, value: &Value) -> ConverterResult {
        match value {
            Value::Null => ConverterResult::Null,
            Value::Array(vec) => {
                let mut array = Vec::with_capacity(vec.len());
                for item in vec.iter() {
                     match self.converter.convert(item) {
                        ConverterResult::Null => continue,
                        ConverterResult::Value(v) => array.push(v),
                        ConverterResult::Err(e) => return ConverterResult::Err(e),
                    }
                }
                ConverterResult::Value(AvroValue::Array(array))
            },
            _ => ConverterResult::Err(format!("invalid value for ArrayToArrayConverter: {:?}", value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::codecs::avro::decoding::AvroDeserializer;
    use crate::codecs::Deserializer;
    use crate::data::GenericRow;
    use super::*;

    fn get_schema() -> AvroSchema {
        let schema_str = r#"
        {
            "type": "record",
            "name": "Data",
            "namespace": "com.example",
            "fields": [
                {
                    "name": "int_field",
                    "type": "int"
                },
                {
                    "name": "long_field",
                    "type": "long"
                },
                {
                    "name": "float_field",
                    "type": "float"
                },
                {
                    "name": "double_field",
                    "type": "double"
                },
                {
                    "name": "string_field",
                    "type": "string"
                },
                {
                    "name": "boolean_field",
                    "type": "boolean"
                },
                {
                    "name": "bytes_field",
                    "type": "bytes"
                },
                {
                    "name": "int_nullable",
                    "type": ["null", "int"]
                },
                {
                    "name": "long_nullable",
                    "type": ["null", "long"]
                },
                {
                    "name": "float_nullable",
                    "type": ["null", "float"]
                },
                {
                    "name": "double_nullable",
                    "type": ["null", "double"]
                },
                {
                    "name": "string_nullable",
                    "type": ["null", "string"]
                },
                {
                    "name": "boolean_nullable",
                    "type": ["null", "boolean"]
                },
                {
                    "name": "bytes_nullable",
                    "type": ["null", "bytes"]
                },
                {
                    "name": "int_array",
                    "type": {"type": "array", "items": "int"}
                },
                {
                    "name": "string_array",
                    "type": {"type": "array", "items": "string"}
                },
                {
                    "name": "double_array",
                    "type": {"type": "array", "items": "double"}
                }
            ]
        }
    "#;
        AvroSchema::parse_str(schema_str).unwrap()
    }

    #[test]
    fn test_serialize() {
        let avro_schema = get_schema();
        let schema = crate::parser::parse_schema(r#"
        int_field int, long_field bigint, float_field float, double_field double, string_field string, boolean_field boolean, bytes_field binary,
        int_nullable int, long_nullable bigint, float_nullable float, double_nullable double, string_nullable string, boolean_nullable boolean, bytes_nullable binary,
        int_array array<int>, string_array array<string>, double_array array<double>
        "#).unwrap();
        let mut row: Box<dyn Row> = Box::new(GenericRow::new(vec![
            Value::int(2),
            Value::long(18),
            Value::float(1.1),
            Value::double(2.2),
            Value::string("莫南"),
            Value::boolean(true),
            Value::Binary(Arc::new(vec![0x01, 0x02, 0x03, 0x04])),
            Value::int(21),
            Value::long(181),
            Value::float(11.1),
            Value::double(21.2),
            Value::string("莫南1"),
            Value::boolean(false),
            Value::Binary(Arc::new(vec![0x02, 0x04])),
            Value::Array(Arc::new( vec![Value::int(10), Value::int(20), Value::int(30),] )),
            Value::Array(Arc::new( vec![Value::string("1"), Value::string("2"), Value::string("3"),] )),
            Value::Array(Arc::new( vec![Value::double(10.1), Value::double(20.2), Value::double(30.3),] )),
        ]));
        let mut serializer = AvroSerializer::new(schema.clone(), avro_schema.clone()).unwrap();
        let mut deserializer = AvroDeserializer::new(schema.clone(), avro_schema.clone()).unwrap();
        let bytes = serializer.serialize(row.as_ref()).unwrap();
        println!("{}", row.as_ref());
        let rst = deserializer.deserialize(&bytes).unwrap();
        println!("{}", rst);

        row.update(1, Value::Null);
        row.update(3, Value::Null);
        row.update(4, Value::Null);
        row.update(6, Value::Null);
        row.update(7, Value::Null);
        row.update(9, Value::Null);
        row.update(11, Value::Null);
        row.update(16, Value::Null);
        let bytes = serializer.serialize(row.as_ref()).unwrap();
        println!("{}", row.as_ref());
        let rst = deserializer.deserialize(&bytes).unwrap();
        println!("{}", rst);
    }


}
