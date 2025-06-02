use apache_avro::Schema;
use apache_avro::schema::{RecordSchema, ResolvedSchema};

const SCHEMA_STR: &str = r#"
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
fn test_type() {
    let schema = Schema::parse_str(SCHEMA_STR).unwrap();
    //println!("{:?}", schema);
    let enclosing_namespace = schema.namespace();
    let rs = ResolvedSchema::try_from(&schema).unwrap();
    if let Schema::Record(RecordSchema { fields, lookup, .. })  = &schema {
        println!( "{:?}", lookup);
        for field in fields {
            println!("{:?}", field);
        }
        for field in fields {
            println!("{:?},{:?},{:?}.{:?}", field.name, field.schema, field.position, lookup.get(&field.name));
        }
    }

}

fn main() {
    test_type();
}
