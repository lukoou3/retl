use std::fmt::Debug;
use std::net::{Ipv4Addr, Ipv6Addr};
use config::Config;
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use crate::{sql_utils, Result};
use crate::connector::faker::{ArrayFaker, CharsStringFaker, EvalFaker, Faker, FieldFaker, FieldsFaker, FormatTimestampFaker, Ipv4Faker, Ipv6Faker, NullAbleFaker, OptionDoubleFaker, OptionIntFaker, OptionLongFaker, OptionStringFaker, RangeDoubleFaker, RangeIntFaker, RangeLongFaker, RegexStringFaker, SequenceFaker, TimestampFaker, TimestampType, TimestampUnit, UnionFaker};
use crate::data::Value;
use crate::expr::BoundReference;
use crate::physical_expr::{create_physical_expr, get_cast_func};
use crate::types::Schema;
pub fn parse_fakers(field_configs: Vec<Config>, schema: &Schema) -> Result<Vec<(usize, Box<dyn Faker>)>> {
    let mut fakers: Vec<(usize, Box<dyn Faker>)> = Vec::with_capacity(field_configs.len());

    for config in field_configs {
        let name = config.get_string("name").unwrap();
        let faker_config: Box<dyn FakerConfig> = config.try_deserialize().map_err(|e| e.to_string())?;
        if let Some(i) = schema.field_index(&name) {
            fakers.push((i, faker_config.build(schema, i)?))
        }
    }

    Ok(fakers)
}

#[derive(Clone, Debug, Serialize,Deserialize)]
pub struct FieldFakerConfig {
    pub name: String,
    #[serde(flatten)]
    pub config: Box<dyn FakerConfig>,
}

#[typetag::serde(tag = "type")]
pub trait FakerConfig: DynClone + Debug + Send + Sync {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>>;
}

dyn_clone::clone_trait_object!(FakerConfig);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct WrapConfig {
    #[serde(default)]
    null_rate: f32,
    #[serde(default)]
    array: bool,
    #[serde(default = "default_array_len_min")]
    array_len_min: usize,
    #[serde(default = "default_array_len_max")]
    array_len_max: usize,
}


#[derive(Clone, Debug, Serialize, Deserialize)]
struct IntFakerConfig {
    #[serde(default)]
    min: i32,
    #[serde(default)]
    max: i32,
    #[serde(default)]
    options: Vec<Option<i32>>,
    #[serde(default = "default_random")]
    random: bool,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "int")]
impl FakerConfig for IntFakerConfig {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let mut options = Vec::with_capacity(self.options.len());
            for option in self.options.clone() {
                match option {
                    Some(value) => options.push(Value::Int(value)),
                    None => options.push(Value::Null),
                }
            }
            let faker = Box::new(OptionIntFaker::new(options, self.random));
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        } else {
            let faker = Box::new(RangeIntFaker::new(self.min, self.max, self.random));
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LongFakerConfig {
    #[serde(default)]
    min: i64,
    #[serde(default)]
    max: i64,
    #[serde(default)]
    options: Vec<Option<i64>>,
    #[serde(default = "default_random")]
    random: bool,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "long")]
impl FakerConfig for LongFakerConfig {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let mut options = Vec::with_capacity(self.options.len());
            for option in self.options.clone() {
                match option {
                    Some(value) => options.push(Value::Long(value)),
                    None => options.push(Value::Null),
               }
            }
            let faker = Box::new(OptionLongFaker::new(options, self.random));
            Ok(wrap_faker_necessary(faker, &self.array_config))
        } else {
            let faker = Box::new(RangeLongFaker::new(self.min, self.max, self.random));
            Ok(wrap_faker_necessary(faker, &self.array_config))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DoubleFakerConfig {
    #[serde(default)]
    min: f64,
    #[serde(default)]
    max: f64,
    #[serde(default)]
    options: Vec<Option<f64>>,
    #[serde(default = "default_random")]
    random: bool,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "double")]
impl FakerConfig for DoubleFakerConfig {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let mut options = Vec::with_capacity(self.options.len());
            for option in self.options.clone() {
                match option {
                    Some(value) => options.push(Value::Double(value)),
                    None => options.push(Value::Null),
               }
            }
            let faker = Box::new(OptionDoubleFaker::new(options, self.random));
            Ok(wrap_faker_necessary(faker, &self.array_config))
        } else {
            let faker = Box::new(RangeDoubleFaker::new(self.min, self.max));
            Ok(wrap_faker_necessary(faker, &self.array_config))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SequenceFakerConfig {
    #[serde(default)]
    start: i64,
    #[serde(default = "default_sequence_step")]
    step: i64,
    #[serde(default = "default_sequence_batch")]
    batch: u32,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "sequence")]
impl FakerConfig for SequenceFakerConfig {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        let faker = Box::new(SequenceFaker::new(self.start, self.step, self.batch));
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }

}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StringFakerConfig {
    #[serde(default = "default_regex")]
    regex: String,
    #[serde(default)]
    chars: String,
    #[serde(default)]
    len: usize,
    #[serde(default)]
    options: Vec<Option<String>>,
    #[serde(default = "default_random")]
    random: bool,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "string")]
impl FakerConfig for StringFakerConfig {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        let faker: Box<dyn Faker> = if !self.options.is_empty() {
            let mut options = Vec::with_capacity(self.options.len());
            for option in self.options.clone() {
                match option {
                    Some(value) => options.push(Value::string(value)),
                    None => options.push(Value::Null),
              }
            }
            Box::new(OptionStringFaker::new(options, self.random))
        } else if !self.chars.is_empty() {
            Box::new(CharsStringFaker::new(self.chars.chars().collect(), self.len))
        } else {
            Box::new(RegexStringFaker::new(self.regex.clone()))
        };
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TimestampConfig {
    #[serde(default)]
    unit: TimestampUnit,
    #[serde(default)]
    timestamp_type: TimestampType,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "timestamp")]
impl FakerConfig for TimestampConfig {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        Ok(Box::new(TimestampFaker::new(self.unit, self.timestamp_type)))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FormatTimestampConfig {
    #[serde(default = "default_timestamp_format")]
    format: String,
    #[serde(default = "default_utc")]
    utc: bool,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "format_timestamp")]
impl FakerConfig for FormatTimestampConfig {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        let faker = Box::new(FormatTimestampFaker{format: self.format.clone(), utc: self.utc});
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ipv4Config {
    #[serde(default = "default_ipv4_start")]
    start: String,
    #[serde(default = "default_ipv4_end")]
    end: String,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "ipv4")]
impl FakerConfig for Ipv4Config {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        let start = self.start.parse::<Ipv4Addr>().map_err(|e| e.to_string())?;
        let end = self.end.parse::<Ipv4Addr>().map_err(|e| e.to_string())?;
        let start = u32::from(start);
        let end = u32::from(end);
        if start >= end {
            return Err("Ipv4Config start must not be greater than end".to_string());
        }
        let faker = Box::new(Ipv4Faker::new(start, end));
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ipv6Config {
    #[serde(default = "default_ipv6_start")]
    start: String,
    #[serde(default = "default_ipv6_end")]
    end: String,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "ipv6")]
impl FakerConfig for Ipv6Config {
    fn build(&self, schema: &Schema, i: usize) -> Result<Box<dyn Faker>> {
        let start = self.start.parse::<Ipv6Addr>().map_err(|e| e.to_string())?;
        let end = self.end.parse::<Ipv6Addr>().map_err(|e| e.to_string())?;
        let start = u128::from(start);
        let end = u128::from(end);
        if start >= end {
            return Err("Ipv6Config start must not be greater than end".to_string());
        }
        let faker = Box::new(Ipv6Faker::new(start, end));
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EvalConfig {
    expression: String,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "eval")]
impl FakerConfig for EvalConfig {
    fn build(&self, schema: &Schema, _: usize) -> Result<Box<dyn Faker>> {
        let expression = sql_utils::parse_expr(&self.expression, schema)?;
        let expr = BoundReference::bind_reference(expression.expr, expression.child.output())?;
        let expr = create_physical_expr(&expr)?;
        let faker = Box::new(EvalFaker::new(expr));
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UnionFieldsConfig {
    fields: Vec<FieldFakerConfig>,
    weight: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UnionFakerConfig {
    union_fields: Vec<UnionFieldsConfig>,
    #[serde(default = "default_random")]
    random: bool,
}

#[typetag::serde(name = "union")]
impl FakerConfig for UnionFakerConfig {
    fn build(&self, schema: &Schema, _: usize) -> Result<Box<dyn Faker>> {
        let mut fields_fakers: Vec<FieldsFaker> = Vec::with_capacity(self.union_fields.len());
        for union_fields_config in &self.union_fields {
            let weight = union_fields_config.weight;

            let field_configs = &union_fields_config.fields;
            let mut field_fakers: Vec<FieldFaker> = Vec::with_capacity(field_configs.len());
            for field_config in field_configs {
                if let Some(i) = schema.field_index(&field_config.name) {
                    let faker = field_config.config.build(schema, i)?;
                    let converter = get_cast_func(faker.data_type(), schema.fields[i].data_type.clone());
                    field_fakers.push(FieldFaker::new(i, faker, converter))
                }
            }

            fields_fakers.push(FieldsFaker::new(field_fakers, weight))
        }

        Ok(Box::new(UnionFaker::new(fields_fakers, self.random)))
    }
}

fn wrap_faker_necessary(mut faker: Box<dyn Faker>, wrap_config: &WrapConfig,) -> Box<dyn Faker> {
    if wrap_config.array {
        faker = Box::new(ArrayFaker::new(faker, wrap_config.array_len_min, wrap_config.array_len_max))
    }
    if wrap_config.null_rate > 0f32 {
        faker = Box::new(NullAbleFaker::new(faker, wrap_config.null_rate))
    }
    faker
}

fn default_sequence_step() -> i64 {
    1
}

fn default_sequence_batch() -> u32 {
    1
}

fn default_ipv6_start() -> String {
    "::".to_string()
}

fn default_ipv6_end() -> String {
    "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".to_string()
}

fn default_ipv4_start() -> String {
    "0.0.0.0".to_string()
}

fn default_ipv4_end() -> String {
    "255.255.255.255".to_string()
}

fn default_timestamp_format() -> String {
    "%Y-%m-%d %H:%M:%S".to_string()
}

fn default_utc() -> bool {
    true
}

fn default_random() -> bool {
    true
}

fn default_array_len_min() -> usize {
    0
}

fn default_array_len_max() -> usize {
    5
}

fn default_regex() -> String {
    "[a-zA-Z]{0,5}".to_string()
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_parse_number_config() {
        let text = r#"
        {
            "type": "int",
            "min": 1,
            "max": 100,
            "options": [1, 2, null, 4, 5],
            "random": true
        }
        "#;
        let config: Box<dyn FakerConfig> = serde_json::from_str(text).unwrap();
        println!("{:?}", config);
        //println!("{:?}", config.build());
        let text = r#"
        {
            "type": "long",
            "min": 1,
            "max": 100,
            "options": [1, 2, null, 4, 5],
            "random": true
        }
        "#;
        let config: Box<dyn FakerConfig> = serde_json::from_str(text).unwrap();
        println!("{:?}", config);
        //println!("{:?}", config.build());
    }

    #[test]
    fn test_parse_string_config() {
        let text = r#"
        {
            "type": "string",
            "regex": "12[a-z]{2}",
            "chars": "abcdefghijklmnopqrstuvwxyz",
            "len": 10,
            "options": ["a", "b", null, "c", "d"],
            "random": true
        }
        "#;
        let config: Box<dyn FakerConfig> = serde_json::from_str(text).unwrap();
        println!("{:?}", config);
        //println!("{:?}", config.build());

        let text = r#"
        {
            "type": "string",
            "random": true
        }
        "#;
        let config: Box<dyn FakerConfig> = serde_json::from_str(text).unwrap();
        println!("{:?}", config);
        //println!("{:?}", config.build());
    }
}