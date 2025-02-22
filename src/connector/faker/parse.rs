use std::fmt::Debug;
use config::Config;
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use crate::Result;
use crate::connector::faker::{CharsStringFaker, Faker, OptionDoubleFaker, OptionIntFaker, OptionLongFaker, OptionStringFaker, RangeDoubleFaker, RangeIntFaker, RangeLongFaker, RegexStringFaker};
use crate::data::Value;
use crate::types::Schema;
pub fn parse_fakers(field_configs: Vec<Config>, schema: &Schema) -> Result<Vec<(usize, Box<dyn Faker>)>> {
    let mut fakers: Vec<(usize, Box<dyn Faker>)> = Vec::with_capacity(field_configs.len());

    for config in field_configs {
        let name = config.get_string("name").unwrap();
        let faker_config: Box<dyn FakerConfig> = config.try_deserialize().map_err(|e| e.to_string())?;
        if let Some(i) = schema.field_index(&name) {
            fakers.push((i, faker_config.build()?))
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
    fn build(&self) -> Result<Box<dyn Faker>>;
}

dyn_clone::clone_trait_object!(FakerConfig);

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
}

#[typetag::serde(name = "int")]
impl FakerConfig for IntFakerConfig {
    fn build(&self) -> Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let mut options = Vec::with_capacity(self.options.len());
            for option in self.options.clone() {
                match option {
                    Some(value) => options.push(Value::Int(value)),
                    None => options.push(Value::Null),
                }
            }
            Ok(Box::new(OptionIntFaker::new(options, self.random)))
        } else {
            Ok(Box::new(RangeIntFaker::new(self.min, self.max, self.random)))
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
}

#[typetag::serde(name = "long")]
impl FakerConfig for LongFakerConfig {
    fn build(&self) -> Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let mut options = Vec::with_capacity(self.options.len());
            for option in self.options.clone() {
                match option {
                    Some(value) => options.push(Value::Long(value)),
                    None => options.push(Value::Null),
               }
            }
            Ok(Box::new(OptionLongFaker::new(options, self.random)))
        } else {
            Ok(Box::new(RangeLongFaker::new(self.min, self.max, self.random)))
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
}

#[typetag::serde(name = "double")]
impl FakerConfig for DoubleFakerConfig {
    fn build(&self) -> Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let mut options = Vec::with_capacity(self.options.len());
            for option in self.options.clone() {
                match option {
                    Some(value) => options.push(Value::Double(value)),
                    None => options.push(Value::Null),
               }
            }
            Ok(Box::new(OptionDoubleFaker::new(options, self.random)))
        } else {
            Ok(Box::new(RangeDoubleFaker::new(self.min, self.max)))
        }
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
}

#[typetag::serde(name = "string")]
impl FakerConfig for StringFakerConfig {
    fn build(&self) -> Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let mut options = Vec::with_capacity(self.options.len());
            for option in self.options.clone() {
                match option {
                    Some(value) => options.push(Value::string(value)),
                    None => options.push(Value::Null),
              }
            }
            Ok(Box::new(OptionStringFaker::new(options, self.random)))
        } else if !self.chars.is_empty() {
            Ok(Box::new(CharsStringFaker::new(self.chars.chars().collect(), self.len)))
        } else {
            Ok(Box::new(RegexStringFaker::new(self.regex.clone())))
        }
    }
}


fn default_random() -> bool {
    true
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
        println!("{:?}", config.build());
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
        println!("{:?}", config.build());
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
        println!("{:?}", config.build());

        let text = r#"
        {
            "type": "string",
            "random": true
        }
        "#;
        let config: Box<dyn FakerConfig> = serde_json::from_str(text).unwrap();
        println!("{:?}", config);
        println!("{:?}", config.build());
    }
}