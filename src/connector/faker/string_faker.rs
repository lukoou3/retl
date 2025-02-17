use std::sync::Arc;
use rand::distr::Iter;
use rand::prelude::StdRng;
use crate::connector::faker::Faker;
use crate::data::Value;
use crate::types::DataType;
use rand::{Rng, SeedableRng};
use rand_regex::Regex;

#[derive(Debug, Clone)]
pub struct OptionStringFaker {
    options: Box<[Arc<String>]>,
    random: bool,
    index: usize,
}

impl OptionStringFaker {
    pub fn new(options: Vec<Arc<String>>, random: bool) -> Self {
        let options = options.into_boxed_slice();
        Self{options, random, index: 0}
    }
}

impl Faker for OptionStringFaker {
    fn data_type(&self) -> DataType {
        DataType::String
    }
    fn gene_value(&mut self) -> Value {
        if self.options.len() == 0 {
            Value::Null
        } else if self.options.len() == 1 {
            Value::String(self.options[0].clone())
        } else {
            if !self.random {
                if self.index == self.options.len() {
                    self.index = 0;
                }
                let value = Value::String(self.options[self.index].clone());
                self.index += 1;
                value
            } else {
                Value::String(self.options[rand::thread_rng().gen_range(0..self.options.len())].clone())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CharsStringFaker {
    chars: Box<[char]>,
    len: usize,
}

impl CharsStringFaker {
    pub fn new(chars: Vec<char>, len: usize) -> Self {
        Self { chars: chars.into_boxed_slice(), len }
    }
}

impl Faker for CharsStringFaker {
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn gene_value(&mut self) -> Value {
        let mut s = String::with_capacity(self.len);
        for _ in 0..self.len {
            s.push(self.chars[rand::thread_rng().gen_range(0..self.chars.len())]);
        }
        Value::String(Arc::new(s))
    }
}

#[derive(Debug)]
pub struct RegexStringFaker {
    regex: String,
    iter: Option<Iter<Regex, StdRng, String>>,
}

impl Clone for RegexStringFaker {
    fn clone(&self) -> Self {
        Self{regex: self.regex.clone(), iter: None}
    }
}

impl RegexStringFaker {
    pub fn new(regex: impl Into<String>) -> Self {
        Self{regex: regex.into(), iter: None}
    }

}

impl Faker for RegexStringFaker {
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn init(&mut self) -> crate::Result<()> {
        let mut parser = regex_syntax::ParserBuilder::new().unicode(false).build();
        let hir = parser.parse(self.regex.as_str()).map_err(|e|e.to_string())?;
        let pattern = Regex::with_hir(hir, 4).map_err(|e|e.to_string())?;
        let iter: Iter<Regex, StdRng, String> = StdRng::seed_from_u64(42).sample_iter::<String, _>(pattern);
        self.iter = Some(iter);
        Ok(())
    }

    fn gene_value(&mut self) -> Value {
        Value::String(Arc::new(self.iter.as_mut().unwrap().next().unwrap()))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use itertools::Itertools;
    use rand::distr::Iter;
    use rand::prelude::ThreadRng;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use crate::connector::faker::Faker;
    use crate::connector::faker::string_faker::{CharsStringFaker, OptionStringFaker, RegexStringFaker};

    #[test]
    fn test_rand_regex() {
        use rand::Rng;
        use rand_regex::Regex;
        let pattern = Regex::compile("[0-9]{2}-[0-9]{2,4}", 4).unwrap();
        let mut iter: Iter<Regex, ThreadRng, String> = rand::rng().sample_iter::<String, _>(pattern);
        for _ in 0..10 {
            println!("{}", iter.next().unwrap());
        }
        println!("{}", "#".repeat(30));
        let pattern = Regex::compile(r"\d{2}-\d{2,4}", 4).unwrap();
        let mut iter = rand::rng().sample_iter::<String, _>(pattern);
        for _ in 0..10 {
            println!("{}", iter.next().unwrap());
        }
        println!("{}", "#".repeat(30));
        let mut parser = regex_syntax::ParserBuilder::new().unicode(false).build();
        let hir = parser.parse(r"\d{2}-\d{2,4}").unwrap();
        let pattern = rand_regex::Regex::with_hir(hir, 4).unwrap();
        let mut iter = rand::rng().sample_iter::<String, _>(pattern);
        for _ in 0..10 {
            println!("{}", iter.next().unwrap());
        }
    }

    #[test]
    fn test_rand_regex2() {
        use rand::Rng;
        use rand_regex::Regex;

        let mut parser = regex_syntax::ParserBuilder::new().unicode(false).build();
        let hir = parser.parse(r"[0-9]{2}-[0-9]{2,4}").unwrap();
        let pattern = Regex::with_hir(hir, 4).unwrap();
        //let mut iter: Iter<Regex, ThreadRng, String> = rand::rng().sample_iter::<String, _>(pattern);
        let mut iter: Iter<Regex, _, String> = StdRng::seed_from_u64(42).sample_iter::<String, _>(pattern);
        for i in 0..100000 {
            let output = iter.next().unwrap();
            /*if i > 99990{
                println!("{}", output);
            }*/
        }
    }


/*    #[test]
    fn test_regex_generate() {
        use chrono::Utc;
        use regex_generate::Generator;
        let mut gen = Generator::new(r"[0-9]{2}-[0-9]{2,4}", rand::thread_rng(), 4).unwrap();
        let mut buffer = vec![];
        let start = Utc::now().timestamp_millis();
        for _ in 0..100000 {
            buffer.clear();
            gen.generate(&mut buffer).unwrap();
            //let output = String::from_utf8_lossy(buffer.as_slice()).into_owned();
            let output = String::from_utf8(buffer.clone());
            //println!("{}", output);
        }
        let end = Utc::now().timestamp_millis();
        println!("time:{}", end - start);
    }*/

    #[test]
    fn test_string_faker() {
        let mut fakes:Vec<Box<dyn Faker>> = vec![
            Box::new(OptionStringFaker::new(vec![Arc::new("ab".to_string()), Arc::new("12".to_string()), Arc::new("哈哈".to_string())], true)),
            Box::new(CharsStringFaker::new(vec!['a', 'b', 'c', 'd', 'e', 'f', 'g'], 4)),
            Box::new(CharsStringFaker::new("123456".chars().collect(), 4)),
            Box::new(RegexStringFaker::new("12[a-z]{2}")),
            Box::new(RegexStringFaker::new("12[a-z]{2,4}")),
        ];
        for f in &mut fakes {
            f.init();
        }
        for _ in 0..30 {
            let values:Vec<_> = fakes.iter_mut().map(| f| f.gene_value()).collect();
            println!("{}", values.iter().map(|x| format!("{x}")).join(", "));
        }
    }
}