use std::sync::Arc;
use csv::ReaderBuilder;
use crate::codecs::csv::config::CsvDeserializerConfig;
use crate::Result;
use crate::codecs::Deserializer;
use crate::data::{GenericRow, Row, Value};
use crate::types::{DataType, Schema};

#[derive(Debug)]
pub struct ReaderConfig {
    delimiter: u8,
    quote: u8,
    quoting: bool,
    double_quote: bool,
    escape: Option<u8>,
}

#[derive(Debug)]
pub struct CsvDeserializer {
    data_types: Vec<(usize, DataType)>,
    row: GenericRow,
    config: ReaderConfig,
}

impl CsvDeserializer {
    pub fn new(schema: Schema, config: CsvDeserializerConfig) -> Result<Self> {
        let data_types = schema.fields.iter().enumerate().map(|(i, field)| (i, field.data_type.clone())).collect();
        let row = GenericRow::new_with_size(schema.fields.len());
        let mut delimiter = b',';
        let mut quote = b'"';
        let mut quoting = true;
        let mut double_quote = true;
        let mut escape = None;
        if let Some(v) = config.delimiter {
            if v.len() != 1 {
                return Err("Invalid delimiter".to_string());
            }
            delimiter = v.as_bytes()[0];
        }
        if let Some(v) = config.quote {
            if v.len() != 1 {
                return Err("Invalid quote".to_string());
            }
            quote = v.as_bytes()[0];
        }
        if let Some(v) = config.quoting {
            quoting = v;
        }
        if let Some(v) = config.double_quote {
            double_quote = v;
        }
        if let Some(v) = config.escape {
            if v.len() != 1 {
                return Err("Invalid escape".to_string());
            }
            escape = Some(v.as_bytes()[0]);
        }
        let config = ReaderConfig{delimiter, quote, quoting, double_quote, escape };
        Ok(CsvDeserializer{data_types, row, config})
    }
}

impl Deserializer for CsvDeserializer {
    fn deserialize(&mut self, bytes: &[u8]) -> Result<&dyn Row> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(self.config.delimiter)
            .quote(self.config.quote).quoting(self.config.quoting)
            .double_quote(self.config.double_quote).escape(self.config.escape)
            .from_reader(bytes);
        match rdr.records().next() {
            None => Err("not input data".to_string()),
            Some(r) => match r {
                Err(e) => Err(e.to_string()),
                Ok(record) => {
                    for (i, data_type) in &self.data_types {
                        match record.get(*i) {
                            None => self.row.update(*i, Value::Null),
                            Some(s) => {
                                if s.is_empty() {
                                    self.row.update(*i, Value::Null);
                                    continue;
                                }
                                let v = match data_type {
                                    DataType::String => Value::String(Arc::new(s.to_string())),
                                    DataType::Int => match s.parse() {
                                        Ok(v) => Value::Int(v),
                                        Err(e) => return Err(format!("parse '{}' to int error: {}", s, e)),
                                    },
                                    DataType::Long => match s.parse() {
                                        Ok(v) => Value::Long(v),
                                        Err(e) => return Err(format!("parse '{}' to bigint error: {}", s, e)),
                                    },
                                    DataType::Float => match s.parse() {
                                        Ok(v) => Value::Float(v),
                                        Err(e) => return Err(format!("parse '{}' to float error: {}", s, e)),
                                    },
                                    DataType::Double => match s.parse() {
                                        Ok(v) => Value::Double(v),
                                        Err(e) => return Err(format!("parse '{}' to double error: {}", s, e)),
                                    },
                                    DataType::Boolean => match s.parse() {
                                        Ok(v) => Value::Boolean(v),
                                        Err(e) => return Err(format!("parse '{}' to boolean error: {}", s, e)),
                                    },
                                    _ => return Err(format!("unsupported type: {:?}", data_type)),
                                };
                                self.row.update(*i, v);
                            },
                        }
                    }
                    Ok(&self.row)
                }
            }
        }
    }
}