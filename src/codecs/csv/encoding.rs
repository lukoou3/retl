use std::cell::RefCell;
use std::io;
use csv::{Terminator, Writer, WriterBuilder};
use crate::codecs::csv::config::CsvSerializerConfig;
use crate::{date_utils, datetime_utils, Result};
use crate::codecs::Serializer;
use crate::data::{Row, Value};
use crate::types::{DataType, Schema};

#[derive(Debug)]
pub struct CsvSerializer {
    pub data_types: Vec<(usize, DataType)>,
    pub writer: Writer<BytesWriter>,
    pub record: Vec<String>,
}

impl CsvSerializer {
    pub fn new(schema: Schema,  config: CsvSerializerConfig) -> Result<Self> {
        let data_types = schema.fields.iter().enumerate().map(|(i, field)| (i, field.data_type.clone())).collect();
        let mut builder = WriterBuilder::new();
        if let Some(delimiter) = config.delimiter {
            if delimiter.len() != 1 {
                return Err("Invalid delimiter".to_string());
            }
            builder.delimiter(delimiter.as_bytes()[0]);
        }
        if let Some(quote) = config.quote {
            if quote.len() != 1 {
                return Err("Invalid quote".to_string());
            }
            builder.quote(quote.as_bytes()[0]);
        }
        if let Some(double_quote) = config.double_quote {
            builder.double_quote(double_quote);
        }
        if let Some(escape) = config.escape {
            if escape.len() != 1 {
                return Err("Invalid escape".to_string());
            }
            builder.escape(escape.as_bytes()[0]);
        }
        let writer = builder.has_headers(false)
            .terminator(Terminator::Any(b'\n'))
            .from_writer(BytesWriter::new());
        let mut record = Vec::with_capacity(schema.fields.len());
        record.resize(schema.fields.len(), String::new());
        Ok(CsvSerializer{ data_types, writer, record})
    }
}

impl Serializer for CsvSerializer {
    fn serialize<'a>(&'a mut self, row: &'a dyn Row) -> Result<&'a [u8]> {
        self.writer.get_ref().clear();
        for ( value, (i, data_type)) in self.record.iter_mut().zip(self.data_types.iter()) {
            match row.get(*i) {
                Value::Null => value.clear(),
                Value::Int(v) => if data_type == DataType::date_type() {
                    *value = date_utils::num_days_to_date(*v).to_string()
                } else {
                    *value = v.to_string()
                },
                Value::Long(v) => if data_type == DataType::timestamp_type() {
                    *value = datetime_utils::from_timestamp_micros_utc(*v).format(datetime_utils::NORM_DATETIME_FMT).to_string()
                } else {
                    *value = v.to_string()
                },
                Value::Float(v) => *value = v.to_string(),
                Value::Double(v) => *value = v.to_string(),
                Value::String(v) => *value = v.as_ref().clone(),
                Value::Boolean(v) => *value = v.to_string(),
                v => return Err(format!("unsupported type: {:?}", v)),
            }
        }
        match self.writer.write_record(self.record.as_slice()) {
            Ok(_) => match self.writer.flush() {
                Ok(_) => Ok(self.writer.get_ref().bytes()),
                Err(e) =>  Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        }
    }
}

#[derive(Debug)]
struct BytesWriter {
    bytes: RefCell<Vec<u8>>,
}

impl BytesWriter {
    fn new() -> Self {
        Self { bytes: RefCell::new(Vec::new()) }
    }

    fn bytes(&self) -> &[u8] {
        let bytes = self.bytes.borrow();
        let len = bytes.len();
        let slice = if len == 0 {
            &bytes[..]
        } else {
            &bytes[..len - 1]
        };
        unsafe { std::slice::from_raw_parts(slice.as_ptr(), slice.len()) }
    }

    fn clear(&self) {
        self.bytes.borrow_mut().clear();
    }
}

impl io::Write for BytesWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}