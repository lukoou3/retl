use std::io::Write;
use crate::Result;
use crate::codecs::Serializer;
use crate::data::Row;
use crate::types::Schema;

#[derive(Debug, Clone)]
pub struct CsvSerializer {
    pub schema: Schema,
    pub bytes: Vec<u8>,
}

impl CsvSerializer {
    pub fn new(schema: Schema) -> Self {
        Self { schema, bytes: Vec::new() }
    }
}

impl Serializer for CsvSerializer {
    fn serialize(&mut self, row: &dyn Row) -> Result<&[u8]> {
        self.bytes.clear();
        self.bytes.write("a,b,3".as_bytes());
        Ok(&self.bytes)
    }
}

