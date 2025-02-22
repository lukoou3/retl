use crate::codecs::{Deserializer, RowVisitor};
use crate::data::{GenericRow, Row};
use crate::types::Schema;

#[derive(Debug, Clone)]
pub struct CsvDeserializer {
    pub schema: Schema,
    pub row: GenericRow,
}

impl CsvDeserializer {
    pub fn new(schema: Schema) -> Self {
        CsvDeserializer{ schema , row: GenericRow::new_with_size(3)}
    }
}

impl Deserializer for CsvDeserializer {
    fn deserialize(&mut self, bytes: &[u8]) -> crate::Result<&dyn Row> {
        Ok(&self.row)
    }
}