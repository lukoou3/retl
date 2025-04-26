use std::fmt::Debug;
use std::io::Write;
use std::sync::Arc;
use prettytable::{Table, Row as TRow, Cell};
use crate::data::{BaseRow, GenericRow, Row};
use crate::physical_expr::PhysicalExpr;
use crate::types::Schema;

pub trait DataFrame {
    fn schema(&self) -> &Schema;
    fn compute(&mut self) -> Box<dyn Iterator<Item = &dyn Row> + '_>;
    fn collect(&mut self) -> Vec<GenericRow> {
        self.compute().map(|row| row.to_generic_row()).collect()
    }

    fn show(&mut self) {
        let mut table = Table::new();
        let types: Vec<_> = self.schema().fields.clone().into_iter().enumerate().map(|(i, field)| (i, field.data_type)).collect();
        let mut rows = 0;
        table.add_row(TRow::new(self.schema().field_names().iter().map(|name| Cell::new(name).style_spec("bFg")).collect()));
        for row in self.compute() {
            let mut cells = Vec::with_capacity(types.len());
            for (i, tp) in &types {
                cells.push(Cell::new(&row.get(*i).to_sql_string(tp)));
            }
            if rows < 1000 {
                table.add_row(TRow::new(cells));
            }
            rows += 1;
        }
        //table.printstd();
        print!("{}", table.to_string());
        println!("\n{} row(s) returned", rows);
    }
}

pub struct MemoryDataFrame {
    schema: Schema,
    rows: Vec<GenericRow>,
}

impl MemoryDataFrame {
    pub fn new(schema: Schema, rows: Vec<GenericRow>) -> Self {
        MemoryDataFrame { schema, rows }
    }
}

impl DataFrame for MemoryDataFrame {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn compute(&mut self) -> Box<dyn Iterator<Item=&dyn Row> + '_> {
        Box::new(self.rows.iter().map(|row| row as &dyn Row))
    }
}

pub struct MapDataFrame {
    schema: Schema,
    prev: Box<dyn DataFrame>,
    map_function: Box<dyn MapFunction>,
    row: GenericRow,
}

impl MapDataFrame {
    pub fn new(schema: Schema, prev: Box<dyn DataFrame>, map_function: Box<dyn MapFunction>) -> Self {
        let row = map_function.init_row();
        MapDataFrame { schema, prev, map_function, row }
    }
}

impl DataFrame for MapDataFrame {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn compute(&mut self) -> Box<dyn Iterator<Item=&dyn Row> + '_> {
        let inner = self.prev.compute();
        Box::new(MapIterator {inner, map_function: &mut self.map_function, output_row: &mut self.row })
    }
}

pub trait MapFunction {
    fn init_row(&self) -> GenericRow;
    fn map(&mut self, row: &dyn Row, output: &mut GenericRow);
}

pub struct ProjectMapFunction {
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl ProjectMapFunction {
    pub fn new(exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        ProjectMapFunction { exprs }
    }
}

impl MapFunction for ProjectMapFunction {
    fn init_row(&self) -> GenericRow {
        GenericRow::new_with_size(self.exprs.len())
    }

    fn map(&mut self, row: &dyn Row, output: &mut GenericRow) {
        for (i, expr) in self.exprs.iter().enumerate() {
            output.update(i, expr.eval(row));
        }
    }
}

pub struct MapIterator<'a> {
    inner: Box<dyn Iterator<Item = &'a dyn Row> + 'a>,
    map_function: &'a mut Box<dyn MapFunction>,
    output_row: &'a mut GenericRow,
}

impl<'a> Iterator for MapIterator<'a> {
    type Item = &'a dyn Row;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(row) => {
                self.map_function.map(row, self.output_row);
                Some(unsafe { &*(self.output_row as *mut GenericRow as *const dyn Row) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::Value;
    use crate::physical_expr as phy;
    use crate::types::{DataType, Field};
    use super::*;

    #[test]
    fn test_memory_data_frame() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int),
            Field::new("name", DataType::String),
        ]);
        let rows = vec![
            GenericRow::new(vec![Value::Int(1), Value::string("莫南")]),
            GenericRow::new(vec![Value::Int(2), Value::string("燕青丝")]),
            GenericRow::new(vec![Value::Int(3), Value::string("沐璇音")]),
        ];

        let mut df = MemoryDataFrame::new(schema, rows);
        let mut iter = df.compute();
        while let Some(row) = iter.next() {
            println!("row:{:?}", row);
        }
        drop(iter);
        println!("{}", "#".repeat(80));
        let schema = Schema::new(vec![
            Field::new("name", DataType::String),
            Field::new("id", DataType::Int),
        ]);
        let mut map_df = MapDataFrame::new(schema, Box::new(df), Box::new(ProjectMapFunction::new(vec![
            Arc::new(phy::BoundReference::new(1, DataType::String)),
            Arc::new(phy::BoundReference::new(0, DataType::Int)),
        ])));
        for row in map_df.compute() {
            println!("row:{:?}", row);
        }
        println!("{}", "#".repeat(80));
        map_df.show();
    }
}
