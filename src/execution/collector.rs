use crate::connector::Sink;
use crate::data::Row;

pub trait Collector{
    fn open(&mut self) -> crate::Result<()> {
        Ok(())
    }
    fn collect(&mut self, row: &dyn Row);

    fn close(&mut self) -> crate::Result<()> {
        Ok(())
    }
}

pub struct SinkCollector {
    sink: Box<dyn Sink>,
}

impl SinkCollector {
    pub fn new(sink: Box<dyn Sink>) -> Self {
        Self { sink }
    }
}

impl Collector for SinkCollector {
    fn open(&mut self) -> crate::Result<()> {
        self.sink.open()
    }
    fn collect(&mut self, row: &dyn Row) {
        self.sink.invoke(row);
    }

    fn close(&mut self) -> crate::Result<()> {
        self.sink.close()
    }
}


pub struct PrintCollector;

impl Collector for PrintCollector {
    fn collect(&mut self, row: &dyn Row) {
        println!("{}", row);
    }
}
