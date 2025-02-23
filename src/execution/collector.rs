use crate::Result;
use crate::connector::Sink;
use crate::data::Row;
use crate::transform::Transform;

pub trait Collector{
    fn open(&mut self) -> Result<()> {
        Ok(())
    }
    fn collect(&mut self, row: &dyn Row) -> Result<()>;

    fn close(&mut self) -> Result<()> {
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
    fn collect(&mut self, row: &dyn Row) -> Result<()> {
        self.sink.invoke(row)
    }

    fn close(&mut self) -> crate::Result<()> {
        self.sink.close()
    }
}

pub struct TransformCollector {
    transform: Box<dyn Transform>,
    out:  Box<dyn Collector>,
}

impl TransformCollector {
    pub fn new(transform: Box<dyn Transform>, out:  Box<dyn Collector>) -> Self {
        Self { transform, out }
    }
}

impl Collector for TransformCollector {
    fn open(&mut self) -> Result<()> {
        self.transform.open()
    }
    fn collect(&mut self, row: &dyn Row) -> Result<()> {
        self.transform.process(row, self.out.as_mut())
    }

    fn close(&mut self) -> Result<()> {
        self.transform.close()
    }
}

pub struct PrintCollector;

impl Collector for PrintCollector {
    fn collect(&mut self, row: &dyn Row) -> Result<()> {
        println!("{}", row);
        Ok(())
    }
}
