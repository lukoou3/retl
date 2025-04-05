use crate::Result;
use crate::connector::Sink;
use crate::data::Row;
use crate::execution::TimeService;
use crate::transform::Transform;

pub trait Collector {
    fn open(&mut self) -> Result<()> {
        Ok(())
    }
    fn collect(&mut self, row: &dyn Row) -> Result<()>;

    fn check_timer(&mut self, time: u64) -> Result<()>;

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct SinkCollector {
    sink: Box<dyn Sink>,
}

impl SinkCollector {
    pub fn new(sink: Box<dyn Sink> ) -> Self {
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

    fn check_timer(&mut self, time: u64) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> crate::Result<()> {
        self.sink.close()
    }
}

pub struct TransformCollector {
    transform: Box<dyn Transform>,
    out:  Box<dyn Collector>,
    time_service: TimeService,
}

impl TransformCollector {
    pub fn new(transform: Box<dyn Transform>, out:  Box<dyn Collector>) -> Self {
        let time_service = TimeService::new();
        Self { transform, out, time_service }
    }
}

impl Collector for TransformCollector {
    fn open(&mut self) -> Result<()> {
        self.out.open()?;
        self.transform.open()
    }
    fn collect(&mut self, row: &dyn Row) -> Result<()> {
        self.transform.process(row, self.out.as_mut(), &mut self.time_service)
    }

    fn check_timer(&mut self, time: u64) -> Result<()> {
        while self.time_service.next_trigger_time() <= time {
            self.transform.on_time(self.time_service.next_trigger_time(), self.out.as_mut())?;
            self.time_service.poll_trigger_time();
        }
        self.out.check_timer(time)?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.transform.close().and(self.out.close())
    }
}

pub struct MultiCollector {
    outs:  Vec<Box<dyn Collector>>,
}

impl MultiCollector {
    pub fn new(outs:  Vec<Box<dyn Collector>>) -> Self {
        Self { outs }
    }
}

impl Collector for MultiCollector {
    fn open(&mut self) -> Result<()> {
        for out in self.outs.iter_mut() {
            out.open()?;
        }
        Ok(())
    }
    fn collect(&mut self, row: &dyn Row) -> Result<()> {
        for out in self.outs.iter_mut() {
            out.collect(row)?;
        }
        Ok(())
    }

    fn check_timer(&mut self, time: u64) -> Result<()> {
        for out in self.outs.iter_mut() {
            out.check_timer(time)?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        for out in self.outs.iter_mut() {
            out.close()?;
        }
        Ok(())
    }
}

pub struct PrintCollector;

impl Collector for PrintCollector {
    fn collect(&mut self, row: &dyn Row) -> Result<()> {
        println!("{}", row);
        Ok(())
    }

    fn check_timer(&mut self, time: u64) -> Result<()> {
        Ok(())
    }
}
