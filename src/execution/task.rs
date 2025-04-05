use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use crate::Result;
use crate::data::{GenericRow, Row};
use crate::execution::Collector;

enum PollStatus {
    More,
    End,
    Err(String),
}

struct TimeService {
    timers: VecDeque<u64>,
}

impl TimeService {
    fn new() -> Self {
        TimeService {
            timers: VecDeque::new(),
        }
    }

    fn register_timer(&mut self, timestamp: u64) {
        self.timers.len();
        self.timers.push_back(timestamp);
    }

}

trait Source {
    fn poll_next(&mut self, out: &mut dyn Collector, time_service: &mut TimeService) -> PollStatus;

    fn on_time(&mut self, timestamp: u64, out: &mut dyn Collector) -> Result<()> {
        Ok(())
    }
}

trait Transform {
    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector, time_service: &mut TimeService) -> Result<()> ;
    fn on_time(&mut self, timestamp: u64, out: &mut dyn Collector) -> Result<()> {
        Ok(())
    }
}

struct TestSource;

impl Source for TestSource {
    fn poll_next(&mut self, out: &mut dyn Collector, time_service: &mut TimeService) -> PollStatus {
        let row = GenericRow::new_with_size(2);
        match out.collect(&row) {
            Ok(_) => (),
            Err(e) => return PollStatus::Err(e),
        };
        PollStatus::More
    }

    fn on_time(&mut self, timestamp: u64, out: &mut dyn Collector) -> Result<()> {
        Ok(())
    }
}
