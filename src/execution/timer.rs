use std::collections::{BinaryHeap, HashSet};
use std::cmp::Reverse;
use ahash::AHasher;
use std::hash::BuildHasherDefault;

#[derive(Debug)]
pub struct TimeService {
    timers: BinaryHeap<Reverse<u64>>, // 最小堆，最早时间戳在顶部
    dup_timers: HashSet<u64, BuildHasherDefault<AHasher>>, // 用于去重
    next_trigger_time: u64, // 下一个触发时间戳
}

impl TimeService {
    pub fn new() -> Self {
        TimeService {
            timers: BinaryHeap::new(),
            dup_timers: HashSet::with_hasher(BuildHasherDefault::default()),
            next_trigger_time: u64::MAX,
        }
    }

    pub fn register_timer(&mut self, time: u64) {
        if !self.dup_timers.contains(&time) {
            self.dup_timers.insert(time);
            self.timers.push(Reverse(time));
            if time < self.next_trigger_time {
                self.next_trigger_time = time;
            }
        }
    }

    pub fn poll_trigger_time(&mut self) {
        let time = self.timers.pop().unwrap().0;
        self.dup_timers.remove(&time);
        if !self.timers.is_empty() {
            self.next_trigger_time = self.timers.peek().unwrap().0;
        } else {
            self.next_trigger_time = u64::MAX;
        }
    }

    #[inline]
    pub fn next_trigger_time(&self) -> u64 {
        self.next_trigger_time
    }
}

mod test {
    use std::thread::sleep;
    use chrono::Local;
    use super::*;

    #[test]
    fn test_time_service() {
        println!("Size of TimeService: {} bytes", size_of::<TimeService>());
        let mut time_service = TimeService::new();
        time_service.register_timer(10);
        time_service.register_timer(5);
        time_service.register_timer(20);
        time_service.register_timer(10);
        println!("{:?}", time_service);
        assert_eq!(time_service.next_trigger_time(), 5);
        time_service.poll_trigger_time();
        println!("{:?}", time_service);
        assert_eq!(time_service.next_trigger_time(), 10);
        time_service.poll_trigger_time();
        println!("{:?}", time_service);
        assert_eq!(time_service.next_trigger_time(), 20);
        time_service.poll_trigger_time();
        println!("{:?}", time_service);
        assert_eq!(time_service.next_trigger_time(), u64::MAX);
    }

    #[test]
    fn test_time_service_trigger() {
        let mut time_service = TimeService::new();
        for i in 0..100 {
            println!("{}: ele {}", Local::now(), i);
            time_service.register_timer(Local::now().timestamp_millis() as u64 / 1000 * 1000 + 1000);
            while time_service.next_trigger_time <= Local::now().timestamp_millis() as u64 {
                println!("{}: trigger {}", Local::now(), time_service.next_trigger_time);
                time_service.poll_trigger_time();
            }
            sleep(std::time::Duration::from_millis(100));
        }

        println!("end");
        while time_service.timers.len() != 0 {
            while time_service.next_trigger_time <= Local::now().timestamp_millis() as u64 {
                println!("{}: trigger {}", Local::now(), time_service.next_trigger_time);
                time_service.poll_trigger_time();
            }
            sleep(std::time::Duration::from_millis(100));
        }
    }
}