use std::fmt::Debug;
use std::sync::{Arc};
use prometheus::{IntCounter,  Registry};

#[derive(Clone)]
pub struct TaskContext {
    pub task_config: TaskConfig,
    pub operator_config: OperatorConfig,
    pub base_iometrics: Arc<BaseIOMetrics>,
}

impl Debug for TaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("task_config", &self.task_config)
            .field("operator_config", &self.operator_config)
            .finish()
    }
}

impl TaskContext {
    pub fn new(task_config: TaskConfig, operator_config: OperatorConfig, base_iometrics: Arc<BaseIOMetrics>) -> Self {
        Self {
            task_config,
            operator_config,
            base_iometrics
        }
    }
}

impl Default for TaskContext {
    fn default() -> Self {
        let registry = Registry::new();
        Self {
            task_config: TaskConfig::new(1, 0, registry.clone()),
            operator_config: OperatorConfig::new(0),
            base_iometrics: Arc::new(BaseIOMetrics::new(&registry, "".to_string())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub subtask_parallelism: u8,
    pub subtask_index: u8,
    pub metrics_registry: Registry,
}

impl TaskConfig {
    pub fn new(subtask_parallelism: u8, subtask_index: u8, metrics_registry: Registry) -> Self {
        Self {
            subtask_parallelism,
            subtask_index,
            metrics_registry,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperatorConfig {
    pub id: u16,
}

impl OperatorConfig {
    pub fn new(id: u16) -> Self {
        Self {
            id,
        }
    }
}

pub struct BaseIOMetrics {
    prefix: String,
    num_records_in: IntCounter,
    num_records_out: IntCounter,
    // num_records_in_rate: IntGauge,
    // num_records_out_rate: IntGauge,
    num_bytes_in: IntCounter,
    num_bytes_out: IntCounter,
    // num_bytes_in_rate: IntGauge,
    // num_bytes_out_rate: IntGauge,
    // num_records_in_rate_stat: Mutex<SlidingWindowRateStat>,
    // num_records_out_rate_stat: Mutex<SlidingWindowRateStat>,
    // num_bytes_in_rate_stat: Mutex<SlidingWindowRateStat>,
    // num_bytes_out_rate_stat: Mutex<SlidingWindowRateStat>,
}

impl BaseIOMetrics {

    pub fn new(registry: &Registry, prefix: String) -> Self {
        let num_records_in = IntCounter::new(format!("{}_num_records_in", prefix), "number of records in").unwrap();
        let num_records_out = IntCounter::new(format!("{}_num_records_out", prefix), "number of records out").unwrap();
        //let num_records_in_rate = IntGauge::new(format!("{}_num_records_in_rate", prefix), "number of records in rate").unwrap();
        //let num_records_out_rate = IntGauge::new(format!("{}_num_records_out_rate", prefix), "number of records out rate").unwrap();
        let num_bytes_in = IntCounter::new(format!("{}_num_bytes_in", prefix), "number of bytes in").unwrap();
        let num_bytes_out = IntCounter::new(format!("{}_num_bytes_out", prefix), "number of bytes out").unwrap();
        //let num_bytes_in_rate = IntGauge::new(format!("{}_num_bytes_in_rate", prefix), "number of bytes in rate").unwrap();
        //let num_bytes_out_rate = IntGauge::new(format!("{}_num_bytes_out_rate", prefix), "number of bytes out rate").unwrap();
        registry.register(Box::new(num_records_in.clone())).unwrap();
        registry.register(Box::new(num_records_out.clone())).unwrap();
        //registry.register(Box::new(num_records_in_rate.clone())).unwrap();
        //registry.register(Box::new(num_records_out_rate.clone())).unwrap();
        registry.register(Box::new(num_bytes_in.clone())).unwrap();
        registry.register(Box::new(num_bytes_out.clone())).unwrap();
        //registry.register(Box::new(num_bytes_in_rate.clone())).unwrap();
        //registry.register(Box::new(num_bytes_out_rate.clone())).unwrap();
        Self {
            prefix,
            num_records_in,
            num_records_out,
            //num_records_in_rate,
            //num_records_out_rate,
            num_bytes_in,
            num_bytes_out,
            // num_bytes_in_rate,
            // num_bytes_out_rate,
            // num_records_in_rate_stat:  Mutex::new(SlidingWindowRateStat::with_window(10)),
            // num_records_out_rate_stat:  Mutex::new(SlidingWindowRateStat::with_window(10)),
            // num_bytes_in_rate_stat:  Mutex::new(SlidingWindowRateStat::with_window(10)),
            // num_bytes_out_rate_stat:  Mutex::new(SlidingWindowRateStat::with_window(10)),
        }
    }

    pub fn num_records_in_inc_by(&self, num_records: u64) {
        self.num_records_in.inc_by(num_records);
        /*let mut rate;
        {
            let mut rate_stat = self.num_records_in_rate_stat.lock().unwrap();
            rate = rate_stat.record_and_get_current_rate(num_records);
        }
        self.num_records_in_rate.set(rate as i64);*/
    }

    pub fn num_records_out_inc_by(& self, num_records: u64) {
        self.num_records_out.inc_by(num_records);
        /*let mut rate;
        {
            let mut rate_stat = self.num_records_out_rate_stat.lock().unwrap();
            rate = rate_stat.record_and_get_current_rate(num_records);
        }
        self.num_records_out_rate.set(rate as i64);*/
    }

    pub fn num_bytes_in_inc_by(& self, num_bytes: u64) {
        self.num_bytes_in.inc_by(num_bytes);
        /*let mut rate;
        {
            let mut rate_stat = self.num_bytes_in_rate_stat.lock().unwrap();
            rate = rate_stat.record_and_get_current_rate(num_bytes);
        }
        self.num_bytes_in_rate.set(rate as i64);*/
    }

    pub fn num_bytes_out_inc_by(& self, num_bytes: u64) {
        self.num_bytes_out.inc_by(num_bytes);
        /*let mut rate;
        {
            let mut rate_stat = self.num_bytes_out_rate_stat.lock().unwrap();
            rate = rate_stat.record_and_get_current_rate(num_bytes);
        }
        self.num_bytes_out_rate.set(rate as i64);*/
    }

}
