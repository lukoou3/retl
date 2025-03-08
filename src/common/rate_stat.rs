use crate::datetime_utils::current_timestamp_millis;

// 用于实时统计速率的滑动窗口实现
#[derive(Clone)]
pub struct SlidingWindowRateStat {
    window_second: u64,       // 窗口大小（秒）
    window_size: u64,         // 窗口大小（毫秒）
    bucket_size: usize,       // 桶的数量
    bucket_window_size: u64,  // 每个桶的时间窗口（毫秒）
    buckets: Vec<u64>,        // 存储每个桶的值
    current: usize,           // 当前桶的索引
    current_window_end_ms: u64, // 当前窗口的结束时间（毫秒）
    total_value: u64,         // 所有桶的总值
}

impl SlidingWindowRateStat {
    // 默认构造函数，使用 5 秒窗口
    pub fn new() -> Self {
        Self::with_window(5)
    }

    // 带窗口大小的构造函数
    pub fn with_window(window_second: u64) -> Self {
        assert!(window_second > 0 && window_second <= 300, "window_second must be between 1 and 300");

        let window_size = window_second * 1000; // 转换为毫秒
        let bucket_window_size = window_size / 100; // 100个桶, 每个桶 100ms
        let bucket_size = (window_size / bucket_window_size) as usize;

        SlidingWindowRateStat {
            window_second,
            window_size,
            bucket_size,
            bucket_window_size,
            buckets: vec![0; bucket_size],
            current: bucket_size - 1,
            current_window_end_ms: current_timestamp_millis() / bucket_window_size * bucket_window_size,
            total_value: 0,
        }
    }

    // 记录一个值
    pub fn record(&mut self, value: u64) {
        let time_ms = current_timestamp_millis();
        self.current_update(time_ms);
        self.buckets[self.current] += value;
        self.total_value += value;
    }

    // 记录值并返回当前速率
    pub fn record_and_get_current_rate(&mut self, value: u64) -> u64 {
        let time_ms = current_timestamp_millis();
        self.current_update(time_ms);
        self.buckets[self.current] += value;
        self.total_value += value;
        self.total_value / self.window_second
    }

    // 获取当前速率
    pub fn get_current_rate(&mut self) -> u64 {
        let time_ms = current_timestamp_millis();
        self.current_update(time_ms);
        self.total_value / self.window_second
    }

    // 使用指定时间获取当前速率
    pub fn get_current_rate_with_time(&mut self, time_ms: u64) -> u64 {
        self.current_update(time_ms);
        self.total_value / self.window_second
    }

    // 更新当前桶，滑动窗口
    fn current_update(&mut self, time_ms: u64) {
        if time_ms > self.current_window_end_ms {
            while self.current_window_end_ms < time_ms {
                self.current += 1;
                if self.current >= self.bucket_size {
                    self.current = 0;
                }
                self.total_value -= self.buckets[self.current];
                self.buckets[self.current] = 0;
                self.current_window_end_ms += self.bucket_window_size;
            }
        }
    }
}