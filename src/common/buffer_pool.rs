use std::cmp::max;
use bytes::BytesMut;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::atomic::Ordering::SeqCst;
use std::time::{Duration, Instant};
use log::info;
use crate::datetime_utils::current_timestamp_millis;

#[derive(Clone)]
pub struct BufferPool {
    pool: Arc<Mutex<VecDeque<BufferWithTs>>>,
    min_cache_size: usize,
    max_cache_size: usize,
    keep_alive_millis: u64,
    clear_interval_millis: u64,
    last_clear_ts: Arc<AtomicU64>,
    current_cache_size: Arc<AtomicUsize>,
    last_log_ts: Arc<AtomicU64>,
}

struct BufferWithTs {
    buffer: BytesMut,
    ts: u64,
}

impl BufferPool {
    pub fn new(min_cache_size: usize, max_cache_size: usize, keep_alive_millis: u64) -> Self {
        let clear_interval_millis = max(keep_alive_millis / 10, 60_000);

        Self {
            pool: Arc::new(Mutex::new(VecDeque::new())),
            min_cache_size,
            max_cache_size,
            keep_alive_millis,
            clear_interval_millis,
            last_clear_ts: Arc::new(AtomicU64::new(current_timestamp_millis())),
            current_cache_size: Arc::new(AtomicUsize::new(0)),
            last_log_ts: Arc::new(AtomicU64::new(current_timestamp_millis())),
        }
    }

    pub fn acquire(&self, size: usize) -> BytesMut {
        let mut pool = self.pool.lock().unwrap();
        if let Some(buffer_with_ts) = pool.pop_front() {
            let buffer = buffer_with_ts.buffer;
            self.current_cache_size.fetch_sub(buffer.capacity(), SeqCst);
            buffer
        } else {
            BytesMut::with_capacity(size)
        }
    }

    pub fn release(&self, mut buffer: BytesMut) {
        let mut pool = self.pool.lock().unwrap();
        let mut current_cache_size = self.current_cache_size.load(SeqCst);

        if current_cache_size + buffer.capacity() <= self.max_cache_size {
            buffer.clear();
            self.current_cache_size.fetch_add(buffer.capacity(), SeqCst);
            current_cache_size = self.current_cache_size.load(SeqCst);
            let buffer_with_ts = BufferWithTs {
                buffer,
                ts: current_timestamp_millis(),
            };
            pool.push_front(buffer_with_ts);
        }

        // 不是可重入锁
        self.clear_expired_buffers_inner(pool);
    }

    pub fn clear_expired_buffers(&self) {
        let mut pool = self.pool.lock().unwrap();
        self.clear_expired_buffers_inner(pool);
    }

    fn clear_expired_buffers_inner(&self, mut pool: MutexGuard<VecDeque<BufferWithTs>>) {
        // 不是可重入锁
        let ts = current_timestamp_millis();
        let last_clear_ts = self.last_clear_ts.load(SeqCst);
        let last_log_ts = self.last_log_ts.load(SeqCst);

        if ts > last_log_ts + 300_000 {
            let current_cache_size = self.get_current_cache_size();
            info!("currentCacheSize: {}M", current_cache_size / 1024 / 1024);
            self.last_log_ts.store(ts, SeqCst);
        }

        if ts < last_clear_ts + self.clear_interval_millis  {
            return;
        }
        self.last_clear_ts.store(ts, SeqCst);

        while let Some(buffer_with_ts) = pool.back() {
            if self.get_current_cache_size() - buffer_with_ts.buffer.capacity() >= self.min_cache_size
                && ts > self.keep_alive_millis + buffer_with_ts.ts
            {
                let buffer_with_ts = pool.pop_back().unwrap();
                self.current_cache_size.fetch_sub(buffer_with_ts.buffer.capacity(), SeqCst);
            } else {
                break;
            }
        }
    }

    pub fn get_current_cache_size(&self) -> usize {
        self.current_cache_size.load(SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use flexi_logger::with_thread;

    fn init_logger() {
        flexi_logger::Logger::try_with_str("info")
            .unwrap()
            .format(with_thread)
            .start()
            .unwrap();
    }

    #[test]
    fn test_buffer_pool() {
        init_logger();
        // 创建 BufferPool
        let pool = BufferPool::new(
            1024 * 1024,                // min_cache_size: 1MB
            10 * 1024 * 1024,           // max_cache_size: 10MB
            300_000,   // keep_alive_time: 5 minutes
        );

        // 模拟多个线程并发申请和释放缓冲区
        let mut handles = vec![];
        //for i in 0..12
        for i in 0..12 {
            let pool_clone = pool.clone();
            let handle = thread::spawn(move || {
                info!("线程 {} 开始", i);
                let mut buffers = VecDeque::new();
                for _ in 0..1000 {
                    // 申请缓冲区
                    let buffer = pool_clone.acquire(1024);
                    buffers.push_back(buffer);
                }
                // 模拟使用缓冲区
                thread::sleep(Duration::from_millis(1000));
                while let Some(buffer) = buffers.pop_front(){
                    // 释放缓冲区
                    pool_clone.release(buffer);
                }

                for _ in 0..1000 {
                    // 申请缓冲区
                    let buffer = pool_clone.acquire(1024);
                    buffers.push_back(buffer);
                }
                thread::sleep(Duration::from_millis(1000));
                while let Some(buffer) = buffers.pop_front(){
                    // 释放缓冲区
                    pool_clone.release(buffer);
                }

                info!("线程 {} 完成", i);
            });
            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            handle.join().unwrap();
        }

        // 检查缓冲区池的状态
        let current_cache_size = pool.get_current_cache_size();
        let pool_size = pool.pool.lock().unwrap().len();
        info!("测试完成，当前缓存大小: {}M({})", current_cache_size / 1024 / 1024, current_cache_size);
        info!("缓冲区池中的缓冲区数量: {}", pool_size);

        // 断言缓冲区池的状态
        assert!(current_cache_size <= pool.max_cache_size);
        assert!(pool_size <= pool.max_cache_size / 1024);
    }
}