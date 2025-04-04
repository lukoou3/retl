use std::collections::VecDeque;
use std::fmt::Debug;
use std::{mem, thread};
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
use itertools::Itertools;
use log::{error, info};
use postgres::{Client, NoTls};
use crate::Result;
use crate::config::{BaseIOMetrics, TaskContext};
use crate::connector::batch::BatchConfig;
use crate::connector::postgres::config::{PostgresDefaultBatchSettings, PostgresSinkConfig};
use crate::connector::Sink;
use crate::data::{Row, Value};
use crate::datetime_utils::{current_timestamp_millis, format_datetime_fafault, from_timestamp_micros_utc};
use crate::types::{DataType, Schema};

pub struct PostgresSink {
    task_context: TaskContext,
    connect_params: Arc<String>,
    table: Arc<String>,
    batch_config: BatchConfig<PostgresDefaultBatchSettings>,
    data_types: Vec<(usize, DataType)>,
    insert_sql_prefix: Arc<String>,
    sql_value: String,
    stoped: Arc<AtomicBool>,
    shared_blocks: Arc<(Mutex<(VecDeque<Block>, Block)>, Condvar)>,
    flush_handle: Option<JoinHandle<()>>,
}

impl PostgresSink {
    pub fn new(task_context: TaskContext, schema: Schema, sink_config: PostgresSinkConfig) -> Result<Self> {
        let mut connect_params = sink_config.connect_params;
        if !connect_params.contains("connect_timeout") {
            connect_params.push_str(" connect_timeout=10");
        }
        if !connect_params.contains("tcp_user_timeout") {
            connect_params.push_str(" tcp_user_timeout=300");
        }
        let data_types = schema.fields.iter().enumerate().map(|(i, field)| (i, field.data_type.clone())).collect();
        let columns = schema.fields.iter().map(|f| &f.name).join(",");
        let insert_sql_prefix = Arc::new(format!("INSERT INTO {} ({}) VALUES ", sink_config.table, columns));
        let sql_value = String::new();
        let stoped = Arc::new(AtomicBool::new(false));
        let shared_blocks = Arc::new((
            Mutex::new((VecDeque::new(), Block::new(insert_sql_prefix.clone()))),
            Condvar::new()
        ));
        Ok(Self {
            task_context,
            connect_params: Arc::new(connect_params),
            table: Arc::new(sink_config.table),
            batch_config: sink_config.batch_config,
            data_types,
            insert_sql_prefix,
            sql_value,
            stoped,
            shared_blocks,
            flush_handle: None,
        })
    }

    fn row_to_sql(&mut self, row: &dyn Row)  {
        self.sql_value.clear();
        self.sql_value.push_str("(");
        for (i, data_type) in self.data_types.iter() {
            if *i > 0 {
                self.sql_value.push_str(",");
            }
           Self::value_to_sql(&mut self.sql_value, row.get(*i), data_type);
        }
        self.sql_value.push_str(")");
    }

    fn value_to_sql(sql_value: &mut String, v: &Value, data_type: &DataType)  {
        if v.is_null() {
            sql_value.push_str("NULL");
            return;
        }

        match data_type {
            DataType::Int => sql_value.push_str(&v.get_int().to_string()),
            DataType::Long => sql_value.push_str(&v.get_long().to_string()),
            DataType::Float => sql_value.push_str(&v.get_float().to_string()),
            DataType::Double => sql_value.push_str(&v.get_double().to_string()),
            DataType::String => {
                let v = v.get_string();
                Self::put_str_escape(sql_value, v);
            },
            DataType::Boolean => sql_value.push_str(if v.get_boolean() { "1" } else { "0" }),
            DataType::Timestamp => {
                let tm = format_datetime_fafault(from_timestamp_micros_utc(v.get_long()));
                Self::put_str_escape(sql_value, &tm);
            },
            _ => sql_value.push_str("NULL")
        }
    }

    fn put_str_escape(sql_value: &mut String, v: &str)  {
        // TODO: escape string.
        sql_value.push_str("'");
        if v.contains('\'') {
            sql_value.push_str(&v.replace("'", "''"));
        } else {
            sql_value.push_str(v);
        }
        sql_value.push_str("'");
    }
}

impl Debug for PostgresSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSink")
            .field("task_context", &self.task_context)
            .field("connect_params", &self.connect_params)
            .field("table", &self.table)
            .field("batch_config", &self.batch_config)
            .field("data_types", &self.data_types)
            .field("insert_sql_prefix", &self.insert_sql_prefix)
            .finish()
    }
}

impl Sink for PostgresSink {
    fn open(&mut self) -> Result<()> {
        let base_iometrics = self.task_context.base_iometrics.clone();
        let connect_params = self.connect_params.clone();
        let stoped = self.stoped.clone();
        let shared_blocks = self.shared_blocks.clone();//block_deque
        let interval_ms = self.batch_config.interval_ms;
        let subtask_index =  self.task_context.task_config.subtask_index;
        let thread_name = format!("flush-{}-{}/{}", self.table.as_str(), subtask_index + 1, self.task_context.task_config.subtask_parallelism);
        let flush_handle = thread::Builder::new().name(thread_name).stack_size(512 * 1024).spawn(move || {
            PostgresSink::process_flush_block(base_iometrics, connect_params, stoped, shared_blocks, interval_ms)
        }).map_err(|e| e.to_string())?;
        self.flush_handle = Some(flush_handle);
        Ok(())
    }

    fn invoke(&mut self, row: &dyn Row) -> Result<()> {
        self.task_context.base_iometrics.num_records_in_inc_by(1);
        self.row_to_sql(row);

        let (lock, cvar) = self.shared_blocks.as_ref();
        let mut shared_blocks = lock.lock().unwrap();
        let block = &mut shared_blocks.1;
        block.write_row(&self.sql_value);
        if block.rows >= self.batch_config.max_rows || block.byte_size >= self.batch_config.max_bytes {
            // 使用 mem::replace 移动数据
            let data_block = mem::replace(block, Block::new(self.insert_sql_prefix.clone()));
            while shared_blocks.0.len() >= 1 {
                shared_blocks = cvar.wait(shared_blocks).unwrap(); // 等待工作线程处理完成
            }
            shared_blocks.0.push_back(data_block);
            cvar.notify_one(); // 通知工作线程
        }

        if shared_blocks.0.len() >= 1 {
            cvar.notify_one(); // 通知工作线程
        }

        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.stoped.store(true, Ordering::SeqCst);
        if let Some(flush_handle) = self.flush_handle.take() {
            let (_, cvar) = &*self.shared_blocks;
            cvar.notify_one(); // 通知工作线程处理剩余数据
            flush_handle.join().map_err(|_| "flush_handle join error".to_string())?;
        }
        Ok(())
    }
}

impl PostgresSink {
    fn process_flush_block(base_iometrics: Arc<BaseIOMetrics>, connect_params: Arc<String>, stoped: Arc<AtomicBool>,
                           shared_blocks: Arc<(Mutex<(VecDeque<Block>, Block)>, Condvar)>, interval_ms: u64)  {
        let mut last_flush_ts = current_timestamp_millis();
        let (lock, cvar) = shared_blocks.as_ref();
        let mut has_stoped = false;

        loop {
            let mut shared_blocks = lock.lock().unwrap();
            // 等待数据或超时
            let current_ms = current_timestamp_millis();
            let wait_ms = if has_stoped || current_ms > last_flush_ts + interval_ms {
                0
            } else {
                last_flush_ts + interval_ms - current_ms
            };
            let result = cvar.wait_timeout(shared_blocks, Duration::from_millis(wait_ms)).unwrap();
            shared_blocks = result.0;
            if let Some(block) = shared_blocks.0.pop_front() {
                cvar.notify_one(); // 通知生产线程
                drop(shared_blocks); // 释放共享数据的锁
                Self::flush_block(&base_iometrics, connect_params.as_str(), &mut last_flush_ts, block);
            } else {
                if current_timestamp_millis() >= last_flush_ts + interval_ms || has_stoped {
                    if shared_blocks.1.rows == 0 {
                        last_flush_ts = current_timestamp_millis();
                        if has_stoped {
                            break;
                        }
                        continue;
                    }

                    let empty_block = shared_blocks.1.copy_empty();
                    let block = mem::replace(&mut shared_blocks.1, empty_block);
                    drop(shared_blocks); // 释放共享数据的锁
                    Self::flush_block(&base_iometrics, connect_params.as_str(), &mut last_flush_ts, block);
                }
            }

            if stoped.load(Ordering::SeqCst) {
                has_stoped = true;
            }
        }
    }

    fn flush_block(base_iometrics: &BaseIOMetrics, connect_params: &str, last_flush_ts: &mut u64, block: Block) {
        let rows = block.rows as u64;
        let byte_size = block.byte_size as u64;
        info!("flush block start:{} rows,{} bytes, after:{}", rows, byte_size, current_timestamp_millis() - *last_flush_ts);
        *last_flush_ts = current_timestamp_millis();
        match Self::flush_block_inner(connect_params, &block) {
            Ok(_) => {
                info!("flush block success:{} rows,{} bytes, {} ms.", rows, byte_size, current_timestamp_millis() - *last_flush_ts);
                base_iometrics.num_records_out_inc_by(rows);
                base_iometrics.num_bytes_out_inc_by(byte_size);
            }
            Err(e) => {
                info!("flush block error:{:?}", e);
            }
        }
    }

    fn flush_block_inner(connect_params: &str, block: &Block) -> anyhow::Result<()>  {
        let mut client = Client::connect(connect_params, NoTls,)?;
        client.batch_execute(&block.update_sql)?;
        Ok(())
    }
}

struct Block {
    insert_sql_prefix: Arc<String>,
    update_sql: String,
    rows: usize,
    byte_size: usize,
}

impl Block {
    fn new(insert_sql_prefix: Arc<String>) -> Self {
        let mut update_sql = String::with_capacity(1024 * 1024);
        update_sql.push_str(insert_sql_prefix.as_str());
        Self { insert_sql_prefix, update_sql, rows: 0, byte_size: 0 }
    }

    fn copy_empty(&self) -> Self {
        Self::new(self.insert_sql_prefix.clone())
    }

    fn write_row(&mut self, row: &str) {
        if self.rows > 0 {
            self.update_sql.push_str(",");
        }
        self.update_sql.push_str(row);
        self.rows += 1;
        self.byte_size += row.len();
    }
}