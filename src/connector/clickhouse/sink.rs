use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::Debug;
use std::{mem, thread};
use std::io::Read;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
use itertools::Itertools;
use log::{info, warn};
use reqwest::blocking::{Body, Client};
use reqwest::Url;
use serde_json::Value;
use crate::Result;
use crate::buffer_pool::BufferPool;
use crate::config::{BaseIOMetrics, TaskContext};
use crate::connector::batch::BatchConfig;
use crate::connector::clickhouse::{lz4, parse_date_type, ArcBlockReader, ArcCompressBlockReader, Block, ClickHouseDefaultBatchSettings, ColumnDesc, ConnectionConfig};
use crate::connector::Sink;
use crate::data::Row;
use crate::datetime_utils::current_timestamp_millis;
use crate::types::Schema;

pub struct ClickHouseSink {
    task_context: TaskContext,
    connection_config: ConnectionConfig,
    batch_config: BatchConfig<ClickHouseDefaultBatchSettings>,
    buffer_pool: BufferPool,
    column_descs: Vec<ColumnDesc>,
    insert_sql: String,
    stoped: Arc<AtomicBool>,
    shared_blocks: Arc<(Mutex<(VecDeque<Block>, Block)>, Condvar)>,
    flush_handle: Option<JoinHandle<()>>,
}

impl ClickHouseSink {
    pub fn new(task_context: TaskContext, schema: &Schema, connection_config: ConnectionConfig, batch_config: BatchConfig<ClickHouseDefaultBatchSettings>) -> Result<Self> {
        let buffer_pool = BufferPool::new(1024 * 1024 * 1, batch_config.max_bytes * 2 + 1024 * 1024 * 1, 600_000);
        let stoped = Arc::new(AtomicBool::new(false));
        let column_descs = Self::get_column_descs(&connection_config, &schema).map_err(|e| e.to_string())?;
        let shared_blocks = Arc::new((
            Mutex::new((VecDeque::new(), Block::new(buffer_pool.clone(), column_descs.clone())? )),
            Condvar::new()
        ));
        let insert_sql = format!("INSERT INTO {}({}) FORMAT Native", connection_config.table, schema.fields.iter().map(|f| f.name.as_str()).join(","));
        Ok(ClickHouseSink {
            task_context,
            connection_config,
            batch_config,
            buffer_pool,
            column_descs,
            insert_sql,
            stoped,
            shared_blocks,
            flush_handle: None,
        })
    }

    pub fn get_column_descs(connection_config: &ConnectionConfig, schema: &Schema) -> anyhow::Result<Vec<ColumnDesc>> {
        fn do_http(url: &str, connection_config: &ConnectionConfig) -> anyhow::Result<String> {
            let client = Client::new();
            let url = format!("{}?user={}&password={}&database={}&query=desc {}&default_format=JSON", url,
                              connection_config.user, connection_config.password, connection_config.database ,connection_config.table);
            let resp = client.get(url).send()?;
            if resp.status().is_success() {
                Ok(resp.text()?)
            } else {
                Err(anyhow::anyhow!("http request failed"))
            }
        }
        let field_types = schema.field_types();
        let mut column_types = HashMap::new();
        let urls = connection_config.build_urls();
        for (i, url) in urls.iter().enumerate() {
            let rst = do_http(url, connection_config);
            match rst {
                Ok(text) => {
                    let json: Value = serde_json::from_str(&text)?;
                    let datas = json["data"].as_array().ok_or(anyhow::anyhow!("can not get column descs"))?;
                    for data in datas {
                        let name = data["name"].as_str().ok_or(anyhow::anyhow!("can not get column name"))?;
                        let type_name = data["type"].as_str().ok_or(anyhow::anyhow!("can not get column type"))?;
                        if field_types.contains_key(name) {
                            column_types.insert(name, type_name);
                        }
                    }
                    let mut cols = Vec::with_capacity(schema.fields.len());
                    for f in schema.fields.iter() {
                        let ck_type_str = *column_types.get(f.name.as_str()).ok_or(anyhow::anyhow!("not column:{}", &f.name))?;
                        let ck_type = parse_date_type(ck_type_str).ok_or(anyhow::anyhow!("can not parse column type:{}", ck_type_str))?;
                        cols.push(ColumnDesc::new(f.name.to_string(), f.data_type.clone(), ck_type))
                    }
                    return Ok(cols)
                },
                Err(e) => return Err(e)
            }
        }

        Err(anyhow::anyhow!("config no host:{:?}", connection_config))
    }
}

impl Debug for ClickHouseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSink")
            .field("task_context", &self.task_context)
            .field("connection_config", &self.connection_config)
            .field("batch_config", &self.batch_config)
            .finish()
    }
}

impl Sink for ClickHouseSink {
    fn open(&mut self) -> Result<()> {
        let base_iometrics = self.task_context.base_iometrics.clone();
        let buffer_pool = self.buffer_pool.clone();
        let insert_sql = self.insert_sql.clone();
        let column_descs = self.column_descs.clone();
        let connection_config = self.connection_config.clone();
        let stoped = self.stoped.clone();
        let shared_blocks = self.shared_blocks.clone();//block_deque
        let interval_ms = self.batch_config.interval_ms;
        let thread_name = format!("flush-{}-{}/{}", self.connection_config.table, self.task_context.task_config.subtask_index + 1, self.task_context.task_config.subtask_parallelism);
        let flush_handle = thread::Builder::new().name(thread_name).stack_size(128 * 1024).spawn(move || {
            ClickHouseSink::process_flush_block(base_iometrics, buffer_pool, insert_sql, column_descs, connection_config, stoped, shared_blocks, interval_ms)
        }).map_err(|e| e.to_string())?;
        self.flush_handle = Some(flush_handle);
        Ok(())
    }

    fn invoke(&mut self, row: &dyn Row) -> Result<()> {
        self.task_context.base_iometrics.num_records_in_inc_by(1);

        let (lock, cvar) = self.shared_blocks.as_ref();
        let mut shared_blocks = lock.lock().unwrap();
        let block = &mut shared_blocks.1;
        block.write(row)?;
        if block.rows() >= self.batch_config.max_rows || block.byte_size() >= self.batch_config.max_bytes {
            // 使用 mem::replace 移动数据
            let data_block = mem::replace(block, Block::new(self.buffer_pool.clone(), self.column_descs.clone())?);
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

impl ClickHouseSink {
    fn process_flush_block(base_iometrics: Arc<BaseIOMetrics>, buffer_pool: BufferPool, insert_sql: String, column_descs: Vec<ColumnDesc>,
                           connection_config: ConnectionConfig, stoped: Arc<AtomicBool>, shared_blocks: Arc<(Mutex<(VecDeque<Block>, Block)>, Condvar)>, interval_ms: u64)  {
        let urls = connection_config.build_urls();
        let mut url_index = 0;
        let mut last_flush_ts = current_timestamp_millis();
        let (lock, cvar) = shared_blocks.as_ref();
        let mut has_stoped = false;

        loop {
            // 持有共享数据的锁
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
                Self::flush_block(&base_iometrics, &insert_sql, &connection_config, &urls, &mut url_index, &mut last_flush_ts, block);
            } else {
                if current_timestamp_millis() >= last_flush_ts + interval_ms || has_stoped {
                    if shared_blocks.1.rows() == 0 {
                        last_flush_ts = current_timestamp_millis();
                        buffer_pool.clear_expired_buffers();
                        if has_stoped {
                            break;
                        }
                        continue;
                    }

                    let empty_block = Block::new(buffer_pool.clone(), column_descs.clone()).unwrap();
                    let block = mem::replace(&mut shared_blocks.1, empty_block);
                    drop(shared_blocks); // 释放共享数据的锁
                    Self::flush_block(&base_iometrics, &insert_sql, &connection_config, &urls, &mut url_index,  &mut last_flush_ts, block);
                }
            }

            if stoped.load(Ordering::SeqCst) {
                has_stoped = true;
            }

        }
    }

    fn flush_block(base_iometrics: &Arc<BaseIOMetrics>, insert_sql: &str,
                   connection_config: &ConnectionConfig, urls: &Vec<String>, url_index: &mut usize, last_flush_ts: &mut u64, block: Block )  {
        let rows = block.rows() as u64;
        let byte_size = block.byte_size() as u64;
        let compress = true;
        let arc_block = Arc::new(Mutex::new(block));
        if let Err(e) = Self::flush_block_inner(insert_sql, connection_config, urls, url_index, arc_block.clone(), rows , byte_size , compress) {
           warn!("flush block error:{:?}", e)
        } else {
            base_iometrics.num_records_out_inc_by(rows);
            base_iometrics.num_bytes_out_inc_by(byte_size);
        }

        arc_block.lock().unwrap().release_buffer();
        *last_flush_ts = current_timestamp_millis();
    }

    fn flush_block_inner(insert_sql: &str, connection_config: &ConnectionConfig, urls: &Vec<String>, url_index: &mut usize, arc_block: Arc<Mutex<Block>>,
                         rows: u64, byte_size: u64, compress: bool ) -> core::result::Result<(), Box<dyn Error>>  {
        info!("flush block start:{} rows,{} bytes", rows, byte_size);

        let host = &urls[*url_index];
        let mut url = Url::parse(host)?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();
        pairs.append_pair("database", "test");
        pairs.append_pair("query", &insert_sql);
        if compress {
            pairs.append_pair("decompress", "1");
        }
        drop(pairs);
        let url = url.as_ref();

        let client = Client::builder().build()?;
        let r = client .post(url)
            .timeout(Duration::from_secs(3000))
            .header("X-ClickHouse-User", &connection_config.user)
            .header("X-ClickHouse-Key", &connection_config.password)
            .header("Content-Type", "application/octet-stream")
            ;

        let body = if compress {
            /*let mut buffer1 = Vec::new();
            arc_block.lock().unwrap().read_to_end(&mut buffer1).unwrap();
            let buffer2 = lz4::compress(&buffer1).unwrap().to_vec();
            Body::from(buffer2)*/
            Body::new(ArcCompressBlockReader::from(arc_block))
        } else {
            Body::new(ArcBlockReader::from(arc_block))
        };

        let response = r.body(body).send()?;
        if response.status().is_success() {
            info!("flush block success:{} rows,{} bytes.", rows, byte_size);
        } else {
            warn!("flush block error:{}, {}", response.status(), response.text()?);
        }

        Ok(())
    }
}

mod test {
    use std::error::Error;
    use reqwest::blocking::Client;
    use reqwest::Url;
    use crate::connector::clickhouse::{ClickHouseDefaultBatchSettings, ClickHouseSink, ConnectionConfig};
    use crate::types::{DataType, Field, Fields, Schema};

    #[test]
    fn test_get_column_descs() {
        let connection_config = ConnectionConfig {
            host: "192.168.216.86:8123".to_string(),
            user: "default".to_string(),
            password: "123456".to_string(),
            database: "test".to_string(),
            table: "test_ck_simple".to_string(),
        };
        let fields = vec![
            Field::new("id", DataType::Long),
            Field::new("datetime", DataType::Timestamp),
            Field::new("int32", DataType::Int),
            Field::new("int32_nullalbe", DataType::Int),
            Field::new("str", DataType::String),
        ];
        let schema = Schema::new(fields);
        let column_descs = ClickHouseSink::get_column_descs(&connection_config, &schema);
        println!("{:#?}", column_descs)
    }

    #[test]
    fn test_http() -> Result<(), Box<dyn Error>> {
        let client = Client::new();

        let query = "SELECT 1";
        let url = "http://192.168.216.86:8123";
        let mut url = Url::parse(url).unwrap();
        let mut pairs = url.query_pairs_mut();
        pairs.clear();
        pairs.append_pair("database", "test");
        pairs.append_pair("query", &query);
        pairs.append_pair("user", "default");
        pairs.append_pair("password", "123456");
        pairs.append_pair("default_format", "JSON");
        drop(pairs);
        let url = url.as_ref();
        println!("url:{}", url);
        let response = client.get(url).send()?;

        println!("Response: {:#?}", response);
        println!("text: {}", response.text()?);

        Ok(())
    }

    #[test]
    fn test_http_get() -> Result<(), Box<dyn Error>> {
        let client = Client::new();
        let query = "desc test_ck_simple";
        let url = "http://192.168.216.86:8123";
        let builder = client.get(url).query(&[("database", "test"), ("query", &query), ("user", "default"), ("password", "123456"), ("default_format", "JSON")]);
        println!("builder:{:#?}", builder);
        let response = builder.send()?;

        println!("Response: {:#?}", response);
        println!("text: {}", response.text()?);

        Ok(())
    }

    #[test]
    fn test_http_get2() -> Result<(), Box<dyn Error>> {
        let client = Client::new();
        let url = "http://single:8123?user=default&password=123456&database=test&query=desc test_ck_simple&default_format=JSON";
        let builder = client.get(url);
        println!("builder:{:#?}", builder);
        let response = builder.send()?;

        println!("Response: {:#?}", response);
        println!("text: {}", response.text()?);

        Ok(())
    }
}