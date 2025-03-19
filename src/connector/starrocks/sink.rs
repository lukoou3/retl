use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::{io, mem, thread};
use std::cmp::min;
use std::error::Error;
use std::str::FromStr;
use std::thread::JoinHandle;
use std::time::Duration;
use anyhow::{anyhow, Context};
use bytes::{BufMut, BytesMut};
use log::{info, warn};
use reqwest::blocking::{Body, Client, Request, Response};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{redirect, StatusCode, Url};
use serde_json::Value;
use crate::Result;
use crate::buffer_pool::BufferPool;
use crate::codecs::{JsonSerializer, Serializer};
use crate::config::{BaseIOMetrics, TaskContext};
use crate::connector::batch::BatchConfig;
use crate::connector::Sink;
use crate::connector::starrocks::{basic_auth_header, lz4, ConnectionConfig, StarRocksDefaultBatchSettings};
use crate::data::Row;
use crate::datetime_utils::current_timestamp_millis;

const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const LOCALHOST: &str = "localhost";
const LOCALHOST_IP: &str = "127.0.0.1";

pub struct StarRocksSink {
    task_context: TaskContext,
    connection_config: ConnectionConfig,
    batch_config: BatchConfig<StarRocksDefaultBatchSettings>,
    serializer: JsonSerializer,
    buffer_pool: BufferPool,
    stoped: Arc<AtomicBool>,
    shared_blocks: Arc<(Mutex<(VecDeque<Block>, Block)>, Condvar)>,
    flush_handle: Option<JoinHandle<()>>,
    flush_err: Option<String>,
    total_flush: Arc<AtomicU64>,
    //last_flush_ts: Arc<AtomicU64>,
}

impl StarRocksSink {
    pub fn new(task_context: TaskContext, connection_config: ConnectionConfig, batch_config: BatchConfig<StarRocksDefaultBatchSettings>, serializer: JsonSerializer) -> Self {
        let buffer_pool = BufferPool::new(1024 * 1024 * 1, batch_config.max_bytes * 2 + 1024 * 1024 * 1, 600_000);
        let stoped = Arc::new(AtomicBool::new(false));
        let shared_blocks = Arc::new((
            Mutex::new((VecDeque::new(), Block::new(buffer_pool.clone())) ),
            Condvar::new()
        ));
        StarRocksSink {
            task_context,
            connection_config,
            batch_config,
            serializer,
            buffer_pool,
            stoped,
            shared_blocks,
            flush_handle: None,
            flush_err: None,
            total_flush: Arc::new(AtomicU64::new(current_timestamp_millis())),
            // last_flush_ts: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Debug for StarRocksSink{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StarRocksSink")
            .field("connection_config", &self.connection_config)
            .field("batch_config", &self.batch_config)
            .field("serializer", &self.serializer)
            .finish()
    }
}

impl Sink for StarRocksSink {

    fn open(&mut self) -> Result<()> {
        let base_iometrics = self.task_context.base_iometrics.clone();
        let connection_config = self.connection_config.clone();
        let stoped = self.stoped.clone();
        let shared_blocks = self.shared_blocks.clone();//block_deque
        let total_flush = self.total_flush.clone();
        let interval_ms = self.batch_config.interval_ms;
        let subtask_index =  self.task_context.task_config.subtask_index;
        let thread_name = format!("flush-{}-{}/{}", self.connection_config.table, subtask_index + 1, self.task_context.task_config.subtask_parallelism);
        let flush_handle = thread::Builder::new().name(thread_name).stack_size(128 * 1024).spawn(move || {
            StarRocksSink::process_flush_block(subtask_index, base_iometrics, connection_config, stoped, shared_blocks, total_flush, interval_ms)
        }).map_err(|e| e.to_string())?;
        self.flush_handle = Some(flush_handle);
        Ok(())
    }

    fn invoke(&mut self, row: &dyn Row) -> Result<()> {
        self.task_context.base_iometrics.num_records_in_inc_by(1);
        let bytes = self.serializer.serialize(row)?;

        let (lock, cvar) = self.shared_blocks.as_ref();
        let mut shared_blocks = lock.lock().unwrap();

        let block = &mut shared_blocks.1;
        block.write_row(bytes);
        if block.batch_rows >= self.batch_config.max_rows || block.batch_bytes >= self.batch_config.max_bytes {
            block.write_end();
            // 使用 mem::replace 移动数据
            let data_block = mem::replace(block, Block::new(self.buffer_pool.clone()));
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

impl StarRocksSink {
    fn process_flush_block(subtask_index: u8, base_iometrics: Arc<BaseIOMetrics>, connection_config: ConnectionConfig, stoped: Arc<AtomicBool>, shared_blocks: Arc<(Mutex<(VecDeque<Block>, Block)>, Condvar)>,
                           total_flush: Arc<AtomicU64>, interval_ms: u64,) {
        let urls = connection_config.build_urls();
        let mut url_index = subtask_index as usize % urls.len();
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
                Self::flush_block(&base_iometrics, &connection_config, &urls, &mut url_index, total_flush.clone(), &mut last_flush_ts, block);
            } else {
                if current_timestamp_millis() >= last_flush_ts + interval_ms || has_stoped {
                    if shared_blocks.1.batch_rows == 0 {
                        last_flush_ts = current_timestamp_millis();
                        shared_blocks.1.buffer_pool.clear_expired_buffers();
                        if has_stoped {
                            break;
                        }
                        continue;
                    }
                    
                    shared_blocks.1.write_end();
                    let empty_block = Block::new(shared_blocks.1.buffer_pool.clone());
                    let block = mem::replace(&mut shared_blocks.1, empty_block);
                    drop(shared_blocks); // 释放共享数据的锁
                    Self::flush_block(&base_iometrics, &connection_config, &urls, &mut url_index, total_flush.clone(), &mut last_flush_ts, block);
                }
            }

            if stoped.load(Ordering::SeqCst) {
                has_stoped = true;
            }
            
        }

    }

    fn flush_block(base_iometrics: &Arc<BaseIOMetrics>, connection_config: &ConnectionConfig, urls: &Vec<String>, url_index: &mut usize, total_flush: Arc<AtomicU64>,  last_flush_ts: &mut u64, block: Block )  {
        let batch_rows = block.batch_rows as u64;
        let batch_bytes = block.batch_bytes;
        let buffers: Arc<Vec<BytesMut>> = Arc::new(block.buffers);
        info!("flush block start:{} rows,{} bytes, after:{}", batch_rows, batch_bytes, current_timestamp_millis() - *last_flush_ts);
        *last_flush_ts = current_timestamp_millis();
        let mut retry = 0;
        loop {
            retry += 1;
            match Self::flush_block_inner(connection_config, urls, url_index, buffers.clone()) {
                Ok(json) => {
                    info!("flush block success:{} rows,{} bytes, {} ms. \t{}", batch_rows, batch_bytes, current_timestamp_millis() - *last_flush_ts , json);
                    total_flush.fetch_add(batch_rows, Ordering::SeqCst);
                    base_iometrics.num_records_out_inc_by(batch_rows);
                    base_iometrics.num_bytes_out_inc_by(batch_bytes as u64);
                    break;
                },
                Err(e) => {
                    if retry >= 2 || retry >= urls.len() {
                        warn!("flush block error:{:?}", e);
                        break;
                    } else {
                        warn!("retry({}) flush block error:{:?}", retry, e);
                    }
                }
            }
            *url_index += 1;
            if *url_index == urls.len() {
                *url_index = 0;
            }
        }
        if let Ok(buffers) = Arc::try_unwrap(buffers) {
            for buffer in buffers {
                block.buffer_pool.release(buffer);
            }
        } else {
            warn!("Arc<Vec<BytesMut>> still has multiple references, cannot recycle yet");
        }
    }

    fn flush_block_inner(connection_config: &ConnectionConfig, urls: &Vec<String>, url_index: &mut usize,  buffers: Arc<Vec<BytesMut>>) ->anyhow::Result<serde_json::Value>  {
        let url = &urls[*url_index];
        let fe_host = Url::parse(&url)?.host_str().unwrap().to_string();
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .redirect(redirect::Policy::none())
            .build()?;
        let header_map = Self::construct_headers(connection_config)?;
        let request = client.put(url)
            .timeout(Duration::from_secs(300))
            .headers(header_map.clone())
            .build()?;;

        let mut be_request = match send_stream_load_request(client.clone(), request, &fe_host)? {
            StreamLoadResponse::BeRequest(be_request) => be_request,
            StreamLoadResponse::HttpResponse(resp) => {
                // If we get a response here, it should be from BE, so we extract the URL
                // and create a new request based on it.
                let url = resp.url().clone();
                client.put(url)
                    .timeout(Duration::from_secs(300))
                    .headers(header_map)
                    .build()?
            }
        };
        if connection_config.compress {
            *be_request.body_mut() = Some(Body::new(lz4::VecBytesMutCompressReader::new(buffers)));
        } else {
            *be_request.body_mut() = Some(Body::new(VecBytesMutReader::new(buffers)));
        }

        let response = client.execute(be_request)?;

        if response.status().is_success() {
            let json: serde_json::Value = response.json()?;
            Ok(json)
        } else {
            Err(anyhow!("Stream load failed: {}, {}", response.status(), response.text()?))
        }

    }

    fn construct_headers(connection_config: &ConnectionConfig) -> anyhow::Result<HeaderMap>  {
        let mut headers = HeaderMap::new();
        for (k, v) in connection_config.properties.iter() {
            headers.insert(HeaderName::from_str(k)?, HeaderValue::from_str(v)?);
        }
        Ok(headers)
    }
}

/// Send the request and handle redirection if any.
/// The reason we handle the redirection manually is that if we let `reqwest` handle the redirection
/// automatically, it will remove sensitive headers (such as Authorization) during the redirection,
/// and there's no way to prevent this behavior.
/// Please note, the FE address that user specified might be a FE follower not the leader, in this case,
/// the follower FE will redirect request to leader FE and then to BE.
fn send_stream_load_request(
    client: Client,
    mut request: Request,
    fe_host: &str,
) -> anyhow::Result<StreamLoadResponse> {
    // possible redirection paths:
    // RW <-> follower FE -> leader FE -> BE
    // RW <-> leader FE -> BE
    // RW <-> leader FE
    for _ in 0..2 {
        info!("conn url:{}", request.url());
        let original_http_port = request.url().port();
        let mut request_for_redirection = request.try_clone().ok_or_else(|| anyhow!("Can't clone request"))?;
        *request_for_redirection.timeout_mut() = Some(Duration::from_secs(300));
        let resp = client.execute(request).context("sending stream load request failed")?;
        let be_url = try_get_be_url(&resp, fe_host)?;
        match be_url {
            Some(be_url) => {
                // we used an unconventional method to detect if we are currently redirecting to FE leader, i.e.,
                // by comparing the port of the redirected url with that of the original request, if they are same, we consider
                // this is a FE address. Because in practice, no one would deploy their `StarRocks` cluster with the same
                // http port for both FE and BE. However, this is a potentially problematic assumption,
                // we may investigate a better way to do this. For example, we could use the `show backends` command to check
                // if the host of the redirected url is in the list. However, `show backends` requires
                // the system-level privilege, which could break the backward compatibility.
                info!("redirect url:{}", be_url);
                let redirected_port = be_url.port();
                *request_for_redirection.url_mut() = be_url;
                if redirected_port == original_http_port {
                    // redirected to FE, continue another round.
                    request = request_for_redirection;
                } else {
                    // we got BE address here
                    return Ok(StreamLoadResponse::BeRequest(request_for_redirection));
                }
            }
            None => {
                println!("response");
                return Ok(StreamLoadResponse::HttpResponse(resp))
            },
        }
    }

    Err(anyhow!("redirection occur more than twice when sending stream load request"))
}

/// Try getting BE url from a redirected response, returning `Ok(None)` indicates this request does
/// not redirect.
///
/// The reason we handle the redirection manually is that if we let `reqwest` handle the redirection
/// automatically, it will remove sensitive headers (such as Authorization) during the redirection,
/// and there's no way to prevent this behavior.
fn try_get_be_url(resp: &Response, fe_host: &str) -> anyhow::Result<Option<Url>> {
    match resp.status() {
        StatusCode::TEMPORARY_REDIRECT => {
            let be_url = resp
                .headers()
                .get("location")
                .ok_or_else(|| anyhow!("Can't get doris BE url in header"))?
                .to_str()
                .context("Can't get doris BE url in header")?
                .to_string();

            let mut parsed_be_url = Url::parse(&be_url)?;

            if fe_host != LOCALHOST && fe_host != LOCALHOST_IP {
                let be_host = parsed_be_url.host_str().ok_or_else(|| anyhow!("Can't get be host from url"))?;

                if be_host == LOCALHOST || be_host == LOCALHOST_IP {
                    // if be host is 127.0.0.1, we may can't connect to it directly,
                    // so replace it with fe host
                    parsed_be_url
                        .set_host(Some(fe_host))?;
                }
            }
            Ok(Some(parsed_be_url))
        }
        StatusCode::OK => {
            // Some of the `StarRocks` transactional APIs will respond directly from FE. For example,
            // the request to `/api/transaction/commit` endpoint does not seem to redirect to BE.
            // In this case, the request should be treated as finished.
            Ok(None)
        }
        _ => Err(anyhow!("Can't get doris BE url")),
    }
}


enum StreamLoadResponse {
    BeRequest(Request),
    HttpResponse(Response),
}

pub struct VecBytesMutReader {
    buffers: Arc<Vec<BytesMut>>,    // 持有 Vec<BytesMut> 的引用
    pos: usize,                // 当前读取的 buffer 索引
    offset: usize,             // 当前 buffer 的读取偏移量
}

impl VecBytesMutReader {
    pub fn new(buffers: Arc<Vec<BytesMut>>) -> Self {
        VecBytesMutReader {
            buffers,
            pos: 0,
            offset: 0,
        }
    }

    pub fn reset(&mut self) {
        self.pos = 0;
        self.offset = 0;
    }
}

impl Read for VecBytesMutReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.buffers.len() {
            return Ok(0); // 已经读取到末尾
        }

        let mut total_bytes_read = 0;

        while total_bytes_read < buf.len() && self.pos < self.buffers.len() {
            let current_buffer = &self.buffers[self.pos];
            let remaining = current_buffer.len() - self.offset;

            if remaining > 0 {
                // 计算当前这次能读取多少字节
                let bytes_to_read = min(remaining, buf.len() - total_bytes_read);

                // 复制数据到输出缓冲区
                buf[total_bytes_read..total_bytes_read + bytes_to_read]
                    .copy_from_slice(&current_buffer[self.offset..self.offset + bytes_to_read]);

                // 更新位置
                self.offset += bytes_to_read;
                total_bytes_read += bytes_to_read;
            }

            // 如果当前 buffer 读完，移动到下一个
            if self.offset >= current_buffer.len() {
                self.pos += 1;
                self.offset = 0;
            }
        }

        Ok(total_bytes_read)
    }
}

struct Block {
    buffer_pool: BufferPool,
    buffers: Vec<BytesMut>,
    buffer_index: usize,
    batch_rows: usize,
    batch_bytes: usize,
}

const BLOCK_BUFFER_SIZE: usize = 1024 * 256;
const FIRST: [u8; 1] = [b'['];
const SEP: [u8; 1] = [b','];
const END: [u8; 1] = [b']'];

impl Block {

    fn new(buffer_pool: BufferPool) -> Self {
        let mut buffers = Vec::new();
        let buffer = buffer_pool.acquire(BLOCK_BUFFER_SIZE);
        buffers.push(buffer);
        Block {
            buffer_pool,
            buffers,
            buffer_index: 0,
            batch_rows: 0,
            batch_bytes: 0,
        }
    }

    fn write_row(&mut self, row: &[u8]) {
        if self.batch_rows == 0 {
            self.write_byte(&FIRST);
            self.batch_bytes += 1; // 分隔符也算
        } else {
            self.write_byte(&SEP);
        }
        self.write_binary(row);
        self.batch_rows += 1;
        self.batch_bytes += row.len() + 1;
    }

    fn write_end(&mut self) {
        self.write_byte(&END);
    }

    fn release(&mut self) {
        for buffer in self.buffers.drain(..) {
            self.buffer_pool.release(buffer);
        }
        self.buffer_index = 0;
        self.batch_rows = 0;
        self.batch_bytes = 0;
    }

    fn write_byte(&mut self, byte: &[u8; 1]) {
        let buffer = &mut self.buffers[self.buffer_index];
        buffer.extend_from_slice(byte);
        self.flush_to_target(false);
    }

    fn write_binary(&mut self, bytes: &[u8]) {
        let mut offset = 0;
        let mut length = bytes.len();

        while length > 0 {
            let buffer = &mut self.buffers[self.buffer_index];
            let remaining = buffer.capacity() - buffer.len(); // buffer.capacity() - buffer.len();
            if remaining < length {
                buffer.extend_from_slice(&bytes[offset..offset + remaining]);
                self.flush_to_target(true);
                offset += remaining;
                length -= remaining;
            } else {
                buffer.extend_from_slice(&bytes[offset..offset + length]);
                break;
            }
        }

        self.flush_to_target(false);
    }

    fn flush_to_target(&mut self, force: bool) {
        let buffer = &mut self.buffers[self.buffer_index];
        let remaining = buffer.capacity() - buffer.len();
        if remaining > 0 && !force {
            return;
        }

        let buffer = self.buffer_pool.acquire(BLOCK_BUFFER_SIZE);
        self.buffers.push(buffer);
        self.buffer_index += 1;
    }

}

#[cfg(test)]
mod tests {
    use reqwest::{redirect, Method};
    use reqwest::blocking::Body;
    use super::*;
    #[test]
    fn test_block() {
        let mut block = Block::new(BufferPool::new(1024 * 1024 * 1, 1024 * 1024 * 10, 600_000));
        let mut combine_bytes = Vec::new();
        let bytes = Box::new([1u8; BLOCK_BUFFER_SIZE - 2]); // remaining: 1
        block.write_row(bytes.as_slice());
        combine_bytes.extend_from_slice(&FIRST);
        combine_bytes.extend_from_slice(bytes.as_slice());
        let bytes =  Box::new([2u8; 1]); // remaining: 0 => BLOCK_BUFFER_SIZE
        block.write_row(bytes.as_slice());
        combine_bytes.extend_from_slice(&SEP);
        combine_bytes.extend_from_slice(bytes.as_slice());
        let bytes =  Box::new([1u8; BLOCK_BUFFER_SIZE - 1]); // remaining: 0 => BLOCK_BUFFER_SIZE
        block.write_row(bytes.as_slice());
        combine_bytes.extend_from_slice(&SEP);
        combine_bytes.extend_from_slice(bytes.as_slice());
        let bytes =  Box::new([2u8; BLOCK_BUFFER_SIZE + 5]); // remaining: BLOCK_BUFFER_SIZE - 6
        block.write_row(bytes.as_slice());
        combine_bytes.extend_from_slice(&SEP);
        combine_bytes.extend_from_slice(bytes.as_slice());

        block.write_end();
        combine_bytes.extend_from_slice(&END);

        let mut expect_combine_bytes = Vec::new();
        let mut byte_size = 0;
        for buffer in &block.buffers {
            byte_size += buffer.len();
            expect_combine_bytes.extend_from_slice(buffer);
            println!("len:{}, capacity:{}", buffer.len(), buffer.capacity());
        }
        println!("byte_size:{}, combine_bytes:{}, expect_combine_bytes:{}", byte_size, combine_bytes.len(), expect_combine_bytes.len());
        assert_eq!(combine_bytes, expect_combine_bytes);
    }

    #[test]
    fn do_stream_load_request() -> anyhow::Result<()> {
        let url = "http://192.168.216.86:8061/api/test/object_stat/_stream_load";
        let data = r#"[
        {"timestamp":"2025-03-02 14:19:25","object_id":1},
        {"timestamp":"2025-03-02 14:19:25","object_id":2}
        ]"#;

        let fe_host = Url::parse(&url)?.host_str().unwrap().to_string();
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .redirect(redirect::Policy::none())
            .build()?;
        let request = client.put(url)
            .timeout(Duration::from_secs(1800))
            .header("authorization", "Basic cm9vdDo=")
            //.basic_auth(username, Some(password))
            .header("Expect", "100-continue")
            .header("two_phase_commit", "false")
            .header("format", "json")
            .header("strip_outer_array", "true")
            .header("ignore_json_size", "true")
            .build()?;
        let mut be_request = match send_stream_load_request(client.clone(), request, &fe_host)? {
                StreamLoadResponse::BeRequest(be_request) => be_request,
                StreamLoadResponse::HttpResponse(resp) => {
                    // If we get a response here, it should be from BE, so we extract the URL
                    // and create a new request based on it.
                    let url = resp.url().clone();
                    client.put(url)
                        .timeout(Duration::from_secs(1800))
                        .header("authorization", "Basic cm9vdDo=")
                        //.basic_auth(username, Some(password))
                        .header("Expect", "100-continue")
                        .header("two_phase_commit", "false")
                        .header("format", "json")
                        .header("strip_outer_array", "true")
                        .header("ignore_json_size", "true")
                        .build()?
                }
            };
        *be_request.body_mut() = Some(Body::from(data));
        let response = client .execute(be_request)?;

        if response.status().is_success() {
            println!("Stream Load 成功: {}", response.text()?);
        } else {
            println!("Stream Load 失败: {:?}", response);
        }

        Ok(())
    }
}