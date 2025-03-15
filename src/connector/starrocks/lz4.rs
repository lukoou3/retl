use std::cmp::min;
use std::{io, mem};
use std::io::{Read, Write};
use std::sync::Arc;
use bytes::BytesMut;
use log::info;
use lz4_flex::frame::{BlockSize, FrameEncoder, FrameInfo};

pub struct VecBytesMutCompressReader {
    buffers: Arc<Vec<BytesMut>>,    // 持有 Vec<BytesMut> 的引用，作为未压缩数据源
    pos: usize,                     // 当前读取的 buffer 索引，指向 buffers 中的某个 BytesMut
    encoder: FrameEncoder<Vec<u8>>, // LZ4 帧压缩器，持有底层缓冲区
    buf_pos: usize,                 // encoder 中底层缓冲区的读取偏移量
    finished: bool,
    uncompressed_size: usize,
    compressed_size: usize,
}

impl VecBytesMutCompressReader {
    pub fn new(buffers: Arc<Vec<BytesMut>>) -> Self {
        let frame_info = FrameInfo::new()
            .block_size(BlockSize::Max4MB) // 设置块大小为 4MB
            .content_checksum(false)        // 禁用内容校验和
            .block_checksums(false);        // 禁用块校验和

        // 初始化 FrameEncoder，内部包含 Vec<u8> 作为底层缓冲区
        let encoder = FrameEncoder::with_frame_info(frame_info, Vec::new());

        VecBytesMutCompressReader {
            buffers,
            pos: 0,
            encoder,
            buf_pos: 0,
            finished: false,
            uncompressed_size: 0,
            compressed_size: 0,
        }
    }
}

impl Read for VecBytesMutCompressReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut total_read = 0;
        loop {
            let inner = self.encoder.get_mut(); // 获取底层 Vec<u8>，存储压缩数据

            // 如果底层缓冲区有数据可读，直接返回给调用者
            while self.buf_pos < inner.len() {
                let to_read = min(inner.len() - self.buf_pos, buf.len() - total_read);
                buf[total_read..total_read + to_read].copy_from_slice(&inner[self.buf_pos..self.buf_pos + to_read]);
                self.buf_pos += to_read;
                total_read += to_read;
                self.compressed_size += to_read;

                // 只有当缓冲区完全读完时才清空
                if self.buf_pos >= inner.len() {
                    inner.clear();
                    self.buf_pos = 0;
                    break;
                }
                //return Ok(to_read);
                if total_read >= buf.len() {
                    return Ok(total_read);
                }
            }

            // 已经读完
            if self.pos >= self.buffers.len() && self.finished {
                if total_read == 0 {
                    inner.truncate(0);
                    info!("lz4 bytes: {} => {}", self.uncompressed_size, self.compressed_size);
                    //println!("lz4 bytes: {} => {}", self.uncompressed_size, self.compressed_size);
                }
                return Ok(total_read);
            }

            // 缓冲区为空，尝试从输入缓冲区读取数据
            while self.pos < self.buffers.len() {
                let current_buffer = &self.buffers[self.pos];
                self.pos += 1;
                if current_buffer.len() > 0 {
                    self.uncompressed_size += current_buffer.len();
                    self.encoder.write_all(current_buffer)?;
                    if self.encoder.get_mut().len() > 0 {
                        // 如果有压缩数据，退出循环以检查是否有压缩输出
                        break;
                    }
                }
            }

            let inner = self.encoder.get_mut();
            if inner.len() > 0 {
                continue;
            }

            // 如果所有输入缓冲区已耗尽，尝试完成压缩
            if self.pos >= self.buffers.len() {
                self.encoder.try_finish()?; // 完成压缩，保留数据在 encoder 中
                self.finished = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::io::{Read, Write};
    use std::sync::Arc;
    use std::time::Duration;
    use bytes::{BufMut, BytesMut};
    use chrono::{DateTime, Utc};
    use isahc::{HttpClient, ReadResponseExt, Request, RequestExt};
    use isahc::config::RedirectPolicy;
    use isahc::prelude::Configurable;
    use lz4_flex::frame::{BlockSize, FrameEncoder, FrameInfo};
    use rand::Rng;
    use crate::connector::starrocks::lz4::VecBytesMutCompressReader;

    #[test]
    fn test_http() -> crate::Result<(), Box<dyn Error>> {
        let starrocks_host = "http://192.168.216.86:8061";
        let database = "test";
        let table = "object_stat";

        let url = format!("{}/api/{}/{}/_stream_load", starrocks_host, database, table);

        // 构造 JSON 数据
        let json_data = r#"[
        {"timestamp":"2025-03-15 14:19:25","object_id":1},
        {"timestamp":"2025-03-15 14:19:25","object_id":2},
        {"timestamp":"2025-03-15 14:19:25","object_id":2}
        ]"#;

        let client = HttpClient::builder()
            .redirect_policy(RedirectPolicy::Follow)
            .timeout(Duration::from_secs(60))
            .default_headers(HashMap::from([
                ("authorization", "Basic cm9vdDo="),
                ("Expect", "100-continue"),
                ("two_phase_commit", "false"),
                ("format", "json"),
                ("strip_outer_array", "true"),
                ("ignore_json_size", "true"),
                ("compression", "lz4_frame"),
            ]))
            .build()?;

        let mut output = Vec::new();
        let frame_info = FrameInfo::new()
            .block_size(BlockSize::Max1MB) // 匹配 4MB 块大小
            .content_checksum(false)         // 启用内容校验和
            .block_checksums(false);        // 禁用块校验和（根据需要调整）
        // 创建 FrameEncoder 并应用配置
        let mut encoder = FrameEncoder::with_frame_info(frame_info, &mut output);
        //encoder.get_mut();

        // 写入数据
        encoder.write_all(json_data.as_bytes()).unwrap();

        // 完成压缩
        encoder.finish().unwrap();

        let mut response = client.put(url, output)?;
        /*.header("authorization", "Basic cm9vdDo=")
        //.basic_auth(username, Some(password))
        .header("Expect", "100-continue")
        .header("two_phase_commit", "false")
        .header("format", "json")
        .header("strip_outer_array", "true")
        .header("ignore_json_size", "true")
        .send()?;*/

        // 处理响应
        println!("Response Status: {}", response.status());
        let body = response.text()?;
        println!("Response Body: {}", body);

        Ok(())
    }

    #[test]
    fn test_bytes_lz4() {
        println!("start:{}", Utc::now());
        let mut bytes = Vec::new();
        for i in 0..10 {
            let mut buf = BytesMut::new();
            /*for _ in 0..(1024 * 256 * (i + 1)) {
                buf.put_u8(rand::thread_rng().gen_range(0..= 255));
            }*/
            for j in 0..(1024 * 64 * (i + 1)) {
                buf.extend_from_slice(format!("str{}", j % 20).as_bytes());
            }
            bytes.push(buf);
        }
        println!("start:{}", Utc::now());

        let mut output1 = Vec::new();
        let frame_info = FrameInfo::new()
            .block_size(BlockSize::Max4MB)
            .content_checksum(false)
            .block_checksums(false);
        let mut encoder = FrameEncoder::with_frame_info(frame_info, &mut output1);
        let mut input_size = 0;
        for buf in &bytes {
            input_size += buf.len();
            encoder.write_all(buf).unwrap();
        }

        encoder.try_finish().unwrap();
        println!("encoder:{}", Utc::now());
        println!("input_size:{}", input_size);

        let arc_bytes = Arc::new(bytes);
        for len in [1024, 8, 1024 * 1024] {
            let mut output2 = Vec::new();
            let mut reader = VecBytesMutCompressReader::new(arc_bytes.clone());
            let mut chunk = vec![0u8; len]; // 4KB 缓冲区
            loop {
                match reader.read(&mut chunk) {
                    Ok(0) => break, // 读取到末尾
                    Ok(n) => output2.extend_from_slice(&chunk[..n]),
                    Err(e) =>  panic!("{}", e.to_string()),
                }
            }
            println!("reader:{}", Utc::now());
            println!("output1 len:{}, output2 len:{}, eq:{}", output1.len(), output2.len(), output1 == output2);
        }


    }

    #[test]
    fn test_lz4() {
        let mut output = Vec::new();
        let frame_info = FrameInfo::new()
            .block_size(BlockSize::Max4MB) // 匹配 4MB 块大小
            .content_checksum(true)         // 启用内容校验和
            .block_checksums(false);        // 禁用块校验和（根据需要调整）
        // 创建 FrameEncoder 并应用配置
        let mut encoder = FrameEncoder::with_frame_info(frame_info, &mut output);
        //encoder.get_mut();

        // 写入数据
        encoder.write_all(b"Hello, LZ4!").unwrap();

        // 完成压缩
        encoder.finish().unwrap();

        println!("Compressed data: {:?}", output);
    }

}