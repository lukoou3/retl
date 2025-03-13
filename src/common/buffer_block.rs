use std::cmp::min;
use std::{io, mem};
use std::io::Read;
use bytes::BytesMut;
use crate::buffer_pool::BufferPool;

pub struct BufferBlock {
    buffer_pool: BufferPool,
    buffers: Vec<BytesMut>,
    buffer_size: usize,
    buffer_index: usize,
    read_pos: usize,                // 当前读取的 buffer 索引
    read_offset: usize,             // 当前 buffer 的读取偏移量
}

impl BufferBlock {
    pub fn new(buffer_pool: BufferPool, buffer_size: usize) -> Self {
        let mut buffers = Vec::new();
        //let buffer = buffer_pool.acquire(buffer_size);
        //buffers.push(buffer);
        BufferBlock {
            buffer_pool,
            buffers,
            buffer_size,
            buffer_index: 0,
            read_pos: 0,
            read_offset: 0,
        }
    }

    pub fn read_reset(&mut self) {
        self.read_pos = 0;
        self.read_offset = 0;
    }

    pub fn release_buffer(&mut self) {
        let buffers = mem::replace(&mut self.buffers, Vec::new());
        for buffer in buffers {
            self.buffer_pool.release(buffer);
        }
    }

    #[inline]
    pub fn put_i8(&mut self, n: i8) {
        let src = [n as u8];
        self.extend_from_slice(&src)
    }

    #[inline]
    pub fn put_u8(&mut self, n: u8) {
        let src = [n];
        self.extend_from_slice(&src);
    }

    #[inline]
    pub fn put_i16_le(&mut self, n: i16) {
        self.extend_from_slice(&n.to_le_bytes())
    }

    #[inline]
    pub fn put_u16_le(&mut self, n: u16) {
        self.extend_from_slice(&n.to_le_bytes())
    }

    #[inline]
    pub fn put_i32_le(&mut self, n: i32) {
        self.extend_from_slice(&n.to_le_bytes())
    }

    #[inline]
    pub fn put_u32_le(&mut self, n: u32) {
        self.extend_from_slice(&n.to_le_bytes())
    }

    #[inline]
    pub fn put_i64_le(&mut self, n: i64) {
        self.extend_from_slice(&n.to_le_bytes())
    }

    #[inline]
    pub fn put_u64_le(&mut self, n: u64) {
        self.extend_from_slice(&n.to_le_bytes())
    }

    #[inline]
    pub fn put_f32_le(&mut self, n: f32) {
        self.put_u32_le(n.to_bits());
    }

    #[inline]
    pub fn put_f64_le(&mut self, n: f64) {
        self.put_u64_le(n.to_bits());
    }

    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.ensure_buffers_not_empty();
        let mut offset = 0;
        let mut length = bytes.len();
        while length > 0 {
            let buffer = &mut self.buffers[self.buffer_index];
            let remaining = buffer.capacity() - buffer.len(); // buffer.capacity() - buffer.len();
            if remaining < length {
                buffer.extend_from_slice(&bytes[offset..offset + remaining]);

                let buffer = self.buffer_pool.acquire(self.buffer_size);
                self.buffers.push(buffer);
                self.buffer_index += 1;

                offset += remaining;
                length -= remaining;
            } else {
                buffer.extend_from_slice(&bytes[offset..offset + length]);
                break;
            }
        }
    }

    #[inline]
    fn ensure_buffers_not_empty(&mut self) {
        if self.buffer_index == 0 && self.buffers.len() == 0 {
            self.buffers.push(self.buffer_pool.acquire(self.buffer_size));
        }
    }
}



impl Read for BufferBlock {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.read_pos >= self.buffers.len() {
            return Ok(0); // 已经读取到末尾
        }

        let mut total_bytes_read = 0;

        while total_bytes_read < buf.len() && self.read_pos < self.buffers.len() {
            let current_buffer = &self.buffers[self.read_pos];
            let remaining = current_buffer.len() - self.read_offset;

            if remaining > 0 {
                // 计算当前这次能读取多少字节
                let bytes_to_read = min(remaining, buf.len() - total_bytes_read);

                // 复制数据到输出缓冲区
                buf[total_bytes_read..total_bytes_read + bytes_to_read]
                    .copy_from_slice(&current_buffer[self.read_offset..self.read_offset + bytes_to_read]);

                // 更新位置
                self.read_offset += bytes_to_read;
                total_bytes_read += bytes_to_read;
            }

            // 如果当前 buffer 读完，移动到下一个
            if self.read_offset >= current_buffer.len() {
                self.read_pos += 1;
                self.read_offset = 0;
            }
        }

        Ok(total_bytes_read)
    }
}