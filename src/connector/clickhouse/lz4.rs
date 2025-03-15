use std::error::Error;
use anyhow::anyhow;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use cityhash_rs::cityhash_102_128;
use log::info;
use lz4_flex::block;

const MAX_COMPRESSED_SIZE: u32 = 1024 * 1024 * 1024;

// Meta = checksum + header
// - [16b] checksum
// - [ 1b] magic number (0x82)
// - [ 4b] compressed size (data + header)
// - [ 4b] uncompressed size
const LZ4_CHECKSUM_SIZE: usize = 16;
const LZ4_HEADER_SIZE: usize = 9;
const LZ4_META_SIZE: usize = LZ4_CHECKSUM_SIZE + LZ4_HEADER_SIZE;
const LZ4_MAGIC: u8 = 0x82;

struct Lz4Meta {
    checksum: u128,
    compressed_size: u32,
    uncompressed_size: u32,
}

impl Lz4Meta {
    fn total_size(&self) -> usize {
        LZ4_CHECKSUM_SIZE + self.compressed_size as usize
    }

    fn read(mut bytes: &[u8]) -> anyhow::Result<Lz4Meta> {
        let checksum = bytes.get_u128_le();
        let magic = bytes.get_u8();
        let compressed_size = bytes.get_u32_le();
        let uncompressed_size = bytes.get_u32_le();

        if magic != LZ4_MAGIC {
            return Err(anyhow!("incorrect magic number"));
        }

        if compressed_size > MAX_COMPRESSED_SIZE {
            return Err(anyhow!("too big compressed data"));
        }

        Ok(Lz4Meta {
            checksum,
            compressed_size,
            uncompressed_size,
        })
    }

    fn write_checksum(&self, mut buffer: &mut [u8]) {
        buffer.put_u128_le(self.checksum);
    }

    fn write_header(&self, mut buffer: &mut [u8]) {
        buffer.put_u8(LZ4_MAGIC);
        buffer.put_u32_le(self.compressed_size);
        buffer.put_u32_le(self.uncompressed_size);
    }
}

pub fn max_compressed_size(uncompressed_size: usize) -> usize {
    block::get_maximum_output_size(uncompressed_size) + LZ4_META_SIZE
}

fn calc_checksum(buffer: &[u8]) -> u128 {
    let hash = cityhash_102_128(buffer);
    hash.rotate_right(64)
}

pub fn compress(uncompressed: &[u8]) -> anyhow::Result<Bytes> {
    let max_compressed_size = block::get_maximum_output_size(uncompressed.len());

    let mut buffer = BytesMut::new();
    buffer.resize(LZ4_META_SIZE + max_compressed_size, 0);

    let compressed_data_size = block::compress_into(uncompressed, &mut buffer[LZ4_META_SIZE..])?;
    info!("lz4 bytes: {} => {}", uncompressed.len(), LZ4_META_SIZE + compressed_data_size);

    buffer.truncate(LZ4_META_SIZE + compressed_data_size);

    let mut meta = Lz4Meta {
        checksum: 0, // will be calculated below.
        compressed_size: (LZ4_HEADER_SIZE + compressed_data_size) as u32,
        uncompressed_size: uncompressed.len() as u32,
    };

    meta.write_header(&mut buffer[LZ4_CHECKSUM_SIZE..]);
    meta.checksum = calc_checksum(&buffer[LZ4_CHECKSUM_SIZE..]);
    meta.write_checksum(&mut buffer[..]);

    Ok(buffer.freeze())
}

pub fn compress_into(uncompressed: &[u8], buffer: &mut [u8]) -> anyhow::Result<usize> {
    let compressed_data_size = block::compress_into(uncompressed, &mut buffer[LZ4_META_SIZE..])?;
    let compressed_size = LZ4_META_SIZE + compressed_data_size;
    info!("lz4 bytes: {} => {}", uncompressed.len(), LZ4_META_SIZE + compressed_data_size);

    let mut meta = Lz4Meta {
        checksum: 0, // will be calculated below.
        compressed_size: (LZ4_HEADER_SIZE + compressed_data_size) as u32,
        uncompressed_size: uncompressed.len() as u32,
    };

    meta.write_header(&mut buffer[LZ4_CHECKSUM_SIZE..]);
    meta.checksum = calc_checksum(&buffer[LZ4_CHECKSUM_SIZE..compressed_size]);
    meta.write_checksum(&mut buffer[..]);

    Ok(compressed_size)
}