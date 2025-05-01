use crypto::aes::{cbc_encryptor, cbc_decryptor, KeySize};
use crypto::blockmodes::PkcsPadding;
use crypto::buffer::{BufferResult, ReadBuffer, WriteBuffer};

use crate::Result;

pub fn aes_encrypt(input: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>> {
    let mut encryptor = cbc_encryptor(KeySize::KeySize128, key, iv, PkcsPadding);

    let mut final_result = Vec::<u8>::new();
    let mut read_buffer = crypto::buffer::RefReadBuffer::new(input);
    let mut buffer = [0; 4096];
    let mut write_buffer = crypto::buffer::RefWriteBuffer::new(&mut buffer);

    loop {
        let result = encryptor
            .encrypt(&mut read_buffer, &mut write_buffer, true)
            .map_err(|e| format!("Symmetric cipher error: {:?}", e))?;
        final_result.extend(write_buffer.take_read_buffer().take_remaining().iter().copied());

        match result {
            BufferResult::BufferUnderflow => break,
            BufferResult::BufferOverflow => continue,
        }
    }

    //Ok(STANDARD.encode(&final_result))
    Ok(final_result)
}

pub fn aes_decrypt(input: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>> {
    let mut decryptor = cbc_decryptor(KeySize::KeySize128, key, iv, PkcsPadding);

    let mut final_result = Vec::<u8>::new();
    let mut read_buffer = crypto::buffer::RefReadBuffer::new(input);
    let mut buffer = [0; 4096];
    let mut write_buffer = crypto::buffer::RefWriteBuffer::new(&mut buffer);

    loop {
        let result = decryptor
            .decrypt(&mut read_buffer, &mut write_buffer, true)
            .map_err(|e| format!("Symmetric cipher error: {:?}", e))?;
        final_result.extend(write_buffer.take_read_buffer().take_remaining().iter().copied());

        match result {
            BufferResult::BufferUnderflow => break,
            BufferResult::BufferOverflow => continue,
        }
    }

    Ok(final_result)
}