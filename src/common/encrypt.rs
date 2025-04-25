use crypto::aes::{cbc_encryptor, cbc_decryptor, KeySize};
use crypto::blockmodes::PkcsPadding;
use crypto::buffer::{BufferResult, ReadBuffer, WriteBuffer};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;

use crate::Result;

pub fn aes_encrypt(plaintext: &str, key: &[u8; 16], iv: &[u8; 16]) -> Result<String> {
    let mut encryptor = cbc_encryptor(KeySize::KeySize128, key, iv, PkcsPadding);

    let mut final_result = Vec::<u8>::new();
    let mut read_buffer = crypto::buffer::RefReadBuffer::new(plaintext.as_bytes());
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

    Ok(STANDARD.encode(&final_result))
}

pub fn aes_decrypt(ciphertext: &str, key: &[u8; 16], iv: &[u8; 16]) -> Result<String> {
    let ciphertext_bytes = STANDARD
        .decode(ciphertext)
        .map_err(|e| format!("Base64 decode error: {}", e))?;

    let mut decryptor = cbc_decryptor(KeySize::KeySize128, key, iv, PkcsPadding);

    let mut final_result = Vec::<u8>::new();
    let mut read_buffer = crypto::buffer::RefReadBuffer::new(&ciphertext_bytes);
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

    Ok(String::from_utf8(final_result)
        .map_err(|e| format!("UTF-8 decode error: {}", e))?)
}