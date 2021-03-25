use crypto::{ buffer, aes, blockmodes };
use crypto::symmetriccipher::SymmetricCipherError;
use crypto::buffer::{ ReadBuffer, WriteBuffer };
use crypto::digest::Digest;
use crypto::sha3::Sha3;
use rand_chacha::ChaCha20Rng;
use rand_core::{RngCore, SeedableRng};

static mut COUNTER :  u64 = 0;

pub fn gen_key()->[u8; 16]{
    unsafe{
        // Using unsafe global counter for now (so runs the same every time)
        // but would use safe from_entropy() once ready for production
        let mut rng = ChaCha20Rng::seed_from_u64(COUNTER);
        COUNTER = COUNTER + 1;
        let mut key = [0u8; 16];
        rng.fill_bytes(&mut key);
        key
    }
}

pub fn prf(plaintext_bytes : &[u8], key : &[u8]) -> [u8; 32] {
    let mut hash_func = Sha3::sha3_256();
    let ciphertext = fixed_encrypt(plaintext_bytes, key);
    hash_func.input(&ciphertext[..]);
    
    let output = hash_func.result_str();
    let mut result = [0u8; 32];
    result.copy_from_slice(&output.as_bytes()[0..32]);
    result
}

pub fn fixed_encrypt(plaintext_bytes : &[u8], key : &[u8]) -> Vec<u8>{
    let iv = &[0u8; 16][..];
    let mut encryptor = aes::cbc_encryptor(aes::KeySize::KeySize128,
        key,
        &iv,
        blockmodes::PkcsPadding);
    
    let mut final_result = Vec::<u8>::new();
    let mut read_buffer = buffer::RefReadBuffer::new(plaintext_bytes);
    let mut buffer = [0; 4096];
    let mut write_buffer = buffer::RefWriteBuffer::new(&mut buffer);
    
    loop {
        let result = encryptor.encrypt(&mut read_buffer, &mut write_buffer, true);

        final_result.extend(write_buffer.take_read_buffer().take_remaining().iter().map(|&i| i));

        match result {
            Ok(_) => break,
            Err(e) => {
                match e {
                    SymmetricCipherError::InvalidLength => println!("Invalid length"),
                    SymmetricCipherError::InvalidPadding => println!("Invalid padding"),
                }
                break;
            }
        }
    }
    
    final_result
}

pub fn fixed_decrypt(ciphertext_bytes: &[u8], key: &[u8]) -> Vec<u8> {
    let iv = &[0u8; 16][..];
    let mut decryptor = aes::cbc_decryptor(
            aes::KeySize::KeySize128,
            key,
            &iv,
            blockmodes::PkcsPadding);

    let mut final_result = Vec::<u8>::new();
    let mut read_buffer = buffer::RefReadBuffer::new(ciphertext_bytes);
    let mut buffer = [0; 4096];
    let mut write_buffer = buffer::RefWriteBuffer::new(&mut buffer);

    loop {
        let result = decryptor.decrypt(&mut read_buffer, &mut write_buffer, true);
        final_result.extend(write_buffer.take_read_buffer().take_remaining().iter().map(|&i| i));
        match result {
            Ok(_) => break,
            Err(e) => {
                match e {
                    SymmetricCipherError::InvalidLength => println!("Invalid length"),
                    SymmetricCipherError::InvalidPadding => println!("Invalid padding"),
                }
                break;
            }
        }
    }

    final_result
}