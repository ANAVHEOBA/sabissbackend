use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use data_encoding::BASE32_NOPAD;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng as DalekOsRng;
use thiserror::Error;

use crate::config::environment::Environment;

const STELLAR_ACCOUNT_ID_VERSION_BYTE: u8 = 6 << 3;
const STELLAR_SECRET_SEED_VERSION_BYTE: u8 = 18 << 3;

#[derive(Debug, Error)]
pub enum WalletCryptoError {
    #[error("AA owner encryption key must be a 32-byte hex string")]
    InvalidEncryptionKey,
    #[error("wallet encryption failed")]
    EncryptFailed,
    #[error("wallet decryption failed")]
    DecryptFailed,
}

#[derive(Debug, Clone)]
pub struct ManagedOwnerKeyMaterial {
    pub owner_address: String,
    pub owner_public_key_hex: String,
    pub encrypted_private_key: String,
    pub encryption_nonce: String,
    pub key_version: i32,
}

pub fn create_managed_owner_key(
    env: &Environment,
) -> Result<ManagedOwnerKeyMaterial, WalletCryptoError> {
    let mut rng = DalekOsRng;
    let mut secret_key = [0_u8; 32];
    rng.fill_bytes(&mut secret_key);
    let signing_key = SigningKey::from_bytes(&secret_key);
    let secret_key = signing_key.to_bytes();
    let public_key = signing_key.verifying_key().to_bytes();
    let encrypted = encrypt_private_key(env, secret_key.as_slice())?;

    Ok(ManagedOwnerKeyMaterial {
        owner_address: encode_stellar_public_key(&public_key),
        owner_public_key_hex: hex::encode(public_key),
        encrypted_private_key: encrypted.ciphertext,
        encryption_nonce: encrypted.nonce,
        key_version: env.aa_owner_encryption_key_version,
    })
}

pub fn decrypt_private_key(
    env: &Environment,
    ciphertext_b64: &str,
    nonce_b64: &str,
) -> Result<Vec<u8>, WalletCryptoError> {
    let cipher = build_cipher(env)?;
    let nonce_bytes = STANDARD
        .decode(nonce_b64)
        .map_err(|_| WalletCryptoError::DecryptFailed)?;
    let ciphertext = STANDARD
        .decode(ciphertext_b64)
        .map_err(|_| WalletCryptoError::DecryptFailed)?;

    cipher
        .decrypt(Nonce::from_slice(&nonce_bytes), ciphertext.as_ref())
        .map_err(|_| WalletCryptoError::DecryptFailed)
}

pub fn encode_stellar_secret_key(private_key: &[u8; 32]) -> String {
    let mut payload = [0_u8; 33];
    payload[0] = STELLAR_SECRET_SEED_VERSION_BYTE;
    payload[1..].copy_from_slice(private_key);

    let checksum = crc16_xmodem(&payload).to_le_bytes();
    let mut encoded = [0_u8; 35];
    encoded[..33].copy_from_slice(&payload);
    encoded[33] = checksum[0];
    encoded[34] = checksum[1];

    BASE32_NOPAD.encode(&encoded)
}

struct EncryptedWalletKey {
    ciphertext: String,
    nonce: String,
}

fn encrypt_private_key(
    env: &Environment,
    private_key_bytes: &[u8],
) -> Result<EncryptedWalletKey, WalletCryptoError> {
    let cipher = build_cipher(env)?;
    let mut nonce_bytes = [0_u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), private_key_bytes)
        .map_err(|_| WalletCryptoError::EncryptFailed)?;

    Ok(EncryptedWalletKey {
        ciphertext: STANDARD.encode(ciphertext),
        nonce: STANDARD.encode(nonce_bytes),
    })
}

fn build_cipher(env: &Environment) -> Result<Aes256Gcm, WalletCryptoError> {
    let key_bytes = hex::decode(&env.aa_owner_encryption_key)
        .map_err(|_| WalletCryptoError::InvalidEncryptionKey)?;

    if key_bytes.len() != 32 {
        return Err(WalletCryptoError::InvalidEncryptionKey);
    }

    Aes256Gcm::new_from_slice(&key_bytes).map_err(|_| WalletCryptoError::InvalidEncryptionKey)
}

fn encode_stellar_public_key(public_key: &[u8; 32]) -> String {
    let mut payload = [0_u8; 33];
    payload[0] = STELLAR_ACCOUNT_ID_VERSION_BYTE;
    payload[1..].copy_from_slice(public_key);

    let checksum = crc16_xmodem(&payload).to_le_bytes();
    let mut encoded = [0_u8; 35];
    encoded[..33].copy_from_slice(&payload);
    encoded[33] = checksum[0];
    encoded[34] = checksum[1];

    BASE32_NOPAD.encode(&encoded)
}

fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc = 0u16;

    for byte in data {
        crc ^= u16::from(*byte) << 8;

        for _ in 0..8 {
            crc = if (crc & 0x8000) != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }

    crc
}
