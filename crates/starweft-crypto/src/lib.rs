use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ed25519_dalek::{Signature as DalekSignature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use starweft_id::KeyId;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("invalid secret key bytes")]
    InvalidSecretKey,
    #[error("invalid public key bytes")]
    InvalidPublicKey,
    #[error("invalid signature bytes")]
    InvalidSignature,
    #[error("signature verification failed")]
    SignatureVerificationFailed,
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("io failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageSignature {
    pub alg: String,
    pub key_id: KeyId,
    pub sig: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredKeypair {
    pub key_id: KeyId,
    pub created_at: OffsetDateTime,
    pub secret_key: String,
    pub public_key: String,
}

impl StoredKeypair {
    #[must_use]
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        Self {
            key_id: KeyId::generate(),
            created_at: OffsetDateTime::now_utc(),
            secret_key: STANDARD.encode(signing_key.to_bytes()),
            public_key: STANDARD.encode(verifying_key.to_bytes()),
        }
    }

    pub fn signing_key(&self) -> Result<SigningKey, CryptoError> {
        let bytes = decode_32_bytes(&self.secret_key).ok_or(CryptoError::InvalidSecretKey)?;
        Ok(SigningKey::from_bytes(&bytes))
    }

    pub fn verifying_key(&self) -> Result<VerifyingKey, CryptoError> {
        let bytes = decode_32_bytes(&self.public_key).ok_or(CryptoError::InvalidPublicKey)?;
        VerifyingKey::from_bytes(&bytes).map_err(|_| CryptoError::InvalidPublicKey)
    }

    pub fn secret_key_bytes(&self) -> Result<[u8; 32], CryptoError> {
        decode_32_bytes(&self.secret_key).ok_or(CryptoError::InvalidSecretKey)
    }

    pub fn sign_bytes(&self, payload: &[u8]) -> Result<MessageSignature, CryptoError> {
        let signing_key = self.signing_key()?;
        let signature = signing_key.sign(payload);
        Ok(MessageSignature {
            alg: "ed25519".to_owned(),
            key_id: self.key_id.clone(),
            sig: STANDARD.encode(signature.to_bytes()),
        })
    }

    pub fn sign_json<T: Serialize>(&self, payload: &T) -> Result<MessageSignature, CryptoError> {
        self.sign_bytes(&canonical_json(payload)?)
    }

    pub fn write_to_path(&self, path: &std::path::Path) -> Result<(), CryptoError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(self)?)?;
        set_private_permissions(path)?;
        Ok(())
    }

    pub fn read_from_path(path: &std::path::Path) -> Result<Self, CryptoError> {
        let bytes = std::fs::read(path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

pub fn canonical_json<T: Serialize>(payload: &T) -> Result<Vec<u8>, CryptoError> {
    let value = serde_json::to_value(payload)?;
    Ok(serde_json::to_vec(&value)?)
}

pub fn verifying_key_from_base64(encoded: &str) -> Result<VerifyingKey, CryptoError> {
    let bytes = decode_32_bytes(encoded).ok_or(CryptoError::InvalidPublicKey)?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| CryptoError::InvalidPublicKey)
}

pub fn verify_json<T: Serialize>(
    verifying_key: &VerifyingKey,
    payload: &T,
    signature: &MessageSignature,
) -> Result<(), CryptoError> {
    verify_bytes(verifying_key, &canonical_json(payload)?, signature)
}

pub fn verify_bytes(
    verifying_key: &VerifyingKey,
    payload: &[u8],
    signature: &MessageSignature,
) -> Result<(), CryptoError> {
    let signature_bytes = STANDARD
        .decode(signature.sig.as_bytes())
        .map_err(|_| CryptoError::InvalidSignature)?;
    let signature = DalekSignature::try_from(signature_bytes.as_slice())
        .map_err(|_| CryptoError::InvalidSignature)?;

    verifying_key
        .verify(payload, &signature)
        .map_err(|_| CryptoError::SignatureVerificationFailed)
}

pub fn read_json_file<T: DeserializeOwned>(path: &std::path::Path) -> Result<T, CryptoError> {
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn decode_32_bytes(encoded: &str) -> Option<[u8; 32]> {
    let bytes = STANDARD.decode(encoded.as_bytes()).ok()?;
    bytes.try_into().ok()
}

#[cfg(unix)]
fn set_private_permissions(path: &std::path::Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;

    let permissions = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(path, permissions)
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &std::path::Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_sign_and_verify_json() {
        let keypair = StoredKeypair::generate();
        let signature = keypair
            .sign_json(&serde_json::json!({ "message": "hello" }))
            .expect("signature");

        let verifying_key = keypair.verifying_key().expect("verifying key");
        verify_json(
            &verifying_key,
            &serde_json::json!({ "message": "hello" }),
            &signature,
        )
        .expect("verify");
    }
}
