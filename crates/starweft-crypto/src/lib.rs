//! Cryptographic primitives for the Starweft protocol.
//!
//! Provides Ed25519 key generation, message signing, signature verification,
//! and deterministic canonical JSON serialization for envelope integrity.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ed25519_dalek::{Signature as DalekSignature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use starweft_id::KeyId;
use thiserror::Error;
use time::OffsetDateTime;
use zeroize::Zeroize;

/// Errors that can occur during cryptographic operations.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// The secret key bytes could not be decoded or are invalid.
    #[error("invalid secret key bytes")]
    InvalidSecretKey,
    /// The public key bytes could not be decoded or are invalid.
    #[error("invalid public key bytes")]
    InvalidPublicKey,
    /// The signature bytes could not be decoded or are malformed.
    #[error("invalid signature bytes")]
    InvalidSignature,
    /// The signature did not match the payload and public key.
    #[error("signature verification failed")]
    SignatureVerificationFailed,
    /// JSON serialization or deserialization failed.
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// An I/O operation failed (e.g. reading/writing key files).
    #[error("io failed: {0}")]
    Io(#[from] std::io::Error),
}

/// A detached Ed25519 signature with algorithm and key metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageSignature {
    /// Signature algorithm identifier (always `"ed25519"`).
    pub alg: String,
    /// Identifier of the key that produced this signature.
    pub key_id: KeyId,
    /// Base64-encoded signature bytes.
    pub sig: String,
}

/// An Ed25519 keypair stored as base64-encoded strings with metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredKeypair {
    /// Unique identifier for this keypair.
    pub key_id: KeyId,
    /// Timestamp when the keypair was generated.
    pub created_at: OffsetDateTime,
    /// Base64-encoded Ed25519 secret key (32 bytes).
    pub secret_key: String,
    /// Base64-encoded Ed25519 public key (32 bytes).
    pub public_key: String,
}

impl StoredKeypair {
    /// Generates a new random Ed25519 keypair.
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

    /// Decodes and returns the Ed25519 signing key.
    pub fn signing_key(&self) -> Result<SigningKey, CryptoError> {
        let mut bytes = decode_32_bytes(&self.secret_key).ok_or(CryptoError::InvalidSecretKey)?;
        let key = SigningKey::from_bytes(&bytes);
        bytes.zeroize();
        Ok(key)
    }

    /// Decodes and returns the Ed25519 verifying (public) key.
    pub fn verifying_key(&self) -> Result<VerifyingKey, CryptoError> {
        let bytes = decode_32_bytes(&self.public_key).ok_or(CryptoError::InvalidPublicKey)?;
        VerifyingKey::from_bytes(&bytes).map_err(|_| CryptoError::InvalidPublicKey)
    }

    /// Returns the raw 32-byte secret key.
    ///
    /// 呼び出し側で使用後に bytes.zeroize() を呼ぶ責務があります
    pub fn secret_key_bytes(&self) -> Result<[u8; 32], CryptoError> {
        decode_32_bytes(&self.secret_key).ok_or(CryptoError::InvalidSecretKey)
    }

    /// Signs raw bytes and returns a detached [`MessageSignature`].
    pub fn sign_bytes(&self, payload: &[u8]) -> Result<MessageSignature, CryptoError> {
        let signing_key = self.signing_key()?;
        let signature = signing_key.sign(payload);
        Ok(MessageSignature {
            alg: "ed25519".to_owned(),
            key_id: self.key_id.clone(),
            sig: STANDARD.encode(signature.to_bytes()),
        })
    }

    /// Serializes `payload` to canonical JSON, then signs the bytes.
    pub fn sign_json<T: Serialize>(&self, payload: &T) -> Result<MessageSignature, CryptoError> {
        self.sign_bytes(&canonical_json(payload)?)
    }

    /// Writes the keypair to a JSON file with restrictive permissions (0600 on Unix).
    pub fn write_to_path(&self, path: &std::path::Path) -> Result<(), CryptoError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Clear read-only before overwrite (Windows sets read-only for protection).
        // On Unix, set_private_permissions uses mode 0o600, not readonly, so this
        // branch only activates on Windows where readonly was previously set.
        #[allow(clippy::permissions_set_readonly_false)]
        if path.exists() {
            let mut perms = std::fs::metadata(path)?.permissions();
            if perms.readonly() {
                perms.set_readonly(false);
                std::fs::set_permissions(path, perms)?;
            }
        }
        std::fs::write(path, serde_json::to_vec_pretty(self)?)?;
        set_private_permissions(path)?;
        Ok(())
    }

    /// Reads a keypair from a JSON file at the given path.
    pub fn read_from_path(path: &std::path::Path) -> Result<Self, CryptoError> {
        let bytes = std::fs::read(path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

/// Serializes a value to deterministic canonical JSON bytes.
///
/// Keys are sorted recursively at every level to produce identical byte
/// output regardless of field insertion order or `serde_json` feature flags.
pub fn canonical_json<T: Serialize>(payload: &T) -> Result<Vec<u8>, CryptoError> {
    let value = sort_json_keys_recursive(serde_json::to_value(payload)?);
    Ok(serde_json::to_vec(&value)?)
}

/// Recursively sorts object keys. `serde_json::Map` is backed by `BTreeMap`
/// by default (keys already sorted), but we recurse into nested values to
/// guarantee canonical output even if `preserve_order` is ever enabled.
fn sort_json_keys_recursive(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k, sort_json_keys_recursive(v)))
                .collect();
            serde_json::Value::Object(sorted)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(sort_json_keys_recursive).collect())
        }
        other => other,
    }
}

/// Decodes a base64-encoded Ed25519 public key into a [`VerifyingKey`].
pub fn verifying_key_from_base64(encoded: &str) -> Result<VerifyingKey, CryptoError> {
    let bytes = decode_32_bytes(encoded).ok_or(CryptoError::InvalidPublicKey)?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| CryptoError::InvalidPublicKey)
}

/// Verifies a signature against canonical JSON of the payload.
pub fn verify_json<T: Serialize>(
    verifying_key: &VerifyingKey,
    payload: &T,
    signature: &MessageSignature,
) -> Result<(), CryptoError> {
    verify_bytes(verifying_key, &canonical_json(payload)?, signature)
}

/// Verifies a signature against raw payload bytes.
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

/// Reads and deserializes a JSON file from the given path.
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
fn set_private_permissions(path: &std::path::Path) -> Result<(), std::io::Error> {
    // Mark private key files as read-only on Windows to prevent accidental
    // modification. Directory-level ACL protection is applied by the
    // application's config layer (see config.rs ensure_layout).
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(path, perms)
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

    #[test]
    fn canonical_json_sorts_nested_keys() {
        // Build JSON with known key order via serde_json::json!
        let input = serde_json::json!({
            "z": 1,
            "a": { "c": 3, "b": 2 },
            "m": [{ "y": 4, "x": 5 }]
        });
        let bytes = canonical_json(&input).expect("canonical");
        // Keys must be alphabetically sorted at every level
        let expected = r#"{"a":{"b":2,"c":3},"m":[{"x":5,"y":4}],"z":1}"#;
        assert_eq!(String::from_utf8(bytes).expect("utf8"), expected);
    }
}
