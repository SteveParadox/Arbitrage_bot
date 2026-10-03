use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::ExecutionError;

type HmacSha256 = Hmac<Sha256>;

pub(crate) fn sign_hmac_sha256(
    secret: &str,
    timestamp_ms: u64,
    api_key: &str,
    recv_window_ms: u64,
    payload: &str,
) -> Result<String, ExecutionError> {
    let message = format!("{timestamp_ms}{api_key}{recv_window_ms}{payload}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| ExecutionError::Authentication(error.to_string()))?;
    mac.update(message.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_signature_is_deterministic() {
        let first = sign_hmac_sha256("secret", 123, "key", 5000, "category=spot").unwrap();
        let second = sign_hmac_sha256("secret", 123, "key", 5000, "category=spot").unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }
}
