//! Signing-key authentication and request signing.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::sync::LazyLock;

static PREFIX_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^signkey-\w+-").unwrap());

/// Strip the `signkey-<env>-` prefix.
pub fn normalize_key(key: &str) -> &str {
    match PREFIX_RE.find(key) {
        Some(m) => &key[m.end()..],
        None => key,
    }
}

/// hex(sha256(hex_decode(key))) — the form SDKs send as a Bearer token.
pub fn hashed_key(key: &str) -> Option<String> {
    let raw = hex::decode(normalize_key(key)).ok()?;
    Some(crate::util::sha256_hex(&raw))
}

/// Precomputed signing key material.
#[derive(Clone, Debug)]
pub struct SigningKey {
    /// Normalized (prefix-stripped) key.
    pub key: String,
    pub hashed: Option<String>,
}

impl SigningKey {
    pub fn new(key: &str) -> SigningKey {
        SigningKey {
            key: normalize_key(key).to_string(),
            hashed: hashed_key(key),
        }
    }

    /// Validate an `Authorization` header value.
    pub fn check_bearer(&self, header: Option<&str>) -> bool {
        let Some(h) = header else { return false };
        if h.len() <= 7 || !h[..6].eq_ignore_ascii_case("bearer") {
            return false;
        }
        let token = normalize_key(h[7..].trim());
        if constant_eq(token.as_bytes(), self.key.as_bytes()) {
            return true;
        }
        match &self.hashed {
            Some(hashed) => constant_eq(token.as_bytes(), hashed.as_bytes()),
            None => false,
        }
    }

    /// Produce an `X-Inngest-Signature` header value for `body` at unix time `t`.
    pub fn sign(&self, body: &[u8], t: i64) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.key.as_bytes()).expect("hmac key");
        mac.update(body);
        mac.update(t.to_string().as_bytes());
        format!("t={}&s={}", t, hex::encode(mac.finalize().into_bytes()))
    }
}

fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer() {
        let k = SigningKey::new("signkey-prod-deadbeef");
        assert!(k.check_bearer(Some("Bearer deadbeef")));
        assert!(k.check_bearer(Some("Bearer signkey-prod-deadbeef")));
        let hashed = hashed_key("deadbeef").unwrap();
        assert!(k.check_bearer(Some(&format!("Bearer signkey-prod-{hashed}"))));
        assert!(k.check_bearer(Some(&format!("bearer {hashed}"))));
        assert!(!k.check_bearer(Some("Bearer nope")));
        assert!(!k.check_bearer(None));
    }

    #[test]
    fn signature_format() {
        let k = SigningKey::new("abcd");
        let s = k.sign(b"{}", 1700000000);
        assert!(s.starts_with("t=1700000000&s="));
        assert_eq!(s.len(), "t=1700000000&s=".len() + 64);
    }
}
