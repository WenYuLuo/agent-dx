//! Existing account-database credential protection; unrelated to Binding initialization.
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};

/// AES-256-GCM envelope; associated data binds ciphertext to its purpose and owner.
pub struct CredentialCipher(aead::LessSafeKey);
impl CredentialCipher {
    pub fn new(key: [u8; 32]) -> Self {
        // AES_256_GCM accepts exactly 32 bytes; the array type enforces this invariant.
        let key = aead::UnboundKey::new(&aead::AES_256_GCM, &key).expect("32-byte AES key");
        Self(aead::LessSafeKey::new(key))
    }
    pub fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, &'static str> {
        let mut nonce = [0; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| "credential randomness unavailable")?;
        let mut encrypted = plaintext.to_vec();
        self.0
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(aad),
                &mut encrypted,
            )
            .map_err(|_| "credential encryption failed")?;
        let mut result = nonce.to_vec();
        result.extend(encrypted);
        Ok(result)
    }
    pub fn open(&self, aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, &'static str> {
        if ciphertext.len() < 28 {
            return Err("invalid encrypted credential");
        }
        let nonce: [u8; 12] = ciphertext[..12].try_into().map_err(|_| "invalid nonce")?;
        let mut encrypted = ciphertext[12..].to_vec();
        self.0
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(aad),
                &mut encrypted,
            )
            .map(|v| v.to_vec())
            .map_err(|_| "credential decryption failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credentials_are_bound_to_owner_and_reject_tampering() {
        let cipher = CredentialCipher::new([9; 32]);
        let sealed = cipher.seal(b"alice-binding", b"sk-private").unwrap();
        assert_eq!(
            cipher.open(b"alice-binding", &sealed).unwrap(),
            b"sk-private"
        );
        assert!(cipher.open(b"bob-binding", &sealed).is_err());
        assert!(CredentialCipher::new([8; 32])
            .open(b"alice-binding", &sealed)
            .is_err());
        let mut damaged = sealed;
        damaged[12] ^= 1;
        assert!(cipher.open(b"alice-binding", &damaged).is_err());
    }
}
