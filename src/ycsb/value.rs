//! The record values of the benchmark: either the 8-byte value stored inline in the page, or YCSB's default 1 KB record
//! (10 fields of 100 bytes) behind a pointer. The pointer is a `triomphe::Arc`, i.e., an `Arc` without weak counter:
//! the page slot holds 8 bytes either way (so the page layouts of both variants are identical), and the copies the
//! trees make (reorganizations, scan results) are one atomic increment, not a 1 KB copy.

use std::fmt::{Display, Formatter};
use std::sync::OnceLock;

use triomphe::Arc;

pub const RECORD_BYTES: usize = 1024;

/// What the trees store as payload, and how the benchmark builds and verifies it.
pub trait Value: Clone + Default + Display + Send + Sync + 'static {
    /// Size of a record in bytes, as reported.
    const BYTES: usize;
    /// A fresh record for `key` (the benchmark's insert and update).
    fn record(key: u64) -> Self;
    /// A record carrying `tag`, for tests that need to tell updates apart. `check` still accepts it for blobs.
    fn tagged(key: u64, tag: u64) -> Self;
    fn tag(&self) -> u64;
    /// Verifies a record the benchmark wrote for `key`.
    fn check(&self, key: u64) -> bool;
}

/// The 8-byte value is stored inline; `record(key)` is the key itself.
impl Value for u64 {
    const BYTES: usize = 8;

    #[inline(always)]
    fn record(key: u64) -> Self { key }
    #[inline(always)]
    fn tagged(_key: u64, tag: u64) -> Self { tag }
    #[inline(always)]
    fn tag(&self) -> u64 { *self }
    #[inline(always)]
    fn check(&self, key: u64) -> bool { *self == key }
}

/// A 1 KB record: bytes 0..8 hold the key, 8..16 a tag (a per-record counter in the benchmark), the rest a pattern
/// derived from the key, so a record that was mixed up or torn is detected.
#[derive(Clone)]
pub struct Blob(Arc<[u8; RECORD_BYTES]>);

impl Blob {
    fn word(key: u64, index: usize) -> u64 {
        key.rotate_left((index % 61) as u32) ^ 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(index as u64 + 1)
    }

    fn build(key: u64, tag: u64) -> Self {
        let mut bytes = [0u8; RECORD_BYTES];
        bytes[..8].copy_from_slice(&key.to_le_bytes());
        bytes[8..16].copy_from_slice(&tag.to_le_bytes());
        for (i, chunk) in bytes[16..].chunks_exact_mut(8).enumerate() {
            chunk.copy_from_slice(&Self::word(key, i).to_le_bytes());
        }
        Blob(Arc::new(bytes))
    }
}

thread_local! {
    static NEXT_TAG: std::cell::Cell<u64> = const { std::cell::Cell::new(1) };
}

impl Value for Blob {
    const BYTES: usize = RECORD_BYTES;

    fn record(key: u64) -> Self {
        let tag = NEXT_TAG.with(|t| { let v = t.get(); t.set(v + 1); v });
        Self::build(key, tag)
    }

    fn tagged(key: u64, tag: u64) -> Self {
        Self::build(key, tag)
    }

    fn tag(&self) -> u64 {
        u64::from_le_bytes(self.0[8..16].try_into().unwrap())
    }

    fn check(&self, key: u64) -> bool {
        self.0[..8] == key.to_le_bytes()
            && self.0[16..].chunks_exact(8).enumerate()
            .all(|(i, chunk)| chunk == Self::word(key, i).to_le_bytes())
    }
}

/// The trees create placeholder payloads; they all share one empty record.
impl Default for Blob {
    fn default() -> Self {
        static EMPTY: OnceLock<Blob> = OnceLock::new();
        EMPTY.get_or_init(|| Blob(Arc::new([0u8; RECORD_BYTES]))).clone()
    }
}

impl Display for Blob {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Blob(key = {}, tag = {})", u64::from_le_bytes(self.0[..8].try_into().unwrap()), self.tag())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ValueKind {
    /// 8 bytes, inline.
    Inline8,
    /// 1 KB behind a pointer.
    Blob1K,
}

impl ValueKind {
    pub fn parse(bytes: &str) -> Result<ValueKind, String> {
        match bytes {
            "8" => Ok(ValueKind::Inline8),
            "1024" | "1k" | "1K" => Ok(ValueKind::Blob1K),
            other => Err(format!("unsupported --value-size '{other}' (8 or 1024)")),
        }
    }

    pub const fn bytes(self) -> usize {
        match self {
            ValueKind::Inline8 => 8,
            ValueKind::Blob1K => RECORD_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_slots_are_eight_bytes_either_way() {
        assert_eq!(std::mem::size_of::<u64>(), std::mem::size_of::<Blob>());
        assert_eq!(std::mem::size_of::<Option<Blob>>(), 8, "niche: an absent payload costs nothing");
    }

    #[test]
    fn blobs_are_verified_and_cheap_to_copy() {
        let a = Blob::record(42);
        assert!(a.check(42) && !a.check(43));
        let b = a.clone();
        assert!(std::ptr::eq(&*a.0 as *const _, &*b.0 as *const _), "a clone shares the record");
        assert_ne!(Blob::record(42).tag(), a.tag(), "every record gets a new tag");
        assert_eq!(Blob::tagged(7, 99).tag(), 99);
        assert!(Blob::tagged(7, 99).check(7));
        assert!(Blob::default().tag() == 0);

        let mut torn = [0u8; RECORD_BYTES];
        torn.copy_from_slice(&a.0[..]);
        torn[700] ^= 1;
        assert!(!Blob(Arc::new(torn)).check(42), "a flipped bit in the body is detected");
    }

    #[test]
    fn inline_values() {
        assert!(u64::record(5).check(5) && !u64::record(5).check(6));
        assert_eq!(u64::tagged(5, 77).tag(), 77);
        assert_eq!(ValueKind::parse("8").unwrap().bytes(), 8);
        assert_eq!(ValueKind::parse("1024").unwrap().bytes(), 1024);
        assert!(ValueKind::parse("16").is_err());
    }
}
