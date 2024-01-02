use std::sync::atomic::AtomicU64;

pub mod record_point;
pub mod unsafe_clone;
pub mod version_info;

/// Declares the atomic version type.
pub type AtomicVersion = AtomicU64;