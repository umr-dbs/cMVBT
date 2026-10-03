pub mod smart_cell;
pub mod safe_cell;
pub mod clock;
pub mod version_handle;
pub mod block_sync;

/// The commit clocks (`clock.rs`) and some environment variables are process-global: tests that run trees or change the
/// environment hold this lock, so that `cargo test` can run them in parallel without disturbing each other.
#[cfg(test)]
pub(crate) static TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
