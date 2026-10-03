//! Retries of optimistic write traversals, grouped as in the paper: 0, 1-5, 6-9, 10-19, 20+.
//! Counted per thread and flushed into the global histogram when the thread ends.

use std::cell::Cell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

pub const GROUPS: usize = 5;
pub const GROUP_NAMES: [&str; GROUPS] = ["0", "1-5", "6-9", "10-19", "20+"];

static GLOBAL: [AtomicU64; GROUPS] = [const { AtomicU64::new(0) }; GROUPS];

struct Local([Cell<u64>; GROUPS]);

impl Drop for Local {
    fn drop(&mut self) {
        flush(&self.0);
    }
}

fn flush(local: &[Cell<u64>; GROUPS]) {
    for (global, local) in GLOBAL.iter().zip(local) {
        global.fetch_add(local.take(), Relaxed);
    }
}

thread_local! {
    static LOCAL: Local = const { Local([const { Cell::new(0) }; GROUPS]) };
}

#[inline(always)]
const fn group(retries: u32) -> usize {
    match retries {
        0 => 0,
        1..=5 => 1,
        6..=9 => 2,
        10..=19 => 3,
        _ => 4,
    }
}

/// Records that a write traversal succeeded after `retries` failed attempts.
#[inline(always)]
pub fn record(retries: u32) {
    let _ = LOCAL.try_with(|local| {
        let c = &local.0[group(retries)];
        c.set(c.get() + 1);
    });
}

/// The global histogram so far; resets it. Threads must have ended, or call `flush_current_thread`.
pub fn take() -> [u64; GROUPS] {
    GLOBAL.each_ref().map(|g| g.swap(0, Relaxed))
}

pub fn flush_current_thread() {
    let _ = LOCAL.try_with(|local| flush(&local.0));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_follow_the_paper() {
        let groups: Vec<usize> = [0, 1, 5, 6, 9, 10, 19, 20, 1000].iter().map(|r| group(*r)).collect();
        assert_eq!(groups, [0, 1, 1, 2, 2, 3, 3, 4, 4]);
        assert_eq!(GROUP_NAMES.len(), GROUPS);
    }
}
