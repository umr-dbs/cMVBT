use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Div;
use std::thread::spawn;
use std::time::SystemTime;
use itertools::Itertools;
use rand::Rng;
use rand::rngs::StdRng;
use crate::block::block_manager::{_4KB, bsz_alignment};
use crate::bplus_tree::BPlusTree;
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::Tree;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::locking_strategy::LockingStrategy;

pub const VALIDATE_OPERATION_RESULT: bool = false;
pub const EXE_LOOK_UPS: bool = false;
pub const EXE_RANGE_LOOK_UPS: bool = false;

pub const BSZ_BASE: usize = _4KB;
pub const BSZ: usize = BSZ_BASE - 0; // bsz_alignment::<Key, Payload>();
pub const FAN_OUT: usize = BSZ / 8 / 2;
pub const NUM_RECORDS: usize = (BSZ - 2) / (8 + 8);

// pub const FAN_OUT: usize = 16;
// pub const NUM_RECORDS: usize = 16;

// pub const NUM_RECORDS: usize = 64;

pub type Key = u64;
pub type Payload = f64;

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}

pub type INDEX = BPlusTree<FAN_OUT, NUM_RECORDS, Key>;

pub const MAKE_INDEX: fn(LockingStrategy) -> INDEX
= |ls| INDEX::new_with(ls);

#[inline(always)]
pub fn bulk_crud(worker_threads: usize, tree: Tree, operations_queue: &[CRUDOperation<Key>]) -> (u128, u64) {
    let mut data_buff = operations_queue
        .iter()
        .chunks(operations_queue.len() / worker_threads)
        .into_iter()
        .map(|s| s.into_iter().cloned().collect::<Vec<_>>())
        .collect::<VecDeque<_>>();

    if data_buff.len() > worker_threads {
        let back = data_buff.pop_back().unwrap();
        data_buff.front_mut().unwrap().extend(back);
    }

    let mut handles
        = Vec::with_capacity(worker_threads);

    let start = SystemTime::now();
    for _ in 1..=worker_threads {
        let current_chunk
            = data_buff.pop_front().unwrap();

        let index = tree.clone();
        handles.push(spawn(move || {
            let mut counter_errs = 0;
            current_chunk
                .iter()
                .for_each(|next_query| match index.dispatch(next_query) { // tree.execute(operation),
                    CRUDOperationResult::Error => counter_errs += 1,
                    _ => {}
                });
            counter_errs
        }));
    }

    let errs = handles
        .into_iter()
        .map(|handle| handle
            .join()
            .unwrap()
        ).fold(0, |errors, n_e| errors + n_e);

    let time_elapsed
        = SystemTime::now().duration_since(start).unwrap();

    (time_elapsed.as_millis(), errs)
}

pub fn format_insertions(i: usize) -> String {
    if i % 1_000_000_000 == 0 {
        format!("{} B", i as f64 / 1_000_000_000_f64)
    } else if i % 1_000_000 == 0 {
        format!("{} Mio", i as f64 / 1_000_000_f64)
    } else if i % 1_000 == 0 {
        format!("{} K", i as f64 / 1_000_f64)
    } else {
        i.to_string()
    }
}

pub fn gen_data_exp(limit: u64, lambda: f64, rnd: &mut StdRng) -> Vec<u64> {
    (1..=limit)
        .map(|i|
            gen_rand_key(i, 0, i, lambda, rnd))
        .collect()
}

pub fn gen_rand_key(i: u64, range_start: u64, range_end: u64, lambda: f64, rnd: &mut StdRng) -> u64 {
    #[inline(always)]
    fn sample_next(lambda: f64, rnd: &mut StdRng) -> f64 {
        let num
            = rnd.gen_range(0_f64..1_f64);

        (1_f64 - num)
            .ln()
            .div(-lambda)
    }

    let range = range_end - range_start;

    (((loop {
        let key = i as f64 * (1_f64 - sample_next(lambda, rnd));
        if key >= 0_f64 {
            break key;
        }
    }) / range as f64) * u64::MAX as f64) as _
}