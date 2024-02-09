use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use crossbeam_channel::Receiver;
use rayon::ThreadPool;
// use threadpool::ThreadPool;
// use rayon::{ThreadPool, ThreadPoolBuilder};
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::MVTree;
use crate::record_model::version_info::{AtomicVersion, Version};
use crate::tree::mvbplus_tree::MVBPlusTree;
use crate::tree::version_manager::VersionManager;
use crate::tx_model::transaction::{AtomicTransaction, AtomicTransactionResult, snapshot_from_atomic_tx_result, snapshot_from_tx_result, Transaction, TransactionResult};
use crate::tx_model::tx_api::{IsolatedSnapShot, TransactionDispatcher};
use crate::utils::safe_cell::SafeCell;


// enum TransactionHolder<
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Hash + Ord + Copy + Display> {
//     Atomic(AtomicTransaction<Key>),
//     Multi(Transaction<Key>),
// }
//
// pub enum TxExecutionResult<'a,
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Hash + Ord + Copy + Display + 'static>
// {
//     AtomicTxResult(AtomicTransactionResult<'a, FAN_OUT, NUM_RECORDS, Key>),
//     TxResult(TransactionResult<'a, FAN_OUT, NUM_RECORDS, Key>),
// }
//
// impl<'a,
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Hash + Ord + Copy + Display + 'static
// > TxExecutionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
//     #[inline(always)]
//     fn is_ok(&self) -> bool {
//         match self {
//             TxExecutionResult::AtomicTxResult(atomic) =>
//                 atomic.is_ok(),
//             TxExecutionResult::TxResult(tx_result) =>
//                 tx_result.is_ok()
//         }
//     }
//
//     #[inline(always)]
//     fn unwrap_transaction_result(self) -> TransactionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
//         match self {
//             TxExecutionResult::TxResult(tx) => tx,
//             _ => unreachable!()
//         }
//     }
//
//     #[inline(always)]
//     fn unwrap_atomic_result(self) -> AtomicTransactionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
//         match self {
//             TxExecutionResult::AtomicTxResult(atomic) => atomic,
//             _ => unreachable!()
//         }
//     }
// }
//
// impl<'a,
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Hash + Ord + Copy + Display> TransactionHolder<FAN_OUT, NUM_RECORDS, Key>
// {
//     #[inline]
//     fn execute(self, dispatcher: &'a impl TransactionDispatcher<'a, FAN_OUT, NUM_RECORDS, Key>)
//                -> TxExecutionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
//         match self {
//             TransactionHolder::Atomic(atomic) =>
//                 TxExecutionResult::AtomicTxResult(
//                     dispatcher.dispatch_atomic_transaction(atomic)),
//             TransactionHolder::Multi(tx) =>
//                 TxExecutionResult::TxResult(
//                     dispatcher.dispatch_transaction(tx)),
//         }
//     }
// }

pub struct TransactionManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display
> {
    oldest_version: AtomicVersion,
    pool: ThreadPool,
    index: SafeCell<NonNull<MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>>>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + Send + 'static
> TransactionManager<FAN_OUT, NUM_RECORDS, Key>
{
    #[inline(always)]
    fn mv_btree_handle(&self) -> &'static MVBPlusTree<FAN_OUT, NUM_RECORDS, Key> {
        unsafe { mem::transmute(self.index.get_mut().as_ref()) }
    }

    #[inline(always)]
    fn version_handle(&self) -> &'static AtomicVersion {
        unsafe { mem::transmute(&self.oldest_version) }
    }

    pub fn join(self) {
        self.pool.join(|| {}, || {});
    }

    pub fn new(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>) -> Self {
        Self {
            oldest_version: AtomicVersion::new(VersionManager::START_VERSION),
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .thread_name(|t| format!("TxRunner{}", t))
                .build()
                .unwrap(),
            index: SafeCell::new(NonNull::new(Box::leak(Box::new(index))).unwrap()),
        }
    }

    #[inline]
    pub fn execute_transaction(&self, tx: Transaction<Key>)
                               -> Receiver<TransactionResult<'static, FAN_OUT, NUM_RECORDS, Key>>
    {
        let (sender, receiver)
            = crossbeam_channel::unbounded();

        let index
            = self.mv_btree_handle();

        let version_handle
            = self.version_handle();

        self.pool.spawn(move || sender
            .send(index.dispatch_transaction(tx))
            .unwrap());

        receiver
    }

    #[inline]
    pub fn execute_atomic_transaction(&self, tx: AtomicTransaction<Key>)
                                      -> Receiver<AtomicTransactionResult<'static, FAN_OUT, NUM_RECORDS, Key>>
    {
        let (sender, receiver)
            = crossbeam_channel::unbounded();

        let index
            = self.mv_btree_handle();

        let version_handle
            = self.version_handle();

        self.pool.spawn(move || sender
            .send(index.dispatch_atomic_transaction(tx))
            .unwrap());

        receiver
    }

    #[inline]
    pub fn execute_atomic_transaction_non_reader(&self, tx: AtomicTransaction<Key>) {
        let index
            = self.mv_btree_handle();

        let version_handle
            = self.version_handle();

        self.pool.spawn(move || {
            let snapshot
                = tx.snapshot();

            let _
                = index.dispatch_atomic_transaction(tx);
        });
    }

    #[inline]
    pub fn execute_transaction_non_reader(&self, tx: Transaction<Key>) {
        let index
            = self.mv_btree_handle();

        let version_handle
            = self.version_handle();

        self.pool.spawn(move || {
            let snapshot
                = tx.snapshot();

            let _ = index.dispatch_transaction(tx);
        });
    }
}