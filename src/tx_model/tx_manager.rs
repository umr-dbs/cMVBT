use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use crossbeam_channel::Receiver;
use rayon::{ThreadPool, ThreadPoolBuilder};
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::MVTree;
use crate::record_model::version_info::Version;
use crate::tree::mvbplus_tree::MVBPlusTree;
use crate::tree::version_manager::VersionManager;
use crate::tx_model::dispatch::{AtomicTransactionResult, TransactionResult};
use crate::tx_model::transaction::{AtomicTransaction, Transaction};
use crate::tx_model::tx_api::{IsolatedSnapShot, TransactionDispatcher};


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
    threads: usize,
    oldest_version: Version,
    pool: ThreadPool,
    index: Box<MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>>
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + Send
> TransactionManager<FAN_OUT, NUM_RECORDS, Key>
{
    pub fn join(self) {
        self.pool.join(|| {}, || {});
    }

    pub fn new(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>) -> Self {
        Self {
            threads,
            oldest_version: VersionManager::START_VERSION,
            pool: ThreadPoolBuilder::new()
                .num_threads(threads)
                .thread_name(|f| format!("TxRunner_{}", f))
                .build()
                .unwrap(),
            index: Box::new(index)
        }
    }

    pub fn execute_transaction(&self, tx: Transaction<Key>)
    -> Receiver<TransactionResult<'static, FAN_OUT, NUM_RECORDS, Key>>
    {
        let (sender, receiver)
            = crossbeam_channel::bounded(1);

        let snapshot: &MVBPlusTree<FAN_OUT, NUM_RECORDS, Key> = unsafe {
            mem::transmute(self.index.deref())
        };

        self.pool.spawn(move || unsafe {
            let si
                = snapshot.snapshot_for(tx.snapshot());

            let mv_tree: &MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>
                = mem::transmute(si.mv_tree());

            sender.send(mv_tree.dispatch_transaction(tx));
        });

        receiver
    }

    pub fn execute_atomic_transaction(&self, tx: AtomicTransaction<Key>)
    -> Receiver<AtomicTransactionResult<'static, FAN_OUT, NUM_RECORDS, Key>>
    {
        let (sender, receiver)
            = crossbeam_channel::bounded(1);

        let snapshot: &MVBPlusTree<FAN_OUT, NUM_RECORDS, Key> = unsafe {
            mem::transmute(self.index.deref())
        };

        self.pool.spawn(move || {
            sender.send(snapshot.dispatch_atomic_transaction(tx));
        });

        receiver
    }
}







