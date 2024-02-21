use std::collections::LinkedList;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::ptr::NonNull;
use std::sync::Arc;
use crossbeam_channel::{at, Receiver};
use parking_lot::Mutex;
use rayon::ThreadPool;
use rb_tree::RBTree;
use crate::record_model::version_info::Version;
use crate::tree::locking_strategy::LockingStrategy;
use crate::tree::mvbplus_tree::{ClockType, MVBPlusTree};
use crate::tx_model::transaction::{AtomicTransaction, AtomicTransactionResult, SnapShot, Transaction, TransactionResult};
use crate::tx_model::tx_api::TransactionDispatcher;
use crate::utils::safe_cell::SafeCell;

enum TransactionHolder<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display>
{
    Atomic(AtomicTransaction<Key>),
    Multi(Transaction<Key>),
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display
> Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key>> for Transaction<Key> {
    #[inline(always)]
    fn into(self) -> TransactionHolder<FAN_OUT, NUM_RECORDS, Key> {
        TransactionHolder::Multi(self)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display
> Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key>> for AtomicTransaction<Key> {
    #[inline(always)]
    fn into(self) -> TransactionHolder<FAN_OUT, NUM_RECORDS, Key> {
        TransactionHolder::Atomic(self)
    }
}

pub enum TxExecutionResult<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + 'static>
{
    AtomicTxResult(AtomicTransactionResult<'a, FAN_OUT, NUM_RECORDS, Key>),
    TxResult(TransactionResult<'a, FAN_OUT, NUM_RECORDS, Key>),
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + 'static
> TxExecutionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    pub const fn is_ok(&self) -> bool {
        match self {
            TxExecutionResult::AtomicTxResult(atomic) =>
                atomic.is_ok(),
            TxExecutionResult::TxResult(tx_result) =>
                tx_result.is_ok()
        }
    }

    #[inline(always)]
    pub fn unwrap_transaction(self) -> TransactionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
        match self {
            TxExecutionResult::TxResult(tx) => tx,
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn unwrap_atomic(self) -> AtomicTransactionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
        match self {
            TxExecutionResult::AtomicTxResult(atomic) => atomic,
            _ => unreachable!()
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display> TransactionHolder<FAN_OUT, NUM_RECORDS, Key>
{
    #[inline]
    fn execute<'a>(self, dispatcher: &'a impl TransactionDispatcher<'a, FAN_OUT, NUM_RECORDS, Key>)
                   -> TxExecutionResult<'a, FAN_OUT, NUM_RECORDS, Key> {
        match self {
            TransactionHolder::Atomic(atomic) =>
                TxExecutionResult::AtomicTxResult(
                    dispatcher.dispatch_atomic_transaction(atomic)),
            TransactionHolder::Multi(tx) =>
                TxExecutionResult::TxResult(
                    dispatcher.dispatch_transaction(tx)),
        }
    }

    #[inline(always)]
    const fn snapshot(&self) -> Option<SnapShot> {
        match self {
            TransactionHolder::Atomic(atomic) => atomic.snapshot,
            TransactionHolder::Multi(tx) => tx.snapshot
        }
    }

    #[inline(always)]
    pub fn set_snapshot(&mut self, si: Option<SnapShot>) {
        match self {
            TransactionHolder::Atomic(atomic) => atomic.snapshot = si,
            TransactionHolder::Multi(tx) => tx.snapshot = si
        }
    }

    #[inline(always)]
    fn snapshot_version(&self) -> SnapShot {
        match self {
            TransactionHolder::Atomic(atomic) => atomic.snapshot(),
            TransactionHolder::Multi(tx) => tx.snapshot()
        }
    }
}

pub type ActiveTransactions = Arc<Mutex<RBTree<SnapShot>>>;

type TxDispatcher<const FAN_OUT: usize, const NUM_RECORDS: usize, Key>
= &'static MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>;

pub struct TransactionManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display
> {
    active_tx: Option<ActiveTransactions>,
    pool: ThreadPool,
    index: SafeCell<Box<MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>>>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + Send + 'static
> TransactionManager<FAN_OUT, NUM_RECORDS, Key>
{
    #[inline(always)]
    pub(crate) fn active_tx(&self) -> Option<ActiveTransactions> {
        self.active_tx.clone()
    }

    #[inline(always)]
    fn tx_dispatcher(&self) -> TxDispatcher<FAN_OUT, NUM_RECORDS, Key> {
        unsafe { mem::transmute(self.index()) }
    }

    #[inline(always)]
    pub fn index(&self) -> &MVBPlusTree<FAN_OUT, NUM_RECORDS, Key> {
        self.index.as_ref().as_ref()
    }

    pub fn locking_protocol(&self) -> &LockingStrategy {
        &self.index.as_ref().as_ref().locking_strategy
    }

    pub fn clock_type(&self) -> ClockType {
        self.index.as_ref().as_ref().clock_type()
    }

    pub fn disable_gc(&mut self) {
        self.active_tx.take();
        unsafe {
            self.index
                .as_mut()
                .as_mut()
                .block_manager
                .set_active_tx_for_gc(None);
        }
    }

    pub fn threads(&self) -> usize {
        self.pool.current_num_threads()
    }

    pub const fn is_gc_enabled(&self) -> bool {
        self.active_tx.is_some()
    }

    pub fn enable_gc(&mut self) {
        self.active_tx = Some(Arc::new(Default::default()));
        unsafe {
            let clone = self.active_tx();
            self.index
                .as_mut()
                .as_mut()
                .block_manager
                .set_active_tx_for_gc(clone);
        }
    }

    pub fn join(&self) {
        self.pool.join(|| {}, || {});
    }

    pub fn new(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>) -> Self {
        Self {
            active_tx: None,
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .thread_name(|t| format!("TxRunner{}", t))
                .build()
                .unwrap(),
            index: SafeCell::new(Box::new(index)),
        }
    }

    pub fn new_with(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>, gc: bool) -> Self {
        if gc {
            Self::new_with_gc(threads, index)
        }
        else {
            Self::new(threads, index)
        }
    }

    pub fn new_with_gc(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>) -> Self {
        let mut manager = Self {
            active_tx: Some(Arc::new(Default::default())),
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .thread_name(|t| format!("TxRunner{}", t))
                .build()
                .unwrap(),
            index: SafeCell::new(Box::new(index)),
        };

        unsafe {
            let clone = manager.active_tx();
            manager
                .index
                .as_mut()
                .as_mut()
                .block_manager
                .set_active_tx_for_gc(clone);
        }

        manager
    }

    #[inline(always)]
    fn enq_bookkeeping(&self, tx: &mut TransactionHolder<FAN_OUT, NUM_RECORDS, Key>) -> bool {
        if let Some(si) = tx.snapshot() {
            self.active_tx.as_ref().map(|active_list|
                active_list.lock().insert(si)
            ).unwrap_or(false)
        } else {
            let curr_si = unsafe { self.index.as_ref().as_ref().current_version() };
            tx.set_snapshot(curr_si.into());

            self.active_tx.as_ref().map(|active_list|
                active_list.lock().insert(curr_si)
            ).unwrap_or(false)
        }
    }

    fn deq_bookkeeping(bk: bool, active_tx: Option<ActiveTransactions>, snap_shot: Option<SnapShot>) {
        // bk.then(|| snap_shot.map(|si| active_tx.as_ref().map(|active_list|
        //     active_list.lock().remove(&si))));
        if bk {
            if let Some(si) = snap_shot {
                active_tx.as_ref().map(|active_list|
                    active_list.lock().remove(&si));
            }
        }
    }

    #[inline]
    pub fn execute_tx<Tx: Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key>>>(&self, tx: Tx)
    -> Receiver<TxExecutionResult<'static, FAN_OUT, NUM_RECORDS, Key>>
    {
        let mut tx = tx.into();
        let bk = self.enq_bookkeeping(&mut tx);
        self.execute_tx_reader_internal(tx, bk)
    }

    #[inline]
    pub fn execute_tx_non_reader<Tx: Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key>>>(&self, tx: Tx) {
        let mut tx = tx.into();
        let bk = self.enq_bookkeeping(&mut tx);
        self.execute_tx_non_reader_internal(tx, bk)
    }

    #[inline(always)]
    fn execute_tx_non_reader_internal(&self, tx: TransactionHolder<FAN_OUT, NUM_RECORDS, Key>, bk: bool) {
        let dispatcher
            = self.tx_dispatcher();

        let deq_active_query
            = self.active_tx();

        self.pool.spawn(move || {
            let si
                = tx.snapshot();

            let _ = tx.execute(dispatcher);
            Self::deq_bookkeeping(bk, deq_active_query, si);
        });
    }

    #[inline(always)]
    fn execute_tx_reader_internal(&self, tx: TransactionHolder<FAN_OUT, NUM_RECORDS, Key>, bk: bool)
                                  -> Receiver<TxExecutionResult<'static, FAN_OUT, NUM_RECORDS, Key>>
    {
        let dispatcher
            = self.tx_dispatcher();

        let deq_active_query
            = self.active_tx();

        let (sender, receiver)
            = crossbeam_channel::unbounded();

        self.pool.spawn(move || {
            let si
                = tx.snapshot();

            let _ = sender.send(tx.execute(dispatcher));
            Self::deq_bookkeeping(bk, deq_active_query, si);
        });

        receiver
    }
}