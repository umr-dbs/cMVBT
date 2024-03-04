use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::Relaxed;
use cc_bplustree::crud_model::crud_api::CRUDDispatcher;
use cc_bplustree::crud_model::crud_operation::CRUDOperation;
use cc_bplustree::crud_model::crud_operation_result::CRUDOperationResult;
use cc_bplustree::locking::locking_strategy::LockingStrategy::OLC;
use cc_bplustree::tree::bplus_tree::BPlusTree;
use crossbeam_channel::Receiver;
use threadpool::ThreadPool;
// use rayon::ThreadPool;
use crate::test::{dec_key, inc_key};
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
    pub fn is_read(&self) -> bool {
        match self {
            TransactionHolder::Atomic(atomic) => atomic.crud.is_read(),
            TransactionHolder::Multi(mul) => mul.crud
                .iter()
                .all(|crud| crud.is_read())
        }
    }

    #[inline(always)]
    pub fn set_snapshot(&mut self, si: Option<SnapShot>) {
        match self {
            TransactionHolder::Atomic(atomic) => atomic.snapshot = si,
            TransactionHolder::Multi(tx) => tx.snapshot = si
        }
    }

    fn as_atomic(&self) -> &AtomicTransaction<Key> {
        match self {
            TransactionHolder::Atomic(atomic) => atomic,
            TransactionHolder::Multi(tx) => unreachable!()
        }
    }
}

// pub type ActiveTransactions = Arc<Mutex<RBTree<SnapShot>>>;

const AUX_ATX_FAN_OUT: usize = 250;
const AUX_ATX_LEAF_SIZE: usize = 499;

pub type ActiveTransactions
= Arc<SafeCell<BPlusTree<AUX_ATX_FAN_OUT, AUX_ATX_LEAF_SIZE, SnapShot, NullValue>>>;

type Dispatcher<const FAN_OUT: usize, const NUM_RECORDS: usize, Key>
= AtomicPtr<MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>>;

#[derive(Default, Clone)]
pub struct NullValue;

impl Display for NullValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "()")
    }
}

type TxDispatcher<const FAN_OUT: usize, const NUM_RECORDS: usize, Key>
= &'static MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>;

pub struct TransactionManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + 'static
> {
    active_tx: Option<ActiveTransactions>,
    pool: ThreadPool,
    index: Dispatcher<FAN_OUT, NUM_RECORDS, Key>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + 'static
> Drop for TransactionManager<FAN_OUT, NUM_RECORDS, Key> {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.index.load(Relaxed));
        }
    }
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
        unsafe {
            self.index.load(Relaxed).as_ref().unwrap()
        }
    }

    #[inline(always)]
    pub fn index_mut(&self) -> &mut MVBPlusTree<FAN_OUT, NUM_RECORDS, Key> {
        unsafe {
            self.index.load(Relaxed).as_mut().unwrap()
        }
    }

    pub fn locking_protocol(&self) -> &LockingStrategy {
        &self.index().locking_strategy
    }

    pub fn clock_type(&self) -> ClockType {
        self.index().clock_type()
    }

    pub fn disable_gc(&mut self) {
        self.active_tx.take();
        unsafe {
            self.index_mut()
                .block_manager
                .set_active_tx_for_gc(None);
        }
    }

    pub fn threads(&self) -> usize {
        // self.pool.current_num_threads()
        self.pool.max_count()
    }

    pub const fn is_gc_enabled(&self) -> bool {
        self.active_tx.is_some()
    }

    pub fn enable_gc(&mut self) {
        self.active_tx = Some(Arc::new(SafeCell::new(BPlusTree::new_with(
            OLC,
            SnapShot::MIN,
            SnapShot::MAX,
            inc_key,
            dec_key,
        ))));

        let clone = self.active_tx();
        self.index_mut()
            .block_manager
            .set_active_tx_for_gc(clone);
    }

    pub fn join(&self) {
        // self.pool.join(|| {}, || {});
        self.pool.join();
    }

    pub fn new(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>) -> Self {
        Self {
            active_tx: None,
            pool: threadpool::Builder::new()
                .num_threads(threads)
                // .thread_name(|t| format!("TxRunner{}", t))
                .build(),
                // .unwrap(),
            index: AtomicPtr::new(Box::into_raw(Box::new(index))),
        }
    }

    pub fn new_with(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>, gc: bool) -> Self {
        if gc {
            Self::new_with_gc(threads, index)
        } else {
            Self::new(threads, index)
        }
    }

    pub fn new_with_gc(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key>) -> Self {
        let mut manager = Self {
            active_tx: Some(Arc::new(SafeCell::new(BPlusTree::new_with(
                OLC,
                SnapShot::MIN,
                SnapShot::MAX,
                inc_key,
                dec_key,
            )))),
            pool: threadpool::Builder::new()
                .num_threads(threads)
                // .thread_name(|t| format!("TxRunner{}", t))
                .build(),
                // .unwrap(),
            index: AtomicPtr::new(Box::into_raw(Box::new(index))),
        };

        unsafe {
            let clone = manager.active_tx();
            manager
                .index_mut()
                .block_manager
                .set_active_tx_for_gc(clone);
        }

        manager
    }

    #[inline(always)]
    fn enq_bookkeeping(&self, tx: &mut TransactionHolder<FAN_OUT, NUM_RECORDS, Key>) -> bool {
        if tx.is_read() {
            let snapshot = tx
                .snapshot()
                .unwrap_or(self.tx_dispatcher().current_version());

            self.active_tx.as_ref()
                .map(|active_list|
                    match active_list.dispatch(CRUDOperation::Insert(snapshot, NullValue))
                    {
                        (.., CRUDOperationResult::Inserted(..)) => true,
                        _ => false
                    }
                ).unwrap_or(false)
        } else {
            false
        }
    }

    #[inline(always)]
    fn deq_bookkeeping(active_tx: ActiveTransactions, si: SnapShot) {
        match active_tx.dispatch(CRUDOperation::Delete(si)) {
            (_, CRUDOperationResult::Deleted(..)) => {}
            _ => unreachable!()
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

        self.pool.execute(move || {
            let si
                = tx.snapshot();

            let _ = tx.execute(dispatcher);
            if bk && si.is_some() && deq_active_query.is_some() {
                Self::deq_bookkeeping(deq_active_query.unwrap(), si.unwrap());
            }
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

        self.pool.execute(move || unsafe {
            let si
                = tx.snapshot();

            let _ = sender.send(tx.execute(dispatcher));
            if bk && si.is_some() && deq_active_query.is_some() {
                Self::deq_bookkeeping(deq_active_query.unwrap(), si.unwrap());
            }
        });

        receiver
    }
}