use std::arch::x86_64::_mm_mfence;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_channel::Receiver;
use threadpool::ThreadPool;
use crate::mv_gc::db_tracker::{DBTracker, MDBTracker};
use crate::mv_tree::locking_strategy::LockingStrategy;
use crate::mv_tree::mvbplus_tree::{ClockType, MVBPlusTree};
use crate::mv_tx_model::transaction::{AtomicTransaction, AtomicTransactionResult, SnapShot, Transaction, TransactionResult};
use crate::mv_tx_model::tx_api::TransactionDispatcher;
use crate::mv_utils::safe_cell::SafeCell;

#[derive(Clone)]
pub enum TransactionHolder<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display,
    Payload: Clone
> {
    Atomic(AtomicTransaction<Key, Payload>),
    Multi(Transaction<Key, Payload>),
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display,
    Payload: Clone
> Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>> for Transaction<Key, Payload> {
    #[inline(always)]
    fn into(self) -> TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload> {
        TransactionHolder::Multi(self)
    }
}

unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display,
    Payload: Clone
> Send for TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display,
    Payload: Clone
> Sync for TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display,
    Payload: Clone
> Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>> for AtomicTransaction<Key, Payload> {
    #[inline(always)]
    fn into(self) -> TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload> {
        TransactionHolder::Atomic(self)
    }
}

pub enum TxExecutionResult<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + 'static,
    Payload: Clone + Default + 'static>
{
    AtomicTxResult(AtomicTransactionResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>),
    TxResult(TransactionResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload>),
}

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + 'static,
    Payload: Clone + Default
> TxExecutionResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
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
    pub fn unwrap_transaction(self) -> TransactionResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            TxExecutionResult::TxResult(tx) => tx,
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn unwrap_atomic(self) -> AtomicTransactionResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            TxExecutionResult::AtomicTxResult(atomic) => atomic,
            _ => unreachable!()
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display,
    Payload: Clone + Default> TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn execute<'a>(self, dispatcher: &'a impl TransactionDispatcher<'a, FAN_OUT, NUM_RECORDS, Key, Payload>)
                   -> TxExecutionResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
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

    fn as_atomic(&self) -> &AtomicTransaction<Key, Payload> {
        match self {
            TransactionHolder::Atomic(atomic) => atomic,
            TransactionHolder::Multi(..) => unreachable!()
        }
    }
}

type Dispatcher<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= AtomicPtr<MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>>;

type TxDispatcher<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= &'static MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>;

const POOL_DISABLED: usize = 0;
pub struct TransactionManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + 'static,
    Payload: Clone + Default + 'static
> {
    db_tracker: SafeCell<Option<MDBTracker<FAN_OUT, NUM_RECORDS, Key, Payload>>>,
    pool: Option<ThreadPool>,
    index: Dispatcher<FAN_OUT, NUM_RECORDS, Key, Payload>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + 'static,
    Payload: Clone + Default
> Drop for TransactionManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.index.load(Relaxed));
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Hash + Ord + Copy + Display + Send + 'static,
    Payload: Clone + Default + Send + 'static
> TransactionManager<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn db_tracker(&self) -> Option<MDBTracker<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        self.db_tracker.clone()
    }

    #[inline(always)]
    pub fn tx_dispatcher(&self) -> TxDispatcher<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe { mem::transmute(self.index()) }
    }

    #[inline(always)]
    pub fn index(&self) -> &MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe {
            self.index.load(Relaxed).as_ref().unwrap()
        }
    }

    #[inline(always)]
    pub fn index_mut(&self) -> &mut MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {
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

    pub fn disable_gc(&self) {
        self.db_tracker
            .get_mut()
            .take();

        self.index()
            .block_manager
            .del_aux();

        unsafe { _mm_mfence() }
    }

    pub fn threads(&self) -> usize {
        self.pool
            .as_ref()
            .map(|pool| pool.max_count())
            .unwrap_or(0)
    }

    pub fn is_gc_enabled(&self) -> bool {
        self.db_tracker.is_some()
    }

    pub fn enable_gc(&self) {
        *self.db_tracker.get_mut() = Some(Arc::new(DBTracker::new()));

        let clone
            = self.db_tracker.clone();

        self.index_mut()
            .block_manager
            .pass_aux_tx_tracker(clone);

        unsafe { _mm_mfence() }
    }

    pub fn join(&self) {
        self.pool
            .as_ref()
            .map(|pool| pool.join())
            .unwrap_or_default()
    }

    #[inline]
    pub fn execute_on_caller_thread<Tx: Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>>>(
        &self, tx: Tx,
    ) -> TxExecutionResult<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let tx
            = tx.into();

        let bk
            = self.enq_bookkeeping(&tx);

        let dispatcher
            = self.tx_dispatcher();

        let deq_active_query
            = self.db_tracker();

        let si
            = tx.snapshot();

        let r = tx.execute(dispatcher);
        if bk && si.is_some() && deq_active_query.is_some() {
            Self::deq_bookkeeping(deq_active_query.unwrap(), si.unwrap());
        }

        r
    }
    
    pub fn managed(&mut self, threads: usize) {
        self.join();
        self.pool.replace(threadpool::Builder::new()
            .num_threads(threads)
            .build());
    }
    
    pub fn unmanaged(&mut self) {
        self.join();
        self.pool.take();
    }

    pub fn new_unmanaged(index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>, gc: bool) -> Self {
        Self::new(POOL_DISABLED, index, gc)
    }
    
    pub fn new(threads: usize, index: MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>, gc: bool) -> Self {
        let manager = Self {
            db_tracker: if gc {
                SafeCell::new(Some(Arc::new(DBTracker::new())))
            }
            else {
                SafeCell::new(None)
            },
            pool: if threads == POOL_DISABLED {
                None
            } else {
                Some(threadpool::Builder::new()
                    .num_threads(threads)
                    .build())
            },
            index: AtomicPtr::new(Box::into_raw(Box::new(index))),
        };

        let clone
            = manager.db_tracker();

        manager
            .index()
            .block_manager
            .pass_aux_tx_tracker(clone);

        manager
    }

    #[inline(always)]
    pub fn enq_bookkeeping(&self, tx: &TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool {
        Self::enq_bookkeeping_from_tracker(self.db_tracker.as_ref().as_ref(), tx)
    }

    #[inline(always)]
    fn enq_bookkeeping_from_tracker(
        tracker: Option<&MDBTracker<FAN_OUT, NUM_RECORDS, Key, Payload>>,
        tx: &TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool
    {
        if tx.is_read() {
            match (tracker, tx.snapshot()) {
                (Some(tracker), Some(snapshot)) =>
                    return tracker.on_tx_start(snapshot),
                _ => {}
            }
        }

        false
    }

    #[inline(always)]
    fn deq_bookkeeping(db_tracker: MDBTracker<FAN_OUT, NUM_RECORDS, Key, Payload>, si: SnapShot) {
        db_tracker.on_tx_completed(si)
    }

    pub fn deq_book_keeping(&self, si: SnapShot) {
        if let Some(db_tracker) = self.db_tracker.as_ref() {
            Self::deq_bookkeeping(db_tracker.clone(), si)
        }
    }

    #[inline]
    pub fn execute_tx<Tx: Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>>>(&self, tx: Tx)
                                                                                       -> Receiver<TxExecutionResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>>
    {
        let tx = tx.into();
        let bk = self.enq_bookkeeping(&tx);
        self.execute_tx_reader_internal(tx, bk)
    }

    #[inline]
    pub fn execute_tx_non_reader<Tx: Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>>>(&self, tx: Tx) {
        let tx = tx.into();
        let bk = self.enq_bookkeeping(&tx);
        self.execute_tx_non_reader_internal(tx, bk)
    }

    #[inline]
    pub fn execute_tx_non_reader_batch<Tx: Into<TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>> + 'static>(
        &self,
        txs: SafeCell<Vec<Tx>>,
    ) {
        let m_db_tracker
            = self.db_tracker();

        let dispatcher
            = self.tx_dispatcher();

        self.pool.as_ref().unwrap().execute(move || txs.into_inner().into_iter().for_each(|tx| {
            let tx
                = tx.into();

            let si
                = tx.snapshot();

            let bk
                = Self::enq_bookkeeping_from_tracker(m_db_tracker.as_ref(), &tx);

            let _
                = tx.execute(dispatcher);

            if bk {
                if let (Some(tracker), Some(si)) = (m_db_tracker.as_ref(), si) {
                    tracker.on_tx_completed(si)
                }
            }
        }));
    }

    #[inline(always)]
    fn execute_tx_non_reader_internal(&self, tx: TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>, bk: bool) {
        let dispatcher
            = self.tx_dispatcher();

        let deq_active_query
            = self.db_tracker();

        self.pool.as_ref().unwrap().execute(move || {
            let si
                = tx.snapshot();

            let _ = tx.execute(dispatcher);
            if bk && si.is_some() && deq_active_query.is_some() {
                Self::deq_bookkeeping(deq_active_query.unwrap(), si.unwrap());
            }
        });
    }

    #[inline(always)]
    fn execute_tx_reader_internal(&self, tx: TransactionHolder<FAN_OUT, NUM_RECORDS, Key, Payload>, bk: bool)
                                  -> Receiver<TxExecutionResult<'static, FAN_OUT, NUM_RECORDS, Key, Payload>>
    {
        let dispatcher
            = self.tx_dispatcher();

        let deq_active_query
            = self.db_tracker();

        let (sender, receiver)
            = crossbeam_channel::unbounded();

        self.pool.as_ref().unwrap().execute(move || {
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