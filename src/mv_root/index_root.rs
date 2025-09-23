use std::collections::LinkedList;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use crate::mv_block::block::BlockUnsafeDegree;
use crate::mv_block::block_manager::BlockManager;
use crate::mv_page_model::{BlockRef, Height};
use crate::mv_root::frugal_root::{AtomicFrugalList, FrugalRootList};
use crate::mv_root::root::Root;
use crate::mv_root::sk_root::RootSkipList;
use crate::mv_root::tree_root::{RootTree, ValueRootInner};
use crate::mv_root::vanilla_root::VanillaRootSt;
use crate::mv_tree::version_manager::VersionManager;
use crate::mv_sync::smart_cell::LatchType;
use crate::mv_tx_model::transaction_result::SnapShot;

pub(crate) fn make_start_value_root_inner<
    const F: usize,
    const N: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone>(bk: &BlockManager<F, N, Key, Payload>, latch_type: LatchType
) -> (ValueRootInner<F, N, Key, Payload>, SnapShot)
{
    (ValueRootInner::initial(bk.new_empty_leaf(latch_type)), VersionManager::START_VERSION)
}

#[derive(Copy, Clone)]
pub enum RootIndexType {
    FrugalList(LatchType),
    SkipList(LatchType),
    BTree(LatchType),
    LinkedList(LatchType),
}

impl Display for RootIndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RootIndexType::FrugalList(latch) => write!(f, "FrugalList({latch})"),
            RootIndexType::SkipList(latch) => write!(f, "SkipList({latch})"),
            RootIndexType::BTree(latch) => write!(f, "BTree({latch})"),
            RootIndexType::LinkedList(latch) => write!(f, "LinkedList({latch})"),
        }
    }
}

impl Default for RootIndexType {
    fn default() -> Self {
        // Self::LinkedList(LatchType::default())
        Self::FrugalList(LatchType::default())
        // Self::SkipList(LatchType::default())
    }
}

#[derive(Clone)]
pub enum RootIndex<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static>
{
    FrugalList(Arc<Mutex<FrugalRootList<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    BTree(Arc<Mutex<RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    SkipList(Arc<Mutex<RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    LinkedList(Arc<Mutex<VanillaRootSt<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
}

type FrugalRootValue<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize, Key, Payload
> = ValueRootInner<FAN_OUT, NUM_RECORDS, Key, Payload>;

pub enum RootIndexGuard<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static>
{
    FrugalGuard(Arc<Mutex<AtomicFrugalList<ValueRootInner<FAN_OUT, NUM_RECORDS, Key, Payload>>>>),
    FrugalGuardMut(ArcMutexGuard<RawMutex, AtomicFrugalList<FrugalRootValue<FAN_OUT, NUM_RECORDS, Key, Payload>>>),

    BTreeGuard(Arc<Mutex<RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    BTreeGuardMut(ArcMutexGuard<RawMutex, RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>),

    SkipListGuard(Arc<Mutex<RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    SkipListGuardMut(ArcMutexGuard<RawMutex, RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>),

    LinkedListGuard(Arc<Mutex<VanillaRootSt<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    LinkedListGuardMut(ArcMutexGuard<RawMutex, LinkedList<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>>)
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn block(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndexGuard::BTreeGuard(tree) => unsafe {
                (*tree.data_ptr()).current_root().block
            }
            RootIndexGuard::BTreeGuardMut(tree) =>
                tree.current_root().block(),
            RootIndexGuard::SkipListGuard(sk) => unsafe {
                (*sk.data_ptr()).current_root().block
            }
            RootIndexGuard::SkipListGuardMut(sk) =>
                sk.current_root().block(),
            RootIndexGuard::LinkedListGuard(ll) => unsafe {
                (*ll.data_ptr()).back().unwrap().block()
            }
            RootIndexGuard::LinkedListGuardMut(ll) =>
                ll.back().unwrap().block(),
            RootIndexGuard::FrugalGuard(fg) => unsafe {
                (*fg.data_ptr()).current_root().0.0
            }
            RootIndexGuard::FrugalGuardMut(fg) => {
                fg.current_root().0.0
            }
        }
    }

    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            RootIndexGuard::FrugalGuard(fg) => {
                match fg.try_lock_arc() {
                    None => false,
                    Some(guard) => {
                        *self = RootIndexGuard::FrugalGuardMut(guard);
                        true
                    }
                }
            }
            RootIndexGuard::LinkedListGuard(ll) => {
                match ll.try_lock_arc() {
                    None => false,
                    Some(guard) => {
                        *self = RootIndexGuard::LinkedListGuardMut(guard);
                        true
                    }
                }
            }
            RootIndexGuard::SkipListGuard(sk) => {
                match sk.try_lock_arc() {
                    None => false,
                    Some(guard) => {
                        *self = RootIndexGuard::SkipListGuardMut(guard);
                        true
                    }
                }
            }
            RootIndexGuard::BTreeGuard(tree) => {
                match tree.try_lock_arc() {
                    None => false,
                    Some(guard) => {
                        *self = RootIndexGuard::BTreeGuardMut(guard);
                        true
                    }
                }
            }
            _ => true
        }
    }

    #[inline(always)]
    pub fn unsafe_degree_root(&self) -> BlockUnsafeDegree {
        match self {
            RootIndexGuard::BTreeGuard(tree) => unsafe {
                (&*tree.data_ptr()).current_root().block.unsafe_borrow().unsafe_degree_root()
            }
            RootIndexGuard::BTreeGuardMut(tree) =>
                tree.current_root().block.unsafe_borrow().unsafe_degree_root(),
            RootIndexGuard::SkipListGuard(sk, ..) => unsafe {
                (&*sk.data_ptr()).current_root().block.unsafe_borrow().unsafe_degree_root()
            }
            RootIndexGuard::SkipListGuardMut(sk, ..) =>
                sk.current_root().block.unsafe_borrow().unsafe_degree_root(),
            RootIndexGuard::LinkedListGuard(guard) => unsafe {
                (&*guard.data_ptr()).back().unwrap().block.unsafe_borrow().unsafe_degree_root()
            }
            RootIndexGuard::LinkedListGuardMut(guard) =>
                guard.back().unwrap().block.unsafe_borrow().unsafe_degree_root(),
            RootIndexGuard::FrugalGuard(fg) => unsafe {
                (*fg.data_ptr()).current_root().0.0.unsafe_borrow().unsafe_degree_root()
            } 
            RootIndexGuard::FrugalGuardMut(fg) => {
                fg.current_root().0.0.unsafe_borrow().unsafe_degree_root()
            }
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> RootIndex<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn count_roots(&self) -> usize {
        match self {
            RootIndex::FrugalList(fg) => unsafe {
                (*fg.data_ptr()).len()
            },
            RootIndex::BTree(t) => unsafe {
                2usize.pow((*t.data_ptr()).height() as _) - 1
            }
            RootIndex::SkipList(sk) => unsafe {
                (*sk.data_ptr()).0.len()
            }
            RootIndex::LinkedList(ll) => unsafe {
                (*ll.data_ptr()).len()
            }
        }
    }

    pub(crate) fn index_type(&self, latch_type: LatchType) -> RootIndexType {
        match self {
            RootIndex::FrugalList(..) => RootIndexType::FrugalList(latch_type),
            RootIndex::BTree(..) => RootIndexType::BTree(latch_type),
            RootIndex::SkipList(..) => RootIndexType::SkipList(latch_type),
            RootIndex::LinkedList(..) => RootIndexType::LinkedList(latch_type),
        }
    }
    pub fn new(variant: RootIndexType, block_manager: &BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>) -> Self {
        match variant {
            RootIndexType::BTree(latch) =>
                Self::BTree(Arc::new(Mutex::new(RootTree::new(latch)))),
            RootIndexType::SkipList(latch) => {
                let sk = RootSkipList::new();
                let (root_inner, version)
                    = make_start_value_root_inner(block_manager, latch);

                sk.0.insert(version, root_inner);
                Self::SkipList(Arc::new(Mutex::new(sk)))}

            RootIndexType::LinkedList(latch) => {
                let mut ll = LinkedList::new();
                let (root_inner, version)
                    = make_start_value_root_inner(block_manager, latch);

                ll.push_back(Root::new(root_inner.0, version, root_inner.1));
                Self::LinkedList(Arc::new(Mutex::new(ll)))
            }
            RootIndexType::FrugalList(latch) => {
                let (root_inner, version)
                    = make_start_value_root_inner(block_manager, latch);
                
                Self::FrugalList(Arc::new(Mutex::new(FrugalRootList::new(root_inner, version))))
            }
        }
    }

    #[inline(always)]
    pub fn borrow_read(&self) -> RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) =>
                RootIndexGuard::BTreeGuard(btree.clone()),
            RootIndex::SkipList(sk) =>
                RootIndexGuard::SkipListGuard(sk.clone()),
            RootIndex::LinkedList(ll) =>
                RootIndexGuard::LinkedListGuard(ll.clone()),
            RootIndex::FrugalList(fg) =>
                RootIndexGuard::FrugalGuard(fg.clone()),
        }
    }

    #[inline(always)]
    pub fn height(&self) -> Height {
        match self {
            RootIndex::BTree(tree) => unsafe {
                &*tree.data_ptr() }.height(),
            RootIndex::SkipList(sk, ..) => unsafe {
                &*sk.data_ptr() }.height(),
            RootIndex::LinkedList(list, ..) => unsafe {
                &*list.data_ptr() }.back().unwrap().height(),
            RootIndex::FrugalList(fg) => unsafe {
                (&*fg.data_ptr()).current_root().0.height()
            }
        }
    }

    #[inline(always)]
    pub fn current_root(&self) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) => unsafe {
                &*btree.data_ptr() }.current_root(),
            RootIndex::SkipList(sk, ..) => unsafe {
                &*sk.data_ptr() }.current_root(),
            RootIndex::LinkedList(ll, ..) => unsafe {
                &*ll.data_ptr() }.back().unwrap().clone(),
            RootIndex::FrugalList(fg) => unsafe {
                let (root_inner, version)
                    = (&*fg.data_ptr()).current_root();

                Root::new(root_inner.0, version, root_inner.1)
            }
        }
    }

    #[inline(always)]
    pub fn root_for(&self, si: SnapShot) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) => unsafe {
                &*btree.data_ptr() }.root_for(si),
            RootIndex::SkipList(sk, ..) =>  unsafe {
                &*sk.data_ptr() }.root_for(si),
            RootIndex::LinkedList(ll, ..) =>  unsafe {
                &*ll.data_ptr() }.iter().rev().find(|r| r.version() <= si).unwrap().clone(),
            RootIndex::FrugalList(fg) => unsafe {
                let fg
                    = &*fg.data_ptr();

                let frugal_node = fg
                    .find(si)
                    .unwrap();

                let root_inner
                    = frugal_node.payload.clone();

                Root::new(root_inner.0, frugal_node.insert_version, root_inner.1)
            }
        }
    }

    #[inline(always)]
    pub fn append_root(&self, root: Root<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool {
        match self {
            RootIndex::BTree(btree) =>
                unsafe { &*btree.data_ptr() }.append_root(root),
            RootIndex::SkipList(sk, ..) =>
                unsafe { &*sk.data_ptr() }.append_root(root),
            RootIndex::LinkedList(ll) => unsafe {
                (&mut *ll.data_ptr()).push_back(root);
                true
            }
            RootIndex::FrugalList(fg) => unsafe {
                (&*fg.data_ptr()).push(
                    ValueRootInner::from(root.block, root.height), root.version);
                true
            }
        }
    }
}