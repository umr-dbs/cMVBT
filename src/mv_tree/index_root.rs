use std::collections::LinkedList;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::tree::bplus_tree::BPlusTree;
use crossbeam_skiplist::SkipMap;
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use crate::mv_block::block::BlockUnsafeDegree;
use crate::mv_block::block_manager::BlockManager;
use crate::mv_page_model::{BlockRef, Height};
use crate::mv_tree::root::Root;
use crate::mv_tree::version_manager::VersionManager;
use crate::mv_tx_model::transaction::SnapShot;
use crate::mv_utils::smart_cell::LatchType;

fn mk_start_root<
    const F: usize,
    const N: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone>(bk: &BlockManager<F, N, Key, Payload>, latch_type: LatchType)
    -> (ValueRootInner<F, N, Key, Payload>, SnapShot)
{
    (ValueRootInner::new(bk.new_empty_leaf(latch_type)), VersionManager::START_VERSION)
}
pub enum RootIndexType {
    SkipList(LatchType),
    BTree(LatchType),
    LinkedList(LatchType),
}

impl Default for RootIndexType {
    fn default() -> Self {
        Self::SkipList(LatchType::default())
    }
}

// pub(crate) type LinkedRoots<
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key,
//     Payload> = UnCell<SmartRoot<FAN_OUT, NUM_RECORDS, Key, Payload>>;

pub(crate) struct RootSkipList<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone>(SkipMap<SnapShot, ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload>>);

#[derive(Clone, Default)]
struct ValueRootInner<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone>
(
    pub(crate) BlockRef<FANOUT, NUM_RECORDS, Key, Payload>,
    pub(crate) Height
);

impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone> ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub(crate) const fn new(block: BlockRef<FANOUT, NUM_RECORDS, Key, Payload>) -> Self {
        ValueRootInner(block, 1)
    }

    #[inline(always)]
    pub(crate) fn block(&self) -> BlockRef<FANOUT, NUM_RECORDS, Key, Payload> {
        self.0.clone()
    }
    #[inline(always)]
    pub(crate) fn height(&self) -> Height {
        self.1
    }
}

unsafe impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> Sync
for ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync,
    Payload: Display + Default + Clone + Sync> Send
for ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload> {}

impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> Display for ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InnerRoot<F: {FANOUT}, N: {NUM_RECORDS}> Height: {}", self.1)
    }
}

pub(crate) struct RootTree<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static>
(BPlusTree<FANOUT, NUM_RECORDS, SnapShot, ValueRootInner<FANOUT, NUM_RECORDS, Key, Payload>>);

#[derive(Clone)]
pub(crate) enum RootIndex<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static>
{
    BTree(Arc<Mutex<RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    SkipList(Arc<Mutex<RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    LinkedList(Arc<Mutex<LinkedList<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>>>),
}

pub(crate) enum RootIndexGuard<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static>
{
    BTreeGuard(Arc<Mutex<RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    BTreeGuardMut(ArcMutexGuard<RawMutex, RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>),

    SkipListGuard(Arc<Mutex<RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    SkipListGuardMut(ArcMutexGuard<RawMutex, RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>),

    LinkedListGuard(Arc<Mutex<LinkedList<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>>>),
    LinkedListGuardMut(ArcMutexGuard<RawMutex, LinkedList<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>>)
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub(crate) fn block(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndexGuard::BTreeGuard(tree) => unsafe {
                (*tree.data_ptr()).current_root().block()
            }
            RootIndexGuard::BTreeGuardMut(tree) =>
                tree.current_root().block(),
            RootIndexGuard::SkipListGuard(sk) => unsafe {
                (*sk.data_ptr()).current_root().block()
            }
            RootIndexGuard::SkipListGuardMut(sk) =>
                sk.current_root().block(),
            RootIndexGuard::LinkedListGuard(ll) => unsafe {
                (*ll.data_ptr()).back().unwrap().block()
            }
            RootIndexGuard::LinkedListGuardMut(ll) =>
                ll.back().unwrap().block()
        }
    }

    #[inline(always)]
    pub(crate) fn upgrade_write_lock(&mut self) -> bool {
        match self {
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
    pub(crate) fn unsafe_degree_root(&self) -> BlockUnsafeDegree {
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
                guard.back().unwrap().block.unsafe_borrow().unsafe_degree_root()
        }
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> RootIndex<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub fn new(variant: RootIndexType, block_manager: &BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>) -> Self {
        match variant {
            RootIndexType::BTree(latch) =>
                Self::BTree(Arc::new(Mutex::new(RootTree::new(latch)))),
            RootIndexType::SkipList(latch) => {
                let sk = RootSkipList::new();
                let (root_inner, version)
                    = mk_start_root(block_manager, latch);

                sk.0.insert(version, root_inner);
                Self::SkipList(Arc::new(Mutex::new(sk)))}

            RootIndexType::LinkedList(latch) => {
                let mut ll = LinkedList::new();
                let (root_inner, version)
                    = mk_start_root(block_manager, latch);

                ll.push_back(Root::new(root_inner.0, version, root_inner.1));
                Self::LinkedList(Arc::new(Mutex::new(ll)))
            }
        }
    }

    #[inline(always)]
    pub(crate) fn borrow_read(&self) -> RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) =>
                RootIndexGuard::BTreeGuard(btree.clone()),
            RootIndex::SkipList(sk) =>
                RootIndexGuard::SkipListGuard(sk.clone()),
            RootIndex::LinkedList(ll) =>
                RootIndexGuard::LinkedListGuard(ll.clone())
        }
    }

    #[inline(always)]
    pub(crate) fn height(&self) -> Height {
        match self {
            RootIndex::BTree(tree) => unsafe {
                &*tree.data_ptr() }.height(),
            RootIndex::SkipList(sk, ..) => unsafe {
                &*sk.data_ptr() }.height(),
            RootIndex::LinkedList(list, ..) => unsafe {
                &*list.data_ptr() }.back().unwrap().height(),
        }
    }

    #[inline(always)]
    pub(crate) fn current_root(&self) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) => unsafe {
                &*btree.data_ptr() }.current_root(),
            RootIndex::SkipList(sk, ..) => unsafe {
                &*sk.data_ptr() }.current_root(),
            RootIndex::LinkedList(ll, ..) => unsafe {
                &*ll.data_ptr() }.back().unwrap().clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn root_for(&self, si: SnapShot) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) => unsafe {
                &*btree.data_ptr() }.root_for(si),
            RootIndex::SkipList(sk, ..) =>  unsafe {
                &*sk.data_ptr() }.root_for(si),
            RootIndex::LinkedList(ll, ..) =>  unsafe {
                &*ll.data_ptr() }.iter().rev().find(|r| r.version() <= si).unwrap().clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn append_root(&self, root: Root<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool {
        match self {
            RootIndex::BTree(btree) =>
                unsafe { &*btree.data_ptr() }.append_root(root),
            RootIndex::SkipList(sk, ..) =>
                unsafe { &*sk.data_ptr() }.append_root(root),
            RootIndex::LinkedList(ll) => unsafe {
                (&mut *ll.data_ptr()).push_back(root);
                true
            }
        }
    }
}

impl<
    const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Default + Clone + Display + Sync + 'static> RootSkipList<FANOUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn new() -> Self {
        Self(SkipMap::new())
    }

    #[inline(always)]
    pub(crate) fn height(&self) -> Height {
        self.0.back().unwrap().value().height()
    }

    #[inline(always)]
    pub(crate) fn current_root(&self) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let last
            = self.0.back().unwrap();

        Root::new(last.value().block(), *last.key(), last.value().height())
    }

    #[inline(always)]
    pub(crate) fn root_for(&self, si: SnapShot) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let root_si
            = self.0.range(..=si).rev().next().unwrap();

        Root::new(root_si.value().block(), *root_si.key(), root_si.value().height())
    }

    #[inline(always)]
    pub(crate) fn append_root(&self, root: Root<FANOUT, NUM_RECORDS, Key, Payload>) -> bool {
        self.0.insert(root.version, ValueRootInner(root.block, root.height));
        true
    }
}

// impl<
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
//     Payload: Display + Default + Clone + Sync + 'static> LinkedRoots<FAN_OUT, NUM_RECORDS, Key, Payload>
// {
//     #[inline(always)]
//     pub(crate) fn height(&self) -> usize {
//         self.0.height() as usize
//     }
//
//     #[inline(always)]
//     pub(crate) fn current_root(&self) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
//         self.0.root.clone()
//     }
//
//     #[inline(always)]
//     pub(crate) fn root_for(&self, si: SnapShot) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
//           let mut root_anker
//             = self.deref().clone();
//
//         loop {
//             let root_h
//                 = root_anker.borrow_read();
//
//             let root_item
//                 = root_h.deref().unwrap();
//
//             if root_item.version().match_version_active(si) {
//                 break root_item.root.clone()
//             } else {
//                 root_anker = match root_item.prev {
//                     Some(ref p_root) => p_root.clone(),
//                     _ => unreachable!()
//                 };
//             }
//         }
//     }
//
//     #[inline(always)]
//     pub(crate) fn append_root(&self, root: Root<FAN_OUT, NUM_RECORDS, Key, Payload>, latch_type: LatchType) -> bool {
//         let sr = MVBPlusTree::make_smart_root(
//             latch_type,
//             RootItem {
//                 root,
//                 prev: Some(self.deref().clone()),
//             });
//
//         let _ = mem::replace(self.get_mut(), sr);
//         true
//     }
// }

impl<const FANOUT: usize,
    const NUM_RECORDS: usize,
    Key: Display + Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Display + Default + Clone + Sync + 'static> RootTree<FANOUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn new(latch_type: LatchType) -> Self {
        Self(BPlusTree::new_with(
                latch_type.into_cc_locking_strategy(),
                VersionManager::START_VERSION,
                SnapShot::MAX,
                |v| v + 1,
                |v| v - 1))
    }

    #[inline(always)]
    pub(crate) fn height(&self) -> Height {
        self.0.height()
    }

    #[inline(always)]
    pub(crate) fn current_root(&self) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let (_nv, crud)
            = self.0.dispatch(CRUDOperation::PeekMax);

        if let CRUDOperationResult::MatchedRecord(Some(latest_root)) = crud {
           Root::new(latest_root.payload.0, latest_root.key, latest_root.payload.1)
        }
        else {
            unreachable!("Failed access current_root!")
        }
    }

    #[inline(always)]
    pub(crate) fn root_for(&self, si: SnapShot) -> Root<FANOUT, NUM_RECORDS, Key, Payload> {
        let (_nv, crud)
            = self.0.dispatch(CRUDOperation::Pred(si));

        if let CRUDOperationResult::MatchedRecord(Some(si_root)) = crud {
            Root::new(si_root.payload.0, si_root.key, si_root.payload.1)
        }
        else {
            unreachable!("Failed access root_for!")
        }
    }

    #[inline(always)]
    pub(crate) fn append_root(&self, root: Root<FANOUT, NUM_RECORDS, Key, Payload>) -> bool {
        let (_nv, crud)
            = self.0.dispatch(CRUDOperation::Insert(root.version, ValueRootInner(root.block, root.height)));

        if let CRUDOperationResult::Inserted(..) = crud {
            true
        }
        else {
            false
        }
    }
}