use std::collections::LinkedList;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use CCBPlusTree::record_model::Version;
use itertools::Itertools;
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_page_model::{BlockRef, Height};
use crate::mv_root::frugal_root::{AtomicFrugalList, FrugalRootList};
use crate::mv_root::root::Root;
use crate::mv_root::sk_root::RootSkipList;
use crate::mv_root::tree_root::{RootTree, ValueRootInner};
use crate::mv_root::vanilla_root::VanillaRootSt;
use crate::mv_sync::smart_cell::{OptCell, SmartCell, SmartGuard};
use crate::mv_sync::version_handle;
use crate::mv_tree::smo::BlockUnsafeDegree;
use crate::mv_tx_model::transaction_result::SnapShot;

pub(crate) fn make_start_value_root_inner<
    const F: usize,
    const N: usize,
    Key: Display + Default + Ord + Copy + Hash,
    Payload: Default + Clone>(bk: &BlockAllocManager<F, N, Key, Payload>
) -> (ValueRootInner<F, N, Key, Payload>, SnapShot)
{
    (ValueRootInner::initial(bk.new_empty_leaf()), version_handle::START_VERSION)
}

#[derive(Copy, Clone)]
pub enum RootIndexType {
    // HybridArray,
    FrugalList,
    SkipList,
    BTree,
    LinkedList,
}

impl Display for RootIndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RootIndexType::FrugalList => write!(f, "FrugalList"),
            RootIndexType::SkipList => write!(f, "SkipList"),
            RootIndexType::BTree => write!(f, "BTree"),
            RootIndexType::LinkedList => write!(f, "LinkedList"),
            // RootIndexType::HybridArray => write!(f, "HybridArray"),
        }
    }
}

impl Default for RootIndexType {
    fn default() -> Self {
        // Self::LinkedList(LatchType::default())
        Self::FrugalList
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
    FrugalList(SmartCell<FrugalRootList<FAN_OUT, NUM_RECORDS, Key, Payload>>),
    BTree(SmartCell<RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>),
    SkipList(SmartCell<RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>),
    LinkedList(SmartCell<VanillaRootSt<FAN_OUT, NUM_RECORDS, Key, Payload>>),
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
    FrugalGuard(SmartGuard<AtomicFrugalList<ValueRootInner<FAN_OUT, NUM_RECORDS, Key, Payload>>>),
    // FrugalGuardMut(ArcMutexGuard<RawMutex, AtomicFrugalList<FrugalRootValue<FAN_OUT, NUM_RECORDS, Key, Payload>>>),

    BTreeGuard(SmartGuard<RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>),
    // BTreeGuardMut(SmartGuard<RootTree<FAN_OUT, NUM_RECORDS, Key, Payload>>),

    SkipListGuard(SmartGuard<RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>),
    // SkipListGuardMut(SmartGuard<RootSkipList<FAN_OUT, NUM_RECORDS, Key, Payload>>),

    LinkedListGuard(SmartGuard<VanillaRootSt<FAN_OUT, NUM_RECORDS, Key, Payload>>),
    // LinkedListGuardMut(SmartGuard<LinkedList<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>>)
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
            RootIndexGuard::BTreeGuard(tree) =>
                tree.current_root().block,
            // RootIndexGuard::BTreeGuardMut(tree) =>
            //     tree.current_root().block(),
            RootIndexGuard::SkipListGuard(sk) =>
                sk.current_root().block,
            // RootIndexGuard::SkipListGuardMut(sk) =>
            //     sk.current_root().block(),
            RootIndexGuard::LinkedListGuard(ll) =>
                ll.back().unwrap().block(),
            // RootIndexGuard::LinkedListGuardMut(ll) =>
            //     ll.back().unwrap().block(),
            RootIndexGuard::FrugalGuard(fg) =>
                fg.current_root().0.0,
            // RootIndexGuard::FrugalGuardMut(fg) =>
            //     fg.current_root().0.0
        }
    }

    #[inline(always)]
    pub fn version(&self) -> Version {
        match self {
            RootIndexGuard::BTreeGuard(tree) =>
                tree.current_root().version,
            // RootIndexGuard::BTreeGuardMut(tree) =>
            //     tree.current_root().version,
            RootIndexGuard::SkipListGuard(sk) =>
                sk.current_root().version,
            // RootIndexGuard::SkipListGuardMut(sk) =>
            //     sk.current_root().version,
            RootIndexGuard::LinkedListGuard(ll) =>
                ll.back().unwrap().version,
            // RootIndexGuard::LinkedListGuardMut(ll) =>
            //     ll.back().unwrap().version,
            RootIndexGuard::FrugalGuard(fg) =>
                fg.current_root().1,
            // RootIndexGuard::FrugalGuardMut(fg) =>
            //     fg.current_root().1

        }
    }

    #[inline(always)]
    pub fn upgrade_write_lock(&mut self) -> bool {
        match self {
            RootIndexGuard::FrugalGuard(fg) =>
                fg.upgrade_write_lock(),
            RootIndexGuard::LinkedListGuard(ll) =>
                ll.upgrade_write_lock(),
            RootIndexGuard::SkipListGuard(sk) =>
                sk.upgrade_write_lock(),
            RootIndexGuard::BTreeGuard(tree) =>
                tree.upgrade_write_lock(),
            _ => true
        }
    }

    #[inline(always)]
    pub fn unsafe_degree_root(&self) -> BlockUnsafeDegree {
        match self {
            RootIndexGuard::BTreeGuard(tree) =>
                tree.current_root().block.unsafe_borrow().unsafe_degree_root(),
            // RootIndexGuard::BTreeGuardMut(tree) =>
            //     tree.current_root().block.unsafe_borrow().unsafe_degree_root(),
            RootIndexGuard::SkipListGuard(sk, ..) =>
                sk.current_root().block.unsafe_borrow().unsafe_degree_root(),
            // RootIndexGuard::SkipListGuardMut(sk, ..) =>
            //     sk.current_root().block.unsafe_borrow().unsafe_degree_root(),
            RootIndexGuard::LinkedListGuard(guard) =>
                guard.back().unwrap().block.unsafe_borrow().unsafe_degree_root(),
            // RootIndexGuard::LinkedListGuardMut(guard) =>
            //     guard.back().unwrap().block.unsafe_borrow().unsafe_degree_root(),
            RootIndexGuard::FrugalGuard(fg) =>
                fg.current_root().0.0.unsafe_borrow().unsafe_degree_root(),
            // RootIndexGuard::FrugalGuardMut(fg) =>
            //     fg.current_root().0.0.unsafe_borrow().unsafe_degree_root()
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
            RootIndex::FrugalList(fg) =>
                fg.unsafe_borrow().len(),
            RootIndex::BTree(t) =>
                2usize.pow(t.unsafe_borrow().height() as _) - 1,
            RootIndex::SkipList(sk) =>
                sk.unsafe_borrow().0.len(),
            RootIndex::LinkedList(ll) =>
                ll.unsafe_borrow().len()
        }
    }

    pub(crate) fn index_type(&self) -> RootIndexType {
        match self {
            RootIndex::FrugalList(..) => RootIndexType::FrugalList,
            RootIndex::BTree(..) => RootIndexType::BTree,
            RootIndex::SkipList(..) => RootIndexType::SkipList,
            RootIndex::LinkedList(..) => RootIndexType::LinkedList,
        }
    }
    pub fn new(variant: RootIndexType, block_manager: &BlockAllocManager<FAN_OUT, NUM_RECORDS, Key, Payload>) -> Self {
        match variant {
            RootIndexType::BTree => {
                let rt = RootTree::new();

                let (root_inner, version)
                    = make_start_value_root_inner(block_manager);

                rt.append_root(
                    Root::new(root_inner.0, version, root_inner.1));

                Self::BTree(SmartCell(Arc::new(OptCell::new(rt))))
            },
            RootIndexType::SkipList => {
                let sk = RootSkipList::new();
                let (root_inner, version)
                    = make_start_value_root_inner(block_manager,);

                sk.0.insert(version, root_inner);
                Self::SkipList(SmartCell(Arc::new(OptCell::new(sk))))
            }
            RootIndexType::LinkedList => {
                let mut ll = LinkedList::new();
                let (root_inner, version)
                    = make_start_value_root_inner(block_manager);

                ll.push_back(Root::new(root_inner.0, version, root_inner.1));
                Self::LinkedList(SmartCell(Arc::new(OptCell::new(ll))))
            }
            RootIndexType::FrugalList => {
                let (root_inner, version)
                    = make_start_value_root_inner(block_manager);

                Self::FrugalList(SmartCell(Arc::new(OptCell::new(FrugalRootList::new(root_inner, version)))))
            }
            // RootIndexType::HybridArray => {}
        }
    }

    #[inline(always)]
    pub fn borrow_read(&self) -> RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) =>
                RootIndexGuard::BTreeGuard(btree.borrow_read()),
            RootIndex::SkipList(sk) =>
                RootIndexGuard::SkipListGuard(sk.borrow_read()),
            RootIndex::LinkedList(ll) =>
                RootIndexGuard::LinkedListGuard(ll.borrow_read()),
            RootIndex::FrugalList(fg) =>
                RootIndexGuard::FrugalGuard(fg.borrow_read()),
        }
    }

    #[inline(always)]
    pub fn height(&self) -> Height {
        match self {
            RootIndex::BTree(tree) =>
                tree.height(),
            RootIndex::SkipList(sk, ..) =>
                sk.height(),
            RootIndex::LinkedList(list, ..) =>
                list.back().unwrap().height(),
            RootIndex::FrugalList(fg) =>
                fg.current_root().0.height()
        }
    }

    #[inline(always)]
    pub fn current_root(&self) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) =>
                btree.current_root(),
            RootIndex::SkipList(sk, ..) =>
                sk.current_root(),
            RootIndex::LinkedList(ll, ..) =>
                ll.back().unwrap().clone(),
            RootIndex::FrugalList(fg) => {
                let (root_inner, version)
                    = fg.current_root();

                Root::new(root_inner.0, version, root_inner.1)
            }
        }
    }

    #[inline(always)]
    pub fn root_for(&self, si: SnapShot) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self {
            RootIndex::BTree(btree) =>
                btree.root_for(si),
            RootIndex::SkipList(sk, ..) =>
                sk.root_for(si),
            RootIndex::LinkedList(ll, ..) => {
                    ll.iter().rfind(|r| r.version() <= si)
                        .unwrap_or_else(|| ll.front().unwrap()).clone()
            },
            RootIndex::FrugalList(fg) => {
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
                btree.append_root(root),
            RootIndex::SkipList(sk, ..) =>
                sk.append_root(root),
            RootIndex::LinkedList(ll) => {
                ll.unsafe_borrow_mut().push_back(root);
                true
            }
            RootIndex::FrugalList(fg) => {
                fg.push(ValueRootInner::from(root.block, root.height), root.version);
                true
            }
        }
    }
}