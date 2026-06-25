use std::cell::Cell;
use std::fmt::Display;
use std::sync::Arc;
use arc_swap::ArcSwap;

use crate::mv_record_model::version_info::Version;
use crate::mv_root::tree_root::ValueRootInner;
use crate::mv_tx_model::transaction_result::SnapShot;

pub(crate) type FrugalRootList<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key,
    Payload
> = AtomicFrugalList<ValueRootInner<FAN_OUT, NUM_RECORDS, Key, Payload>>;

type TowerLevel = usize;
pub type FrugalNode<Payload> = Arc<FrugalNodeSt<Payload>>;
// nullable ptr right away
pub type FrugalNodeLink<Payload> = Option<FrugalNode<Payload>>;
// wrap memory order for arc loaders and posters into a single indirection instead of 2
type FrugalHeadNodeLink<Payload> = ArcSwap<FrugalNodeSt<Payload>>;

const FLAT_LEVEL: TowerLevel = 0; // all linear links
const SENTINEL_LEVEL: TowerLevel = TowerLevel::MAX; // head starter node

#[inline(always)]
fn pick_level() -> TowerLevel {
    const COIN_TOSS_PROBABILITY: f64 = 0.5;
    let mut lvl: TowerLevel = 1; // tower nodes start at 1; FLAT_LEVEL = 0 is for append_next
    while rand::random_bool(COIN_TOSS_PROBABILITY) {
        lvl += 1;
    }
    lvl
}

#[derive(Default)]
pub struct AtomicFrugalList<
    Payload: Clone + Display + Sync + Send + 'static>
{
    head: FrugalHeadNodeLink<Payload>, // We use handshake for arc (not refcount) loaders/posters
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static>
Clone for AtomicFrugalList<Payload>
{
    fn clone(&self) -> Self { // shallow clone; check atomicvlists, maybe shallow clone with arcswap
        AtomicFrugalList {
            head: ArcSwap::new(self.head.load().clone()),
        }
    }
}

#[derive(Default, Clone)]
pub struct FrugalNodeSt<
    Payload: Clone + Display + Send + Sync + 'static>
{
    pub next: FrugalNodeLink<Payload>, // linear to prev versions
    pub v_ridgy: FrugalNodeLink<Payload>, // skip to prev versions

    pub payload: Payload,
    pub insert_version: Version,
    pub level: Cell<TowerLevel>
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> AtomicFrugalList<Payload>
{
    #[inline(always)]
    pub fn current_root(&self) -> (Payload, SnapShot) {
        let guard_load
            = self.head.load();

        (guard_load.payload.clone(), guard_load.insert_version)
    }

    #[inline(always)]
    pub fn iter(&self) -> FrugalVersionIterator<Payload> {
        FrugalVersionIterator {
            current: Some(self.head.load_full())
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: Version) -> Self {
        Self {
            head: ArcSwap::new(Arc::new(FrugalNodeSt::new(
                payload,
                insert_version,
                SENTINEL_LEVEL // acts as sentinel, i.e., any coin toss matches a v_ridgy eventually
            )))
        }
    }

    #[inline(always)]
    pub fn push(&self, payload: Payload, insert_version: Version) {
        self.append(payload, insert_version);
    }

    #[inline]
    fn append(&self, payload: Payload, insert_version: Version) {
        const COIN_TOSS_PROBABILITY: f64 = 0.5;

        if rand::random_bool(COIN_TOSS_PROBABILITY) {
            self.append_tower(payload, insert_version)
        }
        else {
            self.append_next(payload, insert_version)
        }
    }

    #[inline(always)]
    fn append_next(&self, payload: Payload, insert_version: Version) {
        let head
            = self.head.load_full();

        let new_head = Arc::new(FrugalNodeSt::new_with(
            payload,
            insert_version,
            FLAT_LEVEL,
            Some(head.clone()), // next
            Some(head) // v_ridgy
        ));

        self.head.store(new_head);
    }

    #[inline(always)]
    fn append_tower(&self, payload: Payload, insert_version: Version) {
        let mut curr
            = self.head.load_full();

        let mut new_tower_node = FrugalNodeSt::new_with(
            payload,
            insert_version,
            pick_level(),
            Some(curr.clone()), // next
            None // v_ridgy
        );

        let new_tower_level
            = new_tower_node.level.get();

        while curr.level.get() < new_tower_level {
            curr = match curr.v_ridgy.as_ref() {
                Some(next) => next.clone(),
                None => unreachable!("frugal sentinel never seen!")
            };
        }

        new_tower_node.v_ridgy = Some(curr);
        self.head.store(Arc::new(new_tower_node));
    }

    #[inline(always)]
    pub fn find_from(mut curr: FrugalNode<Payload>,
                     look_up_version: Version) -> FrugalNodeLink<Payload>
    {
        while curr.level.get() < SENTINEL_LEVEL && curr.insert_version > look_up_version {
            match curr.v_ridgy.as_ref() {
                Some(v_ridgy) if v_ridgy.insert_version > look_up_version =>
                    curr = v_ridgy.clone(),
                _ => curr = curr.next.as_ref().unwrap().clone(),
            }
        }

        (curr.insert_version <= look_up_version)
            .then(move || curr)
    }

    #[inline]
    pub fn find(&self, look_up_version: Version) -> FrugalNodeLink<Payload> {
        Self::find_from(self.head.load_full(), look_up_version)
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> FrugalNodeSt<Payload>
{
    #[inline(always)]
    fn new(payload: Payload, insert_version: Version, level: TowerLevel) -> Self {
        Self::new_with(payload, insert_version, level, None, None)
    }

    #[inline(always)]
    pub fn new_with(payload: Payload,
                    insert_version: Version,
                    level: TowerLevel,
                    next: FrugalNodeLink<Payload>,
                    v_ridgy: FrugalNodeLink<Payload>) -> Self
    {
        Self {
            next,
            v_ridgy,
            payload,
            insert_version,
            level: Cell::new(level),
        }
    }
}

pub struct FrugalVersionIterator<
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    current: FrugalNodeLink<Payload>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static>
Iterator for FrugalVersionIterator<Payload> {
    type Item = FrugalNode<Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current.take() {
                Some(curr) => {
                    self.current = curr.next.clone();
                    break Some(curr)
                }
                _ => break None
            }
        }
    }
}