use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::mv_page_model::{BlockRef, Height};
use crate::mv_record_model::version_info::Version;

pub const LEVEL_ROOT: Height = 1;

#[derive(Default, Clone)]
pub(crate) struct Root<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    pub(crate) block: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) version: Version,
    pub(crate) height: Height
}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Send for Root<FAN_OUT, NUM_RECORDS, Key, Payload> { }

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Display for Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Root(height: {}, version: {})", self.height(), self.version)
    }
}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Sync for Root<FAN_OUT, NUM_RECORDS, Key, Payload> { }

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Into<Root<FAN_OUT, NUM_RECORDS, Key, Payload>> for (BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, Version, Height) {
    #[inline(always)]
    fn into(self) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
        Root::new(self.0, self.1, self.2)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub(crate) fn new(block: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, version: Version, height: Height) -> Self {
        Self {
            block,
            version,
            height
        }
    }

    #[inline(always)]
    pub(crate) fn block(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.block.clone()
    }

    #[inline(always)]
    pub const fn height(&self) -> Height {
        self.height
    }

    #[inline(always)]
    pub const fn version(&self) -> Version {
        self.version
    }
}