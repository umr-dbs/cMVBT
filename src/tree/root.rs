use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::page_model::{BlockRef, Height};
use crate::record_model::version_info::Version;

pub const LEVEL_ROOT: Height = 1;

#[derive(Default, Clone)]
pub(crate) struct Root<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> {
    pub(crate) block: BlockRef<FAN_OUT, NUM_RECORDS, Key>,
    pub(crate) version: Version,
    pub(crate) height: Height
}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> Send for Root<FAN_OUT, NUM_RECORDS, Key> { }

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> Display for Root<FAN_OUT, NUM_RECORDS, Key> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Root(height: {}, version: {})", self.height(), self.version)
    }
}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> Sync for Root<FAN_OUT, NUM_RECORDS, Key> { }

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> Into<Root<FAN_OUT, NUM_RECORDS, Key>> for (BlockRef<FAN_OUT, NUM_RECORDS, Key>, Version, Height) {
    #[inline(always)]
    fn into(self) -> Root<FAN_OUT, NUM_RECORDS, Key> {
        Root::new(self.0, self.1, self.2)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display
> Root<FAN_OUT, NUM_RECORDS, Key> {
    #[inline(always)]
    pub(crate) fn new(block: BlockRef<FAN_OUT, NUM_RECORDS, Key>, version: Version, height: Height) -> Self {
        Self {
            block,
            version,
            height
        }
    }

    #[inline(always)]
    pub(crate) fn block(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key> {
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