use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use crate::mv_block::block::Block;
use crate::mv_page_model::BlockRef;
use crate::mv_sync::smart_cell::{OptCell, SmartCell};

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn into_cell(self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        SmartCell(Arc::new(OptCell::new(self)))
    }
}