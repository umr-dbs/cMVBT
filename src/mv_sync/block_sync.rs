use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use crate::mv_block::block::Block;
use crate::mv_page_model::BlockRef;
use crate::mv_sync::safe_cell::SafeCell;
use crate::mv_sync::smart_cell::{LatchType, OptCell, SmartCell, SmartFlavor};

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn into_cell(self, latch: LatchType) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match latch {
            LatchType::Optimistic => self.into_olc(),
            LatchType::None => self.into_free(),
        }
    }

    #[inline(always)]
    fn into_free(self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        SmartCell(Arc::new(SmartFlavor::FreeCell(
            SafeCell::new(self))))
    }

    #[inline(always)]
    fn into_olc(self) ->  BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        SmartCell(Arc::new(SmartFlavor::OLCCell(
            OptCell::new(self))))
    }
}