use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::mv_page_model::{Height, Level};
use crate::mv_sync::smart_cell::LatchType;

#[inline(always)]
pub const fn OLC() -> LockingStrategy {
    LockingStrategy::OLC
}

pub trait LevelExtras {
    fn is_lock(&self, height: Height, lock_from: f32) -> bool;
}

impl LevelExtras for Level {
    #[inline(always)]
    fn is_lock(&self, height: Height, lock_from: f32) -> bool {
        (lock_from * height as f32) as Self <= *self
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub enum LockingStrategy {
    #[default]
    MonoWriter,
    OLC,
}

pub type CRUDProtocol = LockingStrategy;

impl Display for LockingStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LockingStrategy::MonoWriter => write!(f, "MonoWriter"),
            LockingStrategy::OLC => write!(f, "OLC"),
        }
    }
}

impl LockingStrategy {
    #[inline(always)]
    pub const fn latch_type(&self) -> LatchType {
        match self {
            LockingStrategy::MonoWriter => LatchType::None,
            LockingStrategy::OLC => LatchType::Optimistic,
        }
    }

    #[inline(always)]
    pub(crate) const fn is_concurrent(&self) -> bool {
        match self {
            Self::OLC => true,
            _ => false
        }
    }

    #[inline(always)]
    pub(crate) const fn is_mono_writer(&self) -> bool {
        match self {
            Self::MonoWriter => true,
            _ => false
        }
    }
}