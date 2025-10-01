use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::mv_page_model::{Height, Level};
use crate::mv_sync::smart_cell::LatchType;

#[inline(always)]
pub const fn OLC() -> LatchProtocol {
    LatchProtocol::OLC
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
pub enum LatchProtocol {
    #[default]
    MonoWriter,
    OLC,
}

pub type CRUDProtocol = LatchProtocol;

impl Display for LatchProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LatchProtocol::MonoWriter => write!(f, "MonoWriter"),
            LatchProtocol::OLC => write!(f, "OLC"),
        }
    }
}

impl LatchProtocol {
    #[inline(always)]
    pub const fn latch_type(&self) -> LatchType {
        match self {
            LatchProtocol::MonoWriter => LatchType::None,
            LatchProtocol::OLC => LatchType::Optimistic,
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