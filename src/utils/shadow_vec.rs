use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use crate::page_model::ObjectCount;

pub struct ShadowVec<E: Default> {
    pub(crate) unreal_vec: ManuallyDrop<Vec<E>>,
    pub(crate) obj_cnt: *mut ObjectCount,
    pub(crate) update_len: bool
}

impl<E: Default> Drop for ShadowVec<E> {
    fn drop(&mut self) {
        unsafe {
            if self.update_len {
                *self.obj_cnt = self.unreal_vec.len() as _
            }
        }
    }
}

impl<E: Default> Deref for ShadowVec<E> {
    type Target = Vec<E>;

    fn deref(&self) -> &Self::Target {
        self.unreal_vec.as_ref()
    }
}

impl<E: Default> DerefMut for ShadowVec<E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.unreal_vec.as_mut()
    }
}