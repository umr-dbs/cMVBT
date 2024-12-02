use std::arch::x86_64::_mm_mfence;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::mem::ManuallyDrop;
use std::sync;
use std::sync::atomic::{AtomicUsize, fence};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use serde::de::Unexpected::Seq;
use crate::mv_page_model::BlockRef;
use crate::mv_page_model::internal_page::InternalPage;
use crate::mv_page_model::leaf_page::LeafPage;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::version_info::{Version, VersionInfo};
use crate::mv_utils::interval::Interval;

// #[derive(Clone)]
// pub enum Node<
//     const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display
// > {
//     Index(InternalPage<FAN_OUT, NUM_RECORDS, Key>),
//     Leaf(LeafPage<NUM_RECORDS, Key>),
// }

pub struct Node<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    m_type: AtomicUsize,
    page: InnerPage<FAN_OUT, NUM_RECORDS, Key, Payload>,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Drop for Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn drop(&mut self) {
        fence(SeqCst);
        match self.m_type.load(SeqCst) {
            PAGE_TYPE_INTERNAL => unsafe {
                ManuallyDrop::drop(&mut self.page.internal)
            },
            PAGE_TYPE_LEAF => unsafe {
                ManuallyDrop::drop(&mut self.page.leaf)
            },
            _ => {}
        }
    }
}

pub union InnerPage<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    internal: ManuallyDrop<InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    leaf: ManuallyDrop<LeafPage<NUM_RECORDS, Key, Payload>>,
}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Sync for InnerPage<FAN_OUT, NUM_RECORDS, Key, Payload> { }

pub const PAGE_TYPE_INTERNAL: usize = 0;
pub const PAGE_TYPE_LEAF: usize = 1;

pub enum PageType<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    LeafRef(&'a LeafPage<NUM_RECORDS, Key, Payload>),
    IndexRef(&'a InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>),
    LeafMut(&'a mut LeafPage<NUM_RECORDS, Key, Payload>),
    IndexMut(&'a mut InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>),
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn m_type(&self) -> usize {
        self.m_type.load(Acquire)
    }

    #[inline(always)]
    pub fn as_page_ref(&self) -> PageType<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self.m_type() {
            PAGE_TYPE_INTERNAL => PageType::IndexRef(unsafe { &self.page.internal }),
            _ => PageType::LeafRef(unsafe { &self.page.leaf })
        }
    }

    #[inline(always)]
    pub fn as_page_mut(&mut self) -> PageType<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self.m_type() {
            PAGE_TYPE_INTERNAL => PageType::IndexMut(unsafe { &mut self.page.internal }),
            _ => PageType::LeafMut(unsafe { &mut self.page.leaf })
        }
    }

    #[inline(always)]
    pub const fn new_leaf() -> Self {
        Self {
            m_type: AtomicUsize::new(PAGE_TYPE_LEAF),
            page: InnerPage {
                leaf: ManuallyDrop::new(LeafPage::new())
            },
        }
    }

    #[inline(always)]
    pub const fn new_internal() -> Self {
        Self {
            m_type: AtomicUsize::new(PAGE_TYPE_INTERNAL),
            page: InnerPage {
                internal: ManuallyDrop::new(InternalPage::new())
            },
        }
    }

    #[inline(always)]
    pub fn is_leaf(&self) -> bool {
        self.m_type() == PAGE_TYPE_LEAF
    }

    #[inline(always)]
    pub fn as_records(&self) -> &[RecordPoint<Key, Payload>] {
        match self.m_type() {
            PAGE_TYPE_LEAF => unsafe {
                let deref
                    = &self.page.leaf;

                deref.as_records()
            },
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .as_records")
        }
    }

    #[inline(always)]
    pub fn keys_versions(&self) -> (&[Interval<Key>], &[Version]) {
        match self.m_type()  {
            PAGE_TYPE_INTERNAL => unsafe {
                let deref
                    = &self.page.internal;

                deref.keys_versions()
            },
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .keys_versions")
        }
    }

    #[inline(always)]
    pub unsafe fn keys(&self) -> &[Interval<Key>] {
        match self.m_type()  {
            PAGE_TYPE_INTERNAL => unsafe {
                let deref
                    = &self.page.internal;

                deref.keys()
            },
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .keys")
        }
    }

    #[inline(always)]
    pub fn children(&self) -> &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>] {
        match self.m_type()  {
            PAGE_TYPE_INTERNAL => unsafe {
                let deref
                    = &self.page.internal;

                deref.children()
            },
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .children")
        }
    }

    #[inline(always)]
    pub fn keys_versions_pointers(&self) -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>]) {
        match self.m_type()  {
            PAGE_TYPE_INTERNAL => unsafe {
                let deref
                    = &self.page.internal;

                deref.keys_versions_pointers()
            },
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .keys_versions_pointers")
        }
    }

    // #[inline]
    // pub fn delete_key(&mut self, key: Key, del: Version) -> Option<VersionInfo> {
    //     match self.m_type()  {
    //         PAGE_TYPE_LEAF => unsafe {
    //             let derefmut
    //                 = &mut self.page.leaf;
    // 
    //             derefmut.delete(key, del)
    //         },
    //         _ => None
    //     }
    // }

    #[inline(always)]
    pub fn as_leaf_page(&mut self) -> &mut LeafPage<NUM_RECORDS, Key, Payload> {
        match self.m_type()  {
            PAGE_TYPE_LEAF => unsafe { &mut self.page.leaf },
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn as_leaf_page_ref(&self) -> &LeafPage<NUM_RECORDS, Key, Payload> {
        match self.m_type()  {
            PAGE_TYPE_LEAF => unsafe { &self.page.leaf },
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn on_reuse(&mut self) {
        match self.m_type()  {
            PAGE_TYPE_INTERNAL => unsafe {
                let derefmut
                    = &mut self.page.internal;

                derefmut.on_reuse()
            },
            _ => unsafe {
                let derefmut
                    = &mut self.page.leaf;

                derefmut.on_reuse()
            },
        }
    }

    #[inline(always)]
    pub fn as_internal_page(&mut self) -> &mut InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self.m_type()  {
            PAGE_TYPE_INTERNAL => unsafe { &mut self.page.internal },
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn as_internal_page_ref(&self) -> &InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload> {
        match self.m_type()  {
            PAGE_TYPE_INTERNAL => unsafe { &self.page.internal },
            _ => unreachable!()
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self.m_type()  {
            PAGE_TYPE_INTERNAL => unsafe { self.page.internal.len() },
            _ => unsafe { self.page.leaf.len() },
        }
    }

    #[inline(always)]
    pub fn mark_leaf(&mut self) {
        self.m_type.store(PAGE_TYPE_LEAF, Release)
    }

    #[inline]
    pub fn mark_internal(&mut self) {
        self.m_type.store(PAGE_TYPE_INTERNAL, Release)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> AsRef<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        &self
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Default for Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        Self {
            m_type: AtomicUsize::new(PAGE_TYPE_LEAF),
            page: InnerPage { leaf: ManuallyDrop::new(LeafPage::new()) },
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> Clone for Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        if self.is_leaf() {
            Self {
                m_type: AtomicUsize::new(PAGE_TYPE_LEAF),
                page: InnerPage {
                    leaf: unsafe { self.page.leaf.clone() }
                },
            }
        } else {
            Self {
                m_type: AtomicUsize::new(PAGE_TYPE_INTERNAL),
                page: InnerPage {
                    internal: unsafe { self.page.internal.clone() }
                },
            }

        }
    }
}

// impl<const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display
// > Node<FAN_OUT, NUM_RECORDS, Key> {
//     #[inline(always)]
//     pub const fn is_leaf(&self) -> bool {
//         match self {
//             Node::Index(..) => false,
//             _ => true
//         }
//     }
//
//     #[inline]
//     pub fn mark_leaf(&mut self) {
//         unsafe {
//             ptr::write(self as *mut _ as *mut _, 1_usize)
//         }
//         // match self {
//         //     Node::Index(internal_page) => unsafe {
//         //         let as_leaf
//         //             = internal_page as *mut _ as *mut LeafPage< NUM_RECORDS, Key>;
//         //         *self = Node::Leaf(as_leaf.read())
//         //     }
//         //     _ => {}
//         // }
//     }
//
//     #[inline]
//     pub fn mark_internal(&mut self) {
//         unsafe {
//             ptr::write(self as *mut _ as *mut _, 0_usize)
//         }
//         // match self {
//         //     Node::Leaf(leaf_page) => unsafe {
//                 // let as_internal
//                 //     = leaf_page as *mut _ as *mut InternalPage<FAN_OUT, NUM_RECORDS, Key>;
//                 // *self = Node::Index(as_internal.read());
//         //     }
//         //     _ => {}
//         // }
//     }
//
//     #[inline(always)]
//     pub fn as_records(&self) -> &[RecordPoint<Key>] {
//         match self {
//             Node::Leaf(records_page) =>
//                 records_page.as_records(),
//             _ => unreachable!("Sleepy Joe hit me -> Not mv_tree Page .as_records")
//         }
//     }
//
//     #[inline(always)]
//     pub fn keys_versions(&self) -> (&[Interval<Key>], &[Version]) {
//         match self {
//             Node::Index(internal_page) =>
//                 internal_page.keys_versions(),
//             _ => unreachable!("Sleepy Joe hit me -> Not mv_tree Page .keys_versions")
//         }
//     }
//
//     #[inline(always)]
//     pub unsafe fn keys(&self) -> &[Interval<Key>] {
//         match self {
//             Node::Index(internal_page) =>
//                 internal_page.keys(),
//             _ => unreachable!("Sleepy Joe hit me -> Not mv_tree Page .keys")
//         }
//     }
//
//     #[inline(always)]
//     pub fn children(&self) -> &[BlockRef<FAN_OUT, NUM_RECORDS, Key>] {
//         match self {
//             Node::Index(internal_page) =>
//                 internal_page.children(),
//             _ => unreachable!("Sleepy Joe hit me -> Not mv_tree Page .children")
//         }
//     }
//
//     #[inline(always)]
//     pub fn keys_versions_pointers(&self) -> (&[Interval<Key>], &[Version], &[BlockRef<FAN_OUT, NUM_RECORDS, Key>]) {
//         match self {
//             Node::Index(internal_page) =>
//                 internal_page.keys_versions_pointers(),
//             _ => unreachable!("Sleepy Joe hit me -> Not mv_tree Page .keys_versions_pointers")
//         }
//     }
//
//     #[inline]
//     pub fn delete_key(&mut self, key: Key, del: Version) -> Option<VersionInfo> {
//         match self {
//             Node::Leaf(records) =>
//                 records.delete(key, del),
//             _ => None
//         }
//     }
//
//     #[inline(always)]
//     pub fn as_leaf_page(&mut self) -> &mut LeafPage<NUM_RECORDS, Key> {
//         match self {
//             Node::Leaf(records_page) => records_page,
//             _ => unreachable!()
//         }
//     }
//
//     #[inline(always)]
//     pub fn on_reuse(&mut self) {
//         match self {
//             Node::Index(internal_page) => internal_page.on_reuse(),
//             Node::Leaf(leaf_page) => leaf_page.on_reuse()
//         }
//     }
//
//     #[inline(always)]
//     pub fn as_internal_page(&mut self) -> &mut InternalPage<FAN_OUT, NUM_RECORDS, Key> {
//         match self {
//             Node::Index(internal_page) => internal_page,
//             _ => unreachable!()
//         }
//     }
//
//     #[inline(always)]
//     pub fn as_internal_page_ref(&self) -> &InternalPage<FAN_OUT, NUM_RECORDS, Key> {
//         match self {
//             Node::Index(internal_page) => internal_page,
//             _ => unreachable!()
//         }
//     }
//
//     #[inline(always)]
//     pub fn len(&self) -> usize {
//         match self {
//             Node::Index(index_page) => index_page.len(),
//             Node::Leaf(records_page) => records_page.len(),
//         }
//     }
// }
//
// impl<const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display
// > AsRef<Node<FAN_OUT, NUM_RECORDS, Key>> for Node<FAN_OUT, NUM_RECORDS, Key> {
//     fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key> {
//         &self
//     }
// }
//
// impl<const FAN_OUT: usize,
//     const NUM_RECORDS: usize,
//     Key: Default + Ord + Copy + Hash + Display
// > Default for Node<FAN_OUT, NUM_RECORDS, Key> {
//     fn default() -> Self {
//         Self::Leaf(LeafPage::default())
//     }
// }