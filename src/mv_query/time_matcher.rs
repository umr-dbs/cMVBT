use crate::mv_record_model::version_info::Version;

pub const OBSOLETE_VERSION_MARK: Version = 0x80_00000000000000;

pub trait TimeMatcher {
    fn into_cmp(self) -> Self;
    // fn le_other_any(self, other: Version) -> bool;

    fn match_version_active(self, other: Version) -> bool;

    fn lt_self_any(self, other: Version) -> bool;

    fn is_obsolete(&self) -> bool;

    fn is_active(&self) -> bool;

    fn matched(self, other: Version) -> bool;
}

impl TimeMatcher for Version {
    #[inline(always)]
    fn into_cmp(self) -> Self {
        self & !OBSOLETE_VERSION_MARK
    }

    #[inline(always)]
    fn match_version_active(self, other: Version) -> bool {
        self <= other
    }

    #[inline(always)]
    fn lt_self_any(self, other: Version) -> bool {
        self & !OBSOLETE_VERSION_MARK < other
    }

    #[inline(always)]
    fn is_obsolete(&self) -> bool {
        *self & OBSOLETE_VERSION_MARK != 0
    }

    #[inline(always)]
    fn is_active(&self) -> bool {
        *self & OBSOLETE_VERSION_MARK == 0
    }

    #[inline(always)]
    fn matched(self, other: Version) -> bool {
        self & !OBSOLETE_VERSION_MARK <= other
    }
}