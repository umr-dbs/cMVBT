use std::hash::Hash;
use std::fmt::Display;
use std::mem;
use crate::crud_model::crud_api::CRUDDispatcher;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::crud_model::query::RangeQueryIter;
use crate::record_model::record_point::RecordPoint;
use crate::record_model::version_info::VersionInfo;
use crate::tree::mvbplus_tree::MVBPlusTree;
use crate::utils::smart_cell::sched_yield;

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + 'static + Display,
    Payload: Clone + Default + 'static
> CRUDDispatcher<'a, FAN_OUT, NUM_RECORDS, Key, Payload> for MVBPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn dispatch_crud(&'a self, crud: CRUDOperation<Key, Payload>) -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let is_optimistic = self.locking_strategy
            .is_optimistic();

        match crud {
            CRUDOperation::Insert(key, payload) => {
                let leaf_guard = if is_optimistic {
                    self.traversal_write_olc(key)
                } else {
                    self.traversal_write(key)
                };

                let leaf_deref_mut = leaf_guard
                    .deref_mut()
                    .unwrap();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                let current_len
                    = leaf_page.len();

                let mut commit_handle
                    = self.begin_commit();

                let version
                    = commit_handle.read_handle_version();

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(version), payload),
                    current_len);

                let mut commit_attempts
                    = 0;

                let committed_version = loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) if commit_attempts > 0 => unsafe {
                            let records
                                = leaf_page.as_records_mut();

                            records.get_unchecked_mut(current_len)
                                .version_mut()
                                .insert_version = commit;

                            break commit;
                        }
                        Ok(..) => break version,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                };

                leaf_page.commit_until(current_len);
                CRUDOperationResult::Inserted(committed_version)
            }
            CRUDOperation::Update(key, payload) => {
                let leaf_guard = if is_optimistic {
                    self.traversal_write_olc(key)
                } else {
                    self.traversal_write(key)
                };

                let leaf_deref_mut = leaf_guard
                    .deref_mut()
                    .unwrap();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                let current_len
                    = leaf_page.len();

                let mut commit_handle
                    = self.begin_commit();

                let version
                    = commit_handle.read_handle_version();

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(version), payload),
                    current_len);

                let mut commit_attempts
                    = 0;

                let committed_version = loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) if commit_attempts > 0 => unsafe {
                            let records
                                = leaf_page.as_records_mut();

                            records.get_unchecked_mut(current_len)
                                .version_mut()
                                .insert_version = commit;

                            break commit;
                        }
                        Ok(..) => break version,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                };

                match leaf_page.delete(key, version) {
                    None => {
                        leaf_page.undo_uncommitted(current_len);
                        CRUDOperationResult::Error
                    }
                    Some(..) => {
                        leaf_page.commit_until(current_len);
                        CRUDOperationResult::Updated(committed_version)
                    }
                }
            }
            CRUDOperation::Delete(key) => {
                let leaf_guard = if is_optimistic {
                    self.traversal_write_olc(key)
                } else {
                    self.traversal_write(key)
                };

                let leaf_deref_mut = leaf_guard
                    .deref_mut()
                    .unwrap();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                let mut commit_handle
                    = self.begin_commit();

                let mut commit_attempts
                    = 0;

                // maybe just fetch_add the atomic underneath, because? same for attempts overloads for any crud
                let committed_version = loop {
                    match self.try_end_commit(commit_handle) {
                        Ok(commit) => break commit,
                        Err(opt) => {
                            commit_attempts += 1;
                            sched_yield(commit_attempts);
                            commit_handle = opt
                        }
                    }
                };

                match leaf_page.delete(key, committed_version) {
                    None => CRUDOperationResult::Error,
                    Some(..) => CRUDOperationResult::Deleted(committed_version)
                }
            }
            CRUDOperation::Range(range, version) => Self::key_range_read_from_root(
                self.retrieve_root_for(version),
                range,
                version),
            CRUDOperation::Point(key, version) => Self::key_point_read_from_root(
                self.retrieve_root_for(version),
                key,
                version),
            CRUDOperation::RangeIter(key, version) =>
                CRUDOperationResult::MatchedRecordIter(RangeQueryIter::new(
                    self,
                    version,
                    key)),
            _ => CRUDOperationResult::Error,
        }
    }
}