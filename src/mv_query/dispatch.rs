use std::hash::Hash;
use std::fmt::Display;
use std::mem;

use itertools::Itertools;
use crate::mv_crud_model::crud_api::CRUDDispatcher;
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_crud_model::crud_operation_result::CRUDOperationInnerReason::{KeyAlreadyDeleted, KeyDoesNotExist};
use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
use crate::mv_query::rand_query::RAND_ATTEMPTS_MAX;
use crate::mv_query::iter_query::RangeQueryIter;
use crate::mv_record_model::record_point::RecordPoint;
use crate::mv_record_model::version_info::VersionInfo;
use crate::mv_test::VERBOSE;
use crate::mv_tree::mvtree::MVTreeSt;
use crate::mv_sync::smart_cell::sched_yield;

pub const RANGE_DISPATCH_LAZY: bool = true;

impl<'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static
> CRUDDispatcher<'a, FAN_OUT, NUM_RECORDS, Key, Payload> for MVTreeSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn dispatch_crud(&'a self, crud: CRUDOperation<Key, Payload>) -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let is_concurrent = self.locking_strategy
            .is_concurrent();

        match crud {
            CRUDOperation::Insert(key, payload) => {
                let leaf_guard = if is_concurrent {
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

                let version
                    = self.start_tx_commit();

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(version), payload),
                    current_len);

                leaf_page.commit_delta(1, 0);

                self.end_tx_commit(version);

                CRUDOperationResult::Inserted(version)
            }
            CRUDOperation::Update(key, payload) => {
                let leaf_guard = if is_concurrent {
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

                match self.tracker() {
                    Some(db_tracker) if self.has_update_in_place() => match db_tracker.newest_live_si() {
                        Some(newest_si) => match leaf_page
                            .as_records_mut()
                            .iter_mut()
                            .rfind(|r| r.key() == key)
                        {
                            Some(record)
                            if record.version.insert_version > newest_si => {
                                record.version_mut().undelete();
                                *record.payload_mut() = payload;
                                leaf_page.commit_delta(1, -1);

                                return CRUDOperationResult::Updated(self.current_version_for_reader())
                            },
                            _ => { }
                        }
                        None => match leaf_page // empty live index: No readers; e.g., only updates!
                            .as_records_mut()
                            .iter_mut()
                            .rfind(|r| r.key() == key)
                        {
                            Some(record) => {
                                record.version_mut().undelete();
                                *record.payload_mut() = payload;
                                leaf_page.commit_delta(1, -1);

                                return CRUDOperationResult::Updated(self.current_version_for_reader())
                            },
                            _ => { }
                        }
                    }
                    _ => { }
                }

                let version
                    = self.start_tx_commit();

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(version), payload),
                    current_len);

                // soft commit for atomic visibility of new published record
                leaf_page.commit_delta(1, 0);

                match leaf_page.delete_after_update(key, version) {
                    Ok(Some(..)) => {
                        // Apply second soft atomic commit for lifetime end
                        leaf_page.commit_delta(-1, 1);
                        self.end_tx_commit(version);

                        CRUDOperationResult::Updated(version)
                    }
                    Ok(None) => {
                        leaf_page.undo_uncommitted(current_len);
                        CRUDOperationResult::ZeroAffected(KeyDoesNotExist)
                    }
                    Err(()) => {
                        leaf_page.undo_uncommitted(current_len);
                        CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
                    }
                }
            }
            CRUDOperation::Delete(key) => {
                if VERBOSE {
                    println!("dispatch delete key={key}");
                }
                let leaf_guard = if is_concurrent {
                    if VERBOSE {
                        println!("traverse olc start");
                    }
                    self.traversal_write_olc(key)
                } else {
                    self.traversal_write(key)
                };

                if VERBOSE {
                    println!("traverse olc end");
                    println!("[key={key}] - Leaf: ({:?}) records", leaf_guard.deref().unwrap().active_dead_count());
                }
                let leaf_deref_mut = leaf_guard
                    .deref_mut()
                    .unwrap();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();
                
                if VERBOSE {
                    println!("[key={key}] - Begin_commit()");
                }
                if VERBOSE {
                    println!("[key={key}] - Loop start");
                }

                let version
                    = self.start_tx_commit();

                if VERBOSE {
                    println!("[key={key}] - Commit succeeded: {version}, Attempts: 0");
                }
                match leaf_page.delete(key, version) {
                    Ok(Some(..)) => {
                        leaf_page.commit_delta(-1, 1);
                        if VERBOSE {
                            println!("After delete Leaf-records:\n{}", leaf_page.as_records().iter().join("\n"));
                        }

                        self.end_tx_commit(version);
                        CRUDOperationResult::Deleted(version)
                    },
                    Ok(None) => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
                    Err(()) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
                }
            }
            CRUDOperation::Range(range, version)
            if RANGE_DISPATCH_LAZY => match self.dispatch_crud(
                CRUDOperation::RangeIter(range, version)) {
                CRUDOperationResult::MatchedRecordIter(iter) =>
                    CRUDOperationResult::MatchedRecords(iter.collect()),
                other => other
            },
            CRUDOperation::Range(range, version) => {
                self.on_acquire_reader_snapshot(version);
                let res = Self::key_range_read_from_root(
                    self.retrieve_root_for(version),
                    range,
                    version);
                self.on_release_reader_snapshot(version);
                res
            },
            CRUDOperation::Point(key, version) => {
                self.on_acquire_reader_snapshot(version);
                let res = Self::key_point_read_from_root(
                    self.retrieve_root_for(version),
                    key,
                    version);
                self.on_release_reader_snapshot(version);
                res
            },
            CRUDOperation::RangeIter(key, version) =>
                CRUDOperationResult::MatchedRecordIter(RangeQueryIter::new(
                    self,
                    version,
                    key)),
            CRUDOperation::UpdateRand => {
                let (_fence, leaf_guard) =
                    self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard
                    .deref_mut()
                    .unwrap();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                let current_len
                    = leaf_page.len();

                let (live_n, _dead_n)
                    = leaf_page.active_dead_count();

                let mut find_i
                    = rand::random_range(0..live_n as usize);

                let mut key = Key::default();
                let payload = Payload::default();

                for r in leaf_page.as_records() {
                    if !r.version().is_deleted() {
                        if find_i == 0 {
                            key = r.key;
                            break
                        }
                        find_i -= 1;
                    }
                };

                match self.tracker() {
                    Some(db_tracker) if self.has_update_in_place() => match db_tracker.newest_live_si() {
                        Some(newest_si) => match leaf_page
                            .as_records_mut()
                            .iter_mut()
                            .rfind(|r| r.key() == key)
                        {
                            Some(record)
                            if record.version.insert_version > newest_si => {
                                record.version_mut().undelete();
                                *record.payload_mut() = payload;
                                leaf_page.commit_delta(1, -1);

                                return CRUDOperationResult::UpdatedRand(key, self.current_version_for_reader())
                            },
                            _ => { }
                        }
                        None => match leaf_page // empty live index: No readers; e.g., only updates!
                            .as_records_mut()
                            .iter_mut()
                            .rfind(|r| r.key() == key)
                        {
                            Some(record) => {
                                record.version_mut().undelete();
                                *record.payload_mut() = payload;
                                leaf_page.commit_delta(1, -1);

                                return CRUDOperationResult::UpdatedRand(key, self.current_version_for_reader())
                            },
                            _ => { }
                        }
                    }
                    _ => { }
                }

                let version
                    = self.start_tx_commit();

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(version), payload),
                    current_len);

                // two steps soft commit: Mark new record visible
                leaf_page.commit_delta(1, 0);
                match leaf_page.delete_after_update(key, version) {
                    Ok(Some(..)) => {
                        // second step soft commit: Correct counters
                        leaf_page.commit_delta(-1, 1);
                        self.end_tx_commit(version);

                        CRUDOperationResult::UpdatedRand(key, version)
                    }
                    Ok(None) => {
                        leaf_page.undo_uncommitted(current_len);
                        CRUDOperationResult::ZeroAffected(KeyDoesNotExist)
                    }
                    Err(()) => {
                        leaf_page.undo_uncommitted(current_len);
                        CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
                    }
                }
            }
            CRUDOperation::DeleteRand => {
                let (_fence, leaf_guard)
                    = self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard
                    .deref_mut()
                    .unwrap();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                let (live_n, _dead_n)
                    = leaf_page.active_dead_count();

                let mut find_i
                    = rand::random_range(0..live_n as usize);

                let mut key
                    = Key::default();

                for r in leaf_page.as_records() {
                    if !r.version().is_deleted() {
                        if find_i == 0 {
                            key = r.key;
                            break
                        }
                        find_i -= 1;
                    }
                };

                let version
                    = self.start_tx_commit();

                if VERBOSE {
                    println!("[key={key}] - Commit succeeded: {version}, Attempts: 0");
                }
                match leaf_page.delete(key, version) {
                    Ok(Some(..)) => {
                        leaf_page.commit_delta(-1, 1);
                        if VERBOSE {
                            println!("After delete Leaf-records:\n{}", leaf_page.as_records().iter().join("\n"));
                        }

                        self.end_tx_commit(version);
                        CRUDOperationResult::DeletedRand(key, version)
                    },
                    Ok(None) => CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
                    Err(()) => CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)
                }
            }
            CRUDOperation::InsertRand => {
                // return CRUDOperationResult::Error;
                let (fence, leaf_guard) =
                    self.traversal_write_rand_query();

                let leaf_deref_mut = leaf_guard
                    .deref_mut()
                    .unwrap();

                let leaf_page
                    = leaf_deref_mut.as_leaf_page();

                if size_of::<Key>() != mem::size_of::<u64>() { // Not supported
                    println!(">>>> CRUDOperation::InsertRand only supported on *u64* !");
                    return CRUDOperationResult::Error
                }

                let min = unsafe { *((&fence.lower) as * const _ as *const u64) };
                let max = unsafe { *((&fence.upper) as * const _ as *const u64) };

                let mut rand_attempts = 0;
                let key = loop {
                    let generated = rand::random_range(min..=max);
                    let gen_key = unsafe { *((&generated) as * const _ as * const Key) };

                    match leaf_page.as_records()
                        .iter()
                        .rfind(|r| r.key == gen_key)
                    {
                        None => break Some(gen_key),
                        Some(record) if record.version().is_deleted() =>
                            break Some(gen_key),
                        _ if rand_attempts >= RAND_ATTEMPTS_MAX => break None,
                        _ => {
                            rand_attempts += 1;
                            sched_yield(rand_attempts);
                        }
                    }
                };

                if key.is_none() {
                    println!(">> RandKey Generation Failed!\
                    >> RAND_ATTEMPTS_MAX = {RAND_ATTEMPTS_MAX}\
                    >> Fence = {fence}");

                    return self.dispatch_crud(CRUDOperation::InsertRand)
                }

                let key = key.unwrap();
                debug_assert!(key <= fence.upper && key >= fence.lower);
                if VERBOSE {
                    println!("[RandInsert] - Key: {key}, Fence= min: {min}, max: {max}");
                }
                let payload = Payload::default();

                let current_len
                    = leaf_page.len();

                let version
                    = self.start_tx_commit();

                leaf_page.push_uncommitted(
                    RecordPoint::new(key, VersionInfo::new(version), payload),
                    current_len);

                leaf_page.commit_delta(1, 0);
                self.end_tx_commit(version);
                
                CRUDOperationResult::InsertedRand(key, version)
            }
            _ => CRUDOperationResult::Error,
        }
    }
}