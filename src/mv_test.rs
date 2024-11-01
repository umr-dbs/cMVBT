use std::ops::Div;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::thread::{spawn, JoinHandle};
use std::time::SystemTime;
use crossbeam_channel::{Sender, TryRecvError};
use itertools::{Either, Itertools};
use rand::{Rng, thread_rng};
use rand::rngs::{StdRng, ThreadRng};
use crate::mv_crud_model::crud_operation::CRUDOperation;
use crate::mv_tree::locking_strategy::CRUDProtocol;
use crate::mv_tree::mvbplus_tree::{ClockType, MVBPlusTree};
use crate::mv_tx_model::transaction::AtomicTransaction;
use crate::mv_tx_model::tx_manager::TransactionManager;
use crate::mv_utils::safe_cell::SafeCell;

pub type IndexHandler =
Either<Arc<SafeCell<TransactionManager<FAN_OUT, NUM_RECORDS, u64, u64>>>, (CRUDProtocol, ClockType)>;

pub const FAN_OUT: usize = 127;
pub const NUM_RECORDS: usize = 127;

pub type Key = u64;
pub type Payload = u64;

pub fn inc_key(k: Key) -> Key {
    k.checked_add(1).unwrap_or(Key::MAX)
}

pub fn dec_key(k: Key) -> Key {
    k.checked_sub(1).unwrap_or(Key::MIN)
}


pub fn experiment(num_threads: usize,
                  index_handler: IndexHandler,
                  gc_enable: bool,
                  lambda: f64,
                  range_start: u64,
                  range_end: u64,
                  insert_ratio: usize,
                  update_ratio: usize,
                  delete_ratio: usize,
                  points_reads_ratio: usize,
                  range_reads_ratio: usize,
                  range_size: u64,
                  total_tx: &'static AtomicUsize)
                  -> (IndexHandler, Vec<(JoinHandle<(usize, usize, u128)>, Sender<()>)>)
{
    assert_eq!(insert_ratio + update_ratio + delete_ratio + points_reads_ratio + range_reads_ratio,
               100,
               "Ratios must add to 100%");

    #[inline(always)]
    fn gen_key(i: u64, range_start: u64, range_end: u64, lambda: f64, rnd: &mut ThreadRng) -> u64 {
        #[inline(always)]
        fn sample_next(lambda: f64, rnd: &mut ThreadRng) -> f64 {
            let num
                = rnd.gen_range(0_f64..1_f64);

            (1_f64 - num)
                .ln()
                .div(-lambda)
        }
        let range = range_end - range_start;
        (((loop {
            let key = i as f64 * (1_f64 - sample_next(lambda, rnd));
            if key >= 0_f64 {
                break key;
            }
        }) / range as f64) * u64::MAX as f64) as _
    }

    let manager = match index_handler {
        Either::Left(m_manager) => m_manager,
        Either::Right((protocol, clock_type)) => Arc::new(SafeCell::new(TransactionManager::new_with(
            1,
            MVBPlusTree::make_standard(protocol, clock_type),
            gc_enable,
        ))),
    };

    type WorkerSignal = ();

    let handles = (0..num_threads).map(|_| {
        let manager
            = manager.clone();

        let (thread_killer, thread_control)
            = crossbeam_channel::bounded::<WorkerSignal>(0);

        // tx_success, tx_error, time_spent
        let handle = spawn(move || {
            let mut rng
                = thread_rng();

            let mut generator = ||
                gen_key(range_end, range_start, range_end, lambda, &mut rng);

            let (mut tx_success, mut tx_error, start_execution_time)
                = (0usize, 0usize, SystemTime::now());

            let local_tx = |key: u64| -> AtomicTransaction<u64, u64> {
                let random_number
                    = thread_rng().gen_range(0..100);

                if random_number < insert_ratio {
                    AtomicTransaction::from_crud(CRUDOperation::Insert(key, u64::default()))
                } else if random_number < insert_ratio + points_reads_ratio {
                    AtomicTransaction::from_crud(CRUDOperation::PointSi(key))
                } else if random_number < insert_ratio + points_reads_ratio + range_reads_ratio {
                    if u64::MAX - range_size <= key {
                        AtomicTransaction::from_crud(CRUDOperation::RangeSi((key..=u64::MAX).into()))
                    } else {
                        AtomicTransaction::from_crud(CRUDOperation::RangeSi((key..key + range_size).into()))
                    }
                } else if random_number < insert_ratio + points_reads_ratio + range_reads_ratio + delete_ratio {
                    AtomicTransaction::from_crud(CRUDOperation::Delete(key))
                } else {
                    AtomicTransaction::from_crud(CRUDOperation::Update(key, u64::default()))
                }
            };
            loop {
                match thread_control.try_recv() {
                    Err(TryRecvError::Disconnected) => break,
                    _ => {
                        let next
                            = local_tx(generator());

                        match manager.execute_on_caller_thread(next).unwrap_atomic() {
                            Ok(_) => tx_success += 1,
                            Err(_) => tx_error += 1
                        }

                        total_tx.fetch_add(1, Relaxed);
                    }
                }
            }

            (tx_success,
             tx_error,
             SystemTime::now().duration_since(start_execution_time).unwrap().as_millis())
        });

        (handle, thread_killer)
    }).collect_vec();

    (IndexHandler::Left(manager), handles)
}

pub fn format_insertions(i: usize) -> String {
    if i % 1_000_000_000 == 0 {
        format!("{} B", i as f64 / 1_000_000_000_f64)
    } else if i % 1_000_000 == 0 {
        format!("{} Mio", i as f64 / 1_000_000_f64)
    } else if i % 1_000 == 0 {
        format!("{} K", i as f64 / 1_000_f64)
    } else {
        i.to_string()
    }
}

pub fn gen_data_exp(limit: u64, lambda: f64, rnd: &mut StdRng) -> Vec<u64> {
    (1..=limit)
        .map(|i|
            gen_rand_key(i, 0, i, lambda, rnd))
        .collect()
}

pub fn gen_rand_key(i: u64, range_start: u64, range_end: u64, lambda: f64, rnd: &mut StdRng) -> u64 {
    #[inline(always)]
    fn sample_next(lambda: f64, rnd: &mut StdRng) -> f64 {
        let num
            = rnd.gen_range(0_f64..1_f64);

        (1_f64 - num)
            .ln()
            .div(-lambda)
    }

    let range = range_end - range_start;

    (((loop {
        let key = i as f64 * (1_f64 - sample_next(lambda, rnd));
        if key >= 0_f64 {
            break key;
        }
    }) / range as f64) * u64::MAX as f64) as _
}