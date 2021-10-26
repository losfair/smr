use std::num::NonZeroU32;

use crossbeam::atomic::AtomicCell;

pub static REBALANCE_CPU_USAGE_HIGH_THRESHOLD: AtomicCell<f64> = AtomicCell::new(0.8);
pub static REBALANCE_CPU_USAGE_LOW_THRESHOLD: AtomicCell<f64> = AtomicCell::new(0.5);
pub static MAX_WORKERS_PER_APP: AtomicCell<usize> = AtomicCell::new(4);
pub static REBALANCE_TIMEOUT_MS: AtomicCell<u64> = AtomicCell::new(10);
pub static SPRING_CLEANING_INTERVAL_MS: AtomicCell<u64> = AtomicCell::new(100);
pub static APP_INACTIVE_TIMEOUT_MS: AtomicCell<u64> = AtomicCell::new(60000);
pub(crate) static PID_OVERRIDE: AtomicCell<Option<NonZeroU32>> = AtomicCell::new(None);
