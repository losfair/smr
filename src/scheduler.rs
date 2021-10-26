use anyhow::Result;
use parking_lot::Mutex;
use procfs::process::Process;
use rand::prelude::SliceRandom;
use rand::Rng;
use ringbuffer::{ConstGenericRingBuffer, RingBufferExt, RingBufferWrite};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::{collections::HashMap, time::Instant};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::{spawn_local, LocalSet};
use tokio::time::MissedTickBehavior;

use crate::config::{
  APP_INACTIVE_TIMEOUT_MS, MAX_WORKERS_PER_APP, REBALANCE_CPU_USAGE_HIGH_THRESHOLD,
  REBALANCE_CPU_USAGE_LOW_THRESHOLD, REBALANCE_TIMEOUT_MS, SPRING_CLEANING_INTERVAL_MS,
};
use crate::{
  pm::{PmHandle, WorkerManager},
  types::Request,
};

pub type InitDataGen<T> = Box<dyn Fn() -> T + Send + Sync + 'static>;

pub struct Scheduler<K, Req: Request> {
  pm: mpsc::UnboundedSender<(oneshot::Sender<Result<WorkerManager<Req>>>, Req::InitData)>,
  app_workers: HashMap<K, AppState<K, Req>>,
}

struct AppState<K, Req: Request> {
  key: K,
  workers: Vec<WorkerState<Req>>,
  last_use: Mutex<Instant>,
  worker_limit_reached: bool,
  init_data_gen: InitDataGen<Req::InitData>,
}

struct WorkerManagerWaiter<Req: Request>(Arc<RwLock<Option<Arc<WorkerManager<Req>>>>>);

impl<Req: Request> Clone for WorkerManagerWaiter<Req> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

struct WorkerState<Req: Request> {
  m: WorkerManagerWaiter<Req>,
  last_stat: Option<WorkerStats>,
  cpu_usage_window: ConstGenericRingBuffer<f64, 32>,
  crashed: Arc<AtomicBool>,
}

struct WorkerStats {
  stat_time: Instant,
  utime: f64,
  stime: f64,
  _rss_size_in_bytes: u64,
}

struct WorkerDelta {
  cpu_usage: f64,
}

impl WorkerStats {
  fn diff(&self, before: &WorkerStats) -> Result<WorkerDelta> {
    #[derive(Error, Debug)]
    #[error("cannot compute diff between the provided WorkerStats")]
    struct Invalid;

    if self.stat_time <= before.stat_time || self.utime < before.utime || self.stime < before.stime
    {
      return Err(Invalid.into());
    }
    let time_diff = self
      .stat_time
      .duration_since(before.stat_time)
      .as_secs_f64();
    Ok(WorkerDelta {
      cpu_usage: (self.utime - before.utime) / time_diff + (self.stime - before.stime) / time_diff,
    })
  }
}

impl<Req: Request> WorkerManagerWaiter<Req> {
  async fn wait(&self) -> Result<Arc<WorkerManager<Req>>> {
    #[derive(Error, Debug)]
    #[error("failed to initialize worker manager")]
    struct Failure;

    let m = (*self.0.read().await).clone();
    m.ok_or_else(|| Failure.into())
  }
}

impl<Req: Request> WorkerState<Req> {
  fn collect_stats_nb(&self) -> Option<WorkerStats> {
    let m = self.m.0.try_read().ok()?;
    let m = (&*m).as_ref()?;
    let tps = procfs::ticks_per_second().ok()? as u64;
    let p = Process::new(m.pid()).ok()?;
    let stat = p.stat().ok()?;
    Some(WorkerStats {
      stat_time: Instant::now(),
      utime: stat.utime as f64 / tps as f64,
      stime: stat.stime as f64 / tps as f64,
      _rss_size_in_bytes: stat.rss_bytes() as u64,
    })
  }
}

impl<K: Eq + Hash + Debug + Display + Clone + Send + Sync + 'static, Req: Request>
  AppState<K, Req>
{
  fn scale_out(
    &mut self,
    pm: &mpsc::UnboundedSender<(oneshot::Sender<Result<WorkerManager<Req>>>, Req::InitData)>,
    init_data: Req::InitData,
  ) -> Result<()> {
    #[derive(Error, Debug)]
    #[error("scale_out service error")]
    struct GenericError;
    let (tx, rx) = oneshot::channel();
    pm.send((tx, init_data)).map_err(|_| GenericError)?;

    let m: Arc<RwLock<Option<Arc<WorkerManager<Req>>>>> = Arc::new(RwLock::new(None));
    let mut m_write_owner = m.clone().try_write_owned().unwrap();
    let crashed = Arc::new(AtomicBool::new(false));

    // Asynchronously initialize the worker manager.
    {
      let crashed = crashed.clone();
      let start = Instant::now();
      let key = self.key.clone();

      tokio::spawn(async move {
        let m = rx.await;
        if let Ok(Ok(m)) = m {
          // Watch for crash.
          //
          // Normally only the `spring_cleaning` routine can start or stop workers.
          // If termination happens otherwise, the worker has crashed.
          m.notify_on_termination(move || {
            crashed.store(true, Ordering::Relaxed);
          });
          *m_write_owner = Some(Arc::new(m));
          let dur = start.elapsed();
          log::debug!("app {}: Worker initialization took {:?}.", key, dur);
        } else {
          // Start failure is equivalent to a crash!
          crashed.store(true, Ordering::Relaxed);
          *m_write_owner = None;
        }
      });
    }

    let state = WorkerState {
      m: WorkerManagerWaiter(m),
      last_stat: None,
      cpu_usage_window: ConstGenericRingBuffer::new(),
      crashed,
    };
    self.workers.push(state);
    Ok(())
  }

  fn scale_in(&mut self) {
    if self.workers.len() <= 1 {
      return;
    }

    let mut rng = rand::thread_rng();
    let index = rng.gen_range(0..self.workers.len());
    self.workers.swap_remove(index);
  }

  fn pick_worker(&self) -> Result<WorkerManagerWaiter<Req>> {
    #[derive(Error, Debug)]
    #[error("all workers have crashed")]
    struct Crashed;

    *self.last_use.lock() = Instant::now();

    let mut workers = self
      .workers
      .iter()
      .filter(|x| !x.crashed.load(Ordering::Relaxed))
      .collect::<Vec<_>>();

    // If we have workers that we know are ready, use it
    {
      let ready_workers = workers
        .iter()
        .filter(|x| x.m.0.try_read().is_ok())
        .copied()
        .collect::<Vec<_>>();
      if !ready_workers.is_empty() {
        workers = ready_workers;
      }
    }

    // This only happens if all worker processes have crashed. This is considered an unlikely event.
    if workers.is_empty() {
      return Err(Crashed.into());
    }

    let index = rand::thread_rng().gen_range(0..workers.len());

    Ok(workers[index].m.clone())
  }
}

impl<K: Eq + Hash + Debug + Display + Clone + Send + Sync + 'static, Req: Request>
  Scheduler<K, Req>
{
  pub fn new(pm: PmHandle<Req>) -> Arc<RwLock<Self>> {
    let (pms_tx, pms_rx) = mpsc::unbounded_channel();
    let scheduler = Arc::new(RwLock::new(Self {
      pm: pms_tx,
      app_workers: HashMap::new(),
    }));
    let weak = Arc::downgrade(&scheduler);
    tokio::spawn(Self::background_task(weak));
    std::thread::spawn(move || {
      tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(pm_service(pm, pms_rx))
    });
    scheduler
  }

  pub async fn get_num_workers_for_app(self_wrapper: &RwLock<Self>, k: &K) -> usize {
    let me = self_wrapper.read().await;
    if let Some(app) = me.app_workers.get(k) {
      app.workers.len()
    } else {
      0
    }
  }

  pub async fn get_worker(
    self_wrapper: &RwLock<Self>,
    k: &K,
    init_data_gen: impl Fn() -> Req::InitData + Send + Sync + 'static,
  ) -> Result<Arc<WorkerManager<Req>>> {
    let me = self_wrapper.read().await;
    if let Some(app) = me.app_workers.get(k) {
      let worker = app.pick_worker()?;
      drop(me);
      Ok(worker.wait().await?)
    } else {
      drop(me);

      // Acquire write lock and double check
      let mut me = self_wrapper.write().await;
      if let Some(app) = me.app_workers.get(k) {
        let worker = app.pick_worker()?;
        drop(me);
        Ok(worker.wait().await?)
      } else {
        log::debug!("app {}: Starting first worker.", k);
        let init_data_gen: InitDataGen<Req::InitData> = Box::new(init_data_gen);
        let init_data = init_data_gen();
        let mut app = AppState {
          key: k.clone(),
          workers: vec![],
          last_use: Mutex::new(Instant::now()),
          worker_limit_reached: false,
          init_data_gen,
        };
        app.scale_out(&me.pm, init_data)?;
        let m = app.workers[0].m.clone();
        me.app_workers.insert(k.clone(), app);
        drop(me);
        Ok(m.wait().await?)
      }
    }
  }

  async fn background_task(me: Weak<RwLock<Self>>) {
    log::debug!("Started spring cleaning task.");
    let mut spring_cleaning_interval =
      tokio::time::interval(Duration::from_millis(SPRING_CLEANING_INTERVAL_MS.load()));
    spring_cleaning_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
      spring_cleaning_interval.tick().await;
      let me = match me.upgrade() {
        Some(x) => x,
        None => {
          log::debug!("Shutting down spring cleaning task.");
          break;
        }
      };
      let mut me = me.write().await;
      let start = Instant::now();
      me.do_spring_cleaning().await;
      let end = Instant::now();
      log::trace!(
        "Spring cleaning finished in {:?}.",
        end.duration_since(start)
      );
    }
  }

  async fn do_spring_cleaning(&mut self) {
    self.cleanup_crashed_workers();

    let rebalance_deadline = Instant::now() + Duration::from_millis(REBALANCE_TIMEOUT_MS.load());
    self.rebalance(rebalance_deadline).await;
  }

  fn cleanup_crashed_workers(&mut self) {
    let mut keys_to_remove = vec![];
    for (k, v) in &mut self.app_workers {
      let workers = std::mem::replace(&mut v.workers, vec![]);
      let prev_count = workers.len();
      v.workers = workers
        .into_iter()
        .filter(|x| !x.crashed.load(Ordering::Relaxed))
        .collect();
      let new_count = v.workers.len();
      let crash_count = prev_count - new_count;
      if crash_count != 0 {
        log::warn!("app {}: {} worker(s) crashed.", k, crash_count);
      }

      // Don't allow zero-length `workers` list.
      if v.workers.len() == 0 {
        keys_to_remove.push(k.clone());
      }
    }

    for k in keys_to_remove {
      self.app_workers.remove(&k);
    }
  }

  async fn rebalance(&mut self, deadline: Instant) {
    let mut apps = self.app_workers.iter_mut().collect::<Vec<_>>();
    apps.shuffle(&mut rand::thread_rng());

    let mut to_remove: Vec<K> = vec![];

    for (i, (k, app_state)) in apps.into_iter().enumerate() {
      let now = Instant::now();

      // Soft realtime!
      if now > deadline {
        log::warn!("Rebalance job exceeded its deadline. Visited {} app(s).", i);
        break;
      }

      // Remove apps that are not used in a long time.
      let last_use = *app_state.last_use.lock();
      if now.duration_since(last_use) > Duration::from_millis(APP_INACTIVE_TIMEOUT_MS.load()) {
        log::debug!("app {}: Removing due to inactivity.", k);
        to_remove.push(k.clone());
        continue;
      }

      // Collect stats
      for w in &mut *app_state.workers {
        if let Some(stat) = w.collect_stats_nb() {
          if let Some(last_stat) = &w.last_stat {
            if let Ok(delta) = stat.diff(last_stat) {
              w.cpu_usage_window.push(delta.cpu_usage);
            }
          }
          w.last_stat = Some(stat);
        }
      }

      // Make rebalance decisions based on stats.
      let global_max_usage = app_state
        .workers
        .iter()
        .flat_map(|x| x.cpu_usage_window.iter().copied())
        .reduce(|a, b| a.max(b))
        .unwrap_or(0.0);
      let recent_min_usage = app_state
        .workers
        .iter()
        .filter_map(|x| x.cpu_usage_window.back().copied())
        .reduce(|a, b| a.min(b))
        .unwrap_or(0.0);

      let max_workers_per_app = MAX_WORKERS_PER_APP.load();

      // Scale out. Sensitive to recent usage.
      if recent_min_usage > REBALANCE_CPU_USAGE_HIGH_THRESHOLD.load() {
        if app_state.workers.len() >= max_workers_per_app {
          if !app_state.worker_limit_reached {
            app_state.worker_limit_reached = true;
            log::warn!("app {}: CPU usage is above threshold but the number of running workers is already {}. Not scaling out.", k, app_state.workers.len());
          }
        } else {
          log::debug!(
            "app {}: Scaling out. Previously, the number of running workers is {}.",
            k,
            app_state.workers.len()
          );
          let init_data = (app_state.init_data_gen)();
          if let Err(e) = app_state.scale_out(&self.pm, init_data) {
            log::error!("app {}: Scale out failed: {:?}", k, e);
          }
        }
      }

      // Scale in. Sensitive to global usage.
      if global_max_usage < REBALANCE_CPU_USAGE_LOW_THRESHOLD.load() {
        if app_state.workers.len() > 1 {
          log::debug!(
            "app {}: Scaling in. Previously, the number of running workers is {}.",
            k,
            app_state.workers.len()
          );
          app_state.scale_in();
          app_state.worker_limit_reached = false;
        }
      }

      // Respond to MAX_WORKERS_PER_APP change.
      if app_state.workers.len() > max_workers_per_app {
        log::info!(
          "app {}: Shrinking worker set from {} to {}.",
          k,
          app_state.workers.len(),
          app_state.workers.len() - 1
        );
        app_state.scale_in();
      }
    }

    for k in to_remove {
      self.app_workers.remove(&k);
    }
  }
}

async fn pm_service<Req: Request>(
  handle: PmHandle<Req>,
  mut rx: mpsc::UnboundedReceiver<(oneshot::Sender<Result<WorkerManager<Req>>>, Req::InitData)>,
) {
  let handle = Rc::new(handle);
  let local = LocalSet::new();
  local
    .run_until(async move {
      loop {
        let (back, init_data) = match rx.recv().await {
          Some(x) => x,
          None => break,
        };
        let handle = handle.clone();
        spawn_local(async move {
          let worker = handle.start_worker(init_data).await.map(|x| x.grab());
          let _ = back.send(worker);
        });
      }
    })
    .await
}
